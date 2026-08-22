// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Committed apply path for cache-mode metadata (task #3, contract §3).
//!
//! Every cache journal entry is applied to the authoritative store by
//! exactly one of the `apply_*` methods here, identically on the leader and
//! on followers: the service layer proposes, the committed apply mutates
//! RocksDB, and nothing pre-applies or touches the UFS. Replay entries
//! carry absolute values (incarnation / generation / object id), so replay
//! is deterministic; the methods additionally tolerate re-applying an
//! entry whose effects are already present (exact match or a strictly
//! later committed state) so a restart that replays over populated state
//! converges instead of aborting. Any other divergence fails loudly.
//!
//! The single-writer precondition of [`crate::master::meta::cache::store`]
//! holds here: the committed journal apply is the only creator of these
//! rows.

use crate::master::meta::block_id::{BlockIdCodec, CacheObjectId};
use crate::master::meta::cache::entry::{
    validate_incarnation, CacheEntry, CacheEntryState, ExpiryRow, IncarnationRow, ObjectRow,
    OpOutcome, OpToken,
};
use crate::master::meta::cache::store::{state_tags, CacheWrite, LocalCacheIndexStore};
use curvine_core_error::{err_box, err_msg, CommonError, CommonResult};
use curvine_runtime::sync::AtomicLong;

/// Highest incarnation the in-process allocator may issue. Kept at
/// `i64::MAX` so the watermark fits the i64 state tag; strictly below the
/// store-side [`MAX_ALLOCATABLE_INCARNATION`] second gate.
pub const MAX_ISSUABLE_INCARNATION: u64 = i64::MAX as u64;

pub struct CacheManager {
    object_ids: CacheObjectId,
    /// Last issued mount incarnation; 0 = none issued yet.
    incarnations: AtomicLong,
}

/// Pre-write classification of an identity-producing operation against the
/// idempotency index (contract §3 rev2). The same token can never execute
/// twice and can never re-execute after its outcome window evicted it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenGate {
    /// No recorded outcome and the token is above the client's
    /// high-watermark: execute and persist the outcome.
    Execute,
    /// The exact outcome is already committed (replay/retry): no write.
    AlreadyApplied,
    /// The outcome window evicted the record and the token is at or below
    /// the client high-watermark: terminal — never re-allocate, no write.
    /// (A committed journal entry in this state was applied before the
    /// eviction, so replay converges without re-executing.)
    Expired,
}

impl Default for CacheManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheManager {
    pub fn new() -> Self {
        Self {
            object_ids: CacheObjectId::new(),
            incarnations: AtomicLong::new(0),
        }
    }

    pub fn current_object_id(&self) -> i64 {
        self.object_ids.current()
    }

    /// Leader-only issuance: consume the next object id from the volatile
    /// allocator. Uniqueness and in-segment monotonicity are guaranteed by
    /// the caller holding the service issue lock; the durable fence (id
    /// must be <= the committed reserve watermark) is enforced by the
    /// committed allocate apply.
    pub fn next_object_id(&self) -> CommonResult<i64> {
        self.object_ids.next()
    }

    pub fn current_incarnation(&self) -> u64 {
        self.incarnations.get() as u64
    }

    /// Restore in-memory allocators from the durable watermarks after a
    /// snapshot restore (both only move forward; the store-side validation
    /// is the first gate, this is the second).
    pub fn restore_watermarks<S: LocalCacheIndexStore>(&self, store: &S) -> CommonResult<()> {
        if let Some(v) = store
            .cache_get_state(state_tags::CACHE_OBJECT_ID)
            .map_err(cv)?
        {
            self.advance_object_watermark(v)?;
        }
        if let Some(v) = store
            .cache_get_state(state_tags::CACHE_INCARNATION)
            .map_err(cv)?
        {
            if v < 0 || v as u64 > MAX_ISSUABLE_INCARNATION {
                return err_box!(
                    "restored incarnation watermark outside issuable range: {}",
                    v
                );
            }
            self.advance_incarnation(v as u64);
        }
        Ok(())
    }

    /// Forward-only in-memory object id watermark move (replay over already
    /// advanced state must not fail).
    fn advance_object_watermark(&self, value: i64) -> CommonResult<()> {
        if value > self.object_ids.current() {
            self.object_ids.reset(value)?;
        }
        Ok(())
    }

    fn advance_incarnation(&self, value: u64) {
        loop {
            let c = self.incarnations.get();
            if value <= c as u64 {
                return;
            }
            if self.incarnations.compare_and_set(c, value as i64) {
                return;
            }
        }
    }

    fn check_token(token: OpToken) -> CommonResult<()> {
        if token.op_seq == 0 {
            return err_box!("op token op_seq must be >= 1: {:?}", token);
        }
        Ok(())
    }

    /// Classify an identity-producing op against the durable idempotency
    /// index. `outcome` is what this entry would persist if executed.
    fn classify_token<S: LocalCacheIndexStore>(
        store: &S,
        token: OpToken,
        outcome: &OpOutcome,
    ) -> CommonResult<TokenGate> {
        match store.cache_get_outcome(token).map_err(cv)? {
            Some(recorded) => {
                if &recorded == outcome {
                    Ok(TokenGate::AlreadyApplied)
                } else {
                    err_box!(
                        "idempotent op replay divergence for token {:?}: recorded {:?}, entry {:?}",
                        token,
                        recorded,
                        outcome
                    )
                }
            }
            None => match store.cache_client_watermark(token.client_id).map_err(cv)? {
                Some(hw) if token.op_seq <= hw => Ok(TokenGate::Expired),
                _ => Ok(TokenGate::Execute),
            },
        }
    }

    /// Identity-producing: reserve the global cache object id segment
    /// `[start, end)`. Advances the durable and in-memory watermark to
    /// `end - 1` and persists the exact outcome for the token.
    pub fn apply_id_reserve<S: LocalCacheIndexStore>(
        &self,
        store: &S,
        token: OpToken,
        start: i64,
        end: i64,
    ) -> CommonResult<()> {
        Self::check_token(token)?;
        if start < BlockIdCodec::CACHE_OBJECT_MIN || end <= start {
            return err_box!("cache id reserve segment invalid: [{}, {})", start, end);
        }
        if end - 1 > BlockIdCodec::CACHE_OBJECT_MAX {
            return err_box!(
                "cache id reserve segment exceeds domain end {}: [{}, {})",
                BlockIdCodec::CACHE_OBJECT_MAX,
                start,
                end
            );
        }

        let gate = Self::classify_token(store, token, &OpOutcome::Reserved { start, end })?;
        match gate {
            // Exact recorded history: the only permitted way to recover the
            // in-memory watermark on replay.
            TokenGate::AlreadyApplied => {
                self.advance_object_watermark(end - 1)?;
                return Ok(());
            }
            // Terminal, strict no-op: an expired token's parameters are NOT
            // trusted history (they may never have executed), so neither the
            // durable state nor the volatile allocator may move.
            TokenGate::Expired => return Ok(()),
            TokenGate::Execute => (),
        }

        // Execute: only a genuinely-new absolute transition. The segment
        // must be contiguous with the durable watermark — a different token
        // may never re-reserve, overlap, or regress a segment.
        let durable = store
            .cache_get_state(state_tags::CACHE_OBJECT_ID)
            .map_err(cv)?;
        let expected_start = match durable {
            Some(hw) => hw + 1,
            None => BlockIdCodec::CACHE_OBJECT_MIN,
        };
        if start != expected_start {
            return err_box!(
                "cache id reserve segment [{}, {}) is not contiguous with durable watermark {:?}: expected start {}",
                start,
                end,
                durable,
                expected_start
            );
        }

        self.advance_object_watermark(end - 1)?;

        let mut w = store.cache_write();
        w.set_state(state_tags::CACHE_OBJECT_ID, end - 1)
            .map_err(cv)?;
        w.put_outcome(token, &OpOutcome::Reserved { start, end })
            .map_err(cv)?;
        w.set_client_watermark(token.client_id, token.op_seq)
            .map_err(cv)?;
        w.commit().map_err(cv)?;
        Ok(())
    }

    /// Identity-producing: allocate a never-reused mount incarnation.
    pub fn apply_incarnation_allocate<S: LocalCacheIndexStore>(
        &self,
        store: &S,
        token: OpToken,
        mount_id: u32,
        incarnation: u64,
    ) -> CommonResult<()> {
        Self::check_token(token)?;
        if incarnation == 0 || incarnation > MAX_ISSUABLE_INCARNATION {
            return err_box!(
                "incarnation outside issuable range [1, {}]: {}",
                MAX_ISSUABLE_INCARNATION,
                incarnation
            );
        }

        let gate = Self::classify_token(
            store,
            token,
            &OpOutcome::IncarnationAllocated { incarnation },
        )?;
        match gate {
            // Exact recorded history: may recover the in-memory watermark.
            TokenGate::AlreadyApplied => {
                self.advance_incarnation(incarnation);
                return Ok(());
            }
            // Terminal, strict no-op: the entry's parameters are not
            // trusted history.
            TokenGate::Expired => return Ok(()),
            TokenGate::Execute => (),
        }

        // Execute: only a genuinely-new absolute transition. The incarnation
        // must be strictly above the durable watermark and its row absent —
        // a different token may never alias an existing incarnation, even
        // an identical or revoked one.
        if let Some(row) = store.cache_get_incarnation(incarnation).map_err(cv)? {
            return err_box!(
                "incarnation {} already exists (mount {}, revoked {}) under a different token",
                incarnation,
                row.mount_id,
                row.revoked
            );
        }
        if let Some(hw) = store
            .cache_get_state(state_tags::CACHE_INCARNATION)
            .map_err(cv)?
        {
            if incarnation <= hw as u64 {
                return err_box!(
                    "incarnation {} is not above the durable watermark {}",
                    incarnation,
                    hw
                );
            }
        }

        // Never regress an existing newer pointer for this mount.
        let write_pointer = match store.cache_current_incarnation(mount_id).map_err(cv)? {
            Some(c) => c < incarnation,
            None => true,
        };

        // The in-memory allocator tracks the transition it just executed
        // (mirrors reserve/allocate; only replay recovery goes through the
        // AlreadyApplied branch above).
        self.advance_incarnation(incarnation);

        let mut w = store.cache_write();
        w.put_incarnation(
            incarnation,
            IncarnationRow {
                mount_id,
                revoked: false,
            },
        )
        .map_err(cv)?;
        if write_pointer {
            w.set_current_incarnation(mount_id, incarnation)
                .map_err(cv)?;
        }
        w.put_outcome(token, &OpOutcome::IncarnationAllocated { incarnation })
            .map_err(cv)?;
        w.set_client_watermark(token.client_id, token.op_seq)
            .map_err(cv)?;
        w.set_state(state_tags::CACHE_INCARNATION, incarnation as i64)
            .map_err(cv)?;
        w.commit().map_err(cv)?;
        Ok(())
    }

    /// Conditional: revoke a mount incarnation. The row is kept (marked
    /// revoked, durable forever); the mount pointer is cleared only if it
    /// still names this incarnation.
    pub fn apply_incarnation_revoke<S: LocalCacheIndexStore>(
        &self,
        store: &S,
        mount_id: u32,
        incarnation: u64,
    ) -> CommonResult<()> {
        validate_incarnation(incarnation)?;
        match store.cache_get_incarnation(incarnation).map_err(cv)? {
            None => {
                // Not present: nothing to revoke (already vacuumed or a
                // replay over newer state).
                return Ok(());
            }
            Some(row) => {
                if row.mount_id != mount_id {
                    return err_box!(
                        "incarnation {} belongs to mount {}, revoke says mount {}",
                        incarnation,
                        row.mount_id,
                        mount_id
                    );
                }
                if row.revoked {
                    return Ok(());
                }
            }
        }

        let clear_pointer = matches!(store.cache_current_incarnation(mount_id).map_err(cv)?, Some(c) if c == incarnation);

        let mut w = store.cache_write();
        w.put_incarnation(
            incarnation,
            IncarnationRow {
                mount_id,
                revoked: true,
            },
        )
        .map_err(cv)?;
        if clear_pointer {
            w.clear_current_incarnation(mount_id).map_err(cv)?;
        }
        w.commit().map_err(cv)?;
        Ok(())
    }

    /// Identity-producing: per-key load allocation. Writes the `Reserved`
    /// entry, its reverse row, and the outcome binding the object id.
    pub fn apply_allocate<S: LocalCacheIndexStore>(
        &self,
        store: &S,
        token: OpToken,
        incarnation: u64,
        key: &str,
        entry: &CacheEntry,
    ) -> CommonResult<()> {
        Self::check_token(token)?;
        validate_incarnation(incarnation)?;
        if entry.state != CacheEntryState::Reserved {
            return err_box!(
                "cache allocate entry must be Reserved, got {:?}",
                entry.state
            );
        }

        let gate = Self::classify_token(
            store,
            token,
            &OpOutcome::Allocated {
                incarnation,
                key: key.to_string(),
                generation: entry.generation,
                object_id: entry.object_id,
            },
        )?;
        match gate {
            // Exact recorded history: may recover the in-memory watermark;
            // the durable entry state is whatever the earlier execution left.
            TokenGate::AlreadyApplied => {
                self.advance_object_watermark(entry.object_id)?;
                return Ok(());
            }
            // Terminal, strict no-op: the entry's parameters are not
            // trusted history.
            TokenGate::Expired => return Ok(()),
            TokenGate::Execute => (),
        }

        // Execute: only a genuinely-new absolute transition. A different
        // token may never alias an existing row (exact match, later
        // generation, or overwriting a live entry): the only legal
        // re-allocation is Tombstoned@g -> Reserved@g+1, or a fresh key at
        // generation 1.
        match store.cache_get_entry(incarnation, key).map_err(cv)? {
            Some(cur) => {
                if cur.state != CacheEntryState::Tombstoned {
                    return err_box!(
                        "cache allocate alias for ({}, {}): committed {:?}@{} under a different token, only Tombstoned@g -> Reserved@g+1 re-opens a key",
                        incarnation,
                        key,
                        cur.state,
                        cur.generation
                    );
                }
                let next = cur.generation.checked_add(1).ok_or_else(|| {
                    CommonError::from(err_msg!(
                        "cache allocate generation overflow at u64::MAX for ({}, {})",
                        incarnation,
                        key
                    ))
                })?;
                if entry.generation != next {
                    return err_box!(
                        "cache allocate CAS violation for ({}, {}): Tombstoned@{} may only move to Reserved@{}, entry says {}",
                        incarnation,
                        key,
                        cur.generation,
                        next,
                        entry.generation
                    );
                }
                // Per-key durable issuance fence: every new generation of
                // a key must consume a strictly greater object id than the
                // generation it replaces (segment issuance is monotonic).
                if entry.object_id <= cur.object_id {
                    return err_box!(
                        "cache allocate object id regression for ({}, {}): new generation {} may not reuse object id {} <= {}",
                        incarnation,
                        key,
                        entry.generation,
                        entry.object_id,
                        cur.object_id
                    );
                }
                // Tombstoned@g -> Reserved@g+1: a fresh load generation.
            }
            None => {
                if entry.generation != 1 {
                    return err_box!(
                        "cache allocate for missing entry ({}, {}) must start at generation 1, got {}",
                        incarnation,
                        key,
                        entry.generation
                    );
                }
            }
        }

        // Object id fencing (contract §3: no object ID escapes Allocate
        // before its segment reservation passes the barrier, and no issued
        // ID is reused). A fresh allocation may only consume an id at or
        // below the durable reserve watermark, and may never collide with
        // a live reverse row owned by another key or token. (The reverse
        // row is deleted only when its owning version is tombstoned; the
        // per-key monotonic check above plus the monotonic segment
        // watermark are the durable fences. Cross-key reuse of an id whose
        // owning load is already dead cannot be observed durably here and
        // is excluded by issuer discipline: the cache service consumes
        // each segment id exactly once.)
        let durable_hw = store
            .cache_get_state(state_tags::CACHE_OBJECT_ID)
            .map_err(cv)?
            .unwrap_or(BlockIdCodec::CACHE_OBJECT_MIN - 1);
        if entry.object_id > durable_hw {
            return err_box!(
                "cache allocate object id {} is beyond the durable reserve watermark {} for ({}, {})",
                entry.object_id,
                durable_hw,
                incarnation,
                key
            );
        }
        if let Some(row) = store.cache_get_object(entry.object_id).map_err(cv)? {
            return err_box!(
                "cache allocate object id {} is already owned by ({}, {})@{} under a different token",
                entry.object_id,
                row.incarnation,
                row.key,
                row.generation
            );
        }

        self.advance_object_watermark(entry.object_id)?;

        let mut w = store.cache_write();
        w.put_entry(incarnation, key, entry).map_err(cv)?;
        w.put_object(
            entry.object_id,
            &ObjectRow {
                incarnation,
                key: key.to_string(),
                generation: entry.generation,
            },
        )
        .map_err(cv)?;
        w.put_outcome(
            token,
            &OpOutcome::Allocated {
                incarnation,
                key: key.to_string(),
                generation: entry.generation,
                object_id: entry.object_id,
            },
        )
        .map_err(cv)?;
        w.set_client_watermark(token.client_id, token.op_seq)
            .map_err(cv)?;
        w.commit().map_err(cv)?;
        Ok(())
    }

    /// Conditional CAS: `Reserved@generation` -> `Valid` with the final
    /// `(len, ufs_mtime, expire_at)`. Writing the expiry row (when
    /// `expire_at > 0`) is part of the same atomic batch.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_commit<S: LocalCacheIndexStore>(
        &self,
        store: &S,
        incarnation: u64,
        key: &str,
        generation: u64,
        expected_object_id: i64,
        len: i64,
        ufs_mtime: i64,
        expire_at: i64,
    ) -> CommonResult<()> {
        validate_incarnation(incarnation)?;
        let cur = match store.cache_get_entry(incarnation, key).map_err(cv)? {
            Some(v) => v,
            None => return err_box!("cache commit for missing entry ({}, {})", incarnation, key),
        };

        // A strictly later generation already advanced: this commit was
        // superseded before it could apply, or replay runs over final
        // state. Terminal no-op (the old load is dead).
        if cur.generation > generation {
            return Ok(());
        }

        // Object identity CAS (contract §2.3): at or below the commit
        // generation, the commit must land on the object its allocate
        // reserved. A mismatch is replay divergence.
        if cur.object_id != expected_object_id {
            return err_box!(
                "cache commit object identity mismatch for ({}, {})@{}: committed object {} vs expected {}",
                incarnation,
                key,
                generation,
                cur.object_id,
                expected_object_id
            );
        }
        if cur.generation == generation {
            match cur.state {
                CacheEntryState::Valid
                    if cur.len == len
                        && cur.ufs_mtime == ufs_mtime
                        && cur.expire_at == expire_at =>
                {
                    return Ok(()); // already applied
                }
                // The normal transition: Reserved@generation -> Valid.
                CacheEntryState::Reserved => (),
                other => {
                    return err_box!(
                        "cache commit CAS violation for ({}, {})@{}: committed state {:?} with len {} mtime {} expire {}",
                        incarnation,
                        key,
                        generation,
                        other,
                        cur.len,
                        cur.ufs_mtime,
                        cur.expire_at
                    )
                }
            }
        } else {
            // cur.generation < generation: a commit may never skip
            // generations — its allocate wrote Reserved@generation.
            return err_box!(
                "cache commit CAS violation for ({}, {}): committed generation {} is below commit generation {}",
                incarnation,
                key,
                cur.generation,
                generation
            );
        }

        let new = CacheEntry {
            generation,
            state: CacheEntryState::Valid,
            object_id: cur.object_id,
            len,
            ufs_mtime,
            block_size: cur.block_size,
            expire_at,
        };

        let mut w = store.cache_write();
        // Validates: Valid requires ufs_mtime > 0 and expire_at >= 0.
        w.put_entry(incarnation, key, &new).map_err(cv)?;
        if expire_at > 0 {
            w.put_expiry(&ExpiryRow {
                expire_at,
                incarnation,
                object_id: cur.object_id,
                key: key.to_string(),
                generation,
            })
            .map_err(cv)?;
        }
        w.commit().map_err(cv)?;
        Ok(())
    }

    /// Conditional CAS: any state at `expected_generation` ->
    /// `Tombstoned` at `expected_generation + 1`. Drops the superseded
    /// version's expiry and reverse rows atomically.
    pub fn apply_remove<S: LocalCacheIndexStore>(
        &self,
        store: &S,
        incarnation: u64,
        key: &str,
        expected_generation: u64,
        new_generation: u64,
        expected_object_id: i64,
    ) -> CommonResult<()> {
        validate_incarnation(incarnation)?;
        if Some(new_generation) != expected_generation.checked_add(1) {
            return err_box!(
                "cache remove generations not adjacent: expected {}, new {}",
                expected_generation,
                new_generation
            );
        }

        let cur = match store.cache_get_entry(incarnation, key).map_err(cv)? {
            Some(v) => v,
            None => return err_box!("cache remove for missing entry ({}, {})", incarnation, key),
        };

        // Later state already advanced past this remove: converge.
        if cur.generation > new_generation {
            return Ok(());
        }

        // Object identity CAS (contract §2.3): at or below the remove's
        // generations, the remove must target the object the caller
        // observed. A mismatch is replay divergence.
        if cur.object_id != expected_object_id {
            return err_box!(
                "cache remove object identity mismatch for ({}, {})@{}: committed object {} vs expected {}",
                incarnation,
                key,
                expected_generation,
                cur.object_id,
                expected_object_id
            );
        }
        if cur.generation == new_generation {
            if cur.state == CacheEntryState::Tombstoned {
                return Ok(()); // already applied
            }
            return err_box!(
                "cache remove replay divergence for ({}, {}): state {:?} at new generation {}",
                incarnation,
                key,
                cur.state,
                new_generation
            );
        }
        if cur.generation != expected_generation {
            return err_box!(
                "cache remove CAS violation for ({}, {}): committed generation {} vs expected {}",
                incarnation,
                key,
                cur.generation,
                expected_generation
            );
        }

        let new = CacheEntry {
            generation: new_generation,
            state: CacheEntryState::Tombstoned,
            object_id: cur.object_id,
            len: 0,
            ufs_mtime: cur.ufs_mtime,
            block_size: cur.block_size,
            expire_at: 0,
        };

        let mut w = store.cache_write();
        w.put_entry(incarnation, key, &new).map_err(cv)?;
        if cur.expire_at > 0 {
            w.delete_expiry(cur.expire_at, incarnation, cur.object_id)
                .map_err(cv)?;
        }
        // The reverse row is only a GC hint for the superseded version.
        w.delete_object(cur.object_id).map_err(cv)?;
        w.commit().map_err(cv)?;
        Ok(())
    }
}

/// FsResult -> CommonResult error adapter (single definition for the whole
/// committed-apply path).
fn cv(e: curvine_error::FsError) -> CommonError {
    e.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::store::RocksInodeStore;
    use crate::master::Master;
    use curvine_rocksdb::DBConf;
    use curvine_runtime::common::Utils;

    fn new_store(name: &str) -> RocksInodeStore {
        Master::init_test_metrics();
        let conf = DBConf::new(Utils::test_sub_dir(format!(
            "cache-manager-test/{}-{}",
            name,
            Utils::rand_str(6)
        )));
        RocksInodeStore::new(conf, true).expect("store")
    }

    const OBJ: i64 = BlockIdCodec::CACHE_OBJECT_MIN;

    fn token(client: u64, seq: u64) -> OpToken {
        OpToken {
            client_id: client,
            op_seq: seq,
        }
    }

    fn reserved(gen: u64, object_id: i64) -> CacheEntry {
        CacheEntry {
            generation: gen,
            state: CacheEntryState::Reserved,
            object_id,
            len: 0,
            ufs_mtime: 0,
            block_size: 128,
            expire_at: 0,
        }
    }

    #[test]
    fn test_id_reserve_and_watermark_restore() {
        let store = new_store("id-reserve");
        let mgr = CacheManager::new();

        mgr.apply_id_reserve(&store, token(1, 1), OBJ, OBJ + 10)
            .unwrap();
        assert_eq!(mgr.current_object_id(), OBJ + 9);
        assert_eq!(
            store.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
            Some(OBJ + 9)
        );
        assert_eq!(
            store.cache_get_outcome(token(1, 1)).unwrap(),
            Some(OpOutcome::Reserved {
                start: OBJ,
                end: OBJ + 10
            })
        );
        assert_eq!(store.cache_client_watermark(1).unwrap(), Some(1));

        // A fresh manager restores the watermark from the state CF.
        let mgr2 = CacheManager::new();
        mgr2.restore_watermarks(&store).unwrap();
        assert_eq!(mgr2.current_object_id(), OBJ + 9);

        // Segments chain forward; replaying the same entry is idempotent.
        mgr.apply_id_reserve(&store, token(1, 2), OBJ + 10, OBJ + 20)
            .unwrap();
        mgr.apply_id_reserve(&store, token(1, 2), OBJ + 10, OBJ + 20)
            .unwrap();
        assert_eq!(mgr.current_object_id(), OBJ + 19);

        // Invalid segments rejected.
        assert!(mgr
            .apply_id_reserve(&store, token(1, 3), OBJ - 1, OBJ)
            .is_err());
        assert!(mgr
            .apply_id_reserve(&store, token(1, 3), OBJ + 5, OBJ + 5)
            .is_err());
        assert!(mgr
            .apply_id_reserve(
                &store,
                token(1, 3),
                BlockIdCodec::CACHE_OBJECT_MAX,
                BlockIdCodec::CACHE_OBJECT_MAX + 2
            )
            .is_err());
        assert!(mgr
            .apply_id_reserve(&store, token(1, 0), OBJ, OBJ + 1)
            .is_err());
    }

    #[test]
    fn test_incarnation_lifecycle() {
        let store = new_store("incarnation");
        let mgr = CacheManager::new();

        mgr.apply_incarnation_allocate(&store, token(2, 1), 7, 1)
            .unwrap();
        assert_eq!(store.cache_current_incarnation(7).unwrap(), Some(1));
        assert!(!store.cache_get_incarnation(1).unwrap().unwrap().revoked);
        assert_eq!(
            store.cache_get_outcome(token(2, 1)).unwrap(),
            Some(OpOutcome::IncarnationAllocated { incarnation: 1 })
        );

        // Idempotent replay and restore.
        mgr.apply_incarnation_allocate(&store, token(2, 1), 7, 1)
            .unwrap();
        let mgr2 = CacheManager::new();
        mgr2.restore_watermarks(&store).unwrap();
        assert_eq!(mgr2.current_incarnation(), 1);

        // Remount: new incarnation, pointer moves.
        mgr.apply_incarnation_allocate(&store, token(2, 2), 7, 2)
            .unwrap();
        assert_eq!(store.cache_current_incarnation(7).unwrap(), Some(2));

        // Revoke the current incarnation: pointer cleared, row kept.
        mgr.apply_incarnation_revoke(&store, 7, 2).unwrap();
        assert_eq!(store.cache_current_incarnation(7).unwrap(), None);
        assert!(store.cache_get_incarnation(2).unwrap().unwrap().revoked);
        // Idempotent revoke.
        mgr.apply_incarnation_revoke(&store, 7, 2).unwrap();

        // Late allocate replay for an incarnation that was later revoked
        // converges instead of resurrecting the pointer.
        mgr.apply_incarnation_allocate(&store, token(2, 2), 7, 2)
            .unwrap();
        assert_eq!(store.cache_current_incarnation(7).unwrap(), None);

        // Divergence: same incarnation claimed by another mount.
        assert!(mgr
            .apply_incarnation_allocate(&store, token(3, 1), 8, 1)
            .is_err());
        // Issuable bound is the second gate (u64::MAX rejected by the store
        // as the first gate, i64::MAX by the allocator).
        assert!(mgr
            .apply_incarnation_allocate(&store, token(3, 1), 8, u64::MAX)
            .is_err());
        assert!(mgr
            .apply_incarnation_allocate(&store, token(3, 1), 8, MAX_ISSUABLE_INCARNATION + 1)
            .is_err());
    }

    #[test]
    fn test_allocate_commit_remove_lifecycle() {
        let store = new_store("key-lifecycle");
        let mgr = CacheManager::new();
        mgr.apply_id_reserve(&store, token(1, 1), OBJ, OBJ + 10)
            .unwrap();

        let alloc = reserved(1, OBJ);
        mgr.apply_allocate(&store, token(1, 2), 1, "/a/b", &alloc)
            .unwrap();
        assert_eq!(
            store.cache_get_entry(1, "/a/b").unwrap(),
            Some(alloc.clone())
        );
        assert_eq!(
            store.cache_get_outcome(token(1, 2)).unwrap(),
            Some(OpOutcome::Allocated {
                incarnation: 1,
                key: "/a/b".into(),
                generation: 1,
                object_id: OBJ
            })
        );

        // Idempotent allocate replay.
        mgr.apply_allocate(&store, token(1, 2), 1, "/a/b", &alloc)
            .unwrap();

        // Commit: Reserved@1 -> Valid with len/ufs_mtime; TTL row appears.
        mgr.apply_commit(&store, 1, "/a/b", 1, OBJ, 300, 12345, 5000)
            .unwrap();
        let committed = store.cache_get_entry(1, "/a/b").unwrap().unwrap();
        assert_eq!(committed.state, CacheEntryState::Valid);
        assert_eq!(
            (committed.len, committed.ufs_mtime, committed.expire_at),
            (300, 12345, 5000)
        );
        assert_eq!(committed.generation, 1);
        let due = store.cache_scan_expiry(5000, 10).unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].key, "/a/b");

        // Commit replay is idempotent (exact match).
        mgr.apply_commit(&store, 1, "/a/b", 1, OBJ, 300, 12345, 5000)
            .unwrap();
        // A different commit at the same generation is a divergence.
        assert!(mgr
            .apply_commit(&store, 1, "/a/b", 1, OBJ, 999, 12345, 5000)
            .is_err());

        // Remove: Valid@1 -> Tombstoned@2, expiry and reverse rows dropped.
        mgr.apply_remove(&store, 1, "/a/b", 1, 2, OBJ).unwrap();
        let removed = store.cache_get_entry(1, "/a/b").unwrap().unwrap();
        assert_eq!(removed.state, CacheEntryState::Tombstoned);
        assert_eq!(removed.generation, 2);
        assert!(store.cache_scan_expiry(100000, 10).unwrap().is_empty());
        assert!(store.cache_get_object(OBJ).unwrap().is_none());

        // Late commit against the superseded generation: terminal no-op.
        mgr.apply_commit(&store, 1, "/a/b", 1, OBJ, 300, 12345, 5000)
            .unwrap();
        assert_eq!(
            store.cache_get_entry(1, "/a/b").unwrap().unwrap().state,
            CacheEntryState::Tombstoned
        );

        // Remove replay idempotent; non-adjacent generations rejected.
        mgr.apply_remove(&store, 1, "/a/b", 1, 2, OBJ).unwrap();
        assert!(mgr.apply_remove(&store, 1, "/a/b", 2, 4, OBJ).is_err());

        // A later allocate for the same key advances the generation.
        let alloc3 = reserved(3, OBJ + 1);
        mgr.apply_allocate(&store, token(1, 3), 1, "/a/b", &alloc3)
            .unwrap();
        assert_eq!(
            store
                .cache_get_entry(1, "/a/b")
                .unwrap()
                .unwrap()
                .generation,
            3
        );
    }

    #[test]
    fn test_replay_over_final_state_converges() {
        // Simulates a restart that replays the whole log over RocksDB that
        // already holds the final state: every entry must converge.
        let store = new_store("replay-final");
        let mgr = CacheManager::new();
        mgr.apply_id_reserve(&store, token(1, 1), OBJ, OBJ + 10)
            .unwrap();
        mgr.apply_incarnation_allocate(&store, token(2, 1), 5, 1)
            .unwrap();
        let alloc = reserved(1, OBJ);
        mgr.apply_allocate(&store, token(1, 2), 1, "/k", &alloc)
            .unwrap();
        mgr.apply_commit(&store, 1, "/k", 1, OBJ, 100, 777, 0)
            .unwrap();

        let replay = CacheManager::new();
        replay.restore_watermarks(&store).unwrap();
        replay
            .apply_id_reserve(&store, token(1, 1), OBJ, OBJ + 10)
            .unwrap();
        replay
            .apply_incarnation_allocate(&store, token(2, 1), 5, 1)
            .unwrap();
        replay
            .apply_allocate(&store, token(1, 2), 1, "/k", &alloc)
            .unwrap();
        replay
            .apply_commit(&store, 1, "/k", 1, OBJ, 100, 777, 0)
            .unwrap();
        assert_eq!(replay.current_object_id(), OBJ + 9);
        assert_eq!(replay.current_incarnation(), 1);

        // Commit without a prior entry fails loudly.
        assert!(replay
            .apply_commit(&store, 1, "/missing", 1, OBJ, 1, 1, 0)
            .is_err());
        // Allocate with a non-Reserved state fails loudly.
        let mut bad = reserved(9, OBJ + 5);
        bad.state = CacheEntryState::Valid;
        bad.ufs_mtime = 5;
        assert!(replay
            .apply_allocate(&store, token(9, 9), 1, "/bad", &bad)
            .is_err());
    }

    #[test]
    fn test_identity_token_gate() {
        let store = new_store("token-gate");
        let mgr = CacheManager::new();

        // Same token, different parameters: divergence, never executes twice.
        mgr.apply_id_reserve(&store, token(1, 1), OBJ, OBJ + 10)
            .unwrap();
        assert!(mgr
            .apply_id_reserve(&store, token(1, 1), OBJ + 100, OBJ + 110)
            .is_err());
        // The divergent re-run wrote nothing.
        assert_eq!(
            store.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
            Some(OBJ + 9)
        );

        // A late token at or below the client high-watermark with no
        // recorded outcome is Expired: terminal, never re-allocates.
        mgr.apply_id_reserve(&store, token(1, 10), OBJ + 10, OBJ + 20)
            .unwrap();
        assert_eq!(
            store.cache_client_watermark(1).unwrap(),
            Some(10),
            "watermark must advance to the highest applied op_seq"
        );
        mgr.apply_id_reserve(&store, token(1, 9), OBJ + 20, OBJ + 30)
            .unwrap();
        assert_eq!(
            store.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
            Some(OBJ + 19),
            "expired token must not mutate durable state"
        );
        assert!(store.cache_get_outcome(token(1, 9)).unwrap().is_none());

        // Replay after the outcome window evicted the record (simulated by
        // a direct outcome delete, the same thing eviction does): the
        // identity is unrecoverable and must not be re-created.
        let alloc = reserved(1, OBJ);
        mgr.apply_allocate(&store, token(2, 1), 1, "/k", &alloc)
            .unwrap();

        // Allocate divergence on the same token is loud (outcome present,
        // different parameters).
        let alloc_diff = reserved(1, OBJ + 1);
        assert!(mgr
            .apply_allocate(&store, token(2, 1), 1, "/k", &alloc_diff)
            .is_err());

        let mut w = store.cache_write();
        w.delete_outcome(token(2, 1)).unwrap();
        w.commit().unwrap();
        // Client 2 watermark is 1, so the same token is now Expired, not
        // AlreadyApplied: terminal no-op, entry row untouched.
        mgr.apply_allocate(&store, token(2, 1), 1, "/k", &alloc)
            .unwrap();
        assert_eq!(
            store.cache_get_entry(1, "/k").unwrap(),
            Some(alloc),
            "expired replay must not rewrite the entry row"
        );
        assert!(store.cache_get_outcome(token(2, 1)).unwrap().is_none());

        // Incarnation allocate divergence on the same token is loud.
        mgr.apply_incarnation_allocate(&store, token(3, 1), 7, 1)
            .unwrap();
        assert!(mgr
            .apply_incarnation_allocate(&store, token(3, 1), 7, 2)
            .is_err());
        assert_eq!(store.cache_current_incarnation(7).unwrap(), Some(1));
    }

    /// Cross-token aliasing (1c review blocker 1): Execute only accepts
    /// genuinely-new absolute transitions. A *different* token may never
    /// alias an existing identity — identical parameters included — and a
    /// rejected alias must leave no outcome row.
    #[test]
    fn test_cross_token_alias_rejected() {
        let store = new_store("cross-token-alias");
        let mgr = CacheManager::new();

        // --- id reserve: segment must be contiguous with the durable HW.
        mgr.apply_id_reserve(&store, token(5, 1), OBJ, OBJ + 10)
            .unwrap();
        // Same segment under a different token: not Execute's expected
        // absolute transition (start != HW + 1) -> error, no outcome.
        assert!(mgr
            .apply_id_reserve(&store, token(6, 1), OBJ, OBJ + 10)
            .is_err());
        // Overlapping and regressing segments are equally rejected.
        assert!(mgr
            .apply_id_reserve(&store, token(6, 2), OBJ + 5, OBJ + 15)
            .is_err());
        // A gap ahead of the watermark is not contiguous either.
        assert!(mgr
            .apply_id_reserve(&store, token(6, 3), OBJ + 100, OBJ + 110)
            .is_err());
        assert!(store.cache_get_outcome(token(6, 1)).unwrap().is_none());
        assert!(store.cache_get_outcome(token(6, 2)).unwrap().is_none());
        assert!(store.cache_get_outcome(token(6, 3)).unwrap().is_none());
        assert_eq!(
            store.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
            Some(OBJ + 9)
        );

        // --- incarnation allocate: strictly above HW and row absent.
        mgr.apply_incarnation_allocate(&store, token(7, 1), 5, 3)
            .unwrap();
        // Existing incarnation under a different token -> error.
        assert!(mgr
            .apply_incarnation_allocate(&store, token(8, 1), 5, 3)
            .is_err());
        // Below (or at) the durable incarnation watermark, row absent ->
        // still not a new identity -> error.
        assert!(mgr
            .apply_incarnation_allocate(&store, token(8, 2), 5, 2)
            .is_err());
        assert!(store.cache_get_outcome(token(8, 1)).unwrap().is_none());
        assert!(store.cache_get_outcome(token(8, 2)).unwrap().is_none());
        assert_eq!(store.cache_current_incarnation(5).unwrap(), Some(3));

        // --- allocate: only None -> Reserved@1 or Tombstoned@g -> g+1.
        let alloc = reserved(1, OBJ);
        mgr.apply_allocate(&store, token(9, 1), 1, "/k", &alloc)
            .unwrap();
        // Identical parameters under a different token over a live
        // Reserved row -> alias error, entry untouched, no outcome.
        assert!(mgr
            .apply_allocate(&store, token(9, 2), 1, "/k", &alloc)
            .is_err());
        let alloc2 = reserved(2, OBJ + 1);
        assert!(mgr
            .apply_allocate(&store, token(9, 3), 1, "/k", &alloc2)
            .is_err());
        assert!(store.cache_get_outcome(token(9, 2)).unwrap().is_none());
        assert!(store.cache_get_outcome(token(9, 3)).unwrap().is_none());
        assert_eq!(store.cache_get_entry(1, "/k").unwrap(), Some(alloc));

        // --- object id fencing (round-2 review): an allocate may only
        // consume an id inside the durable reserved segment, may never
        // collide with a live reverse row owned by another key, and a
        // re-opened key must consume a strictly greater id than the
        // generation it replaces.
        // Distinct key, same object id: the live reverse row for /k@OBJ
        // belongs to another identity -> error, no outcome.
        let alloc_alias = reserved(1, OBJ);
        assert!(mgr
            .apply_allocate(&store, token(10, 1), 1, "/other", &alloc_alias)
            .is_err());
        assert!(store.cache_get_outcome(token(10, 1)).unwrap().is_none());
        assert!(store.cache_get_entry(1, "/other").unwrap().is_none());
        // Unreserved id ahead of the durable HW (OBJ+9) -> error.
        let alloc_unreserved = reserved(1, OBJ + 500);
        assert!(mgr
            .apply_allocate(&store, token(10, 2), 1, "/other", &alloc_unreserved)
            .is_err());
        assert!(store.cache_get_outcome(token(10, 2)).unwrap().is_none());
        assert!(store.cache_get_entry(1, "/other").unwrap().is_none());
        // Same key re-open: remove /k@1 (object OBJ) tombstones at gen 2,
        // then a gen-3 allocate trying to reuse OBJ is a per-key id
        // regression -> error.
        mgr.apply_remove(&store, 1, "/k", 1, 2, OBJ).unwrap();
        let alloc_reuse = reserved(3, OBJ);
        assert!(mgr
            .apply_allocate(&store, token(10, 3), 1, "/k", &alloc_reuse)
            .is_err());
        assert!(store.cache_get_outcome(token(10, 3)).unwrap().is_none());
        // /k stays at the tombstone; a monotonic re-open still works.
        assert_eq!(
            store.cache_get_entry(1, "/k").unwrap().unwrap().state,
            CacheEntryState::Tombstoned
        );
        let alloc_reopen = reserved(3, OBJ + 1);
        mgr.apply_allocate(&store, token(10, 4), 1, "/k", &alloc_reopen)
            .unwrap();
        assert_eq!(store.cache_get_entry(1, "/k").unwrap(), Some(alloc_reopen));
    }

    /// Expired is a strict no-op (1c review blocker 2): an untrusted late
    /// entry's parameters may never move the durable state or the volatile
    /// allocator — not even "harmlessly large" ones.
    #[test]
    fn test_expired_noop_ignores_entry_parameters() {
        let store = new_store("expired-noop");
        let mgr = CacheManager::new();

        // Reserve: client 1's watermark is already at op 5; a late op 1
        // claiming a huge segment is Expired -> nothing moves.
        mgr.apply_id_reserve(&store, token(1, 5), OBJ, OBJ + 100)
            .unwrap();
        let durable = store.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap();
        assert_eq!(durable, Some(OBJ + 99));
        mgr.apply_id_reserve(
            &store,
            token(1, 1),
            OBJ + 100,
            BlockIdCodec::CACHE_OBJECT_MAX,
        )
        .unwrap();
        assert_eq!(
            store.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
            durable,
            "expired reserve must not move durable state"
        );
        assert_eq!(mgr.current_object_id(), OBJ + 99);
        assert!(store.cache_get_outcome(token(1, 1)).unwrap().is_none());

        // Incarnation: late op claiming a huge incarnation is Expired.
        mgr.apply_incarnation_allocate(&store, token(2, 5), 6, 1)
            .unwrap();
        mgr.apply_incarnation_allocate(&store, token(2, 1), 6, MAX_ISSUABLE_INCARNATION)
            .unwrap();
        assert_eq!(mgr.current_incarnation(), 1);
        assert!(store
            .cache_get_incarnation(MAX_ISSUABLE_INCARNATION)
            .unwrap()
            .is_none());
        assert!(store.cache_get_outcome(token(2, 1)).unwrap().is_none());

        // Allocate: a live entry first (fresh token, ordinary object id
        // inside the reserved segment), then a late op claiming the top of
        // the object domain.
        let alloc = reserved(1, OBJ + 50);
        mgr.apply_allocate(&store, token(3, 1), 1, "/k", &alloc)
            .unwrap();
        let alloc_huge = reserved(1, BlockIdCodec::CACHE_OBJECT_MAX - 1);
        assert!(mgr
            .apply_allocate(&store, token(3, 1), 1, "/other", &alloc_huge)
            .is_err());
        // Client 3 watermark is 1, so a fresh lower op_seq is Expired only
        // after the outcome is gone; with the outcome present it is a
        // divergence instead. Use client 4 with an established watermark.
        mgr.apply_id_reserve(&store, token(4, 5), OBJ + 100, OBJ + 110)
            .unwrap();
        let volatile_before = mgr.current_object_id();
        mgr.apply_allocate(&store, token(4, 1), 1, "/late", &alloc_huge)
            .unwrap();
        assert!(
            store.cache_get_entry(1, "/late").unwrap().is_none(),
            "expired allocate must not write the entry row"
        );
        assert!(store.cache_get_outcome(token(4, 1)).unwrap().is_none());
        assert_eq!(
            mgr.current_object_id(),
            volatile_before,
            "expired allocate must not move the volatile allocator"
        );
    }

    /// Generation overflow (1c review blocker 4): Tombstoned@u64::MAX is
    /// terminal — the next generation does not exist, so allocate must fail
    /// closed instead of wrapping.
    #[test]
    fn test_allocate_generation_overflow_terminal() {
        let store = new_store("gen-overflow");
        let mgr = CacheManager::new();

        // Write the terminal tombstone directly (the store boundary allows
        // any generation >= 1; no journal path can produce it except a
        // remove at u64::MAX, but replay must still be safe).
        let terminal = CacheEntry {
            generation: u64::MAX,
            state: CacheEntryState::Tombstoned,
            object_id: OBJ,
            len: 0,
            ufs_mtime: 1,
            block_size: 64,
            expire_at: 0,
        };
        let mut w = store.cache_write();
        w.put_entry(1, "/k", &terminal).unwrap();
        w.commit().unwrap();

        let alloc = reserved(u64::MAX, OBJ + 1);
        assert!(mgr
            .apply_allocate(&store, token(1, 1), 1, "/k", &alloc)
            .is_err());
        // The rejected allocate wrote nothing.
        assert_eq!(
            store.cache_get_entry(1, "/k").unwrap().unwrap().state,
            CacheEntryState::Tombstoned
        );
        assert_eq!(
            store.cache_get_entry(1, "/k").unwrap().unwrap().generation,
            u64::MAX
        );
        assert!(store.cache_get_outcome(token(1, 1)).unwrap().is_none());
    }

    #[test]
    fn test_commit_and_allocate_cas_matrix() {
        let store = new_store("cas-matrix");
        let mgr = CacheManager::new();

        // Fencing (round-2 review): every allocate must consume an id at or
        // below the durable reserve watermark, so establish one first.
        mgr.apply_id_reserve(&store, token(9, 1), OBJ, OBJ + 100)
            .unwrap();

        let alloc = reserved(1, OBJ);
        mgr.apply_allocate(&store, token(1, 1), 1, "/k", &alloc)
            .unwrap();

        // Commit may never skip generations: Reserved@1 + commit@2 errors.
        assert!(mgr
            .apply_commit(&store, 1, "/k", 2, OBJ, 100, 777, 0)
            .is_err());

        // Exact Reserved@g -> Valid works, and exact replay is idempotent.
        mgr.apply_commit(&store, 1, "/k", 1, OBJ, 100, 777, 0)
            .unwrap();
        mgr.apply_commit(&store, 1, "/k", 1, OBJ, 100, 777, 0)
            .unwrap();

        // A different payload at the committed generation is a divergence.
        assert!(mgr
            .apply_commit(&store, 1, "/k", 1, OBJ, 200, 777, 0)
            .is_err());

        // Allocate may not overwrite a Valid/Reserved row at a later
        // generation: only Tombstoned@g -> Reserved@g+1 re-opens the key.
        let alloc2 = reserved(2, OBJ + 1);
        assert!(mgr
            .apply_allocate(&store, token(1, 2), 1, "/k", &alloc2)
            .is_err());
        // Cross-generation jumps are forbidden even from a tombstone.
        mgr.apply_remove(&store, 1, "/k", 1, 2, OBJ).unwrap();
        let alloc4 = reserved(4, OBJ + 2);
        assert!(mgr
            .apply_allocate(&store, token(1, 3), 1, "/k", &alloc4)
            .is_err());
        // Adjacent tombstone re-open is the only legal re-allocation.
        let alloc3 = reserved(3, OBJ + 1);
        mgr.apply_allocate(&store, token(1, 3), 1, "/k", &alloc3)
            .unwrap();
        assert_eq!(
            store.cache_get_entry(1, "/k").unwrap().unwrap().generation,
            3
        );

        // A first allocation for a fresh key must start at generation 1.
        let alloc9 = reserved(9, OBJ + 5);
        assert!(mgr
            .apply_allocate(&store, token(1, 4), 1, "/fresh", &alloc9)
            .is_err());
    }

    /// Segment-reserve crash matrix (contract §7): a client crash at any
    /// point around the reserve must observe exactly one segment identity.
    /// Crash points A (before any durable write), B (after committed apply,
    /// before ACK), and C (after ACK) are indistinguishable at the store:
    /// the same token either re-reads its persisted outcome (B/C retry) or
    /// executes for the first time (A). A retry never re-allocates, and
    /// after the outcome window evicts the record the segment identity is
    /// unrecoverable — terminal, never re-executed.
    #[test]
    fn test_segment_reserve_crash_points_single_identity() {
        let store = new_store("reserve-crash");
        let mgr = CacheManager::new();

        // Points B/C: committed apply survived the crash; the retry (same
        // token, same parameters) re-reads the exact outcome.
        mgr.apply_id_reserve(&store, token(9, 5), OBJ, OBJ + 100)
            .unwrap();
        let identity = store.cache_get_outcome(token(9, 5)).unwrap();
        assert_eq!(
            identity,
            Some(OpOutcome::Reserved {
                start: OBJ,
                end: OBJ + 100
            })
        );
        let watermark = store.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap();

        for _ in 0..3 {
            mgr.apply_id_reserve(&store, token(9, 5), OBJ, OBJ + 100)
                .unwrap();
        }
        assert_eq!(store.cache_get_outcome(token(9, 5)).unwrap(), identity);
        assert_eq!(
            store.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
            watermark,
            "retries must not move the watermark"
        );

        // The next distinct token allocates strictly after the segment.
        mgr.apply_id_reserve(&store, token(9, 6), OBJ + 100, OBJ + 200)
            .unwrap();
        assert_eq!(
            store.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
            Some(OBJ + 199)
        );
        // The old token still re-reads its identity even now.
        mgr.apply_id_reserve(&store, token(9, 5), OBJ, OBJ + 100)
            .unwrap();
        assert_eq!(store.cache_get_outcome(token(9, 5)).unwrap(), identity);

        // Outcome-window eviction (bounded window GC'd below the client
        // high-watermark): the identity is gone forever — Expired, never
        // re-executed, watermark untouched.
        let mut w = store.cache_write();
        w.delete_outcome(token(9, 5)).unwrap();
        w.commit().unwrap();
        mgr.apply_id_reserve(&store, token(9, 5), OBJ, OBJ + 100)
            .unwrap();
        assert!(store.cache_get_outcome(token(9, 5)).unwrap().is_none());
        assert_eq!(
            store.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
            Some(OBJ + 199),
            "expired token must never re-execute or move durable state"
        );
    }

    /// Replay determinism (contract §7): applying the same committed command
    /// sequence twice over the same store converges to byte-identical
    /// durable state — every token has exactly one outcome and no command
    /// re-executes.
    #[test]
    fn test_replay_determinism_single_outcome() {
        let store = new_store("replay-determinism");
        let mgr = CacheManager::new();

        let run = |mgr: &CacheManager| {
            mgr.apply_id_reserve(&store, token(1, 1), OBJ, OBJ + 10)
                .unwrap();
            mgr.apply_incarnation_allocate(&store, token(2, 1), 5, 1)
                .unwrap();
            let alloc = reserved(1, OBJ);
            mgr.apply_allocate(&store, token(1, 2), 1, "/a", &alloc)
                .unwrap();
            let alloc_b = reserved(1, OBJ + 1);
            mgr.apply_allocate(&store, token(1, 3), 1, "/b", &alloc_b)
                .unwrap();
            mgr.apply_commit(&store, 1, "/a", 1, OBJ, 300, 111, 9000)
                .unwrap();
            mgr.apply_remove(&store, 1, "/b", 1, 2, OBJ + 1).unwrap();
        };

        run(&mgr);
        let dump = |store: &RocksInodeStore| -> Vec<(String, CacheEntry)> {
            store.cache_scan_entries(1, None, 100).unwrap()
        };
        let first_entries = dump(&store);
        let first_outcomes = vec![
            store.cache_get_outcome(token(1, 1)).unwrap(),
            store.cache_get_outcome(token(1, 2)).unwrap(),
            store.cache_get_outcome(token(1, 3)).unwrap(),
            store.cache_get_outcome(token(2, 1)).unwrap(),
        ];
        let first_state = (
            store.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
            store
                .cache_get_state(state_tags::CACHE_INCARNATION)
                .unwrap(),
        );

        // Full replay over already-populated state: converges, no error,
        // byte-identical durable state.
        run(&mgr);

        assert_eq!(dump(&store), first_entries);
        assert_eq!(
            vec![
                store.cache_get_outcome(token(1, 1)).unwrap(),
                store.cache_get_outcome(token(1, 2)).unwrap(),
                store.cache_get_outcome(token(1, 3)).unwrap(),
                store.cache_get_outcome(token(2, 1)).unwrap(),
            ],
            first_outcomes,
            "each token keeps exactly one outcome across replay"
        );
        assert_eq!(
            (
                store.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
                store
                    .cache_get_state(state_tags::CACHE_INCARNATION)
                    .unwrap(),
            ),
            first_state
        );
    }
}
