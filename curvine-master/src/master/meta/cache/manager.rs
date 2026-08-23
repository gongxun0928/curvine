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
    key_in_scope, validate_expiry_row, validate_incarnation, CacheEntry, CacheEntryState,
    ExpiryRow, IncarnationPolicyRow, IncarnationRow, ObjectRow, OpOutcome, OpToken, OutcomeGcGroup,
    ScopeRemoveVictim, VacuumVictim, MAX_CACHE_KEY_BYTES,
};
use crate::master::meta::cache::store::{
    state_tags, validate_page_len, CacheWrite, LocalCacheIndexStore,
};
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

    // NOTE: the manager deliberately exposes NO in-segment issuance. The
    // durable reserve watermark (applied by `apply_id_reserve`) is the
    // only issuance-relevant state here; the leader service owns the
    // volatile `{next, end, epoch}` segment cursor and burns its tail on
    // leadership loss, restart, or a watermark moved by another leader.

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

    /// Identity-producing (4a legacy): allocate a never-reused mount
    /// incarnation WITHOUT a policy snapshot. Only reachable by replaying a
    /// legacy `CacheIncarnationAllocate` journal entry written before 4b;
    /// the 4b issuer writes [`Self::apply_incarnation_allocate_v2`]. The
    /// recorded outcome is the legacy shape (incarnation only): a new V2
    /// request carrying mount/ttl parameters can never match it exactly.
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
        // Legacy outcome shape: incarnation only. No policy row is written;
        // readers treat a missing policy row as `ttl_ms == 0`.
        w.put_outcome(token, &OpOutcome::IncarnationAllocated { incarnation })
            .map_err(cv)?;
        w.set_client_watermark(token.client_id, token.op_seq)
            .map_err(cv)?;
        w.set_state(state_tags::CACHE_INCARNATION, incarnation as i64)
            .map_err(cv)?;
        w.commit().map_err(cv)?;
        Ok(())
    }

    /// Identity-producing (4b): allocate a never-reused mount incarnation
    /// with the frozen policy snapshot. The TTL is persisted in a separate
    /// policy row (option A) so the 4a `IncarnationRow` bytes stay
    /// decodable; the recorded outcome is the V2 variant binding the full
    /// request (mount + ttl + incarnation) so a replayed token with
    /// different parameters is divergence, never AlreadyApplied.
    pub fn apply_incarnation_allocate_v2<S: LocalCacheIndexStore>(
        &self,
        store: &S,
        token: OpToken,
        mount_id: u32,
        incarnation: u64,
        ttl_ms: i64,
    ) -> CommonResult<()> {
        Self::check_token(token)?;
        if incarnation == 0 || incarnation > MAX_ISSUABLE_INCARNATION {
            return err_box!(
                "incarnation outside issuable range [1, {}]: {}",
                MAX_ISSUABLE_INCARNATION,
                incarnation
            );
        }
        if ttl_ms < 0 {
            return err_box!("mount ttl_ms must be non-negative: {}", ttl_ms);
        }

        let gate = Self::classify_token(
            store,
            token,
            &OpOutcome::IncarnationAllocatedV2 {
                incarnation,
                mount_id,
                ttl_ms,
            },
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
        // Legacy-shape row (frozen at 4a) + separate 4b policy row.
        w.put_incarnation(
            incarnation,
            IncarnationRow {
                mount_id,
                revoked: false,
            },
        )
        .map_err(cv)?;
        w.put_incarnation_policy(incarnation, IncarnationPolicyRow { ttl_ms })
            .map_err(cv)?;
        if write_pointer {
            w.set_current_incarnation(mount_id, incarnation)
                .map_err(cv)?;
        }
        w.put_outcome(
            token,
            &OpOutcome::IncarnationAllocatedV2 {
                incarnation,
                mount_id,
                ttl_ms,
            },
        )
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
        // The policy row (if any) is never touched: it is durable history,
        // not live configuration.
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

    /// Apply-time incarnation fence (4b contract): a cache write may only
    /// execute inside a live, current incarnation. `Ok(true)` = active
    /// (row present, not revoked, mount pointer still names it);
    /// `Ok(false)` = fenced → the caller performs a deterministic no-op and
    /// the service's barrier readback reports terminal revoked/stale. Only
    /// store failures return `Err`.
    fn incarnation_active<S: LocalCacheIndexStore>(
        store: &S,
        incarnation: u64,
    ) -> CommonResult<bool> {
        match store.cache_get_incarnation(incarnation).map_err(cv)? {
            Some(row) if !row.revoked => {
                Ok(store.cache_current_incarnation(row.mount_id).map_err(cv)? == Some(incarnation))
            }
            _ => Ok(false),
        }
    }

    /// Identity-producing: per-key load allocation. Writes the `Reserved`
    /// entry, its reverse row, and the outcome binding the object id plus
    /// the exact request geometry (file_len/block_size).
    #[allow(clippy::too_many_arguments)]
    pub fn apply_allocate<S: LocalCacheIndexStore>(
        &self,
        store: &S,
        token: OpToken,
        incarnation: u64,
        key: &str,
        file_len: i64,
        entry: &CacheEntry,
    ) -> CommonResult<()> {
        Self::check_token(token)?;
        validate_incarnation(incarnation)?;
        if file_len < 0 {
            return err_box!("cache allocate file length must be >= 0: {}", file_len);
        }
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
                file_len,
                block_size: entry.block_size,
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

        // Apply-time incarnation fence (4b): a revoke or remount that
        // interleaved between the service precheck and this apply fences
        // the write deterministically; the service's barrier readback
        // reports terminal revoked/stale.
        if !Self::incarnation_active(store, incarnation)? {
            return Ok(());
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
                file_len,
                block_size: entry.block_size,
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
    /// `expire_at > 0`) is part of the same atomic batch. `load_token`
    /// must carry the recorded `Allocated` outcome of the load this commit
    /// belongs to; `token` is the commit's own durable idempotency token.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_commit<S: LocalCacheIndexStore>(
        &self,
        store: &S,
        load_token: OpToken,
        token: OpToken,
        incarnation: u64,
        key: &str,
        generation: u64,
        expected_object_id: i64,
        len: i64,
        ufs_mtime: i64,
        expire_at: i64,
    ) -> CommonResult<()> {
        validate_incarnation(incarnation)?;
        Self::check_token(token)?;

        // Abort first-winner (task #5 gate 2, gpt56 `21bb7129` +
        // `52db24f3`): this load's commit token was consumed by a
        // durable abort of THE SAME load — the commit lost the race and
        // is a terminal no-op (the row is Tombstoned by the abort;
        // nothing may publish). Field-exact: an Aborted record of a
        // DIFFERENT request under this token is divergence, not a
        // silent pass (gpt56 `52db24f3`). Checked BEFORE the load
        // binding so the abort outcome is authoritative history for
        // this token.
        if let Some(OpOutcome::Aborted {
            incarnation: a_inc,
            key: a_key,
            generation: a_gen,
            object_id: a_obj,
            load_token: a_load,
        }) = store.cache_get_outcome(token).map_err(cv)?
        {
            if a_inc == incarnation
                && a_key == key
                && a_gen == generation
                && a_obj == expected_object_id
                && a_load == load_token
            {
                return Ok(());
            }
            return err_box!(
                "cache commit token {:?} recorded an Aborted outcome of a different load: divergence (recorded ({}, {})@{} object {} load {:?}, commit says ({}, {})@{} object {} load {:?})",
                token,
                a_inc,
                a_key,
                a_gen,
                a_obj,
                a_load,
                incarnation,
                key,
                generation,
                expected_object_id,
                load_token
            );
        }

        // Load binding first: this commit may only land on the object its
        // allocate reserved (identity AND geometry), recorded under the
        // load token.
        match store.cache_get_outcome(load_token).map_err(cv)? {
            Some(OpOutcome::Allocated {
                incarnation: inc,
                key: out_key,
                generation: out_gen,
                object_id: out_obj,
                file_len: out_len,
                ..
            }) => {
                if inc != incarnation
                    || out_key != key
                    || out_gen != generation
                    || out_obj != expected_object_id
                    || out_len != len
                {
                    return err_box!(
                        "cache commit does not match its load allocation: load token {:?} recorded ({}, {})@{} object {} len {}, commit says ({}, {})@{} object {} len {}",
                        load_token,
                        inc,
                        out_key,
                        out_gen,
                        out_obj,
                        out_len,
                        incarnation,
                        key,
                        generation,
                        expected_object_id,
                        len
                    );
                }
            }
            other => {
                return err_box!(
                    "cache commit load token {:?} has no recorded allocation: {:?}",
                    load_token,
                    other
                )
            }
        }

        // Commit-token gate: an exact recorded history wins over the entry
        // row (whose state this commit itself may have advanced), so a
        // lost-response retry resolves to its recorded result. The
        // comparison covers the FULL immutable request: any parameter
        // difference is divergence.
        let gate = Self::classify_token(
            store,
            token,
            &OpOutcome::Committed {
                incarnation,
                key: key.to_string(),
                generation,
                object_id: expected_object_id,
                load_token,
                len,
                ufs_mtime,
                expire_at,
            },
        )?;
        match gate {
            // Exact recorded history: strict no-op.
            TokenGate::AlreadyApplied => return Ok(()),
            // Terminal, strict no-op: an expired token's parameters are
            // NOT trusted history.
            TokenGate::Expired => return Ok(()),
            TokenGate::Execute => (),
        }

        // Apply-time incarnation fence (4b): a revoke/remount interleaved
        // between the service precheck and this apply fences the commit
        // deterministically (the load stays Reserved and dies with its
        // namespace).
        if !Self::incarnation_active(store, incarnation)? {
            return Ok(());
        }

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
        w.put_outcome(
            token,
            &OpOutcome::Committed {
                incarnation,
                key: key.to_string(),
                generation,
                object_id: cur.object_id,
                load_token,
                len,
                ufs_mtime,
                expire_at,
            },
        )
        .map_err(cv)?;
        w.set_client_watermark(token.client_id, token.op_seq)
            .map_err(cv)?;
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

        // Apply-time incarnation fence (4b): a remove for a fenced
        // namespace is a deterministic no-op (the row dies with its
        // namespace; there is nothing left to tombstone for a client).
        if !Self::incarnation_active(store, incarnation)? {
            return Ok(());
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
            // Exact-identity CAS delete (4c.1): the frozen position is
            // (expire_at, incarnation, object_id) and the row at it was
            // written by the commit that made this version Valid, so the
            // committed (key, generation) must match exactly; any
            // mismatch is divergence and fails the apply.
            w.delete_expiry(
                cur.expire_at,
                incarnation,
                cur.object_id,
                key,
                cur.generation,
            )
            .map_err(cv)?;
        }
        // The reverse row is only a GC hint for the superseded version.
        w.delete_object(cur.object_id).map_err(cv)?;
        w.commit().map_err(cv)?;
        Ok(())
    }

    /// Durable load abort (task #5 gate 2, gpt56 `21bb7129`):
    /// `Reserved@expected -> Tombstoned@new` for a load that failed
    /// BEFORE its commit was issued, releasing the key for later
    /// allocates. First-winner classification runs on the load's COMMIT
    /// token — shared with `apply_commit`. Commit applied first
    /// (recorded `Committed`) is a loud refusal — a Valid row is NEVER
    /// removed by an abort; abort applied first (recorded `Aborted`)
    /// makes a later commit of the same token a terminal no-op (see
    /// `apply_commit`). The row CAS accepts ONLY an exact
    /// `Reserved@expected` row with the load's object identity; a Valid
    /// row at the fence is fail-closed.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_abort<S: LocalCacheIndexStore>(
        &self,
        store: &S,
        load_token: OpToken,
        commit_token: OpToken,
        incarnation: u64,
        key: &str,
        expected_generation: u64,
        new_generation: u64,
        expected_object_id: i64,
    ) -> CommonResult<()> {
        validate_incarnation(incarnation)?;
        Self::check_token(load_token)?;
        Self::check_token(commit_token)?;
        if Some(new_generation) != expected_generation.checked_add(1) {
            return err_box!(
                "cache abort generations not adjacent: expected {}, new {}",
                expected_generation,
                new_generation
            );
        }

        // First-winner classification on the SHARED commit token.
        let aborted = OpOutcome::Aborted {
            incarnation,
            key: key.to_string(),
            generation: expected_generation,
            object_id: expected_object_id,
            load_token,
        };
        match store.cache_get_outcome(commit_token).map_err(cv)? {
            Some(recorded) if recorded == aborted => {
                return Ok(()); // exact abort replay: strict no-op
            }
            // Any OTHER recorded outcome (a Committed, or an Aborted of a
            // different request) means this abort LOST the first-winner
            // race for the shared commit token. A deterministic no-op,
            // never an Err: the journal loader treats any cache apply
            // error as FATAL (gpt56 `52db24f3` blocker 1), and a
            // prechecked race loser must not poison the state machine.
            // Loud classification is the handler's post-barrier
            // readback's job.
            Some(_recorded) => return Ok(()),
            None => {
                match store
                    .cache_client_watermark(commit_token.client_id)
                    .map_err(cv)?
                {
                    // Expired: another op already advanced the window and
                    // the parameters are not trusted history. Terminal
                    // no-op (mirrors the commit gate).
                    Some(hw) if commit_token.op_seq <= hw => return Ok(()),
                    _ => (),
                }
            }
        }

        // Load binding: the abort may only release its own allocation
        // (identity AND geometry).
        match store.cache_get_outcome(load_token).map_err(cv)? {
            Some(OpOutcome::Allocated {
                incarnation: out_inc,
                key: out_key,
                generation: out_gen,
                object_id: out_obj,
                ..
            }) => {
                if out_inc != incarnation
                    || out_key != key
                    || out_gen != expected_generation
                    || out_obj != expected_object_id
                {
                    return err_box!(
                        "cache abort does not match its load allocation: load token {:?} recorded ({}, {})@{} object {}, abort targets ({}, {})@{} object {}",
                        load_token,
                        out_inc,
                        out_key,
                        out_gen,
                        out_obj,
                        incarnation,
                        key,
                        expected_generation,
                        expected_object_id
                    );
                }
            }
            other => {
                return err_box!(
                    "cache abort load token {:?} has no recorded allocation: {:?}",
                    load_token,
                    other
                )
            }
        }

        // Apply-time incarnation fence (4b): deterministic no-op.
        if !Self::incarnation_active(store, incarnation)? {
            return Ok(());
        }

        let cur = match store.cache_get_entry(incarnation, key).map_err(cv)? {
            Some(v) => v,
            None => return err_box!("cache abort for missing entry ({}, {})", incarnation, key),
        };

        // Later state already advanced past this abort: converge.
        if cur.generation > new_generation {
            return Ok(());
        }
        if cur.generation == new_generation {
            if cur.state == CacheEntryState::Tombstoned {
                return Ok(()); // already applied (or an equivalent fence)
            }
            return err_box!(
                "cache abort replay divergence for ({}, {}): state {:?} at new generation {}",
                incarnation,
                key,
                cur.state,
                new_generation
            );
        }
        if cur.generation != expected_generation {
            return err_box!(
                "cache abort CAS violation for ({}, {}): committed generation {} vs expected {}",
                incarnation,
                key,
                cur.generation,
                expected_generation
            );
        }
        // Object identity CAS (contract §2.3).
        if cur.object_id != expected_object_id {
            return err_box!(
                "cache abort object identity mismatch for ({}, {})@{}: committed object {} vs expected {}",
                incarnation,
                key,
                cur.generation,
                cur.object_id,
                expected_object_id
            );
        }
        // STRICTLY Reserved: an abort never removes a Valid (committed)
        // row — that case is the apply-level first-winner fence.
        if cur.state != CacheEntryState::Reserved {
            return err_box!(
                "cache abort CAS violation for ({}, {})@{}: committed state {:?} (only Reserved rows are abortable)",
                incarnation,
                key,
                cur.generation,
                cur.state
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
        // A Reserved row carries no expiry row (entry invariant), so no
        // delete_expiry is needed; the reverse row is only a GC hint.
        w.delete_object(cur.object_id).map_err(cv)?;
        w.put_outcome(commit_token, &aborted).map_err(cv)?;
        w.set_client_watermark(commit_token.client_id, commit_token.op_seq)
            .map_err(cv)?;
        w.commit().map_err(cv)?;
        Ok(())
    }

    // ---- 4c.2 bounded mutation/journal apply paths. Every batch is
    // validated `1..=MUTATION_PAGE_CAP` at the boundary, mutates only the
    // journaled exact victim identities (the apply NEVER re-runs a range
    // scan), commits at most one bounded transaction per entry, and
    // resolves stale/missing victims as deterministic no-ops so journal
    // replay converges. ----

    /// Validate a scope-remove page: length bound, strictly ascending keys
    /// (a page of the ordered key scan), adjacent generations, and
    /// cache-domain object ids.
    fn validate_scope_victims(scope: &str, victims: &[ScopeRemoveVictim]) -> CommonResult<()> {
        validate_page_len(victims.len()).map_err(cv)?;
        if scope.len() > MAX_CACHE_KEY_BYTES {
            return err_box!(
                "cache scope remove scope exceeds {} bytes: {}",
                MAX_CACHE_KEY_BYTES,
                scope.len()
            );
        }
        for (i, v) in victims.iter().enumerate() {
            if v.key.len() > MAX_CACHE_KEY_BYTES {
                return err_box!(
                    "cache scope remove victim key exceeds {} bytes: {}",
                    MAX_CACHE_KEY_BYTES,
                    v.key.len()
                );
            }
            // Scope membership (review 303fb807 P0-3): a /a scope batch may
            // never name keys outside /a, whatever the journal claims.
            if !key_in_scope(&v.key, scope) {
                return err_box!(
                    "cache scope remove victim {} is outside scope {}",
                    v.key,
                    scope
                );
            }
            if v.expected_generation < 1 {
                return err_box!(
                    "scope remove victim generation must be >= 1: {}",
                    v.expected_generation
                );
            }
            if Some(v.new_generation) != v.expected_generation.checked_add(1) {
                return err_box!(
                    "scope remove victim generations not adjacent for {}: expected {}, new {}",
                    v.key,
                    v.expected_generation,
                    v.new_generation
                );
            }
            if !BlockIdCodec::is_cache_owner(v.object_id) {
                return err_box!(
                    "scope remove victim object id outside cache domain: {}",
                    v.object_id
                );
            }
            if i > 0 && victims[i - 1].key >= v.key {
                return err_box!(
                    "scope remove victims must be strictly ascending by key: {} !< {}",
                    victims[i - 1].key,
                    v.key
                );
            }
        }
        Ok(())
    }

    /// Conditional batch CAS (4c.2): prefix-scope remove. The victims were
    /// paged by the leader with the 4c.1 scoped scan; this apply only runs
    /// an exact CAS per victim against the authoritative entry — it never
    /// re-scans the scope. A fenced incarnation is a whole-batch
    /// deterministic no-op (the rows die with their namespace, reclaimed by
    /// vacuum).
    pub fn apply_scope_remove<S: LocalCacheIndexStore>(
        &self,
        store: &S,
        incarnation: u64,
        scope: &str,
        victims: &[ScopeRemoveVictim],
    ) -> CommonResult<()> {
        validate_incarnation(incarnation)?;
        if scope.is_empty() {
            return err_box!("cache scope remove scope must be a non-empty prefix path");
        }
        Self::validate_scope_victims(scope, victims)?;

        // Apply-time incarnation fence (4b): same terminal semantics as a
        // single-key remove.
        if !Self::incarnation_active(store, incarnation)? {
            return Ok(());
        }

        let mut w = store.cache_write();
        for v in victims {
            let cur = match store.cache_get_entry(incarnation, &v.key).map_err(cv)? {
                Some(cur) => cur,
                // Missing: stale victim (page raced a remove/vacuum), no-op.
                None => continue,
            };
            // Later state already advanced past this victim's tombstone:
            // converge (same rule as a single-key remove replay).
            if cur.generation > v.new_generation {
                continue;
            }
            if cur.generation == v.new_generation {
                // Exact-tombstone-only replay (review 303fb807 P0-3): the
                // tombstone at the victim's new generation must be THIS
                // victim's — same object, and a tombstone never carries an
                // expiry. Any other row at that generation is divergence.
                if cur.state == CacheEntryState::Tombstoned
                    && cur.object_id == v.object_id
                    && cur.expire_at == 0
                {
                    continue; // already applied
                }
                return err_box!(
                    "cache scope remove replay divergence for ({}, {})@{}: state {:?} object {} expire {} vs victim object {}",
                    incarnation,
                    v.key,
                    v.new_generation,
                    cur.state,
                    cur.object_id,
                    cur.expire_at,
                    v.object_id
                );
            }
            // Identity CAS at the victim's expected generation: the
            // committed row must match the observed (object_id, expire_at)
            // exactly — the version is only fully pinned by the triple.
            if cur.object_id != v.object_id || cur.expire_at != v.expire_at {
                return err_box!(
                    "cache scope remove identity mismatch for ({}, {})@{}: committed (object {}, expire {}) vs victim (object {}, expire {})",
                    incarnation,
                    v.key,
                    v.expected_generation,
                    cur.object_id,
                    cur.expire_at,
                    v.object_id,
                    v.expire_at
                );
            }
            if cur.generation != v.expected_generation {
                return err_box!(
                    "cache scope remove CAS violation for ({}, {}): committed generation {} vs victim {}",
                    incarnation,
                    v.key,
                    cur.generation,
                    v.expected_generation
                );
            }

            let new = CacheEntry {
                generation: v.new_generation,
                state: CacheEntryState::Tombstoned,
                object_id: cur.object_id,
                len: 0,
                ufs_mtime: cur.ufs_mtime,
                block_size: cur.block_size,
                expire_at: 0,
            };
            w.put_entry(incarnation, &v.key, &new).map_err(cv)?;
            if cur.expire_at > 0 {
                // Exact-identity CAS delete at the frozen position; the
                // committed (key, generation) at it was written by this
                // version's commit, so it must match exactly.
                w.delete_expiry(
                    cur.expire_at,
                    incarnation,
                    cur.object_id,
                    &v.key,
                    cur.generation,
                )
                .map_err(cv)?;
            }
            w.delete_object(cur.object_id).map_err(cv)?;
        }
        w.commit().map_err(cv)?;
        Ok(())
    }

    /// Validate a TTL sweep page: length bound, positive deadline, and
    /// strictly ascending frozen `(expire_at, incarnation, object_id)`
    /// positions (a page of the ordered expiry scan).
    fn validate_ttl_victims(now: i64, victims: &[ExpiryRow]) -> CommonResult<()> {
        validate_page_len(victims.len()).map_err(cv)?;
        for v in victims {
            validate_expiry_row(v)?;
            validate_incarnation(v.incarnation)?;
            if v.key.len() > MAX_CACHE_KEY_BYTES {
                return err_box!(
                    "ttl sweep victim key exceeds {} bytes: {}",
                    MAX_CACHE_KEY_BYTES,
                    v.key.len()
                );
            }
            // Due check (review 303fb807 P0-2): an illegal entry may not
            // tombstone a future deadline early, regardless of identity.
            if v.expire_at > now {
                return err_box!(
                    "ttl sweep victim ({}, {}) deadline {} is beyond the sweep deadline {}",
                    v.incarnation,
                    v.key,
                    v.expire_at,
                    now
                );
            }
        }
        for i in 1..victims.len() {
            let a = &victims[i - 1];
            let b = &victims[i];
            if (a.expire_at, a.incarnation, a.object_id)
                >= (b.expire_at, b.incarnation, b.object_id)
            {
                return err_box!(
                    "ttl sweep victims must be ascending in frozen index order: ({}, {}, {}) !< ({}, {}, {})",
                    a.expire_at,
                    a.incarnation,
                    a.object_id,
                    b.expire_at,
                    b.incarnation,
                    b.object_id
                );
            }
        }
        Ok(())
    }

    /// Conditional batch CAS (4c.2): TTL sweep over a page of due expiry
    /// rows. Per victim, exactly two identity-bound steps: (1) exact-CAS
    /// delete of the victim's OWN expiry row at its frozen position — a
    /// missing row is an idempotent no-op, an identity mismatch is loud
    /// divergence, and NO other expiry position is ever touched; (2) a full
    /// `(generation, object_id, expire_at)` identity CAS against the
    /// authoritative entry — an exact match tombstones the entry (and drops
    /// its reverse row) exactly like a remove; a missing or advanced entry
    /// is the stale terminal no-op (the load is dead; the old object is
    /// reclaimable).
    pub fn apply_ttl_sweep<S: LocalCacheIndexStore>(
        &self,
        store: &S,
        now: i64,
        victims: &[ExpiryRow],
    ) -> CommonResult<()> {
        if now <= 0 {
            return err_box!("ttl sweep deadline must be positive: {}", now);
        }
        Self::validate_ttl_victims(now, victims)?;

        let mut w = store.cache_write();
        for v in victims {
            // (1) The victim's own frozen position, exact identity only.
            w.delete_expiry(
                v.expire_at,
                v.incarnation,
                v.object_id,
                &v.key,
                v.generation,
            )
            .map_err(cv)?;

            // (2) Authoritative-entry CAS: only the exact version that
            // produced this expiry row may be tombstoned by the sweep —
            // and only inside a live namespace. In a fenced (revoked or
            // stale) incarnation the sweep stops at the expiry-index
            // cleanup above: the authoritative row is left for the
            // revoked-incarnation vacuum, the only variant allowed to
            // delete primary rows in a revoked namespace (4b fence).
            if !Self::incarnation_active(store, v.incarnation)? {
                continue;
            }
            match store.cache_get_entry(v.incarnation, &v.key).map_err(cv)? {
                Some(cur) if cur.generation == v.generation => {
                    if cur.object_id != v.object_id || cur.expire_at != v.expire_at {
                        return err_box!(
                            "ttl sweep identity divergence for ({}, {})@{}: committed (object {}, expire {}) vs victim (object {}, expire {})",
                            v.incarnation,
                            v.key,
                            v.generation,
                            cur.object_id,
                            cur.expire_at,
                            v.object_id,
                            v.expire_at
                        );
                    }
                    let new_generation = v.generation.checked_add(1).ok_or_else(|| {
                        CommonError::from(err_msg!(
                            "ttl sweep generation overflow at u64::MAX for ({}, {})",
                            v.incarnation,
                            v.key
                        ))
                    })?;
                    let new = CacheEntry {
                        generation: new_generation,
                        state: CacheEntryState::Tombstoned,
                        object_id: cur.object_id,
                        len: 0,
                        ufs_mtime: cur.ufs_mtime,
                        block_size: cur.block_size,
                        expire_at: 0,
                    };
                    w.put_entry(v.incarnation, &v.key, &new).map_err(cv)?;
                    w.delete_object(v.object_id).map_err(cv)?;
                }
                // Missing row: terminal no-op (stale victim; the expiry row
                // cleanup above already reclaimed the victim's own index
                // position).
                None => continue,
                // A later generation already advanced past the victim:
                // stale terminal no-op.
                Some(cur) if cur.generation > v.generation => continue,
                // A victim generation BEYOND the committed row is an
                // illegal entry (review 303fb807 P0-2): never a legit
                // race — loud, and the uncommitted batch leaves zero
                // writes (the staged expiry delete included).
                Some(cur) => {
                    return err_box!(
                        "ttl sweep victim generation {} is beyond the committed row generation {} for ({}, {})",
                        v.generation,
                        cur.generation,
                        v.incarnation,
                        v.key
                    )
                }
            }
        }
        w.commit().map_err(cv)?;
        Ok(())
    }

    /// Validate a vacuum page: length bound, strictly ascending keys,
    /// generation and object-domain checks.
    fn validate_vacuum_victims(victims: &[VacuumVictim]) -> CommonResult<()> {
        validate_page_len(victims.len()).map_err(cv)?;
        for (i, v) in victims.iter().enumerate() {
            if v.key.len() > MAX_CACHE_KEY_BYTES {
                return err_box!(
                    "vacuum victim key exceeds {} bytes: {}",
                    MAX_CACHE_KEY_BYTES,
                    v.key.len()
                );
            }
            if v.generation < 1 {
                return err_box!("vacuum victim generation must be >= 1: {}", v.generation);
            }
            if !BlockIdCodec::is_cache_owner(v.object_id) {
                return err_box!(
                    "vacuum victim object id outside cache domain: {}",
                    v.object_id
                );
            }
            if i > 0 && victims[i - 1].key >= v.key {
                return err_box!(
                    "vacuum victims must be strictly ascending by key: {} !< {}",
                    victims[i - 1].key,
                    v.key
                );
            }
        }
        Ok(())
    }

    /// Conditional batch (4c.2): revoked-incarnation vacuum. Gate-3
    /// re-verification at apply time — the incarnation row must exist,
    /// belong to `mount_id`, be revoked, and NOT be the mount's current
    /// pointer (revoke is permanent and pointers only move forward, so the
    /// check replays deterministically). Victims are then deleted WHOLE —
    /// entry row (no tombstone), own expiry row (exact identity), reverse
    /// row. Vacuum never touches the incarnation row, the policy row,
    /// outcomes, client watermarks, allocator watermarks, or the pointer.
    pub fn apply_vacuum<S: LocalCacheIndexStore>(
        &self,
        store: &S,
        incarnation: u64,
        mount_id: u32,
        victims: &[VacuumVictim],
    ) -> CommonResult<()> {
        validate_incarnation(incarnation)?;
        Self::validate_vacuum_victims(victims)?;

        match store.cache_get_incarnation(incarnation).map_err(cv)? {
            Some(row) => {
                if row.mount_id != mount_id {
                    return err_box!(
                        "vacuum incarnation {} belongs to mount {}, entry says mount {}",
                        incarnation,
                        row.mount_id,
                        mount_id
                    );
                }
                if !row.revoked {
                    return err_box!(
                        "vacuum incarnation {} is not revoked: vacuum of a live namespace is illegal",
                        incarnation
                    );
                }
            }
            // Incarnation rows are durable forever, so a committed vacuum
            // entry always finds its row — replay included.
            None => return err_box!("vacuum incarnation {} has no incarnation row", incarnation),
        }
        if store.cache_current_incarnation(mount_id).map_err(cv)? == Some(incarnation) {
            return err_box!(
                "vacuum incarnation {} is still mount {}'s current incarnation",
                incarnation,
                mount_id
            );
        }

        let mut w = store.cache_write();
        for v in victims {
            let cur = match store.cache_get_entry(incarnation, &v.key).map_err(cv)? {
                Some(cur) => cur,
                // Missing: already vacuumed page replay, no-op.
                None => continue,
            };
            if cur.generation != v.generation {
                if cur.generation > v.generation {
                    // A later version exists: the page raced a mutation;
                    // the row stays for the next vacuum page. Deterministic
                    // no-op (replay sees the same committed state).
                    continue;
                }
                return err_box!(
                    "vacuum victim generation {} is beyond the committed row generation {} for ({}, {})",
                    v.generation,
                    cur.generation,
                    incarnation,
                    v.key
                );
            }
            if cur.object_id != v.object_id || cur.expire_at != v.expire_at {
                return err_box!(
                    "vacuum identity mismatch for ({}, {})@{}: committed (object {}, expire {}) vs victim (object {}, expire {})",
                    incarnation,
                    v.key,
                    v.generation,
                    cur.object_id,
                    cur.expire_at,
                    v.object_id,
                    v.expire_at
                );
            }
            w.delete_entry(incarnation, &v.key).map_err(cv)?;
            if cur.expire_at > 0 {
                w.delete_expiry(
                    cur.expire_at,
                    incarnation,
                    cur.object_id,
                    &v.key,
                    cur.generation,
                )
                .map_err(cv)?;
            }
            w.delete_object(cur.object_id).map_err(cv)?;
        }
        w.commit().map_err(cv)?;
        Ok(())
    }

    /// Conditional batch (4c.2): bounded outcome-window GC with the frozen
    /// eligibility fence. Eligibility is judged against the
    /// leader-observed `evict_below` frozen in the entry — NEVER against
    /// the apply-time watermark, which would make the entry's effect
    /// depend on apply timing (a first no-op could turn into a delete on
    /// replay after the watermark advanced — non-convergent). The apply
    /// loud-rejects a group whose `evict_below` exceeds the client's
    /// durable watermark (an illegal entry; watermark monotonicity keeps
    /// this check replay-stable), then evicts the listed outcome rows
    /// unconditionally: a missing outcome is the idempotent replay no-op.
    /// The watermark itself is never moved — an evicted token keeps
    /// answering Expired (terminal, never re-executed).
    pub fn apply_outcome_gc<S: LocalCacheIndexStore>(
        &self,
        store: &S,
        groups: &[OutcomeGcGroup],
    ) -> CommonResult<()> {
        let total: usize = groups.iter().map(|g| g.op_seqs.len()).sum();
        validate_page_len(total).map_err(cv)?;
        for (i, g) in groups.iter().enumerate() {
            Self::check_token(OpToken {
                client_id: g.client_id,
                op_seq: g.evict_below,
            })?;
            if g.op_seqs.is_empty() {
                return err_box!("outcome gc group for client {} is empty", g.client_id);
            }
            if i > 0 && groups[i - 1].client_id >= g.client_id {
                return err_box!("outcome gc groups must be strictly ascending by client_id");
            }
            for (j, seq) in g.op_seqs.iter().enumerate() {
                if *seq == 0 {
                    return err_box!("outcome gc op_seq must be >= 1: {}", seq);
                }
                if *seq >= g.evict_below {
                    return err_box!(
                        "outcome gc op_seq {} is not below the frozen eligibility fence {} for client {}",
                        seq,
                        g.evict_below,
                        g.client_id
                    );
                }
                if j > 0 && g.op_seqs[j - 1] >= *seq {
                    return err_box!(
                        "outcome gc op_seqs must be strictly ascending for client {}",
                        g.client_id
                    );
                }
            }
            // Frozen-fence legality: the observed watermark must still (or
            // already) be at/above the frozen cutoff. Monotonic, so this
            // replays identically.
            match store.cache_client_watermark(g.client_id).map_err(cv)? {
                Some(hw) if g.evict_below <= hw => (),
                other => {
                    return err_box!(
                    "outcome gc eligibility fence {} exceeds client {}'s durable watermark {:?}",
                    g.evict_below,
                    g.client_id,
                    other
                )
                }
            }
        }

        let mut w = store.cache_write();
        for g in groups {
            for seq in &g.op_seqs {
                let token = OpToken {
                    client_id: g.client_id,
                    op_seq: *seq,
                };
                // Unconditional within the frozen fence: missing = the
                // idempotent replay no-op.
                if store.cache_get_outcome(token).map_err(cv)?.is_some() {
                    w.delete_outcome(token).map_err(cv)?;
                }
            }
        }
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

        mgr.apply_incarnation_allocate_v2(&store, token(2, 1), 7, 1, 3_600_000)
            .unwrap();
        assert_eq!(store.cache_current_incarnation(7).unwrap(), Some(1));
        let row1 = store.cache_get_incarnation(1).unwrap().unwrap();
        assert!(!row1.revoked);
        // Frozen TTL lives in the separate policy row (option A); a legacy
        // row decodes without it and readers synthesize ttl 0.
        assert_eq!(
            store.cache_get_incarnation_policy(1).unwrap(),
            Some(IncarnationPolicyRow { ttl_ms: 3_600_000 })
        );
        assert_eq!(
            store.cache_get_outcome(token(2, 1)).unwrap(),
            Some(OpOutcome::IncarnationAllocatedV2 {
                incarnation: 1,
                mount_id: 7,
                ttl_ms: 3_600_000,
            })
        );

        // Idempotent replay and restore.
        mgr.apply_incarnation_allocate_v2(&store, token(2, 1), 7, 1, 3_600_000)
            .unwrap();
        // Replay with different parameters (ttl) is divergence.
        assert!(mgr
            .apply_incarnation_allocate_v2(&store, token(2, 1), 7, 1, 999)
            .is_err());
        // Replay with a different mount id is divergence.
        assert!(mgr
            .apply_incarnation_allocate_v2(&store, token(2, 1), 8, 1, 3_600_000)
            .is_err());
        let mgr2 = CacheManager::new();
        mgr2.restore_watermarks(&store).unwrap();
        assert_eq!(mgr2.current_incarnation(), 1);

        // Remount: new incarnation, pointer moves; frozen ttl may differ.
        mgr.apply_incarnation_allocate_v2(&store, token(2, 2), 7, 2, 0)
            .unwrap();
        assert_eq!(store.cache_current_incarnation(7).unwrap(), Some(2));

        // Revoke the current incarnation: pointer cleared, row kept; the
        // frozen policy row is untouched history.
        mgr.apply_incarnation_revoke(&store, 7, 2).unwrap();
        assert_eq!(store.cache_current_incarnation(7).unwrap(), None);
        let row2 = store.cache_get_incarnation(2).unwrap().unwrap();
        assert!(row2.revoked);
        // Idempotent revoke.
        mgr.apply_incarnation_revoke(&store, 7, 2).unwrap();

        // Late allocate replay for an incarnation that was later revoked
        // converges instead of resurrecting the pointer.
        mgr.apply_incarnation_allocate_v2(&store, token(2, 2), 7, 2, 0)
            .unwrap();
        assert_eq!(store.cache_current_incarnation(7).unwrap(), None);

        // A legacy (4a-shape) outcome for the same token is NOT an exact
        // match for a V2 request: the parameters are not trusted history.
        mgr.apply_incarnation_allocate(&store, token(4, 1), 9, 3)
            .unwrap();
        assert!(mgr
            .apply_incarnation_allocate_v2(&store, token(4, 1), 9, 3, 0)
            .is_err());
        // And symmetrically, a V2 outcome is not matched by a legacy-shape
        // replay of the same token.
        mgr.apply_incarnation_allocate_v2(&store, token(5, 1), 9, 4, 0)
            .unwrap();
        assert!(mgr
            .apply_incarnation_allocate(&store, token(5, 1), 9, 4)
            .is_err());

        // Negative ttl is rejected outright.
        assert!(mgr
            .apply_incarnation_allocate_v2(&store, token(3, 1), 8, 3, -1)
            .is_err());
        // Divergence: same incarnation claimed by another mount.
        assert!(mgr
            .apply_incarnation_allocate_v2(&store, token(3, 1), 8, 1, 0)
            .is_err());
        // Issuable bound is the second gate (u64::MAX rejected by the store
        // as the first gate, i64::MAX by the allocator).
        assert!(mgr
            .apply_incarnation_allocate_v2(&store, token(3, 1), 8, u64::MAX, 0)
            .is_err());
        assert!(mgr
            .apply_incarnation_allocate_v2(&store, token(3, 1), 8, MAX_ISSUABLE_INCARNATION + 1, 0)
            .is_err());
    }

    #[test]
    fn test_allocate_commit_remove_lifecycle() {
        let store = new_store("key-lifecycle");
        let mgr = CacheManager::new();
        mgr.apply_id_reserve(&store, token(1, 1), OBJ, OBJ + 10)
            .unwrap();

        // 4b: the apply-time incarnation fence requires an active
        // incarnation row before any allocate/commit/remove executes.
        mgr.apply_incarnation_allocate(&store, token(90, 1), 5, 1)
            .unwrap();

        let alloc = reserved(1, OBJ);
        mgr.apply_allocate(&store, token(1, 2), 1, "/a/b", 300, &alloc)
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
                object_id: OBJ,
                file_len: 300,
                block_size: alloc.block_size,
            })
        );

        // Idempotent allocate replay.
        mgr.apply_allocate(&store, token(1, 2), 1, "/a/b", 300, &alloc)
            .unwrap();

        // Commit: Reserved@1 -> Valid with len/ufs_mtime; TTL row appears.
        // The load token binds the allocation; the commit carries its own
        // distinct op token.
        mgr.apply_commit(
            &store,
            token(1, 2),
            token(1, 3),
            1,
            "/a/b",
            1,
            OBJ,
            300,
            12345,
            5000,
        )
        .unwrap();
        let committed = store.cache_get_entry(1, "/a/b").unwrap().unwrap();
        assert_eq!(committed.state, CacheEntryState::Valid);
        assert_eq!(
            (committed.len, committed.ufs_mtime, committed.expire_at),
            (300, 12345, 5000)
        );
        assert_eq!(committed.generation, 1);
        let due = store.cache_scan_expiry(5000, None, 10).unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].key, "/a/b");

        // Commit replay is idempotent: the recorded Committed outcome for
        // the commit token wins over the (advanced) entry row.
        mgr.apply_commit(
            &store,
            token(1, 2),
            token(1, 3),
            1,
            "/a/b",
            1,
            OBJ,
            300,
            12345,
            5000,
        )
        .unwrap();
        // A commit replay whose payload diverges from the recorded load
        // geometry (len 999 != allocated 300) is rejected by the load
        // binding, before any outcome gate.
        assert!(mgr
            .apply_commit(
                &store,
                token(1, 2),
                token(1, 3),
                1,
                "/a/b",
                1,
                OBJ,
                999,
                12345,
                5000
            )
            .is_err());
        assert_eq!(store.cache_get_entry(1, "/a/b").unwrap().unwrap().len, 300);
        // A DIFFERENT commit token at the consumed generation with a
        // different payload is a divergence.
        assert!(mgr
            .apply_commit(
                &store,
                token(1, 2),
                token(7, 1),
                1,
                "/a/b",
                1,
                OBJ,
                999,
                12345,
                5000
            )
            .is_err());

        // Remove: Valid@1 -> Tombstoned@2, expiry and reverse rows dropped.
        mgr.apply_remove(&store, 1, "/a/b", 1, 2, OBJ).unwrap();
        let removed = store.cache_get_entry(1, "/a/b").unwrap().unwrap();
        assert_eq!(removed.state, CacheEntryState::Tombstoned);
        assert_eq!(removed.generation, 2);
        assert!(store
            .cache_scan_expiry(100000, None, 10)
            .unwrap()
            .is_empty());
        assert!(store.cache_get_object(OBJ).unwrap().is_none());

        // Late commit against the superseded generation: terminal no-op
        // (fresh commit token, so the entry-row supersede path is exercised).
        mgr.apply_commit(
            &store,
            token(1, 2),
            token(7, 2),
            1,
            "/a/b",
            1,
            OBJ,
            300,
            12345,
            5000,
        )
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
        mgr.apply_allocate(&store, token(1, 4), 1, "/a/b", 300, &alloc3)
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
        mgr.apply_allocate(&store, token(1, 2), 1, "/k", 100, &alloc)
            .unwrap();
        mgr.apply_commit(
            &store,
            token(1, 2),
            token(1, 3),
            1,
            "/k",
            1,
            OBJ,
            100,
            777,
            0,
        )
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
            .apply_allocate(&store, token(1, 2), 1, "/k", 100, &alloc)
            .unwrap();
        replay
            .apply_commit(
                &store,
                token(1, 2),
                token(1, 3),
                1,
                "/k",
                1,
                OBJ,
                100,
                777,
                0,
            )
            .unwrap();
        assert_eq!(replay.current_object_id(), OBJ + 9);
        assert_eq!(replay.current_incarnation(), 1);

        // Commit without a recorded load allocation fails loudly.
        assert!(replay
            .apply_commit(
                &store,
                token(3, 9),
                token(3, 10),
                1,
                "/missing",
                1,
                OBJ,
                1,
                1,
                0
            )
            .is_err());
        // Allocate with a non-Reserved state fails loudly.
        let mut bad = reserved(9, OBJ + 5);
        bad.state = CacheEntryState::Valid;
        bad.ufs_mtime = 5;
        assert!(replay
            .apply_allocate(&store, token(9, 9), 1, "/bad", 1, &bad)
            .is_err());
    }

    #[test]
    fn test_identity_token_gate() {
        let store = new_store("token-gate");
        let mgr = CacheManager::new();

        // 4b: the apply-time incarnation fence requires an active
        // incarnation row before any allocate/commit/remove executes.
        mgr.apply_incarnation_allocate(&store, token(90, 1), 5, 1)
            .unwrap();

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
        mgr.apply_allocate(&store, token(2, 1), 1, "/k", 100, &alloc)
            .unwrap();

        // Allocate divergence on the same token is loud (outcome present,
        // different parameters).
        let alloc_diff = reserved(1, OBJ + 1);
        assert!(mgr
            .apply_allocate(&store, token(2, 1), 1, "/k", 100, &alloc_diff)
            .is_err());

        let mut w = store.cache_write();
        w.delete_outcome(token(2, 1)).unwrap();
        w.commit().unwrap();
        // Client 2 watermark is 1, so the same token is now Expired, not
        // AlreadyApplied: terminal no-op, entry row untouched.
        mgr.apply_allocate(&store, token(2, 1), 1, "/k", 100, &alloc)
            .unwrap();
        assert_eq!(
            store.cache_get_entry(1, "/k").unwrap(),
            Some(alloc),
            "expired replay must not rewrite the entry row"
        );
        assert!(store.cache_get_outcome(token(2, 1)).unwrap().is_none());

        // Incarnation allocate divergence on the same token is loud
        // (incarnation 1 is owned by the fence setup; use 2/3 here).
        mgr.apply_incarnation_allocate(&store, token(3, 1), 7, 2)
            .unwrap();
        assert!(mgr
            .apply_incarnation_allocate(&store, token(3, 1), 7, 3)
            .is_err());
        assert_eq!(store.cache_current_incarnation(7).unwrap(), Some(2));
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

        // 4b: the apply-time incarnation fence requires an active
        // incarnation row before any allocate/commit/remove executes.
        // Mount 4 on purpose: this test later moves mount 5's pointer to
        // incarnation 3, which would stale incarnation 1.
        mgr.apply_incarnation_allocate(&store, token(90, 1), 4, 1)
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
        mgr.apply_allocate(&store, token(9, 1), 1, "/k", 100, &alloc)
            .unwrap();
        // Identical parameters under a different token over a live
        // Reserved row -> alias error, entry untouched, no outcome.
        assert!(mgr
            .apply_allocate(&store, token(9, 2), 1, "/k", 100, &alloc)
            .is_err());
        let alloc2 = reserved(2, OBJ + 1);
        assert!(mgr
            .apply_allocate(&store, token(9, 3), 1, "/k", 100, &alloc2)
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
            .apply_allocate(&store, token(10, 1), 1, "/other", 100, &alloc_alias)
            .is_err());
        assert!(store.cache_get_outcome(token(10, 1)).unwrap().is_none());
        assert!(store.cache_get_entry(1, "/other").unwrap().is_none());
        // Unreserved id ahead of the durable HW (OBJ+9) -> error.
        let alloc_unreserved = reserved(1, OBJ + 500);
        assert!(mgr
            .apply_allocate(&store, token(10, 2), 1, "/other", 100, &alloc_unreserved)
            .is_err());
        assert!(store.cache_get_outcome(token(10, 2)).unwrap().is_none());
        assert!(store.cache_get_entry(1, "/other").unwrap().is_none());
        // Same key re-open: remove /k@1 (object OBJ) tombstones at gen 2,
        // then a gen-3 allocate trying to reuse OBJ is a per-key id
        // regression -> error.
        mgr.apply_remove(&store, 1, "/k", 1, 2, OBJ).unwrap();
        let alloc_reuse = reserved(3, OBJ);
        assert!(mgr
            .apply_allocate(&store, token(10, 3), 1, "/k", 100, &alloc_reuse)
            .is_err());
        assert!(store.cache_get_outcome(token(10, 3)).unwrap().is_none());
        // /k stays at the tombstone; a monotonic re-open still works.
        assert_eq!(
            store.cache_get_entry(1, "/k").unwrap().unwrap().state,
            CacheEntryState::Tombstoned
        );
        let alloc_reopen = reserved(3, OBJ + 1);
        mgr.apply_allocate(&store, token(10, 4), 1, "/k", 100, &alloc_reopen)
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
        mgr.apply_allocate(&store, token(3, 1), 1, "/k", 100, &alloc)
            .unwrap();
        let alloc_huge = reserved(1, BlockIdCodec::CACHE_OBJECT_MAX - 1);
        assert!(mgr
            .apply_allocate(&store, token(3, 1), 1, "/other", 100, &alloc_huge)
            .is_err());
        // Client 3 watermark is 1, so a fresh lower op_seq is Expired only
        // after the outcome is gone; with the outcome present it is a
        // divergence instead. Use client 4 with an established watermark.
        mgr.apply_id_reserve(&store, token(4, 5), OBJ + 100, OBJ + 110)
            .unwrap();
        let volatile_before = mgr.current_object_id();
        mgr.apply_allocate(&store, token(4, 1), 1, "/late", 100, &alloc_huge)
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

        // 4b: the apply-time incarnation fence requires an active
        // incarnation row before any allocate/commit/remove executes.
        mgr.apply_incarnation_allocate(&store, token(90, 1), 5, 1)
            .unwrap();

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
            .apply_allocate(&store, token(1, 1), 1, "/k", 100, &alloc)
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

        // 4b: the apply-time incarnation fence requires an active
        // incarnation row before any allocate/commit/remove executes.
        mgr.apply_incarnation_allocate(&store, token(90, 1), 5, 1)
            .unwrap();

        let alloc = reserved(1, OBJ);
        mgr.apply_allocate(&store, token(1, 1), 1, "/k", 100, &alloc)
            .unwrap();

        // Commit may never skip generations: Reserved@1 + commit@2 errors
        // (the load binding records generation 1, so the skip is rejected).
        assert!(mgr
            .apply_commit(
                &store,
                token(1, 1),
                token(1, 2),
                1,
                "/k",
                2,
                OBJ,
                100,
                777,
                0
            )
            .is_err());

        // Exact Reserved@g -> Valid works, and exact replay is idempotent.
        mgr.apply_commit(
            &store,
            token(1, 1),
            token(1, 2),
            1,
            "/k",
            1,
            OBJ,
            100,
            777,
            0,
        )
        .unwrap();
        mgr.apply_commit(
            &store,
            token(1, 1),
            token(1, 2),
            1,
            "/k",
            1,
            OBJ,
            100,
            777,
            0,
        )
        .unwrap();

        // A different payload at the committed generation (fresh commit
        // token) diverges from the recorded load geometry.
        assert!(mgr
            .apply_commit(
                &store,
                token(1, 1),
                token(8, 1),
                1,
                "/k",
                1,
                OBJ,
                200,
                777,
                0
            )
            .is_err());

        // Allocate may not overwrite a Valid/Reserved row at a later
        // generation: only Tombstoned@g -> Reserved@g+1 re-opens the key.
        let alloc2 = reserved(2, OBJ + 1);
        assert!(mgr
            .apply_allocate(&store, token(1, 2), 1, "/k", 100, &alloc2)
            .is_err());
        // Cross-generation jumps are forbidden even from a tombstone.
        mgr.apply_remove(&store, 1, "/k", 1, 2, OBJ).unwrap();
        let alloc4 = reserved(4, OBJ + 2);
        assert!(mgr
            .apply_allocate(&store, token(1, 3), 1, "/k", 100, &alloc4)
            .is_err());
        // Adjacent tombstone re-open is the only legal re-allocation.
        let alloc3 = reserved(3, OBJ + 1);
        mgr.apply_allocate(&store, token(1, 3), 1, "/k", 100, &alloc3)
            .unwrap();
        assert_eq!(
            store.cache_get_entry(1, "/k").unwrap().unwrap().generation,
            3
        );

        // A first allocation for a fresh key must start at generation 1.
        let alloc9 = reserved(9, OBJ + 5);
        assert!(mgr
            .apply_allocate(&store, token(1, 4), 1, "/fresh", 100, &alloc9)
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
            mgr.apply_allocate(&store, token(1, 2), 1, "/a", 300, &alloc)
                .unwrap();
            let alloc_b = reserved(1, OBJ + 1);
            mgr.apply_allocate(&store, token(1, 3), 1, "/b", 100, &alloc_b)
                .unwrap();
            mgr.apply_commit(
                &store,
                token(1, 2),
                token(1, 4),
                1,
                "/a",
                1,
                OBJ,
                300,
                111,
                9000,
            )
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

    /// 4b P0-1: the apply-time incarnation fence. A revoked (or stale)
    /// incarnation turns allocate/commit/remove into deterministic no-ops —
    /// rows, outcomes, and watermarks stay untouched — while an exact
    /// outcome replay (AlreadyApplied) still resolves in front of the
    /// fence.
    #[test]
    fn test_apply_time_incarnation_fence() {
        let store = new_store("apply-fence");
        let mgr = CacheManager::new();
        mgr.apply_id_reserve(&store, token(1, 1), OBJ, OBJ + 10)
            .unwrap();
        mgr.apply_incarnation_allocate_v2(&store, token(2, 1), 5, 1, 0)
            .unwrap();
        let alloc = reserved(1, OBJ);
        mgr.apply_allocate(&store, token(1, 2), 1, "/k", 100, &alloc)
            .unwrap();

        // Revoke fences everything under incarnation 1.
        mgr.apply_incarnation_revoke(&store, 5, 1).unwrap();

        // Fenced allocate: deterministic no-op — no row, no outcome.
        let alloc_j = reserved(1, OBJ + 1);
        mgr.apply_allocate(&store, token(1, 5), 1, "/j", 100, &alloc_j)
            .unwrap();
        assert!(store.cache_get_entry(1, "/j").unwrap().is_none());
        assert!(store.cache_get_outcome(token(1, 5)).unwrap().is_none());

        // Fenced commit: the Reserved row stays untouched, no commit
        // outcome, no client watermark advance.
        mgr.apply_commit(
            &store,
            token(1, 2),
            token(1, 6),
            1,
            "/k",
            1,
            OBJ,
            100,
            777,
            0,
        )
        .unwrap();
        assert_eq!(
            store.cache_get_entry(1, "/k").unwrap(),
            Some(alloc.clone()),
            "fenced commit must not touch the Reserved row"
        );
        assert!(store.cache_get_outcome(token(1, 6)).unwrap().is_none());

        // Fenced remove: the row keeps its Reserved state.
        mgr.apply_remove(&store, 1, "/k", 1, 2, OBJ).unwrap();
        assert_eq!(
            store.cache_get_entry(1, "/k").unwrap(),
            Some(alloc),
            "fenced remove must not tombstone the row"
        );

        // Exact outcome replay runs BEFORE the fence: the allocate token's
        // AlreadyApplied path resolves from history even though the
        // incarnation is revoked.
        mgr.apply_allocate(&store, token(1, 2), 1, "/k", 100, &reserved(1, OBJ))
            .unwrap();
        assert_eq!(
            store.cache_get_outcome(token(1, 2)).unwrap(),
            Some(OpOutcome::Allocated {
                incarnation: 1,
                key: "/k".into(),
                generation: 1,
                object_id: OBJ,
                file_len: 100,
                block_size: 128,
            })
        );

        // Stale fence: a newer incarnation owns the mount — the old one is
        // fenced exactly like a revoked one.
        mgr.apply_incarnation_allocate_v2(&store, token(2, 2), 5, 2, 0)
            .unwrap();
        assert_eq!(store.cache_current_incarnation(5).unwrap(), Some(2));
        mgr.apply_allocate(&store, token(1, 7), 1, "/s", 100, &reserved(1, OBJ + 2))
            .unwrap();
        assert!(store.cache_get_entry(1, "/s").unwrap().is_none());
        // Writes under the NEW incarnation are admitted.
        mgr.apply_allocate(&store, token(1, 8), 2, "/s", 100, &reserved(1, OBJ + 3))
            .unwrap();
        assert!(store.cache_get_entry(2, "/s").unwrap().is_some());

        // Revoke preserves the watermark and every recorded outcome.
        assert_eq!(
            store
                .cache_get_state(state_tags::CACHE_INCARNATION)
                .unwrap(),
            Some(2)
        );
        assert!(store.cache_get_outcome(token(2, 1)).unwrap().is_some());
        assert!(store.cache_get_outcome(token(2, 2)).unwrap().is_some());
    }

    // ---- 4c.2 bounded mutation batches ----

    /// Unit-test stand-in for a completed allocate+commit round trip in
    /// namespace `incarnation` (mount `mount_id`): a Valid@1 entry with an
    /// expiry row at `expire_at` (0 = none) and its reverse row. Repeated
    /// calls replay idempotently (issuer-side rows use fixed tokens).
    #[allow(clippy::too_many_arguments)]
    fn seed_committed(
        store: &RocksInodeStore,
        mgr: &CacheManager,
        mount_id: u32,
        incarnation: u64,
        key: &str,
        object_id: i64,
        len: i64,
        expire_at: i64,
    ) {
        mgr.apply_incarnation_allocate_v2(
            store,
            OpToken {
                client_id: 91,
                op_seq: incarnation,
            },
            mount_id,
            incarnation,
            0,
        )
        .unwrap();
        mgr.apply_id_reserve(store, token(1, 1), OBJ, OBJ + 1000)
            .unwrap();
        let alloc_token = OpToken {
            client_id: 31,
            op_seq: object_id as u64,
        };
        mgr.apply_allocate(
            store,
            alloc_token,
            incarnation,
            key,
            len,
            &reserved(1, object_id),
        )
        .unwrap();
        mgr.apply_commit(
            store,
            alloc_token,
            OpToken {
                client_id: 32,
                op_seq: object_id as u64,
            },
            incarnation,
            key,
            1,
            object_id,
            len,
            777,
            expire_at,
        )
        .unwrap();
    }

    /// Re-open a Tombstoned key: the only legal re-allocation
    /// (Tombstoned@g -> Reserved@g+1) followed by a commit, producing a
    /// Valid@{g+1} row with a strictly greater object id.
    #[allow(clippy::too_many_arguments)]
    fn reopen_committed(
        store: &RocksInodeStore,
        mgr: &CacheManager,
        incarnation: u64,
        key: &str,
        old_generation: u64,
        object_id: i64,
        len: i64,
        expire_at: i64,
    ) {
        let generation = old_generation + 1;
        let alloc_token = OpToken {
            client_id: 41,
            op_seq: object_id as u64,
        };
        mgr.apply_allocate(
            store,
            alloc_token,
            incarnation,
            key,
            len,
            &reserved(generation, object_id),
        )
        .unwrap();
        mgr.apply_commit(
            store,
            alloc_token,
            OpToken {
                client_id: 42,
                op_seq: object_id as u64,
            },
            incarnation,
            key,
            generation,
            object_id,
            len,
            777,
            expire_at,
        )
        .unwrap();
    }

    fn scope_victim(key: &str, expected: u64, object_id: i64, expire_at: i64) -> ScopeRemoveVictim {
        ScopeRemoveVictim {
            key: key.to_string(),
            expected_generation: expected,
            new_generation: expected + 1,
            object_id,
            expire_at,
        }
    }

    fn vacuum_victim(key: &str, generation: u64, object_id: i64, expire_at: i64) -> VacuumVictim {
        VacuumVictim {
            key: key.to_string(),
            generation,
            object_id,
            expire_at,
        }
    }

    fn expiry_row(expire_at: i64, incarnation: u64, object_id: i64, key: &str) -> ExpiryRow {
        ExpiryRow {
            expire_at,
            incarnation,
            object_id,
            key: key.to_string(),
            generation: 1,
        }
    }

    #[test]
    fn test_apply_scope_remove_cas_fences_validation() {
        let store = new_store("scope-remove");
        let mgr = CacheManager::new();

        seed_committed(&store, &mgr, 5, 1, "/a/x", OBJ, 300, 5000);
        seed_committed(&store, &mgr, 5, 1, "/a/y", OBJ + 1, 400, 5000);
        seed_committed(&store, &mgr, 5, 1, "/b/z", OBJ + 2, 500, 6000);

        // Exact page over the /a scope: both victims tombstoned at g+1,
        // expiry and reverse rows dropped, len cleared, geometry preserved.
        let victims = vec![
            scope_victim("/a/x", 1, OBJ, 5000),
            scope_victim("/a/y", 1, OBJ + 1, 5000),
        ];
        mgr.apply_scope_remove(&store, 1, "/a", &victims).unwrap();
        for (key, block_size) in [("/a/x", 128), ("/a/y", 128)] {
            let e = store.cache_get_entry(1, key).unwrap().unwrap();
            assert_eq!(
                (
                    e.state,
                    e.generation,
                    e.object_id,
                    e.len,
                    e.expire_at,
                    e.block_size
                ),
                (
                    CacheEntryState::Tombstoned,
                    2,
                    e.object_id,
                    0,
                    0,
                    block_size
                )
            );
        }
        assert!(store.cache_get_object(OBJ).unwrap().is_none());
        assert!(store.cache_get_object(OBJ + 1).unwrap().is_none());
        // Outside the scope: untouched, its expiry row survives.
        let outside = store.cache_get_entry(1, "/b/z").unwrap().unwrap();
        assert_eq!(outside.state, CacheEntryState::Valid);
        assert_eq!(store.cache_scan_expiry(100_000, None, 10).unwrap().len(), 1);

        // Idempotent replay: every victim meets Tombstoned@new_generation.
        mgr.apply_scope_remove(&store, 1, "/a", &victims).unwrap();

        // Missing victim (page raced a remove/vacuum): deterministic no-op.
        mgr.apply_scope_remove(&store, 1, "/a", &[scope_victim("/a/gone", 3, OBJ + 9, 0)])
            .unwrap();
        assert!(store.cache_get_entry(1, "/a/gone").unwrap().is_none());

        // Later generation already advanced past the victim's tombstone:
        // converge (stale no-op). /a/x re-opened to Valid@3 first.
        reopen_committed(&store, &mgr, 1, "/a/x", 2, OBJ + 10, 350, 0);
        mgr.apply_scope_remove(&store, 1, "/a", &[scope_victim("/a/x", 1, OBJ, 5000)])
            .unwrap();
        assert_eq!(
            store
                .cache_get_entry(1, "/a/x")
                .unwrap()
                .unwrap()
                .generation,
            3
        );

        // Identity divergence: expected generation matches the committed
        // row but the observed (object, expire) does not — loud, and the
        // batch leaves zero writes behind.
        assert!(mgr
            .apply_scope_remove(&store, 1, "/b", &[scope_victim("/b/z", 1, OBJ + 9, 6000)])
            .is_err());
        assert_eq!(
            store.cache_get_entry(1, "/b/z").unwrap().unwrap().state,
            CacheEntryState::Valid
        );

        // Replay divergence: the row sits at the victim's new generation
        // but is NOT the victim's tombstone (a newer identity lives there).
        assert!(mgr
            .apply_scope_remove(&store, 1, "/a", &[scope_victim("/a/x", 2, OBJ + 10, 0)])
            .is_err());
        assert_eq!(
            store.cache_get_entry(1, "/a/x").unwrap().unwrap().state,
            CacheEntryState::Valid
        );

        // CAS violation: the victim's expected generation is beyond the
        // committed row (illegal entry, never a legit race).
        assert!(mgr
            .apply_scope_remove(&store, 1, "/b", &[scope_victim("/b/z", 5, OBJ + 2, 6000)])
            .is_err());

        // 4b fence: a revoked namespace turns the whole batch into a no-op.
        mgr.apply_incarnation_revoke(&store, 5, 1).unwrap();
        mgr.apply_scope_remove(&store, 1, "/b", &[scope_victim("/b/z", 1, OBJ + 2, 6000)])
            .unwrap();
        assert_eq!(
            store.cache_get_entry(1, "/b/z").unwrap().unwrap().state,
            CacheEntryState::Valid
        );
        assert_eq!(store.cache_scan_expiry(100_000, None, 10).unwrap().len(), 1);

        // Validation: empty scope, empty page, page-cap bomb, non-adjacent
        // generations, zero generation, non-cache object id, unsorted keys.
        assert!(mgr.apply_scope_remove(&store, 1, "", &victims).is_err());
        assert!(mgr.apply_scope_remove(&store, 1, "/a", &[]).is_err());
        let mut bomb: Vec<ScopeRemoveVictim> = (0..65)
            .map(|i| scope_victim(&format!("/a/{:04}", i), 1, OBJ + 100 + i, 0))
            .collect();
        assert!(mgr.apply_scope_remove(&store, 1, "/a", &bomb).is_err());
        assert!(mgr
            .apply_scope_remove(
                &store,
                1,
                "/a",
                &[ScopeRemoveVictim {
                    key: "/a/x".into(),
                    expected_generation: 1,
                    new_generation: 3,
                    object_id: OBJ,
                    expire_at: 0,
                }]
            )
            .is_err());
        assert!(mgr
            .apply_scope_remove(&store, 1, "/a", &[scope_victim("/a/x", 0, OBJ, 0)])
            .is_err());
        assert!(mgr
            .apply_scope_remove(&store, 1, "/a", &[scope_victim("/a/x", 1, 5, 0)])
            .is_err());
        bomb.sort_by(|a, b| b.key.cmp(&a.key));
        assert!(mgr.apply_scope_remove(&store, 1, "/a", &bomb[1..]).is_err());
    }

    #[test]
    fn test_apply_ttl_sweep_identity_and_fence() {
        let store = new_store("ttl-sweep");
        let mgr = CacheManager::new();

        seed_committed(&store, &mgr, 5, 1, "/t/a", OBJ, 300, 1000);
        seed_committed(&store, &mgr, 5, 1, "/t/b", OBJ + 1, 400, 1000);
        seed_committed(&store, &mgr, 5, 1, "/t/c", OBJ + 2, 500, 2000);
        // A due row inside a revoked namespace: expiry-index cleanup only.
        seed_committed(&store, &mgr, 6, 2, "/t/d", OBJ + 3, 500, 1000);
        mgr.apply_incarnation_revoke(&store, 6, 2).unwrap();

        // The leader page at now=1500 covers exactly the due rows, in
        // frozen index order (expire_at, incarnation, object_id).
        let page = store.cache_scan_expiry(1500, None, 10).unwrap();
        assert_eq!(page.len(), 3);
        assert_eq!(
            (
                page[0].key.as_str(),
                page[1].key.as_str(),
                page[2].key.as_str()
            ),
            ("/t/a", "/t/b", "/t/d")
        );

        mgr.apply_ttl_sweep(&store, 1500, &page).unwrap();
        // Live namespace: full remove semantics at g+1.
        for key in ["/t/a", "/t/b"] {
            let e = store.cache_get_entry(1, key).unwrap().unwrap();
            assert_eq!(
                (e.state, e.generation, e.len, e.expire_at),
                (CacheEntryState::Tombstoned, 2, 0, 0)
            );
        }
        // Fenced namespace (4b retention): the expiry row was reclaimed
        // but the authoritative row stays for the revoked-incarnation
        // vacuum — the ONLY variant allowed to delete it.
        let fenced = store.cache_get_entry(2, "/t/d").unwrap().unwrap();
        assert_eq!(fenced.state, CacheEntryState::Valid);
        assert_eq!(fenced.expire_at, 1000);
        // Only the not-yet-due row remains in the index.
        let left = store.cache_scan_expiry(1_000_000, None, 10).unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].key, "/t/c");
        assert!(store.cache_get_object(OBJ).unwrap().is_none());
        assert!(store.cache_get_object(OBJ + 1).unwrap().is_none());
        assert!(store.cache_get_object(OBJ + 3).unwrap().is_some());

        // Idempotent replay: every victim now meets a missing/stale row.
        mgr.apply_ttl_sweep(&store, 1500, &page).unwrap();
        assert_eq!(
            store.cache_get_entry(2, "/t/d").unwrap().unwrap().state,
            CacheEntryState::Valid
        );

        // Stale victim: the entry advanced past the victim generation —
        // the expiry cleanup stays a no-op and the row is untouched.
        mgr.apply_remove(&store, 1, "/t/c", 1, 2, OBJ + 2).unwrap();
        reopen_committed(&store, &mgr, 1, "/t/c", 2, OBJ + 12, 550, 0);
        mgr.apply_ttl_sweep(&store, 2500, &[expiry_row(2000, 1, OBJ + 2, "/t/c")])
            .unwrap();
        assert_eq!(
            store
                .cache_get_entry(1, "/t/c")
                .unwrap()
                .unwrap()
                .generation,
            3
        );

        // Identity divergence: generation matches (/t/c sits at Valid@3),
        // identity does not — loud, zero writes.
        assert!(mgr
            .apply_ttl_sweep(
                &store,
                2500,
                &[ExpiryRow {
                    expire_at: 1000,
                    incarnation: 1,
                    object_id: OBJ + 9,
                    key: "/t/c".into(),
                    generation: 3,
                }]
            )
            .is_err());

        // Illegal deadlines and unsorted pages are rejected before writes.
        assert!(mgr
            .apply_ttl_sweep(&store, 0, &[expiry_row(1, 1, OBJ, "/k")])
            .is_err());
        assert!(mgr
            .apply_ttl_sweep(&store, -5, &[expiry_row(1, 1, OBJ, "/k")])
            .is_err());
        assert!(mgr
            .apply_ttl_sweep(
                &store,
                1500,
                &[
                    expiry_row(2000, 1, OBJ + 2, "/t/c"),
                    expiry_row(1000, 1, OBJ, "/t/a"),
                ]
            )
            .is_err());
        let bomb: Vec<ExpiryRow> = (0..65)
            .map(|i| expiry_row(1000 + i, 1, OBJ + 100 + i, "/bomb"))
            .collect();
        assert!(mgr.apply_ttl_sweep(&store, 1_000_000, &bomb).is_err());
    }

    #[test]
    fn test_apply_vacuum_revoked_namespace() {
        let store = new_store("vacuum");
        let mgr = CacheManager::new();

        seed_committed(&store, &mgr, 5, 1, "/v/a", OBJ, 300, 5000);
        seed_committed(&store, &mgr, 5, 1, "/v/b", OBJ + 1, 400, 0);
        // Raced row: re-opened past the victim generation.
        seed_committed(&store, &mgr, 5, 1, "/v/c", OBJ + 2, 500, 0);
        mgr.apply_remove(&store, 1, "/v/c", 1, 2, OBJ + 2).unwrap();
        reopen_committed(&store, &mgr, 1, "/v/c", 2, OBJ + 12, 550, 0);

        // Gate-3 failures, loud, before any write: wrong mount, live
        // namespace, missing incarnation row.
        assert!(mgr
            .apply_vacuum(&store, 1, 4, &[vacuum_victim("/v/a", 1, OBJ, 5000)])
            .is_err());
        assert!(mgr
            .apply_vacuum(&store, 1, 5, &[vacuum_victim("/v/a", 1, OBJ, 5000)])
            .is_err());
        assert!(mgr
            .apply_vacuum(&store, 9, 5, &[vacuum_victim("/v/a", 1, OBJ, 5000)])
            .is_err());
        assert!(store.cache_get_entry(1, "/v/a").unwrap().is_some());

        // Revoke the namespace (pointer moves off it), then vacuum the
        // exact page: rows deleted WHOLE — no tombstone left behind.
        mgr.apply_incarnation_revoke(&store, 5, 1).unwrap();
        let victims = vec![
            vacuum_victim("/v/a", 1, OBJ, 5000),
            vacuum_victim("/v/b", 1, OBJ + 1, 0),
            vacuum_victim("/v/c", 1, OBJ + 2, 0),
        ];
        mgr.apply_vacuum(&store, 1, 5, &victims).unwrap();
        assert!(store.cache_get_entry(1, "/v/a").unwrap().is_none());
        assert!(store.cache_get_entry(1, "/v/b").unwrap().is_none());
        assert!(store.cache_get_object(OBJ).unwrap().is_none());
        assert!(store.cache_get_object(OBJ + 1).unwrap().is_none());
        assert!(store
            .cache_scan_expiry(1_000_000, None, 10)
            .unwrap()
            .is_empty());
        // The raced row (cur.generation 3 > victim 1) survives for the
        // next vacuum page.
        let raced = store.cache_get_entry(1, "/v/c").unwrap().unwrap();
        assert_eq!((raced.generation, raced.state), (3, CacheEntryState::Valid));

        // Replay is a no-op (every victim row is gone).
        mgr.apply_vacuum(&store, 1, 5, &victims).unwrap();

        // The raced row now vacuums at its current generation.
        mgr.apply_vacuum(&store, 1, 5, &[vacuum_victim("/v/c", 3, OBJ + 12, 0)])
            .unwrap();
        assert!(store.cache_get_entry(1, "/v/c").unwrap().is_none());

        // Identity mismatch and beyond-committed generation are loud.
        seed_committed(&store, &mgr, 6, 3, "/v/w", OBJ + 20, 100, 0);
        mgr.apply_incarnation_revoke(&store, 6, 3).unwrap();
        assert!(mgr
            .apply_vacuum(&store, 3, 6, &[vacuum_victim("/v/w", 1, OBJ + 99, 0)])
            .is_err());
        assert!(mgr
            .apply_vacuum(&store, 3, 6, &[vacuum_victim("/v/w", 5, OBJ + 20, 0)])
            .is_err());
        assert!(store.cache_get_entry(3, "/v/w").unwrap().is_some());

        // Page-cap bomb and unsorted pages rejected.
        let bomb: Vec<VacuumVictim> = (0..65)
            .map(|i| vacuum_victim(&format!("/v/{:04}", i), 1, OBJ + 100 + i, 0))
            .collect();
        assert!(mgr.apply_vacuum(&store, 3, 6, &bomb).is_err());
        assert!(mgr
            .apply_vacuum(
                &store,
                3,
                6,
                &[
                    vacuum_victim("/v/z", 1, OBJ + 30, 0),
                    vacuum_victim("/v/w", 1, OBJ + 20, 0),
                ]
            )
            .is_err());
    }

    #[test]
    fn test_apply_outcome_gc_frozen_fence() {
        let store = new_store("outcome-gc");
        let mgr = CacheManager::new();

        // A live namespace so the allocations record their outcomes.
        mgr.apply_incarnation_allocate_v2(
            &store,
            OpToken {
                client_id: 91,
                op_seq: 1,
            },
            7,
            1,
            0,
        )
        .unwrap();

        // Object ids must sit inside a durable reserve segment.
        mgr.apply_id_reserve(&store, token(1, 1), OBJ, OBJ + 100)
            .unwrap();

        // Client 11 executes five ops; its durable watermark is 5.
        for seq in 1..=5u64 {
            mgr.apply_allocate(
                &store,
                token(11, seq),
                1,
                &format!("/g/{}", seq),
                100,
                &reserved(1, OBJ + seq as i64),
            )
            .unwrap();
        }
        assert_eq!(store.cache_client_watermark(11).unwrap(), Some(5));
        assert!(store.cache_get_outcome(token(11, 1)).unwrap().is_some());

        // Frozen-fence GC below the observed watermark: outcomes 1..3
        // evicted, 4..5 retained, watermark never moved.
        mgr.apply_outcome_gc(
            &store,
            &[OutcomeGcGroup {
                client_id: 11,
                evict_below: 5,
                op_seqs: vec![1, 2, 3],
            }],
        )
        .unwrap();
        for seq in 1..=3u64 {
            assert!(store.cache_get_outcome(token(11, seq)).unwrap().is_none());
        }
        for seq in 4..=5u64 {
            assert!(store.cache_get_outcome(token(11, seq)).unwrap().is_some());
        }
        assert_eq!(store.cache_client_watermark(11).unwrap(), Some(5));

        // Idempotent replay: missing outcomes are no-ops.
        mgr.apply_outcome_gc(
            &store,
            &[OutcomeGcGroup {
                client_id: 11,
                evict_below: 5,
                op_seqs: vec![1, 2, 3],
            }],
        )
        .unwrap();
        assert!(store.cache_get_outcome(token(11, 4)).unwrap().is_some());

        // Fence legality: an evict_below above the durable watermark is an
        // illegal entry — loud, and the whole batch leaves zero writes
        // (token 11:4 survives a rejected batch that also lists it).
        assert!(mgr
            .apply_outcome_gc(
                &store,
                &[OutcomeGcGroup {
                    client_id: 11,
                    evict_below: 99,
                    op_seqs: vec![4],
                }]
            )
            .is_err());
        assert!(store.cache_get_outcome(token(11, 4)).unwrap().is_some());
        // A client with no watermark at all is equally illegal.
        assert!(mgr
            .apply_outcome_gc(
                &store,
                &[OutcomeGcGroup {
                    client_id: 12,
                    evict_below: 2,
                    op_seqs: vec![1],
                }]
            )
            .is_err());

        // Structural rejections (journal bombs): empty group, seq 0, seq at
        // or above the fence, unsorted seqs, duplicate clients, total above
        // the page cap.
        assert!(mgr
            .apply_outcome_gc(
                &store,
                &[OutcomeGcGroup {
                    client_id: 11,
                    evict_below: 5,
                    op_seqs: vec![],
                }]
            )
            .is_err());
        assert!(mgr
            .apply_outcome_gc(
                &store,
                &[OutcomeGcGroup {
                    client_id: 11,
                    evict_below: 5,
                    op_seqs: vec![4, 0],
                }]
            )
            .is_err());
        assert!(mgr
            .apply_outcome_gc(
                &store,
                &[OutcomeGcGroup {
                    client_id: 11,
                    evict_below: 5,
                    op_seqs: vec![4, 5],
                }]
            )
            .is_err());
        assert!(mgr
            .apply_outcome_gc(
                &store,
                &[OutcomeGcGroup {
                    client_id: 11,
                    evict_below: 5,
                    op_seqs: vec![2, 1],
                }]
            )
            .is_err());
        assert!(mgr
            .apply_outcome_gc(
                &store,
                &[
                    OutcomeGcGroup {
                        client_id: 11,
                        evict_below: 5,
                        op_seqs: vec![4],
                    },
                    OutcomeGcGroup {
                        client_id: 11,
                        evict_below: 5,
                        op_seqs: vec![5],
                    },
                ]
            )
            .is_err());
        // Total op_seqs above the page cap: 65 seqs under one fence.
        {
            let mut w = store.cache_write();
            w.set_client_watermark(13, 1_000).unwrap();
            w.commit().unwrap();
        }
        let seqs: Vec<u64> = (1..=65u64).collect();
        assert!(mgr
            .apply_outcome_gc(
                &store,
                &[OutcomeGcGroup {
                    client_id: 13,
                    evict_below: 1_000,
                    op_seqs: seqs,
                }]
            )
            .is_err());
        assert!(mgr.apply_outcome_gc(&store, &[]).is_err());

        // Multi-group batch across ascending clients in one bounded entry.
        for seq in 1..=2u64 {
            mgr.apply_allocate(
                &store,
                token(21, seq),
                1,
                &format!("/h/{}", seq),
                100,
                &reserved(1, OBJ + 50 + seq as i64),
            )
            .unwrap();
        }
        assert_eq!(store.cache_client_watermark(21).unwrap(), Some(2));
        mgr.apply_outcome_gc(
            &store,
            &[
                OutcomeGcGroup {
                    client_id: 11,
                    evict_below: 5,
                    op_seqs: vec![4],
                },
                OutcomeGcGroup {
                    client_id: 21,
                    evict_below: 2,
                    op_seqs: vec![1],
                },
            ],
        )
        .unwrap();
        assert!(store.cache_get_outcome(token(11, 4)).unwrap().is_none());
        assert!(store.cache_get_outcome(token(21, 1)).unwrap().is_none());
        assert!(store.cache_get_outcome(token(21, 2)).unwrap().is_some());
        assert_eq!(store.cache_client_watermark(21).unwrap(), Some(2));
    }

    /// Byte-comparable projection of every durable cache-mode row family
    /// the 4c.2 mutations touch (entries, expiry index, outcomes,
    /// incarnation rows, mount pointers, client watermarks, reverse rows).
    fn dump_state(store: &RocksInodeStore) -> String {
        let mut out = String::new();
        for incarnation in [1u64, 2, 3] {
            for (key, e) in store.cache_scan_entries(incarnation, None, 100).unwrap() {
                out.push_str(&format!(
                    "E {} {} {:?} {} {} {} {} {}\n",
                    incarnation,
                    key,
                    e.state,
                    e.generation,
                    e.object_id,
                    e.len,
                    e.expire_at,
                    e.block_size
                ));
            }
        }
        for r in store.cache_scan_expiry(1_000_000, None, 100).unwrap() {
            out.push_str(&format!(
                "X {} {} {} {} {}\n",
                r.expire_at, r.incarnation, r.object_id, r.key, r.generation
            ));
        }
        for t in store.cache_scan_outcomes(None, 100).unwrap() {
            out.push_str(&format!(
                "O {} {} {:?}\n",
                t.client_id,
                t.op_seq,
                store.cache_get_outcome(t).unwrap()
            ));
        }
        for incarnation in [1u64, 2, 3] {
            if let Some(row) = store.cache_get_incarnation(incarnation).unwrap() {
                out.push_str(&format!(
                    "I {} {} {}\n",
                    incarnation, row.mount_id, row.revoked
                ));
            }
        }
        for mount in [5u32, 6] {
            out.push_str(&format!(
                "P {} {:?}\n",
                mount,
                store.cache_current_incarnation(mount).unwrap()
            ));
        }
        for client in [1u64, 21, 91] {
            out.push_str(&format!(
                "W {} {:?}\n",
                client,
                store.cache_client_watermark(client).unwrap()
            ));
        }
        for object in OBJ..OBJ + 12 {
            if let Some(row) = store.cache_get_object(object).unwrap() {
                out.push_str(&format!(
                    "R {} {} {} {}\n",
                    object, row.incarnation, row.key, row.generation
                ));
            }
        }
        out
    }

    type ReplayStep<'a> = Box<dyn Fn(&CacheManager, &RocksInodeStore) -> CommonResult<()> + 'a>;

    /// Fault/replay gate (4c.2): a paged mutation segment — with a
    /// mid-segment page replayed (leader restart between propose and
    /// ack) — must converge byte-identically whether the journal replays
    /// once, twice, or partially-overlapped.
    #[test]
    fn test_mutation_journal_replay_double_run_converges() {
        // Journal segment (built exactly as the leader drivers would page
        // it): scope-remove pages over /j, a mid-segment page replay,
        // a TTL sweep page, a vacuum page for the revoked namespace, and
        // outcome-GC groups (with their own replay).
        let scope_page1: Vec<ScopeRemoveVictim> = vec![
            scope_victim("/j/a", 1, OBJ, 1000),
            scope_victim("/j/b", 1, OBJ + 1, 1000),
        ];
        let scope_page2: Vec<ScopeRemoveVictim> = vec![
            scope_victim("/j/c", 1, OBJ + 2, 1000),
            scope_victim("/j/d", 1, OBJ + 3, 1000),
            scope_victim("/j/e", 1, OBJ + 4, 1000),
        ];
        let ttl_page: Vec<ExpiryRow> = vec![
            expiry_row(1000, 1, OBJ + 5, "/k/a"),
            expiry_row(1000, 1, OBJ + 6, "/k/b"),
        ];
        let vacuum_page: Vec<VacuumVictim> = vec![vacuum_victim("/j2/x", 1, OBJ + 7, 1000)];
        let gc_groups: Vec<OutcomeGcGroup> = vec![OutcomeGcGroup {
            client_id: 21,
            evict_below: 3,
            op_seqs: vec![1, 2],
        }];
        let journal: Vec<ReplayStep> = vec![
            Box::new(|mgr, store| mgr.apply_scope_remove(store, 1, "/j", &scope_page1)),
            Box::new(|mgr, store| mgr.apply_scope_remove(store, 1, "/j", &scope_page1)),
            Box::new(|mgr, store| mgr.apply_scope_remove(store, 1, "/j", &scope_page2)),
            Box::new(|mgr, store| mgr.apply_ttl_sweep(store, 2000, &ttl_page)),
            Box::new(|mgr, store| mgr.apply_vacuum(store, 2, 6, &vacuum_page)),
            Box::new(|mgr, store| mgr.apply_outcome_gc(store, &gc_groups)),
            Box::new(|mgr, store| mgr.apply_outcome_gc(store, &gc_groups)),
        ];

        let seed = |store: &RocksInodeStore, mgr: &CacheManager| {
            for (i, key) in ["/j/a", "/j/b", "/j/c", "/j/d", "/j/e"].iter().enumerate() {
                seed_committed(store, mgr, 5, 1, key, OBJ + i as i64, 100, 1000);
            }
            seed_committed(store, mgr, 5, 1, "/k/a", OBJ + 5, 100, 1000);
            seed_committed(store, mgr, 5, 1, "/k/b", OBJ + 6, 100, 1000);
            seed_committed(store, mgr, 6, 2, "/j2/x", OBJ + 7, 100, 1000);
            mgr.apply_incarnation_revoke(store, 6, 2).unwrap();
            // Client 21's op history (watermark 3) for outcome GC.
            for seq in 1..=3u64 {
                mgr.apply_allocate(
                    store,
                    OpToken {
                        client_id: 21,
                        op_seq: seq,
                    },
                    1,
                    &format!("/g/{}", seq),
                    100,
                    &reserved(1, OBJ + 20 + seq as i64),
                )
                .unwrap();
            }
        };

        // Run A: single replay.
        let store_a = new_store("replay-a");
        let mgr_a = CacheManager::new();
        seed(&store_a, &mgr_a);
        for step in &journal {
            step(&mgr_a, &store_a).unwrap();
        }

        // Run B: full double replay.
        let store_b = new_store("replay-b");
        let mgr_b = CacheManager::new();
        seed(&store_b, &mgr_b);
        for _ in 0..2 {
            for step in &journal {
                step(&mgr_b, &store_b).unwrap();
            }
        }

        // Run C: interrupted leader restart — entries 0..=1 applied, then
        // the whole segment replays over them (overlap), then continues.
        let store_c = new_store("replay-c");
        let mgr_c = CacheManager::new();
        seed(&store_c, &mgr_c);
        for step in &journal[..2] {
            step(&mgr_c, &store_c).unwrap();
        }
        for step in &journal {
            step(&mgr_c, &store_c).unwrap();
        }

        let dump_a = dump_state(&store_a);
        let dump_b = dump_state(&store_b);
        let dump_c = dump_state(&store_c);
        assert_eq!(dump_a, dump_b, "double replay diverged");
        assert_eq!(dump_a, dump_c, "overlapped restart replay diverged");

        // The converged end state is the expected projection: /j fully
        // tombstoned, /k swept, namespace 2 vacuumed empty, outcomes 21:1
        // and 21:2 evicted under the frozen fence.
        assert!(dump_a.contains("E 1 /j/a Tombstoned 2"));
        assert!(dump_a.contains("E 1 /k/a Tombstoned 2"));
        assert!(!dump_a.contains("E 2 "));
        assert!(!dump_a.contains("O 21 1"));
        assert!(!dump_a.contains("O 21 2"));
        assert!(dump_a.contains("O 21 3"));
        assert!(dump_a.contains("W 21 Some(3)"));
    }

    /// Review `303fb807` P0-3: a scope batch may never name keys outside
    /// its scope, and the Tombstoned@new_generation replay branch accepts
    /// ONLY this victim's exact tombstone (same object, no expiry) — a
    /// forged same-generation tombstone of a different object is loud.
    #[test]
    fn test_scope_remove_membership_and_exact_tombstone_replay() {
        let store = new_store("scope-membership");
        let mgr = CacheManager::new();

        seed_committed(&store, &mgr, 5, 1, "/a/x", OBJ, 300, 5000);
        seed_committed(&store, &mgr, 5, 1, "/b/z", OBJ + 1, 400, 6000);

        // Cross-scope victim: loud, zero writes (the /b/z entry and its
        // expiry row both survive untouched).
        assert!(mgr
            .apply_scope_remove(&store, 1, "/a", &[scope_victim("/b/z", 1, OBJ + 1, 6000)])
            .is_err());
        assert_eq!(
            store.cache_get_entry(1, "/b/z").unwrap().unwrap().state,
            CacheEntryState::Valid
        );
        assert_eq!(
            store.cache_scan_expiry(1_000_000, None, 10).unwrap().len(),
            2
        );

        // Real page applies; the row becomes Tombstoned@2 for object OBJ.
        mgr.apply_scope_remove(&store, 1, "/a", &[scope_victim("/a/x", 1, OBJ, 5000)])
            .unwrap();

        // Forged replay: same key/generations but a different object at
        // the tombstone position — loud divergence, zero writes.
        assert!(mgr
            .apply_scope_remove(&store, 1, "/a", &[scope_victim("/a/x", 1, OBJ + 9, 5000)])
            .is_err());
        let e = store.cache_get_entry(1, "/a/x").unwrap().unwrap();
        assert_eq!(
            (e.state, e.generation, e.object_id),
            (CacheEntryState::Tombstoned, 2, OBJ)
        );

        // Exact replay stays the idempotent no-op.
        mgr.apply_scope_remove(&store, 1, "/a", &[scope_victim("/a/x", 1, OBJ, 5000)])
            .unwrap();
        assert_eq!(
            store
                .cache_get_entry(1, "/a/x")
                .unwrap()
                .unwrap()
                .generation,
            2
        );
    }

    /// Review `303fb807` P0-2: a future deadline may never be swept early,
    /// and a victim generation BEYOND the committed row is an illegal
    /// entry — both loud with the whole batch leaving zero writes (the
    /// staged expiry delete included).
    #[test]
    fn test_ttl_sweep_rejects_future_deadline_and_future_generation() {
        let store = new_store("ttl-illegal");
        let mgr = CacheManager::new();

        seed_committed(&store, &mgr, 5, 1, "/f/a", OBJ, 300, 1000);

        // Future deadline: reject before any write.
        assert!(mgr
            .apply_ttl_sweep(&store, 500, &[expiry_row(1000, 1, OBJ, "/f/a")])
            .is_err());
        assert_eq!(
            store.cache_get_entry(1, "/f/a").unwrap().unwrap().state,
            CacheEntryState::Valid
        );
        assert_eq!(
            store.cache_scan_expiry(1_000_000, None, 10).unwrap().len(),
            1
        );

        // Future-generation victim (gen 5 vs committed gen 1): loud, and
        // the victim's own expiry row must still be there afterwards.
        assert!(mgr
            .apply_ttl_sweep(
                &store,
                2000,
                &[ExpiryRow {
                    expire_at: 1000,
                    incarnation: 1,
                    object_id: OBJ,
                    key: "/f/a".into(),
                    generation: 5,
                }]
            )
            .is_err());
        assert_eq!(
            store.cache_get_entry(1, "/f/a").unwrap().unwrap().state,
            CacheEntryState::Valid
        );
        let left = store.cache_scan_expiry(1_000_000, None, 10).unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!((left[0].key.as_str(), left[0].generation), ("/f/a", 1));
    }

    /// Review `303fb807` bounded gate: the victim COUNT cap is not a byte
    /// bound — scope and every victim key are hard-capped at
    /// `MAX_CACHE_KEY_BYTES` at the apply boundary (4096 passes
    /// validation as a missing row no-op; 4097 is loud), across all three
    /// key-carrying variants.
    #[test]
    fn test_mutation_key_and_scope_byte_caps() {
        let store = new_store("byte-caps");
        let mgr = CacheManager::new();

        let wide_scope = format!("/{}", "a".repeat(MAX_CACHE_KEY_BYTES - 1));
        assert_eq!(wide_scope.len(), MAX_CACHE_KEY_BYTES);
        let too_wide_scope = format!("/{}", "a".repeat(MAX_CACHE_KEY_BYTES));

        let key_at_cap = format!("/a/{}", "k".repeat(MAX_CACHE_KEY_BYTES - 3));
        assert_eq!(key_at_cap.len(), MAX_CACHE_KEY_BYTES);
        let key_over_cap = format!("/a/{}", "k".repeat(MAX_CACHE_KEY_BYTES - 2));

        // Scope at/over the cap.
        assert!(mgr
            .apply_scope_remove(
                &store,
                1,
                &too_wide_scope,
                &[scope_victim(&wide_scope, 1, OBJ, 0)]
            )
            .is_err());
        // Scope at cap, victim key at cap (the scope's own path, which is
        // in-scope by definition): legal shape, missing row no-op.
        mgr.apply_scope_remove(
            &store,
            1,
            &wide_scope,
            &[scope_victim(&wide_scope, 1, OBJ, 0)],
        )
        .unwrap();
        // Victim key at cap under a normal scope: legal shape no-op;
        // over the cap: loud.
        mgr.apply_scope_remove(&store, 1, "/a", &[scope_victim(&key_at_cap, 1, OBJ, 0)])
            .unwrap();
        assert!(mgr
            .apply_scope_remove(&store, 1, "/a", &[scope_victim(&key_over_cap, 1, OBJ, 0)])
            .is_err());

        // TTL and vacuum victim keys carry the same bound.
        assert!(mgr
            .apply_ttl_sweep(
                &store,
                1000,
                &[ExpiryRow {
                    expire_at: 1,
                    incarnation: 1,
                    object_id: OBJ,
                    key: key_over_cap.clone(),
                    generation: 1,
                }]
            )
            .is_err());
        assert!(mgr
            .apply_vacuum(&store, 1, 5, &[vacuum_victim(&key_over_cap, 1, OBJ, 0)])
            .is_err());
    }
}
