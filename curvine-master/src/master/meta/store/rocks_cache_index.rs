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

//! RocksDB implementation of the [`LocalCacheIndexStore`] boundary, backed by
//! the master's shared `RocksInodeStore` engine so checkpoints and restores
//! capture the cache column families for free (contract §4/§5).

use crate::master::meta::cache::entry::{
    decode_key, encode_key, key_in_scope, validate_entry, validate_expiry_row,
    validate_incarnation, validate_object_row, CacheEntry, ExpiryCursor, ExpiryRow,
    IncarnationPolicyRow, IncarnationRow, ObjectRow, OpOutcome, OpToken,
    MAX_ALLOCATABLE_INCARNATION,
};
use crate::master::meta::cache::store::validate_scan_limit;
use crate::master::meta::cache::store::CacheWrite;
use crate::master::meta::cache::store::LocalCacheIndexStore;
use crate::master::meta::cache::BlockIdCodec;
use crate::master::meta::store::RocksInodeStore;
use curvine_core_error::{err_msg, CommonError};
use curvine_error::FsResult;
use curvine_rocksdb::{RocksUtils, WriteBatchWithTransaction};
use curvine_runtime::common::SerdeUtils as Serde;
use std::collections::HashMap;

type RocksError = curvine_rocksdb::Error;

fn rocks_err(e: RocksError) -> curvine_error::FsError {
    FsError::from(CommonError::from(err_msg!(
        "rocksdb cache index error: {}",
        e
    )))
}

/// Big-endian +1 upper bound for a full-incarnation range scan. Every
/// creating write entry validates `incarnation <= MAX_ALLOCATABLE_INCARNATION`
/// (`u64::MAX` is reserved exactly so this bound always exists), so the
/// `+1` prefix cannot collide with the incarnation itself.
fn incarnation_range_end(incarnation: u64) -> FsResult<Vec<u8>> {
    if incarnation > MAX_ALLOCATABLE_INCARNATION {
        return Err(FsError::from(CommonError::from(err_msg!(
            "incarnation {} is reserved and cannot be scanned",
            incarnation
        ))));
    }
    Ok(incarnation.checked_add(1).unwrap().to_be_bytes().to_vec())
}

impl RocksInodeStore {
    pub const CF_CACHE_ENTRY: &'static str = "cache_entry";
    pub const CF_CACHE_OBJECT: &'static str = "cache_object";
    pub const CF_CACHE_EXPIRY: &'static str = "cache_expiry";
    pub const CF_CACHE_IDEMPOTENCY: &'static str = "cache_idempotency";
    pub const CF_CACHE_MOUNT: &'static str = "cache_mount";
    pub const CF_CACHE_STATE: &'static str = "cache_state";

    pub const IDEMPOTENCY_TAG_OUTCOME: u8 = 0x01;
    pub const IDEMPOTENCY_TAG_WATERMARK: u8 = 0x02;
    pub const MOUNT_TAG_CURRENT: u8 = 0x01;
    pub const MOUNT_TAG_INCARNATION: u8 = 0x02;
    /// 4b option A: policy snapshot under a separate key so the legacy
    /// IncarnationRow bytes stay decodable.
    pub const MOUNT_TAG_POLICY: u8 = 0x03;

    fn entry_key(incarnation: u64, key: &str) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + key.len());
        buf.extend_from_slice(&incarnation.to_be_bytes());
        buf.extend_from_slice(&encode_key(key));
        buf
    }

    /// Ordered expiry index position — the layout frozen at 4a:
    /// `(expire_at, incarnation, object_id)` as three big-endian fixed
    /// fields. Deterministic order gives stable same-timestamp paging by
    /// `(incarnation, object_id)`.
    fn expiry_key(expire_at: i64, incarnation: u64, object_id: i64) -> [u8; 24] {
        let mut key = [0u8; 24];
        key[..8].copy_from_slice(&expire_at.to_be_bytes());
        key[8..16].copy_from_slice(&incarnation.to_be_bytes());
        key[16..24].copy_from_slice(&object_id.to_be_bytes());
        key
    }

    /// Point-read the committed expiry index row at the frozen
    /// `(expire_at, incarnation, object_id)` position as
    /// `(key, generation)`.
    fn expiry_row_at(
        &self,
        expire_at: i64,
        incarnation: u64,
        object_id: i64,
    ) -> FsResult<Option<(String, u64)>> {
        let ck = Self::expiry_key(expire_at, incarnation, object_id);
        match self.db.get_cf(Self::CF_CACHE_EXPIRY, ck)? {
            None => Ok(None),
            Some(bytes) => Ok(Some(Serde::deserialize(&bytes)?)),
        }
    }

    fn outcome_key(token: &OpToken) -> [u8; 17] {
        let mut key = [0u8; 17];
        key[0] = Self::IDEMPOTENCY_TAG_OUTCOME;
        key[1..9].copy_from_slice(&token.client_id.to_be_bytes());
        key[9..17].copy_from_slice(&token.op_seq.to_be_bytes());
        key
    }

    fn watermark_key(client_id: u64) -> [u8; 9] {
        let mut key = [0u8; 9];
        key[0] = Self::IDEMPOTENCY_TAG_WATERMARK;
        key[1..9].copy_from_slice(&client_id.to_be_bytes());
        key
    }

    fn current_incarnation_key(mount_id: u32) -> [u8; 5] {
        let mut key = [0u8; 5];
        key[0] = Self::MOUNT_TAG_CURRENT;
        key[1..5].copy_from_slice(&mount_id.to_be_bytes());
        key
    }

    fn incarnation_row_key(incarnation: u64) -> [u8; 9] {
        let mut key = [0u8; 9];
        key[0] = Self::MOUNT_TAG_INCARNATION;
        key[1..9].copy_from_slice(&incarnation.to_be_bytes());
        key
    }

    fn incarnation_policy_key(incarnation: u64) -> [u8; 9] {
        let mut key = [0u8; 9];
        key[0] = Self::MOUNT_TAG_POLICY;
        key[1..9].copy_from_slice(&incarnation.to_be_bytes());
        key
    }
}

pub struct RocksCacheWrite<'a> {
    /// Single-writer precondition: the committed apply path (CacheManager)
    /// is the only creator of these rows; concurrent uncoordinated writers
    /// on the same keys are not supported.
    db: &'a RocksInodeStore,
    batch: WriteBatchWithTransaction<false>,
    /// Max watermark staged in this batch, per client. Monotonicity must
    /// hold against values staged earlier in the *same* batch, not just the
    /// last committed value.
    staged_watermarks: HashMap<u64, u64>,
    /// Max state watermark staged in this batch, per tag.
    staged_states: HashMap<String, i64>,
}

impl RocksCacheWrite<'_> {
    fn put_cf<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        cf: &str,
        key: K,
        value: V,
    ) -> FsResult<()> {
        let handle = self.db.db.cf(cf)?;
        self.batch.put_cf(handle, key, value);
        Ok(())
    }

    fn delete_cf<K: AsRef<[u8]>>(&mut self, cf: &str, key: K) -> FsResult<()> {
        let handle = self.db.db.cf(cf)?;
        self.batch.delete_cf(handle, key);
        Ok(())
    }
}

impl CacheWrite for RocksCacheWrite<'_> {
    fn put_entry(&mut self, incarnation: u64, key: &str, entry: &CacheEntry) -> FsResult<()> {
        // The authoritative write boundary enforces entry invariants; no
        // caller (journal, replay, service) can persist a malformed row.
        validate_incarnation(incarnation).map_err(FsError::from)?;
        validate_entry(entry)?;
        let ck = RocksInodeStore::entry_key(incarnation, key);
        let value = Serde::serialize(entry)?;
        self.put_cf(RocksInodeStore::CF_CACHE_ENTRY, ck, value)
    }

    fn delete_entry(&mut self, incarnation: u64, key: &str) -> FsResult<()> {
        let ck = RocksInodeStore::entry_key(incarnation, key);
        self.delete_cf(RocksInodeStore::CF_CACHE_ENTRY, ck)
    }

    fn put_object(&mut self, object_id: i64, row: &ObjectRow) -> FsResult<()> {
        validate_object_row(object_id, row)?;
        validate_incarnation(row.incarnation).map_err(FsError::from)?;
        let value = Serde::serialize(row)?;
        self.put_cf(
            RocksInodeStore::CF_CACHE_OBJECT,
            RocksUtils::i64_to_bytes(object_id),
            value,
        )
    }

    fn delete_object(&mut self, object_id: i64) -> FsResult<()> {
        self.delete_cf(
            RocksInodeStore::CF_CACHE_OBJECT,
            RocksUtils::i64_to_bytes(object_id),
        )
    }

    fn put_expiry(&mut self, row: &ExpiryRow) -> FsResult<()> {
        validate_expiry_row(row)?;
        validate_incarnation(row.incarnation).map_err(FsError::from)?;
        // Exact-identity CAS (4c.1): the frozen position is uniquely
        // bound to one (key, generation) in legal history — an object id
        // never advances generations. An existing committed row must be a
        // bit-exact identity match (idempotent replay); any key or
        // generation difference, in either direction, is divergence and
        // fails loudly. The guard reads committed state only — the
        // single-writer apply path never stages two writes to one
        // position in a batch.
        if let Some((key, gen)) =
            self.db
                .expiry_row_at(row.expire_at, row.incarnation, row.object_id)?
        {
            if key == row.key && gen == row.generation {
                return Ok(());
            }
            return Err(FsError::from(CommonError::from(err_msg!(
                "cache expiry identity divergence at ({}, {}, {}): committed ({}, {}) vs write ({}, {})",
                row.expire_at,
                row.incarnation,
                row.object_id,
                key,
                gen,
                row.key,
                row.generation
            ))));
        }
        let ck = RocksInodeStore::expiry_key(row.expire_at, row.incarnation, row.object_id);
        let value = Serde::serialize(&(row.key.clone(), row.generation))?;
        self.put_cf(RocksInodeStore::CF_CACHE_EXPIRY, ck, value)
    }

    fn delete_expiry(
        &mut self,
        expire_at: i64,
        incarnation: u64,
        object_id: i64,
        key: &str,
        generation: u64,
    ) -> FsResult<()> {
        // Exact-identity CAS delete (4c.1): only the expected
        // (key, generation) at the frozen position may be removed; any
        // mismatch is divergence; a missing row is an idempotent no-op.
        match self.db.expiry_row_at(expire_at, incarnation, object_id)? {
            None => Ok(()),
            Some((committed_key, committed_gen)) => {
                if committed_key != key || committed_gen != generation {
                    return Err(FsError::from(CommonError::from(err_msg!(
                        "cache expiry identity divergence at ({}, {}, {}): committed ({}, {}) vs delete ({}, {})",
                        expire_at,
                        incarnation,
                        object_id,
                        committed_key,
                        committed_gen,
                        key,
                        generation
                    ))));
                }
                let ck = RocksInodeStore::expiry_key(expire_at, incarnation, object_id);
                self.delete_cf(RocksInodeStore::CF_CACHE_EXPIRY, ck)
            }
        }
    }

    fn put_incarnation(&mut self, incarnation: u64, row: IncarnationRow) -> FsResult<()> {
        validate_incarnation(incarnation).map_err(FsError::from)?;
        let value = Serde::serialize(&row)?;
        self.put_cf(
            RocksInodeStore::CF_CACHE_MOUNT,
            RocksInodeStore::incarnation_row_key(incarnation),
            value,
        )
    }

    /// 4b option A: policy snapshot under a separate key, written once with
    /// the allocation and never mutated (readers treat a missing row as
    /// `ttl_ms == 0`, i.e. pre-4b allocations had no TTL).
    fn put_incarnation_policy(
        &mut self,
        incarnation: u64,
        row: IncarnationPolicyRow,
    ) -> FsResult<()> {
        validate_incarnation(incarnation).map_err(FsError::from)?;
        if row.ttl_ms < 0 {
            return Err(FsError::from(CommonError::from(err_msg!(
                "mount ttl_ms must be non-negative: {}",
                row.ttl_ms
            ))));
        }
        let value = Serde::serialize(&row)?;
        self.put_cf(
            RocksInodeStore::CF_CACHE_MOUNT,
            RocksInodeStore::incarnation_policy_key(incarnation),
            value,
        )
    }

    fn set_current_incarnation(&mut self, mount_id: u32, incarnation: u64) -> FsResult<()> {
        validate_incarnation(incarnation).map_err(FsError::from)?;
        let value = Serde::serialize(&incarnation)?;
        self.put_cf(
            RocksInodeStore::CF_CACHE_MOUNT,
            RocksInodeStore::current_incarnation_key(mount_id),
            value,
        )
    }

    fn clear_current_incarnation(&mut self, mount_id: u32) -> FsResult<()> {
        self.delete_cf(
            RocksInodeStore::CF_CACHE_MOUNT,
            RocksInodeStore::current_incarnation_key(mount_id),
        )
    }

    fn put_outcome(&mut self, token: OpToken, outcome: &OpOutcome) -> FsResult<()> {
        let value = Serde::serialize(outcome)?;
        self.put_cf(
            RocksInodeStore::CF_CACHE_IDEMPOTENCY,
            RocksInodeStore::outcome_key(&token),
            value,
        )
    }

    fn delete_outcome(&mut self, token: OpToken) -> FsResult<()> {
        self.delete_cf(
            RocksInodeStore::CF_CACHE_IDEMPOTENCY,
            RocksInodeStore::outcome_key(&token),
        )
    }

    fn set_client_watermark(&mut self, client_id: u64, op_seq: u64) -> FsResult<()> {
        // Monotonic against BOTH the last committed value and values staged
        // earlier in this same batch: a regressing journal replay cannot
        // move the watermark backwards, and a later smaller set inside one
        // batch cannot override a larger staged value. (Staged put ordering
        // makes the largest staged value the last write in the batch.)
        let persisted = self.db.cache_client_watermark(client_id)?;
        let staged = self.staged_watermarks.get(&client_id).copied();
        let current = persisted.max(staged);
        if let Some(c) = current {
            if op_seq <= c {
                return Ok(());
            }
        }
        self.staged_watermarks.insert(client_id, op_seq);
        let value = Serde::serialize(&op_seq)?;
        self.put_cf(
            RocksInodeStore::CF_CACHE_IDEMPOTENCY,
            RocksInodeStore::watermark_key(client_id),
            value,
        )
    }

    fn set_state(&mut self, tag: &str, value: i64) -> FsResult<()> {
        let persisted = self.db.cache_get_state(tag)?;
        let staged = self.staged_states.get(tag).copied();
        let current = persisted.max(staged);
        if let Some(c) = current {
            if value <= c {
                return Ok(());
            }
        }
        self.staged_states.insert(tag.to_string(), value);
        self.put_cf(
            RocksInodeStore::CF_CACHE_STATE,
            tag.as_bytes(),
            RocksUtils::i64_to_bytes(value),
        )
    }

    fn commit(self) -> FsResult<()> {
        // Cache identity writes span multiple CFs (state/entry/reverse/
        // outcome/client watermark) in one batch, and the barrier ACK
        // happens right after this commit. The meta DB defaults to
        // disable_wal, which is not crash-atomic across CFs and can lose
        // the whole batch on abrupt exit — so cache commits override the
        // DB default with a per-write WAL + fsync (all-or-nothing).
        self.db.db.write_batch_durable(self.batch)?;
        Ok(())
    }
}

use curvine_error::FsError;

impl LocalCacheIndexStore for RocksInodeStore {
    type Write<'a> = RocksCacheWrite<'a>;

    fn cache_get_entry(&self, incarnation: u64, key: &str) -> FsResult<Option<CacheEntry>> {
        let ck = Self::entry_key(incarnation, key);
        match self.db.get_cf(Self::CF_CACHE_ENTRY, ck)? {
            None => Ok(None),
            Some(bytes) => Ok(Some(Serde::deserialize(&bytes)?)),
        }
    }

    fn cache_get_object(&self, object_id: i64) -> FsResult<Option<ObjectRow>> {
        match self
            .db
            .get_cf(Self::CF_CACHE_OBJECT, RocksUtils::i64_to_bytes(object_id))?
        {
            None => Ok(None),
            Some(bytes) => Ok(Some(Serde::deserialize(&bytes)?)),
        }
    }

    fn cache_scan_expiry(
        &self,
        now: i64,
        after: Option<&ExpiryCursor>,
        limit: usize,
    ) -> FsResult<Vec<ExpiryRow>> {
        validate_scan_limit(limit)?;
        if now < 0 {
            return Err(FsError::from(CommonError::from(err_msg!(
                "expiry scan instant must be non-negative: {}",
                now
            ))));
        }
        // Cursor validation at the boundary: a page cursor always comes
        // from a row this index produced, so it must carry a positive
        // deadline, an allocatable incarnation, and a cache-domain object.
        if let Some(c) = after {
            if c.expire_at <= 0 {
                return Err(FsError::from(CommonError::from(err_msg!(
                    "expiry scan cursor deadline must be positive: {}",
                    c.expire_at
                ))));
            }
            validate_incarnation(c.incarnation).map_err(FsError::from)?;
            if !BlockIdCodec::is_cache_owner(c.object_id) {
                return Err(FsError::from(CommonError::from(err_msg!(
                    "expiry scan cursor object id outside cache domain: {}",
                    c.object_id
                ))));
            }
        }
        // Range: [cursor_or_min, upper) where `upper` is exactly the 8
        // big-endian bytes of `now + 1` — expire_at is the first field and
        // every real row key is 24 bytes, so the exclusive upper bound
        // covers exactly the rows with expire_at <= now (a row at
        // expire_at == now + 1 sorts strictly after `upper` and is NOT due
        // yet). `now == i64::MAX` cannot express +1: the sentinel
        // MAX ++ 0xFF*16 is strictly after every real 24-byte key
        // (incarnation <= u64::MAX-1 keeps the 9th byte below 0xFF), so
        // the final deadline is still covered. All deadlines are positive
        // (`put_expiry` rejects <= 0), so signed big-endian byte order
        // equals deadline order.
        let start = after
            .map(|c| Self::expiry_key(c.expire_at, c.incarnation, c.object_id).to_vec())
            .unwrap_or_else(|| [0u8; 24].to_vec());
        let end = if now == i64::MAX {
            let mut e = [0xFFu8; 24];
            e[..8].copy_from_slice(&i64::MAX.to_be_bytes());
            e.to_vec()
        } else {
            (now + 1).to_be_bytes().to_vec()
        };

        let iter = self.db.range_scan(Self::CF_CACHE_EXPIRY, start, end)?;
        let mut rows = Vec::new();
        let mut skip_after = after;
        for item in iter {
            if rows.len() >= limit {
                break;
            }
            let (key, value) = item.map_err(rocks_err)?;
            if key.len() != 24 {
                return Err(FsError::from(CommonError::from(err_msg!(
                    "corrupt expiry row key length: {}",
                    key.len()
                ))));
            }
            let expire_at = RocksUtils::i64_from_bytes(&key[..8])?;
            let incarnation = RocksUtils::u64_from_bytes(&key[8..16])?;
            let object_id = RocksUtils::i64_from_bytes(&key[16..24])?;
            // Exclusive cursor: skip only the exact cursor row; later
            // object ids in the same (expire_at, incarnation) group still
            // surface.
            if let Some(c) = skip_after.take() {
                if expire_at == c.expire_at
                    && incarnation == c.incarnation
                    && object_id == c.object_id
                {
                    continue;
                }
            }
            let (key_str, generation): (String, u64) = Serde::deserialize(&value)?;
            rows.push(ExpiryRow {
                expire_at,
                incarnation,
                object_id,
                key: key_str,
                generation,
            });
        }
        Ok(rows)
    }

    fn cache_scan_entries(
        &self,
        incarnation: u64,
        after: Option<&str>,
        limit: usize,
    ) -> FsResult<Vec<(String, CacheEntry)>> {
        validate_scan_limit(limit)?;
        // Inclusive start at the continuation key (or the incarnation's
        // lowest key); the first yielded row equal to `after` is skipped.
        // Encoded keys are raw bytes (order-preserving), so the scan sees
        // incarnation-scoped rows in string key order.
        let start = Self::entry_key(incarnation, after.unwrap_or(""));
        let end = incarnation_range_end(incarnation)?;
        let iter = self.db.range_scan(Self::CF_CACHE_ENTRY, start, end)?;
        let mut rows = Vec::new();
        let mut skip_after = after.map(|a| a.to_string());
        for item in iter {
            if rows.len() >= limit {
                break;
            }
            let (key, value) = item.map_err(rocks_err)?;
            if key.len() < 8 {
                return Err(FsError::from(CommonError::from(err_msg!(
                    "corrupt cache entry key length: {}",
                    key.len()
                ))));
            }
            let raw_key = decode_key(&key[8..])?;
            if let Some(a) = skip_after.take() {
                if raw_key == a {
                    continue;
                }
            }
            let entry: CacheEntry = Serde::deserialize(&value)?;
            rows.push((raw_key, entry));
        }
        Ok(rows)
    }

    fn cache_scan_entries_in_scope(
        &self,
        incarnation: u64,
        scope: &str,
        after: Option<&str>,
        limit: usize,
    ) -> FsResult<Vec<(String, CacheEntry)>> {
        validate_scan_limit(limit)?;
        // Membership is judged ONLY by key_in_scope on decoded keys: the
        // encoded key of "/a" is a byte-prefix of "/ab", so RocksDB range
        // bounds can narrow iteration but must never decide membership.
        // A cursor outside the scope is rejected at the boundary: a page
        // cursor always comes from an earlier page of this same scoped
        // scan (membership is a property of the strings, so a cursor
        // whose row was deleted stays valid); accepting a foreign one
        // (e.g. `after` far below the scope) would let the loop skip an
        // unbounded unrelated key region without filling any page.
        if let Some(a) = after {
            if !key_in_scope(a, scope) {
                return Err(FsError::from(CommonError::from(err_msg!(
                    "scoped entry scan cursor {:?} is outside scope {:?}",
                    a,
                    scope
                ))));
            }
        }
        // The scope's smallest possible member is the exact scope key,
        // recovered by a point read (iteration yields strictly-after rows);
        // the family — every string with prefix "{scope}/" — is one
        // contiguous lexicographic interval, so iteration terminates at
        // the first decoded key that is out of scope AND sorts after the
        // family; no in-scope key can follow it. Keys that sort before
        // the family (e.g. "/a0" before "/a/") are simply skipped.
        let trimmed = scope.trim_end_matches('/');
        let family = format!("{}/", trimmed);
        let mut rows = Vec::new();
        let mut skip_after = after.map(|a| a.to_string());
        if after.is_none() && key_in_scope(trimmed, scope) {
            if let Some(e) = self.cache_get_entry(incarnation, trimmed)? {
                rows.push((trimmed.to_string(), e));
                // Defensive: if iteration still yields the seek target,
                // do not emit it twice.
                skip_after = Some(trimmed.to_string());
            }
        }
        let start = Self::entry_key(incarnation, after.unwrap_or(trimmed));
        let end = incarnation_range_end(incarnation)?;
        let iter = self.db.range_scan(Self::CF_CACHE_ENTRY, start, end)?;
        for item in iter {
            if rows.len() >= limit {
                break;
            }
            let (key, value) = item.map_err(rocks_err)?;
            if key.len() < 8 {
                return Err(FsError::from(CommonError::from(err_msg!(
                    "corrupt cache entry key length: {}",
                    key.len()
                ))));
            }
            let raw_key = decode_key(&key[8..])?;
            if !key_in_scope(&raw_key, scope) {
                if raw_key.as_bytes() > family.as_bytes() {
                    break;
                }
                continue;
            }
            if let Some(a) = skip_after.take() {
                if raw_key == a {
                    continue;
                }
            }
            let entry: CacheEntry = Serde::deserialize(&value)?;
            rows.push((raw_key, entry));
        }
        Ok(rows)
    }

    fn cache_get_incarnation(&self, incarnation: u64) -> FsResult<Option<IncarnationRow>> {
        match self
            .db
            .get_cf(Self::CF_CACHE_MOUNT, Self::incarnation_row_key(incarnation))?
        {
            None => Ok(None),
            Some(bytes) => Ok(Some(Serde::deserialize(&bytes)?)),
        }
    }

    fn cache_current_incarnation(&self, mount_id: u32) -> FsResult<Option<u64>> {
        match self.db.get_cf(
            Self::CF_CACHE_MOUNT,
            Self::current_incarnation_key(mount_id),
        )? {
            None => Ok(None),
            Some(bytes) => Ok(Some(Serde::deserialize(&bytes)?)),
        }
    }

    fn cache_get_incarnation_policy(
        &self,
        incarnation: u64,
    ) -> FsResult<Option<IncarnationPolicyRow>> {
        match self.db.get_cf(
            Self::CF_CACHE_MOUNT,
            Self::incarnation_policy_key(incarnation),
        )? {
            // Pre-4b allocations carry no policy row: no TTL.
            None => Ok(Some(IncarnationPolicyRow { ttl_ms: 0 })),
            Some(bytes) => Ok(Some(Serde::deserialize(&bytes)?)),
        }
    }

    fn cache_get_outcome(&self, token: OpToken) -> FsResult<Option<OpOutcome>> {
        match self
            .db
            .get_cf(Self::CF_CACHE_IDEMPOTENCY, Self::outcome_key(&token))?
        {
            None => Ok(None),
            Some(bytes) => Ok(Some(Serde::deserialize(&bytes)?)),
        }
    }

    fn cache_client_watermark(&self, client_id: u64) -> FsResult<Option<u64>> {
        match self
            .db
            .get_cf(Self::CF_CACHE_IDEMPOTENCY, Self::watermark_key(client_id))?
        {
            None => Ok(None),
            Some(bytes) => Ok(Some(Serde::deserialize(&bytes)?)),
        }
    }

    fn cache_get_state(&self, tag: &str) -> FsResult<Option<i64>> {
        match self.db.get_cf(Self::CF_CACHE_STATE, tag.as_bytes())? {
            None => Ok(None),
            Some(bytes) => Ok(Some(RocksUtils::i64_from_bytes(&bytes)?)),
        }
    }

    fn cache_write(&self) -> Self::Write<'_> {
        RocksCacheWrite {
            db: self,
            batch: WriteBatchWithTransaction::<false>::default(),
            staged_watermarks: HashMap::new(),
            staged_states: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::cache::entry::CacheEntryState;
    use crate::master::meta::cache::store::{state_tags, SCAN_HARD_CAP};
    use crate::master::meta::BlockIdCodec;
    use crate::master::Master;
    use curvine_rocksdb::DBConf;
    use curvine_runtime::common::Utils;

    fn new_store(name: &str) -> FsResult<RocksInodeStore> {
        Master::init_test_metrics();
        let conf = DBConf::new(Utils::test_sub_dir(format!(
            "cache-index-test/{}-{}",
            name,
            Utils::rand_str(6)
        )));
        Ok(RocksInodeStore::new(conf, true)?)
    }

    fn entry(gen: u64, object_id: i64, len: i64, block_size: i64, expire_at: i64) -> CacheEntry {
        CacheEntry {
            generation: gen,
            state: CacheEntryState::Valid,
            object_id,
            len,
            ufs_mtime: 42,
            block_size,
            expire_at,
        }
    }

    const OBJ: i64 = BlockIdCodec::CACHE_OBJECT_MIN;

    #[test]
    fn test_entry_put_get_delete() -> FsResult<()> {
        let store = new_store("entry-crud")?;
        assert!(store.cache_get_entry(7, "/a/b")?.is_none());

        let e = entry(1, OBJ, 300, 128, 0);
        let mut w = store.cache_write();
        w.put_entry(7, "/a/b", &e)?;
        w.commit()?;
        assert_eq!(store.cache_get_entry(7, "/a/b")?.as_ref(), Some(&e));

        // Generation overwrite in place: single row per key.
        let e2 = entry(2, OBJ + 1, 600, 128, 0);
        let mut w = store.cache_write();
        w.put_entry(7, "/a/b", &e2)?;
        w.commit()?;
        assert_eq!(store.cache_get_entry(7, "/a/b")?.as_ref(), Some(&e2));
        assert!(store.cache_get_entry(8, "/a/b")?.is_none());

        let mut w = store.cache_write();
        w.delete_entry(7, "/a/b")?;
        w.commit()?;
        assert!(store.cache_get_entry(7, "/a/b")?.is_none());
        Ok(())
    }

    #[test]
    fn test_batch_is_atomic() -> FsResult<()> {
        let store = new_store("batch-atomic")?;

        // An uncommitted batch writes nothing.
        {
            let mut w = store.cache_write();
            w.put_entry(1, "/k", &entry(1, OBJ, 10, 8, 0))?;
            w.put_object(
                OBJ,
                &ObjectRow {
                    incarnation: 1,
                    key: "/k".into(),
                    generation: 1,
                },
            )?;
            // no commit; dropped here
        }
        assert!(store.cache_get_entry(1, "/k")?.is_none());
        assert!(store.cache_get_object(OBJ)?.is_none());

        // A committed batch writes all CFs together.
        let mut w = store.cache_write();
        w.put_entry(1, "/k", &entry(1, OBJ, 10, 8, 0))?;
        w.put_object(
            OBJ,
            &ObjectRow {
                incarnation: 1,
                key: "/k".into(),
                generation: 1,
            },
        )?;
        w.commit()?;
        assert!(store.cache_get_entry(1, "/k")?.is_some());
        assert!(store.cache_get_object(OBJ)?.is_some());
        Ok(())
    }

    #[test]
    fn test_expiry_scan_ordered_and_bounded() -> FsResult<()> {
        let store = new_store("expiry-scan")?;
        let rows = [
            (500i64, 1u64, OBJ, "/c"),
            (100, 1, OBJ + 1, "/a"),
            (300, 2, OBJ + 2, "/b"),
        ];
        let mut w = store.cache_write();
        for (expire_at, incarnation, object_id, key) in rows {
            w.put_expiry(&ExpiryRow {
                expire_at,
                incarnation,
                object_id,
                key: key.into(),
                generation: 1,
            })?;
        }
        w.commit()?;

        // now=400 covers expire_at 100 and 300, ascending.
        let due = store.cache_scan_expiry(400, None, 10)?;
        assert_eq!(due.len(), 2);
        assert_eq!(due[0].expire_at, 100);
        assert_eq!(due[0].key, "/a");
        assert_eq!(due[1].expire_at, 300);
        assert_eq!(due[1].key, "/b");

        // The upper bound is exact: a row at expire_at == now + 1 is NOT
        // due yet (4c.1 fixes the old off-by-one that included it).
        assert_eq!(store.cache_scan_expiry(499, None, 10)?.len(), 2);
        assert_eq!(store.cache_scan_expiry(500, None, 10)?.len(), 3);

        // Limit is respected.
        assert_eq!(store.cache_scan_expiry(600, None, 10)?.len(), 3);
        assert_eq!(store.cache_scan_expiry(600, None, 2)?.len(), 2);

        // Deletion removes exactly one row.
        let mut w = store.cache_write();
        w.delete_expiry(100, 1, OBJ + 1, "/a", 1)?;
        w.commit()?;
        assert_eq!(store.cache_scan_expiry(600, None, 10)?.len(), 2);
        Ok(())
    }

    #[test]
    fn test_expiry_scan_cursor_same_timestamp_and_resume_after_delete() -> FsResult<()> {
        let store = new_store("expiry-cursor")?;
        // Five rows, four sharing expire_at 100: same-timestamp groups
        // must page deterministically by (incarnation, object_id) in the
        // frozen key order.
        let rows = [
            (100i64, 2u64, OBJ + 4, "/y"),
            (100, 1, OBJ, "/b"),
            (100, 2, OBJ + 3, "/x"),
            (100, 1, OBJ + 1, "/c"),
            (200, 1, OBJ + 2, "/z"),
        ];
        let mut w = store.cache_write();
        for (expire_at, incarnation, object_id, key) in rows {
            w.put_expiry(&ExpiryRow {
                expire_at,
                incarnation,
                object_id,
                key: key.into(),
                generation: 1,
            })?;
        }
        w.commit()?;

        let expect = ["/b", "/c", "/x", "/y", "/z"];

        // Full-order ground truth: deterministic
        // (expire, incarnation, object_id) order.
        let all = store.cache_scan_expiry(300, None, 10)?;
        assert_eq!(
            all.iter().map(|r| r.key.as_str()).collect::<Vec<_>>(),
            expect
        );

        // limit=1 chained pages via the exclusive cursor: no skips, no
        // duplicates, restartable at any point.
        let mut seen = Vec::new();
        let mut cursor: Option<ExpiryCursor> = None;
        loop {
            let page = store.cache_scan_expiry(300, cursor.as_ref(), 1)?;
            match page.last() {
                None => break,
                Some(row) => {
                    seen.push(row.key.clone());
                    cursor = Some(ExpiryCursor::from(row));
                }
            }
        }
        assert_eq!(seen, expect);

        // Cursor positioned exactly at a same-timestamp group member: the
        // rest of the group still surfaces (strictly-after semantics).
        let after_c = ExpiryCursor {
            expire_at: 100,
            incarnation: 1,
            object_id: OBJ + 1,
        };
        let page = store.cache_scan_expiry(300, Some(&after_c), 10)?;
        assert_eq!(
            page.iter().map(|r| r.key.as_str()).collect::<Vec<_>>(),
            ["/x", "/y", "/z"]
        );

        // Delete the cursor's own row, then resume from the same cursor:
        // no skips (its successors are untouched) and no duplicates.
        let mut w = store.cache_write();
        w.delete_expiry(100, 1, OBJ + 1, "/c", 1)?;
        w.commit()?;
        let page = store.cache_scan_expiry(300, Some(&after_c), 10)?;
        assert_eq!(
            page.iter().map(|r| r.key.as_str()).collect::<Vec<_>>(),
            ["/x", "/y", "/z"]
        );
        Ok(())
    }

    #[test]
    fn test_expiry_scan_max_deadline_and_cursor_validation() -> FsResult<()> {
        let store = new_store("expiry-max")?;
        let mut w = store.cache_write();
        w.put_expiry(&ExpiryRow {
            expire_at: i64::MAX,
            incarnation: 1,
            object_id: OBJ,
            key: "/max".into(),
            generation: 1,
        })?;
        w.put_expiry(&ExpiryRow {
            expire_at: i64::MAX - 1,
            incarnation: 1,
            object_id: OBJ + 1,
            key: "/prev".into(),
            generation: 1,
        })?;
        w.commit()?;

        // now == i64::MAX must still surface the MAX-deadline row (the
        // successor sentinel covers every positive deadline).
        let due = store.cache_scan_expiry(i64::MAX, None, 10)?;
        assert_eq!(
            due.iter().map(|r| r.key.as_str()).collect::<Vec<_>>(),
            ["/prev", "/max"]
        );
        // MAX-1 excludes the MAX row.
        let due = store.cache_scan_expiry(i64::MAX - 1, None, 10)?;
        assert_eq!(
            due.iter().map(|r| r.key.as_str()).collect::<Vec<_>>(),
            ["/prev"]
        );

        // Cursor validation: a foreign cursor is rejected at the boundary
        // before any iteration.
        assert!(store
            .cache_scan_expiry(
                10,
                Some(&ExpiryCursor {
                    expire_at: 0,
                    incarnation: 1,
                    object_id: OBJ
                }),
                10
            )
            .is_err());
        assert!(store
            .cache_scan_expiry(
                10,
                Some(&ExpiryCursor {
                    expire_at: 100,
                    incarnation: 0,
                    object_id: OBJ
                }),
                10
            )
            .is_err());
        assert!(store
            .cache_scan_expiry(
                10,
                Some(&ExpiryCursor {
                    expire_at: 100,
                    incarnation: 1,
                    object_id: 42
                }),
                10
            )
            .is_err());
        Ok(())
    }

    #[test]
    fn test_expiry_identity_cas_exact_or_loud() -> FsResult<()> {
        let store = new_store("expiry-cas")?;
        let pos = |key: &str, generation: u64| ExpiryRow {
            expire_at: 500,
            incarnation: 3,
            object_id: OBJ,
            key: key.into(),
            generation,
        };

        // Bit-exact replay of the same row is an idempotent no-op.
        let mut w = store.cache_write();
        w.put_expiry(&pos("/k", 1))?;
        w.commit()?;
        let mut w = store.cache_write();
        w.put_expiry(&pos("/k", 1))?;
        w.commit()?;
        let rows = store.cache_scan_expiry(600, None, 10)?;
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].object_id, rows[0].generation), (OBJ, 1));

        // Same frozen position, any identity difference — generation
        // higher OR lower, or a different key — is loud divergence, not a
        // silent no-op or overwrite.
        {
            let mut w = store.cache_write();
            assert!(w.put_expiry(&pos("/k", 2)).is_err());
        }
        {
            let mut w = store.cache_write();
            assert!(w.put_expiry(&pos("/other", 1)).is_err());
        }
        let rows = store.cache_scan_expiry(600, None, 10)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, "/k");

        // Delete: exact identity removes the row; deleting the now-missing
        // row is idempotent.
        let mut w = store.cache_write();
        w.delete_expiry(500, 3, OBJ, "/k", 1)?;
        w.delete_expiry(500, 3, OBJ, "/k", 1)?;
        w.commit()?;
        assert!(store.cache_scan_expiry(600, None, 10)?.is_empty());

        // Delete against a committed row with a different key or
        // generation: loud divergence, row survives.
        let mut w = store.cache_write();
        w.put_expiry(&pos("/k", 7))?;
        w.commit()?;
        {
            let mut w = store.cache_write();
            assert!(w.delete_expiry(500, 3, OBJ, "/k", 6).is_err());
            assert!(w.delete_expiry(500, 3, OBJ, "/other", 7).is_err());
        }
        assert_eq!(store.cache_scan_expiry(600, None, 10)?.len(), 1);
        Ok(())
    }

    /// Literal frozen bytes of expiry row
    /// `(expire_at=100, incarnation=1, object_id=OBJ, key="/a", generation=1)`
    /// as any 4a/4b writer (7e3f8a02..919756de) committed it:
    /// key = three big-endian fixed fields (100 || 1 || 1<<38), value =
    /// bincode ("/a", 1) = u64-LE len 2 ++ "2f61" ++ u64-LE 1.
    const FROZEN_KEY_HEX: &str = "000000000000006400000000000000010000004000000000";
    const FROZEN_VALUE_HEX: &str = "02000000000000002f610100000000000000";
    /// A second frozen row (200, 1, OBJ+1, "/b", generation 2), used as the
    /// exclusive-cursor resume target past the first frozen position.
    const FROZEN_KEY2_HEX: &str = "00000000000000c800000000000000010000004000000001";
    const FROZEN_VALUE2_HEX: &str = "02000000000000002f620200000000000000";

    /// Reader half: bytes written **raw** through the engine (never via
    /// `expiry_key`/`put_expiry`) must decode through the public scan into
    /// the frozen rows. Locks the reader against any encoding drift
    /// without sharing an implementation with the writer.
    #[test]
    fn test_expiry_frozen_reader_decodes_raw_bytes() -> FsResult<()> {
        let store = new_store("expiry-frozen-reader")?;
        store.db.put_cf(
            RocksInodeStore::CF_CACHE_EXPIRY,
            hex_bytes(FROZEN_KEY_HEX),
            hex_bytes(FROZEN_VALUE_HEX),
        )?;
        store.db.put_cf(
            RocksInodeStore::CF_CACHE_EXPIRY,
            hex_bytes(FROZEN_KEY2_HEX),
            hex_bytes(FROZEN_VALUE2_HEX),
        )?;

        let row1 = ExpiryRow {
            expire_at: 100,
            incarnation: 1,
            object_id: OBJ,
            key: "/a".into(),
            generation: 1,
        };
        let row2 = ExpiryRow {
            expire_at: 200,
            incarnation: 1,
            object_id: OBJ + 1,
            key: "/b".into(),
            generation: 2,
        };
        assert_eq!(
            store.cache_scan_expiry(200, None, 10)?,
            vec![row1, row2.clone()]
        );

        // Exclusive cursor resume from the first frozen position.
        let cursor = ExpiryCursor {
            expire_at: 100,
            incarnation: 1,
            object_id: OBJ,
        };
        assert_eq!(store.cache_scan_expiry(200, Some(&cursor), 1)?, vec![row2]);
        Ok(())
    }

    /// Writer half: on a fresh store, `put_expiry` must commit exactly the
    /// frozen bytes. Locks the writer against any encoding drift.
    #[test]
    fn test_expiry_frozen_writer_emits_frozen_bytes() -> FsResult<()> {
        let store = new_store("expiry-frozen-writer")?;
        {
            let mut w = store.cache_write();
            w.put_expiry(&ExpiryRow {
                expire_at: 100,
                incarnation: 1,
                object_id: OBJ,
                key: "/a".into(),
                generation: 1,
            })?;
            w.put_expiry(&ExpiryRow {
                expire_at: 200,
                incarnation: 1,
                object_id: OBJ + 1,
                key: "/b".into(),
                generation: 2,
            })?;
            w.commit()?;
        }

        let iter = store.db.range_scan(
            RocksInodeStore::CF_CACHE_EXPIRY,
            [0u8; 24].to_vec(),
            [0xFF; 24].to_vec(),
        )?;
        let mut raw = Vec::new();
        for item in iter {
            let (k, v) = item.map_err(rocks_err)?;
            raw.push((k, v));
        }
        assert_eq!(raw.len(), 2);
        assert_eq!(raw[0].0.to_vec(), hex_bytes(FROZEN_KEY_HEX));
        assert_eq!(raw[0].1.to_vec(), hex_bytes(FROZEN_VALUE_HEX));
        assert_eq!(raw[1].0.to_vec(), hex_bytes(FROZEN_KEY2_HEX));
        assert_eq!(raw[1].1.to_vec(), hex_bytes(FROZEN_VALUE2_HEX));
        Ok(())
    }

    fn hex_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn test_entry_scan_in_scope_prefix_boundaries_and_paging() -> FsResult<()> {
        let store = new_store("entry-scope")?;
        // "/a0" sorts between "/a" and "/a/": a byte-prefix bound would
        // misclassify it; component semantics must not.
        let keys = ["/z", "/a/b", "/a/b/c", "/aa", "/a", "/ab", "/a0", "/b"];
        {
            let mut w = store.cache_write();
            for (i, k) in keys.iter().enumerate() {
                w.put_entry(1, k, &entry(1, OBJ + i as i64, 10, 8, 0))?;
                // Another incarnation must not leak into the scoped scan.
                w.put_entry(2, k, &entry(1, OBJ + 100 + i as i64, 10, 8, 0))?;
            }
            w.commit()?;
        }

        // Prefix "/a": exactly "/a" and descendants "/a/...", never
        // "/a0", "/aa", "/ab", "/b", "/z".
        let all = store.cache_scan_entries_in_scope(1, "/a", None, 10)?;
        assert_eq!(
            all.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["/a", "/a/b", "/a/b/c"]
        );
        // Trailing-slash scope: "/a" itself is NOT a member (only exact
        // "/a/" or descendants "/a/..."), matching key_in_scope.
        let all = store.cache_scan_entries_in_scope(1, "/a/", None, 10)?;
        assert_eq!(
            all.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["/a/b", "/a/b/c"]
        );
        // Deeper scope narrows to exact + descendants only.
        let deep = store.cache_scan_entries_in_scope(1, "/a/b", None, 10)?;
        assert_eq!(
            deep.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["/a/b", "/a/b/c"]
        );
        // An exact-only scope yields just that key.
        let exact = store.cache_scan_entries_in_scope(1, "/b", None, 10)?;
        assert_eq!(
            exact.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["/b"]
        );
        // A scope that matches nothing yields an empty page.
        assert!(store
            .cache_scan_entries_in_scope(1, "/a/b/c/d", None, 10)?
            .is_empty());

        // A cursor outside the scope is rejected at the boundary — a
        // foreign cursor can never turn the scan into an unbounded
        // filtered walk over unrelated keys ("/ab" is a byte-prefix
        // neighbor but not a component member; "/" sorts below "/z").
        assert!(store
            .cache_scan_entries_in_scope(1, "/a", Some("/ab"), 10)
            .is_err());
        assert!(store
            .cache_scan_entries_in_scope(1, "/z", Some("/"), 10)
            .is_err());
        // An in-scope cursor stays valid even after its row is gone.
        assert!(store
            .cache_scan_entries_in_scope(1, "/a", Some("/a/b"), 10)
            .is_ok());

        // limit=1 chained pages with the exclusive cursor: restart/resume
        // without skips or duplicates.
        let mut seen = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let page = store.cache_scan_entries_in_scope(1, "/a", after.as_deref(), 1)?;
            match page.last() {
                None => break,
                Some((k, _)) => {
                    seen.push(k.clone());
                    after = Some(k.clone());
                }
            }
        }
        assert_eq!(seen, vec!["/a", "/a/b", "/a/b/c"]);

        // Delete the middle of the scope, then resume from a cursor whose
        // row is gone: successors surface exactly once, no skip, no dup.
        {
            let mut w = store.cache_write();
            w.delete_entry(1, "/a/b")?;
            w.commit()?;
        }
        let after_b = "/a/b";
        let page = store.cache_scan_entries_in_scope(1, "/a", Some(after_b), 10)?;
        assert_eq!(
            page.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["/a/b/c"]
        );

        // Keys after the family boundary terminate the scan early even
        // with a huge incarnation tail (correctness of the family bound).
        let page = store.cache_scan_entries_in_scope(1, "/a0", None, 10)?;
        assert_eq!(
            page.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["/a0"]
        );
        Ok(())
    }

    #[test]
    fn test_scan_limit_validation() -> FsResult<()> {
        let store = new_store("scan-limit")?;

        // limit 0 is rejected everywhere; SCAN_HARD_CAP is the largest
        // accepted page; one past it is rejected.
        assert!(store.cache_scan_entries(1, None, 0).is_err());
        assert!(store.cache_scan_expiry(10, None, 0).is_err());
        assert!(store.cache_scan_entries_in_scope(1, "/a", None, 0).is_err());

        assert!(store.cache_scan_entries(1, None, SCAN_HARD_CAP).is_ok());
        assert!(store.cache_scan_expiry(10, None, SCAN_HARD_CAP).is_ok());
        assert!(store
            .cache_scan_entries_in_scope(1, "/a", None, SCAN_HARD_CAP)
            .is_ok());

        assert!(store
            .cache_scan_entries(1, None, SCAN_HARD_CAP + 1)
            .is_err());
        assert!(store
            .cache_scan_expiry(10, None, SCAN_HARD_CAP + 1)
            .is_err());
        assert!(store
            .cache_scan_entries_in_scope(1, "/a", None, SCAN_HARD_CAP + 1)
            .is_err());
        Ok(())
    }

    #[test]
    fn test_entry_scan_paged_and_incarnation_scoped() -> FsResult<()> {
        let store = new_store("entry-scan")?;
        let mut w = store.cache_write();
        for i in 0..5 {
            w.put_entry(1, &format!("/k{}", i), &entry(1, OBJ + i, 10, 8, 0))?;
            // A different incarnation with overlapping keys must not leak.
            w.put_entry(2, &format!("/k{}", i), &entry(1, OBJ + 100 + i, 10, 8, 0))?;
        }
        w.commit()?;

        let page1 = store.cache_scan_entries(1, None, 2)?;
        assert_eq!(
            page1.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["/k0", "/k1"]
        );
        let page2 = store.cache_scan_entries(1, Some("/k1"), 2)?;
        assert_eq!(
            page2.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["/k2", "/k3"]
        );
        let page3 = store.cache_scan_entries(1, Some("/k3"), 10)?;
        assert_eq!(page3.len(), 1);
        assert_eq!(page3[0].0, "/k4");

        // Incarnation 2 sees only its own rows.
        let all2 = store.cache_scan_entries(2, None, 10)?;
        assert_eq!(all2.len(), 5);
        assert!(all2.iter().all(|(_, e)| e.object_id >= OBJ + 100));
        Ok(())
    }

    #[test]
    fn test_entry_scan_order_across_lengths_and_deleted_continuation() -> FsResult<()> {
        let store = new_store("entry-order")?;
        // Different lengths sharing a component prefix: raw-byte encoded
        // order must equal string order: /a, /a/b, /aa, /ab, /z.
        let keys = ["/z", "/a/b", "/aa", "/a", "/ab"];
        {
            let mut w = store.cache_write();
            for (i, k) in keys.iter().enumerate() {
                w.put_entry(1, k, &entry(1, OBJ + i as i64, 10, 8, 0))?;
            }
            w.commit()?;
        }
        let all = store.cache_scan_entries(1, None, 10)?;
        assert_eq!(
            all.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["/a", "/a/b", "/aa", "/ab", "/z"]
        );

        // Continuation: start strictly after "/a/b" — next row is "/aa".
        assert_eq!(store.cache_scan_entries(1, Some("/a/b"), 10)?[0].0, "/aa");

        // Continuation row itself deleted: the scan still starts at the
        // continuation position and yields the following rows.
        {
            let mut w = store.cache_write();
            w.delete_entry(1, "/a/b")?;
            w.commit()?;
        }
        let page = store.cache_scan_entries(1, Some("/a/b"), 10)?;
        assert_eq!(
            page.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            vec!["/aa", "/ab", "/z"]
        );

        // The reserved u64::MAX incarnation cannot be scanned.
        assert!(store.cache_scan_entries(u64::MAX, None, 10).is_err());
        assert!(store
            .cache_scan_entries(MAX_ALLOCATABLE_INCARNATION, None, 0)
            .is_err());
        Ok(())
    }

    #[test]
    fn test_write_boundary_rejects_invalid_rows() -> FsResult<()> {
        let store = new_store("write-validate")?;

        // Valid entry with ufs_mtime == 0 is rejected at the store boundary.
        let mut e = entry(1, OBJ, 10, 8, 0);
        e.ufs_mtime = 0;
        {
            let mut w = store.cache_write();
            assert!(w.put_entry(1, "/k", &e).is_err());
        }
        // Generation 0 rejected.
        {
            let mut e = entry(1, OBJ, 10, 8, 0);
            e.generation = 0;
            let mut w = store.cache_write();
            assert!(w.put_entry(1, "/k", &e).is_err());
        }
        // Reserved/Tombstoned with an expiry deadline rejected.
        {
            let mut e = entry(1, OBJ, 10, 8, 100);
            e.state = CacheEntryState::Reserved;
            let mut w = store.cache_write();
            assert!(w.put_entry(1, "/k", &e).is_err());
        }

        // Reverse row outside the cache domain rejected.
        {
            let mut w = store.cache_write();
            assert!(w
                .put_object(
                    42,
                    &ObjectRow {
                        incarnation: 1,
                        key: "/k".into(),
                        generation: 1
                    }
                )
                .is_err());
        }

        // Expiry row with non-positive or negative deadline rejected; a
        // negative scan instant is rejected too.
        {
            let mut w = store.cache_write();
            assert!(w
                .put_expiry(&ExpiryRow {
                    expire_at: 0,
                    incarnation: 1,
                    object_id: OBJ,
                    key: "/k".into(),
                    generation: 1
                })
                .is_err());
            assert!(w
                .put_expiry(&ExpiryRow {
                    expire_at: -5,
                    incarnation: 1,
                    object_id: OBJ,
                    key: "/k".into(),
                    generation: 1
                })
                .is_err());
        }
        assert!(store.cache_scan_expiry(-1, None, 10).is_err());

        // Nothing above was committed: all stores remain empty.
        assert!(store.cache_get_entry(1, "/k")?.is_none());
        assert!(store.cache_get_object(OBJ)?.is_none());
        assert!(store.cache_scan_expiry(1000, None, 10)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_watermarks_are_monotonic() -> FsResult<()> {
        let store = new_store("monotonic")?;

        {
            let mut w = store.cache_write();
            w.set_client_watermark(11, 5)?;
            w.set_state(state_tags::CACHE_OBJECT_ID, OBJ + 9)?;
            w.commit()?;
        }
        // Regressing replay: no-op, values stay at the high-water mark.
        {
            let mut w = store.cache_write();
            w.set_client_watermark(11, 3)?;
            w.set_client_watermark(11, 5)?; // equal is also a no-op
            w.set_state(state_tags::CACHE_OBJECT_ID, OBJ + 1)?;
            w.commit()?;
        }
        assert_eq!(store.cache_client_watermark(11)?, Some(5));
        assert_eq!(
            store.cache_get_state(state_tags::CACHE_OBJECT_ID)?,
            Some(OBJ + 9)
        );

        // Advancing still works.
        {
            let mut w = store.cache_write();
            w.set_client_watermark(11, 6)?;
            w.commit()?;
        }
        assert_eq!(store.cache_client_watermark(11)?, Some(6));

        // Monotonic against values staged in the SAME batch: on an empty
        // store, 5 then 3 in one batch must commit 5, not 3.
        {
            let mut w = store.cache_write();
            w.set_client_watermark(22, 5)?;
            w.set_client_watermark(22, 3)?;
            w.set_state(state_tags::CACHE_OBJECT_ID, OBJ + 20)?;
            w.set_state(state_tags::CACHE_OBJECT_ID, OBJ + 10)?;
            w.commit()?;
        }
        assert_eq!(store.cache_client_watermark(22)?, Some(5));
        assert_eq!(
            store.cache_get_state(state_tags::CACHE_OBJECT_ID)?,
            Some(OBJ + 20)
        );

        // Persisted 5 (client 11): a batch of 7 then 6 must commit 7 — the
        // smaller trailing set cannot override the larger staged value.
        {
            let mut w = store.cache_write();
            w.set_client_watermark(11, 7)?;
            w.set_client_watermark(11, 6)?;
            w.commit()?;
        }
        assert_eq!(store.cache_client_watermark(11)?, Some(7));
        Ok(())
    }

    #[test]
    fn test_incarnation_bounds_at_write_boundary() -> FsResult<()> {
        let store = new_store("incarnation-bounds")?;
        let e = entry(1, OBJ, 10, 8, 0);
        let o = ObjectRow {
            incarnation: 1,
            key: "/k".into(),
            generation: 1,
        };
        let x = ExpiryRow {
            expire_at: 100,
            incarnation: 1,
            object_id: OBJ,
            key: "/k".into(),
            generation: 1,
        };

        // Every creating write entry rejects 0 and the reserved u64::MAX.
        for bad in [0u64, MAX_ALLOCATABLE_INCARNATION + 1] {
            let mut w = store.cache_write();
            assert!(w.put_entry(bad, "/k", &e).is_err(), "put_entry {bad}");
            assert!(w
                .put_object(
                    OBJ,
                    &ObjectRow {
                        incarnation: bad,
                        ..o.clone()
                    }
                )
                .is_err());
            assert!(w
                .put_expiry(&ExpiryRow {
                    incarnation: bad,
                    ..x.clone()
                })
                .is_err());
            assert!(w
                .put_incarnation(
                    bad,
                    IncarnationRow {
                        mount_id: 1,
                        revoked: false
                    }
                )
                .is_err());
            assert!(w.set_current_incarnation(1, bad).is_err());
        }
        // The highest allocatable incarnation is accepted everywhere.
        {
            let mut w = store.cache_write();
            w.put_incarnation(
                MAX_ALLOCATABLE_INCARNATION,
                IncarnationRow {
                    mount_id: 1,
                    revoked: false,
                },
            )?;
            w.set_current_incarnation(1, MAX_ALLOCATABLE_INCARNATION)?;
            w.commit()?;
        }
        assert_eq!(
            store.cache_current_incarnation(1)?,
            Some(MAX_ALLOCATABLE_INCARNATION)
        );

        // Nothing from the rejected writes was committed.
        assert!(store.cache_get_entry(0, "/k")?.is_none());
        assert!(store.cache_get_object(OBJ)?.is_none());
        assert!(store.cache_scan_expiry(1000, None, 10)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_incarnation_pointer_and_rows() -> FsResult<()> {
        let store = new_store("incarnation")?;
        assert!(store.cache_current_incarnation(9)?.is_none());

        let mut w = store.cache_write();
        w.put_incarnation(
            1,
            IncarnationRow {
                mount_id: 9,
                revoked: false,
            },
        )?;
        w.set_current_incarnation(9, 1)?;
        w.commit()?;

        assert_eq!(store.cache_current_incarnation(9)?, Some(1));
        assert_eq!(
            store.cache_get_incarnation(1)?,
            Some(IncarnationRow {
                mount_id: 9,
                revoked: false
            })
        );

        // Revoke: pointer cleared, row retained and marked.
        let mut w = store.cache_write();
        w.put_incarnation(
            1,
            IncarnationRow {
                mount_id: 9,
                revoked: true,
            },
        )?;
        w.clear_current_incarnation(9)?;
        w.commit()?;

        assert!(store.cache_current_incarnation(9)?.is_none());
        assert_eq!(
            store.cache_get_incarnation(1)?,
            Some(IncarnationRow {
                mount_id: 9,
                revoked: true
            })
        );
        Ok(())
    }

    #[test]
    fn test_idempotency_outcome_and_watermark() -> FsResult<()> {
        let store = new_store("idempotency")?;
        let token = OpToken {
            client_id: 11,
            op_seq: 3,
        };
        assert!(store.cache_get_outcome(token)?.is_none());
        assert!(store.cache_client_watermark(11)?.is_none());

        let outcome = OpOutcome::Reserved {
            start: OBJ,
            end: OBJ + 100,
        };
        let mut w = store.cache_write();
        w.put_outcome(token, &outcome)?;
        w.set_client_watermark(11, 3)?;
        w.commit()?;

        assert_eq!(store.cache_get_outcome(token)?, Some(outcome));
        assert_eq!(store.cache_client_watermark(11)?, Some(3));

        // Window eviction: outcome row deleted, watermark retained.
        let mut w = store.cache_write();
        w.delete_outcome(token)?;
        w.commit()?;
        assert!(store.cache_get_outcome(token)?.is_none());
        assert_eq!(store.cache_client_watermark(11)?, Some(3));
        Ok(())
    }

    #[test]
    fn test_state_watermarks() -> FsResult<()> {
        let store = new_store("state")?;
        assert!(store
            .cache_get_state(state_tags::CACHE_OBJECT_ID)?
            .is_none());
        let mut w = store.cache_write();
        w.set_state(state_tags::CACHE_OBJECT_ID, OBJ + 9)?;
        w.commit()?;
        assert_eq!(
            store.cache_get_state(state_tags::CACHE_OBJECT_ID)?,
            Some(OBJ + 9)
        );
        Ok(())
    }
}
