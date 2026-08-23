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
    decode_key, encode_key, validate_entry, validate_expiry_row, validate_incarnation,
    validate_object_row, CacheEntry, ExpiryRow, IncarnationPolicyRow, IncarnationRow, ObjectRow,
    OpOutcome, OpToken, MAX_ALLOCATABLE_INCARNATION,
};
use crate::master::meta::cache::store::CacheWrite;
use crate::master::meta::cache::store::LocalCacheIndexStore;
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

    fn expiry_key(expire_at: i64, incarnation: u64, object_id: i64) -> [u8; 24] {
        let mut key = [0u8; 24];
        key[..8].copy_from_slice(&expire_at.to_be_bytes());
        key[8..16].copy_from_slice(&incarnation.to_be_bytes());
        key[16..24].copy_from_slice(&object_id.to_be_bytes());
        key
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
        let key = RocksInodeStore::expiry_key(row.expire_at, row.incarnation, row.object_id);
        let value = Serde::serialize(&(row.key.clone(), row.generation))?;
        self.put_cf(RocksInodeStore::CF_CACHE_EXPIRY, key, value)
    }

    fn delete_expiry(&mut self, expire_at: i64, incarnation: u64, object_id: i64) -> FsResult<()> {
        let key = RocksInodeStore::expiry_key(expire_at, incarnation, object_id);
        self.delete_cf(RocksInodeStore::CF_CACHE_EXPIRY, key)
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

    fn cache_scan_expiry(&self, now: i64, limit: usize) -> FsResult<Vec<ExpiryRow>> {
        if now < 0 {
            return Err(FsError::from(CommonError::from(err_msg!(
                "expiry scan instant must be non-negative: {}",
                now
            ))));
        }
        // Range: [min_key, upper) where `upper` leads with `now + 1` —
        // expire_at is the first field, so this covers every row with
        // expire_at <= now regardless of the trailing fields. All rows are
        // non-negative (`put_expiry` rejects <= 0), so signed big-endian
        // byte order equals deadline order.
        let start = [0u8; 24];
        let mut end = [0xFFu8; 24];
        let upper = now.saturating_add(1);
        // `now == i64::MAX` cannot saturate further; clamping to MAX keeps
        // the upper bound after every real row (last field bytes are 0xFF).
        end[..8].copy_from_slice(&upper.to_be_bytes());

        let iter = self
            .db
            .range_scan(Self::CF_CACHE_EXPIRY, start.to_vec(), end.to_vec())?;
        let mut rows = Vec::new();
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
    use crate::master::meta::cache::store::state_tags;
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
        let due = store.cache_scan_expiry(400, 10)?;
        assert_eq!(due.len(), 2);
        assert_eq!(due[0].expire_at, 100);
        assert_eq!(due[0].key, "/a");
        assert_eq!(due[1].expire_at, 300);
        assert_eq!(due[1].key, "/b");

        // Limit is respected.
        assert_eq!(store.cache_scan_expiry(600, 10)?.len(), 3);
        assert_eq!(store.cache_scan_expiry(600, 2)?.len(), 2);

        // Deletion removes exactly one row.
        let mut w = store.cache_write();
        w.delete_expiry(100, 1, OBJ + 1)?;
        w.commit()?;
        assert_eq!(store.cache_scan_expiry(600, 10)?.len(), 2);
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
            .is_ok());
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
        assert!(store.cache_scan_expiry(-1, 10).is_err());

        // Nothing above was committed: all stores remain empty.
        assert!(store.cache_get_entry(1, "/k")?.is_none());
        assert!(store.cache_get_object(OBJ)?.is_none());
        assert!(store.cache_scan_expiry(1000, 10)?.is_empty());
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
        assert!(store.cache_scan_expiry(1000, 10)?.is_empty());
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
