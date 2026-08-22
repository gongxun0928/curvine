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

//! Leader-side cache-mode metadata service (dual-mode split, task #4
//! milestone 4a).
//!
//! Every durable identity mutation goes through the single committed
//! `CacheManager::apply_*` path via `JournalWriter::sync_propose_cache`:
//! the propose returns only after the FSM applied the entry, and the
//! service then re-reads the committed state (token outcome / entry row)
//! to resolve the RPC — never the pre-propose in-memory view.
//!
//! The object-id issuer is leader-only: ids come from the volatile
//! `CacheObjectId` allocator, are consumed strictly inside the durably
//! reserved segment, and clients can never supply their own object id
//! (the wire has no such field). After a leader crash the allocator
//! restarts at the durable reserve watermark — the unconsumed tail of the
//! old segment is permanently burned and the next reserve starts after
//! the watermark.
//!
//! Block locations are volatile master state (contract: a full block
//! report may restore lost locations but must never resurrect a
//! `Valid` CacheIndex row). They are not journaled: `CacheGet` treats
//! any missing block location as a whole-object miss so the caller
//! falls back to the UFS.

use crate::master::journal::{
    CacheAllocateEntry, CacheCommitEntry, CacheIdReserveEntry, CacheRemoveEntry, JournalEntry,
    JournalWriter,
};
use crate::master::meta::cache::entry::{CacheEntry, CacheEntryState, OpOutcome, OpToken};
use crate::master::meta::cache::state_tags;
use crate::master::meta::cache::LocalCacheIndexStore;
use crate::master::meta::{BlockIdCodec, CacheBlockLayout};
use crate::master::{MasterMonitor, SyncFsDir};
use curvine_core_error::{err_box, err_msg, CommonError, CommonResult};
use curvine_model::BlockLocation;
use curvine_runtime::common::LocalTime;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Object ids per durable reserve segment. Reserves are rare (one per
/// segment consumed), so the per-reserve outcome rows of the issuer
/// client stay trivially small.
const CACHE_RESERVE_SEGMENT: i64 = 4096;

/// Hard cap on block location entries accepted by one commit. Bounded so
/// a malformed commit can never make the master build an unbounded
/// response or volatile table (contract: bounded ops).
pub const MAX_COMMIT_BLOCKS: usize = 1 << 16;

/// Hard cap on replica locations per block.
pub const MAX_LOCATIONS_PER_BLOCK: usize = 16;

/// The internal client identity used for segment reserves. It is disjoint
/// from any RPC client id space in use (client ids are random u64s; the
/// watermark of this client only advances via reserves, which lazily
/// evicts older reserve outcome rows once window eviction lands in 4c).
const CACHE_ISSUER_CLIENT_ID: u64 = 0;

#[derive(Debug, Clone)]
pub struct CacheBlockLocation {
    pub block_id: i64,
    pub block_len: i64,
    pub workers: Vec<BlockLocation>,
}

#[derive(Debug, Clone)]
pub struct CacheGetResult {
    pub object_id: i64,
    pub len: i64,
    pub block_size: i64,
    pub blocks: Vec<CacheBlockLocation>,
}

/// Volatile per-object block locations, keyed by object id, one ordered
/// worker set per 1-based block sequence.
/// Arguments of one commit RPC (keeps the service signature clippy-sized).
#[derive(Debug, Clone)]
pub struct CacheCommitParams<'a> {
    pub incarnation: u64,
    pub key: &'a str,
    pub generation: u64,
    pub object_id: i64,
    pub len: i64,
    pub ufs_mtime: i64,
    pub ttl_ms: i64,
    pub blocks: Vec<CacheBlockLocation>,
}

#[derive(Default)]
struct ObjectLocations {
    blocks: HashMap<i64, Vec<BlockLocation>>,
}

pub struct CacheService {
    fs_dir: SyncFsDir,
    journal_writer: Arc<JournalWriter>,
    monitor: MasterMonitor,
    /// Serializes the reserve+issue critical section so concurrent
    /// allocates consume strictly monotonic, unique ids.
    issue_lock: Mutex<()>,
    /// Monotonic op_seq source for issuer reserve tokens. Seeded from the
    /// wall clock so restarts do not collide (a repeated token with
    /// different segment params would be treated as divergence).
    issuer_seq: AtomicU64,
    locations: Mutex<HashMap<i64, ObjectLocations>>,
}

impl CacheService {
    pub fn new(
        fs_dir: SyncFsDir,
        journal_writer: Arc<JournalWriter>,
        monitor: MasterMonitor,
    ) -> Self {
        Self {
            fs_dir,
            journal_writer,
            monitor,
            issue_lock: Mutex::new(()),
            issuer_seq: AtomicU64::new(LocalTime::mills()),
            locations: Mutex::new(HashMap::new()),
        }
    }

    /// Whole-object lookup. `hit` requires a `Valid`, unexpired entry AND
    /// a complete volatile location set for the derived block layout —
    /// anything missing is a miss (caller falls back to the UFS).
    pub fn get(&self, incarnation: u64, key: &str) -> CommonResult<Option<CacheGetResult>> {
        let store = self.fs_dir.read();
        let rocks = store.get_rocks_store();
        let entry = rocks.cache_get_entry(incarnation, key).map_err(fs_err)?;
        let Some(entry) = entry else {
            return Ok(None);
        };
        if entry.state != CacheEntryState::Valid {
            return Ok(None);
        }
        // Passive expiry check; active expiry scanning lands in 4c.
        if entry.expire_at != 0 && entry.expire_at <= LocalTime::mills() as i64 {
            return Ok(None);
        }

        let layout = CacheBlockLayout::derive(entry.object_id, entry.len, entry.block_size)?;
        let locations = self.locations.lock().unwrap();
        let Some(object_locations) = locations.get(&entry.object_id) else {
            return Ok(None);
        };
        if object_locations.blocks.len() != layout.block_count as usize {
            return Ok(None);
        }

        let mut blocks = Vec::with_capacity(layout.block_count as usize);
        for index in 1..=layout.block_count {
            let Some(workers) = object_locations.blocks.get(&index) else {
                return Ok(None);
            };
            if workers.is_empty() {
                return Ok(None);
            }
            blocks.push(CacheBlockLocation {
                block_id: layout.block_id(index)?,
                block_len: if index == layout.block_count {
                    layout.last_len
                } else {
                    layout.block_size
                },
                workers: workers.clone(),
            });
        }
        Ok(Some(CacheGetResult {
            object_id: entry.object_id,
            len: entry.len,
            block_size: entry.block_size,
            blocks,
        }))
    }

    /// Allocate a fresh cache object for `key` and hand back the
    /// leader-issued `(object_id, generation)`. The client cannot
    /// influence the object id; generation is the next absolute
    /// transition of the entry row (None -> 1, Tombstoned@g -> g+1).
    pub fn allocate(
        &self,
        token: OpToken,
        incarnation: u64,
        key: &str,
        block_size: i64,
    ) -> CommonResult<(i64, u64)> {
        self.require_leader()?;
        if block_size <= 0 {
            return err_box!("cache allocate block size must be positive: {}", block_size);
        }

        // Serialize issuance: reserve + consume must be one critical
        // section for uniqueness and in-segment monotonicity.
        let _guard = self.issue_lock.lock().unwrap();

        // Fast-fail entry-state check and generation selection. The
        // committed apply re-checks the absolute transition, so a racing
        // mutation between here and the propose fails loudly there.
        let generation = {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            match rocks.cache_get_entry(incarnation, key).map_err(fs_err)? {
                None => 1,
                Some(cur) if cur.state == CacheEntryState::Tombstoned => {
                    cur.generation.checked_add(1).ok_or_else(|| {
                        cm_err("cache allocate generation overflow: entry is terminal")
                    })?
                }
                Some(cur) => {
                    return err_box!(
                        "cache allocate for live entry ({}, {})@{} state {:?}: only None or Tombstoned rows allocate",
                        incarnation,
                        key,
                        cur.generation,
                        cur.state
                    )
                }
            }
        };

        // Ensure the volatile allocator still has durable capacity: ids
        // may only be consumed inside a committed reserve segment. Guards
        // are dropped around the propose (the apply worker takes its own
        // fs_dir lock; holding a read guard across the barrier would
        // deadlock the FSM).
        loop {
            let durable_hw = {
                let store = self.fs_dir.read();
                store
                    .get_rocks_store()
                    .cache_get_state(state_tags::CACHE_OBJECT_ID)
                    .map_err(fs_err)?
                    .unwrap_or(BlockIdCodec::CACHE_OBJECT_MIN - 1)
            };
            if self.current_volatile_object_id() < durable_hw {
                break;
            }
            // Reserve the next contiguous segment [HW+1, HW+1+SEG).
            let start = durable_hw
                .checked_add(1)
                .ok_or_else(|| cm_err("cache object id segment space exhausted"))?;
            let end = start
                .checked_add(CACHE_RESERVE_SEGMENT)
                .filter(|end| *end <= BlockIdCodec::CACHE_OBJECT_MAX + 1)
                .ok_or_else(|| cm_err("cache object id segment space exhausted"))?;
            let reserve_token = self.next_issuer_token();
            let entry = JournalEntry::CacheIdReserve(CacheIdReserveEntry {
                op_id: 0,
                rpc_id: 0,
                token: reserve_token,
                start,
                end,
            });
            // The propose barrier returns only after the FSM applied the
            // reserve, so after it the durable HW covers [start, end).
            self.journal_writer
                .sync_propose_cache(entry)
                .map_err(fs_err)?;
        }

        let issued = {
            let store = self.fs_dir.read();
            store.cache.next_object_id()?
        };

        let entry = CacheEntry {
            generation,
            state: CacheEntryState::Reserved,
            object_id: issued,
            len: 0,
            ufs_mtime: 0,
            block_size,
            expire_at: 0,
        };
        let journal_entry = JournalEntry::CacheAllocate(CacheAllocateEntry {
            op_id: 0,
            rpc_id: 0,
            token,
            incarnation,
            key: key.to_string(),
            entry,
        });
        self.journal_writer
            .sync_propose_cache(journal_entry)
            .map_err(fs_err)?;

        // Resolve the RPC from committed state only: the token must
        // resolve to exactly this allocate outcome.
        let store = self.fs_dir.read();
        let rocks = store.get_rocks_store();
        match rocks.cache_get_outcome(token).map_err(fs_err)? {
            Some(OpOutcome::Allocated {
                object_id,
                generation: out_generation,
                ..
            }) if object_id == issued && out_generation == generation => {
                Ok((object_id, out_generation))
            }
            other => err_box!(
                "cache allocate barrier readback failed for token {:?}: {:?}",
                token,
                other
            ),
        }
    }

    /// Commit the loaded object: `Reserved@g` -> `Valid` with the final
    /// `(len, ufs_mtime, expire_at)` and the volatile block locations.
    /// The location set must cover the derived block layout exactly —
    /// one entry per block, sequence-ordered, no gaps, duplicates, or
    /// extra blocks — and is only recorded after the barrier ACK.
    pub fn commit(&self, params: CacheCommitParams<'_>) -> CommonResult<()> {
        let CacheCommitParams {
            incarnation,
            key,
            generation,
            object_id,
            len,
            ufs_mtime,
            ttl_ms,
            blocks,
        } = params;
        self.require_leader()?;

        // Validation happens against the committed entry row (block_size
        // is immutable entry metadata), then the guard is dropped for the
        // propose. The committed apply re-checks identity via CAS.
        let (_block_size, layout) = {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            let cur = rocks
                .cache_get_entry(incarnation, key)
                .map_err(fs_err)?
                .ok_or_else(|| {
                    cm_err(format!(
                        "cache commit for missing entry ({}, {})",
                        incarnation, key
                    ))
                })?;
            if cur.state != CacheEntryState::Reserved {
                return err_box!(
                    "cache commit for non-reserved entry ({}, {})@{} state {:?}",
                    incarnation,
                    key,
                    cur.generation,
                    cur.state
                );
            }
            if cur.generation != generation || cur.object_id != object_id {
                return err_box!(
                    "cache commit identity mismatch for ({}, {}): entry ({}, {}) vs request ({}, {})",
                    incarnation,
                    key,
                    cur.generation,
                    cur.object_id,
                    generation,
                    object_id
                );
            }
            (
                cur.block_size,
                CacheBlockLayout::derive(object_id, len, cur.block_size)?,
            )
        };

        validate_commit_locations(&layout, &blocks)?;

        let expire_at = if ttl_ms > 0 {
            let now = LocalTime::mills() as i64;
            now.checked_add(ttl_ms)
                .filter(|v| *v > 0)
                .ok_or_else(|| cm_err("cache commit ttl overflow"))?
        } else {
            0
        };

        let entry = JournalEntry::CacheCommit(CacheCommitEntry {
            op_id: 0,
            rpc_id: 0,
            incarnation,
            key: key.to_string(),
            generation,
            expected_object_id: object_id,
            len,
            ufs_mtime,
            expire_at,
        });
        self.journal_writer
            .sync_propose_cache(entry)
            .map_err(fs_err)?;

        // Readback from committed state: the row must now be Valid at the
        // committed generation with the committed object identity. Only
        // then are the volatile locations recorded.
        {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            let cur = rocks
                .cache_get_entry(incarnation, key)
                .map_err(fs_err)?
                .ok_or_else(|| cm_err("cache commit readback: entry vanished"))?;
            if cur.state != CacheEntryState::Valid
                || cur.generation != generation
                || cur.object_id != object_id
            {
                return err_box!(
                    "cache commit barrier readback failed for ({}, {}): {:?}",
                    incarnation,
                    key,
                    cur
                );
            }
        }

        let mut locations = self.locations.lock().unwrap();
        let object_locations = locations.entry(object_id).or_default();
        object_locations.blocks.clear();
        for (index, block) in blocks.into_iter().enumerate() {
            object_locations
                .blocks
                .insert((index + 1) as i64, block.workers);
        }
        Ok(())
    }

    /// Invalidate / remove one cache object: a generation fence to
    /// `Tombstoned@expected_generation + 1` that must target the object
    /// the caller observed (`expected_object_id` CAS). Volatile locations
    /// are dropped only after the barrier readback confirms the fence.
    /// (Bulk prefix/mount removal and physical vacuum land in 4c.)
    pub fn invalidate(
        &self,
        incarnation: u64,
        key: &str,
        expected_generation: u64,
        expected_object_id: i64,
    ) -> CommonResult<()> {
        self.require_leader()?;

        let new_generation = expected_generation
            .checked_add(1)
            .ok_or_else(|| cm_err("cache invalidate generation overflow: entry is terminal"))?;
        let entry = JournalEntry::CacheRemove(CacheRemoveEntry {
            op_id: 0,
            rpc_id: 0,
            incarnation,
            key: key.to_string(),
            expected_generation,
            new_generation,
            expected_object_id,
        });
        self.journal_writer
            .sync_propose_cache(entry)
            .map_err(fs_err)?;

        let store = self.fs_dir.read();
        let rocks = store.get_rocks_store();
        let cur = rocks.cache_get_entry(incarnation, key).map_err(fs_err)?;
        match cur {
            Some(cur)
                if cur.state == CacheEntryState::Tombstoned && cur.generation >= new_generation =>
            {
                drop(store);
                self.locations.lock().unwrap().remove(&expected_object_id);
                Ok(())
            }
            other => err_box!(
                "cache invalidate barrier readback failed for ({}, {}): {:?}",
                incarnation,
                key,
                other
            ),
        }
    }

    fn require_leader(&self) -> CommonResult<()> {
        if !self.monitor.is_active() {
            return err_box!("cache metadata mutations are leader-only");
        }
        Ok(())
    }

    fn current_volatile_object_id(&self) -> i64 {
        self.fs_dir.read().cache.current_object_id()
    }

    fn next_issuer_token(&self) -> OpToken {
        let op_seq = self.issuer_seq.fetch_add(1, Ordering::SeqCst) + 1;
        OpToken {
            client_id: CACHE_ISSUER_CLIENT_ID,
            op_seq,
        }
    }

    /// Test/restore seam for volatile locations (a full block report will
    /// use the same table in 4d). Accepts only complete, validated sets.
    #[cfg(test)]
    pub(crate) fn install_locations(
        &self,
        object_id: i64,
        blocks: Vec<CacheBlockLocation>,
    ) -> CommonResult<()> {
        let mut locations = self.locations.lock().unwrap();
        let object_locations = locations.entry(object_id).or_default();
        object_locations.blocks.clear();
        for (index, block) in blocks.into_iter().enumerate() {
            object_locations
                .blocks
                .insert((index + 1) as i64, block.workers);
        }
        Ok(())
    }
}

/// Exact-layout validation: the commit's block list must equal the
/// derived `1..=n` sequence — right count, right ids, right lengths,
/// non-empty bounded worker sets.
fn validate_commit_locations(
    layout: &CacheBlockLayout,
    blocks: &[CacheBlockLocation],
) -> CommonResult<()> {
    if blocks.len() > MAX_COMMIT_BLOCKS {
        return err_box!(
            "cache commit block count {} exceeds cap {}",
            blocks.len(),
            MAX_COMMIT_BLOCKS
        );
    }
    if blocks.len() != layout.block_count as usize {
        return err_box!(
            "cache commit location count {} does not match derived block count {} (len {}, block size {})",
            blocks.len(),
            layout.block_count,
            layout.len,
            layout.block_size
        );
    }
    for (index, block) in blocks.iter().enumerate() {
        let expected_id = layout.block_id((index + 1) as i64)?;
        if block.block_id != expected_id {
            return err_box!(
                "cache commit block id {} at position {} is not the derived id {}",
                block.block_id,
                index + 1,
                expected_id
            );
        }
        let expected_len = if (index + 1) as i64 == layout.block_count {
            layout.last_len
        } else {
            layout.block_size
        };
        if block.block_len != expected_len {
            return err_box!(
                "cache commit block {} length {} != expected {}",
                block.block_id,
                block.block_len,
                expected_len
            );
        }
        if block.workers.is_empty() {
            return err_box!(
                "cache commit block {} has no replica locations",
                block.block_id
            );
        }
        if block.workers.len() > MAX_LOCATIONS_PER_BLOCK {
            return err_box!(
                "cache commit block {} has {} locations, cap {}",
                block.block_id,
                block.workers.len(),
                MAX_LOCATIONS_PER_BLOCK
            );
        }
    }
    Ok(())
}

fn fs_err(e: curvine_error::FsError) -> CommonError {
    e.into()
}

fn cm_err(msg: impl Into<String>) -> CommonError {
    CommonError::from(err_msg!(msg.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::FsDir;
    use crate::master::quota::eviction::evictor::{Evictor, LRUEvictor};
    use crate::master::quota::eviction::EvictionConf;
    use crate::master::Master;
    use curvine_config::{ClusterConf, JournalConf, MasterConf};
    use curvine_model::StorageType;
    use curvine_raft::raft::{RaftClient, RoleState};
    use curvine_runtime::sync::StateCtl;
    use std::sync::Arc;

    const OBJ: i64 = BlockIdCodec::CACHE_OBJECT_MIN;

    fn service_conf(name: &str) -> ClusterConf {
        let mut journal = JournalConf::with_test();
        journal.enable = true;
        let mut conf = ClusterConf {
            testing: true,
            format_master: true,
            journal,
            master: MasterConf::default(),
            ..Default::default()
        };
        conf.change_test_meta_dir(name);
        conf
    }

    fn build_service(name: &str) -> CacheService {
        let conf = service_conf(name);
        Master::init_test_metrics();
        let rt = conf.journal.create_runtime();
        let client = RaftClient::from_conf(rt, &conf.journal);
        let writer = Arc::new(JournalWriter::new(true, client, &conf.journal).unwrap());
        let ttl_bucket_list = Arc::new(
            crate::master::meta::inode::ttl::TtlBucketList::new(
                conf.master.ttl_bucket_interval_ms() as i64,
            )
            .unwrap(),
        );
        let eviction_conf = EvictionConf::from_conf(&conf);
        let evictor: Arc<dyn Evictor> = Arc::new(LRUEvictor::new(eviction_conf.clone()));
        let fs_dir =
            SyncFsDir::new(FsDir::new(&conf, writer.clone(), ttl_bucket_list, evictor).unwrap());
        let monitor = MasterMonitor::new(StateCtl::new(0), StateCtl::new(0));
        // Unit tests exercise the leader-side validation and readback
        // logic; the barrier itself needs a real raft cluster (covered by
        // the journal fault tests and the 4e matrix) and fails closed in
        // testing mode.
        monitor.journal_ctl.set_state(RoleState::Leader);
        CacheService::new(fs_dir, writer, monitor)
    }

    fn worker(id: u32) -> BlockLocation {
        BlockLocation {
            worker_id: id,
            storage_type: StorageType::Mem,
        }
    }

    fn full_locations(layout: &CacheBlockLayout) -> Vec<CacheBlockLocation> {
        (1..=layout.block_count)
            .map(|index| {
                let block_len = if index == layout.block_count {
                    layout.last_len
                } else {
                    layout.block_size
                };
                CacheBlockLocation {
                    block_id: layout.block_id(index).unwrap(),
                    block_len,
                    workers: vec![worker(1), worker(2)],
                }
            })
            .collect()
    }

    fn token(client: u64, seq: u64) -> OpToken {
        OpToken {
            client_id: client,
            op_seq: seq,
        }
    }

    /// Writes a committed entry straight through the manager apply path
    /// (unit-test stand-in for a completed allocate+commit round trip,
    /// which needs a real raft barrier).
    fn committed_entry(
        service: &CacheService,
        key: &str,
        object_id: i64,
        len: i64,
        expire_at: i64,
    ) {
        let store = service.fs_dir.read();
        let rocks = store.get_rocks_store();
        let mgr = &store.cache;
        mgr.apply_id_reserve(rocks, token(1, 1), OBJ, OBJ + 100)
            .unwrap();
        let alloc = CacheEntry {
            generation: 1,
            state: CacheEntryState::Reserved,
            object_id,
            len: 0,
            ufs_mtime: 0,
            block_size: 64,
            expire_at: 0,
        };
        mgr.apply_allocate(rocks, token(2, 1), 1, key, &alloc)
            .unwrap();
        mgr.apply_commit(rocks, 1, key, 1, object_id, len, 777, expire_at)
            .unwrap();
    }

    fn layout(object_id: i64, len: i64) -> CacheBlockLayout {
        CacheBlockLayout::derive(object_id, len, 64).unwrap()
    }

    /// Whole-object semantics: a Valid entry without volatile locations
    /// is a miss; a complete location set is a hit; any missing block is
    /// a miss again.
    #[test]
    fn test_get_is_whole_object() {
        let service = build_service("get-whole-object");
        let object_id = OBJ;
        let len = 150; // 3 blocks of 64: 64, 64, 22
        committed_entry(&service, "/k", object_id, len, 0);
        let lay = layout(object_id, len);
        assert_eq!(lay.block_count, 3);
        assert_eq!(lay.last_len, 22);

        // No locations -> miss.
        assert!(service.get(1, "/k").unwrap().is_none());

        // Complete -> hit with exact derived ids/lengths.
        service
            .install_locations(object_id, full_locations(&lay))
            .unwrap();
        let hit = service
            .get(1, "/k")
            .unwrap()
            .expect("complete set must hit");
        assert_eq!(hit.object_id, object_id);
        assert_eq!(hit.len, len);
        assert_eq!(hit.block_size, 64);
        assert_eq!(hit.blocks.len(), 3);
        assert_eq!(
            hit.blocks[0].block_id,
            BlockIdCodec::encode_block_id(object_id, 1).unwrap()
        );
        assert_eq!(hit.blocks[0].block_len, 64);
        assert_eq!(hit.blocks[2].block_len, 22);
        assert_eq!(hit.blocks[2].workers.len(), 2);

        // Drop the last block -> whole-object miss.
        let mut partial = full_locations(&lay);
        partial.pop();
        service.install_locations(object_id, partial).unwrap();
        assert!(
            service.get(1, "/k").unwrap().is_none(),
            "missing block location must be a whole-object miss"
        );

        // Other incarnation / key -> miss.
        assert!(service.get(2, "/k").unwrap().is_none());
        assert!(service.get(1, "/other").unwrap().is_none());
    }

    #[test]
    fn test_get_miss_on_expired_entry() {
        let service = build_service("get-expired");
        // Expire in the past (passive expiry; active scan lands in 4c).
        committed_entry(&service, "/k", OBJ, 64, 1);
        service
            .install_locations(OBJ, full_locations(&layout(OBJ, 64)))
            .unwrap();
        assert!(service.get(1, "/k").unwrap().is_none());
    }

    /// Commit validation happens BEFORE the barrier: every malformed
    /// location set errors without touching the journal, and only a
    /// perfect set reaches the (testing-mode fail-closed) barrier.
    #[test]
    fn test_commit_rejects_malformed_location_sets() {
        let service = build_service("commit-malformed");
        let len = 130; // 3 blocks: 64, 64, 2
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            let mgr = &store.cache;
            mgr.apply_id_reserve(rocks, token(1, 1), OBJ, OBJ + 100)
                .unwrap();
            let alloc = CacheEntry {
                generation: 1,
                state: CacheEntryState::Reserved,
                object_id: OBJ,
                len: 0,
                ufs_mtime: 0,
                block_size: 64,
                expire_at: 0,
            };
            mgr.apply_allocate(rocks, token(2, 1), 1, "/k", &alloc)
                .unwrap();
        }
        let lay = layout(OBJ, len);

        let cases: Vec<(&str, Vec<CacheBlockLocation>)> = vec![
            ("wrong count (too few)", full_locations(&lay)[..2].to_vec()),
            ("wrong count (too many)", {
                let mut v = full_locations(&lay);
                v.push(v[0].clone());
                v
            }),
            ("wrong block id", {
                let mut v = full_locations(&lay);
                v[1].block_id = BlockIdCodec::encode_block_id(OBJ, 9).unwrap();
                v
            }),
            ("wrong last block length", {
                let mut v = full_locations(&lay);
                v[2].block_len = 64;
                v
            }),
            ("empty worker set", {
                let mut v = full_locations(&lay);
                v[0].workers.clear();
                v
            }),
            ("worker cap exceeded", {
                let mut v = full_locations(&lay);
                v[0].workers = (0..=MAX_LOCATIONS_PER_BLOCK as u32).map(worker).collect();
                v
            }),
        ];
        for (name, blocks) in cases {
            let err = service
                .commit(CacheCommitParams {
                    incarnation: 1,
                    key: "/k",
                    generation: 1,
                    object_id: OBJ,
                    len,
                    ufs_mtime: 777,
                    ttl_ms: 0,
                    blocks,
                })
                .unwrap_err();
            assert!(
                !format!("{}", err).contains("raft"),
                "{}: must be rejected before the barrier, got: {}",
                name,
                err
            );
        }

        // A perfect set reaches the barrier, which fails closed in
        // testing mode (no raft cluster) — proving no path bypasses the
        // sync propose.
        let err = service
            .commit(CacheCommitParams {
                incarnation: 1,
                key: "/k",
                generation: 1,
                object_id: OBJ,
                len,
                ufs_mtime: 777,
                ttl_ms: 0,
                blocks: full_locations(&lay),
            })
            .unwrap_err();
        assert!(
            format!("{}", err).contains("raft"),
            "well-formed commit must reach the (fail-closed) barrier: {}",
            err
        );
    }

    #[test]
    fn test_commit_rejects_identity_mismatch() {
        let service = build_service("commit-identity");
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            let mgr = &store.cache;
            mgr.apply_id_reserve(rocks, token(1, 1), OBJ, OBJ + 100)
                .unwrap();
            let alloc = CacheEntry {
                generation: 1,
                state: CacheEntryState::Reserved,
                object_id: OBJ,
                len: 0,
                ufs_mtime: 0,
                block_size: 64,
                expire_at: 0,
            };
            mgr.apply_allocate(rocks, token(2, 1), 1, "/k", &alloc)
                .unwrap();
        }
        let lay = layout(OBJ, 64);
        let blocks = full_locations(&lay);
        fn params<'a>(
            generation: u64,
            object_id: i64,
            key: &'a str,
            blocks: Vec<CacheBlockLocation>,
        ) -> CacheCommitParams<'a> {
            CacheCommitParams {
                incarnation: 1,
                key,
                generation,
                object_id,
                len: 64,
                ufs_mtime: 777,
                ttl_ms: 0,
                blocks,
            }
        }
        // Wrong generation.
        assert!(service
            .commit(params(2, OBJ, "/k", blocks.clone()))
            .is_err());
        // Wrong object id.
        assert!(service
            .commit(params(1, OBJ + 5, "/k", blocks.clone()))
            .is_err());
        // Missing entry.
        assert!(service.commit(params(1, OBJ, "/missing", blocks)).is_err());
    }

    /// Allocate fails closed end to end in testing mode (the reserve
    // propose needs a real raft cluster), and pre-barrier entry-state
    /// checks reject live rows before any issuance.
    #[test]
    fn test_allocate_fails_closed_and_rejects_live_entry() {
        let service = build_service("allocate-fail-closed");

        // Live entry -> rejected before the barrier.
        committed_entry(&service, "/k", OBJ, 64, 0);
        let err = service.allocate(token(3, 1), 1, "/k", 64).unwrap_err();
        assert!(format!("{}", err).contains("live entry"), "{}", err);

        // Invalid block size -> rejected immediately.
        assert!(service.allocate(token(3, 2), 1, "/new", 0).is_err());

        // Fresh key: the issuer needs a reserve first and the barrier
        // fails closed without a raft cluster.
        let err = service.allocate(token(3, 3), 1, "/new", 64).unwrap_err();
        assert!(
            format!("{}", err).contains("raft"),
            "allocate must go through the fail-closed sync barrier: {}",
            err
        );
    }
}
