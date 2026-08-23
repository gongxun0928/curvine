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
//! The object-id issuer is leader-only and owns a volatile segment cursor
//! `{next, end, epoch}`: ids are consumed strictly inside the durably
//! reserved segment, one at a time, and the whole tail is burned when the
//! segment is exhausted, when the durable reserve watermark moves past it
//! (another leader reserved onward), or when the leadership epoch changed
//! (lost-and-regained leadership, even invisibly between two RPCs). The
//! wire has no object id field, so clients can never supply their own.
//!
//! Allocate plans the whole placement master-side (bounded worker sets
//! per derived block) and returns it with the identity; the plan is also
//! kept in a volatile table keyed by the load token. Commit reports the
//! locations that actually succeeded and validates them against that plan
//! (planned workers only, deduplicated, one entry per block); without the
//! plan (master restart) the commit fails closed as a retryable miss — a
//! retry may never silently swap placements.
//!
//! Block locations are volatile master state (contract: a full block
//! report may restore lost locations but must never resurrect a `Valid`
//! CacheIndex row). They are not journaled: `CacheGet` treats any missing
//! block location as a whole-object miss so the caller falls back to the
//! UFS.

use crate::master::fs::policy::ChooseContext;
use crate::master::fs::WorkerManager;
use crate::master::journal::{
    CacheAllocateEntry, CacheCommitEntry, CacheIdReserveEntry, CacheIncarnationAllocateV2Entry,
    CacheIncarnationRevokeEntry, CacheRemoveEntry, JournalEntry, JournalWriter,
};
use crate::master::meta::cache::entry::{CacheEntry, CacheEntryState, OpOutcome, OpToken};
use crate::master::meta::cache::state_tags;
use crate::master::meta::cache::LocalCacheIndexStore;
use crate::master::meta::cache::MAX_ISSUABLE_INCARNATION;
use crate::master::meta::{BlockIdCodec, CacheBlockLayout};
use crate::master::{MasterMonitor, SyncFsDir};
use curvine_core_error::{err_box, err_msg, CommonError, CommonResult};
use curvine_model::WorkerAddress;
use curvine_runtime::common::LocalTime;
use curvine_runtime::sync::ArcRwLock;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Object ids per durable reserve segment. Reserves are rare (one per
/// segment consumed), so the per-reserve outcome rows of the issuer
/// client stay trivially small.
const CACHE_RESERVE_SEGMENT: i64 = 4096;

/// Hard cap on block location entries per commit / placement. Bounded so
/// a malformed commit can never make the master build an unbounded
/// response or volatile table (contract: bounded ops).
pub const MAX_COMMIT_BLOCKS: usize = 1 << 16;

/// Hard cap on replica locations per block.
pub const MAX_LOCATIONS_PER_BLOCK: usize = 16;

/// Hard cap on the UTF-8 size of a cache key across all cache RPCs.
pub const MAX_KEY_BYTES: usize = 4096;

/// Conservative cap on the serialized byte size of an Allocate response
/// plan (65536 blocks x 16 workers x variable-length addresses can exceed
/// the transport's 16 MiB header cap, which only bounds INBOUND messages).
/// The plan is estimated and rejected BEFORE any object id is issued.
pub const MAX_PLAN_WIRE_BYTES: usize = 8 << 20;

/// The internal client identity used for segment reserves. It is disjoint
/// from any RPC client id space in use (client ids are random u64s; the
/// watermark of this client only advances via reserves, which lazily
/// evicts older reserve outcome rows once window eviction lands in 4c).
/// RPC clients may not present this id.
const CACHE_ISSUER_CLIENT_ID: u64 = 0;

/// Terminal status of a conditional cache mutation (contract §3 rev2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOpStatus {
    /// This command's expected generation matched and it applied now.
    Applied,
    /// The committed state already reflects this command (replay/retry).
    AlreadyApplied,
    /// A later generation advanced; the command (and its load) is dead.
    /// Carries the fence the caller expected and the generation the
    /// committed row has advanced to, so the caller can terminate the old
    /// load and diagnostics can show the fence gap.
    Superseded { expected: u64, current: u64 },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CacheBlockLocation {
    pub block_id: i64,
    pub block_len: i64,
    pub workers: Vec<WorkerAddress>,
}

#[derive(Debug, Clone)]
pub struct CacheGetResult {
    pub object_id: i64,
    pub len: i64,
    pub block_size: i64,
    pub generation: u64,
    pub ufs_mtime: i64,
    pub expire_at: i64,
    pub blocks: Vec<CacheBlockLocation>,
}

#[derive(Debug, Clone)]
pub struct CacheAllocateResult {
    pub object_id: i64,
    pub generation: u64,
    /// Master-planned placement: one entry per derived block (empty for a
    /// zero-length object). On an exact retry whose volatile plan was lost
    /// to a master restart, a fresh plan is regenerated for the SAME
    /// identity — a second identity is never minted.
    pub blocks: Vec<CacheBlockLocation>,
}

/// Arguments of one commit RPC (keeps the service signature clippy-sized).
#[derive(Debug, Clone)]
pub struct CacheCommitParams<'a> {
    /// Durable idempotency token of THIS commit operation.
    pub token: OpToken,
    /// The load identity token from CacheAllocate; binds the commit to
    /// its recorded Allocated outcome and volatile plan.
    pub load_token: OpToken,
    pub rpc_id: i64,
    pub incarnation: u64,
    pub key: &'a str,
    pub generation: u64,
    pub object_id: i64,
    pub len: i64,
    pub ufs_mtime: i64,
    pub ttl_ms: i64,
    pub blocks: Vec<CacheBlockLocation>,
}

/// Worker selection for cache placements: one bounded worker set per
/// block. The production implementation goes through the real worker
/// policy (availability/capacity aware); tests install a fixed chooser.
/// `replica_policy` is the minimum successful locations every planned
/// block must carry (and every commit must confirm).
pub trait CacheWorkerChooser: Send + Sync {
    fn choose_block(&self, block_size: i64) -> CommonResult<Vec<WorkerAddress>>;
    fn replica_policy(&self) -> usize;
}

/// Production chooser: the cluster worker policy behind the master's
/// worker manager, at the server-configured cache replication factor
/// (`ClusterConf.client.replicas`, validated against the master's
/// min/max replication at construction). A policy that cannot satisfy
/// the configured replica count (too few live/capable workers) fails
/// the placement instead of planning under-replicated blocks.
pub struct PolicyWorkerChooser {
    workers: ArcRwLock<WorkerManager>,
    replicas: u16,
}

impl PolicyWorkerChooser {
    pub fn new(workers: ArcRwLock<WorkerManager>, replicas: u16) -> Self {
        Self { workers, replicas }
    }
}

impl CacheWorkerChooser for PolicyWorkerChooser {
    fn choose_block(&self, block_size: i64) -> CommonResult<Vec<WorkerAddress>> {
        let wm = self.workers.read();
        let chosen = wm.choose_worker(ChooseContext::with_num(
            self.replicas,
            block_size,
            Vec::new(),
        ))?;
        drop(wm);
        if (chosen.len() as u16) < self.replicas {
            return err_box!(
                "cache placement could only choose {} of {} required replicas",
                chosen.len(),
                self.replicas
            );
        }
        Ok(chosen)
    }

    fn replica_policy(&self) -> usize {
        self.replicas as usize
    }
}

#[derive(Default)]
struct ObjectLocations {
    blocks: HashMap<i64, Vec<WorkerAddress>>,
}

/// Volatile leader-scoped segment cursor for the object id issuer.
#[derive(Debug, Clone, Copy)]
struct Segment {
    /// Next unconsumed id (issue `next`, then `next += 1`).
    next: i64,
    /// Exclusive segment end; `durable_hw == end - 1` while this segment
    /// is the latest reserve.
    end: i64,
    /// Leadership epoch observed when the segment was reserved. Any
    /// difference from the current epoch means leadership was lost (and
    /// possibly regained) since: burn the tail.
    epoch: u64,
}

/// Volatile master-planned placement of one load, keyed by its allocate
/// (load) token. Lost on restart — a commit without its plan fails closed
/// unless the exact allocate is replayed to re-generate one.
#[derive(Debug, Clone)]
struct LoadPlan {
    object_id: i64,
    generation: u64,
    file_len: i64,
    block_size: i64,
    /// Minimum successful locations per block (the chooser's replica
    /// policy at planning time): commit evidence must reach it.
    replicas: usize,
    blocks: Vec<CacheBlockLocation>,
}

pub struct CacheService {
    fs_dir: SyncFsDir,
    journal_writer: Arc<JournalWriter>,
    monitor: MasterMonitor,
    chooser: Arc<dyn CacheWorkerChooser>,
    /// Production-default-false capability gate (4b gate 5): every cache
    /// entry point rejects until `master.cache_metadata_enabled` is flipped
    /// (the atomic flip lands with task #6). Tests construct the service
    /// with the gate explicitly enabled.
    enabled: bool,
    /// Serializes the reserve+issue critical section so concurrent
    /// allocates consume strictly monotonic, unique ids.
    issue_lock: Mutex<()>,
    /// Leader-scoped segment cursor (see `Segment`). None = burned.
    segment: Mutex<Option<Segment>>,
    /// Volatile load plans by allocate token.
    plans: Mutex<HashMap<OpToken, LoadPlan>>,
    locations: Mutex<HashMap<i64, ObjectLocations>>,
    /// One-shot fault-injection seam fired between a sync-propose
    /// barrier's return and the code's post-barrier verification (reserve
    /// epoch check / commit-invalidate readback). Tests use it to make
    /// "another mutation raced the barrier" deterministic. Never set in
    /// production; compiled out entirely outside `cfg(test)`.
    #[cfg(test)]
    barrier_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

impl CacheService {
    pub fn new(
        fs_dir: SyncFsDir,
        journal_writer: Arc<JournalWriter>,
        monitor: MasterMonitor,
        chooser: Arc<dyn CacheWorkerChooser>,
        enabled: bool,
    ) -> Self {
        Self {
            fs_dir,
            journal_writer,
            monitor,
            chooser,
            enabled,
            issue_lock: Mutex::new(()),
            segment: Mutex::new(None),
            plans: Mutex::new(HashMap::new()),
            locations: Mutex::new(HashMap::new()),
            #[cfg(test)]
            barrier_hook: Mutex::new(None),
        }
    }

    /// Capability gate (4b gate 5): default-off in production, every cache
    /// entry point rejects until explicitly enabled.
    fn require_enabled(&self) -> CommonResult<()> {
        if !self.enabled {
            return err_box!(
                "cache metadata capability is disabled (master.cache_metadata_enabled=false)"
            );
        }
        Ok(())
    }

    /// Service-side incarnation fence read (4b P0-1/P0-2): mirrors the
    /// apply-time fence. `Ok(false)` = revoked or stale (a newer incarnation
    /// owns the mount): terminal for the caller, never retried against the
    /// same incarnation.
    fn incarnation_active(&self, incarnation: u64) -> CommonResult<bool> {
        let store = self.fs_dir.read();
        let rocks = store.get_rocks_store();
        match rocks.cache_get_incarnation(incarnation).map_err(fs_err)? {
            Some(row) if !row.revoked => Ok(rocks
                .cache_current_incarnation(row.mount_id)
                .map_err(fs_err)?
                == Some(incarnation)),
            _ => Ok(false),
        }
    }

    /// Terminal revoked/stale diagnostic shared by every fenced path
    /// (get/allocate/commit/invalidate). TYPED: a boxed
    /// FsError::CacheIncarnationFenced, so the handler `?` (From<
    /// CommonError> downcasts and preserves the FsError) and the wire
    /// encode/decode keep a machine-recognizable ErrorKind — clients
    /// branch on the kind, never on the message string.
    fn fenced(incarnation: u64) -> CommonError {
        curvine_error::FsError::cache_incarnation_fenced(incarnation).into()
    }

    /// Arm the one-shot post-barrier fault-injection seam (test-only).
    /// The hook fires exactly once, at the first sync-propose barrier
    /// return after arming, before that call's post-barrier verification.
    #[cfg(test)]
    pub(crate) fn set_barrier_hook(&self, hook: Box<dyn Fn() + Send + Sync>) {
        *self.barrier_hook.lock().unwrap() = Some(hook);
    }

    /// Fire (and disarm) the barrier hook if armed. The guard is released
    /// before invoking the hook: a hook that re-enters the service (e.g. a
    /// nested invalidate proposing its own command) must not re-lock this
    /// mutex on the same thread.
    #[cfg(test)]
    fn fire_barrier_hook(&self) {
        let hook = self.barrier_hook.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Production no-op: the seam is compiled out outside tests, so a
    /// cache barrier never pays an extra mutex lock.
    #[cfg(not(test))]
    #[inline(always)]
    fn fire_barrier_hook(&self) {}

    /// Whole-object lookup. `hit` requires a `Valid`, unexpired entry AND
    /// (when `need_locations`) a complete volatile location set for the
    /// derived block layout — anything missing is a miss (caller falls
    /// back to the UFS).
    pub fn get(
        &self,
        incarnation: u64,
        key: &str,
        need_locations: bool,
    ) -> CommonResult<Option<CacheGetResult>> {
        self.require_enabled()?;
        validate_key(key)?;
        // Incarnation gate first (4b P0-2, no-token path), fail-closed
        // (gate 2): a missing, revoked, or stale namespace is a TYPED
        // TERMINAL error — never a plain miss. Folding it into a miss
        // would silently send the caller to the UFS fallback under a dead
        // namespace; the caller must re-resolve the mount instead.
        if !self.incarnation_active(incarnation)? {
            return Err(Self::fenced(incarnation));
        }
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
        let mut blocks = Vec::new();
        if need_locations {
            let locations = self.locations.lock().unwrap();
            let Some(object_locations) = locations.get(&entry.object_id) else {
                return Ok(None);
            };
            if object_locations.blocks.len() != layout.block_count as usize {
                return Ok(None);
            }
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
        }
        Ok(Some(CacheGetResult {
            object_id: entry.object_id,
            len: entry.len,
            block_size: entry.block_size,
            generation: entry.generation,
            ufs_mtime: entry.ufs_mtime,
            expire_at: entry.expire_at,
            blocks,
        }))
    }

    /// Allocate a fresh cache object for `key`: leader-issued identity
    /// `(object_id, generation)` plus the master-planned whole-object
    /// placement. The client cannot influence the object id; generation
    /// is the next absolute transition of the entry row (None -> 1,
    /// Tombstoned@g -> g+1). `file_len == 0` is a legal empty object
    /// (empty placement, empty commit evidence).
    pub fn allocate(
        &self,
        token: OpToken,
        rpc_id: i64,
        incarnation: u64,
        key: &str,
        file_len: i64,
        block_size: i64,
    ) -> CommonResult<CacheAllocateResult> {
        self.require_enabled()?;
        self.require_leader_or_burn()?;
        validate_key(key)?;
        validate_client_token(token)?;
        if file_len < 0 {
            return err_box!("cache allocate file length must be >= 0: {}", file_len);
        }
        if block_size <= 0 {
            return err_box!("cache allocate block size must be positive: {}", block_size);
        }
        // Bounded layout, checked before any identity is issued (a
        // layout the wire cannot carry must never burn an object id).
        let block_count = if file_len == 0 {
            0
        } else {
            (file_len - 1) / block_size + 1
        };
        if block_count as usize > MAX_COMMIT_BLOCKS {
            return err_box!(
                "cache allocate derived block count {} exceeds cap {} (len {}, block size {})",
                block_count,
                MAX_COMMIT_BLOCKS,
                file_len,
                block_size
            );
        }

        // Serialize issuance: reserve + consume must be one critical
        // section for uniqueness and in-segment monotonicity.
        let _guard = self.issue_lock.lock().unwrap();

        // Idempotent retry, classified from committed state only: the
        // token outcome is read (and its immutable payload compared) BEFORE
        // any entry-state check, incarnation gate, or issuance, so a retry
        // after a lost response replays the EXACT recorded geometry; if the
        // volatile plan was lost to a master restart a fresh plan is
        // generated for the SAME identity (a second identity is never
        // minted, and the placement is never silently swapped behind a
        // commit: commit still validates against the live plan). An exact
        // match only RECORDS the replay below — the incarnation gate then
        // decides whether that history may still be handed back to the
        // client (4b P0-2: a revoked/stale namespace is terminal even for
        // an exact retry; only divergence reporting precedes the fence).
        let mut replay: Option<(i64, u64)> = None; // (object_id, generation)
        {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            match rocks.cache_get_outcome(token).map_err(fs_err)? {
                Some(OpOutcome::Allocated {
                    incarnation: out_inc,
                    key: out_key,
                    generation,
                    object_id,
                    file_len: out_len,
                    block_size: out_bs,
                }) => {
                    if out_inc != incarnation
                        || out_key != key
                        || out_len != file_len
                        || out_bs != block_size
                    {
                        return err_box!(
                            "cache allocate token {:?} replayed with different parameters ({}, {}, len {}, bs {}): committed ({}, {})@{} object {} len {} bs {}",
                            token,
                            incarnation,
                            key,
                            file_len,
                            block_size,
                            out_inc,
                            out_key,
                            generation,
                            object_id,
                            out_len,
                            out_bs
                        );
                    }
                    replay = Some((object_id, generation));
                }
                Some(other) => {
                    return err_box!(
                        "cache allocate token {:?} has a non-allocate committed outcome: {:?}",
                        token,
                        other
                    )
                }
                None => {
                    // Below the client watermark with no outcome: the
                    // outcome window has moved past this token — its
                    // identity (if any ever existed) cannot be recovered.
                    let watermark = rocks
                        .cache_client_watermark(token.client_id)
                        .map_err(fs_err)?;
                    if let Some(hw) = watermark {
                        if token.op_seq <= hw {
                            return err_box!(
                                "cache allocate token {:?} is expired (client watermark {}): terminal, re-issue with a fresh token",
                                token,
                                hw
                            );
                        }
                    }
                }
            }
        }

        // Incarnation gate (4b P0-2): divergence is detected from the
        // recorded outcome first (a token replayed with different
        // parameters is loud divergence, never a generic fenced error);
        // the gate itself is terminal for this allocate EVEN for an exact
        // recorded retry — a revoked or stale namespace never hands its
        // identities back to the client.
        if !self.incarnation_active(incarnation)? {
            return Err(Self::fenced(incarnation));
        }

        // Exact recorded replay: resolve from history now that the
        // namespace is confirmed live.
        if let Some((object_id, generation)) = replay {
            // Scoped lock: the guard must be released before the replan
            // arm, which takes the plans lock again.
            let prior = {
                let plans = self.plans.lock().unwrap();
                plans.get(&token).map(|plan| plan.blocks.clone())
            };
            let blocks = match prior {
                Some(blocks) => blocks,
                None => {
                    // Plan lost (master restart): regenerate a fresh
                    // volatile plan for the same identity.
                    let layout = CacheBlockLayout::derive(object_id, file_len, block_size)?;
                    self.replan(token, generation, layout)?
                }
            };
            return Ok(CacheAllocateResult {
                object_id,
                generation,
                blocks,
            });
        }

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

        // Plan the placement before any id is issued: a failed worker
        // selection (no workers / below replica policy) must not burn an
        // object id.
        let layout_worker_sets = self.plan_worker_sets(block_count as usize, block_size)?;
        // Wire-size gate before issuance: the response carries the whole
        // plan, and the transport's inbound cap does not protect responses.
        // A plan whose serialized estimate exceeds the cap is rejected
        // without issuing (or burning) any identity.
        let plan_wire_estimate = estimate_plan_wire_bytes(&layout_worker_sets);
        if plan_wire_estimate > MAX_PLAN_WIRE_BYTES {
            return err_box!(
                "cache allocate plan would serialize to ~{} bytes, above the response cap {}: block count {}, file_len {}",
                plan_wire_estimate,
                MAX_PLAN_WIRE_BYTES,
                block_count,
                file_len
            );
        }
        let replicas = self.chooser.replica_policy();

        // Ensure the volatile segment cursor is valid (burning stale
        // segments first) and consume one id (under the epoch it was
        // consumed in).
        let (issued, issued_epoch) = self.ensure_segment_and_issue(rpc_id)?;

        let layout = CacheBlockLayout::derive(issued, file_len, block_size)?;
        let mut planned_blocks = Vec::with_capacity(block_count as usize);
        for (index, workers) in layout_worker_sets.into_iter().enumerate() {
            planned_blocks.push(CacheBlockLocation {
                block_id: layout.block_id((index + 1) as i64)?,
                block_len: if (index + 1) as i64 == layout.block_count {
                    layout.last_len
                } else {
                    layout.block_size
                },
                workers,
            });
        }

        // Leadership may have changed (even invisibly) between the issue
        // and this propose: the entry must be proposed by the same leader
        // epoch that consumed the id, so re-verify and burn on mismatch.
        if !self.monitor.is_active() || self.monitor.journal_epoch() != issued_epoch {
            *self.segment.lock().unwrap() = None;
            return err_box!(
                "cache allocate lost leadership after issuance (epoch {} -> {}): retry with the same token",
                issued_epoch,
                self.monitor.journal_epoch()
            );
        }

        let entry = CacheEntry {
            generation,
            state: CacheEntryState::Reserved,
            object_id: issued,
            len: 0,
            ufs_mtime: 0,
            block_size,
            expire_at: 0,
        };
        let op_id = self.fs_dir.read().next_op_id();
        let journal_entry = JournalEntry::CacheAllocate(CacheAllocateEntry {
            op_id,
            rpc_id,
            token,
            incarnation,
            key: key.to_string(),
            file_len,
            entry,
        });
        self.journal_writer
            .sync_propose_cache(journal_entry)
            .map_err(fs_err)?;

        // Barrier readback: if a revoke/remount fenced the incarnation
        // while the barrier ran, the apply was a deterministic no-op and
        // no outcome exists — report the terminal fence, not a generic
        // readback failure.
        if !self.incarnation_active(incarnation)? {
            return Err(Self::fenced(incarnation));
        }

        // Resolve the RPC from committed state only: the answer is the
        // committed outcome itself, never the locally issued id — if the
        // FSM recorded a different identity (an already-applied earlier
        // execution of this token), that committed answer wins and the
        // burned local id is never handed to the client.
        let outcome = {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            rocks.cache_get_outcome(token).map_err(fs_err)?
        };
        match outcome {
            Some(OpOutcome::Allocated {
                incarnation: out_inc,
                key: out_key,
                object_id,
                generation: out_generation,
                ..
            }) if out_inc == incarnation && out_key == key && out_generation == generation => {
                // P2-1 (4a review): never trust the locally issued identity
                // for the wire answer or the volatile plan. If the durable
                // outcome records a different object id (an earlier
                // execution of this token already won it), re-derive the
                // block layout from the committed identity — same geometry,
                // same chosen worker sets — so no block id can reference
                // the burned local id.
                let blocks = if object_id == issued {
                    planned_blocks
                } else {
                    Self::rebuild_blocks_for_identity(
                        planned_blocks,
                        object_id,
                        file_len,
                        block_size,
                    )?
                };
                self.plans.lock().unwrap().insert(
                    token,
                    LoadPlan {
                        object_id,
                        generation: out_generation,
                        file_len,
                        block_size,
                        replicas,
                        blocks: blocks.clone(),
                    },
                );
                Ok(CacheAllocateResult {
                    object_id,
                    generation: out_generation,
                    blocks,
                })
            }
            other => err_box!(
                "cache allocate barrier readback failed for token {:?} ({}, {}): {:?}",
                token,
                incarnation,
                key,
                other
            ),
        }
    }

    /// Regenerate a volatile plan for an already-committed (durable)
    /// allocation: same object identity and geometry, fresh worker sets.
    /// Used by an exact allocate retry whose plan was lost to a master
    /// restart — a second identity is never minted, and any in-flight
    /// commit from the previous incarnation of this master fails closed
    /// against the plan table either way.
    fn replan(
        &self,
        token: OpToken,
        generation: u64,
        layout: CacheBlockLayout,
    ) -> CommonResult<Vec<CacheBlockLocation>> {
        let sets = self.plan_worker_sets(layout.block_count as usize, layout.block_size)?;
        let replicas = self.chooser.replica_policy();
        let mut blocks = Vec::with_capacity(layout.block_count as usize);
        for (index, workers) in sets.into_iter().enumerate() {
            blocks.push(CacheBlockLocation {
                block_id: layout.block_id((index + 1) as i64)?,
                block_len: if (index + 1) as i64 == layout.block_count {
                    layout.last_len
                } else {
                    layout.block_size
                },
                workers,
            });
        }
        self.plans.lock().unwrap().insert(
            token,
            LoadPlan {
                object_id: layout.object_id,
                generation,
                file_len: layout.len,
                block_size: layout.block_size,
                replicas,
                blocks: blocks.clone(),
            },
        );
        Ok(blocks)
    }

    /// P2-1 (4a review): re-derive the block layout of a plan from a
    /// committed object identity, preserving the chosen worker sets and
    /// geometry. The volatile `issued` identity is never trusted for the
    /// wire answer or the stored plan when the durable outcome records a
    /// different (earlier, winning) identity.
    fn rebuild_blocks_for_identity(
        planned: Vec<CacheBlockLocation>,
        object_id: i64,
        file_len: i64,
        block_size: i64,
    ) -> CommonResult<Vec<CacheBlockLocation>> {
        let committed = CacheBlockLayout::derive(object_id, file_len, block_size)?;
        let mut rebuilt = Vec::with_capacity(planned.len());
        for (index, mut loc) in planned.into_iter().enumerate() {
            loc.block_id = committed.block_id((index + 1) as i64)?;
            loc.block_len = if (index + 1) as i64 == committed.block_count {
                committed.last_len
            } else {
                committed.block_size
            };
            rebuilt.push(loc);
        }
        Ok(rebuilt)
    }

    /// Commit the loaded object: `Reserved@g` -> `Valid` with the final
    /// `(len, ufs_mtime)` and the locations that actually succeeded. The
    /// reported locations must match the allocate plan bound to the load
    /// token (same blocks, planned workers only, deduplicated) and are
    /// published only after the barrier readback confirms `Valid`.
    pub fn commit(&self, params: CacheCommitParams<'_>) -> CommonResult<CacheOpStatus> {
        let CacheCommitParams {
            token,
            load_token,
            rpc_id,
            incarnation,
            key,
            generation,
            object_id,
            len,
            ufs_mtime,
            ttl_ms,
            blocks,
        } = params;
        self.require_enabled()?;
        self.require_leader()?;
        validate_key(key)?;
        validate_client_token(token)?;
        validate_client_token(load_token)?;
        if ttl_ms != 0 {
            return err_box!(
                "cache commit ttl is not supported until mount-policy TTL lands (4b): ttl_ms must be 0, got {}",
                ttl_ms
            );
        }

        // 4b P0-3: the expiry deadline is derived ONCE, from the
        // incarnation's frozen durable policy — never from the client and
        // never from a later mutable mount table entry — and ONLY for an
        // outcome-free fresh commit (the None arm below). An exact
        // outcome retry reuses the recorded absolute value bit-exactly and
        // NEVER recomputes: the policy row is not even read, so a retry
        // cannot fail on clock drift or a ttl overflow that did not exist
        // when the commit first applied.
        #[allow(unused_assignments)] // the 0 is never read: exact replay and
        // the fresh-derivation arm below both assign before any read
        let mut expire_at: i64 = 0;

        // Commit-token durable idempotency first: a retry after a lost
        // response resolves to its recorded Committed outcome. The outcome
        // binds the FULL immutable request (load token + geometry + fence):
        // only an exact match of every field is a replay — a token replayed
        // with any different parameter is divergence, never a silent
        // AlreadyApplied. An exact match does NOT short-circuit here,
        // though: Superseded is terminal, so the retry is still classified
        // from the committed row below. If a later generation fenced the
        // entry after this commit recorded its outcome, the row (missing /
        // Tombstoned / advanced) answers Superseded; only a same-generation
        // exact Valid row answers AlreadyApplied.
        //
        // 4b P0-3: the recorded absolute `expire_at` is authoritative — an
        // exact retry reuses it bit-exactly so the deadline can never drift
        // across retries, and the comparison deliberately excludes it (a
        // recomputed value that differs only by retry latency is still an
        // exact replay).
        let mut committed_outcome_exact = false;
        {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            match rocks.cache_get_outcome(token).map_err(fs_err)? {
                Some(OpOutcome::Committed {
                    incarnation: out_inc,
                    key: out_key,
                    generation: out_generation,
                    object_id: out_object,
                    load_token: out_load_token,
                    len: out_len,
                    ufs_mtime: out_mtime,
                    expire_at: out_expire_at,
                }) => {
                    if out_inc == incarnation
                        && out_key == key
                        && out_generation == generation
                        && out_object == object_id
                        && out_load_token == load_token
                        && out_len == len
                        && out_mtime == ufs_mtime
                    {
                        committed_outcome_exact = true;
                        expire_at = out_expire_at;
                    } else {
                        return err_box!(
                        "cache commit token {:?} replayed with different parameters: committed ({}, {})@{} object {} load {:?} len {} mtime {} expire_at {}",
                        token,
                        out_inc,
                        out_key,
                        out_generation,
                        out_object,
                        out_load_token,
                        out_len,
                        out_mtime,
                        out_expire_at
                    );
                    }
                }
                Some(other) => {
                    return err_box!(
                        "cache commit token {:?} has a non-commit committed outcome: {:?}",
                        token,
                        other
                    )
                }
                None => {
                    let watermark = rocks
                        .cache_client_watermark(token.client_id)
                        .map_err(fs_err)?;
                    if let Some(hw) = watermark {
                        if token.op_seq <= hw {
                            return err_box!(
                                "cache commit token {:?} is expired (client watermark {}): terminal, the load must be restarted with a fresh token",
                                token,
                                hw
                            );
                        }
                    }
                    // Fresh commit (no outcome): derive the absolute
                    // deadline ONCE from the frozen policy row.
                    // Fail-closed on an unsatisfiable deadline — the
                    // entry is rejected before any propose.
                    let policy_ttl_ms = rocks
                        .cache_get_incarnation_policy(incarnation)
                        .map_err(fs_err)?
                        .map(|p| p.ttl_ms)
                        .unwrap_or(0);
                    expire_at = if policy_ttl_ms == 0 {
                        0
                    } else {
                        (LocalTime::mills() as i64)
                            .checked_add(policy_ttl_ms)
                            .ok_or_else(|| {
                                cm_err(format!(
                                    "cache commit expire_at overflow: now + ttl {} ms",
                                    policy_ttl_ms
                                ))
                            })?
                    };
                }
            }
        }

        // Incarnation gate (4b P0-2): ordered AFTER the token outcome read
        // — divergence is detected from the recorded outcome first (a token
        // replayed with different parameters is loud divergence, never a
        // generic fenced error) — and BEFORE the load binding / row
        // classification. In a live namespace the classification below
        // resolves an exact recorded retry (Superseded/AlreadyApplied) from
        // durable state; under a revoked or stale incarnation the load is
        // terminal and dies with its namespace.
        if !self.incarnation_active(incarnation)? {
            return Err(Self::fenced(incarnation));
        }

        // Load binding: the durable Allocated outcome must record exactly
        // this load (identity AND geometry). The manager re-verifies this
        // atomically at apply; checking here keeps divergence off the wire.
        {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            match rocks.cache_get_outcome(load_token).map_err(fs_err)? {
                Some(OpOutcome::Allocated {
                    incarnation: out_inc,
                    key: out_key,
                    generation: out_generation,
                    object_id: out_object,
                    file_len: out_len,
                    ..
                }) => {
                    if out_inc != incarnation
                        || out_key != key
                        || out_generation != generation
                        || out_object != object_id
                        || out_len != len
                    {
                        return err_box!(
                            "cache commit does not match its recorded load allocation for load token {:?}: ({}, {})@{} object {} len {}",
                            load_token,
                            incarnation,
                            key,
                            generation,
                            object_id,
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
        }

        // Committed row classification — BEFORE the volatile plan lookup:
        // terminal states must resolve from durable state alone. If the
        // plan were consulted first, a retry whose Superseded response was
        // lost (and whose plan was already cleared by the fencing
        // invalidate/remove, or by a restart) would report a retryable
        // "no live plan" miss instead of the terminal Superseded the
        // committed row still proves. The load binding above verified the
        // token's identity, so clearing the plan below can never delete
        // another load's plan.
        {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            match rocks.cache_get_entry(incarnation, key).map_err(fs_err)? {
                None => {
                    drop(store);
                    self.plans.lock().unwrap().remove(&load_token);
                    return Ok(CacheOpStatus::Superseded {
                        expected: generation,
                        current: 0,
                    });
                }
                Some(cur) if cur.generation > generation => {
                    drop(store);
                    self.plans.lock().unwrap().remove(&load_token);
                    return Ok(CacheOpStatus::Superseded {
                        expected: generation,
                        current: cur.generation,
                    });
                }
                Some(cur) if cur.generation < generation => {
                    return err_box!(
                        "cache commit generation {} is beyond the committed row generation {} for ({}, {})",
                        generation,
                        cur.generation,
                        incarnation,
                        key
                    );
                }
                Some(cur) if cur.object_id != object_id => {
                    return err_box!(
                        "cache commit identity mismatch for ({}, {})@{}: committed object {} vs expected {}",
                        incarnation,
                        key,
                        generation,
                        cur.object_id,
                        object_id
                    );
                }
                Some(cur) if cur.state == CacheEntryState::Tombstoned => {
                    drop(store);
                    self.plans.lock().unwrap().remove(&load_token);
                    return Ok(CacheOpStatus::Superseded {
                        expected: generation,
                        current: cur.generation,
                    });
                }
                Some(cur) if cur.state == CacheEntryState::Valid => {
                    // Same generation and object: only this load's commit
                    // could have written it.
                    if cur.len == len && cur.ufs_mtime == ufs_mtime && cur.expire_at == expire_at {
                        drop(store);
                        self.plans.lock().unwrap().remove(&load_token);
                        return Ok(CacheOpStatus::AlreadyApplied);
                    }
                    return err_box!(
                        "cache commit parameter divergence for ({}, {})@{}: committed len {} mtime {} expire_at {} vs request len {} mtime {} expire_at {}",
                        incarnation,
                        key,
                        generation,
                        cur.len,
                        cur.ufs_mtime,
                        cur.expire_at,
                        len,
                        ufs_mtime,
                        expire_at
                    );
                }
                Some(_) if committed_outcome_exact => {
                    // Unreachable in practice: the commit entry writes the
                    // Committed outcome and the Valid row atomically. If a
                    // Reserved row ever shows up here at the exact recorded
                    // identity, resolve idempotently and spend the plan.
                    drop(store);
                    self.plans.lock().unwrap().remove(&load_token);
                    return Ok(CacheOpStatus::AlreadyApplied);
                }
                Some(_) => (), // Reserved at the right identity: apply.
            }
        }

        // The volatile plan is mandatory: without it the reported
        // locations cannot be validated against what the master planned
        // (master restart), and a silent re-plan would misattribute old
        // writes. Fail closed as a retryable miss — the caller retries the
        // exact allocate (which re-plans the same identity) and re-commits.
        let plan = {
            let plans = self.plans.lock().unwrap();
            plans
                .get(&load_token)
                .cloned()
                .ok_or_else(|| {
                    cm_err(format!(
                        "cache commit for load token {:?} has no live plan (master restart or unknown token): retryable miss, replay the exact allocate to re-plan, then re-commit",
                        load_token
                    ))
                })?
        };
        if plan.object_id != object_id || plan.generation != generation {
            return err_box!(
                "cache commit identity does not match the load plan: request ({}, {}) vs plan ({}, {})",
                generation,
                object_id,
                plan.generation,
                plan.object_id
            );
        }
        if plan.file_len != len {
            return err_box!(
                "cache commit file length {} differs from the allocated plan {}",
                len,
                plan.file_len
            );
        }

        // Evidence validation against the plan happens before the
        // barrier: every rejected set below never touches the journal.
        validate_commit_against_plan(&plan, &blocks)?;

        let op_id = self.fs_dir.read().next_op_id();
        let entry = JournalEntry::CacheCommit(CacheCommitEntry {
            op_id,
            rpc_id,
            token,
            load_token,
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
        // Test seam: "another mutation raced the barrier" lands
        // deterministically here, before the terminal readback.
        self.fire_barrier_hook();

        // Barrier readback: if a revoke/remount fenced the incarnation
        // while the barrier ran, the apply was a deterministic no-op (the
        // entry stays Reserved) — report the terminal fence, never a
        // generic readback failure a client would keep retrying.
        if !self.incarnation_active(incarnation)? {
            self.plans.lock().unwrap().remove(&load_token);
            return Err(Self::fenced(incarnation));
        }

        // Readback from committed state, re-classified from the committed
        // row: another mutation (invalidate, a later allocation) may have
        // fenced the entry between our propose barrier and this read.
        // Only an exact Valid readback publishes the volatile locations;
        // a fenced or advanced row is terminal Superseded for this load —
        // never a generic error that a client would keep retrying.
        {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            let cur = rocks
                .cache_get_entry(incarnation, key)
                .map_err(fs_err)?
                .ok_or_else(|| cm_err("cache commit readback: entry vanished"))?;
            if cur.state == CacheEntryState::Valid
                && cur.generation == generation
                && cur.object_id == object_id
            {
                if cur.len != len || cur.ufs_mtime != ufs_mtime || cur.expire_at != expire_at {
                    return err_box!(
                        "cache commit readback divergence for ({}, {})@{}: committed len {} mtime {} expire_at {} vs request len {} mtime {} expire_at {}",
                        incarnation,
                        key,
                        generation,
                        cur.len,
                        cur.ufs_mtime,
                        cur.expire_at,
                        len,
                        ufs_mtime,
                        expire_at
                    );
                }
                // Exact Valid readback (this propose or an identical
                // replay): publish below.
            } else if cur.generation > generation
                || (cur.generation == generation && cur.state == CacheEntryState::Tombstoned)
            {
                // The load is dead: a later generation fenced it mid-
                // barrier. Terminal, no retry.
                drop(store);
                self.plans.lock().unwrap().remove(&load_token);
                return Ok(CacheOpStatus::Superseded {
                    expected: generation,
                    current: cur.generation,
                });
            } else {
                return err_box!(
                    "cache commit barrier readback failed for ({}, {}): {:?}",
                    incarnation,
                    key,
                    cur
                );
            }
        }

        // Publishing the client-reported set is safe here: every reported
        // worker was just verified field-wise against the planned worker,
        // so the published endpoints are byte-identical to the plan's
        // canonical addresses (the subset that actually holds the block).
        let mut locations = self.locations.lock().unwrap();
        let object_locations = locations.entry(object_id).or_default();
        object_locations.blocks.clear();
        for (index, block) in blocks.into_iter().enumerate() {
            object_locations
                .blocks
                .insert((index + 1) as i64, block.workers);
        }
        drop(locations);
        // Terminal state reached: the plan is spent.
        self.plans.lock().unwrap().remove(&load_token);
        Ok(CacheOpStatus::Applied)
    }

    /// Invalidate / remove one cache object: a generation fence to
    /// `Tombstoned@expected_generation + 1` that must target the object
    /// the caller observed (`expected_object_id` CAS). Volatile locations
    /// and any plan for the object are dropped only after the fence is
    /// confirmed. (Bulk prefix/mount removal and physical vacuum land in
    /// 4c.)
    pub fn invalidate(
        &self,
        rpc_id: i64,
        incarnation: u64,
        key: &str,
        expected_generation: u64,
        expected_object_id: i64,
    ) -> CommonResult<CacheOpStatus> {
        self.require_enabled()?;
        self.require_leader()?;
        validate_key(key)?;
        // Incarnation gate first (4b P0-2, no-token path): a revoked or
        // stale namespace is terminal — the entry dies with its namespace
        // and no fence is proposed.
        if !self.incarnation_active(incarnation)? {
            return Err(Self::fenced(incarnation));
        }
        let new_generation = expected_generation
            .checked_add(1)
            .ok_or_else(|| cm_err("cache invalidate generation overflow: entry is terminal"))?;

        // Classify against the committed row first: terminal states
        // resolve without a propose.
        {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            match rocks.cache_get_entry(incarnation, key).map_err(fs_err)? {
                None => {
                    return Ok(CacheOpStatus::Superseded {
                        expected: new_generation,
                        current: 0,
                    })
                }
                Some(cur) if cur.generation > new_generation => {
                    // Fenced far past our target: terminal Superseded.
                    // Volatile cleanup only when the live row confirms the
                    // object identity we were told to fence — never on an
                    // unverified id.
                    let verified = cur.object_id == expected_object_id;
                    drop(store);
                    if verified {
                        self.drop_object_state(&cur.object_id);
                    }
                    return Ok(CacheOpStatus::Superseded {
                        expected: new_generation,
                        current: cur.generation,
                    });
                }
                Some(cur) if cur.generation == new_generation => {
                    if cur.state == CacheEntryState::Tombstoned {
                        // Identity must be confirmed against the live row
                        // before any volatile state is dropped: a forged
                        // invalidate quoting another object's tombstone
                        // generation must not clear that object's state.
                        if cur.object_id != expected_object_id {
                            return err_box!(
                                "cache invalidate identity mismatch for ({}, {})@{}: committed object {} vs expected {}",
                                incarnation,
                                key,
                                cur.generation,
                                cur.object_id,
                                expected_object_id
                            );
                        }
                        drop(store);
                        self.drop_object_state(&cur.object_id);
                        return Ok(CacheOpStatus::AlreadyApplied);
                    }
                    // Some other mutation wrote the fenced generation:
                    // divergence, not silent classification.
                    return err_box!(
                        "cache invalidate replay divergence for ({}, {})@{}: state {:?}",
                        incarnation,
                        key,
                        cur.generation,
                        cur.state
                    );
                }
                Some(cur) if cur.generation > expected_generation => {
                    // Our fence generation was taken by another mutation
                    // (e.g. a UFS-write fence): terminal Superseded, with
                    // the same verified-identity cleanup rule.
                    let verified = cur.object_id == expected_object_id;
                    drop(store);
                    if verified {
                        self.drop_object_state(&cur.object_id);
                    }
                    return Ok(CacheOpStatus::Superseded {
                        expected: new_generation,
                        current: cur.generation,
                    });
                }
                Some(cur) if cur.generation < expected_generation => {
                    return err_box!(
                        "cache invalidate expected generation {} is beyond the committed row generation {} for ({}, {})",
                        expected_generation,
                        cur.generation,
                        incarnation,
                        key
                    );
                }
                Some(cur) if cur.object_id != expected_object_id => {
                    return err_box!(
                        "cache invalidate identity mismatch for ({}, {})@{}: committed object {} vs expected {}",
                        incarnation,
                        key,
                        cur.generation,
                        cur.object_id,
                        expected_object_id
                    );
                }
                Some(_) => (), // Reserved/Valid at the expected fence: apply.
            }
        }

        let op_id = self.fs_dir.read().next_op_id();
        let entry = JournalEntry::CacheRemove(CacheRemoveEntry {
            op_id,
            rpc_id,
            incarnation,
            key: key.to_string(),
            expected_generation,
            new_generation,
            expected_object_id,
        });
        self.journal_writer
            .sync_propose_cache(entry)
            .map_err(fs_err)?;
        // Test seam: "another mutation raced the barrier" lands
        // deterministically here, before the terminal readback.
        self.fire_barrier_hook();

        // Barrier readback: a revoke/remount that fenced the incarnation
        // while the barrier ran turned the apply into a deterministic
        // no-op — report the terminal fence.
        if !self.incarnation_active(incarnation)? {
            return Err(Self::fenced(incarnation));
        }

        // Readback from committed state, re-classified from the committed
        // row: another mutation may have fenced past ours between the
        // propose barrier and this read. `generation >= new` is NOT enough
        // for Applied — a later tombstone must report terminal Superseded,
        // and volatile state is dropped only on a verified object identity.
        let store = self.fs_dir.read();
        let rocks = store.get_rocks_store();
        let cur = rocks.cache_get_entry(incarnation, key).map_err(fs_err)?;
        match cur {
            Some(cur)
                if cur.state == CacheEntryState::Tombstoned
                    && cur.generation == new_generation
                    && cur.object_id == expected_object_id =>
            {
                drop(store);
                self.drop_object_state(&cur.object_id);
                Ok(CacheOpStatus::Applied)
            }
            Some(cur)
                if cur.generation > new_generation
                    || (cur.generation == new_generation
                        && cur.state == CacheEntryState::Tombstoned) =>
            {
                // Someone else fenced at/after our target generation: our
                // invalidate is terminal Superseded (its load, if any, is
                // dead). Cleanup only on a verified identity.
                let verified = cur.object_id == expected_object_id;
                drop(store);
                if verified {
                    self.drop_object_state(&cur.object_id);
                }
                Ok(CacheOpStatus::Superseded {
                    expected: new_generation,
                    current: cur.generation,
                })
            }
            other => err_box!(
                "cache invalidate barrier readback failed for ({}, {}): {:?}",
                incarnation,
                key,
                other
            ),
        }
    }

    /// 4b: allocate the next never-reused mount incarnation for `mount_id`.
    ///
    /// The caller supplies a PERSISTENT `OpToken` (request-level
    /// idempotency, P0): a retry after a lost response — or after a master
    /// restart that replays the journal — resolves from the recorded
    /// `IncarnationAllocatedV2` outcome FIRST, returning the original
    /// incarnation without minting a second identity; only an outcome-free
    /// token proceeds to issuance.
    ///
    /// Capability is verified against the PERSISTED mount table (gate 4):
    /// the mount must exist and be write-cache-enabled, and the TTL +
    /// capability snapshot frozen into the V2 journal entry come from that
    /// durable `MountInfo` — never from the request and never from a later
    /// mutable mount entry. Issuance is serialized under the issue lock;
    /// the committed apply re-verifies the snapshot (fs_dir dispatch,
    /// deterministic no-op on mismatch) and the service re-reads the
    /// persisted mount table after the barrier.
    pub fn allocate_incarnation(
        &self,
        token: OpToken,
        rpc_id: i64,
        mount_id: u32,
    ) -> CommonResult<u64> {
        self.require_enabled()?;
        self.require_leader()?;
        validate_client_token(token)?;
        let _guard = self.issue_lock.lock().unwrap();

        // Outcome-first (P0 idempotency): an exact recorded retry resolves
        // from durable history — the outcome binds the request's immutable
        // parameters (mount id), and its ttl must still agree with the
        // frozen policy row of the incarnation it issued. A token replayed
        // against a different mount is divergence, never a silent rebind.
        {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            match rocks.cache_get_outcome(token).map_err(fs_err)? {
                Some(OpOutcome::IncarnationAllocatedV2 {
                    incarnation: out_inc,
                    mount_id: out_mount,
                    ttl_ms: out_ttl,
                }) => {
                    if out_mount != mount_id {
                        return err_box!(
                            "cache incarnation allocate token {:?} replayed with different parameters (mount {}): committed incarnation {} for mount {}",
                            token,
                            mount_id,
                            out_inc,
                            out_mount
                        );
                    }
                    let frozen_ttl = rocks
                        .cache_get_incarnation_policy(out_inc)
                        .map_err(fs_err)?
                        .map(|p| p.ttl_ms)
                        .unwrap_or(0);
                    if frozen_ttl != out_ttl {
                        return err_box!(
                            "cache incarnation allocate token {:?} outcome ttl {} disagrees with the frozen policy row ttl {} of incarnation {}",
                            token,
                            out_ttl,
                            frozen_ttl,
                            out_inc
                        );
                    }
                    return Ok(out_inc);
                }
                Some(other) => {
                    return err_box!(
                        "cache incarnation allocate token {:?} has a non-issuance committed outcome: {:?}",
                        token,
                        other
                    )
                }
                None => {
                    // Outcome-window expiry (b27b6bad P0-1): the issuance
                    // outcome may have been evicted from the bounded
                    // outcome window while the client watermark survives.
                    // A token at or below the watermark is TERMINAL —
                    // re-proposing would journal a no-op V2 entry on every
                    // late retry. Mirror the Allocate/Commit watermark
                    // gate: zero propose, terminal Expired.
                    let watermark = rocks
                        .cache_client_watermark(token.client_id)
                        .map_err(fs_err)?;
                    if let Some(hw) = watermark {
                        if token.op_seq <= hw {
                            return err_box!(
                                "cache incarnation allocate token {:?} is expired (client watermark {}): terminal, re-issue with a fresh token",
                                token,
                                hw
                            );
                        }
                    }
                }
            }
        }

        // Policy snapshot from the persisted mount table.
        let (ttl_ms, cache_write) = {
            let fs = self.fs_dir.read();
            let mounts = fs.get_mount_table()?;
            let m = mounts
                .iter()
                .find(|m| m.mount_id == mount_id)
                .ok_or_else(|| {
                    cm_err(format!(
                        "cache incarnation allocation: mount {} not found in the persisted table",
                        mount_id
                    ))
                })?;
            if !m.write_cache_enabled() {
                return err_box!(
                    "cache incarnation allocation: mount {} is not write-cache-enabled (cache mode + read-write access required)",
                    mount_id
                );
            }
            (m.ttl_ms, true)
        };

        // The next incarnation (durable watermark + 1) is strictly
        // monotonic under the issue lock.
        let incarnation = {
            let store = self.fs_dir.read();
            let hw = store
                .get_rocks_store()
                .cache_get_state(state_tags::CACHE_INCARNATION)
                .map_err(fs_err)?
                .map(|h| h as u64)
                .unwrap_or(0);
            hw.checked_add(1)
                .filter(|&i| i <= MAX_ISSUABLE_INCARNATION)
                .ok_or_else(|| cm_err("cache incarnation space exhausted (i64 watermark bound)"))?
        };

        let op_id = self.fs_dir.read().next_op_id();
        let entry = JournalEntry::CacheIncarnationAllocateV2(CacheIncarnationAllocateV2Entry {
            op_id,
            rpc_id,
            token,
            mount_id,
            incarnation,
            ttl_ms,
            cache_write,
        });
        self.journal_writer
            .sync_propose_cache(entry)
            .map_err(fs_err)?;

        // Post-barrier capability recheck (gate 4): the mount must still be
        // write-cache-enabled with the same frozen TTL. A mount update that
        // raced the barrier is reported loudly; the allocated incarnation
        // stays durable (revoked) history — commits under it derive their
        // deadline from the frozen policy row, never from the mutable
        // table. Mount-id reuse across unmount/remount is fenced by task
        // #6's atomic switch, out of 4b scope.
        {
            let fs = self.fs_dir.read();
            let mounts = fs.get_mount_table()?;
            match mounts.iter().find(|m| m.mount_id == mount_id) {
                Some(m) if m.write_cache_enabled() && m.ttl_ms == ttl_ms => {}
                other => {
                    return err_box!(
                        "cache incarnation allocation for mount {} raced a mount table change (post-barrier state {:?}): the incarnation is durable but its policy snapshot no longer matches the persisted mount",
                        mount_id,
                        other.map(|m| (m.write_cache_enabled(), m.ttl_ms))
                    )
                }
            }
        }

        // Resolve from the committed outcome only.
        let outcome = {
            let store = self.fs_dir.read();
            store
                .get_rocks_store()
                .cache_get_outcome(token)
                .map_err(fs_err)?
        };
        match outcome {
            Some(OpOutcome::IncarnationAllocatedV2 {
                incarnation: out,
                mount_id: out_mount,
                ttl_ms: out_ttl,
            }) if out == incarnation && out_mount == mount_id && out_ttl == ttl_ms => Ok(out),
            other => err_box!(
                "cache incarnation allocate barrier readback failed for mount {} incarnation {}: {:?}",
                mount_id,
                incarnation,
                other
            ),
        }
    }

    /// 4b: revoke a mount incarnation (unmount fence). The row is kept
    /// forever (marked revoked); the mount pointer is cleared only if it
    /// still names this incarnation. Idempotent: revoking a missing or
    /// already-revoked incarnation is AlreadyApplied.
    pub fn revoke_incarnation(
        &self,
        rpc_id: i64,
        mount_id: u32,
        incarnation: u64,
    ) -> CommonResult<CacheOpStatus> {
        self.require_enabled()?;
        self.require_leader()?;

        // Classify from the committed row first.
        {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            match rocks.cache_get_incarnation(incarnation).map_err(fs_err)? {
                None => return Ok(CacheOpStatus::AlreadyApplied),
                Some(row) if row.revoked => return Ok(CacheOpStatus::AlreadyApplied),
                Some(row) if row.mount_id != mount_id => {
                    return err_box!(
                        "cache incarnation revoke: incarnation {} belongs to mount {}, revoke says mount {}",
                        incarnation,
                        row.mount_id,
                        mount_id
                    )
                }
                Some(_) => (),
            }
        }

        let op_id = self.fs_dir.read().next_op_id();
        let entry = JournalEntry::CacheIncarnationRevoke(CacheIncarnationRevokeEntry {
            op_id,
            rpc_id,
            mount_id,
            incarnation,
        });
        self.journal_writer
            .sync_propose_cache(entry)
            .map_err(fs_err)?;

        let store = self.fs_dir.read();
        let rocks = store.get_rocks_store();
        match rocks.cache_get_incarnation(incarnation).map_err(fs_err)? {
            Some(row) if row.revoked => Ok(CacheOpStatus::Applied),
            other => err_box!(
                "cache incarnation revoke barrier readback failed for incarnation {}: {:?}",
                incarnation,
                other
            ),
        }
    }

    /// Drop the volatile state (locations + every plan) of one object:
    /// called when the object's row reaches a terminal state.
    fn drop_object_state(&self, object_id: &i64) {
        self.locations.lock().unwrap().remove(object_id);
        self.plans
            .lock()
            .unwrap()
            .retain(|_, plan| &plan.object_id != object_id);
    }

    fn require_leader(&self) -> CommonResult<()> {
        if !self.monitor.is_active() {
            return err_box!("cache metadata mutations are leader-only");
        }
        Ok(())
    }

    /// Leader gate for issuance paths: a failed gate also burns the
    /// segment (leadership was lost, even if only observed now).
    fn require_leader_or_burn(&self) -> CommonResult<()> {
        if !self.monitor.is_active() {
            *self.segment.lock().unwrap() = None;
            return err_box!("cache metadata mutations are leader-only");
        }
        Ok(())
    }

    /// Plan the per-block worker sets for a whole object. Chooses before
    /// any identity is issued so worker-selection failures never burn an
    /// object id. Each set is bounded by the chooser's replica policy
    /// (production: `ClusterConf.client.replicas` under the worker
    /// policy's availability/capacity rules).
    fn plan_worker_sets(
        &self,
        block_count: usize,
        block_size: i64,
    ) -> CommonResult<Vec<Vec<WorkerAddress>>> {
        let mut sets = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            let mut workers = self.chooser.choose_block(block_size)?;
            workers.truncate(MAX_LOCATIONS_PER_BLOCK);
            if workers.is_empty() {
                return err_box!("cache placement chooser returned no workers");
            }
            sets.push(workers);
        }
        Ok(sets)
    }

    /// Ensure the leader-scoped segment cursor is usable and consume one
    /// id (returning the id together with the leadership epoch it was
    /// consumed in). Caller holds the issue lock. Burn rules:
    /// - the leadership epoch changed since the segment was reserved
    ///   (lost-and-regained leadership, even invisibly between RPCs);
    /// - the durable reserve watermark moved past the segment end
    ///   (another leader reserved onward);
    /// - the segment is exhausted (next == end);
    /// - leadership was lost across the reserve barrier (the just
    ///   reserved segment is dropped and the loop reserves again under
    ///   the new epoch).
    ///
    /// The epoch is re-read at the top of EVERY attempt: an attempt that
    /// lost leadership mid-barrier must bind the next reserve to the new
    /// epoch, never keep comparing against a stale one (which would burn
    /// segments forever). A burned/exhausted segment triggers a fresh
    /// durable reserve `[HW+1, HW+1+SEG)` through the sync barrier; the
    /// old tail is permanently lost to this leader.
    fn ensure_segment_and_issue(&self, rpc_id: i64) -> CommonResult<(i64, u64)> {
        let mut seg = self.segment.lock().unwrap();
        loop {
            let epoch = self.monitor.journal_epoch();
            let durable_hw = {
                let store = self.fs_dir.read();
                store
                    .get_rocks_store()
                    .cache_get_state(state_tags::CACHE_OBJECT_ID)
                    .map_err(fs_err)?
                    .unwrap_or(BlockIdCodec::CACHE_OBJECT_MIN - 1)
            };
            if let Some(s) = *seg {
                if s.epoch != epoch || durable_hw > s.end - 1 {
                    *seg = None;
                }
            }
            if let Some(s) = seg.as_mut() {
                if s.next < s.end {
                    let id = s.next;
                    s.next += 1;
                    return Ok((id, s.epoch));
                }
                *seg = None; // exhausted
            }

            // Reserve the next contiguous segment [HW+1, HW+1+SEG).
            // Guards are dropped around the propose (the apply worker
            // takes its own fs_dir lock; holding a read guard across the
            // barrier would deadlock the FSM).
            let start = durable_hw
                .checked_add(1)
                .ok_or_else(|| cm_err("cache object id segment space exhausted"))?;
            let end = start
                .checked_add(CACHE_RESERVE_SEGMENT)
                .filter(|end| *end <= BlockIdCodec::CACHE_OBJECT_MAX + 1)
                .ok_or_else(|| cm_err("cache object id segment space exhausted"))?;
            let reserve_token = self.next_issuer_token()?;
            let op_id = self.fs_dir.read().next_op_id();
            let entry = JournalEntry::CacheIdReserve(CacheIdReserveEntry {
                op_id,
                rpc_id,
                token: reserve_token,
                start,
                end,
            });
            // The propose barrier returns only after the FSM applied the
            // reserve, so after it the durable HW covers [start, end).
            self.journal_writer
                .sync_propose_cache(entry)
                .map_err(fs_err)?;
            // Test seam: "the epoch changed while the barrier ran" lands
            // deterministically here, before the post-barrier epoch check.
            self.fire_barrier_hook();
            let now_hw = {
                let store = self.fs_dir.read();
                store
                    .get_rocks_store()
                    .cache_get_state(state_tags::CACHE_OBJECT_ID)
                    .map_err(fs_err)?
                    .unwrap_or(BlockIdCodec::CACHE_OBJECT_MIN - 1)
            };
            if now_hw != end - 1 {
                return err_box!(
                    "cache id reserve barrier verification failed: durable watermark {} != {}",
                    now_hw,
                    end - 1
                );
            }
            // Leadership may have transitioned while the barrier ran:
            // never install a segment reserved by a previous epoch —
            // burn it (and the tail) and reserve again under the current
            // one (the next attempt re-reads the epoch at the loop top).
            if !self.monitor.is_active() || self.monitor.journal_epoch() != epoch {
                *seg = None;
                continue;
            }
            *seg = Some(Segment {
                next: start,
                end,
                epoch,
            });
        }
    }

    /// Next issuer reserve token. The op sequence is durable: read from
    /// the issuer client's committed watermark (+1, overflow-checked), so
    /// restarts and clock skew can never collide or regress tokens.
    /// Caller holds the issue lock.
    fn next_issuer_token(&self) -> CommonResult<OpToken> {
        let store = self.fs_dir.read();
        let rocks = store.get_rocks_store();
        let hw = rocks
            .cache_client_watermark(CACHE_ISSUER_CLIENT_ID)
            .map_err(fs_err)?
            .unwrap_or(0);
        let op_seq = hw.checked_add(1).ok_or_else(|| {
            cm_err("cache issuer op sequence exhausted at u64::MAX: id space is terminal")
        })?;
        Ok(OpToken {
            client_id: CACHE_ISSUER_CLIENT_ID,
            op_seq,
        })
    }

    /// Test/restore seam for volatile locations (a full block report will
    /// use the same table in 4d). Accepts only complete, validated sets.
    #[cfg(test)]
    fn install_locations(
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

    /// Test seam for volatile load plans (stand-in for a completed
    /// allocate whose raft barrier is unavailable in unit tests).
    #[cfg(test)]
    fn install_plan(&self, token: OpToken, plan: LoadPlan) {
        self.plans.lock().unwrap().insert(token, plan);
    }
}

fn validate_key(key: &str) -> CommonResult<()> {
    if key.len() > MAX_KEY_BYTES {
        return err_box!("cache key is {} bytes, cap {}", key.len(), MAX_KEY_BYTES);
    }
    Ok(())
}

/// RPC clients may not present the internal issuer client id: a forged
/// issuer token could otherwise alias/deny the reserve token space.
fn validate_client_token(token: OpToken) -> CommonResult<()> {
    if token.client_id == CACHE_ISSUER_CLIENT_ID {
        return err_box!(
            "cache rpc token may not use the internal issuer client id {}",
            CACHE_ISSUER_CLIENT_ID
        );
    }
    Ok(())
}

/// Evidence validation: the commit's reported locations must match the
/// allocate plan bound to the load token — same blocks in sequence order,
/// and per block only planned workers, deduplicated, at least the replica
/// policy's location count. This is what stops a client from publishing
/// un-planned replicas (or faking replica counts by repeating a worker).
fn validate_commit_against_plan(
    plan: &LoadPlan,
    blocks: &[CacheBlockLocation],
) -> CommonResult<()> {
    if blocks.len() > MAX_COMMIT_BLOCKS {
        return err_box!(
            "cache commit block count {} exceeds cap {}",
            blocks.len(),
            MAX_COMMIT_BLOCKS
        );
    }
    // The plan itself must be internally consistent: its block list is the
    // layout derived from (file_len, block_size).
    let derived_count = if plan.file_len == 0 {
        0
    } else {
        ((plan.file_len - 1) / plan.block_size + 1) as usize
    };
    if plan.blocks.len() != derived_count {
        return err_box!(
            "cache load plan block count {} does not match the derived layout {} (len {}, block size {})",
            plan.blocks.len(),
            derived_count,
            plan.file_len,
            plan.block_size
        );
    }
    if blocks.len() != plan.blocks.len() {
        return err_box!(
            "cache commit location count {} does not match the planned block count {}",
            blocks.len(),
            plan.blocks.len()
        );
    }
    for (index, (block, planned)) in blocks.iter().zip(plan.blocks.iter()).enumerate() {
        if block.block_id != planned.block_id {
            return err_box!(
                "cache commit block id {} at position {} is not the planned id {}",
                block.block_id,
                index + 1,
                planned.block_id
            );
        }
        if block.block_len != planned.block_len {
            return err_box!(
                "cache commit block {} length {} != planned {}",
                block.block_id,
                block.block_len,
                planned.block_len
            );
        }
        if block.workers.is_empty() {
            return err_box!(
                "cache commit block {} has no successful replica locations",
                block.block_id
            );
        }
        if block.workers.len() < plan.replicas {
            return err_box!(
                "cache commit block {} reports {} locations, below the replica policy {}",
                block.block_id,
                block.workers.len(),
                plan.replicas
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
        let mut seen = Vec::with_capacity(block.workers.len());
        for worker in &block.workers {
            // Full address comparison, NOT `==`: WorkerAddress's PartialEq
            // compares worker_id only, and a spoofed hostname/ip/port with
            // a genuine worker id must not pass evidence validation (a
            // forged endpoint would be published verbatim into CacheGet
            // locations otherwise).
            if !planned
                .workers
                .iter()
                .any(|p| same_worker_address(p, worker))
            {
                return err_box!(
                    "cache commit block {} reports worker {} which is not in the allocate plan",
                    block.block_id,
                    worker.worker_id
                );
            }
            if seen.contains(&worker.worker_id) {
                return err_box!(
                    "cache commit block {} reports worker {} more than once",
                    block.block_id,
                    worker.worker_id
                );
            }
            seen.push(worker.worker_id);
        }
    }
    Ok(())
}

/// Field-wise worker address equality. `WorkerAddress: PartialEq` compares
/// only `worker_id` (routing semantics); evidence validation needs the full
/// endpoint to match the plan exactly.
fn same_worker_address(a: &WorkerAddress, b: &WorkerAddress) -> bool {
    a.worker_id == b.worker_id
        && a.hostname == b.hostname
        && a.ip_addr == b.ip_addr
        && a.rpc_port == b.rpc_port
        && a.web_port == b.web_port
}

/// Conservative serialized-size estimate of a planned block list on the
/// Allocate response wire: fixed overhead per block and per worker plus
/// the variable-length hostname/ip bytes. Deliberately overestimates
/// (tags, varints, nesting) so a plan under the estimate can never exceed
/// the real wire encoding.
fn estimate_plan_wire_bytes(sets: &[Vec<WorkerAddress>]) -> usize {
    const BLOCK_FIXED: usize = 32;
    const WORKER_FIXED: usize = 32;
    sets.iter()
        .map(|workers| {
            BLOCK_FIXED
                + workers
                    .iter()
                    .map(|w| WORKER_FIXED + w.hostname.len() + w.ip_addr.len())
                    .sum::<usize>()
        })
        .sum()
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
    use crate::master::meta::cache::store::CacheWrite;
    use crate::master::meta::FsDir;
    use crate::master::quota::eviction::evictor::{Evictor, LRUEvictor};
    use crate::master::quota::eviction::EvictionConf;
    use crate::master::Master;
    use curvine_config::{ClusterConf, JournalConf, MasterConf};
    use curvine_core_error::ErrorExt;
    use curvine_model::{AccessMode, MountOptions, WriteType};
    use curvine_raft::raft::{RaftClient, RoleState};
    use curvine_runtime::sync::StateCtl;

    const OBJ: i64 = BlockIdCodec::CACHE_OBJECT_MIN;

    fn token(client: u64, seq: u64) -> OpToken {
        OpToken {
            client_id: client,
            op_seq: seq,
        }
    }

    fn worker(id: u32) -> WorkerAddress {
        WorkerAddress {
            worker_id: id,
            hostname: format!("worker-{}", id),
            ip_addr: "10.0.0.1".into(),
            rpc_port: 8200,
            web_port: 8300,
        }
    }

    /// Deterministic chooser: returns the fixed set, or fails like a real
    /// worker policy with no available workers.
    struct FixedChooser {
        workers: Vec<WorkerAddress>,
        fail: bool,
        replicas: usize,
    }

    impl CacheWorkerChooser for FixedChooser {
        fn choose_block(&self, _block_size: i64) -> CommonResult<Vec<WorkerAddress>> {
            if self.fail {
                return err_box!("fixed chooser: no available workers");
            }
            Ok(self.workers.clone())
        }

        fn replica_policy(&self) -> usize {
            self.replicas
        }
    }

    fn chooser(workers: Vec<WorkerAddress>) -> Arc<dyn CacheWorkerChooser> {
        Arc::new(FixedChooser {
            workers,
            fail: false,
            replicas: 1,
        })
    }

    fn chooser_with_policy(
        workers: Vec<WorkerAddress>,
        replicas: usize,
    ) -> Arc<dyn CacheWorkerChooser> {
        Arc::new(FixedChooser {
            workers,
            fail: false,
            replicas,
        })
    }

    fn failing_chooser() -> Arc<dyn CacheWorkerChooser> {
        Arc::new(FixedChooser {
            workers: vec![worker(1)],
            fail: true,
            replicas: 1,
        })
    }

    fn build_service(name: &str, chooser: Arc<dyn CacheWorkerChooser>) -> CacheService {
        build_service_enabled(name, chooser, true)
    }

    /// 4b gate 5: `enabled=false` mirrors a production master started with
    /// `master.cache_metadata_enabled=false` (the default); the dedicated
    /// disabled test uses this to assert every entry point rejects.
    fn build_service_enabled(
        name: &str,
        chooser: Arc<dyn CacheWorkerChooser>,
        enabled: bool,
    ) -> CacheService {
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
        // Unit tests exercise the leader-side validation, token, plan, and
        // burn logic; the raft barrier itself fails closed in testing mode
        // (no cluster) and is covered by the journal fault tests / 4e.
        monitor.journal_ctl.set_state(RoleState::Leader);
        // 4b gate 5: production defaults the capability OFF; tests enable
        // it explicitly (a dedicated test covers the disabled rejection).
        CacheService::new(fs_dir, writer, monitor, chooser, enabled)
    }

    /// Grants `incarnation` an active incarnation row with a frozen TTL
    /// (unit-test stand-in for the 4b issuer: the real path verifies a
    /// persisted write-cache-enabled mount table and crosses a raft
    /// barrier). One call per incarnation; the token derives from the
    /// incarnation so repeated calls never collide.
    fn mount_incarnation(service: &CacheService, incarnation: u64, ttl_ms: i64) {
        let store = service.fs_dir.read();
        let rocks = store.get_rocks_store();
        store
            .cache
            .apply_incarnation_allocate_v2(
                rocks,
                OpToken {
                    client_id: 91,
                    op_seq: incarnation,
                },
                5,
                incarnation,
                ttl_ms,
            )
            .unwrap();
    }

    /// Writes a committed entry straight through the manager apply path
    /// (unit-test stand-in for a completed allocate+commit round trip,
    /// which needs a real raft barrier).
    fn committed_entry(
        service: &CacheService,
        alloc_token: OpToken,
        commit_token: OpToken,
        key: &str,
        object_id: i64,
        len: i64,
        expire_at: i64,
    ) {
        let store = service.fs_dir.read();
        let rocks = store.get_rocks_store();
        let mgr = &store.cache;
        mgr.apply_incarnation_allocate_v2(
            rocks,
            OpToken {
                client_id: 91,
                op_seq: 1,
            },
            5,
            1,
            0,
        )
        .unwrap();
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
        mgr.apply_allocate(rocks, alloc_token, 1, key, len, &alloc)
            .unwrap();
        mgr.apply_commit(
            rocks,
            alloc_token,
            commit_token,
            1,
            key,
            1,
            object_id,
            len,
            777,
            expire_at,
        )
        .unwrap();
    }

    fn layout(object_id: i64, len: i64) -> CacheBlockLayout {
        CacheBlockLayout::derive(object_id, len, 64).unwrap()
    }

    /// Planned/reported location set for a whole layout, two workers.
    fn full_locations(lay: &CacheBlockLayout) -> Vec<CacheBlockLocation> {
        (1..=lay.block_count)
            .map(|index| {
                let block_len = if index == lay.block_count {
                    lay.last_len
                } else {
                    lay.block_size
                };
                CacheBlockLocation {
                    block_id: lay.block_id(index).unwrap(),
                    block_len,
                    workers: vec![worker(1), worker(2)],
                }
            })
            .collect()
    }

    fn plan_for(lay: &CacheBlockLayout) -> LoadPlan {
        LoadPlan {
            object_id: lay.object_id,
            generation: 1,
            file_len: lay.len,
            block_size: lay.block_size,
            replicas: 1,
            blocks: full_locations(lay),
        }
    }

    fn commit_params<'a>(
        load_token: OpToken,
        token: OpToken,
        blocks: Vec<CacheBlockLocation>,
    ) -> CacheCommitParams<'a> {
        CacheCommitParams {
            token,
            load_token,
            rpc_id: 7,
            incarnation: 1,
            key: "/k",
            generation: 1,
            object_id: OBJ,
            len: 130,
            ufs_mtime: 777,
            ttl_ms: 0,
            blocks,
        }
    }

    #[test]
    fn test_validate_key_and_client_token() {
        assert!(validate_key("/a/b").is_ok());
        assert!(validate_key(&"x".repeat(MAX_KEY_BYTES)).is_ok());
        assert!(validate_key(&"x".repeat(MAX_KEY_BYTES + 1)).is_err());

        assert!(validate_client_token(token(1, 1)).is_ok());
        // The internal issuer id is disjoint from the RPC client space.
        assert!(validate_client_token(token(CACHE_ISSUER_CLIENT_ID, 1)).is_err());
    }

    /// Whole-object semantics: a Valid entry without volatile locations
    /// is a miss when locations are needed, but a metadata-only hit when
    /// they are not; a complete location set is a hit; any missing block
    /// is a miss again.
    #[test]
    fn test_get_is_whole_object() {
        let service = build_service("get-whole-object", chooser(vec![worker(1), worker(2)]));
        mount_incarnation(&service, 1, 0);
        let len = 150; // 3 blocks of 64: 64, 64, 22
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, len, 0);
        let lay = layout(OBJ, len);
        assert_eq!(lay.block_count, 3);
        assert_eq!(lay.last_len, 22);

        // No locations -> miss when needed.
        assert!(service.get(1, "/k", true).unwrap().is_none());
        // Metadata-only lookup hits without any volatile location set.
        let meta = service.get(1, "/k", false).unwrap().expect("meta hit");
        assert!(meta.blocks.is_empty());
        assert_eq!(meta.object_id, OBJ);
        assert_eq!(meta.len, len);
        assert_eq!(meta.generation, 1);
        assert_eq!(meta.ufs_mtime, 777);
        assert_eq!(meta.expire_at, 0);

        // Complete -> hit with exact derived ids/lengths.
        service
            .install_locations(OBJ, full_locations(&lay))
            .unwrap();
        let hit = service.get(1, "/k", true).unwrap().expect("hit");
        assert_eq!(hit.blocks.len(), 3);
        assert_eq!(hit.blocks[0].block_len, 64);
        assert_eq!(hit.blocks[2].block_len, 22);
        assert_eq!(
            hit.blocks[0].block_id,
            BlockIdCodec::encode_block_id(OBJ, 1).unwrap()
        );

        // Drop the last block -> whole-object miss.
        let mut partial = full_locations(&lay);
        partial.pop();
        service.install_locations(OBJ, partial).unwrap();
        assert!(
            service.get(1, "/k", true).unwrap().is_none(),
            "missing block location must be a whole-object miss"
        );

        // Other (never-mounted) incarnation -> typed terminal fenced error
        // (gate-2: never a plain miss, no silent UFS fallback); other key ->
        // miss; oversized key -> error.
        let fenced = service.get(2, "/k", true).unwrap_err();
        assert!(fenced
            .to_string()
            .contains("cache incarnation 2 is revoked or stale"));
        assert!(service.get(1, "/other", true).unwrap().is_none());
        assert!(service
            .get(1, &"x".repeat(MAX_KEY_BYTES + 1), true)
            .is_err());
    }

    #[test]
    fn test_get_miss_on_expired_entry() {
        let service = build_service("get-expired", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        // Expire in the past (passive expiry; active scan lands in 4c).
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 64, 1);
        service
            .install_locations(OBJ, full_locations(&layout(OBJ, 64)))
            .unwrap();
        assert!(service.get(1, "/k", true).unwrap().is_none());
        assert!(service.get(1, "/k", false).unwrap().is_none());
    }

    /// Pre-barrier allocate validation: nothing malformed may reach the
    /// barrier, worker-selection failures never reach it either, and a
    /// fully valid allocate lands on the (fail-closed in testing mode)
    /// reserve barrier.
    #[test]
    fn test_allocate_pre_barrier_validation() {
        let service = build_service("allocate-validation", chooser(vec![worker(1), worker(2)]));
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 64, 0);

        // Forged issuer identity.
        let err = service
            .allocate(token(CACHE_ISSUER_CLIENT_ID, 1), 7, 1, "/new", 128, 64)
            .unwrap_err();
        assert!(format!("{}", err).contains("issuer"), "{}", err);

        // Geometry: len 0 is a LEGAL empty object (it still reaches the
        // barrier); negative length and non-positive block size are not.
        let err = service
            .allocate(token(3, 1), 7, 1, "/empty", 0, 64)
            .unwrap_err();
        assert!(format!("{}", err).contains("raft"), "{}", err);
        assert!(service.allocate(token(3, 2), 7, 1, "/new", -1, 64).is_err());
        assert!(service.allocate(token(3, 3), 7, 1, "/new", 128, 0).is_err());

        // Derived block count over the wire cap.
        let block_size: i64 = 64;
        let file_len = (MAX_COMMIT_BLOCKS as i64) * block_size + 1;
        let err = service
            .allocate(token(3, 4), 7, 1, "/new", file_len, block_size)
            .unwrap_err();
        assert!(format!("{}", err).contains("cap"), "{}", err);

        // Key cap.
        assert!(service
            .allocate(token(3, 5), 7, 1, &"x".repeat(MAX_KEY_BYTES + 1), 128, 64)
            .is_err());

        // Live entry (Reserved/Valid) may not re-allocate.
        let err = service
            .allocate(token(3, 6), 7, 1, "/k", 128, 64)
            .unwrap_err();
        assert!(format!("{}", err).contains("live entry"), "{}", err);

        // A chooser with no workers fails before the barrier.
        let service = build_service("allocate-no-workers", failing_chooser());
        let err = service
            .allocate(token(3, 7), 7, 1, "/new", 128, 64)
            .unwrap_err();
        assert!(!format!("{}", err).contains("raft"), "{}", err);

        // A fully valid allocate reaches the sync-propose barrier, which
        // fails closed in testing mode: the reserve is never skipped.
        let service = build_service("allocate-barrier", chooser(vec![worker(1), worker(2)]));
        mount_incarnation(&service, 1, 0);
        let err = service
            .allocate(token(3, 8), 7, 1, "/new", 128, 64)
            .unwrap_err();
        assert!(
            format!("{}", err).contains("raft"),
            "allocate must go through the fail-closed sync barrier: {}",
            err
        );
    }

    /// Token-first idempotency: a retried allocate resolves from the
    /// committed Allocated outcome — the exact recorded geometry returns
    /// the committed identity (regenerating a volatile plan for the SAME
    /// identity when it was lost to a master restart, never a second
    /// identity), a different geometry or key is divergence, and an
    /// evicted token is terminal.
    #[test]
    fn test_allocate_token_retry_and_expired() {
        let service = build_service("allocate-retry", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        // A committed allocate outcome written by a previous leader
        // (file_len 128 = 2 blocks of 64).
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
            mgr.apply_allocate(rocks, token(2, 1), 1, "/k", 128, &alloc)
                .unwrap();
            // Client 3's outcome window moved to 7 (another op completed);
            // its reserve must stay contiguous with the durable watermark.
            mgr.apply_id_reserve(rocks, token(3, 7), OBJ + 100, OBJ + 110)
                .unwrap();
        }

        // Retry of the recorded allocation: committed identity, and the
        // lost volatile plan is regenerated for the SAME identity (same
        // object id/generation, non-empty blocks for a non-empty object).
        let result = service.allocate(token(2, 1), 7, 1, "/k", 128, 64).unwrap();
        assert_eq!(result.object_id, OBJ);
        assert_eq!(result.generation, 1);
        assert_eq!(result.blocks.len(), 2, "re-plan must rebuild the placement");
        assert_eq!(result.blocks[0].workers, vec![worker(1)]);
        // The regenerated plan is now live: a further retry replays it
        // verbatim (no identity, no placement swap).
        let again = service.allocate(token(2, 1), 7, 1, "/k", 128, 64).unwrap();
        assert_eq!(again.blocks, result.blocks);
        assert!(service.plans.lock().unwrap().contains_key(&token(2, 1)));

        // Same token aimed at a different key or geometry: divergence.
        assert!(service
            .allocate(token(2, 1), 7, 1, "/other", 128, 64)
            .is_err());
        assert!(service.allocate(token(2, 1), 7, 1, "/k", 129, 64).is_err());
        assert!(service.allocate(token(2, 1), 7, 1, "/k", 128, 32).is_err());

        // Token below the client watermark with no outcome: terminal.
        let err = service
            .allocate(token(3, 7), 7, 1, "/x", 128, 64)
            .unwrap_err();
        assert!(format!("{}", err).contains("expired"), "{}", err);
        let err = service
            .allocate(token(3, 1), 7, 1, "/x", 128, 64)
            .unwrap_err();
        assert!(format!("{}", err).contains("expired"), "{}", err);
        // Above the watermark: proceeds to the fail-closed barrier.
        let err = service
            .allocate(token(3, 8), 7, 1, "/x", 128, 64)
            .unwrap_err();
        assert!(format!("{}", err).contains("raft"), "{}", err);
    }

    /// len=0 is a legal empty object end to end at the retry path: the
    /// regenerated plan (and thus the future commit evidence) is empty.
    #[test]
    fn test_allocate_len0_replan_is_empty() {
        let service = build_service("allocate-len0", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
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
            mgr.apply_allocate(rocks, token(2, 1), 1, "/empty", 0, &alloc)
                .unwrap();
        }
        let result = service
            .allocate(token(2, 1), 7, 1, "/empty", 0, 64)
            .unwrap();
        assert_eq!(result.object_id, OBJ);
        assert_eq!(result.generation, 1);
        assert!(
            result.blocks.is_empty(),
            "an empty object plans (and commits) zero blocks"
        );
    }

    /// Segment burn rules: consume in order inside the epoch; burn on
    /// epoch change, on the durable watermark passing the segment, and on
    /// exhaustion — each burn forces a fresh durable reserve through the
    /// (testing-mode fail-closed) barrier.
    #[test]
    fn test_segment_burn_rules() {
        let service = build_service("segment-burn", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        let epoch = service.monitor.journal_epoch();

        // Install a fresh segment: ids are consumed in order.
        *service.segment.lock().unwrap() = Some(Segment {
            next: OBJ + 5,
            end: OBJ + 10,
            epoch,
        });
        assert_eq!(
            service.ensure_segment_and_issue(7).unwrap(),
            (OBJ + 5, epoch)
        );
        assert_eq!(
            service.ensure_segment_and_issue(7).unwrap(),
            (OBJ + 6, epoch)
        );

        // Invisible leadership loss (epoch bumped, role unchanged): the
        // tail burns and the next issue needs a fresh reserve.
        service.monitor.journal_epoch.advance();
        let err = service.ensure_segment_and_issue(7).unwrap_err();
        assert!(format!("{}", err).contains("raft"), "{}", err);
        assert!(service.segment.lock().unwrap().is_none());

        // Durable watermark passing the segment end burns it too.
        *service.segment.lock().unwrap() = Some(Segment {
            next: OBJ,
            end: OBJ + 10,
            epoch: service.monitor.journal_epoch(),
        });
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store
                .cache
                .apply_id_reserve(rocks, token(1, 1), OBJ, OBJ + 110)
                .unwrap();
        }
        let err = service.ensure_segment_and_issue(7).unwrap_err();
        assert!(format!("{}", err).contains("raft"), "{}", err);
        assert!(service.segment.lock().unwrap().is_none());

        // An exhausted segment likewise forces a reserve.
        *service.segment.lock().unwrap() = Some(Segment {
            next: OBJ + 5,
            end: OBJ + 5,
            epoch: service.monitor.journal_epoch(),
        });
        let err = service.ensure_segment_and_issue(7).unwrap_err();
        assert!(format!("{}", err).contains("raft"), "{}", err);
    }

    /// The issuer's own op sequence is durable: always watermark+1, never
    /// a wall clock, so restarts cannot collide or regress tokens.
    #[test]
    fn test_issuer_seq_is_durable_watermark() {
        let service = build_service("issuer-seq", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        assert_eq!(service.next_issuer_token().unwrap(), token(0, 1));
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store
                .cache
                .apply_id_reserve(rocks, token(0, 1), OBJ, OBJ + 10)
                .unwrap();
        }
        assert_eq!(service.next_issuer_token().unwrap(), token(0, 2));
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store
                .cache
                .apply_id_reserve(rocks, token(0, 5), OBJ + 10, OBJ + 20)
                .unwrap();
        }
        assert_eq!(service.next_issuer_token().unwrap(), token(0, 6));
    }

    /// A leader gate failure on an issuance path burns the segment.
    #[test]
    fn test_leader_gate_burns_segment() {
        let service = build_service("leader-burn", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        let epoch = service.monitor.journal_epoch();
        *service.segment.lock().unwrap() = Some(Segment {
            next: OBJ,
            end: OBJ + 10,
            epoch,
        });
        service.monitor.journal_ctl.set_state(RoleState::Follower);
        assert!(service.allocate(token(3, 1), 7, 1, "/k", 128, 64).is_err());
        assert!(service.segment.lock().unwrap().is_none());
    }

    /// The volatile plan is mandatory: a commit without it fails closed
    /// as a retryable miss (master restart) — before any other judgment
    /// about the entry row.
    #[test]
    fn test_commit_requires_plan() {
        let service = build_service("commit-plan-mandatory", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        // Reserved row + recorded load outcome exist, but no volatile plan
        // for the load token.
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
            mgr.apply_allocate(rocks, token(9, 1), 1, "/k", 130, &alloc)
                .unwrap();
        }
        let lay = layout(OBJ, 130);
        let err = service
            .commit(commit_params(
                token(9, 1),
                token(9, 2),
                full_locations(&lay),
            ))
            .unwrap_err();
        assert!(
            format!("{}", err).contains("live plan"),
            "commit without a plan must fail closed: {}",
            err
        );
        assert!(!format!("{}", err).contains("raft"), "{}", err);
    }

    /// A lost-response commit retry resolves to its recorded Committed
    /// outcome as AlreadyApplied, regardless of the entry row — but ONLY
    /// on an exact match of the FULL immutable request (load token, len,
    /// ufs_mtime, expire_at): any divergence is rejected, never silently
    /// resolved. A commit that does not match its recorded load allocation
    /// is rejected before any plan lookup.
    #[test]
    fn test_commit_outcome_retry_already_applied() {
        let service = build_service("commit-retry", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);

        // Same-generation exact Valid row: the recorded-outcome retry is
        // AlreadyApplied, and a live plan for the load is spent (terminal
        // cleanup).
        let lay = layout(OBJ, 130);
        service.install_plan(token(2, 1), plan_for(&lay));
        let status = service
            .commit(commit_params(token(2, 1), token(2, 2), vec![]))
            .unwrap();
        assert_eq!(status, CacheOpStatus::AlreadyApplied);
        assert!(
            !service.plans.lock().unwrap().contains_key(&token(2, 1)),
            "recorded-outcome AlreadyApplied must clear the load's plan"
        );

        // Later mutations (removal) fenced the row past the commit: the
        // exact outcome match must NOT resolve AlreadyApplied — Superseded
        // is terminal, so the fenced row wins even for the original token.
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store.cache.apply_remove(rocks, 1, "/k", 1, 2, OBJ).unwrap();
        }
        service.install_plan(token(2, 1), plan_for(&lay));
        assert_eq!(
            service
                .commit(commit_params(token(2, 1), token(2, 2), vec![]))
                .unwrap(),
            CacheOpStatus::Superseded {
                expected: 1,
                current: 2
            }
        );
        assert!(
            !service.plans.lock().unwrap().contains_key(&token(2, 1)),
            "fenced-row Superseded must clear the load's plan"
        );

        // Same commit token with ANY different parameter is divergence,
        // never AlreadyApplied: the outcome binds the full request.
        let mut p = commit_params(token(2, 1), token(2, 2), vec![]);
        p.len = 999;
        let err = service.commit(p.clone()).unwrap_err();
        assert!(
            format!("{}", err).contains("replayed with different parameters"),
            "{}",
            err
        );
        let mut p = commit_params(token(2, 1), token(2, 2), vec![]);
        p.ufs_mtime = 888;
        let err = service.commit(p).unwrap_err();
        assert!(
            format!("{}", err).contains("replayed with different parameters"),
            "{}",
            err
        );
        // Geometry intact, only the load token differs: still divergence.
        let err = service
            .commit(commit_params(token(9, 9), token(2, 2), vec![]))
            .unwrap_err();
        assert!(
            format!("{}", err).contains("replayed with different parameters"),
            "{}",
            err
        );

        // An unknown load token has no recorded allocation: fail closed
        // before the plan lookup.
        let err = service
            .commit(commit_params(token(4, 9), token(4, 1), vec![]))
            .unwrap_err();
        assert!(
            format!("{}", err).contains("no recorded allocation"),
            "{}",
            err
        );

        // A different commit token for the same load: even with NO live
        // plan (restart, or the fencing invalidate already cleared it),
        // the committed Tombstoned@2 row resolves the retry to terminal
        // Superseded — durable state alone classifies, never a retryable
        // "no live plan" miss.
        assert_eq!(
            service
                .commit(commit_params(token(2, 1), token(4, 1), vec![]))
                .unwrap(),
            CacheOpStatus::Superseded {
                expected: 1,
                current: 2
            }
        );
        assert!(
            !service.plans.lock().unwrap().contains_key(&token(2, 1)),
            "pre-read Superseded must clear the load's plan"
        );
    }

    /// Gap-2 regression: a commit that raced an invalidate (row fenced,
    /// no commit outcome recorded) and lost its Superseded response must
    /// still resolve a same-token retry to terminal Superseded from the
    /// durable row — with or without a live plan.
    #[test]
    fn test_commit_superseded_retry_without_plan() {
        let service = build_service("commit-superseded-retry", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        // Reserved row + recorded load allocation.
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
            mgr.apply_allocate(rocks, token(9, 1), 1, "/k", 130, &alloc)
                .unwrap();
        }
        // The invalidate that fenced the row also cleared the load's plan
        // (verified-identity cleanup) — the commit's Superseded response
        // was lost in between.
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store.cache.apply_remove(rocks, 1, "/k", 1, 2, OBJ).unwrap();
        }
        assert!(!service.plans.lock().unwrap().contains_key(&token(9, 1)));

        // First (lost-response replay) attempt: terminal Superseded, not a
        // retryable "no live plan" miss.
        assert_eq!(
            service
                .commit(commit_params(token(9, 1), token(9, 2), vec![]))
                .unwrap(),
            CacheOpStatus::Superseded {
                expected: 1,
                current: 2
            }
        );
        // And again after any plan re-appears (exact allocate replay):
        // still terminal, still durable-state-classified.
        let lay = layout(OBJ, 130);
        service.install_plan(token(9, 1), plan_for(&lay));
        assert_eq!(
            service
                .commit(commit_params(
                    token(9, 1),
                    token(9, 2),
                    full_locations(&lay)
                ))
                .unwrap(),
            CacheOpStatus::Superseded {
                expected: 1,
                current: 2
            }
        );
        assert!(!service.plans.lock().unwrap().contains_key(&token(9, 1)));
    }

    /// TTL stays fail-closed until mount-policy TTL lands (4b).
    #[test]
    fn test_commit_ttl_fail_closed() {
        let service = build_service("commit-ttl", chooser(vec![worker(1)]));
        let mut p = commit_params(token(2, 1), token(2, 2), vec![]);
        p.ttl_ms = 1;
        let err = service.commit(p).unwrap_err();
        assert!(format!("{}", err).contains("ttl"), "{}", err);
    }

    /// Commit evidence is validated against the plan BEFORE the barrier:
    /// same blocks in order, planned workers only, deduplicated, at least
    /// the replica policy's count, capped.
    #[test]
    fn test_commit_rejects_malformed_evidence() {
        let service = build_service("commit-evidence", chooser(vec![worker(1), worker(2)]));
        mount_incarnation(&service, 1, 0);
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
            mgr.apply_allocate(rocks, token(9, 1), 1, "/k", 130, &alloc)
                .unwrap();
        }
        let lay = layout(OBJ, 130); // 3 blocks: 64, 64, 2
        service.install_plan(token(9, 1), plan_for(&lay));

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
            ("unplanned worker", {
                let mut v = full_locations(&lay);
                v[0].workers = vec![worker(1), worker(99)];
                v
            }),
            // Spoofed endpoint: genuine planned worker_id with a forged
            // hostname/ip/port. `WorkerAddress: PartialEq` compares only
            // worker_id, so this must be rejected by the field-wise check.
            ("spoofed worker address", {
                let mut spoof = worker(1);
                spoof.hostname = "evil-host".into();
                spoof.ip_addr = "6.6.6.6".into();
                spoof.rpc_port = 6666;
                let mut v = full_locations(&lay);
                v[0].workers = vec![spoof, worker(2)];
                v
            }),
            ("duplicate worker", {
                let mut v = full_locations(&lay);
                v[0].workers = vec![worker(1), worker(1)];
                v
            }),
            ("location cap exceeded", {
                let mut plan = plan_for(&lay);
                plan.blocks[0].workers = (1..=(MAX_LOCATIONS_PER_BLOCK as u32 + 1))
                    .map(worker)
                    .collect();
                service.install_plan(token(9, 1), plan);
                let mut v = full_locations(&lay);
                v[0].workers = (1..=(MAX_LOCATIONS_PER_BLOCK as u32 + 1))
                    .map(worker)
                    .collect();
                v
            }),
        ];
        for (name, blocks) in cases {
            let err = service
                .commit(commit_params(token(9, 1), token(9, 2), blocks))
                .unwrap_err();
            assert!(
                !format!("{}", err).contains("raft"),
                "{}: must be rejected before the barrier, got: {}",
                name,
                err
            );
        }

        // A perfect evidence set reaches the fail-closed barrier: the
        // journal write is never skipped.
        let err = service
            .commit(commit_params(
                token(9, 1),
                token(9, 2),
                full_locations(&lay),
            ))
            .unwrap_err();
        assert!(
            format!("{}", err).contains("raft"),
            "well-formed commit must reach the fail-closed barrier: {}",
            err
        );
    }

    /// The commit evidence must reach the replica policy: a plan with
    /// replicas=2 rejects single-location blocks before the barrier.
    #[test]
    fn test_commit_replica_policy_enforced() {
        let service = build_service(
            "commit-replica-policy",
            chooser_with_policy(vec![worker(1), worker(2)], 2),
        );
        mount_incarnation(&service, 1, 0);
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
            mgr.apply_allocate(rocks, token(9, 1), 1, "/k", 130, &alloc)
                .unwrap();
        }
        let lay = layout(OBJ, 130);
        let mut plan = plan_for(&lay);
        plan.replicas = 2;
        service.install_plan(token(9, 1), plan);

        // Planned workers, deduplicated, but only one location per block:
        // below the replica policy.
        let starved: Vec<CacheBlockLocation> = full_locations(&lay)
            .into_iter()
            .map(|mut b| {
                b.workers = vec![worker(1)];
                b
            })
            .collect();
        let err = service
            .commit(commit_params(token(9, 1), token(9, 2), starved))
            .unwrap_err();
        assert!(
            format!("{}", err).contains("replica policy"),
            "evidence below the replica policy must be rejected: {}",
            err
        );
        assert!(!format!("{}", err).contains("raft"), "{}", err);

        // Full evidence reaches the fail-closed barrier.
        let err = service
            .commit(commit_params(
                token(9, 1),
                token(9, 2),
                full_locations(&lay),
            ))
            .unwrap_err();
        assert!(format!("{}", err).contains("raft"), "{}", err);
    }

    /// Invalidate classification: terminal rows resolve without a
    /// propose; everything else reaches the fail-closed barrier.
    #[test]
    fn test_invalidate_classification() {
        let service = build_service("invalidate-classify", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        // Missing entry.
        assert_eq!(
            service.invalidate(7, 1, "/missing", 1, OBJ).unwrap(),
            CacheOpStatus::Superseded {
                expected: 2,
                current: 0
            }
        );

        // Committed + removed: Tombstoned@2 is an AlreadyApplied fence
        // for expected_generation 1, and volatile locations are dropped.
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 64, 0);
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store.cache.apply_remove(rocks, 1, "/k", 1, 2, OBJ).unwrap();
        }
        service
            .install_locations(OBJ, full_locations(&layout(OBJ, 64)))
            .unwrap();
        assert_eq!(
            service.invalidate(7, 1, "/k", 1, OBJ).unwrap(),
            CacheOpStatus::AlreadyApplied
        );
        assert!(!service.locations.lock().unwrap().contains_key(&OBJ));

        // Expected generation beyond the row: divergence error.
        assert!(service.invalidate(7, 1, "/k", 9, OBJ).is_err());

        // Expected object mismatch: divergence error.
        assert!(service.invalidate(7, 1, "/k", 2, OBJ + 7).is_err());

        // A live Reserved row at the right fence reaches the barrier.
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            let mgr = &store.cache;
            let alloc = CacheEntry {
                generation: 1,
                state: CacheEntryState::Reserved,
                object_id: OBJ + 3,
                len: 0,
                ufs_mtime: 0,
                block_size: 64,
                expire_at: 0,
            };
            mgr.apply_allocate(rocks, token(5, 1), 1, "/live", 128, &alloc)
                .unwrap();
        }
        let err = service.invalidate(7, 1, "/live", 1, OBJ + 3).unwrap_err();
        assert!(
            format!("{}", err).contains("raft"),
            "live fence must reach the fail-closed barrier: {}",
            err
        );
    }

    /// P1-4 regression: a forged invalidate quoting ANOTHER object's id
    /// against a matching tombstone fence must NOT clear that object's
    /// volatile state — identity is confirmed against the live row before
    /// any drop.
    #[test]
    fn test_invalidate_forged_object_identity() {
        let service = build_service("invalidate-forged-id", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        // Object A committed then removed: row is Tombstoned@2.
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 64, 0);
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store.cache.apply_remove(rocks, 1, "/k", 1, 2, OBJ).unwrap();
        }
        // Object B (a different id) has live volatile state: locations and
        // an in-flight load plan.
        let b = OBJ + 50;
        let b_lay = layout(b, 64);
        service
            .install_locations(b, full_locations(&b_lay))
            .unwrap();
        service.install_plan(token(6, 1), plan_for(&b_lay));

        // Forged invalidate: expected_generation 1 fences at 2, which
        // matches A's tombstone, but the caller quotes B's object id.
        let err = service.invalidate(7, 1, "/k", 1, b).unwrap_err();
        assert!(format!("{}", err).contains("identity mismatch"), "{}", err);
        assert!(
            service.locations.lock().unwrap().contains_key(&b),
            "forged invalidate must not clear another object's locations"
        );
        assert!(
            service.plans.lock().unwrap().contains_key(&token(6, 1)),
            "forged invalidate must not clear another object's plan"
        );

        // Positive control: quoting the row's real object id resolves
        // AlreadyApplied and drops A's own state.
        service
            .install_locations(OBJ, full_locations(&layout(OBJ, 64)))
            .unwrap();
        assert_eq!(
            service.invalidate(7, 1, "/k", 1, OBJ).unwrap(),
            CacheOpStatus::AlreadyApplied
        );
        assert!(!service.locations.lock().unwrap().contains_key(&OBJ));
        // B is untouched by A's terminal resolution.
        assert!(service.locations.lock().unwrap().contains_key(&b));
    }

    /// Invalidate Superseded branches clean volatile state ONLY when the
    /// live row confirms the quoted object identity (P1-7 rule): verified
    /// ids drop, unverified ids never do.
    #[test]
    fn test_invalidate_superseded_cleanup_verified_only() {
        let service = build_service("invalidate-superseded-cleanup", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        // Row fenced twice: Tombstoned@3.
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 64, 0);
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store.cache.apply_remove(rocks, 1, "/k", 1, 2, OBJ).unwrap();
            store.cache.apply_remove(rocks, 1, "/k", 2, 3, OBJ).unwrap();
        }
        service
            .install_locations(OBJ, full_locations(&layout(OBJ, 64)))
            .unwrap();

        // expected_generation 1 fences at 2; the row is already at 3:
        // terminal Superseded with a VERIFIED identity -> state dropped.
        assert_eq!(
            service.invalidate(7, 1, "/k", 1, OBJ).unwrap(),
            CacheOpStatus::Superseded {
                expected: 2,
                current: 3
            }
        );
        assert!(!service.locations.lock().unwrap().contains_key(&OBJ));

        // Same fence quoting a different object id: still terminal
        // Superseded (the row advanced regardless), but the OTHER object's
        // volatile state survives.
        let b = OBJ + 51;
        let b_lay = layout(b, 64);
        service
            .install_locations(b, full_locations(&b_lay))
            .unwrap();
        assert_eq!(
            service.invalidate(7, 1, "/k", 1, b).unwrap(),
            CacheOpStatus::Superseded {
                expected: 2,
                current: 3
            }
        );
        assert!(
            service.locations.lock().unwrap().contains_key(&b),
            "unverified identity must not clear volatile state"
        );
    }

    /// P1-6: the Allocate response plan is wire-size capped BEFORE any
    /// object id is issued — the transport's inbound cap does not protect
    /// responses. A plan over the cap is rejected pre-issue; a plan under
    /// it proceeds to the (fail-closed) barrier.
    #[test]
    fn test_allocate_plan_wire_cap() {
        let mut big = worker(1);
        big.hostname = "h".repeat(200);
        let service = build_service("allocate-wire-cap", chooser(vec![big]));
        mount_incarnation(&service, 1, 0);
        let block_size: i64 = 64;

        // 65536 blocks x (fixed overhead + 200-byte hostname) far exceeds
        // the 8 MiB plan cap: rejected before issuance.
        let file_len = (MAX_COMMIT_BLOCKS as i64) * block_size;
        let err = service
            .allocate(token(3, 1), 7, 1, "/big", file_len, block_size)
            .unwrap_err();
        assert!(format!("{}", err).contains("response cap"), "{}", err);
        assert!(!format!("{}", err).contains("raft"), "{}", err);

        // Under the cap: passes the wire gate and reaches the fail-closed
        // barrier (rejected there only because unit tests have no raft).
        let err = service
            .allocate(token(3, 2), 7, 1, "/ok", 4096 * block_size, block_size)
            .unwrap_err();
        assert!(
            format!("{}", err).contains("raft"),
            "under-cap plan must proceed to the barrier: {}",
            err
        );
    }

    /// 4b gate 5: with the capability disabled (the production default),
    /// every cache entry point rejects before touching any state.
    #[test]
    fn test_service_disabled_rejects_all_entry_points() {
        let service = build_service_enabled("disabled-gate", chooser(vec![worker(1)]), false);

        let expect_disabled = |err: _| {
            let msg = format!("{}", err);
            assert!(
                msg.contains("cache metadata capability is disabled"),
                "{}",
                msg
            );
        };

        expect_disabled(service.get(1, "/k", false).unwrap_err());
        expect_disabled(
            service
                .allocate(token(3, 1), 7, 1, "/k", 64, 64)
                .unwrap_err(),
        );
        expect_disabled(
            service
                .commit(commit_params(token(2, 1), token(2, 2), vec![]))
                .unwrap_err(),
        );
        expect_disabled(service.invalidate(7, 1, "/k", 1, OBJ).unwrap_err());
        expect_disabled(service.allocate_incarnation(token(3, 1), 7, 5).unwrap_err());
        expect_disabled(service.revoke_incarnation(7, 5, 1).unwrap_err());
    }

    /// 4b P0-2 fenced paths: once the incarnation is revoked, every path
    /// is terminal — get fails closed with a typed fenced error (never a
    /// plain miss that would silently fall back to the UFS), and
    /// mutations are fenced errors. An exact recorded retry is fenced
    /// too: a dead namespace never hands identities back; only DIVERGENCE
    /// reporting (a token replayed with different parameters) precedes
    /// the fence.
    #[test]
    fn test_fenced_incarnation_paths() {
        let service = build_service("fenced-paths", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);

        // Revoke the namespace directly through the manager apply (the
        // unit-test stand-in for the service revoke, which needs a raft
        // barrier).
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store.cache.apply_incarnation_revoke(rocks, 5, 1).unwrap();
        }

        // get fails closed: typed terminal fenced error, NOT a plain miss.
        let fenced = |err: _| {
            let msg = format!("{}", err);
            assert!(
                msg.contains("terminal") && msg.contains("incarnation 1"),
                "{}",
                msg
            );
        };
        fenced(service.get(1, "/k", false).unwrap_err());

        // Mutations are fenced terminal errors naming the incarnation.
        fenced(
            service
                .allocate(token(3, 1), 7, 1, "/x", 64, 64)
                .unwrap_err(),
        );
        fenced(service.invalidate(7, 1, "/k", 1, OBJ).unwrap_err());
        // A fresh commit token: no outcome, watermark clear -> the gate is
        // reached and fences.
        fenced(
            service
                .commit(commit_params(token(2, 1), token(3, 2), vec![]))
                .unwrap_err(),
        );
        // An exact recorded commit retry: divergence check passes (exact
        // match), then the gate still fences — the load dies with its
        // namespace; the durable Committed outcome answers any later
        // classification only in a live namespace.
        fenced(
            service
                .commit(commit_params(token(2, 1), token(2, 2), vec![]))
                .unwrap_err(),
        );

        // An EXACT recorded allocate retry is fenced as well: the recorded
        // identity of a dead namespace is not handed back to the client.
        fenced(
            service
                .allocate(token(2, 1), 7, 1, "/k", 130, 64)
                .unwrap_err(),
        );

        // But divergence still precedes the fence: the SAME token replayed
        // with different parameters reports the parameter divergence, not
        // the generic fenced error.
        let err = service
            .allocate(token(2, 1), 7, 1, "/k", 999, 64)
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("replayed with different parameters"),
            "{}",
            msg
        );

        // Wire-typed fence (b27b6bad P0-2): every fenced result is a boxed
        // FsError::CacheIncarnationFenced. The handler boundary is exactly
        // From<CommonError> (the handler `?`), so exercising that
        // conversion plus the FsError wire encode/decode here proves the
        // kind survives service -> handler -> wire -> client for all four
        // fenced paths; clients branch on the kind, not the string.
        for err in [
            service.get(1, "/k", false).unwrap_err(),
            service
                .allocate(token(3, 1), 7, 1, "/x", 64, 64)
                .unwrap_err(),
            service.invalidate(7, 1, "/k", 1, OBJ).unwrap_err(),
            service
                .commit(commit_params(token(2, 1), token(3, 2), vec![]))
                .unwrap_err(),
        ] {
            let wire_err = curvine_error::FsError::from(err);
            assert!(
                matches!(
                    wire_err.kind(),
                    curvine_error::ErrorKind::CacheIncarnationFenced
                ),
                "handler conversion must preserve the typed fence: {}",
                wire_err
            );
            let decoded = curvine_error::FsError::decode(wire_err.encode());
            assert!(
                matches!(
                    decoded.kind(),
                    curvine_error::ErrorKind::CacheIncarnationFenced
                ),
                "the wire round trip must preserve the typed fence: {}",
                decoded
            );
        }
    }

    /// 4b P0-3: commit expiry derivation. The deadline comes from the
    /// incarnation's frozen policy row, computed once at the leader; an
    /// exact recorded retry reuses the durable absolute value bit-exactly
    /// (a recomputation would drift by the retry latency, and the
    /// row-comparison would then report divergence); an unsatisfiable
    /// deadline (now + ttl overflow) fails closed before any propose.
    #[test]
    fn test_commit_ttl_derivation_reuse_and_overflow() {
        let service = build_service("commit-ttl", chooser(vec![worker(1)]));
        // Incarnation 1 with a 1h TTL, and a committed row whose recorded
        // expire_at is a fixed constant far away from now + 3_600_000.
        mount_incarnation(&service, 1, 3_600_000);
        const E: i64 = 1_234_567;
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
            mgr.apply_allocate(rocks, token(2, 1), 1, "/k", 130, &alloc)
                .unwrap();
            mgr.apply_commit(
                rocks,
                token(2, 1),
                token(2, 2),
                1,
                "/k",
                1,
                OBJ,
                130,
                777,
                E,
            )
            .unwrap();
        }

        // Exact retry resolves AlreadyApplied reusing the recorded E: had
        // the retry recomputed now + ttl (or compared that recomputation
        // against the row) this would diverge, because E != now + 3_600_000.
        assert_eq!(
            service
                .commit(commit_params(token(2, 1), token(2, 2), vec![]))
                .unwrap(),
            CacheOpStatus::AlreadyApplied
        );

        // Overflow fail-closed: a policy ttl of i64::MAX cannot produce a
        // deadline; rejected pre-barrier, before the outcome read. Mount a
        // second incarnation with the unsatisfiable ttl (the manager apply
        // only validates ttl >= 0 — the deadline math is commit-time).
        mount_incarnation(&service, 2, i64::MAX);

        // FIRST: an EXACT recorded retry under the unsatisfiable policy
        // must still resolve. The recorded outcome exists (written via the
        // manager with a fixed deadline); the retry reuses it WITHOUT
        // reading the policy row — under the old compute-first code this
        // exact retry failed with an overflow that did not exist when the
        // commit originally applied.
        const E2: i64 = 2_345_678;
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            let mgr = &store.cache;
            mgr.apply_id_reserve(rocks, token(1, 2), OBJ + 100, OBJ + 300)
                .unwrap();
            let alloc = CacheEntry {
                generation: 1,
                state: CacheEntryState::Reserved,
                object_id: OBJ + 200,
                len: 0,
                ufs_mtime: 0,
                block_size: 64,
                expire_at: 0,
            };
            mgr.apply_allocate(rocks, token(4, 1), 2, "/k2", 130, &alloc)
                .unwrap();
            mgr.apply_commit(
                rocks,
                token(4, 1),
                token(4, 2),
                2,
                "/k2",
                1,
                OBJ + 200,
                130,
                777,
                E2,
            )
            .unwrap();
        }
        let params = CacheCommitParams {
            token: token(4, 2),
            load_token: token(4, 1),
            rpc_id: 7,
            incarnation: 2,
            key: "/k2",
            generation: 1,
            object_id: OBJ + 200,
            len: 130,
            ufs_mtime: 777,
            ttl_ms: 0,
            blocks: vec![],
        };
        assert_eq!(
            service.commit(params).unwrap(),
            CacheOpStatus::AlreadyApplied,
            "exact retry must reuse the recorded deadline and never read the overflowing policy"
        );

        // THEN the fresh (outcome-free) commit under the same policy fails
        // closed on the unsatisfiable deadline, pre-barrier.
        let params = CacheCommitParams {
            token: token(4, 3),
            load_token: token(4, 1),
            rpc_id: 7,
            incarnation: 2,
            key: "/k2",
            generation: 1,
            object_id: OBJ + 200,
            len: 130,
            ufs_mtime: 777,
            ttl_ms: 0,
            blocks: vec![],
        };
        let err = service.commit(params).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("overflow"), "{}", msg);
        assert!(!msg.contains("raft"), "{}", msg);
    }

    /// 4b gate 4 + P0 idempotency: incarnation issuance verifies the
    /// PERSISTED mount table; the caller's persistent token resolves an
    /// exact recorded retry from the outcome FIRST (response loss / restart
    /// replay) without minting a second identity.
    #[test]
    fn test_issuer_capability_gates() {
        let service = build_service("issuer-gates", chooser(vec![worker(1)]));
        // The caller's persistent token for mount 5 issuance.
        let issue = token(6, 1);

        // Nothing persisted at all.
        let err = service.allocate_incarnation(issue, 7, 5).unwrap_err();
        assert!(format!("{}", err).contains("not found"), "{}", err);

        // Persist mounts: 5 = write-cache enabled (1h ttl), 6 = cache mode
        // but read-only, 7 = fs mode. Written straight to rocks (no
        // journaling) — this is the persisted table the issuer must trust.
        let write_cache_mount = MountOptions::builder()
            .write_type(WriteType::CacheMode)
            .access_mode(AccessMode::ReadWrite)
            .write_cache(true)
            .ttl_ms(3_600_000)
            .build()
            .to_info(5, "/mnt/a", "file:///tmp/curvine-a");
        let read_only_mount = MountOptions::builder()
            .write_type(WriteType::CacheMode)
            .access_mode(AccessMode::ReadOnly)
            .write_cache(true)
            .build()
            .to_info(6, "/mnt/b", "file:///tmp/curvine-b");
        let fs_mount = MountOptions::builder()
            .write_type(WriteType::FsMode)
            .access_mode(AccessMode::ReadWrite)
            .write_cache(false)
            .build()
            .to_info(7, "/mnt/c", "file:///tmp/curvine-c");
        service
            .fs_dir
            .write()
            .unprotected_store_mount(write_cache_mount)
            .unwrap();
        service
            .fs_dir
            .write()
            .unprotected_store_mount(read_only_mount)
            .unwrap();
        service
            .fs_dir
            .write()
            .unprotected_store_mount(fs_mount)
            .unwrap();

        // Unknown id still not found (the table is keyed by mount id).
        let err = service.allocate_incarnation(issue, 7, 99).unwrap_err();
        assert!(format!("{}", err).contains("not found"), "{}", err);

        // Capability gates: read-only cache mode and fs mode are rejected
        // before any token or id is minted.
        let err = service.allocate_incarnation(issue, 7, 6).unwrap_err();
        assert!(
            format!("{}", err).contains("not write-cache-enabled"),
            "{}",
            err
        );
        let err = service.allocate_incarnation(issue, 7, 7).unwrap_err();
        assert!(
            format!("{}", err).contains("not write-cache-enabled"),
            "{}",
            err
        );

        // P0 idempotency, outcome-first: pre-write the issuance outcome via
        // the manager apply (unit-test stand-in for a completed barrier).
        // A retry with the SAME token — even though the mount table lookup
        // below it would still pass — must resolve from the outcome alone,
        // before any mount check, and return the same incarnation.
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store
                .cache
                .apply_incarnation_allocate_v2(rocks, issue, 5, 3, 3_600_000)
                .unwrap();
        }
        assert_eq!(service.allocate_incarnation(issue, 7, 5).unwrap(), 3);

        // Divergence: the SAME token replayed against a different mount is
        // loud divergence, never a silent rebind.
        let err = service.allocate_incarnation(issue, 7, 6).unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("replayed with different parameters"),
            "{}",
            msg
        );

        // Valid mount, fresh token: passes the capability gates and reaches
        // the fail-closed raft barrier (rejected there only because unit
        // tests have no cluster).
        let err = service.allocate_incarnation(token(6, 2), 7, 5).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("raft"), "{}", msg);
        assert!(
            !msg.contains("not found") && !msg.contains("write-cache-enabled"),
            "{}",
            msg
        );

        // Outcome-window expiry (b27b6bad P0-1): the issuance outcome was
        // evicted from the bounded outcome window but the client watermark
        // survives. The same token is TERMINAL — the service watermark gate
        // fires before any mount lookup or propose, so no no-op V2 entry is
        // journaled, no incarnation/row/HW/pointer state moves.
        {
            let expired = token(6, 3);
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store
                .cache
                .apply_incarnation_allocate_v2(rocks, expired, 5, 4, 3_600_000)
                .unwrap();
            // Evict the outcome row the same way the bounded window does;
            // the watermark row survives.
            let mut w = rocks.cache_write();
            w.delete_outcome(expired).unwrap();
            w.commit().unwrap();
        }
        let err = service.allocate_incarnation(token(6, 3), 7, 5).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("expired (client watermark"), "{}", msg);
        assert!(!msg.contains("raft"), "must not reach the barrier: {}", msg);
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            // Nothing moved: the setup apply left row 4 + watermark 4 +
            // pointer 5→4, and the expired retry may not touch any of it.
            assert_eq!(
                rocks
                    .cache_get_incarnation(4)
                    .unwrap()
                    .map(|r| (r.mount_id, r.revoked)),
                Some((5, false)),
                "the expired retry may not alter the incarnation row"
            );
            assert!(
                rocks.cache_get_outcome(token(6, 3)).unwrap().is_none(),
                "no outcome may reappear for the expired retry"
            );
            assert_eq!(
                rocks
                    .cache_get_state(state_tags::CACHE_INCARNATION)
                    .unwrap(),
                Some(4),
                "the durable incarnation watermark must stay at 4"
            );
            assert_eq!(
                rocks.cache_current_incarnation(5).unwrap(),
                Some(4),
                "the mount pointer must stay unchanged"
            );
        }
    }

    /// 4b gate 4 apply side: the V2 journal apply re-verifies the frozen
    /// policy snapshot against the persisted mount table — leader and
    /// follower replay the same deterministic check. A vanished mount, a
    /// TTL change, or a capability change between issuance and apply makes
    /// the entry a DETERMINISTIC NO-OP (a cache apply error is fatal to
    /// the authoritative FSM) that writes NO outcome, watermark, or
    /// pointer; the FSM keeps advancing and a later valid entry still
    /// applies. `cache_write == false` against a non-write-cache mount
    /// (false == false) must NOT slip through as a match.
    #[test]
    fn test_incarnation_allocate_v2_apply_time_policy_verification() {
        let service = build_service("apply-v2-verify", chooser(vec![worker(1)]));
        let mount = MountOptions::builder()
            .write_type(WriteType::CacheMode)
            .access_mode(AccessMode::ReadWrite)
            .write_cache(true)
            .ttl_ms(3_600_000)
            .build()
            .to_info(5, "/mnt/a", "file:///tmp/curvine-a");
        // A non-write-cache mount for the false==false probe.
        let fs_mount = MountOptions::builder()
            .write_type(WriteType::FsMode)
            .access_mode(AccessMode::ReadWrite)
            .write_cache(false)
            .build()
            .to_info(7, "/mnt/c", "file:///tmp/curvine-c");
        service
            .fs_dir
            .write()
            .unprotected_store_mount(mount)
            .unwrap();
        service
            .fs_dir
            .write()
            .unprotected_store_mount(fs_mount)
            .unwrap();

        let v2 = |ttl_ms: i64, cache_write: bool, mount_id: u32, incarnation: u64| {
            JournalEntry::CacheIncarnationAllocateV2(CacheIncarnationAllocateV2Entry {
                op_id: 1,
                rpc_id: 7,
                token: token(91, incarnation),
                mount_id,
                incarnation,
                ttl_ms,
                cache_write,
            })
        };
        // Every stale/mismatched shape below must leave NO trace: no row,
        // no policy, no outcome, no incarnation watermark, no pointer.
        let assert_no_state = |service: &CacheService, incarnation: u64| {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            assert!(
                rocks.cache_get_incarnation(incarnation).unwrap().is_none(),
                "incarnation {} must not exist",
                incarnation
            );
            // A missing policy key synthesizes the ttl-0 default (never
            // None), so assert the synthesized default — nothing durable
            // was written for this incarnation.
            assert_eq!(
                rocks
                    .cache_get_incarnation_policy(incarnation)
                    .unwrap()
                    .map(|p| p.ttl_ms),
                Some(0),
                "no durable policy row for incarnation {}",
                incarnation
            );
            assert!(
                rocks
                    .cache_get_outcome(token(91, incarnation))
                    .unwrap()
                    .is_none(),
                "outcome for incarnation {} must not exist",
                incarnation
            );
            assert_eq!(
                rocks
                    .cache_get_state(state_tags::CACHE_INCARNATION)
                    .unwrap(),
                None,
                "watermark must stay untouched by no-op applies"
            );
        };

        // TTL mismatch: deterministic no-op.
        service
            .fs_dir
            .read()
            .apply_cache_journal_entry(&v2(9, true, 5, 1))
            .unwrap();
        assert_no_state(&service, 1);

        // Capability mismatch (entry claims no write-cache): no-op.
        service
            .fs_dir
            .read()
            .apply_cache_journal_entry(&v2(3_600_000, false, 5, 1))
            .unwrap();
        assert_no_state(&service, 1);

        // false==false (non-cache entry against a non-write-cache mount)
        // must NOT pass as a match: no-op.
        service
            .fs_dir
            .read()
            .apply_cache_journal_entry(&v2(0, false, 7, 1))
            .unwrap();
        assert_no_state(&service, 1);

        // Vanished mount: no-op.
        service
            .fs_dir
            .read()
            .apply_cache_journal_entry(&v2(3_600_000, true, 99, 1))
            .unwrap();
        assert_no_state(&service, 1);

        // The FSM keeps advancing: the exact snapshot still applies after
        // all the no-ops, with the frozen policy row durable.
        service
            .fs_dir
            .read()
            .apply_cache_journal_entry(&v2(3_600_000, true, 5, 1))
            .unwrap();
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            assert_eq!(
                rocks
                    .cache_get_incarnation_policy(1)
                    .unwrap()
                    .map(|p| p.ttl_ms),
                Some(3_600_000)
            );
            assert_eq!(
                rocks
                    .cache_get_state(state_tags::CACHE_INCARNATION)
                    .unwrap(),
                Some(1)
            );
        }
    }

    /// P2-1 (4a review): when the barrier readback reveals the durable
    /// outcome recorded a different (earlier, winning) object identity, the
    /// locally planned block ids are re-derived from the committed identity
    /// — same geometry, same chosen worker sets, no block id left pointing
    /// at the burned local id.
    #[test]
    fn test_rebuild_blocks_for_identity() {
        let issued = OBJ;
        let committed = OBJ + 37;
        let lay = layout(issued, 130); // 3 blocks of 64: 64, 64, 2
        assert_eq!(lay.block_count, 3);

        let planned = full_locations(&lay);
        let rebuilt =
            CacheService::rebuild_blocks_for_identity(planned.clone(), committed, 130, 64).unwrap();
        let committed_lay = layout(committed, 130);
        for (index, (p, r)) in planned.iter().zip(rebuilt.iter()).enumerate() {
            // Worker sets preserved exactly as chosen.
            assert_eq!(p.workers, r.workers);
            // Block ids re-derived from the committed identity.
            assert_eq!(
                r.block_id,
                committed_lay.block_id(index as i64 + 1).unwrap()
            );
            // Geometry preserved: full blocks except the short tail.
            assert_eq!(
                r.block_len,
                if index as i64 + 1 == committed_lay.block_count {
                    committed_lay.last_len
                } else {
                    committed_lay.block_size
                }
            );
        }
        // No rebuilt id still references the burned identity.
        assert!(rebuilt
            .iter()
            .all(|r| r.block_id != layout(issued, 130).block_id(1).unwrap()));
    }
}
