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
//!
//! # Lock-order invariant (4c.3, review `327b30d2` item 3)
//!
//! All leader-volatile state (published locations + the physical-GC work
//! queue) lives under ONE lock, `CacheService::state`. The lock order is
//! strictly **`state` (volatile) → `fs_dir` read**: a path MAY hold the
//! volatile lock and then take the fs_dir read guard (the commit publish
//! recheck), but NO path may hold the fs_dir guard while acquiring the
//! volatile lock — every store read is scoped and dropped first (see
//! `get`, `classify_dead_victim`, and every invalidate/commit arm). The
//! `plans` map is a separate leaf lock and is never held across either
//! of the other two. While holding the volatile lock it is FORBIDDEN to
//! propose journal entries (`sync_propose_cache`): the GC queue is fed
//! only after the barrier returns. `gc_handoff_tick` extracts a bounded
//! batch under the volatile lock, RELEASES it, and only then takes the
//! WorkerManager write lock — the same release-before-wm rule the
//! worker-heartbeat handler follows when it invokes the tick before its
//! own wm lock.

use crate::master::fs::policy::ChooseContext;
use crate::master::fs::WorkerManager;
use crate::master::journal::{
    CacheAbortEntry, CacheAllocateEntry, CacheCommitEntry, CacheIdReserveEntry,
    CacheIncarnationAllocateV2Entry, CacheIncarnationRevokeEntry, CacheOutcomeGcEntry,
    CacheRemoveEntry, CacheScopeRemoveEntry, CacheTtlSweepEntry, CacheVacuumEntry, JournalEntry,
    JournalWriter,
};
use crate::master::meta::cache::entry::{
    CacheEntry, CacheEntryState, ExpiryCursor, ExpiryRow, OpOutcome, OpToken, OutcomeGcGroup,
    ScopeRemoveVictim, VacuumVictim,
};
use crate::master::meta::cache::state_tags;
use crate::master::meta::cache::LocalCacheIndexStore;
use crate::master::meta::cache::MAX_ISSUABLE_INCARNATION;
use crate::master::meta::cache::MUTATION_PAGE_CAP;
use crate::master::meta::{BlockIdCodec, CacheBlockLayout};
use crate::master::{MasterMonitor, SyncFsDir};
use curvine_core_error::{err_box, err_msg, CommonError, CommonResult};
use curvine_model::{BlockReportInfo, BlockReportStatus, StorageType, WorkerAddress};
use curvine_runtime::common::LocalTime;
use curvine_runtime::sync::ArcRwLock;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
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
/// References the authoritative meta-layer constant so the service
/// boundary and every 4c.2 apply path enforce the same byte bound.
pub const MAX_KEY_BYTES: usize = crate::master::meta::cache::entry::MAX_CACHE_KEY_BYTES;

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
    /// Commit-only, RESERVED-row states that are re-planable (task #5 RC
    /// gpt56 `3d91a095`): the load's volatile plan was lost (master
    /// restart) or its fences were invalidated (worker session/epoch
    /// changed after the writes) while the commit has NOT applied. The
    /// caller must replay the EXACT allocate (same load token — it
    /// re-plans the same identity against current sessions), rewrite the
    /// blocks per the NEW placements, and re-commit. Terminal states
    /// (revoked/fenced namespace, token divergence) stay Err — they are
    /// never re-planable.
    ReplanNeeded,
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

/// 4d.2: one published replica of one block — the holding worker plus
/// the SESSION TAG the plan carried when it was published. The read path
/// (`CacheService::get`) only serves replicas whose tag still matches
/// the worker's current registry tag (R9-1); tag 0 is the UNFENCED
/// sentinel and is never minted in production (publish only records
/// fence tags, which the pre-issuance `validate_plan_fences` guarantees
/// are real).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Replica {
    worker: WorkerAddress,
    tag: u64,
}

#[derive(Default)]
struct ObjectLocations {
    /// Geometry recorded at commit publish (4c.3): the authoritative
    /// `(len, block_size)` of THIS object_id. A committed row is the
    /// other geometry source, but tombstones zero `len` — this recorded
    /// geometry lets the GC handoff freeze work for an object whose row
    /// has already advanced past it.
    len: i64,
    block_size: i64,
    blocks: HashMap<i64, Vec<Replica>>,
}

/// Per-tick hard cap on the number of block ids the GC handoff derives
/// and enqueues into the delete queue in one tick (review `6bc4f569`
/// gate 1). Worker-facing enqueues are bounded by this cap times the
/// per-block replica bound; a full block-id list is never materialized.
pub const GC_HANDOFF_BLOCKS_PER_TICK: usize = 1024;

/// One volatile physical-GC work item (4c.3, review `6bc4f569` gate 1):
/// constant size, deduplicated by `object_id`, resumed via `next_seq`.
/// Block ids are DERIVED from the frozen geometry on demand
/// (`CacheBlockLayout::block_id`), so a million-block object costs the
/// same memory as a one-block one. Nothing here is journaled: crash
/// loses pending work and the 4d full-report/orphan pass re-derives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CacheGcWork {
    incarnation: u64,
    object_id: i64,
    len: i64,
    block_size: i64,
    /// Next 1-based block index whose delete should be enqueued.
    /// `> block_count` (derived from the frozen geometry) means done.
    next_seq: u32,
}

impl CacheGcWork {
    fn layout(&self) -> CommonResult<CacheBlockLayout> {
        CacheBlockLayout::derive(self.object_id, self.len, self.block_size)
    }
}

/// Per-object, per-turn bound of the round-robin GC handoff (review
/// `327b30d2` item 2): one tick gives every unfinished object at most
/// this many derived block ids before rotating it to the back, so a
/// max-layout object cannot monopolize the tick budget and starve
/// later, smaller objects.
pub const GC_HANDOFF_QUANTUM: usize = 256;

/// 4d.2: per-tick hard cap on the number of RETIRED-session block
/// identities the metadata-only reverse drain removes from published
/// locations in one tick. A worker that held a large object when its
/// session ended cannot monopolize the tick: entries beyond the cap stay
/// queued and drain on later ticks. This drain removes replica ROWS only
/// — no physical delete is enqueued here; the physical blocks of a
/// retired session are reclaimed by the 4d.3 full-report reconcile
/// (which re-derives them from the worker's ground truth).
pub const RETIRED_DRAIN_PER_TICK: usize = 1024;

/// 4d.2 RC3: per-worker-visit quantum inside `drain_retired`'s
/// round-robin. Each pending-retired worker removes at most this many
/// identities per visit before being rotated to the back, so two queued
/// workers each advance on every tick (fairness), and the total work per
/// tick stays bounded by `RETIRED_DRAIN_PER_TICK`.
pub const RETIRED_DRAIN_QUANTUM: usize = 256;

/// Volatile FIFO of dead-object GC work items, deduplicated by
/// `object_id` (an object id is never reused, so one item per id fully
/// dedups response-loss retries and repeated handoffs). `order` is the
/// round-robin deque; `items` keeps each work item constant-size.
#[derive(Default)]
struct CacheGcQueue {
    items: HashMap<i64, CacheGcWork>,
    order: VecDeque<i64>,
}

impl CacheGcQueue {
    /// Enqueue one work item. An existing item for the same object_id
    /// with the SAME frozen geometry is an idempotent no-op (the
    /// existing `next_seq` cursor is kept — a response-loss retry never
    /// restarts a drain); the same object_id with DIFFERENT geometry is
    /// impossible for an immutable object and fails loud (review
    /// `6bc4f569` gate 1).
    fn enqueue(&mut self, work: CacheGcWork) -> CommonResult<()> {
        if let Some(existing) = self.items.get(&work.object_id) {
            if existing.incarnation != work.incarnation
                || existing.len != work.len
                || existing.block_size != work.block_size
            {
                return err_box!(
                    "cache gc work geometry divergence for object {}: existing ({}, {}, {}) vs new ({}, {}, {})",
                    work.object_id,
                    existing.incarnation,
                    existing.len,
                    existing.block_size,
                    work.incarnation,
                    work.len,
                    work.block_size
                );
            }
            return Ok(());
        }
        self.order.push_back(work.object_id);
        self.items.insert(work.object_id, work);
        Ok(())
    }
}

/// 4d (R8-2): one retired session of a worker — the block identities it
/// held when its session ended (a later Start superseded it, or
/// End/lost-worker retired it session-exactly). Drained boundedly from
/// the queue front. RC2-round2 shape (gpt56 `25d4b51e` P0-2): entries
/// are object-keyed (`object_id → seqs`) so removing a whole object's
/// trace is one map drop — never a seq materialization. Round-4 shape
/// (gpt56 `36f4e28b` P0-2): the entries are ORDERED (`BTreeMap`/
/// `BTreeSet`) so the drain advances by first-entry/pop-first with an
/// explicit per-slot budget — a HashMap iterator's worst-case
/// O(capacity) traversal can never re-walk a sparse map head.
struct RetiredSession {
    tag: u64,
    entries: BTreeMap<i64, BTreeSet<i64>>,
}

impl RetiredSession {
    /// Total identities queued in this record (tests only — the
    /// production drain never pre-counts, round-3 P0-2).
    #[cfg(test)]
    fn entries_total(&self) -> usize {
        self.entries.values().map(|s| s.len()).sum()
    }
}

/// 4d (R8-2): per-worker reverse view of the published locations.
/// `live` holds the block identities published under the worker's
/// CURRENT session tag; a Start moves live into a retired session
/// record in O(1) instead of swapping sets. RC2-round2 shape: the
/// live set is object-keyed (`object_id → seqs`) so an object-level
/// drop is one map removal per holder — the pre-P0-2 flat
/// `(object_id, seq)` set forced a full identity walk. Round-4 (gpt56
/// `36f4e28b` P0-2): same ordered shape as `RetiredSession` so the
/// retire stays a pure O(1) move into the drained structure.
#[derive(Default)]
struct WorkerRev {
    live: BTreeMap<i64, BTreeSet<i64>>,
    retired: VecDeque<RetiredSession>,
}

impl WorkerRev {
    #[cfg(test)]
    fn live_contains(&self, object_id: i64, seq: i64) -> bool {
        self.live.get(&object_id).is_some_and(|s| s.contains(&seq))
    }

    #[cfg(test)]
    fn live_len(&self) -> usize {
        self.live.values().map(|s| s.len()).sum()
    }

    fn live_insert(&mut self, object_id: i64, seq: i64) {
        self.live.entry(object_id).or_default().insert(seq);
    }

    fn live_extend(&mut self, entries: impl IntoIterator<Item = (i64, i64)>) {
        for (object_id, seq) in entries {
            self.live.entry(object_id).or_default().insert(seq);
        }
    }

    /// Remove ONE identity, dropping the object row once the worker
    /// holds none of its blocks (bounded hygiene, O(log n)).
    fn live_remove_identity(&mut self, object_id: i64, seq: i64) {
        if let Some(seqs) = self.live.get_mut(&object_id) {
            seqs.remove(&seq);
            if seqs.is_empty() {
                self.live.remove(&object_id);
            }
        }
    }
}

/// 4d (R4/R9-3): the cache session registry entry for one worker,
/// installed by its Start heartbeat.
struct WorkerSession {
    /// The worker process's wire session id (the Start heartbeat's
    /// `worker_session_id`): the exact-match key for End/lost-worker
    /// retirement (R9-2) — a stale callback from an older process must
    /// never retire a newer session.
    session: String,
    /// Monotonic process tag (issued from `next_tag`); published
    /// location replicas record the tag they were written under so the
    /// read path can filter to the current session (R9-1).
    tag: u64,
    /// The worker address as reported at Start — the ONLY trusted
    /// address source for cache locations during the full-report window
    /// (the worker manager removes the old row at Start and inserts
    /// nothing until the first regular heartbeat).
    address: WorkerAddress,
    /// Journal epoch the session was installed under. Redundant by
    /// construction with the volatile-domain epoch fence
    /// (`sync_epoch` cold-clears the whole registry on any epoch
    /// change, so a stale row cannot survive it); kept for diagnosis
    /// and the 4d.3 reconcile's per-entry re-verify.
    #[allow(dead_code)]
    epoch: u64,
}

/// 4d RC4 (gpt56 `7ceef2ff` item 4): the FULL-endpoint identity of a
/// planned replica. `WorkerAddress`'s own PartialEq/Hash cover only
/// `worker_id` — exactly the aliasing a fence must not trust — so plan
/// fences bind (worker_id, hostname, ip, rpc_port, web_port): a reused
/// worker_id at a different endpoint can never satisfy another
/// endpoint's fence.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WorkerIdent {
    worker_id: u32,
    hostname: String,
    ip_addr: String,
    rpc_port: u32,
    web_port: u32,
}

impl WorkerIdent {
    fn of(addr: &WorkerAddress) -> Self {
        Self {
            worker_id: addr.worker_id,
            hostname: addr.hostname.clone(),
            ip_addr: addr.ip_addr.clone(),
            rpc_port: addr.rpc_port,
            web_port: addr.web_port,
        }
    }
}

/// Full-field address equality (`WorkerAddress`'s `==` is worker_id
/// only — see `WorkerIdent`). Registry rows are matched against
/// planned/evidence addresses with this, never with `==`.
fn same_full_address(a: &WorkerAddress, b: &WorkerAddress) -> bool {
    a.worker_id == b.worker_id
        && a.hostname == b.hostname
        && a.ip_addr == b.ip_addr
        && a.rpc_port == b.rpc_port
        && a.web_port == b.web_port
}

/// All leader-volatile cache state under ONE lock (4c.3 lock-order
/// contract, review `6bc4f569` gate 3/4): the published locations of
/// live objects and the GC work queue. The lock order is strictly
/// `volatile -> fs_dir read`: no code path may hold the fs_dir read
/// guard while acquiring this lock, which is what makes the
/// commit-publish recheck + GC drain serialization sound.
#[derive(Default)]
struct CacheVolatile {
    /// Journal epoch this volatile state is bound to (4d R5/R7-2
    /// leader-epoch fence): every entry point compares
    /// `monitor.journal_epoch()`; any mismatch cold-clears ALL volatile
    /// state below and rebinds, so no stale-warm state survives a
    /// leadership loss+regain. The initial 0 binds harmlessly (first
    /// touch against a nonzero epoch just clears empty maps).
    epoch: u64,
    locations: HashMap<i64, ObjectLocations>,
    /// Load plans by allocate token, merged into the volatile domain
    /// (4d R8-1 supplement): plan reads/writes are now atomic with the
    /// session registry and the epoch fence, and a cold clear drops
    /// plans too (they are leader-volatile by definition).
    plans: HashMap<OpToken, LoadPlan>,
    /// 4d session registry (R4/R9-3): worker_id → current session.
    worker_sessions: HashMap<u32, WorkerSession>,
    /// 4d reverse index (R8-2): worker_id → live set + retired sessions.
    by_worker: HashMap<u32, WorkerRev>,
    /// 4d reconcile generations (R8-4/R9-4): worker_id → generation of
    /// the last cache incremental accepted for it; a full-report
    /// reconcile only proceeds against a stable generation.
    reconcile_gens: HashMap<u32, u64>,
    /// 4d.2 RC2 delete-pending quarantine (gpt56 `7cc7295c`), reshaped
    /// in RC2-round2 (gpt56 `25d4b51e` P0-2): object_id →
    /// `(worker_id, session_tag) → seqs` this session reported as
    /// orphan/corrupt. A same-tag Finalized re-report of a quarantined
    /// identity only defers — the physical delete is still in flight,
    /// so the read path must not re-serve the block. Released by the
    /// exact `(worker, tag, seq)` Deleted ack (worker-side delete
    /// completed), a new session for that worker (old tags only), or
    /// the epoch cold clear; an object-level drop (GC completion /
    /// no-geometry retire) removes the whole row in O(#reporters).
    quarantine: HashMap<i64, HashMap<(u32, u64), HashSet<i64>>>,
    /// Directed index over `quarantine` (RC2-round2): worker → tag →
    /// objects. A Start purge or an ack resolves EXACTLY this worker's
    /// entries in O(its own identities) — never a scan of every
    /// quarantined object. Rows are cleaned lazily; a stale object
    /// reference is a harmless no-op lookup.
    quarantine_index: HashMap<u32, HashMap<u64, HashSet<i64>>>,
    /// RC2-round2: additive object → holders set fed at publish. An
    /// object-level drop consults it so the per-worker live/retired
    /// rows drop in O(#holders) map removals; a stale holder is a
    /// harmless no-op removal. Never scanned.
    location_holders: HashMap<i64, HashSet<u32>>,
    /// 4d.2 RC3 pending-retired round-robin (gpt56 `7cc7295c`): workers
    /// whose retired deque still holds work, each present AT MOST once
    /// (set membership mirrors the deque — RC2-round2 removes the
    /// linear `contains` scan). `drain_retired` pops from the front,
    /// applies a bounded quantum, and rotates unfinished workers to the
    /// back — traversal is bounded by the pending set (never the full
    /// `by_worker` map) and a large queue cannot starve the others.
    retired_rr: VecDeque<u32>,
    retired_rr_set: HashSet<u32>,
    /// Monotonic session tag issuer — NEVER reset, including by the
    /// epoch cold clear: a tag must never be reused across sessions,
    /// or a stale retired-session drain could match fresh state. 0 is
    /// reserved as the UNFENCED sentinel; the first real tag is 1.
    next_tag: u64,
    gc: CacheGcQueue,
}

impl CacheVolatile {
    /// Round-robin extraction of one tick's delete work (review
    /// `327b30d2` item 2 / `6bc4f569` gate 3): walk the object deque
    /// from the front, give every unfinished object at most one
    /// `GC_HANDOFF_QUANTUM` of derived block ids, rotate unfinished
    /// objects to the back, and stop at the global
    /// `GC_HANDOFF_BLOCKS_PER_TICK` cap. Block ids are derived on
    /// demand from the frozen geometry — never materialized as a full
    /// list. The retained locations entry supplies the target workers:
    /// a block index with NO retained location is SKIPPED (the cursor
    /// still advances; the 4d full report re-derives that replica), and
    /// a replica whose worker set is empty is skipped the same way.
    /// Completing an object (`next_seq > block_count`) removes its work
    /// item AND its retained locations entry — that is the ONLY removal
    /// point, so locations survive exactly until the drain finishes.
    fn gc_take_batch(&mut self) -> CommonResult<Vec<(u32, i64)>> {
        let mut out: Vec<(u32, i64)> = Vec::new();
        let mut budget: usize = GC_HANDOFF_BLOCKS_PER_TICK;
        let mut turns = self.gc.order.len();
        while budget > 0 && turns > 0 {
            turns -= 1;
            let Some(object_id) = self.gc.order.pop_front() else {
                break;
            };
            let Some(mut work) = self.gc.items.get(&object_id).copied() else {
                continue;
            };
            let layout = work.layout()?;
            let start = work.next_seq;
            let end =
                (start + GC_HANDOFF_QUANTUM.min(budget) as u32).min(layout.block_count as u32 + 1);
            let locations = self.locations.get(&object_id);
            for index in start..end {
                let block_id = layout.block_id(i64::from(index))?;
                if let Some(object_locations) = locations {
                    if let Some(workers) = object_locations.blocks.get(&i64::from(index)) {
                        // 4d.2 (gc clarification on `b5becac5`): the GC
                        // drain targets ALL retained replicas, including
                        // old-tag ones — filtering to the current tag here
                        // would leak the physical blocks a retired session
                        // still holds. The current-tag filter is a read-path
                        // (get) concern only.
                        for w in workers {
                            out.push((w.worker.worker_id, block_id));
                        }
                    }
                }
                // No retained location for this index: skip the replica,
                // advance the cursor (4d full report re-derives it).
            }
            budget -= (end - start) as usize;
            work.next_seq = end;
            if work.next_seq > layout.block_count as u32 {
                self.gc.items.remove(&object_id);
                // RC3: completion drops the retained locations AND every
                // reverse trace of the object's identities in the same
                // critical section — live sets, retired records, and the
                // quarantine row — so a finished object never leaks the
                // reverse index. RC2-round2: the drop is O(#holders)
                // map removals (object-keyed live/retired rows), never
                // an identity walk.
                self.drop_object_state(object_id);
            } else {
                self.gc.items.insert(object_id, work);
                self.gc.order.push_back(object_id);
            }
        }
        Ok(out)
    }

    /// 4d R5/R7-2 epoch fence: bind to the current journal epoch,
    /// cold-clearing every volatile map on mismatch. `next_tag` survives
    /// the clear (tags are never reused). Returns true when a clear
    /// happened, so entry points can log/skip accordingly.
    fn sync_epoch(&mut self, journal_epoch: u64) -> bool {
        if self.epoch == journal_epoch {
            return false;
        }
        self.epoch = journal_epoch;
        self.locations.clear();
        self.plans.clear();
        self.worker_sessions.clear();
        self.by_worker.clear();
        self.reconcile_gens.clear();
        self.quarantine.clear();
        self.quarantine_index.clear();
        self.location_holders.clear();
        self.retired_rr.clear();
        self.retired_rr_set.clear();
        self.gc.items.clear();
        self.gc.order.clear();
        true
    }

    /// 4d.2/4d.3 shared apply: mutate the volatile domain per the
    /// per-item decisions computed by `classify_cache_report`. The
    /// caller holds the volatile guard. Every decision is bound to
    /// THIS registry tag (`reg_tag`) and the registry's trusted
    /// address (`reg_address`), so the returned outcome is only safe
    /// to act on for the session it was classified under (P0-1: the
    /// fenced handler apply rechecks the tag under the transition
    /// gate before any WorkerManager side effect).
    fn apply_cache_decisions(
        &mut self,
        worker_id: u32,
        reg_tag: u64,
        reg_address: &WorkerAddress,
        decisions: HashMap<i64, CacheReportDec>,
    ) -> (Vec<i64>, Vec<i64>) {
        let mut remove_blocks = Vec::with_capacity(decisions.len());
        let mut deleted_acks = Vec::with_capacity(decisions.len());
        for (block_id, decision) in decisions {
            match decision {
                CacheReportDec::Defer => {}
                CacheReportDec::Orphan(object_id, seq) => {
                    // RC2: an orphan/corrupt report strips the reporting
                    // worker's current-tag replica and reverse trace
                    // IMMEDIATELY, in this volatile domain — the read
                    // path stops serving the block now, not after the
                    // async physical-delete ack. Identity < 0 marks an
                    // illegal block id with no cache-domain location to
                    // strip.
                    if object_id >= 0 {
                        if let Some(locs) = self.locations.get_mut(&object_id) {
                            if let Some(replicas) = locs.blocks.get_mut(&seq) {
                                replicas.retain(|r| {
                                    !(r.worker.worker_id == worker_id && r.tag == reg_tag)
                                });
                                if replicas.is_empty() {
                                    locs.blocks.remove(&seq);
                                }
                            }
                        }
                        // RC2-round2: the still-holds recheck is
                        // (worker, reg_tag) EXACT — an old-tag replica
                        // row of the same worker must not keep the
                        // current-tag live entry alive.
                        let still_holds = self
                            .locations
                            .get(&object_id)
                            .and_then(|l| l.blocks.get(&seq))
                            .is_some_and(|rs| {
                                rs.iter()
                                    .any(|r| r.worker.worker_id == worker_id && r.tag == reg_tag)
                            });
                        if !still_holds {
                            if let Some(rev) = self.by_worker.get_mut(&worker_id) {
                                rev.live_remove_identity(object_id, seq);
                            }
                        }
                        // Delete-pending quarantine (exact
                        // worker+tag+object+seq): a same-tag Finalized
                        // re-report defers until the Deleted ack or a
                        // new session releases it.
                        self.quarantine
                            .entry(object_id)
                            .or_default()
                            .entry((worker_id, reg_tag))
                            .or_default()
                            .insert(seq);
                        self.quarantine_index
                            .entry(worker_id)
                            .or_default()
                            .entry(reg_tag)
                            .or_default()
                            .insert(object_id);
                    }
                    remove_blocks.push(block_id);
                }
                CacheReportDec::Deleted(object_id, seq) => {
                    if let Some(locs) = self.locations.get_mut(&object_id) {
                        if let Some(replicas) = locs.blocks.get_mut(&seq) {
                            replicas.retain(|r| r.worker.worker_id != worker_id);
                            if replicas.is_empty() {
                                locs.blocks.remove(&seq);
                            }
                        }
                    }
                    if let Some(rev) = self.by_worker.get_mut(&worker_id) {
                        rev.live_remove_identity(object_id, seq);
                    }
                    deleted_acks.push(block_id);
                }
                CacheReportDec::Publish(object_id, seq, len, block_size) => {
                    // RC2: a delete-pending identity (this exact
                    // worker+tag) defers — the physical delete is still
                    // in flight and must not be re-served.
                    let quarantined = self
                        .quarantine
                        .get(&object_id)
                        .and_then(|row| row.get(&(worker_id, reg_tag)))
                        .is_some_and(|s| s.contains(&seq));
                    if quarantined {
                        log::warn!(
                            "cache report: block {} re-reported Finalized while delete-pending; deferred",
                            block_id
                        );
                        continue;
                    }
                    let locs = self.locations.entry(object_id).or_default();
                    if locs.block_size == 0 {
                        locs.len = len;
                        locs.block_size = block_size;
                    }
                    let replicas = locs.blocks.entry(seq).or_default();
                    // Same-worker refresh: an old-tag row (a prior
                    // session's publish) is RE-tagged to the current
                    // session — the worker still holds the block, now
                    // reported under the exact current session, so the
                    // current-tag read path must serve it.
                    let needs_refresh = !replicas
                        .iter()
                        .any(|r| r.worker.worker_id == worker_id && r.tag == reg_tag);
                    if needs_refresh {
                        replicas.retain(|r| r.worker.worker_id != worker_id);
                        replicas.push(Replica {
                            worker: reg_address.clone(),
                            tag: reg_tag,
                        });
                        self.by_worker
                            .entry(worker_id)
                            .or_default()
                            .live_insert(object_id, seq);
                        self.location_holders
                            .entry(object_id)
                            .or_default()
                            .insert(worker_id);
                    }
                }
            }
        }
        (remove_blocks, deleted_acks)
    }

    /// 4d R9-3: install the fresh session a Start heartbeat opens.
    /// Retires the worker's previous live set (O(1) move) if any, issues
    /// the next never-reused tag, and bumps the reconcile generation so
    /// a concurrent full-report reconcile aborts against the new
    /// session. Called with the volatile guard held.
    fn install_session(
        &mut self,
        worker_id: u32,
        session: String,
        address: WorkerAddress,
    ) -> CommonResult<u64> {
        if let Some(prev) = self.worker_sessions.remove(&worker_id) {
            self.retire_live(worker_id, prev.tag);
        }
        // 4d RC5 (gpt56 `7ceef2ff` item 5): the tag issuer is LOUD
        // fail-closed — a u64 overflow must never wrap (a release build
        // would silently reuse tags, and a reused tag can satisfy a stale
        // retired-session drain). The first real tag is 1.
        let tag = match self.next_tag.checked_add(1) {
            Some(tag) => tag,
            None => {
                return err_box!(
                    "cache session tag issuer exhausted u64 for worker {}: refusing to issue a reused tag; a fresh master process restarts the issuer",
                    worker_id
                )
            }
        };
        self.next_tag = tag;
        // RC2 (gpt56 `7cc7295c`): a new session releases the worker's
        // OLD-tag quarantine entries — the fresh tag is a different
        // reporter contract; only same-tag entries may keep blocking.
        // RC2-round2: the purge resolves through the directed
        // `quarantine_index` — it touches only THIS worker's entries,
        // never a scan of every quarantined object.
        if let Some(tags) = self.quarantine_index.remove(&worker_id) {
            for (t, objs) in tags {
                if t == tag {
                    self.quarantine_index
                        .entry(worker_id)
                        .or_default()
                        .insert(t, objs);
                    continue;
                }
                for obj in &objs {
                    if let Some(row) = self.quarantine.get_mut(obj) {
                        row.remove(&(worker_id, t));
                        if row.is_empty() {
                            self.quarantine.remove(obj);
                        }
                    }
                }
            }
        }
        self.worker_sessions.insert(
            worker_id,
            WorkerSession {
                session,
                tag,
                address,
                epoch: self.epoch,
            },
        );
        *self.reconcile_gens.entry(worker_id).or_insert(0) += 1;
        Ok(tag)
    }

    /// 4d R9-2: retire the worker's session EXACTLY. If the registry no
    /// longer records this wire session id (a newer Start intervened),
    /// the whole operation is a no-op — a stale End/lost-worker
    /// callback must never delete a future session's state. On an exact
    /// match the registry row is removed, the live set moves to
    /// retired, and the reconcile generation bumps. Returns true when
    /// the session was retired.
    fn retire_session(&mut self, worker_id: u32, session: &str) -> bool {
        let tag = match self.worker_sessions.get(&worker_id) {
            Some(s) if s.session == session => s.tag,
            _ => return false,
        };
        self.worker_sessions.remove(&worker_id);
        self.retire_live(worker_id, tag);
        *self.reconcile_gens.entry(worker_id).or_insert(0) += 1;
        true
    }

    /// Move the worker's live reverse set into a retired session record
    /// (O(1); an empty live set records nothing). Registers the worker
    /// on the pending-retired round-robin exactly once (RC3).
    fn retire_live(&mut self, worker_id: u32, tag: u64) {
        if let Some(rev) = self.by_worker.get_mut(&worker_id) {
            if !rev.live.is_empty() {
                let live = std::mem::take(&mut rev.live);
                rev.retired.push_back(RetiredSession { tag, entries: live });
                if !self.retired_rr_set.contains(&worker_id) {
                    self.retired_rr.push_back(worker_id);
                    self.retired_rr_set.insert(worker_id);
                }
            }
        }
    }

    /// Object-level volatile drop, round-3 bounded form (gpt56
    /// `f5980e03` P0-3): remove the retained locations entry, the
    /// quarantine row (+ its directed-index traces), the holders-index
    /// row, and each holder's LIVE object row only. Retired-session
    /// records are deliberately NOT scanned — a worker's retired deque
    /// holds one record per past session and rapid restarts make that
    /// generation count unbounded, so a synchronous sweep here would
    /// reintroduce unbounded work at GC completion. Stale identities in
    /// retired records self-heal through the bounded RR drain instead:
    /// the locations row is already gone, so the drain's replica strip
    /// is a no-op map miss and only the record entry goes. With
    /// object-keyed live sets and the additive `location_holders` index
    /// the synchronous part is O(#holders + #quarantine-reporters) map
    /// removals — no seq materialization, no holders×seqs nesting, and
    /// it works when the locations row never existed (quarantine-only
    /// objects MUST clear their row too). Same-guard callers (GC
    /// completion / no-geometry retire) make it atomic.
    fn drop_object_state(&mut self, object_id: i64) {
        self.locations.remove(&object_id);
        if let Some(row) = self.quarantine.remove(&object_id) {
            for (w, t) in row.keys().copied().collect::<Vec<_>>() {
                if let Some(tags) = self.quarantine_index.get_mut(&w) {
                    if let Some(objs) = tags.get_mut(&t) {
                        objs.remove(&object_id);
                    }
                    if tags.get(&t).is_some_and(|s| s.is_empty()) {
                        tags.remove(&t);
                    }
                }
                if self.quarantine_index.get(&w).is_none_or(|m| m.is_empty()) {
                    self.quarantine_index.remove(&w);
                }
            }
        }
        let holders = self.location_holders.remove(&object_id).unwrap_or_default();
        for worker_id in holders {
            if let Some(rev) = self.by_worker.get_mut(&worker_id) {
                // LIVE row only (round-3 P0-3): retired records are left
                // for the bounded RR drain's no-op self-heal.
                rev.live.remove(&object_id);
            }
        }
    }

    /// 4d.2 (R8-2/R9-1, RC3 fairness, round-4 bounded form): bounded
    /// METADATA-ONLY drain of retired-session reverse entries. For each
    /// retired `(object_id, seq)` identity the holding worker's replica
    /// row is removed from the published locations — the current-tag
    /// read filter would already exclude it, so this reclaims the row
    /// space and stops the stale replica from surviving forever. No
    /// physical delete is enqueued (the 4d.3 full-report reconcile owns
    /// physical reclamation for retired sessions). Bounded per call by
    /// `RETIRED_DRAIN_PER_TICK` identities AND per worker visit by
    /// `RETIRED_DRAIN_QUANTUM`; traversal covers only the pending-retired
    /// round-robin (`retired_rr`), and each visited worker is rotated to
    /// the back while unfinished, so one worker's large queue can never
    /// starve another's. A record stays at the front of its deque until
    /// fully visited. Round-3 (gpt56 `f5980e03` P0-2): the visit NEVER
    /// pre-counts the front record, and popping a degenerate EMPTY
    /// record costs one budget unit. Round-4 (gpt56 `36f4e28b` P0-2):
    /// the visit POPS identities ordered — `iter_mut().next()` /
    /// `BTreeSet::pop_first()` on the first entry — so every consumed
    /// budget unit is a concrete removal and the traversal NEVER
    /// restarts from a sparse map head (a HashMap iterator's worst-case
    /// O(capacity) scan is structurally impossible here).
    fn drain_retired(&mut self) -> usize {
        let mut budget = RETIRED_DRAIN_PER_TICK;
        let mut removed = 0usize;
        while budget > 0 {
            let Some(worker_id) = self.retired_rr.front().copied() else {
                break;
            };
            // Pop this visit's identities (tag + drained) without holding
            // the WorkerRev borrow across the locations mutation. Leading
            // empty records (which the live-only object drop can leave
            // behind) are popped at one budget unit each — bounded deque
            // traversal even for degenerate histories.
            let popped: Option<(u64, Vec<(i64, i64)>)> = {
                let Some(rev) = self.by_worker.get_mut(&worker_id) else {
                    self.retired_rr.pop_front();
                    self.retired_rr_set.remove(&worker_id);
                    continue;
                };
                loop {
                    match rev.retired.front_mut() {
                        None => break None,
                        Some(front) if front.entries.is_empty() => {
                            rev.retired.pop_front();
                            budget -= 1;
                            if budget == 0 {
                                break None;
                            }
                        }
                        Some(front) => {
                            // Round-4 P0-2: ordered pop-first drain. Each
                            // iteration removes exactly one identity from
                            // the record's first non-empty entry (and the
                            // emptied object key with it); the visit stops
                            // at the quantum/budget cap. No iteration
                            // scouts ahead, so every consumed unit of
                            // budget is one popped slot.
                            let visit_cap = budget.min(RETIRED_DRAIN_QUANTUM);
                            let tag = front.tag;
                            let mut drained: Vec<(i64, i64)> = Vec::with_capacity(visit_cap);
                            while drained.len() < visit_cap {
                                let Some((&object_id, seqs)) = front.entries.iter_mut().next()
                                else {
                                    break;
                                };
                                if let Some(seq) = seqs.pop_first() {
                                    drained.push((object_id, seq));
                                }
                                if seqs.is_empty() {
                                    front.entries.remove(&object_id);
                                }
                            }
                            break Some((tag, drained));
                        }
                    }
                }
            };
            let Some((front_tag, drained)) = popped else {
                // No non-empty record was reachable within this tick's
                // budget. Dequeue the worker when its retired deque is
                // fully empty; otherwise rotate it so the remainder is
                // visited on a later tick.
                let still_pending = self
                    .by_worker
                    .get(&worker_id)
                    .is_some_and(|rev| !rev.retired.is_empty());
                let front_worker = self.retired_rr.pop_front().unwrap();
                if still_pending {
                    self.retired_rr.push_back(front_worker);
                } else {
                    self.retired_rr_set.remove(&front_worker);
                }
                continue;
            };
            let take = drained.len();
            for (object_id, seq) in drained {
                if let Some(locs) = self.locations.get_mut(&object_id) {
                    if let Some(replicas) = locs.blocks.get_mut(&seq) {
                        // R7-3 conditional remove: only the retired
                        // TAG's replica row goes — the same worker's
                        // newer-session publish of the same identity
                        // (different tag) survives untouched.
                        replicas
                            .retain(|r| !(r.worker.worker_id == worker_id && r.tag == front_tag));
                        if replicas.is_empty() {
                            locs.blocks.remove(&seq);
                        }
                    }
                }
                removed += 1;
            }
            budget -= take;
            // Pop a fully visited record; dequeue the worker entirely
            // when its retired deque is empty, otherwise rotate it to
            // the back so every pending worker gets its turn per tick.
            let still_pending = match self.by_worker.get_mut(&worker_id) {
                Some(rev) => {
                    if let Some(front) = rev.retired.front_mut() {
                        if front.entries.is_empty() {
                            rev.retired.pop_front();
                        }
                    }
                    !rev.retired.is_empty()
                }
                None => false,
            };
            let front_worker = self.retired_rr.pop_front().unwrap();
            if still_pending {
                self.retired_rr.push_back(front_worker);
            } else {
                self.retired_rr_set.remove(&front_worker);
            }
        }
        removed
    }
}

/// #[cfg(test)] observable session-spine snapshot for one worker (see
/// `CacheService::session_spine_snapshot`). Compiled out outside
/// cfg(test).
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct SessionSpine {
    /// Registry row: (wire session, live tag).
    pub(crate) registry: Option<(String, u64)>,
    /// Tag-issuer cursor.
    pub(crate) next_tag: u64,
    /// Accumulator row: (session, terminal flag).
    pub(crate) accumulator: Option<(String, bool)>,
}

/// 4d (R7-5): the strict single-key full-report accumulator for the
/// CACHE domain — one entry per block id carrying the reported
/// `(status, len, storage_type)`. Terminal semantics (`0b900a2f`):
/// unlike the FS legacy accumulator, an invalidated session is NEVER
/// removed-and-restarted — every further full page of the same worker
/// session is permanently cache-skipped; only a successful Start
/// (installing a fresh session) creates a fresh accumulator. No
/// preallocation by `total_len`; the declared total is hard-capped by
/// configuration.
struct CacheReportSession {
    /// Wire session id this accumulator is bound to (exact match with
    /// the Start that opened it; pages from any other session are
    /// skipped).
    session: String,
    /// Declared total from the first accepted page; a later page with
    /// a different total invalidates terminally.
    total_len: u64,
    /// Block id → reported (status, len, storage type). A duplicate id
    /// with the SAME triple is idempotent; any conflicting duplicate
    /// invalidates terminally.
    entries: HashMap<i64, (BlockReportStatus, i64, StorageType)>,
    /// Terminal invalidation flag (sources: F/W/Deleted incremental,
    /// total_len conflict, duplicate conflict, overflow).
    invalid: bool,
    /// RC1 P0-1 (gpt56 `d2546338` item 1): the row is NEVER removed at
    /// checkout. `Accumulating` accepts pages; a checkout (self-
    /// Complete or the 4d.3 end-of-report take) transitions the ROW to
    /// `Reconciling` under `attempt` while the entries are handed out —
    /// so an incremental / exact End / lost landing mid-flight still
    /// finds the row and TERMINALIZES it instead of finding nothing and
    /// letting a blind reopen resurrect the session.
    /// `release_full_accumulator` is the only way back to `Accumulating`
    /// (exact session+attempt CAS; a terminalized row stays terminal).
    phase: ReportPhase,
    /// Monotonic checkout ticket, bumped on every Accumulating →
    /// Reconciling transition; the release CAS is exact on it.
    attempt: u64,
    /// RC2 P0-1 (gpt56 `53516250` window 2): the registry tag of the Start
    /// that installed this row. A same-wire-session Start RETRY installs a
    /// NEW tag and a fresh row — the `(session, tag, attempt)` checkout
    /// ticket therefore distinguishes them, so an old snapshot's ticket
    /// can never act on the retried Start's row/tag.
    tag: u64,
    update_time_ms: u64,
}

/// RC1 P0-1: lifecycle phase of one `CacheReportSession` row. The row
/// lives until a new Start replaces it — checkout hands the entry set
/// out but keeps the row (as `Reconciling`) so terminalization can
/// always reach it.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ReportPhase {
    Accumulating,
    Reconciling,
}

/// 4d.2: WorkerManager effects of one routed incremental report —
/// the caller applies both lists (`wm.remove_block` /
/// `wm.deleted_block`) AFTER the volatile guard is released; the
/// cache service itself never takes the wm lock (declared order).
/// P0-1 (gpt56 `25d4b51e`): the registry tag the decisions were made
/// under — the apply path rechecks it under the transition gate
/// (`start_gate → WM → volatile`) so a Start/lost swap between the
/// decision and the WM side effect makes the whole outcome a loud
/// no-op instead of acting on the new session. 0 = no-op outcome.
/// RC1 P0-2 (gpt56 `d2546338` item 2): the outcome also carries the
/// APPLIED reconcile generation — the fenced apply rechecks tag AND
/// generation and holds the volatile guard across the WM side effects,
/// so a same-session report that bumped the generation in between
/// makes this outcome a drop (its effects can never be re-ordered
/// after the newer report's). 0 = no-op outcome.
#[derive(Debug, Default)]
pub struct CacheIncrOutcome {
    pub session_tag: u64,
    /// Reconcile generation the volatile mutations were applied under
    /// (post-bump value). The fenced WM apply requires it to STILL be
    /// current — any newer same-session report (incremental or
    /// reconcile) supersedes this outcome entirely.
    pub gen: u64,
    /// Block ids proven orphan or corrupt for THIS worker (R1 dead
    /// classification / R3 length violation): each is enqueued into
    /// the worker's delete queue (the BlockMap re-delivers until the
    /// physical delete is acked).
    pub remove_blocks: Vec<i64>,
    /// Block ids the worker reported Deleted: each is acked in the
    /// BlockMap (a master-initiated delete completes); the volatile
    /// replica row was already removed under the guard.
    pub deleted_acks: Vec<i64>,
}

/// RC1 P0-2 (gpt56 `d2546338` item 2): the WorkerManager side effect one
/// outcome apply must perform, delivered through the fenced apply so the
/// volatile guard stays held across the WM mutation (linearization: no
/// same-session report can interleave between the volatile strip and the
/// WM bookkeeping).
pub enum WmEffect {
    RemoveBlock(i64),
    DeletedAck(i64),
}

/// 4d.2 RC2 / RC1 P0-2: release the delete-pending quarantine for one
/// EXACT `(worker, session_tag, object, seq)`. Shared by the public
/// ack path (`ack_cache_deleted`) and the fenced outcome apply
/// (`apply_outcome_fenced`) so the WM ack and the quarantine release can
/// never diverge.
///
/// Round-3 P1 (gpt56 `f5980e03` item 4): the directed index is pruned
/// as soon as THIS exact `(worker, tag)` subrow for the object is empty
/// — independently of the object row's other reporters. Waiting for the
/// whole object row to empty leaks a stale object reference in
/// `quarantine_index[worker][tag]` whenever another reporter still holds
/// quarantine for the object.
fn release_quarantine_identity(
    volatile: &mut CacheVolatile,
    worker_id: u32,
    session_tag: u64,
    object_id: i64,
    seq: i64,
) {
    let key = (worker_id, session_tag);
    let mut index_prune = false;
    let row_now_empty = {
        let Some(row) = volatile.quarantine.get_mut(&object_id) else {
            return;
        };
        let key_empty = if let Some(seqs) = row.get_mut(&key) {
            seqs.remove(&seq);
            seqs.is_empty()
        } else {
            false
        };
        if key_empty {
            row.remove(&key);
            index_prune = true;
        }
        row.is_empty()
    };
    if row_now_empty {
        volatile.quarantine.remove(&object_id);
    }
    if index_prune {
        if let Some(tags) = volatile.quarantine_index.get_mut(&worker_id) {
            if let Some(objs) = tags.get_mut(&session_tag) {
                objs.remove(&object_id);
            }
            if tags.get(&session_tag).is_some_and(|s| s.is_empty()) {
                tags.remove(&session_tag);
            }
        }
        if volatile
            .quarantine_index
            .get(&worker_id)
            .is_none_or(|m| m.is_empty())
        {
            volatile.quarantine_index.remove(&worker_id);
        }
    }
}

/// 4d.2/4d.3: per-item routing decision for ONE reported cache block,
/// classified against the authoritative store (shared by the
/// incremental path and the 4d.3 full-report reconcile so the two can
/// never drift). Computed under an `fs_dir` read guard; applied under
/// the volatile guard.
enum CacheReportDec {
    Defer,
    Orphan(i64, i64),
    Deleted(i64, i64),
    Publish(i64, i64, i64, i64),
}

/// RC2 page fold: one terminal decision per block id, with the
/// conservative precedence Orphan >= Deleted > Publish > Defer —
/// an orphan/corrupt or deleted identity can NEVER be re-published
/// inside the same page/snapshot, regardless of duplicate input order.
fn cache_dec_rank(dec: &CacheReportDec) -> u8 {
    match dec {
        CacheReportDec::Orphan(..) => 3,
        CacheReportDec::Deleted(..) => 2,
        CacheReportDec::Publish(..) => 1,
        CacheReportDec::Defer => 0,
    }
}

/// 4d.3 deterministic-race seam: fired by
/// `CacheService::reconcile_cache_full_report` exactly BETWEEN the
/// fence capture (phase A) and the final recheck guard (phase B) — the
/// window in which a Start / lost retire / incremental / epoch flip
/// must make the whole reconcile a no-op. Never set in production;
/// compiled out entirely outside cfg(test).
#[cfg(test)]
pub(crate) static FULL_RECONCILE_SEAM: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>> =
    std::sync::Mutex::new(None);

/// RC2 deterministic-race seam (gpt56 `53516250` window 1): fired by
/// `CacheService::incr_block_report` exactly BETWEEN the same-session
/// terminalization (invalid written) and the volatile acquisition
/// (generation bump) — while the ACCUMULATOR guard is still held. The
/// production critical section holds that guard across the volatile
/// section, so a concurrent reconcile can only BLOCK on it (never
/// interleave); a hook must therefore NOT take `report_sessions`
/// itself (self-deadlock) — spawn a thread and observe from the test
/// body instead. Never set in production; compiled out outside
/// cfg(test).
#[cfg(test)]
pub(crate) static INCR_TERMINALIZE_SEAM: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>> =
    std::sync::Mutex::new(None);

/// RC2 P0-1 (gpt56 `53516250` window 2): the exact checkout ticket of
/// one full-report snapshot — the accumulator row's
/// `(registry tag, attempt)` AT CHECKOUT TIME. Bound to the unique
/// Start identity (the tag), so a same-wire-session Start RETRY (new
/// tag, fresh row) makes every outstanding old ticket a full no-op.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FullSnapshotTicket {
    pub tag: u64,
    pub attempt: u64,
}

/// 4d (R7-5): result of feeding one full-report page into the cache
/// accumulator.
#[derive(Debug)]
pub enum CacheFullReportOutcome {
    /// No valid accumulation for this page (unknown/foreign session,
    /// terminal-invalid, total conflict, duplicate conflict, or cap
    /// overflow) — the page is cache-skipped.
    Skipped,
    /// Accumulation continues (unique count < declared total).
    Partial,
    /// This page completed a valid accumulation (unique count ==
    /// total): the full entry set is handed out TOGETHER with its
    /// exact checkout ticket (RC2: carried out atomically at the
    /// in-place transition — never re-read afterwards, so a Start
    /// retry between Complete and the reconcile cannot rebind it).
    Complete(Vec<BlockReportInfo>, FullSnapshotTicket),
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
    /// 4d R8-3 fence: journal epoch captured at planning time. A commit
    /// may only propose while the volatile domain still sits on this
    /// epoch; the settle path re-checks it across the barrier.
    epoch: u64,
    /// 4d R8-3 + RC4 fence: per-block map of FULL-endpoint worker
    /// identity → session tag, captured from the registry at planning
    /// time (parallel to `blocks[i]`). At validation the actual commit
    /// evidence workers are looked up BY IDENTITY — not by position —
    /// so subset/reordered evidence cannot mis-pair tags; a missing
    /// identity or a registry row whose tag or full address drifted is
    /// a breach. Empty (test-installed via the `#[cfg(test)]`
    /// `install_plan` seam) plans freeze nothing and skip the
    /// per-replica check.
    fences: Vec<HashMap<WorkerIdent, u64>>,
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
    /// Published locations + GC work queue + load plans + the 4d
    /// session/epoch spine, all under one lock (4c.3 + 4d R8-1
    /// supplement): merging the plans into the volatile domain makes
    /// plan access atomic with the session registry and the epoch
    /// fence, and lets a cold clear drop plans together with the rest.
    state: Mutex<CacheVolatile>,
    /// 4d (R7-5) cache-domain full-report accumulators, keyed by worker
    /// id. A SEPARATE leaf from the volatile domain on purpose: the
    /// declared lock order is accumulator → CacheVolatile (the Start
    /// critical section takes both, in that order, never reversed).
    report_sessions: Mutex<HashMap<u32, CacheReportSession>>,
    /// 4d (R7-5) configured hard cap on a session's declared total.
    report_total_cap: u64,
    /// One-shot fault-injection seam fired between a sync-propose
    /// barrier's return and the code's post-barrier verification (reserve
    /// epoch check / commit-invalidate readback). Tests use it to make
    /// "another mutation raced the barrier" deterministic. Never set in
    /// production; compiled out entirely outside `cfg(test)`.
    #[cfg(test)]
    barrier_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    /// One-shot fault-injection seam fired between a commit's barrier
    /// readback (exact Valid) and the locked locations publish (4c.3,
    /// review `6bc4f569` gate 4). Tests use it to make "the row was
    /// fenced between readback and publish" deterministic. Never set in
    /// production; compiled out entirely outside `cfg(test)`.
    #[cfg(test)]
    publish_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

/// Per-call upper bound on proposed pages for every 4c.2 bounded-mutation
/// driver: the single-call derived work stays bounded even against a
/// pathological namespace; larger jobs resume through the returned cursor.
pub const MUTATION_MAX_PAGES_PER_CALL: usize = 256;

/// Resumable progress of one scope-remove driver call (4c.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeRemoveProgress {
    /// Scan boundary reached: the whole scope was paged.
    pub done: bool,
    /// Exclusive resume cursor (last scanned key); `None` only when
    /// `done` and the scope never yielded a row.
    pub cursor: Option<String>,
    /// Victims journaled this call.
    pub processed: usize,
}

/// Resumable progress of one TTL-sweep driver call (4c.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TtlSweepProgress {
    /// The due range was fully paged at this deadline.
    pub done: bool,
    /// Exclusive resume cursor (last scanned frozen position).
    pub cursor: Option<ExpiryCursor>,
    /// Expiry victims journaled this call.
    pub processed: usize,
}

/// Resumable progress of one vacuum driver call (4c.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VacuumProgress {
    /// The incarnation was fully paged.
    pub done: bool,
    /// Exclusive resume cursor (last scanned key).
    pub cursor: Option<String>,
    /// Victims journaled this call.
    pub processed: usize,
}

/// Resumable progress of one outcome-GC driver call (4c.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeGcProgress {
    /// The outcome window was fully paged.
    pub done: bool,
    /// Exclusive resume cursor (last scanned token).
    pub cursor: Option<OpToken>,
    /// Evictions journaled this call (sum of grouped op seqs).
    pub processed: usize,
}

impl CacheService {
    pub fn new(
        fs_dir: SyncFsDir,
        journal_writer: Arc<JournalWriter>,
        monitor: MasterMonitor,
        chooser: Arc<dyn CacheWorkerChooser>,
        enabled: bool,
        report_total_cap: u64,
    ) -> Self {
        Self {
            fs_dir,
            journal_writer,
            monitor,
            chooser,
            enabled,
            issue_lock: Mutex::new(()),
            segment: Mutex::new(None),
            state: Mutex::new(CacheVolatile::default()),
            report_sessions: Mutex::new(HashMap::new()),
            report_total_cap,
            #[cfg(test)]
            barrier_hook: Mutex::new(None),
            #[cfg(test)]
            publish_hook: Mutex::new(None),
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

    /// Phase 3 (dual-mode metadata split): the AUTHORITATIVE current
    /// incarnation of a mount, read by the job runner when it mints a
    /// `CacheLoadSpec`. `None` = no installed incarnation: the load fails
    /// closed — the worker must never self-issue one (gpt56 `f7788b98`
    /// point 4: provenance is the master's alone).
    pub fn current_incarnation_for_mount(&self, mount_id: u32) -> CommonResult<Option<u64>> {
        let store = self.fs_dir.read();
        let rocks = store.get_rocks_store();
        rocks.cache_current_incarnation(mount_id).map_err(fs_err)
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

    /// Arm the one-shot publish-race seam (test-only): fires between a
    /// commit's exact-Valid readback and its locked locations publish.
    #[cfg(test)]
    pub(crate) fn set_publish_hook(&self, hook: Box<dyn Fn() + Send + Sync>) {
        *self.publish_hook.lock().unwrap() = Some(hook);
    }

    /// Fire (and disarm) the publish hook if armed. The guard is released
    /// before invoking the hook (same rule as the barrier hook: a hook
    /// that re-enters the service must not re-lock this mutex).
    #[cfg(test)]
    fn fire_publish_hook(&self) {
        let hook = self.publish_hook.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Production no-op (see `fire_publish_hook`).
    #[cfg(not(test))]
    #[inline(always)]
    fn fire_publish_hook(&self) {}

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
        // Lock order (4c.3 invariant): the fs_dir guard is scoped to
        // this read and dropped before the volatile lock below.
        let entry = {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            rocks.cache_get_entry(incarnation, key).map_err(fs_err)?
        };
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
            // One guard supplies both views (re-locking inside the loop
            // would self-deadlock on the volatile mutex).
            let volatile = self.lock_volatile();
            let Some(object_locations) = volatile.locations.get(&entry.object_id) else {
                return Ok(None);
            };
            if object_locations.blocks.len() != layout.block_count as usize {
                return Ok(None);
            }
            for index in 1..=layout.block_count {
                let Some(replicas) = object_locations.blocks.get(&index) else {
                    return Ok(None);
                };
                // 4d.2 (R9-1): serve ONLY current-tag replicas — a replica
                // is readable iff its worker's registry row exists AND its
                // tag matches. Tag 0 (UNFENCED, dead-evidence) is never
                // served: production never mints tag-0 published replicas
                // and the registry never records tag 0. A block with no
                // surviving current-tag replica is a whole-object miss.
                let workers: Vec<WorkerAddress> = replicas
                    .iter()
                    .filter(|r| {
                        volatile
                            .worker_sessions
                            .get(&r.worker.worker_id)
                            .is_some_and(|s| s.tag == r.tag)
                    })
                    .map(|r| r.worker.clone())
                    .collect();
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
                    workers,
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
            // Scoped lock: the guard must be released before the fence
            // re-check and the replan arm, which take the volatile lock
            // again.
            let prior = {
                let volatile = self.lock_volatile();
                volatile.plans.get(&token).cloned()
            };
            let blocks = match prior {
                // A surviving plan is served verbatim ONLY while its
                // fences still hold (gpt56 `1c436760`): a plan whose
                // epoch/replica session fences were invalidated would
                // hand the caller the SAME stale placements and loop
                // REPLAN_NEEDED forever — it must re-plan fresh.
                Some(plan) if self.validate_plan_fences(&plan).is_ok() => plan.blocks,
                _ => {
                    // Plan lost (master restart) or fenced out: regenerate
                    // a fresh volatile plan for the same identity.
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
        // 4d R8-3: freeze the plan fences (epoch + per-replica session
        // tags) for the just-chosen sets before any identity is issued.
        let (plan_epoch, plan_fences) = self.capture_plan_fences(&layout_worker_sets)?;
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
                self.lock_volatile().plans.insert(
                    token,
                    LoadPlan {
                        object_id,
                        generation: out_generation,
                        file_len,
                        block_size,
                        replicas,
                        blocks: blocks.clone(),
                        epoch: plan_epoch,
                        fences: plan_fences.clone(),
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
        // 4d R8-3: a re-plan freezes FRESH fences against the current
        // sessions — the lost plan's fences are never resurrected.
        let (plan_epoch, plan_fences) = self.capture_plan_fences(&sets)?;
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
        self.lock_volatile().plans.insert(
            token,
            LoadPlan {
                object_id: layout.object_id,
                generation,
                file_len: layout.len,
                block_size: layout.block_size,
                replicas,
                blocks: blocks.clone(),
                epoch: plan_epoch,
                fences: plan_fences,
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
        // SKIPPED for an exact recorded Committed outcome: that outcome
        // echoes the load token itself, so its exact-match comparison
        // above has ALREADY bound identity and geometry — and the load
        // outcome row is evictable by the bounded outcome window (its
        // op_seq sits below the client watermark the commit itself pushed
        // forward), so a response-loss retry must never depend on
        // re-reading it (task #5 RC gpt56 `4ebcff5a`).
        if !committed_outcome_exact {
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
                    self.lock_volatile().plans.remove(&load_token);
                    return Ok(CacheOpStatus::Superseded {
                        expected: generation,
                        current: 0,
                    });
                }
                Some(cur) if cur.generation > generation => {
                    drop(store);
                    self.lock_volatile().plans.remove(&load_token);
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
                    self.lock_volatile().plans.remove(&load_token);
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
                        self.lock_volatile().plans.remove(&load_token);
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
                    self.lock_volatile().plans.remove(&load_token);
                    return Ok(CacheOpStatus::AlreadyApplied);
                }
                Some(_) => (), // Reserved at the right identity: apply.
            }
        }

        // The volatile plan is mandatory: without it the reported
        // locations cannot be validated against what the master planned
        // (master restart), and a silent re-plan would misattribute old
        // writes. Typed REPLAN_NEEDED (task #5 RC `3d91a095`): this is a
        // re-planable RESERVED-row state, not an error — the caller
        // replays the exact allocate (which re-plans the same identity)
        // and re-commits. Unreachable for an applied commit: an applied
        // commit resolved earlier from its outcome/Valid row.
        let plan = {
            let volatile = self.lock_volatile();
            match volatile.plans.get(&load_token).cloned() {
                Some(plan) => plan,
                None => return Ok(CacheOpStatus::ReplanNeeded),
            }
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

        // 4d R8-3 pre-propose fence: the plan's epoch + replica
        // session tags are validated against the live registry BEFORE
        // anything is proposed. A breach is re-planable the same way a
        // lost plan is (typed REPLAN_NEEDED, task #5 RC `3d91a095`):
        // replay the exact allocate (which re-plans against the current
        // sessions) and re-commit. Every check_plan_fences error is a
        // fence breach (epoch change, unfenced replica, session tag
        // change) — never a divergence, which was rejected above.
        if self.validate_plan_fences(&plan).is_err() {
            // gpt56 `1c436760`: the fenced-out plan must not stay
            // replayable — the exact allocate retry would otherwise hand
            // the SAME stale placements back and the runner would loop
            // REPLAN_NEEDED until timeout. Drop it under the volatile
            // lock so the retry re-plans against the CURRENT sessions.
            self.lock_volatile().plans.remove(&load_token);
            return Ok(CacheOpStatus::ReplanNeeded);
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
        // deterministically here, before the terminal settlement.
        self.fire_barrier_hook();
        self.commit_barrier_settle(
            &load_token,
            incarnation,
            key,
            generation,
            object_id,
            len,
            ufs_mtime,
            expire_at,
            plan.block_size,
            plan.epoch,
            plan.fences,
            blocks,
        )
    }

    /// Post-barrier settlement of one validated commit (4c.3, reviews
    /// `6bc4f569` gate 4 / `4b2e2a72` P0-2): the propose barrier has
    /// returned and the plan + block evidence are already validated. Every
    /// DEAD terminal branch — revoked incarnation, vanished row, advanced
    /// or tombstoned row, fenced-at-publish race — hands the validated
    /// evidence to physical GC under the volatile lock with an
    /// authoritative recheck before the plan is spent; only an exact-Valid
    /// row publishes. Without this, a commit that raced an
    /// invalidate/revoke would strand the workers' physical blocks until
    /// the 4d orphan pass. Lock order (4c.3 invariant): volatile →
    /// fs_dir read, and no propose / WorkerManager access under volatile.
    /// Test seam: unit tests call this directly to reproduce the
    /// propose→readback race deterministically (the raft barrier itself
    /// fails closed in testing mode).
    #[allow(clippy::too_many_arguments)]
    fn commit_barrier_settle(
        &self,
        load_token: &OpToken,
        incarnation: u64,
        key: &str,
        generation: u64,
        object_id: i64,
        len: i64,
        ufs_mtime: i64,
        expire_at: i64,
        block_size: i64,
        plan_epoch: u64,
        plan_fences: Vec<HashMap<WorkerIdent, u64>>,
        blocks: Vec<CacheBlockLocation>,
    ) -> CommonResult<CacheOpStatus> {
        // Test seam (4c.3, review `6bc4f569` gate 4): "the row was fenced
        // between the barrier and the locked publish below" lands
        // deterministically here. Compiled out in production.
        self.fire_publish_hook();

        // Coordinated decision (review `618498f7`): read the
        // authoritative incarnation state AND the entry row while holding
        // the volatile lock (order: volatile → fs_dir read), so an
        // inactive namespace can never be outrun by an exact-Valid
        // publish — a fenced incarnation wins even over an exact row,
        // and the workers' blocks still go to GC:
        //
        // - namespace active AND row exact-Valid full match → publish
        //   (recording the geometry for the later GC handoff of THIS
        //   object);
        // - namespace active, exact identity but divergent immutable
        //   fields → loud divergence (plan kept for triage);
        // - anything else (revoked/stale namespace, missing, advanced,
        //   tombstoned row) → NEVER publish: merge the commit's block
        //   evidence into the retained locations (so the GC drain can
        //   target the workers that actually hold the blocks) and ensure
        //   a work item exists — re-seeding both if the drain already
        //   completed. A late publish would otherwise resurrect a dead
        //   object's locations after GC finished.
        enum Settlement {
            Applied,
            Superseded {
                current: u64,
            },
            Fenced,
            Divergence(CacheEntry),
            ReadbackFailure(CacheEntry),
            /// 4d R8-3 settle re-check: the plan's epoch or a planned
            /// replica's session tag no longer holds (worker restarted
            /// or leadership changed across the propose barrier).
            /// Terminal for THIS commit: nothing is published, no dead
            /// evidence is merged, and the plan is spent — the client
            /// replays the exact allocate (fresh fences) and re-commits.
            /// Any blocks the workers already wrote are re-derived by
            /// the restarted worker's own full report.
            PlanFenceLost,
        }
        let settlement = {
            let mut volatile = self.lock_volatile();
            // 4d R8-3 settle re-check FIRST, under the volatile guard:
            // if the plan fences no longer hold, the commit must not
            // publish NOR merge — even against an exact-Valid row.
            if let Err(e) = Self::check_plan_fences(&volatile, plan_epoch, &blocks, &plan_fences) {
                log::warn!("cache commit settle fence breach: {}", e);
                Settlement::PlanFenceLost
            } else {
                // ONE authoritative snapshot (review `4dd264df` P0-1): the
                // incarnation row, the mount's current pointer, and the entry
                // row are read consecutively under a SINGLE fs_dir guard, so
                // a revoke cannot commit between the namespace read and the
                // entry read (lock order: volatile → fs_dir read).
                let (active, cur) = {
                    let store = self.fs_dir.read();
                    let rocks = store.get_rocks_store();
                    let active = match rocks.cache_get_incarnation(incarnation).map_err(fs_err)? {
                        Some(row) if !row.revoked => {
                            rocks
                                .cache_current_incarnation(row.mount_id)
                                .map_err(fs_err)?
                                == Some(incarnation)
                        }
                        _ => false,
                    };
                    let cur = rocks.cache_get_entry(incarnation, key).map_err(fs_err)?;
                    (active, cur)
                };
                // Proven-dead classification (review `4dd264df` P0-2): only
                // an inactive namespace, a missing row, a row that already
                // belongs to a different object, a same-object tombstone, or
                // an advanced generation prove THIS load dead. An ACTIVE
                // same-object Reserved row (our apply did not land and
                // nothing else fenced the entry) is NOT dead — an exact
                // allocate retry can still plan and commit the same
                // object_id, so GC must never see it and the plan is kept.
                let merge_dead = |volatile: &mut CacheVolatile| -> CommonResult<()> {
                    self.merge_dead_commit_evidence(
                        volatile,
                        incarnation,
                        object_id,
                        len,
                        block_size,
                        &blocks,
                    )
                };
                if !active {
                    merge_dead(&mut volatile)?;
                    Settlement::Fenced
                } else {
                    match cur {
                        None => {
                            merge_dead(&mut volatile)?;
                            Settlement::Superseded { current: 0 }
                        }
                        Some(c) if c.object_id != object_id || c.generation > generation => {
                            merge_dead(&mut volatile)?;
                            Settlement::Superseded {
                                current: c.generation,
                            }
                        }
                        Some(c)
                            if c.generation == generation
                                && c.state == CacheEntryState::Tombstoned =>
                        {
                            merge_dead(&mut volatile)?;
                            Settlement::Superseded {
                                current: c.generation,
                            }
                        }
                        Some(c)
                            if c.state == CacheEntryState::Valid
                                && c.generation == generation
                                && c.object_id == object_id =>
                        {
                            if c.len == len && c.ufs_mtime == ufs_mtime && c.expire_at == expire_at
                            {
                                let object_locations =
                                    volatile.locations.entry(object_id).or_default();
                                object_locations.len = len;
                                object_locations.block_size = block_size;
                                object_locations.blocks.clear();
                                // 4d.2: each published replica records the
                                // session TAG the plan fence bound it to
                                // (`plan_fences` is index-parallel to
                                // `blocks`); production fences always cover
                                // every planned worker (pre-issuance
                                // `validate_plan_fences` fails closed), so
                                // the tag-0 fallback below is test-only and
                                // the current-tag read filter excludes it.
                                // The same identities feed the reverse
                                // index's live set (R8-2).
                                let mut live_feed: HashMap<u32, Vec<(i64, i64)>> = HashMap::new();
                                for (index, block) in blocks.into_iter().enumerate() {
                                    let seq = (index + 1) as i64;
                                    let fence = plan_fences.get(index);
                                    let replicas: Vec<Replica> = block
                                        .workers
                                        .into_iter()
                                        .map(|w| {
                                            let tag = fence
                                                .and_then(|f| f.get(&WorkerIdent::of(&w)))
                                                .copied()
                                                .unwrap_or(0);
                                            live_feed
                                                .entry(w.worker_id)
                                                .or_default()
                                                .push((object_id, seq));
                                            Replica { worker: w, tag }
                                        })
                                        .collect();
                                    object_locations.blocks.insert(seq, replicas);
                                }
                                for (worker_id, entries) in live_feed {
                                    volatile
                                        .by_worker
                                        .entry(worker_id)
                                        .or_default()
                                        .live_extend(entries);
                                    // RC2-round2: the additive holders index
                                    // feeds every publish path so an
                                    // object-level drop is O(#holders).
                                    volatile
                                        .location_holders
                                        .entry(object_id)
                                        .or_default()
                                        .insert(worker_id);
                                }
                                Settlement::Applied
                            } else {
                                Settlement::Divergence(c)
                            }
                        }
                        // Active + same-object Reserved@generation (or an
                        // impossible lower generation): not proven dead —
                        // loud, plan kept, nothing enqueued.
                        Some(c) => Settlement::ReadbackFailure(c),
                    }
                }
            }
        };
        match settlement {
            Settlement::Applied => {
                // Terminal state reached: the plan is spent.
                self.lock_volatile().plans.remove(load_token);
                Ok(CacheOpStatus::Applied)
            }
            Settlement::Superseded { current } => {
                // The load is terminal: the plan is spent either way.
                self.lock_volatile().plans.remove(load_token);
                Ok(CacheOpStatus::Superseded {
                    expected: generation,
                    current,
                })
            }
            Settlement::Fenced => {
                self.lock_volatile().plans.remove(load_token);
                Err(Self::fenced(incarnation))
            }
            Settlement::Divergence(cur) => err_box!(
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
            ),
            Settlement::ReadbackFailure(cur) => err_box!(
                "cache commit barrier readback failed for ({}, {}): {:?}",
                incarnation,
                key,
                cur
            ),
            Settlement::PlanFenceLost => {
                // 4d R8-3: the plan is spent (terminal for this
                // commit); the exact allocate replay re-plans with
                // fresh fences.
                self.lock_volatile().plans.remove(load_token);
                err_box!(
                    "cache commit plan fence lost across the propose barrier for ({}, {})@{}: zero locations published, no GC merge; replay the exact allocate to re-plan, then re-commit",
                    incarnation,
                    key,
                    generation
                )
            }
        }
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
                    // The whole row is gone (vacuum). The object is dead,
                    // but a MISSING row proves nothing about which object
                    // this key owned — `expected_object_id` is
                    // client-supplied, so it must NOT drive any volatile
                    // cleanup (a forged id would delete a live object's
                    // locations; review `4b2e2a72` P0-1). Physical GC for
                    // vacuumed objects comes from the vacuum driver's
                    // server-derived frozen victims.
                    drop(store);
                    return Ok(CacheOpStatus::Superseded {
                        expected: new_generation,
                        current: 0,
                    });
                }
                Some(cur) if cur.generation > new_generation => {
                    // Fenced far past our target: terminal Superseded.
                    // Volatile cleanup only when the live row confirms the
                    // object identity we were told to fence — never on an
                    // unverified id.
                    let verified = cur.object_id == expected_object_id;
                    let geometry = if verified && cur.len > 0 {
                        Some((cur.len, cur.block_size))
                    } else {
                        None
                    };
                    drop(store);
                    if verified {
                        self.retire_object_state(incarnation, cur.object_id, geometry)?;
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
                        // A tombstone zeroes len: geometry falls back to
                        // the commit-published one inside the retire.
                        drop(store);
                        self.retire_object_state(incarnation, cur.object_id, None)?;
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
                    let geometry = if verified && cur.len > 0 {
                        Some((cur.len, cur.block_size))
                    } else {
                        None
                    };
                    drop(store);
                    if verified {
                        self.retire_object_state(incarnation, cur.object_id, geometry)?;
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
                // Our fence applied; the tombstone zeroes len, so the GC
                // geometry falls back to the commit-published one.
                drop(store);
                self.retire_object_state(incarnation, cur.object_id, None)?;
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
                let geometry = if verified && cur.len > 0 {
                    Some((cur.len, cur.block_size))
                } else {
                    None
                };
                drop(store);
                if verified {
                    self.retire_object_state(incarnation, cur.object_id, geometry)?;
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

    /// Runner-side durable escape for a load that failed BEFORE its
    /// commit was issued (task #5 gate 2, gpt56 `fca627f5`): the
    /// allocate persisted a Reserved row, and without an abort that row
    /// would wedge the key forever (allocate only accepts None/Tombstoned
    /// rows). The expected generation/object_id are resolved from the
    /// durable load outcome — never client-supplied — and the abort is
    /// REFUSED fail-closed when the load's commit token already has a
    /// recorded (applied) outcome: a commit that may have applied must be
    /// resolved by its own verbatim retry, never aborted underneath.
    pub fn abort(
        &self,
        rpc_id: i64,
        load_token: OpToken,
        commit_token: OpToken,
        incarnation: u64,
        key: &str,
    ) -> CommonResult<CacheOpStatus> {
        self.require_enabled()?;
        self.require_leader()?;
        validate_key(key)?;
        validate_client_token(load_token)?;
        validate_client_token(commit_token)?;
        if load_token.client_id != commit_token.client_id {
            return err_box!(
                "cache abort token domain mismatch: load {:?} vs commit {:?}",
                load_token,
                commit_token
            );
        }
        // Incarnation gate first: a revoked/stale namespace is terminal.
        if !self.incarnation_active(incarnation)? {
            return Err(Self::fenced(incarnation));
        }

        // Load binding: the abort may only release the row its own
        // allocate reserved, resolved from the durable outcome (never a
        // client-supplied identity).
        let (expected_generation, expected_object_id) = {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            match rocks.cache_get_outcome(load_token).map_err(fs_err)? {
                Some(OpOutcome::Allocated {
                    incarnation: out_inc,
                    key: out_key,
                    generation,
                    object_id,
                    ..
                }) => {
                    if out_inc != incarnation || out_key != key {
                        return err_box!(
                            "cache abort load token {:?} recorded ({}, {}) but abort says ({}, {})",
                            load_token,
                            out_inc,
                            out_key,
                            incarnation,
                            key
                        );
                    }
                    (generation, object_id)
                }
                _other => {
                    // No recorded allocation: this load never reserved
                    // anything (a Reserved row on the key, if any, belongs
                    // to another load and must not be touched). Terminal
                    // no-op classified from the live row.
                    let cur = rocks.cache_get_entry(incarnation, key).map_err(fs_err)?;
                    return Ok(CacheOpStatus::Superseded {
                        expected: 0,
                        current: cur.map(|c| c.generation).unwrap_or(0),
                    });
                }
            }
        };
        let new_generation = expected_generation
            .checked_add(1)
            .ok_or_else(|| cm_err("cache abort generation overflow: entry is terminal"))?;

        // Commit-outcome guard (gpt56 gate-#2 constraint): a recorded
        // Committed outcome means the load's commit applied (outcome
        // eviction keeps the row at the client watermark), so the entry
        // may be Valid — aborting underneath it is refused fail-closed.
        // A recorded Aborted outcome for THIS load is the abort's own
        // durable record (the commit token is the shared first-winner
        // token): the replay continues to the row classification, which
        // resolves AlreadyApplied from the tombstoned fence.
        {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            if let Some(outcome) = rocks.cache_get_outcome(commit_token).map_err(fs_err)? {
                let own_abort = matches!(
                    &outcome,
                    OpOutcome::Aborted {
                        incarnation: out_inc,
                        key: out_key,
                        generation: out_gen,
                        object_id: out_obj,
                        load_token: out_load,
                    }
                        if *out_inc == incarnation
                            && out_key == key
                            && *out_gen == expected_generation
                            && *out_obj == expected_object_id
                            && *out_load == load_token
                );
                if !own_abort {
                    return err_box!(
                        "cache abort for load {:?} refused: its commit token {:?} has a recorded outcome (the commit may have applied — resolve it with a verbatim commit retry): {:?}",
                        load_token,
                        commit_token,
                        outcome
                    );
                }
            }
        }

        // Row classification before the propose.
        {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            match rocks.cache_get_entry(incarnation, key).map_err(fs_err)? {
                None => {
                    // Vacuumed while the load held it: nothing to release.
                    drop(store);
                    self.lock_volatile().plans.remove(&load_token);
                    return Ok(CacheOpStatus::Superseded {
                        expected: new_generation,
                        current: 0,
                    });
                }
                Some(cur) if cur.generation == new_generation => {
                    if cur.state == CacheEntryState::Tombstoned
                        && cur.object_id == expected_object_id
                    {
                        // Idempotent abort replay: the fence already
                        // applied; just re-run the terminal cleanup.
                        drop(store);
                        self.retire_object_state(incarnation, expected_object_id, None)?;
                        self.lock_volatile().plans.remove(&load_token);
                        return Ok(CacheOpStatus::AlreadyApplied);
                    }
                    return err_box!(
                        "cache abort replay divergence for ({}, {})@{}: state {:?} object {} (expected {})",
                        incarnation,
                        key,
                        cur.generation,
                        cur.state,
                        cur.object_id,
                        expected_object_id
                    );
                }
                Some(cur) if cur.generation == expected_generation => {
                    if cur.object_id != expected_object_id {
                        return err_box!(
                            "cache abort identity mismatch for ({}, {})@{}: committed object {} vs load allocation {}",
                            incarnation,
                            key,
                            cur.generation,
                            cur.object_id,
                            expected_object_id
                        );
                    }
                    match cur.state {
                        CacheEntryState::Reserved => (), // the wedge to release
                        CacheEntryState::Valid => {
                            // Contradicts the commit-outcome guard (a
                            // Valid row implies an applied commit): someone
                            // else wrote this generation — refuse.
                            return err_box!(
                                "cache abort for ({}, {})@{} refused: row is already committed (Valid)",
                                incarnation,
                                key,
                                cur.generation
                            );
                        }
                        other => {
                            return err_box!(
                                "cache abort for ({}, {})@{} refused: row state {:?}",
                                incarnation,
                                key,
                                cur.generation,
                                other
                            );
                        }
                    }
                }
                Some(cur) if cur.generation > new_generation => {
                    // Someone else fenced far past this load: it is dead,
                    // nothing of ours to release.
                    drop(store);
                    self.lock_volatile().plans.remove(&load_token);
                    return Ok(CacheOpStatus::Superseded {
                        expected: new_generation,
                        current: cur.generation,
                    });
                }
                Some(cur) => {
                    // Between our fence generations: another mutation took
                    // new_generation, or the row is behind the load's
                    // allocation — both are divergence, not silent pass.
                    return err_box!(
                        "cache abort generation divergence for ({}, {}): row {}@{:?} vs load allocation {}@{}",
                        incarnation,
                        key,
                        cur.object_id,
                        cur.generation,
                        expected_object_id,
                        expected_generation
                    );
                }
            }
        }

        // Dedicated durable abort entry (gpt56 `21bb7129`): the commit
        // token is the shared first-winner token of Commit/Abort, and the
        // apply CAS accepts ONLY an exact Reserved row — the apply layer
        // (not just this precheck) refuses to remove a committed row.
        let op_id = self.fs_dir.read().next_op_id();
        let entry = JournalEntry::CacheAbort(CacheAbortEntry {
            op_id,
            rpc_id,
            load_token,
            commit_token,
            incarnation,
            key: key.to_string(),
            expected_generation,
            new_generation,
            expected_object_id,
        });
        self.journal_writer
            .sync_propose_cache(entry)
            .map_err(fs_err)?;
        self.fire_barrier_hook();

        if !self.incarnation_active(incarnation)? {
            return Err(Self::fenced(incarnation));
        }

        // Readback from committed state, re-classified from the row and
        // the durable outcome.
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
                self.retire_object_state(incarnation, expected_object_id, None)?;
                self.lock_volatile().plans.remove(&load_token);
                Ok(CacheOpStatus::Applied)
            }
            Some(cur) if cur.generation > new_generation => {
                let current = cur.generation;
                drop(store);
                self.lock_volatile().plans.remove(&load_token);
                Ok(CacheOpStatus::Superseded {
                    expected: new_generation,
                    current,
                })
            }
            Some(cur)
                if cur.generation == expected_generation
                    && cur.state == CacheEntryState::Valid
                    && cur.object_id == expected_object_id =>
            {
                // The commit won the shared-token race between this
                // handler's prechecks and the barrier: the abort applied
                // as its deterministic first-winner-loser no-op and the
                // row is published. Loud at the readback (gpt56
                // `52db24f3` blocker 1) — the FSM was never poisoned.
                drop(store);
                err_box!(
                    "cache abort for load {:?} lost the first-winner race: its commit applied concurrently and the entry ({}, {})@{} is Valid",
                    load_token,
                    incarnation,
                    key,
                    expected_generation
                )
            }
            other => err_box!(
                "cache abort barrier readback failed for ({}, {}): {:?}",
                incarnation,
                key,
                other
            ),
        }
    }

    // ---- 4c.2 leader-side bounded mutation drivers. Each driver pages the
    // authoritative store with a 4c.1 bounded scan (exclusive cursor +
    // validated limit), journals ONLY the exact victim identities of one
    // page per journal entry, and returns `{done, cursor, processed}`:
    // `cursor` is the last SCANNED identity (a stale/no-op page still
    // advances it), and `done` is judged by the RAW scan page touching the
    // scan boundary — never by the number of applied mutations. A caller
    // re-invokes with the returned cursor until `done`; per-call work is
    // bounded by `max_pages`. ----

    /// Shared external-cursor byte gate (review `cbd434bd`): every
    /// driver that resumes from a caller-supplied cache-key cursor applies
    /// the same `MAX_KEY_BYTES` bound as keys and scopes themselves, so no
    /// unbounded string reaches a Rocks seek or the echoed progress.
    fn validate_key_cursor(after: Option<&str>) -> CommonResult<()> {
        if let Some(a) = after {
            if a.len() > MAX_KEY_BYTES {
                return err_box!(
                    "cache mutation cursor exceeds {} bytes: {}",
                    MAX_KEY_BYTES,
                    a.len()
                );
            }
        }
        Ok(())
    }

    /// Post-propose dead-identity classification (4c.3, review
    /// `6bc4f569` gate 2): a successful propose does NOT mean every
    /// journaled victim's CAS applied — classify each victim from the
    /// COMMITTED row after the barrier. Dead = the row is missing
    /// (vacuumed), already belongs to a different object, the whole
    /// incarnation is inactive (revoked), or the same object sits
    /// Tombstoned. A same-object Reserved/Valid row in a LIVE incarnation
    /// means the victim's fence did not apply — the object is alive and
    /// is NEVER handed to physical GC. Returns the victim object's
    /// geometry when the committed row still carries it (a tombstone
    /// zeroes `len`; callers fall back to geometry frozen earlier from a
    /// full row or to the commit-published one).
    fn classify_dead_victim(
        &self,
        incarnation: u64,
        key: &str,
        object_id: i64,
    ) -> CommonResult<(bool, Option<(i64, i64)>)> {
        let active = self.incarnation_active(incarnation)?;
        let row = {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            // Lock order (4c.3 invariant): the fs_dir guard is dropped
            // before the caller takes the volatile lock in retire.
            rocks.cache_get_entry(incarnation, key).map_err(fs_err)?
        };
        let geometry = |cur: &CacheEntry| (cur.len > 0).then_some((cur.len, cur.block_size));
        Ok(match row {
            None => (true, None),
            Some(cur) if cur.object_id != object_id => (true, None),
            Some(cur) if !active => (true, geometry(&cur)),
            Some(cur) if cur.state == CacheEntryState::Tombstoned => (true, geometry(&cur)),
            // Same-object Reserved/Valid in a live incarnation: alive.
            Some(_) => (false, None),
        })
    }

    /// Shared post-propose physical handoff of the sweep/scope/vacuum
    /// drivers (4c.3, review `6bc4f569` gate 2): classify each journaled
    /// victim from the COMMITTED row and retire only PROVEN-dead
    /// identities to the GC queue. A successful propose does not mean
    /// every victim's CAS applied — a live same-object row is never
    /// handed to physical GC. Geometry falls back to the driver-frozen
    /// pre-propose geometry when the committed row no longer carries it
    /// (a tombstone zeroes `len`). Extracted so unit tests drive the
    /// exact production loop with an apply stand-in (the raft barrier
    /// itself fails closed in testing mode).
    fn retire_dead_victims(
        &self,
        victims: &[(u64, String, i64)],
        frozen: &HashMap<(String, i64), (i64, i64)>,
    ) -> CommonResult<()> {
        for (incarnation, key, object_id) in victims {
            let (dead, row_geometry) = self.classify_dead_victim(*incarnation, key, *object_id)?;
            if dead {
                self.retire_object_state(
                    *incarnation,
                    *object_id,
                    row_geometry.or(frozen.get(&(key.clone(), *object_id)).copied()),
                )?;
            }
        }
        Ok(())
    }

    /// Derive the journaled victims of one raw scope-scan page. Only rows
    /// that are NOT already Tombstoned produce victims (review
    /// `303fb807` P0-1): a committed scope-remove leaves `Tombstoned@g+1`
    /// primary rows in place, so re-deriving from an unchanged cursor
    /// (leader proposed, response lost, caller retried the same `after`)
    /// must yield ZERO victims for those rows — the retry journals
    /// nothing and never inflates the generation again.
    fn scope_page_victims(page: &[(String, CacheEntry)]) -> CommonResult<Vec<ScopeRemoveVictim>> {
        page.iter()
            .filter(|(_, e)| e.state != CacheEntryState::Tombstoned)
            .map(|(k, e)| {
                Ok(ScopeRemoveVictim {
                    key: k.clone(),
                    expected_generation: e.generation,
                    new_generation: e
                        .generation
                        .checked_add(1)
                        .ok_or_else(|| cm_err("cache scope remove generation overflow"))?,
                    object_id: e.object_id,
                    expire_at: e.expire_at,
                })
            })
            .collect()
    }

    /// Prefix-scope remove (4c.2): page the scope with the 4c.1 scoped
    /// scan, journal one bounded `CacheScopeRemove` batch per page. The
    /// committed apply runs the per-victim exact CAS; this driver only
    /// freezes identities — it never mutates the store itself.
    pub fn remove_scope(
        &self,
        rpc_id: i64,
        incarnation: u64,
        scope: &str,
        after: Option<&str>,
        max_pages: usize,
    ) -> CommonResult<ScopeRemoveProgress> {
        self.require_enabled()?;
        self.require_leader()?;
        if scope.is_empty() {
            return err_box!("cache scope remove scope must be a non-empty prefix path");
        }
        if scope.len() > MAX_KEY_BYTES {
            return err_box!(
                "cache scope remove scope exceeds {} bytes: {}",
                MAX_KEY_BYTES,
                scope.len()
            );
        }
        let max_pages = max_pages.clamp(1, MUTATION_MAX_PAGES_PER_CALL);
        Self::validate_key_cursor(after)?;
        if let Some(a) = after {
            if !crate::master::meta::cache::key_in_scope(a, scope) {
                return err_box!("cache scope remove cursor {} is outside scope {}", a, scope);
            }
        }

        let mut cursor = after.map(|a| a.to_string());
        let mut processed = 0usize;
        for _ in 0..max_pages {
            let page = {
                let store = self.fs_dir.read();
                let rocks = store.get_rocks_store();
                rocks.cache_scan_entries_in_scope(
                    incarnation,
                    scope,
                    cursor.as_deref(),
                    MUTATION_PAGE_CAP,
                )?
            };
            if page.is_empty() {
                return Ok(ScopeRemoveProgress {
                    done: true,
                    cursor,
                    processed,
                });
            }
            // Raw-page cursor + done (a stale page still advances); only
            // live rows are journaled — an all-tombstone page proposes
            // nothing and the loop continues (response-loss re-derive
            // stability, review `303fb807` P0-1).
            let victims = Self::scope_page_victims(&page)?;
            let last_key = page.last().unwrap().0.clone();
            let journaled = victims.len();
            if !victims.is_empty() {
                // 4c.3 (review `6bc4f569` gate 5): freeze each victim's
                // geometry from the raw page row — the applied tombstone
                // will zero `len`, and this row (object ids are immutable
                // and never reused) carries the victim object's exact
                // geometry.
                let frozen: HashMap<(String, i64), (i64, i64)> = page
                    .iter()
                    .filter(|(_, e)| e.state != CacheEntryState::Tombstoned && e.len > 0)
                    .map(|(k, e)| ((k.clone(), e.object_id), (e.len, e.block_size)))
                    .collect();
                let victim_ids: Vec<(u64, String, i64)> = victims
                    .iter()
                    .map(|v| (incarnation, v.key.clone(), v.object_id))
                    .collect();
                let op_id = self.fs_dir.read().next_op_id();
                let entry = JournalEntry::CacheScopeRemove(CacheScopeRemoveEntry {
                    op_id,
                    rpc_id,
                    incarnation,
                    scope: scope.to_string(),
                    victims,
                });
                self.journal_writer
                    .sync_propose_cache(entry)
                    .map_err(fs_err)?;
                // 4c.3 physical handoff (review `6bc4f569` gate 2):
                // classify each journaled victim from the committed row
                // — only a PROVEN-dead identity is retired to GC.
                self.retire_dead_victims(&victim_ids, &frozen)?;
            }
            // Counts journaled victims only: an all-tombstone (already
            // applied) page journals nothing.
            processed += journaled;
            cursor = Some(last_key);
            if page.len() < MUTATION_PAGE_CAP {
                return Ok(ScopeRemoveProgress {
                    done: true,
                    cursor,
                    processed,
                });
            }
        }
        Ok(ScopeRemoveProgress {
            done: false,
            cursor,
            processed,
        })
    }

    /// TTL sweep (4c.2): page the due expiry rows with the 4c.1 ordered
    /// scan and journal one bounded `CacheTtlSweep` batch per page.
    pub fn sweep_ttl(
        &self,
        rpc_id: i64,
        now: i64,
        after: Option<&ExpiryCursor>,
        max_pages: usize,
    ) -> CommonResult<TtlSweepProgress> {
        self.require_enabled()?;
        self.require_leader()?;
        let max_pages = max_pages.clamp(1, MUTATION_MAX_PAGES_PER_CALL);

        let mut cursor: Option<ExpiryCursor> = after.cloned();
        let mut processed = 0usize;
        for _ in 0..max_pages {
            let page: Vec<ExpiryRow> = {
                let store = self.fs_dir.read();
                let rocks = store.get_rocks_store();
                rocks.cache_scan_expiry(now, cursor.as_ref(), MUTATION_PAGE_CAP)?
            };
            if page.is_empty() {
                return Ok(TtlSweepProgress {
                    done: true,
                    cursor,
                    processed,
                });
            }
            let next_cursor = ExpiryCursor::from(page.last().unwrap());
            // 4c.3 (review `6bc4f569` gate 5): expiry rows carry no
            // geometry, so freeze each victim's geometry with a bounded
            // exact entry lookup BEFORE the propose — the applied
            // tombstone zeroes `len`. A row that already belongs to a
            // different object (or is missing) yields no geometry and the
            // retire falls back to the commit-published one.
            let frozen: HashMap<(String, i64), (i64, i64)> = {
                let store = self.fs_dir.read();
                let rocks = store.get_rocks_store();
                let mut frozen = HashMap::new();
                for v in &page {
                    if let Some(cur) = rocks
                        .cache_get_entry(v.incarnation, &v.key)
                        .map_err(fs_err)?
                    {
                        if cur.object_id == v.object_id && cur.len > 0 {
                            frozen.insert((v.key.clone(), v.object_id), (cur.len, cur.block_size));
                        }
                    }
                }
                frozen
            };
            let op_id = self.fs_dir.read().next_op_id();
            let entry = JournalEntry::CacheTtlSweep(CacheTtlSweepEntry {
                op_id,
                rpc_id,
                now,
                victims: page.clone(),
            });
            self.journal_writer
                .sync_propose_cache(entry)
                .map_err(fs_err)?;
            // 4c.3 physical handoff (review `6bc4f569` gate 2).
            let victim_ids: Vec<(u64, String, i64)> = page
                .iter()
                .map(|v| (v.incarnation, v.key.clone(), v.object_id))
                .collect();
            self.retire_dead_victims(&victim_ids, &frozen)?;
            processed += page.len();
            cursor = Some(next_cursor);
            if page.len() < MUTATION_PAGE_CAP {
                return Ok(TtlSweepProgress {
                    done: true,
                    cursor,
                    processed,
                });
            }
        }
        Ok(TtlSweepProgress {
            done: false,
            cursor,
            processed,
        })
    }

    /// Revoked-incarnation vacuum (4c.2): leader-verifies the gate-3
    /// preconditions (row exists, belongs to the mount, revoked, not
    /// current), then pages the incarnation with the 4c.1 entry scan and
    /// journals one bounded `CacheVacuum` batch per page. The apply
    /// re-verifies everything; vacuum never touches pointers, watermarks,
    /// outcomes, or the incarnation/policy rows.
    pub fn vacuum_incarnation(
        &self,
        rpc_id: i64,
        mount_id: u32,
        incarnation: u64,
        after: Option<&str>,
        max_pages: usize,
    ) -> CommonResult<VacuumProgress> {
        self.require_enabled()?;
        self.require_leader()?;
        let max_pages = max_pages.clamp(1, MUTATION_MAX_PAGES_PER_CALL);
        Self::validate_key_cursor(after)?;
        {
            let store = self.fs_dir.read();
            let rocks = store.get_rocks_store();
            match rocks.cache_get_incarnation(incarnation).map_err(fs_err)? {
                Some(row) if row.revoked && row.mount_id == mount_id => {
                    if rocks.cache_current_incarnation(mount_id).map_err(fs_err)?
                        == Some(incarnation)
                    {
                        return err_box!(
                            "vacuum incarnation {} is still mount {}'s current incarnation",
                            incarnation,
                            mount_id
                        );
                    }
                }
                Some(row) => {
                    return err_box!(
                        "vacuum incarnation {} is not vacuumable (mount {}, revoked {})",
                        incarnation,
                        row.mount_id,
                        row.revoked
                    )
                }
                None => {
                    return err_box!("vacuum incarnation {} has no incarnation row", incarnation)
                }
            }
        }

        let mut cursor = after.map(|a| a.to_string());
        let mut processed = 0usize;
        for _ in 0..max_pages {
            let page = {
                let store = self.fs_dir.read();
                let rocks = store.get_rocks_store();
                rocks.cache_scan_entries(incarnation, cursor.as_deref(), MUTATION_PAGE_CAP)?
            };
            if page.is_empty() {
                return Ok(VacuumProgress {
                    done: true,
                    cursor,
                    processed,
                });
            }
            // 4c.3 (review `6bc4f569` gate 5): vacuum pages raw entry
            // rows, so each victim's geometry freezes directly from the
            // page (immutable per object).
            let frozen: HashMap<(String, i64), (i64, i64)> = page
                .iter()
                .filter(|(_, e)| e.len > 0)
                .map(|(k, e)| ((k.clone(), e.object_id), (e.len, e.block_size)))
                .collect();
            let victims: Vec<VacuumVictim> = page
                .iter()
                .map(|(k, e)| VacuumVictim {
                    key: k.clone(),
                    generation: e.generation,
                    object_id: e.object_id,
                    expire_at: e.expire_at,
                })
                .collect();
            let last_key = page.last().unwrap().0.clone();
            let op_id = self.fs_dir.read().next_op_id();
            let entry = JournalEntry::CacheVacuum(CacheVacuumEntry {
                op_id,
                rpc_id,
                incarnation,
                mount_id,
                victims,
            });
            self.journal_writer
                .sync_propose_cache(entry)
                .map_err(fs_err)?;
            // 4c.3 physical handoff (review `6bc4f569` gate 2): the
            // incarnation is revoked, so classify will prove every
            // remaining row of it dead; geometry comes from the frozen
            // page row.
            let victim_ids: Vec<(u64, String, i64)> = page
                .iter()
                .map(|(k, e)| (incarnation, k.clone(), e.object_id))
                .collect();
            self.retire_dead_victims(&victim_ids, &frozen)?;
            processed += page.len();
            cursor = Some(last_key);
            if page.len() < MUTATION_PAGE_CAP {
                return Ok(VacuumProgress {
                    done: true,
                    cursor,
                    processed,
                });
            }
        }
        Ok(VacuumProgress {
            done: false,
            cursor,
            processed,
        })
    }

    /// One physical-GC handoff tick (4c.3, review `6bc4f569` gate 3 /
    /// `327b30d2` item 1): extract a bounded round-robin batch of
    /// `(worker_id, block_id)` pairs under the volatile lock, RELEASE
    /// the lock, then enqueue each pair into the worker delete queue
    /// (`WorkerManager::remove_block` — `BlockMap`'s HashSet makes
    /// duplicate enqueues idempotent). The workers receive the deletes
    /// on their next heartbeats and ack via block reports, exactly like
    /// fs-mode deletes. Leader-gated: a follower's tick is a no-op (its
    /// volatile queue is empty anyway — it is never fed off the raft
    /// apply path). Physical-side failures are logged and never
    /// propagated: a failed tick must not fail the worker heartbeat,
    /// and committed metadata is never rolled back (the drain cursor
    /// stays advanced; the 4d full report re-derives anything the queue
    /// lost).
    pub fn gc_handoff_tick(&self, workers: &ArcRwLock<WorkerManager>) {
        if !self.monitor.is_active() {
            return;
        }
        let batch = match self.state.lock() {
            Ok(mut volatile) => {
                volatile.sync_epoch(self.monitor.journal_epoch());
                // 4d.2: same tick also drains a bounded slice of the
                // retired-session reverse entries (metadata-only row
                // removal; no physical delete enqueued).
                volatile.drain_retired();
                volatile.gc_take_batch()
            }
            Err(_) => return,
        };
        let batch = match batch {
            Ok(batch) => batch,
            Err(e) => {
                log::warn!("cache gc handoff tick skipped a corrupt work item: {}", e);
                return;
            }
        };
        if batch.is_empty() {
            return;
        }
        let mut wm = workers.write();
        for (worker_id, block_id) in batch {
            wm.remove_block(worker_id, block_id);
        }
    }

    /// 4d R5/R7-2 epoch fence helper: acquire the volatile guard bound
    /// to the CURRENT journal epoch, cold-clearing every volatile map on
    /// a leadership mismatch (see `CacheVolatile::sync_epoch`). Every
    /// production entry point into the volatile domain goes through
    /// this — a regained leader never serves stale-warm locations,
    /// plans, session registry, or GC queue. Test seams may keep the
    /// raw lock to control the epoch deterministically.
    fn lock_volatile(&self) -> std::sync::MutexGuard<'_, CacheVolatile> {
        let mut volatile = self.state.lock().unwrap();
        volatile.sync_epoch(self.monitor.journal_epoch());
        volatile
    }

    /// 4d (R4/R9-3): accept a worker Start in the cache domain —
    /// install the fresh session (retiring any previous one) under the
    /// volatile guard, epoch-fenced. The accumulator reset (R7-5) is
    /// driven by MasterFilesystem BEFORE this under the accumulator
    /// guard (declared order: accumulator → volatile).
    pub fn install_worker_session(
        &self,
        worker_id: u32,
        session: &str,
        address: &WorkerAddress,
    ) -> CommonResult<()> {
        let mut volatile = self.lock_volatile();
        volatile.install_session(worker_id, session.to_string(), address.clone())?;
        Ok(())
    }

    /// 4d (R9-2 + final-review `f14fa328`): retire the worker's session
    /// EXACTLY (End heartbeat or lost-worker callback), under the
    /// declared `accumulator → volatile` lock order with BOTH guards
    /// held. Only when the registry's CURRENT wire session equals the
    /// retiring session: retire the registry row and live set, bump the
    /// reconcile generation, AND terminally invalidate the SAME-session
    /// accumulator (per `0b900a2f`, its late full pages stay Skipped —
    /// an ended session must never accumulate to Complete). A stale
    /// End/lost callback (registry holds a different session, or none)
    /// has ZERO side effects on both domains.
    pub fn retire_worker_session(&self, worker_id: u32, session: &str) -> bool {
        if session.is_empty() {
            return false;
        }
        let mut sessions = self.report_sessions.lock().unwrap();
        let mut volatile = self.lock_volatile();
        if !volatile.retire_session(worker_id, session) {
            return false;
        }
        // Exact registry hit: terminalize the matching accumulator row.
        // The guard on the row's own session is defensive parity — the
        // accumulator is only ever installed bound to the registry's
        // current session, so this is the same session by construction;
        // if they ever diverge, only the exact match dies.
        if let Some(sess) = sessions.get_mut(&worker_id) {
            if sess.session == session {
                sess.invalid = true;
                sess.entries.clear();
                sess.update_time_ms = LocalTime::mills();
            }
        }
        true
    }

    /// 4d (R9-3): the Start acceptance for the cache domain — holds the
    /// accumulator guard AND the volatile guard SIMULTANEOUSLY (declared
    /// order: accumulator → volatile) while it atomically replaces the
    /// session registry row (retiring the previous live set, issuing a
    /// fresh never-reused tag, bumping the reconcile generation) and
    /// installs a FRESH accumulator bound to the new session. This is
    /// the ONLY accumulator creation point (`0b900a2f`: a
    /// terminally-invalidated session never gets a new accumulator; a
    /// new Start always does). The epoch fence runs inside
    /// `lock_volatile` first.
    pub fn begin_cache_session(
        &self,
        worker_id: u32,
        session: &str,
        address: &WorkerAddress,
    ) -> CommonResult<()> {
        let mut sessions = self.report_sessions.lock().unwrap();
        let mut volatile = self.lock_volatile();
        // RC5 + RC2-2 (gpt56 `aa41c780` item 2): a refusal — e.g. tag
        // issuer exhaustion — fails ATOMICALLY. install_session's own
        // ordering already retired the previous registry row and live
        // set BEFORE the refusal, so the OLD accumulator must not
        // survive it either: terminally invalidate it here (per
        // `0b900a2f`, later pages of the old session stay Skipped and
        // only a new successful Start creates a fresh accumulator), and
        // propagate the error loud to the heartbeat RPC.
        let tag = match volatile.install_session(worker_id, session.to_string(), address.clone()) {
            Ok(tag) => tag,
            Err(e) => {
                if let Some(old) = sessions.get_mut(&worker_id) {
                    old.invalid = true;
                    old.entries.clear();
                    old.update_time_ms = LocalTime::mills();
                }
                return Err(e);
            }
        };
        sessions.insert(
            worker_id,
            CacheReportSession {
                session: session.to_string(),
                total_len: 0,
                entries: HashMap::new(),
                invalid: false,
                phase: ReportPhase::Accumulating,
                attempt: 0,
                // RC2 P0-1: the row is bound to the Start identity —
                // checkout tickets are exact on (session, tag, attempt),
                // so a same-session Start retry (fresh tag, fresh row)
                // supersedes every outstanding old ticket.
                tag,
                update_time_ms: LocalTime::mills(),
            },
        );
        Ok(())
    }

    /// #[cfg(test)] 4d.2 P0-1/P0-2 handler-test support: observable
    /// exact-identity quarantine probe (and whole-row presence), so
    /// fence tests assert without reaching into the private volatile
    /// lock. Compiled out outside cfg(test).
    #[cfg(test)]
    pub(crate) fn quarantine_contains(
        &self,
        object_id: i64,
        worker_id: u32,
        session_tag: u64,
        seq: i64,
    ) -> bool {
        let volatile = self.state.lock().unwrap();
        volatile
            .quarantine
            .get(&object_id)
            .and_then(|row| row.get(&(worker_id, session_tag)))
            .is_some_and(|s| s.contains(&seq))
    }

    /// #[cfg(test)] 4d.2 P0-1 handler-test support: observable live-set
    /// probe for one identity. Compiled out outside cfg(test).
    #[cfg(test)]
    pub(crate) fn live_contains(&self, worker_id: u32, object_id: i64, seq: i64) -> bool {
        let volatile = self.state.lock().unwrap();
        volatile
            .by_worker
            .get(&worker_id)
            .is_some_and(|rev| rev.live_contains(object_id, seq))
    }

    /// #[cfg(test)] 4d RC2 handler-test support: an observable snapshot
    /// of one worker's session spine — registry row (wire session, live
    /// tag), tag-issuer cursor, and accumulator
    /// row (session, terminal flag) — so handler-level tests assert the
    /// exact install/retire/refuse effects without reaching into the
    /// private leaf locks. Locks taken in the declared order
    /// (accumulator → volatile). Compiled out outside cfg(test).
    #[cfg(test)]
    pub(crate) fn session_spine_snapshot(&self, worker_id: u32) -> SessionSpine {
        let accumulator = self
            .report_sessions
            .lock()
            .unwrap()
            .get(&worker_id)
            .map(|s| (s.session.clone(), s.invalid));
        let volatile = self.state.lock().unwrap();
        let registry = volatile
            .worker_sessions
            .get(&worker_id)
            .map(|s| (s.session.clone(), s.tag));
        SessionSpine {
            registry,
            next_tag: volatile.next_tag,
            accumulator,
        }
    }

    /// #[cfg(test)] 4d RC2 handler-test support: burn the tag issuer to
    /// its exhaustion point so a handler-level Start refusal is
    /// deterministic. Compiled out outside cfg(test).
    #[cfg(test)]
    pub(crate) fn set_next_tag_for_test(&self, tag: u64) {
        self.state.lock().unwrap().next_tag = tag;
    }

    /// 4d RC3 (gpt56 `7ceef2ff` item 3): a Start with an EMPTY wire
    /// session id (a legacy worker) fail-closes the cache domain for
    /// that worker: whatever accumulator exists is terminally
    /// invalidated, and whatever session is CURRENT (whatever its wire
    /// id — an empty Start carries no exact-match key) is retired with
    /// its live set moved to the retired drain and the reconcile
    /// generation bumped. NOTHING is installed in its place: until a
    /// non-empty Start reopens, the worker can neither report into the
    /// cache accumulator nor satisfy any plan fence, so no cache
    /// location can be planned or published for it. In-flight plans
    /// referencing the retired tag breach loudly at commit.
    pub fn purge_worker_cache_session(&self, worker_id: u32) {
        let mut sessions = self.report_sessions.lock().unwrap();
        let mut volatile = self.lock_volatile();
        if let Some(sess) = sessions.get_mut(&worker_id) {
            sess.invalid = true;
            sess.entries.clear();
            sess.update_time_ms = LocalTime::mills();
        }
        if let Some(prev) = volatile.worker_sessions.remove(&worker_id) {
            volatile.retire_live(worker_id, prev.tag);
            *volatile.reconcile_gens.entry(worker_id).or_insert(0) += 1;
        }
    }

    /// 4d (R8-4/R9-4): terminally invalidate the worker's cache
    /// accumulator (an incremental F/W/Deleted landed for the worker).
    /// Per `0b900a2f` the session is NOT removed: later full pages of
    /// the same session stay cache-skipped; only a new Start reopens.
    pub fn invalidate_report_session(&self, worker_id: u32) {
        let mut sessions = self.report_sessions.lock().unwrap();
        if let Some(sess) = sessions.get_mut(&worker_id) {
            sess.invalid = true;
            sess.entries.clear();
            sess.update_time_ms = LocalTime::mills();
        }
    }

    /// 4d.2: route one INCREMENTAL report's cache-domain block items
    /// (the caller diverted them before any FS classification).
    ///
    /// Lock discipline: accumulator guard FIRST — a same-session
    /// incremental terminally invalidates the worker's full-report
    /// accumulation (`0b900a2f`: the session row is kept so its late
    /// pages stay Skipped; only a new Start reopens); then the volatile
    /// guard covers classify + apply atomically, with the row reads in
    /// ONE fs_dir read guard inside it (volatile → fs_dir read).
    ///
    /// Session gate (R4/R9-2): an empty (legacy) session, or one that
    /// does not exactly match the registry's current row, has ZERO side
    /// effects on every domain — a stale incremental from an old
    /// process must never strip a newer session's rows or touch its
    /// accumulator. Reclamation for stale reports is the 4d.3
    /// full-report reconcile's job.
    ///
    /// R1 classification (frozen matrix) per Finalized/Writing item:
    /// no object row / missing entry / stale generation mapping /
    /// non-current-or-revoked incarnation / Tombstoned → proven orphan
    /// → delete; Reserved × any → defer; Valid × Writing → defer;
    /// Valid × Finalized → R3 exact-length check, mismatch = corrupt
    /// orphan → delete, pass → publish the replica under the registry's
    /// CURRENT tag/address (idempotent). Deleted items remove the
    /// worker's volatile replica row and return a BlockMap ack — they
    /// never touch the inode chain (Deleted 零渗透).
    pub fn incr_block_report(
        &self,
        worker_id: u32,
        session: &str,
        items: &[BlockReportInfo],
    ) -> CommonResult<CacheIncrOutcome> {
        if session.is_empty() || items.is_empty() || !self.enabled {
            return Ok(CacheIncrOutcome::default());
        }
        // Followers never feed the volatile domain (leader-gated no-op).
        if !self.monitor.is_active() {
            return Ok(CacheIncrOutcome::default());
        }
        // Accumulator guard FIRST (R9-4): only a SAME-session
        // incremental invalidates — the stale-session case below must
        // not disturb the current session's accumulation.
        // RC2 P0-1 (gpt56 `53516250` window 1): the guard is held
        // ACROSS the volatile section below (declared order
        // accumulator → volatile), so terminalization and the
        // generation bump are ONE atomic fence — a concurrent
        // full-report reconcile phase-B (which holds the same row
        // guard across ITS volatile mutation) can only serialize
        // before or after this whole critical section, never in the
        // "invalid written, generation not yet bumped" middle.
        let mut sessions = self.report_sessions.lock().unwrap();
        if let Some(sess) = sessions.get_mut(&worker_id) {
            if sess.session == session && !sess.invalid {
                sess.invalid = true;
                sess.entries.clear();
                sess.update_time_ms = LocalTime::mills();
            }
        }
        // #[cfg(test)] deterministic seam: the pause point BETWEEN the
        // terminalization and the volatile acquisition (the production
        // window this whole guard-hold closes). The accumulator guard
        // is held here — a hook must not take `report_sessions`.
        #[cfg(test)]
        if let Some(hook) = INCR_TERMINALIZE_SEAM.lock().unwrap().as_ref() {
            hook();
        }
        let mut volatile = self.lock_volatile();
        // Session gate: exact current-session match or a total no-op.
        let (reg_tag, reg_address) = match volatile.worker_sessions.get(&worker_id) {
            Some(s) if s.session == session => (s.tag, s.address.clone()),
            _ => return Ok(CacheIncrOutcome::default()),
        };
        let gen = {
            let entry = volatile.reconcile_gens.entry(worker_id).or_insert(0);
            *entry += 1;
            *entry
        };

        // Per-item routing decision, classified against ONE fs_dir read
        // guard (geometry snapshot carried into the apply phase).
        let decisions = self.classify_cache_report(items);
        // Apply under the same volatile guard (R7-4: classification and
        // location mutation share one serialized domain — no TOCTOU
        // window for a concurrent Start/publish).
        // P0-1 (gpt56 `25d4b51e` item 1): the decision is bound to THIS
        // registry tag; the handler's WorkerManager side effects recheck
        // it under the transition gate before applying, so an outcome
        // computed before a Start/lost swap can never act on the new
        // session.
        // RC1 P0-2 (gpt56 `d2546338` item 2): the outcome also carries
        // the APPLIED generation; the fenced WM apply rechecks tag AND
        // gen, so a newer same-session report supersedes this outcome
        // wholesale (no re-ordering of its WM/ack effects).
        let (remove_blocks, deleted_acks) =
            volatile.apply_cache_decisions(worker_id, reg_tag, &reg_address, decisions);
        Ok(CacheIncrOutcome {
            session_tag: reg_tag,
            gen,
            remove_blocks,
            deleted_acks,
        })
    }

    /// 4d.2/4d.3: classify reported cache blocks against the
    /// authoritative store under ONE `fs_dir` read guard. Shared by the
    /// incremental path (called with the volatile guard already held,
    /// R7-4) and the 4d.3 full-report reconcile (called outside the
    /// fence, whose final recheck covers the window). Per-item master
    /// read/derive failures defer (never delete worker data); the RC2
    /// page fold keeps one terminal decision per id.
    fn classify_cache_report(&self, items: &[BlockReportInfo]) -> HashMap<i64, CacheReportDec> {
        let mut decisions: HashMap<i64, CacheReportDec> = HashMap::with_capacity(items.len());
        let store = self.fs_dir.read();
        let rocks = store.get_rocks_store();
        for item in items {
            let object_id = match BlockIdCodec::block_owner(item.id) {
                Ok(v) if BlockIdCodec::is_cache_owner(v) => v,
                _ => {
                    log::warn!(
                        "cache report: illegal block id {} skipped to orphan",
                        item.id
                    );
                    let decision = CacheReportDec::Orphan(-1, -1);
                    if cache_dec_rank(&decision)
                        > decisions.get(&item.id).map(cache_dec_rank).unwrap_or(0)
                    {
                        decisions.insert(item.id, decision);
                    }
                    continue;
                }
            };
            let seq = BlockIdCodec::get_seq(item.id);
            if item.status == BlockReportStatus::Deleted {
                let decision = CacheReportDec::Deleted(object_id, seq);
                if cache_dec_rank(&decision)
                    > decisions.get(&item.id).map(cache_dec_rank).unwrap_or(0)
                {
                    decisions.insert(item.id, decision);
                }
                continue;
            }
            let mut decision = CacheReportDec::Defer;
            let classified =
                (|| -> CommonResult<()> {
                    let Some(row) = rocks.cache_get_object(object_id).map_err(fs_err)? else {
                        decision = CacheReportDec::Orphan(object_id, seq);
                        return Ok(());
                    };
                    let active = match rocks
                        .cache_get_incarnation(row.incarnation)
                        .map_err(fs_err)?
                    {
                        Some(r) if !r.revoked => {
                            rocks
                                .cache_current_incarnation(r.mount_id)
                                .map_err(fs_err)?
                                == Some(row.incarnation)
                        }
                        _ => false,
                    };
                    if !active {
                        decision = CacheReportDec::Orphan(object_id, seq);
                        return Ok(());
                    }
                    let Some(entry) = rocks
                        .cache_get_entry(row.incarnation, &row.key)
                        .map_err(fs_err)?
                    else {
                        decision = CacheReportDec::Orphan(object_id, seq);
                        return Ok(());
                    };
                    // Stale mapping guard (contract: the entry row is the
                    // authority; a divergent generation is a stale row).
                    if entry.generation != row.generation {
                        decision = CacheReportDec::Orphan(object_id, seq);
                        return Ok(());
                    }
                    // Object identity guard (RC1): the entry row must point
                    // back at exactly the reported object. A same-key,
                    // same-generation entry bound to a DIFFERENT object is
                    // a divergent mapping — the reported block is an
                    // orphan, never a publish.
                    if entry.object_id != object_id {
                        decision = CacheReportDec::Orphan(object_id, seq);
                        return Ok(());
                    }
                    match entry.state {
                        CacheEntryState::Tombstoned => {
                            decision = CacheReportDec::Orphan(object_id, seq)
                        }
                        CacheEntryState::Reserved => {}
                        CacheEntryState::Valid => {
                            if item.status == BlockReportStatus::Writing {
                                return Ok(());
                            }
                            // R3: exact block length. The reported length
                            // must equal the layout-derived length of
                            // THIS seq (block_size, or last_len on the
                            // final block); a truncated/oversize
                            // Finalized block is a corrupt orphan.
                            let layout =
                                CacheBlockLayout::derive(object_id, entry.len, entry.block_size)?;
                            if seq < 1 || seq > layout.block_count {
                                decision = CacheReportDec::Orphan(object_id, seq);
                            } else {
                                let expected = if seq == layout.block_count {
                                    layout.last_len
                                } else {
                                    layout.block_size
                                };
                                if item.block_size != expected {
                                    log::warn!(
                                    "cache report: corrupt block {} reported len {} != expected {}",
                                    item.id, item.block_size, expected
                                );
                                    decision = CacheReportDec::Orphan(object_id, seq);
                                } else {
                                    decision = CacheReportDec::Publish(
                                        object_id,
                                        seq,
                                        entry.len,
                                        entry.block_size,
                                    );
                                }
                            }
                        }
                    }
                    Ok(())
                })();
            if let Err(e) = classified {
                // A master-side read/derive failure never deletes
                // worker data: defer the item for the next report.
                log::warn!("cache report classify deferred block {}: {}", item.id, e);
            }
            if cache_dec_rank(&decision) > decisions.get(&item.id).map(cache_dec_rank).unwrap_or(0)
            {
                decisions.insert(item.id, decision);
            }
        }
        decisions
    }

    /// P0-1 (gpt56 `25d4b51e`): the worker's CURRENT registry tag, read
    /// under the volatile guard. The fenced outcome apply compares it
    /// with the tag captured at decision time.
    pub fn cache_session_tag(&self, worker_id: u32) -> Option<u64> {
        if !self.enabled || !self.monitor.is_active() {
            return None;
        }
        let volatile = self.lock_volatile();
        volatile.worker_sessions.get(&worker_id).map(|s| s.tag)
    }

    /// 4d.2 RC2: release the delete-pending quarantine for one
    /// EXACT `(worker, session_tag, object, seq)` once the worker's
    /// Deleted ack for the physical delete has been processed. P0-1:
    /// the tag is part of the identity — an ack computed under an old
    /// tag can never release a quarantine a newer session built. A
    /// later Finalized re-report of the identity may then publish
    /// again (fresh data legitimately re-written after the delete
    /// completed).
    pub fn ack_cache_deleted(&self, worker_id: u32, session_tag: u64, block_id: i64) {
        if !self.enabled || !self.monitor.is_active() {
            return;
        }
        let Ok(object_id) = BlockIdCodec::block_owner(block_id) else {
            return;
        };
        if !BlockIdCodec::is_cache_owner(object_id) {
            return;
        }
        let seq = BlockIdCodec::get_seq(block_id);
        let mut volatile = self.lock_volatile();
        release_quarantine_identity(&mut volatile, worker_id, session_tag, object_id, seq);
    }

    /// RC1 P0-3 (gpt56 `d2546338` item 3): the worker's CURRENT registry
    /// wire session, read under the volatile guard. `None` = no live
    /// registration (or cache disabled / follower) — full-report pages
    /// only ever feed an accumulator bound to this value.
    pub fn registry_session(&self, worker_id: u32) -> Option<String> {
        if !self.enabled || !self.monitor.is_active() {
            return None;
        }
        let volatile = self.lock_volatile();
        volatile
            .worker_sessions
            .get(&worker_id)
            .map(|s| s.session.clone())
    }

    /// RC1 P0-3 (gpt56 `2b83f05d` tightening) / RC2 P0-1 (gpt56
    /// `53516250` window 2): per-page authorization of a full-report
    /// page against the CURRENT registry wire session, taken BEFORE the
    /// page may create/switch/clear/count the FS trigger row; on
    /// success it returns the registry's CURRENT tag, which the FS
    /// trigger row binds so the eventual snapshot checkout is exact on
    /// the Start identity. `Some(current)` registry row: the page
    /// session must equal it exactly, then its tag is returned. No live
    /// registration: only a legacy EMPTY-session page passes (tag 0).
    /// Cache domain disabled / follower: vacuously authorized with tag
    /// 0 — the FS accumulator's behavior on cache-disabled clusters is
    /// exactly the pre-4d.3 one (no new fence there).
    pub fn authorize_full_report_page(&self, worker_id: u32, page_session: &str) -> Option<u64> {
        if !self.enabled || !self.monitor.is_active() {
            return Some(0);
        }
        let volatile = self.lock_volatile();
        match volatile.worker_sessions.get(&worker_id) {
            Some(s) if s.session == page_session => Some(s.tag),
            Some(_) => None,
            None if page_session.is_empty() => Some(0),
            None => None,
        }
    }

    /// RC1 P0-2 (gpt56 `d2546338` item 2): apply one report outcome's
    /// WorkerManager side effects under the VOLATILE guard held across
    /// the whole apply. The fence rechecks BOTH the registry tag AND the
    /// reconcile generation captured when the outcome's volatile
    /// mutations were applied: any newer same-session report (incremental
    /// page or full reconcile) bumped the generation in between, so this
    /// outcome is superseded — its WM effects are dropped instead of
    /// re-ordering after the newer report's (an old `remove_block` would
    /// clear a fresh delete queue entry; an old `deleted_block` ack would
    /// release a fresh quarantine). Holding the volatile guard across the
    /// WM mutation also linearizes the pair: an incremental report needs
    /// the volatile lock, and a Start/lost transition needs `start_gate`,
    /// so nothing same-session can interleave inside this window.
    /// Returns false when the fence dropped the outcome.
    pub fn apply_outcome_fenced(
        &self,
        worker_id: u32,
        outcome: &CacheIncrOutcome,
        mut wm_effect: impl FnMut(WmEffect),
    ) -> bool {
        if !self.enabled || !self.monitor.is_active() {
            return false;
        }
        let mut volatile = self.lock_volatile();
        let current_gen = volatile
            .reconcile_gens
            .get(&worker_id)
            .copied()
            .unwrap_or(0);
        let fenced = volatile
            .worker_sessions
            .get(&worker_id)
            .is_some_and(|s| s.tag == outcome.session_tag)
            && outcome.session_tag != 0
            && current_gen == outcome.gen;
        if !fenced {
            log::warn!(
                "cache report outcome for worker {} dropped: tag {}/gen {} no longer current ({})",
                worker_id,
                outcome.session_tag,
                outcome.gen,
                current_gen
            );
            return false;
        }
        for id in &outcome.remove_blocks {
            wm_effect(WmEffect::RemoveBlock(*id));
        }
        for id in &outcome.deleted_acks {
            let Ok(object_id) = BlockIdCodec::block_owner(*id) else {
                continue;
            };
            if !BlockIdCodec::is_cache_owner(object_id) {
                continue;
            }
            let seq = BlockIdCodec::get_seq(*id);
            wm_effect(WmEffect::DeletedAck(*id));
            release_quarantine_identity(
                &mut volatile,
                worker_id,
                outcome.session_tag,
                object_id,
                seq,
            );
        }
        true
    }

    /// 4d (R7-5): feed one full-report page into the worker's cache
    /// accumulator. See `CacheFullReportOutcome` for the result
    /// semantics; the terminal rules live on `CacheReportSession`.
    pub fn cache_full_report_page(
        &self,
        worker_id: u32,
        session: &str,
        total_len: u64,
        blocks: &[BlockReportInfo],
    ) -> CacheFullReportOutcome {
        let now = LocalTime::mills();
        let mut sessions = self.report_sessions.lock().unwrap();
        let Some(sess) = sessions.get_mut(&worker_id) else {
            return CacheFullReportOutcome::Skipped;
        };
        // Foreign session, already terminal, or an in-flight checkout
        // (RC1 P0-1): permanently skipped for this call.
        if sess.invalid || sess.session != session || sess.phase != ReportPhase::Accumulating {
            return CacheFullReportOutcome::Skipped;
        }
        sess.update_time_ms = now;
        // First page binds the declared total; every later page must
        // repeat it exactly, and it must sit under the configured cap.
        if sess.total_len == 0 {
            if total_len == 0 || total_len > self.report_total_cap {
                sess.invalid = true;
                return CacheFullReportOutcome::Skipped;
            }
            sess.total_len = total_len;
        } else if sess.total_len != total_len {
            sess.invalid = true;
            sess.entries.clear();
            return CacheFullReportOutcome::Skipped;
        }
        for block in blocks {
            match sess.entries.get(&block.id) {
                Some((status, len, storage))
                    if *status == block.status
                        && *len == block.block_size
                        && *storage == block.storage_type =>
                {
                    // Idempotent duplicate: same id, same triple.
                }
                Some(_) => {
                    // Conflicting duplicate: terminal.
                    sess.invalid = true;
                    sess.entries.clear();
                    return CacheFullReportOutcome::Skipped;
                }
                None => {
                    sess.entries.insert(
                        block.id,
                        (block.status, block.block_size, block.storage_type),
                    );
                }
            }
        }
        // Overflow (more unique ids than declared) is terminal.
        if sess.entries.len() as u64 > sess.total_len {
            sess.invalid = true;
            sess.entries.clear();
            return CacheFullReportOutcome::Skipped;
        }
        if sess.entries.len() as u64 == sess.total_len {
            // RC1 P0-1: checkout keeps the ROW (as Reconciling, under a
            // fresh attempt ticket) and hands the entry set out — an
            // incremental / End / lost landing mid-flight still finds
            // and terminalizes the row; only the exact-attempt release
            // returns it to Accumulating.
            // RC2 P0-1 (gpt56 `53516250` window 2): the ticket carries
            // the row's Start-identity TAG and is returned ATOMICALLY
            // with the entry set — never re-read afterwards, so a
            // same-session Start retry between the Complete and the
            // reconcile cannot rebind the flight onto the new tag/row.
            sess.phase = ReportPhase::Reconciling;
            sess.attempt += 1;
            let ticket = FullSnapshotTicket {
                tag: sess.tag,
                attempt: sess.attempt,
            };
            let entries = std::mem::take(&mut sess.entries)
                .into_iter()
                .map(|(id, (status, block_size, storage_type))| BlockReportInfo {
                    id,
                    status,
                    storage_type,
                    block_size,
                })
                .collect();
            return CacheFullReportOutcome::Complete(entries, ticket);
        }
        CacheFullReportOutcome::Partial
    }

    /// 4d.3: consume the worker's still-PARTIAL cache accumulator as
    /// the full-report snapshot, at the FS accumulator's end-of-report
    /// trigger. A MIXED worker's cache pages can never self-Complete
    /// (its declared total counts ALL ids, cache + FS), so the FS
    /// accumulator reaching its total is the single authoritative
    /// end-of-report signal. Only a non-terminal SAME-session row is
    /// consumable: a terminal row (`0b900a2f`) is left in place — the
    /// reconcile is skipped and the next full report's cache pages stay
    /// cache-skipped until a new Start; an absent row (no cache page in
    /// this report) yields None — there is no authoritative cache
    /// snapshot to exact-replace against.
    pub fn take_cache_full_snapshot(
        &self,
        worker_id: u32,
        session: &str,
        expected_tag: u64,
    ) -> Option<(Vec<BlockReportInfo>, FullSnapshotTicket)> {
        let mut sessions = self.report_sessions.lock().unwrap();
        let sess = sessions.get_mut(&worker_id)?;
        if sess.invalid
            || sess.session != session
            || sess.phase != ReportPhase::Accumulating
            // RC2 P0-1 (gpt56 `53516250` window 2): the checkout is
            // exact on the FS trigger's Start identity — a row a
            // same-session Start RETRY installed (fresh tag, fresh
            // attempt) is NEVER consumable by an older trigger.
            || sess.tag != expected_tag
        {
            return None;
        }
        // RC1 P0-1: the checkout transitions the ROW to Reconciling
        // (fresh attempt ticket) instead of removing it — mid-flight
        // terminalization still reaches the row, and only the
        // exact-attempt release can return it to Accumulating.
        sess.phase = ReportPhase::Reconciling;
        sess.attempt += 1;
        let ticket = FullSnapshotTicket {
            tag: sess.tag,
            attempt: sess.attempt,
        };
        let entries = std::mem::take(&mut sess.entries)
            .into_iter()
            .map(|(id, (status, block_size, storage_type))| BlockReportInfo {
                id,
                status,
                storage_type,
                block_size,
            })
            .collect();
        Some((entries, ticket))
    }

    /// 4d.3 / RC1 P0-1 / RC2 P0-1: finish one checkout — the ONLY path
    /// from a `Reconciling` row back to `Accumulating`, as an exact
    /// `(session, tag, attempt)` CAS on the row that was checked out.
    /// A row that was TERMINALIZED mid-flight (incremental / End /
    /// lost) is left terminal (`0b900a2f` — never resurrected); a row
    /// a newer Start installed (different session OR tag — a
    /// same-session retry) is untouched. There is no
    /// remove-then-blind-insert anywhere in the lifecycle.
    pub fn release_full_accumulator(&self, worker_id: u32, session: &str, tag: u64, attempt: u64) {
        let mut sessions = self.report_sessions.lock().unwrap();
        if let Some(sess) = sessions.get_mut(&worker_id) {
            if sess.session == session
                && sess.tag == tag
                && sess.attempt == attempt
                && sess.phase == ReportPhase::Reconciling
            {
                sess.phase = ReportPhase::Accumulating;
                sess.entries.clear();
                sess.total_len = 0;
                // `invalid` is intentionally NOT cleared: a
                // terminalization that raced the flight wins.
                sess.update_time_ms = LocalTime::mills();
            }
        }
    }

    /// 4d.3 full-report reconcile: apply ONE complete cache snapshot
    /// against the volatile domain under the EXACT fence
    /// `(epoch, session, tag, attempt, reconcile generation)`.
    ///
    /// Two-phase fence: the triple is CAPTURED under a brief volatile
    /// guard (phase A); classification runs outside it under one
    /// `fs_dir` read guard; the final guard (phase B) is ONE ATOMIC
    /// `accumulator → volatile` critical section — the EXACT row guard
    /// `(session, ticket tag, ticket attempt, Reconciling, !invalid)`
    /// is held while the volatile fence re-verifies session + tag +
    /// generation and the whole mutation completes (RC2 P0-1, gpt56
    /// `53516250` window 1: an incremental's terminalization + gen
    /// bump holds the same map lock across its volatile section, so
    /// the two can only serialize, never interleave). A Start (new
    /// tag + gen bump), a same-session Start RETRY (fresh row: the
    /// ticket's exact tag/attempt match fails, `53516250` window 2), a
    /// lost/End retire (registry row gone), an incremental F/W/Deleted
    /// (row terminalized), or an epoch flip (`lock_volatile` cold-clears
    /// the registry) in the window makes the ENTIRE reconcile a no-op.
    /// 复活禁止: nothing a superseded snapshot decided is applied.
    ///
    /// Apply = current-tag EXACT replace in two phases under the one
    /// final guard:
    /// 1. every CURRENT-tag replica + reverse trace of this worker
    ///    whose identity is MISSING from the snapshot is stripped (the
    ///    worker no longer holds it); other workers' replicas,
    ///    old-tag/retired drain state, and other objects are untouched;
    /// 2. the shared per-item decisions (the same
    ///    `classify_cache_report`/`apply_cache_decisions` pair the
    ///    incremental path uses — the two can never drift) publish
    ///    Valid×Finalized×R3-pass replicas, quarantine+strip
    ///    orphan/corrupt ones, ack Deleted ones.
    ///
    /// The returned outcome's WorkerManager side effects are applied by
    /// the caller through the same fenced `apply_cache_incr_outcome`
    /// transition as an incremental report.
    /// #[cfg(test)] 4d.3 test scaffolding (RC1 P0-1): un-terminalize the
    /// worker's accumulator row, modeling the FRESH row a new Start
    /// installs before a full report accumulates. Test seeds route
    /// through `incr_block_report`, which terminalizes the row per
    /// `0b900a2f` — production would never reconcile against that row
    /// without an intervening Start. Compiled out outside cfg(test).
    #[cfg(test)]
    pub(crate) fn reset_accumulator_for_full_test(&self, worker_id: u32) {
        let mut sessions = self.report_sessions.lock().unwrap();
        if let Some(sess) = sessions.get_mut(&worker_id) {
            sess.invalid = false;
            sess.entries.clear();
            sess.total_len = 0;
            sess.phase = ReportPhase::Accumulating;
            sess.update_time_ms = LocalTime::mills();
        }
    }

    /// #[cfg(test)] RC2 test scaffolding: model the production snapshot
    /// checkout — transition the fresh (Accumulating) row in place to
    /// Reconciling and hand out the exact `(tag, attempt)` ticket a
    /// `take_cache_full_snapshot`/self-Complete would return, so
    /// service-level reconcile tests enter with the same row state the
    /// fenced phase-B requires. Compiled out outside cfg(test).
    #[cfg(test)]
    pub(crate) fn checkout_ticket_for_full_test(
        &self,
        worker_id: u32,
    ) -> Option<FullSnapshotTicket> {
        let mut sessions = self.report_sessions.lock().unwrap();
        let sess = sessions.get_mut(&worker_id)?;
        if sess.invalid || sess.phase != ReportPhase::Accumulating {
            return None;
        }
        sess.phase = ReportPhase::Reconciling;
        sess.attempt += 1;
        Some(FullSnapshotTicket {
            tag: sess.tag,
            attempt: sess.attempt,
        })
    }

    /// RC1 P0-1: is the worker's accumulator row still a live
    /// (non-terminal) row bound to exactly this session? Called with the
    /// accumulator guard already held (leaf-before-volatile order).
    fn accumulator_row_live(
        sessions: std::sync::MutexGuard<'_, HashMap<u32, CacheReportSession>>,
        worker_id: u32,
        session: &str,
    ) -> bool {
        sessions
            .get(&worker_id)
            .is_some_and(|s| s.session == session && !s.invalid)
    }

    pub fn reconcile_cache_full_report(
        &self,
        worker_id: u32,
        session: &str,
        ticket: FullSnapshotTicket,
        entries: &[BlockReportInfo],
    ) -> CommonResult<CacheIncrOutcome> {
        if session.is_empty() || !self.enabled || !self.monitor.is_active() {
            return Ok(CacheIncrOutcome::default());
        }

        // RC1 P0-1 (gpt56 `d2546338` item 1): cheap entry check — the
        // accumulator row must still be a NON-TERMINAL same-session row
        // (the checkout keeps the row in place precisely so an
        // incremental / End / lost that WINS the flight can terminalize
        // it). The AUTHORITATIVE recheck is the held-guard exact row
        // match in phase B below.
        if !Self::accumulator_row_live(self.report_sessions.lock().unwrap(), worker_id, session) {
            log::warn!(
                "cache full-report reconcile for worker {} dropped: accumulator row terminal or foreign",
                worker_id
            );
            return Ok(CacheIncrOutcome::default());
        }

        // Phase A: capture the exact fence triple.
        let (reg_tag, reg_address, gen) = {
            let volatile = self.lock_volatile();
            match volatile.worker_sessions.get(&worker_id) {
                Some(s) if s.session == session => (
                    s.tag,
                    s.address.clone(),
                    volatile
                        .reconcile_gens
                        .get(&worker_id)
                        .copied()
                        .unwrap_or(0),
                ),
                _ => return Ok(CacheIncrOutcome::default()),
            }
        };

        // Classification against the authoritative store — outside the
        // volatile guard (one fs_dir read); the phase-B recheck owns
        // the window.
        let decisions = self.classify_cache_report(entries);

        // #[cfg(test)] deterministic seam: the capture → recheck window.
        #[cfg(test)]
        if let Some(hook) = FULL_RECONCILE_SEAM.lock().unwrap().as_ref() {
            hook();
        }

        // Phase B: the ATOMIC final fence (RC2 P0-1, gpt56 `53516250`
        // window 1). The EXACT accumulator row guard —
        // (session, ticket tag, ticket attempt, Reconciling, !invalid)
        // — is ACQUIRED AND HELD while the volatile guard is taken and
        // the whole mutation completes. An incremental's
        // terminalization holds the same map lock across its volatile
        // section, so the two can only serialize fully before or after
        // each other: once `invalid` is (or becomes) visible here the
        // reconcile no-ops BEFORE any volatile mutation; conversely,
        // while this fence holds the row, an incremental cannot write
        // `invalid` mid-mutation. The ticket's tag also makes a
        // same-wire-session Start RETRY (fresh row: new tag, attempt 0,
        // Accumulating) fail this exact match — the old snapshot can
        // never act on the new Start's row (`53516250` window 2).
        let sessions = self.report_sessions.lock().unwrap();
        let row_fenced = sessions.get(&worker_id).is_some_and(|s| {
            s.session == session
                && s.tag == ticket.tag
                && s.attempt == ticket.attempt
                && s.phase == ReportPhase::Reconciling
                && !s.invalid
        });
        if !row_fenced {
            log::warn!(
                "cache full-report reconcile for worker {} dropped: accumulator row terminalized, superseded by a Start retry, or foreign",
                worker_id
            );
            return Ok(CacheIncrOutcome::default());
        }
        let mut volatile = self.lock_volatile();
        // Volatile-side fence — session, tag, AND generation must all
        // still hold; then the generation is bumped so any LATER
        // incremental/reconcile capture fences against THIS one.
        let fenced = volatile
            .worker_sessions
            .get(&worker_id)
            .is_some_and(|s| s.session == session && s.tag == ticket.tag)
            && volatile
                .reconcile_gens
                .get(&worker_id)
                .copied()
                .unwrap_or(0)
                == gen;
        if !fenced {
            log::warn!(
                "cache full-report reconcile for worker {} dropped: session/tag/generation fence changed since snapshot",
                worker_id
            );
            return Ok(CacheIncrOutcome::default());
        }
        let applied_gen = {
            let entry = volatile.reconcile_gens.entry(worker_id).or_insert(0);
            *entry += 1;
            *entry
        };

        // Apply phase 1: current-tag exact replace — strip this
        // worker's current-tag replicas + live reverse traces that the
        // snapshot does NOT contain.
        let mut reported: HashSet<(i64, i64)> = HashSet::with_capacity(entries.len());
        for item in entries {
            if let Ok(owner) = BlockIdCodec::block_owner(item.id) {
                reported.insert((owner, BlockIdCodec::get_seq(item.id)));
            }
        }
        let stale: Vec<(i64, i64)> = volatile
            .by_worker
            .get(&worker_id)
            .map(|rev| {
                rev.live
                    .iter()
                    .flat_map(|(object_id, seqs)| seqs.iter().map(move |seq| (*object_id, *seq)))
                    .filter(|ident| !reported.contains(ident))
                    .collect()
            })
            .unwrap_or_default();
        for (object_id, seq) in stale {
            if let Some(locs) = volatile.locations.get_mut(&object_id) {
                if let Some(replicas) = locs.blocks.get_mut(&seq) {
                    replicas.retain(|r| !(r.worker.worker_id == worker_id && r.tag == reg_tag));
                    if replicas.is_empty() {
                        locs.blocks.remove(&seq);
                    }
                }
            }
            if let Some(rev) = volatile.by_worker.get_mut(&worker_id) {
                rev.live_remove_identity(object_id, seq);
            }
        }

        // Apply phase 2: the shared per-item decision apply.
        let (remove_blocks, deleted_acks) =
            volatile.apply_cache_decisions(worker_id, reg_tag, &reg_address, decisions);
        Ok(CacheIncrOutcome {
            session_tag: reg_tag,
            gen: applied_gen,
            remove_blocks,
            deleted_acks,
        })
    }

    /// Bounded outcome-window GC (4c.2): page the outcome rows with the
    /// ordered outcome scan, filter by the leader-OBSERVED client
    /// high-watermark (`op_seq < hw`, boundary excluded), and journal one
    /// bounded `CacheOutcomeGc` batch of per-client groups carrying the
    /// frozen `evict_below` fence. `done` is judged by the RAW scan page
    /// (a full page with zero eligible tokens still advances the cursor
    /// and continues).
    pub fn gc_outcomes(
        &self,
        rpc_id: i64,
        after: Option<&OpToken>,
        max_pages: usize,
    ) -> CommonResult<OutcomeGcProgress> {
        self.require_enabled()?;
        self.require_leader()?;
        let max_pages = max_pages.clamp(1, MUTATION_MAX_PAGES_PER_CALL);

        let mut cursor = after.copied();
        let mut processed = 0usize;
        for _ in 0..max_pages {
            let (page, hws) = {
                let store = self.fs_dir.read();
                let rocks = store.get_rocks_store();
                let page = rocks.cache_scan_outcomes(cursor.as_ref(), MUTATION_PAGE_CAP)?;
                let mut hws = HashMap::new();
                for t in &page {
                    if let std::collections::hash_map::Entry::Vacant(e) = hws.entry(t.client_id) {
                        e.insert(rocks.cache_client_watermark(t.client_id)?);
                    }
                }
                (page, hws)
            };
            if page.is_empty() {
                return Ok(OutcomeGcProgress {
                    done: true,
                    cursor,
                    processed,
                });
            }
            let next_cursor = *page.last().unwrap();

            // Freeze eligibility against the observed watermarks: group by
            // client, strictly below its observed HW. Page order is
            // (client, seq), so tokens of one client are contiguous.
            let mut groups: Vec<OutcomeGcGroup> = Vec::new();
            for t in &page {
                let eligible = matches!(hws.get(&t.client_id), Some(Some(hw)) if t.op_seq < *hw);
                if eligible {
                    match groups.last_mut() {
                        Some(g) if g.client_id == t.client_id => g.op_seqs.push(t.op_seq),
                        _ => {
                            let hw = match hws.get(&t.client_id) {
                                Some(Some(hw)) => *hw,
                                _ => unreachable!("eligibility checked above"),
                            };
                            groups.push(OutcomeGcGroup {
                                client_id: t.client_id,
                                evict_below: hw,
                                op_seqs: vec![t.op_seq],
                            })
                        }
                    }
                }
            }
            if !groups.is_empty() {
                // `processed` counts journaled evictions (the exact token
                // identities frozen into this batch), not scanned rows.
                let journaled: usize = groups.iter().map(|g| g.op_seqs.len()).sum();
                let op_id = self.fs_dir.read().next_op_id();
                let entry = JournalEntry::CacheOutcomeGc(CacheOutcomeGcEntry {
                    op_id,
                    rpc_id,
                    groups,
                });
                self.journal_writer
                    .sync_propose_cache(entry)
                    .map_err(fs_err)?;
                processed += journaled;
            }
            cursor = Some(next_cursor);
            if page.len() < MUTATION_PAGE_CAP {
                return Ok(OutcomeGcProgress {
                    done: true,
                    cursor,
                    processed,
                });
            }
        }
        Ok(OutcomeGcProgress {
            done: false,
            cursor,
            processed,
        })
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

    /// Retire one object to physical GC (4c.3, review `6bc4f569`
    /// gate 2/3): called only with a PROVEN-dead committed identity
    /// (row missing / different object / same-object Tombstoned, or an
    /// inactive incarnation). Plans for the object are dropped
    /// immediately; the retained locations entry survives until the GC
    /// drain completes (the drain's completion is the only thing that
    /// removes it). A GC work item is enqueued when the frozen geometry
    /// is known — from the retiring row (`geometry`, a full committed
    /// row: tombstones zero `len`) or, failing that, from the geometry
    /// the commit publish recorded next to the locations. With neither
    /// source (e.g. the object never published locations — a Reserved
    /// load fenced mid-write) no item is created: the unreported
    /// physical blocks are the 4d full-report/orphan pass's problem.
    ///
    /// Lock order (4c.3/4d invariant): volatile only (plans included
    /// since the 4d merge), and never an fs_dir guard here.
    fn retire_object_state(
        &self,
        incarnation: u64,
        object_id: i64,
        geometry: Option<(i64, i64)>,
    ) -> CommonResult<()> {
        {
            let mut volatile = self.lock_volatile();
            let recorded = volatile
                .locations
                .get(&object_id)
                .filter(|l| l.block_size > 0 && !l.blocks.is_empty())
                .map(|l| (l.len, l.block_size));
            let known = geometry.or(recorded);
            if let Some((len, block_size)) = known {
                // Idempotent for a response-loss retry: an existing item
                // for this object_id keeps its drain cursor.
                volatile.gc.enqueue(CacheGcWork {
                    incarnation,
                    object_id,
                    len,
                    block_size,
                    next_seq: 1,
                })?;
            } else if !volatile.gc.items.contains_key(&object_id) {
                // No geometry and no earlier item:
                // nothing drainable — drop any retained entry so it
                // cannot leak. The 4d full report re-derives whatever
                // unreported physical blocks exist. RC3: the drop also
                // clears the object's reverse traces in the same guard.
                // RC2-round2: a quarantine-only object (no locations row
                // ever published) MUST clear its quarantine row too —
                // the old locations-None early-return leaked it forever.
                volatile.gc.order.retain(|id| *id != object_id);
                volatile.drop_object_state(object_id);
            }
            // Plans for the object are dropped under the same guard
            // (4d R8-1 supplement): with plans merged into the volatile
            // domain the retain is atomic with the retire above.
            volatile.plans.retain(|_, plan| plan.object_id != object_id);
        }
        Ok(())
    }

    /// Merge a dead commit's block evidence into the retained locations
    /// and (re-)ensure a GC work item exists (review `6bc4f569` gate 4).
    /// Called with the volatile lock held, only when the locked
    /// re-check proved the committed row is no longer an exact-Valid
    /// match for the commit: the client-reported workers DID receive
    /// these blocks, so the GC drain must be able to target them. If
    /// the object's drain already completed (item gone, locations
    /// removed), the evidence re-seeds both — the fresh item restarts
    /// at `next_seq = 1`; BlockMap's HashSet makes any re-derived
    /// duplicate enqueue idempotent.
    fn merge_dead_commit_evidence(
        &self,
        volatile: &mut CacheVolatile,
        incarnation: u64,
        object_id: i64,
        len: i64,
        block_size: i64,
        blocks: &[CacheBlockLocation],
    ) -> CommonResult<()> {
        let (known_len, known_block_size) = {
            let object_locations = volatile.locations.entry(object_id).or_default();
            if object_locations.block_size == 0 {
                object_locations.len = len;
                object_locations.block_size = block_size;
            }
            for (index, block) in blocks.iter().enumerate() {
                let entry = object_locations
                    .blocks
                    .entry((index + 1) as i64)
                    .or_default();
                for w in &block.workers {
                    // 4d.2: dead-evidence replicas are GC-drain targets
                    // only, never read-path candidates — tag 0 (UNFENCED)
                    // keeps them outside the current-tag read filter while
                    // `gc_take_batch` still drains ALL replicas.
                    if !entry.iter().any(|x| x.worker.worker_id == w.worker_id) {
                        entry.push(Replica {
                            worker: w.clone(),
                            tag: 0,
                        });
                    }
                }
            }
            (object_locations.len, object_locations.block_size)
        };
        if !volatile.gc.items.contains_key(&object_id) {
            volatile.gc.enqueue(CacheGcWork {
                incarnation,
                object_id,
                len: known_len,
                block_size: known_block_size,
                next_seq: 1,
            })?;
        }
        Ok(())
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

    /// 4d R8-3 + RC1 (gpt56 `7ceef2ff` item 1): freeze the plan fences
    /// for the chosen worker sets — the current journal epoch plus, for
    /// every chosen worker, its CURRENT session tag keyed by FULL
    /// endpoint identity, captured atomically under the volatile guard.
    /// FAIL-CLOSED: a chosen worker with no registered session (legacy
    /// worker, pre-Start worker) or whose registry address does not
    /// fully match the planned address (worker_id endpoint drift)
    /// refuses the whole placement BEFORE any object id is issued —
    /// legacy workers keep the FS path and can never receive or publish
    /// cache plans (frozen R4).
    fn capture_plan_fences(
        &self,
        sets: &[Vec<WorkerAddress>],
    ) -> CommonResult<(u64, Vec<HashMap<WorkerIdent, u64>>)> {
        let volatile = self.lock_volatile();
        let mut fences = Vec::with_capacity(sets.len());
        for (block_index, workers) in sets.iter().enumerate() {
            let mut block_fences = HashMap::with_capacity(workers.len());
            for worker in workers {
                let Some(row) = volatile.worker_sessions.get(&worker.worker_id) else {
                    return err_box!(
                        "cache plan fence capture failed: block {} worker {} ({}) has no registered cache session (legacy or pre-Start): cache placement refused fail-closed, FS path unchanged",
                        block_index + 1,
                        worker.worker_id,
                        worker.connect_addr()
                    );
                };
                if !same_full_address(&row.address, worker) {
                    return err_box!(
                        "cache plan fence capture failed: block {} worker {} registry address ({} -> {}) does not fully match the planned address ({} -> {}): endpoint drift refuses the fence",
                        block_index + 1,
                        worker.worker_id,
                        row.address.hostname,
                        row.address.connect_addr(),
                        worker.hostname,
                        worker.connect_addr()
                    );
                }
                block_fences.insert(WorkerIdent::of(worker), row.tag);
            }
            fences.push(block_fences);
        }
        Ok((volatile.epoch, fences))
    }

    /// 4d R8-3 + RC4 (gpt56 `7ceef2ff` item 4): do the plan fences
    /// still hold? The epoch must match the volatile domain's bound
    /// epoch, and every ACTUAL evidence worker is looked up in the
    /// per-block identity→tag map — by full endpoint identity, never by
    /// position, so subset/reordered evidence cannot mis-pair tags. A
    /// missing identity (unplanned worker), or a registry row whose tag
    /// advanced or whose full address drifted, is a breach. Empty
    /// `fences` (test-installed plan via the `#[cfg(test)]`
    /// `install_plan` seam) freeze nothing and skip the per-replica
    /// check; production `capture_plan_fences` always freezes one map
    /// per planned block, so this skip is unreachable in production.
    fn check_plan_fences(
        volatile: &CacheVolatile,
        plan_epoch: u64,
        blocks: &[CacheBlockLocation],
        fences: &[HashMap<WorkerIdent, u64>],
    ) -> CommonResult<()> {
        if volatile.epoch != plan_epoch {
            return err_box!(
                "cache plan fence lost: plan epoch {} but volatile epoch {} (leadership changed)",
                plan_epoch,
                volatile.epoch
            );
        }
        if fences.is_empty() {
            return Ok(());
        }
        for (block, block_fences) in blocks.iter().zip(fences.iter()) {
            for worker in &block.workers {
                let ident = WorkerIdent::of(worker);
                let Some(&tag) = block_fences.get(&ident) else {
                    return err_box!(
                        "cache plan fence lost: block {} evidence worker {} ({}) is not a fenced (planned) replica of this plan",
                        block.block_id,
                        worker.worker_id,
                        worker.connect_addr()
                    );
                };
                let current = volatile.worker_sessions.get(&worker.worker_id);
                let holds =
                    current.is_some_and(|s| s.tag == tag && same_full_address(&s.address, worker));
                if !holds {
                    return err_box!(
                        "cache plan fence lost: block {} worker {} ({}) session advanced past the planned tag {} (now {:?})",
                        block.block_id,
                        worker.worker_id,
                        worker.connect_addr(),
                        tag,
                        current.map(|s| s.tag)
                    );
                }
            }
        }
        Ok(())
    }

    /// 4d R8-3 pre-propose fence: validate the plan's epoch + replica
    /// session tags against the live registry BEFORE anything is
    /// proposed. A breach is retryable the same way a lost plan is:
    /// replay the exact allocate (which re-plans against current
    /// sessions) and re-commit.
    fn validate_plan_fences(&self, plan: &LoadPlan) -> CommonResult<()> {
        let volatile = self.lock_volatile();
        Self::check_plan_fences(&volatile, plan.epoch, &plan.blocks, &plan.fences)
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
    /// The layout's geometry is recorded exactly like a real commit
    /// publish would (4c.3), so later GC-retire fallbacks see it.
    /// 4d.2: each replica is tagged with its worker's CURRENT registry
    /// tag (mirroring a real publish, whose fences carry exactly that
    /// tag); a worker with no registry row gets tag 0, which the
    /// current-tag read filter excludes — tests that assert a get() hit
    /// must seed sessions first.
    #[cfg(test)]
    fn install_locations(
        &self,
        object_id: i64,
        layout: &CacheBlockLayout,
        blocks: Vec<CacheBlockLocation>,
    ) -> CommonResult<()> {
        let mut volatile = self.state.lock().unwrap();
        let tags: HashMap<u32, u64> = volatile
            .worker_sessions
            .iter()
            .map(|(id, s)| (*id, s.tag))
            .collect();
        let object_locations = volatile.locations.entry(object_id).or_default();
        object_locations.len = layout.len;
        object_locations.block_size = layout.block_size;
        object_locations.blocks.clear();
        for (index, block) in blocks.into_iter().enumerate() {
            let seq = (index + 1) as i64;
            let replicas: Vec<Replica> = block
                .workers
                .into_iter()
                .map(|w| {
                    let tag = tags.get(&w.worker_id).copied().unwrap_or(0);
                    Replica { worker: w, tag }
                })
                .collect();
            object_locations.blocks.insert(seq, replicas);
        }
        Ok(())
    }

    /// Test seam: does the volatile GC queue hold a work item for this
    /// object?
    #[cfg(test)]
    fn gc_has_work(&self, object_id: i64) -> bool {
        self.state.lock().unwrap().gc.items.contains_key(&object_id)
    }

    /// Test seam: is this object's locations entry still retained (live
    /// published, or retained until the GC drain completes)?
    #[cfg(test)]
    fn location_retained(&self, object_id: i64) -> bool {
        self.state
            .lock()
            .unwrap()
            .locations
            .contains_key(&object_id)
    }

    /// Test seam for volatile load plans (stand-in for a completed
    /// allocate whose raft barrier is unavailable in unit tests).
    #[cfg(test)]
    fn install_plan(&self, token: OpToken, plan: LoadPlan) {
        self.state.lock().unwrap().plans.insert(token, plan);
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
    use curvine_model::{
        AccessMode, HeartbeatStatus, MountOptions, TransferWorkerCapabilities, WorkerCommand,
        WriteType,
    };
    use curvine_raft::raft::{RaftClient, RoleState};
    use curvine_runtime::sync::StateCtl;
    use std::collections::BTreeSet;

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
        CacheService::new(fs_dir, writer, monitor, chooser, enabled, 1_000_000)
    }

    /// Grants `incarnation` an active incarnation row with a frozen TTL
    /// (unit-test stand-in for the 4b issuer: the real path verifies a
    /// persisted write-cache-enabled mount table and crosses a raft
    /// barrier). One call per incarnation; the token derives from the
    /// incarnation so repeated calls never collide.
    /// 4d RC1 (gpt56 `7ceef2ff` item 1): production `capture_plan_fences`
    /// fail-closes on any chosen worker without a registered cache session
    /// whose FULL address matches the registry. Allocate-path unit tests
    /// seed REAL sessions through this explicit test-only helper; fence and
    /// session tests keep installing their own sessions by hand.
    fn seed_sessions(service: &CacheService, workers: &[WorkerAddress]) {
        for w in workers {
            service
                .begin_cache_session(w.worker_id, &format!("seed-{}", w.worker_id), w)
                .unwrap();
        }
    }

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
            // Unfenced test plan: epoch 0 matches the private test
            // epoch; no frozen fences means the R8-3 per-replica check
            // is skipped (fence-specific tests freeze real maps).
            epoch: 0,
            fences: Vec::new(),
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

        // Complete -> hit with exact derived ids/lengths. 4d.2: replicas
        // are served only under the worker's current session tag, so the
        // workers must be sessioned before the locations are installed.
        seed_sessions(&service, &[worker(1), worker(2)]);
        service
            .install_locations(OBJ, &lay, full_locations(&lay))
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
        service.install_locations(OBJ, &lay, partial).unwrap();
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
            .install_locations(OBJ, &layout(OBJ, 64), full_locations(&layout(OBJ, 64)))
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
        seed_sessions(&service, &[worker(1), worker(2)]);
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
        seed_sessions(&service, &[worker(1), worker(2)]);
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
        seed_sessions(&service, &[worker(1)]);
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
        assert!(service
            .state
            .lock()
            .unwrap()
            .plans
            .contains_key(&token(2, 1)));

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

    /// Phase 3 task #5 RC (gpt56 `e0875176`) P0 regression: cache-load
    /// op tokens live in PER-TASK domains (unique client id per task,
    /// fixed load.op_seq=1 / commit.op_seq=2, minted by the master's
    /// `mint_cache_load_spec`). Under a SHARED client id with increasing
    /// op seqs, a later-created task's applied Allocate pushes the
    /// per-client watermark past an earlier task's not-yet-executed
    /// Allocate, which is then misjudged Expired on FIRST execution —
    /// reproducible without any outcome GC, purely by dispatch order.
    #[test]
    fn test_allocate_per_task_token_domain_no_cross_expiry() {
        let service = build_service("alloc-per-task-domain", chooser(vec![worker(1)]));
        seed_sessions(&service, &[worker(1)]);
        mount_incarnation(&service, 1, 0);

        // The shared-domain hazard, reproduced deterministically: task B
        // (minted later, op_seq 102) is dispatched first and its applied
        // Allocate sets the shared client's watermark to 102.
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
            mgr.apply_allocate(rocks, token(7, 102), 1, "/b", 128, &alloc)
                .unwrap();
        }
        // Earlier-created task A (op_seq 100, same shared client) is
        // Expired on its very first execution — the failure the fix
        // removes.
        let err = service
            .allocate(token(7, 100), 7, 1, "/a", 128, 64)
            .unwrap_err();
        assert!(
            format!("{}", err).contains("expired (client watermark 102)"),
            "{}",
            err
        );

        // The fixed scheme: per-task domains. Task B (client 10, load
        // op 1) applied first; earlier task A (client 9, load op 1) has
        // its own watermark domain, so its FIRST execution passes the
        // FSM token gate and reaches the fail-closed raft reserve
        // barrier — the unit-harness terminal for a fully valid
        // allocate (no cluster).
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            let mgr = &store.cache;
            let alloc = CacheEntry {
                generation: 1,
                state: CacheEntryState::Reserved,
                object_id: OBJ + 1,
                len: 0,
                ufs_mtime: 0,
                block_size: 64,
                expire_at: 0,
            };
            mgr.apply_allocate(rocks, token(10, 1), 1, "/b2", 128, &alloc)
                .unwrap();
        }
        let err = service
            .allocate(token(9, 1), 7, 1, "/a2", 128, 64)
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("raft") && !msg.contains("expired"),
            "per-task domain: first execution must pass the token gate: {}",
            msg
        );
    }

    /// Phase 3 task #5: the cache-load task's exact token pair (one
    /// client, load.op_seq=1 / commit.op_seq=2) round-trips both
    /// response-loss retries — the Allocate replays its recorded
    /// geometry (same identity, regenerated placement), and the Commit
    /// resolves its recorded Committed outcome as AlreadyApplied.
    #[test]
    fn test_cache_load_task_token_pair_exact_retry() {
        let service = build_service("load-task-token-pair", chooser(vec![worker(1)]));
        seed_sessions(&service, &[worker(1)]);
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
            mgr.apply_allocate(rocks, token(20, 1), 1, "/t", 130, &alloc)
                .unwrap();
            mgr.apply_commit(
                rocks,
                token(20, 1),
                token(20, 2),
                1,
                "/t",
                1,
                OBJ,
                130,
                777,
                0,
            )
            .unwrap();
        }

        // Allocate response-loss retry: committed identity, regenerated
        // placement for the SAME identity (130 = 3 blocks of 64).
        let replay = service.allocate(token(20, 1), 7, 1, "/t", 130, 64).unwrap();
        assert_eq!(replay.object_id, OBJ);
        assert_eq!(replay.generation, 1);
        assert_eq!(replay.blocks.len(), 3);
        assert_eq!(replay.blocks[0].workers, vec![worker(1)]);

        // Commit response-loss retry with the same pair resolves its
        // recorded outcome — never a re-execution.
        let params = CacheCommitParams {
            token: token(20, 2),
            load_token: token(20, 1),
            rpc_id: 7,
            incarnation: 1,
            key: "/t",
            generation: 1,
            object_id: OBJ,
            len: 130,
            ufs_mtime: 777,
            ttl_ms: 0,
            blocks: replay.blocks.clone(),
        };
        assert_eq!(
            service.commit(params).unwrap(),
            CacheOpStatus::AlreadyApplied
        );
    }

    /// Phase 3 task #5 RC (gpt56 `4ebcff5a`): the load outcome's op_seq
    /// (1) sits below the client watermark the commit (2) itself pushed
    /// forward, so the bounded outcome window may evict it — even while
    /// the commit's own response is still in flight (response loss).
    /// Recovery MUST converge from the commit side: the commit outcome
    /// (at the watermark, boundary-excluded from eviction) plus the
    /// committed Valid row answer AlreadyApplied WITHOUT re-reading the
    /// evicted load outcome, and the cache-index row remains readable
    /// for the master's re-create convergence when the whole task (not
    /// just the response) was lost.
    #[test]
    fn test_commit_exact_retry_survives_load_outcome_gc() {
        let service = build_service("commit-retry-after-gc", chooser(vec![worker(1)]));
        seed_sessions(&service, &[worker(1)]);
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
            mgr.apply_allocate(rocks, token(30, 1), 1, "/g", 130, &alloc)
                .unwrap();
            // The commit applied (HW -> 2) but its response was lost.
            mgr.apply_commit(
                rocks,
                token(30, 1),
                token(30, 2),
                1,
                "/g",
                1,
                OBJ,
                130,
                777,
                0,
            )
            .unwrap();
            // The bounded outcome window evicts the load outcome (1 < 2);
            // the watermark and the commit outcome survive.
            let mut w = rocks.cache_write();
            w.delete_outcome(token(30, 1)).unwrap();
            w.commit().unwrap();
        }

        // Allocate-first recovery is a dead end by design — the evicted
        // load token is TERMINAL below the watermark. This documents WHY
        // the runner retries the commit verbatim instead of re-allocating.
        let err = service
            .allocate(token(30, 1), 7, 1, "/g", 130, 64)
            .unwrap_err();
        assert!(
            format!("{}", err).contains("expired (client watermark 2)"),
            "{}",
            err
        );

        // Commit-side recovery: the exact self-contained replay resolves
        // from the commit outcome + committed Valid row alone.
        let params = CacheCommitParams {
            token: token(30, 2),
            load_token: token(30, 1),
            rpc_id: 7,
            incarnation: 1,
            key: "/g",
            generation: 1,
            object_id: OBJ,
            len: 130,
            ufs_mtime: 777,
            ttl_ms: 0,
            blocks: vec![],
        };
        assert_eq!(
            service.commit(params).unwrap(),
            CacheOpStatus::AlreadyApplied
        );

        // Whole-task loss (worker crash, not just response loss): the
        // committed cache-index row still serves the master's
        // check_already_loaded convergence — same len and ufs_mtime.
        let hit = service
            .get(1, "/g", false)
            .unwrap()
            .expect("row survives outcomes");
        assert_eq!(hit.len, 130);
        assert_eq!(hit.ufs_mtime, 777);
        assert_eq!(hit.object_id, OBJ);
    }

    /// len=0 is a legal empty object end to end at the retry path: the
    /// regenerated plan (and thus the future commit evidence) is empty.
    #[test]
    fn test_allocate_len0_replan_is_empty() {
        let service = build_service("allocate-len0", chooser(vec![worker(1)]));
        seed_sessions(&service, &[worker(1)]);
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
        seed_sessions(&service, &[worker(1)]);
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

    /// The volatile plan is mandatory: a commit without it (master
    /// restart lost the plan) resolves to the typed re-planable status —
    /// REPLAN_NEEDED, not a string Err the runner would retry-blind —
    /// before any other judgment about the entry row (task #5 RC
    /// `40e47dcb`).
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
        let status = service
            .commit(commit_params(
                token(9, 1),
                token(9, 2),
                full_locations(&lay),
            ))
            .unwrap();
        assert_eq!(status, CacheOpStatus::ReplanNeeded);
    }

    /// gpt56 `1c436760` seam: a fence-invalidated plan (worker session
    /// advanced after the writes, before the commit) resolves commit to
    /// typed REPLAN_NEEDED AND drops the stale plan; the exact allocate
    /// replay re-plans the SAME identity with FRESH fences bound to the
    /// CURRENT session tags (never the stale placements), and the
    /// re-commit passes the plan/fence checks.
    #[test]
    fn test_commit_fence_breach_replan_replays_fresh_fences() {
        let service = build_service("fence-breach-replan", chooser(vec![worker(1), worker(2)]));
        mount_incarnation(&service, 1, 0);
        // Reserved row + recorded load outcome (no durable commit yet).
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
            mgr.apply_allocate(rocks, token(21, 1), 1, "/k", 130, &alloc)
                .unwrap();
        }
        service.begin_cache_session(1, "w1-s1", &worker(1)).unwrap();
        service.begin_cache_session(2, "w2-s1", &worker(2)).unwrap();
        let (t1, t2) = {
            let volatile = service.state.lock().unwrap();
            (
                volatile.worker_sessions.get(&1).unwrap().tag,
                volatile.worker_sessions.get(&2).unwrap().tag,
            )
        };
        let lay = layout(OBJ, 130);
        let mut plan = plan_for(&lay);
        plan.epoch = service.monitor.journal_epoch();
        plan.fences = lay
            .block_ids()
            .map(|_| {
                let mut m = HashMap::new();
                m.insert(WorkerIdent::of(&worker(1)), t1);
                m.insert(WorkerIdent::of(&worker(2)), t2);
                m
            })
            .collect();
        service.install_plan(token(21, 1), plan);

        // Worker 2's session advances after the writes: the commit has
        // NOT applied and must come back as typed REPLAN_NEEDED (never a
        // string Err the runner would blind-retry), with the stale plan
        // dropped so the allocate replay cannot hand it back.
        service.begin_cache_session(2, "w2-s2", &worker(2)).unwrap();
        assert_eq!(
            service
                .commit(commit_params(
                    token(21, 1),
                    token(21, 2),
                    full_locations(&lay)
                ))
                .unwrap(),
            CacheOpStatus::ReplanNeeded
        );
        assert!(
            !service
                .state
                .lock()
                .unwrap()
                .plans
                .contains_key(&token(21, 1)),
            "a fenced-out plan must not stay replayable"
        );

        // Exact allocate replay: SAME identity (no second id minted) and
        // a FRESH plan whose fences hold against the CURRENT sessions.
        let replay = service.allocate(token(21, 1), 7, 1, "/k", 130, 64).unwrap();
        assert_eq!(replay.object_id, OBJ);
        assert_eq!(replay.generation, 1);
        assert_eq!(replay.blocks.len() as i64, lay.block_count);
        let new_plan = service
            .state
            .lock()
            .unwrap()
            .plans
            .get(&token(21, 1))
            .cloned()
            .unwrap();
        assert!(
            service.validate_plan_fences(&new_plan).is_ok(),
            "re-planned fences must hold against the current sessions"
        );
        // The fresh plan fences worker 2's NEW session tag, not the
        // stale one the commit rejected.
        let new_t2 = service
            .state
            .lock()
            .unwrap()
            .worker_sessions
            .get(&2)
            .unwrap()
            .tag;
        assert_ne!(t2, new_t2);
        let fenced_t2 = new_plan
            .fences
            .first()
            .and_then(|m| m.get(&WorkerIdent::of(&worker(2))))
            .unwrap();
        assert_eq!(
            *fenced_t2, new_t2,
            "the re-plan must fence the NEW session tag"
        );

        // The re-commit with the NEW placements passes the plan/fence
        // checks and reaches the raft barrier (fail-closed in this
        // harness — full applied-path coverage lives in real-raft tests).
        let err = service
            .commit(commit_params(
                token(21, 1),
                token(21, 2),
                replay.blocks.clone(),
            ))
            .unwrap_err();
        assert!(
            format!("{}", err).contains("raft"),
            "re-commit must reach the raft barrier: {}",
            err
        );
    }

    /// Gate-2 red test (gpt56 `fca627f5`): task A's allocate persists a
    /// Reserved row, the runner's write fails before ANY commit, and the
    /// abort releases the row — after which task B (a NEW token on the
    /// same key) passes the row gate instead of wedging on "only None or
    /// Tombstoned rows allocate", and CacheGet has no Valid hit.
    #[test]
    fn test_abort_releases_reserved_row_and_reopens_the_key() {
        let service = build_service("abort-reopens-key", chooser(vec![worker(1)]));
        seed_sessions(&service, &[worker(1)]);
        mount_incarnation(&service, 1, 0);
        // Task A: durable Reserved row + recorded load outcome (what
        // apply_allocate leaves behind after the raft barrier).
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
            mgr.apply_allocate(rocks, token(31, 1), 1, "/k", 130, &alloc)
                .unwrap();
        }

        // The abort passes every service precheck and reaches the raft
        // barrier (fail-closed "raft" Err in this harness).
        let err = service
            .abort(7, token(31, 1), token(31, 2), 1, "/k")
            .unwrap_err();
        assert!(
            format!("{}", err).contains("raft"),
            "abort must reach the raft barrier: {}",
            err
        );

        // Simulate the barrier passing: the applied abort tombstones the
        // Reserved row, records the Aborted outcome under the SHARED
        // commit token, and advances the client watermark.
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store
                .cache
                .apply_abort(rocks, token(31, 1), token(31, 2), 1, "/k", 1, 2, OBJ)
                .unwrap();
        }
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            let row = rocks.cache_get_entry(1, "/k").unwrap().unwrap();
            assert_eq!(row.state, CacheEntryState::Tombstoned);
            assert_eq!(row.generation, 2);
            assert!(matches!(
                rocks.cache_get_outcome(token(31, 2)).unwrap(),
                Some(OpOutcome::Aborted { .. })
            ));
        }
        // No Valid hit for readers.
        assert!(service.get(1, "/k", false).unwrap().is_none());

        // Abort replay through the service: the tombstoned fence is
        // AlreadyApplied (idempotent, plan cleared).
        let lay = layout(OBJ, 130);
        service.install_plan(token(31, 1), plan_for(&lay));
        assert_eq!(
            service
                .abort(7, token(31, 1), token(31, 2), 1, "/k")
                .unwrap(),
            CacheOpStatus::AlreadyApplied
        );
        assert!(
            !service
                .state
                .lock()
                .unwrap()
                .plans
                .contains_key(&token(31, 1)),
            "abort replay must clear the load's plan"
        );
        // And the applied replay is a strict no-op.
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store
                .cache
                .apply_abort(rocks, token(31, 1), token(31, 2), 1, "/k", 1, 2, OBJ)
                .unwrap();
        }

        // Task B: a NEW token on the same key passes the row gate — the
        // failure is the harness raft barrier (id issuance), never the
        // Reserved wedge.
        let err = service
            .allocate(token(32, 1), 7, 1, "/k", 130, 64)
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("raft"), "task B allocate: {}", msg);
        assert!(
            !msg.contains("only None or Tombstoned"),
            "the key must be re-allocatable after the abort: {}",
            msg
        );
    }

    /// The commit-outcome guard (gpt56 `fca627f5`): once the load's
    /// commit token has a recorded outcome the abort is refused BEFORE
    /// the raft barrier — a commit that may have applied must be
    /// resolved by its own verbatim retry, never aborted underneath.
    #[test]
    fn test_abort_refused_when_commit_outcome_recorded() {
        let service = build_service("abort-refused", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        // A fully applied commit: Valid row + Committed outcome under the
        // commit token.
        committed_entry(&service, token(41, 1), token(41, 2), "/k", OBJ, 130, 0);
        let err = service
            .abort(7, token(41, 1), token(41, 2), 1, "/k")
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("refused"),
            "abort must be refused on a recorded commit outcome: {}",
            msg
        );
        assert!(
            !msg.contains("raft"),
            "refusal is at the guard, before the barrier: {}",
            msg
        );
        // The Valid row survives untouched.
        assert!(service.get(1, "/k", false).unwrap().is_some());
    }

    /// Apply-order race, commit-first (gpt56 `21bb7129` + `52db24f3`
    /// blocker 1): both Commit and Abort passed the service precheck, the
    /// Commit journal entry lands first — the Abort apply is the
    /// DETERMINISTIC first-winner-loser no-op (never an Err: the journal
    /// loader treats cache apply errors as fatal), the Valid row is
    /// preserved, and the loud refusal lives at the handler. Both entries
    /// are applied through the production journal dispatch
    /// (`apply_cache_journal_entry`), not direct manager calls.
    #[test]
    fn test_apply_race_commit_first_keeps_valid_row() {
        let service = build_service("race-commit-first", chooser(vec![worker(1)]));
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
            mgr.apply_allocate(rocks, token(51, 1), 1, "/k", 130, &alloc)
                .unwrap();
        }
        // Commit lands first through the production dispatch: Reserved@1
        // → Valid@1 with a Committed outcome under the shared commit
        // token.
        service
            .fs_dir
            .read()
            .apply_cache_journal_entry(&JournalEntry::CacheCommit(CacheCommitEntry {
                op_id: 1,
                rpc_id: 7,
                token: token(51, 2),
                load_token: token(51, 1),
                incarnation: 1,
                key: "/k".to_string(),
                generation: 1,
                expected_object_id: OBJ,
                len: 130,
                ufs_mtime: 777,
                expire_at: 0,
            }))
            .unwrap();
        // The racing abort entry applies as a no-op loser — an Err here
        // would be FATAL for the journal loader (gpt56 `52db24f3`).
        service
            .fs_dir
            .read()
            .apply_cache_journal_entry(&JournalEntry::CacheAbort(CacheAbortEntry {
                op_id: 2,
                rpc_id: 7,
                load_token: token(51, 1),
                commit_token: token(51, 2),
                incarnation: 1,
                key: "/k".to_string(),
                expected_generation: 1,
                new_generation: 2,
                expected_object_id: OBJ,
            }))
            .unwrap();
        // The Valid row survives: published data is never deleted.
        assert!(service.get(1, "/k", false).unwrap().is_some());
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            let row = rocks.cache_get_entry(1, "/k").unwrap().unwrap();
            assert_eq!(row.state, CacheEntryState::Valid);
            assert_eq!(row.generation, 1);
        }
        // The loud refusal lives at the handler: a later abort RPC for
        // the same load is refused by the commit-outcome guard before
        // the barrier.
        let err = service
            .abort(7, token(51, 1), token(51, 2), 1, "/k")
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("refused"),
            "abort handler must refuse loudly under a recorded commit outcome: {}",
            msg
        );
        assert!(
            !msg.contains("raft"),
            "refusal is at the guard, before the barrier: {}",
            msg
        );
    }

    /// Apply-order race, abort-first (gpt56 `21bb7129`): the Abort entry
    /// lands first — the row is Tombstoned@2 with an Aborted outcome
    /// under the shared commit token, and the racing Commit apply is a
    /// terminal no-op that must NOT publish over the tombstone. Also
    /// driven through the production journal dispatch, and a commit
    /// reusing the token with DIFFERENT parameters is loud divergence,
    /// never a silent no-op (gpt56 `52db24f3`).
    #[test]
    fn test_apply_race_abort_first_never_publishes() {
        let service = build_service("race-abort-first", chooser(vec![worker(1)]));
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
            mgr.apply_allocate(rocks, token(61, 1), 1, "/k", 130, &alloc)
                .unwrap();
        }
        // Abort lands first through the production dispatch: Reserved@1
        // → Tombstoned@2, Aborted outcome under the shared commit token.
        service
            .fs_dir
            .read()
            .apply_cache_journal_entry(&JournalEntry::CacheAbort(CacheAbortEntry {
                op_id: 1,
                rpc_id: 7,
                load_token: token(61, 1),
                commit_token: token(61, 2),
                incarnation: 1,
                key: "/k".to_string(),
                expected_generation: 1,
                new_generation: 2,
                expected_object_id: OBJ,
            }))
            .unwrap();
        // The racing commit apply is a terminal no-op (field-exact
        // Aborted match, checked before the load binding), never a
        // publish.
        service
            .fs_dir
            .read()
            .apply_cache_journal_entry(&JournalEntry::CacheCommit(CacheCommitEntry {
                op_id: 2,
                rpc_id: 7,
                token: token(61, 2),
                load_token: token(61, 1),
                incarnation: 1,
                key: "/k".to_string(),
                generation: 1,
                expected_object_id: OBJ,
                len: 130,
                ufs_mtime: 777,
                expire_at: 0,
            }))
            .unwrap();
        // The SAME commit token re-used with a different object identity
        // is loud divergence under the tightened fast-path, never a
        // silent terminal no-op.
        let err = service
            .fs_dir
            .read()
            .apply_cache_journal_entry(&JournalEntry::CacheCommit(CacheCommitEntry {
                op_id: 3,
                rpc_id: 7,
                token: token(61, 2),
                load_token: token(61, 1),
                incarnation: 1,
                key: "/k".to_string(),
                generation: 1,
                expected_object_id: OBJ + 5,
                len: 130,
                ufs_mtime: 777,
                expire_at: 0,
            }))
            .unwrap_err();
        assert!(
            format!("{}", err).contains("different load"),
            "divergent commit under a consumed token must be loud: {}",
            err
        );
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            let row = rocks.cache_get_entry(1, "/k").unwrap().unwrap();
            assert_eq!(row.state, CacheEntryState::Tombstoned);
            assert_eq!(row.generation, 2);
            assert!(matches!(
                rocks.cache_get_outcome(token(61, 2)).unwrap(),
                Some(OpOutcome::Aborted { .. })
            ));
        }
        assert!(service.get(1, "/k", false).unwrap().is_none());
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
            !service
                .state
                .lock()
                .unwrap()
                .plans
                .contains_key(&token(2, 1)),
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
            !service
                .state
                .lock()
                .unwrap()
                .plans
                .contains_key(&token(2, 1)),
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
            !service
                .state
                .lock()
                .unwrap()
                .plans
                .contains_key(&token(2, 1)),
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
        assert!(!service
            .state
            .lock()
            .unwrap()
            .plans
            .contains_key(&token(9, 1)));

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
        assert!(!service
            .state
            .lock()
            .unwrap()
            .plans
            .contains_key(&token(9, 1)));
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
            .install_locations(OBJ, &layout(OBJ, 64), full_locations(&layout(OBJ, 64)))
            .unwrap();
        assert_eq!(
            service.invalidate(7, 1, "/k", 1, OBJ).unwrap(),
            CacheOpStatus::AlreadyApplied
        );
        assert!(
            service.gc_has_work(OBJ),
            "AlreadyApplied retire enqueues GC work"
        );

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
            .install_locations(b, &b_lay, full_locations(&b_lay))
            .unwrap();
        service.install_plan(token(6, 1), plan_for(&b_lay));

        // Forged invalidate: expected_generation 1 fences at 2, which
        // matches A's tombstone, but the caller quotes B's object id.
        let err = service.invalidate(7, 1, "/k", 1, b).unwrap_err();
        assert!(format!("{}", err).contains("identity mismatch"), "{}", err);
        assert!(
            service.location_retained(b),
            "forged invalidate must not clear another object's locations"
        );
        assert!(
            service
                .state
                .lock()
                .unwrap()
                .plans
                .contains_key(&token(6, 1)),
            "forged invalidate must not clear another object's plan"
        );

        // Positive control: quoting the row's real object id resolves
        // AlreadyApplied and drops A's own state.
        service
            .install_locations(OBJ, &layout(OBJ, 64), full_locations(&layout(OBJ, 64)))
            .unwrap();
        assert_eq!(
            service.invalidate(7, 1, "/k", 1, OBJ).unwrap(),
            CacheOpStatus::AlreadyApplied
        );
        assert!(service.gc_has_work(OBJ), "A's retire enqueues GC work");
        // B is untouched by A's terminal resolution.
        assert!(service.location_retained(b));
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
            .install_locations(OBJ, &layout(OBJ, 64), full_locations(&layout(OBJ, 64)))
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
        assert!(
            service.gc_has_work(OBJ),
            "verified-identity Superseded retire enqueues GC work"
        );

        // Same fence quoting a different object id: still terminal
        // Superseded (the row advanced regardless), but the OTHER object's
        // volatile state survives.
        let b = OBJ + 51;
        let b_lay = layout(b, 64);
        service
            .install_locations(b, &b_lay, full_locations(&b_lay))
            .unwrap();
        assert_eq!(
            service.invalidate(7, 1, "/k", 1, b).unwrap(),
            CacheOpStatus::Superseded {
                expected: 2,
                current: 3
            }
        );
        assert!(
            service.location_retained(b),
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
        let service = build_service("allocate-wire-cap", chooser(vec![big.clone()]));
        seed_sessions(&service, &[big.clone()]);
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
        seed_sessions(&service, &[worker(1)]);
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

    /// 4c.2 driver smoke gates: the entry-point checks (capability,
    /// leadership implied by the test leader state, scope/cursor shape,
    /// vacuum gate-3 pre-verify) and the empty-namespace fast termination
    /// (done on the first empty RAW page, nothing journaled — the raft
    /// barrier fails closed in unit tests, so any page proposal would
    /// surface as an error here).
    #[test]
    fn test_mutation_driver_gates() {
        let service = build_service("mutation-drivers", chooser(vec![worker(1)]));

        // Empty namespaces terminate immediately with nothing journaled.
        let p = service.remove_scope(7, 1, "/a", None, 4).unwrap();
        assert_eq!(
            p,
            ScopeRemoveProgress {
                done: true,
                cursor: None,
                processed: 0
            }
        );
        let t = service.sweep_ttl(7, 1000, None, 4).unwrap();
        assert!(t.done && t.processed == 0);
        let g = service.gc_outcomes(7, None, 4).unwrap();
        assert!(g.done && g.processed == 0);

        // Vacuum re-verifies the gate-3 rows before paging.
        assert!(service.vacuum_incarnation(7, 5, 1, None, 4).is_err());

        // The scope must be a non-empty prefix and the cursor in-scope.
        assert!(service.remove_scope(7, 1, "", None, 4).is_err());
        assert!(service.remove_scope(7, 1, "/a", Some("/zz"), 4).is_err());

        // Capability gate mirrors every other cache entry point.
        let disabled =
            build_service_enabled("mutation-drivers-off", chooser(vec![worker(1)]), false);
        assert!(disabled.remove_scope(7, 1, "/a", None, 4).is_err());
        assert!(disabled.sweep_ttl(7, 1000, None, 4).is_err());
        assert!(disabled.vacuum_incarnation(7, 5, 1, None, 4).is_err());
        assert!(disabled.gc_outcomes(7, None, 4).is_err());
    }

    /// Review `cbd434bd`: the scope-remove driver's external String
    /// cursor shares the 4096-byte key cap. A cap-sized in-scope cursor
    /// is accepted (empty namespace: done with nothing journaled); one
    /// byte over fails loud BEFORE any scan or scope-membership
    /// reasoning.
    #[test]
    fn test_scope_remove_cursor_byte_cap() {
        let service = build_service("scope-cursor-cap", chooser(vec![worker(1)]));
        let at_cap = format!("/a/{}", "k".repeat(MAX_KEY_BYTES - 3));
        assert_eq!(at_cap.len(), MAX_KEY_BYTES);
        let p = service
            .remove_scope(7, 1, "/a", Some(at_cap.as_str()), 4)
            .unwrap();
        assert!(p.done && p.processed == 0);

        let over_cap = format!("/a/{}", "k".repeat(MAX_KEY_BYTES - 2));
        assert_eq!(over_cap.len(), MAX_KEY_BYTES + 1);
        let err = service
            .remove_scope(7, 1, "/a", Some(over_cap.as_str()), 4)
            .unwrap_err();
        assert!(
            err.to_string().contains("cursor exceeds"),
            "expected cursor byte-cap error, got: {}",
            err
        );
    }

    /// Review `cbd434bd`: the vacuum driver's external String cursor is
    /// byte-gated before the incarnation-row verification — at-cap
    /// passes the cursor gate and then fails on the missing gate-3 row;
    /// over-cap fails on the byte cap itself.
    #[test]
    fn test_vacuum_cursor_byte_cap() {
        let service = build_service("vacuum-cursor-cap", chooser(vec![worker(1)]));
        let at_cap = format!("/a/{}", "k".repeat(MAX_KEY_BYTES - 3));
        assert_eq!(at_cap.len(), MAX_KEY_BYTES);
        let err = service
            .vacuum_incarnation(7, 5, 1, Some(at_cap.as_str()), 4)
            .unwrap_err();
        assert!(
            err.to_string().contains("no incarnation row"),
            "at-cap cursor must reach the incarnation gate, got: {}",
            err
        );

        let over_cap = format!("/a/{}", "k".repeat(MAX_KEY_BYTES - 2));
        let err = service
            .vacuum_incarnation(7, 5, 1, Some(over_cap.as_str()), 4)
            .unwrap_err();
        assert!(
            err.to_string().contains("cursor exceeds"),
            "expected cursor byte-cap error, got: {}",
            err
        );
    }

    /// Review `303fb807` P0-1: after a committed scope-remove page whose
    /// progress response was lost, the caller retries with the SAME
    /// cursor. The driver re-scans the same raw page (the tombstone row
    /// is still there), but re-derivation must yield ZERO victims — no
    /// second journal entry is proposed and the committed generation
    /// stays frozen (the manager apply stands in for the committed
    /// propose; the raft barrier itself fails closed in unit tests).
    #[test]
    fn test_scope_remove_response_loss_rederive_stable() {
        let service = build_service("scope-rederive", chooser(vec![worker(1)]));
        committed_entry(&service, token(51, 1), token(52, 1), "/a/x", OBJ, 300, 5000);

        let scan = |after: Option<&str>| -> Vec<(String, CacheEntry)> {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            rocks
                .cache_scan_entries_in_scope(1, "/a", after, 64)
                .unwrap()
        };

        // First call: page the raw scope, derive one live victim, and the
        // propose commits (apply stand-in) — then the response is lost.
        let page1 = scan(None);
        assert_eq!(page1.len(), 1);
        let victims = CacheService::scope_page_victims(&page1).unwrap();
        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0].expected_generation, 1);
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store
                .cache
                .apply_scope_remove(rocks, 1, "/a", &victims)
                .unwrap();
        }
        let applied = service
            .fs_dir
            .read()
            .get_rocks_store()
            .cache_get_entry(1, "/a/x")
            .unwrap()
            .unwrap();
        assert_eq!(
            (applied.state, applied.generation),
            (CacheEntryState::Tombstoned, 2)
        );

        // Retry with the SAME cursor: the raw page still yields the row
        // (cursor/done stay raw-page semantics), but the tombstone
        // derives no victim — nothing is journaled again and the
        // generation does not advance a second time.
        let retry_page = scan(None);
        assert_eq!(retry_page.len(), 1);
        let rederived = CacheService::scope_page_victims(&retry_page).unwrap();
        assert!(
            rederived.is_empty(),
            "response-loss re-derive must not re-journal a tombstone"
        );
        let frozen = service
            .fs_dir
            .read()
            .get_rocks_store()
            .cache_get_entry(1, "/a/x")
            .unwrap()
            .unwrap();
        assert_eq!(
            (frozen.state, frozen.generation),
            (CacheEntryState::Tombstoned, 2)
        );
    }

    // ---- 4c.3 physical GC handoff (reviews `6bc4f569` / `327b30d2` /
    // `4b2e2a72` / `618498f7`) ----

    /// Install one GC job into a bare volatile state: full geometry, and
    /// per-block worker sets only for the first `known` indexes (0/1/many
    /// block objects; unknown indexes exercise the skip-replica rule).
    fn gc_job(volatile: &mut CacheVolatile, object_id: i64, len: i64, known: usize) {
        let mut locs = ObjectLocations {
            len,
            block_size: 64,
            blocks: HashMap::new(),
        };
        for index in 1..=known {
            locs.blocks.insert(
                index as i64,
                vec![Replica {
                    worker: worker(7),
                    tag: 0,
                }],
            );
        }
        volatile.locations.insert(object_id, locs);
        volatile
            .gc
            .enqueue(CacheGcWork {
                incarnation: 1,
                object_id,
                len,
                block_size: 64,
                next_seq: 1,
            })
            .unwrap();
    }

    /// Queue-level dedup and the loud geometry divergence (gate 1): a
    /// response-loss retry with the same frozen geometry keeps the drain
    /// cursor (never restarts a drain); the same object_id with different
    /// geometry is impossible for an immutable object and fails loud.
    #[test]
    fn test_gc_queue_dedup_and_geometry_loud() {
        let mut q = CacheGcQueue::default();
        let work = CacheGcWork {
            incarnation: 1,
            object_id: OBJ,
            len: 130,
            block_size: 64,
            next_seq: 1,
        };
        q.enqueue(work).unwrap();
        // Partially drain (cursor deep into the object).
        q.items.get_mut(&OBJ).unwrap().next_seq = 200;
        // Response-loss retry: same geometry → idempotent, cursor kept.
        q.enqueue(work).unwrap();
        assert_eq!(q.order.len(), 1);
        assert_eq!(q.items[&OBJ].next_seq, 200);
        // Geometry divergence (same id, different immutable geometry):
        // loud, never a silent overwrite.
        let bad = CacheGcWork { len: 131, ..work };
        let err = q.enqueue(bad).unwrap_err();
        assert!(err.to_string().contains("geometry divergence"), "{}", err);
        // A different incarnation for the same id is the same class of
        // impossibility.
        let bad = CacheGcWork {
            incarnation: 2,
            ..work
        };
        assert!(q.enqueue(bad).is_err());
    }

    /// Round-robin extraction (review `327b30d2` item 2): every
    /// unfinished object gets at most one quantum per turn (a late small
    /// job still progresses behind earlier big ones), the global per-tick
    /// cap is honored exactly, unknown-location indexes are skipped while
    /// the cursor advances, and completion removes the work item AND the
    /// retained locations. A max-legal-layout object drains in bounded
    /// ticks without monopolizing the budget.
    #[test]
    fn test_gc_take_batch_round_robin_cap_skip() {
        let mut volatile = CacheVolatile::default();
        // A: 3 blocks (done in one turn). B/C/D/E: 266 blocks each (C has
        // locations for only the first 2 indexes). F: empty object.
        gc_job(&mut volatile, OBJ, 130, 3);
        gc_job(&mut volatile, OBJ + 1, 266 * 64, 266);
        gc_job(&mut volatile, OBJ + 2, 266 * 64, 2);
        gc_job(&mut volatile, OBJ + 3, 266 * 64, 266);
        gc_job(&mut volatile, OBJ + 4, 266 * 64, 266);
        gc_job(&mut volatile, OBJ + 5, 0, 0);

        // Tick 1: budget 1024 is consumed exactly (3 + 256 + 256 + 256 +
        // 253); E still got its turn despite being fifth — fairness.
        let batch1 = volatile.gc_take_batch().unwrap();
        // C's unknown indexes produce no (worker, block) pair but still
        // consume budget; only indexes 1..=2 of C are known.
        assert_eq!(batch1.len(), 3 + 256 + 2 + 256 + 253);
        assert!(!volatile.gc.items.contains_key(&OBJ), "small job completes");
        assert!(!volatile.locations.contains_key(&OBJ));
        assert_eq!(volatile.gc.items[&(OBJ + 1)].next_seq, 257);
        assert_eq!(volatile.gc.items[&(OBJ + 2)].next_seq, 257);
        assert_eq!(volatile.gc.items[&(OBJ + 3)].next_seq, 257);
        assert_eq!(
            volatile.gc.items[&(OBJ + 4)].next_seq,
            254,
            "the fifth job still got its partial quantum"
        );
        assert!(
            volatile.gc.items.contains_key(&(OBJ + 5)),
            "empty job waits for the next tick"
        );

        // Drain to completion: total delivered pairs are exactly the
        // known replicas (F delivers none), and everything is removed.
        let mut total = batch1.len();
        let mut ticks = 1;
        while !volatile.gc.items.is_empty() {
            let batch = volatile.gc_take_batch().unwrap();
            assert!(
                batch.len() <= GC_HANDOFF_BLOCKS_PER_TICK,
                "per-tick hard cap"
            );
            total += batch.len();
            ticks += 1;
            assert!(ticks < 20, "266-block objects must drain in a few ticks");
        }
        assert_eq!(total, 3 + 266 + 2 + 266 + 266);
        assert!(volatile.gc.order.is_empty());
        assert!(
            volatile.locations.is_empty(),
            "completion is the only location removal"
        );

        // Max legal layout (BLOCK_SEQ_MAX blocks): bounded multi-tick
        // progress, never more than one quantum per turn.
        let mut volatile = CacheVolatile::default();
        let max_len = BlockIdCodec::BLOCK_SEQ_MAX * 64;
        gc_job(&mut volatile, OBJ, max_len, 2);
        for _ in 0..3 {
            let batch = volatile.gc_take_batch().unwrap();
            assert!(batch.len() <= 2, "only the 2 known replicas emit pairs");
        }
        assert_eq!(
            volatile.gc.items[&OBJ].next_seq,
            1 + 3 * GC_HANDOFF_QUANTUM as u32,
            "bounded per-turn quanta accumulate exactly"
        );
        assert!(
            volatile.locations.contains_key(&OBJ),
            "retained until drain completes"
        );
    }

    /// The four metadata→GC handoff paths and the never-GC rules (gate 2):
    /// point-invalidate, scope-remove, TTL-sweep all retire ONLY
    /// proven-dead identities; a live same-object row is never handed to
    /// physical GC; a stale/different object id never disturbs the
    /// current object's volatile state. Manager applies stand in for the
    /// committed proposes (the raft barrier fails closed in tests); the
    /// retire loop is the drivers' shared production code.
    #[test]
    fn test_gc_handoff_paths_live_never() {
        let service = build_service("gc-paths", chooser(vec![worker(1), worker(2)]));
        mount_incarnation(&service, 1, 0);
        // 4d.2: the get() hit below requires current-tag replicas.
        seed_sessions(&service, &[worker(1), worker(2)]);

        // Point invalidate (verified post-propose arm): committed Valid
        // row + published locations, the fence applies, then classify +
        // retire exactly as the verified arm runs them.
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        let lay = layout(OBJ, 130);
        service
            .install_locations(OBJ, &lay, full_locations(&lay))
            .unwrap();
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store.cache.apply_remove(rocks, 1, "/k", 1, 2, OBJ).unwrap();
        }
        let (dead, geometry) = service.classify_dead_victim(1, "/k", OBJ).unwrap();
        assert!(dead);
        assert_eq!(geometry, None, "a tombstone zeroes len");
        service.retire_object_state(1, OBJ, geometry).unwrap();
        assert!(service.gc_has_work(OBJ));
        assert!(
            service.location_retained(OBJ),
            "retained until the drain completes"
        );

        // Scope remove: the driver's shared post-propose loop with the
        // frozen-geometry fallback.
        committed_entry(&service, token(3, 1), token(3, 2), "/a/x", OBJ + 10, 300, 0);
        let lay = layout(OBJ + 10, 300);
        service
            .install_locations(OBJ + 10, &lay, full_locations(&lay))
            .unwrap();
        let frozen = {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            let page = rocks
                .cache_scan_entries_in_scope(1, "/a", None, 64)
                .unwrap();
            let victims = CacheService::scope_page_victims(&page).unwrap();
            store
                .cache
                .apply_scope_remove(rocks, 1, "/a", &victims)
                .unwrap();
            page.iter()
                .filter(|(_, e)| e.state != CacheEntryState::Tombstoned && e.len > 0)
                .map(|(k, e)| ((k.clone(), e.object_id), (e.len, e.block_size)))
                .collect::<HashMap<(String, i64), (i64, i64)>>()
        };
        service
            .retire_dead_victims(&[(1, "/a/x".into(), OBJ + 10)], &frozen)
            .unwrap();
        assert!(service.gc_has_work(OBJ + 10));

        // TTL sweep: the due expiry row applies, then the shared loop;
        // the applied tombstone carries no geometry, so the retire falls
        // back to the commit-published one.
        committed_entry(
            &service,
            token(4, 1),
            token(4, 2),
            "/ttl",
            OBJ + 20,
            130,
            1234,
        );
        let lay = layout(OBJ + 20, 130);
        service
            .install_locations(OBJ + 20, &lay, full_locations(&lay))
            .unwrap();
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            let row = ExpiryRow {
                expire_at: 1234,
                incarnation: 1,
                object_id: OBJ + 20,
                key: "/ttl".into(),
                generation: 1,
            };
            store.cache.apply_ttl_sweep(rocks, 2000, &[row]).unwrap();
        }
        service
            .retire_dead_victims(&[(1, "/ttl".into(), OBJ + 20)], &HashMap::new())
            .unwrap();
        assert!(service.gc_has_work(OBJ + 20));

        // Live same-object row (the journaled fence did NOT apply): the
        // classify refuses, nothing is enqueued, the object stays
        // servable.
        committed_entry(
            &service,
            token(5, 1),
            token(5, 2),
            "/live",
            OBJ + 30,
            130,
            0,
        );
        let lay = layout(OBJ + 30, 130);
        service
            .install_locations(OBJ + 30, &lay, full_locations(&lay))
            .unwrap();
        let (dead, geometry) = service.classify_dead_victim(1, "/live", OBJ + 30).unwrap();
        assert!(!dead, "a live same-object row is NEVER dead");
        assert_eq!(geometry, None);
        service
            .retire_dead_victims(&[(1, "/live".into(), OBJ + 30)], &HashMap::new())
            .unwrap();
        assert!(!service.gc_has_work(OBJ + 30));
        assert!(service.location_retained(OBJ + 30));
        assert!(service.get(1, "/live", true).unwrap().is_some());

        // Stale/different object id: the victim identity is dead (the key
        // now belongs to another object), but retiring it must not touch
        // the CURRENT object's volatile state.
        let (dead, geometry) = service.classify_dead_victim(1, "/live", OBJ + 999).unwrap();
        assert!(dead);
        assert_eq!(geometry, None);
        service.retire_object_state(1, OBJ + 999, geometry).unwrap();
        assert!(!service.gc_has_work(OBJ + 999));
        assert!(service.location_retained(OBJ + 30));
        assert!(service.get(1, "/live", true).unwrap().is_some());
    }

    /// Vacuum handoff (gate 2): a revoked incarnation's rows are removed
    /// by the vacuum apply; every page row is server-derived (proven
    /// provenance), so all of them retire with the page-frozen geometry.
    #[test]
    fn test_gc_handoff_vacuum_path() {
        let service = build_service("gc-vacuum", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        let lay = layout(OBJ, 130);
        service
            .install_locations(OBJ, &lay, full_locations(&lay))
            .unwrap();
        let frozen = {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store.cache.apply_incarnation_revoke(rocks, 5, 1).unwrap();
            let page = rocks.cache_scan_entries(1, None, 64).unwrap();
            let victims: Vec<VacuumVictim> = page
                .iter()
                .map(|(k, e)| VacuumVictim {
                    key: k.clone(),
                    generation: e.generation,
                    object_id: e.object_id,
                    expire_at: e.expire_at,
                })
                .collect();
            store.cache.apply_vacuum(rocks, 1, 5, &victims).unwrap();
            page.iter()
                .filter(|(_, e)| e.len > 0)
                .map(|(k, e)| ((k.clone(), e.object_id), (e.len, e.block_size)))
                .collect::<HashMap<(String, i64), (i64, i64)>>()
        };
        service
            .retire_dead_victims(&[(1, "/k".into(), OBJ)], &frozen)
            .unwrap();
        assert!(service.gc_has_work(OBJ));
        assert!(service.location_retained(OBJ));
    }

    /// Review `4b2e2a72` P0-1 regression: a missing row proves NOTHING
    /// about which object the key owned — a point invalidate quoting a
    /// forged (live) victim id must stay terminal-metadata-only and never
    /// touch that object's volatile state.
    #[test]
    fn test_invalidate_missing_row_provenance() {
        let service = build_service("invalidate-provenance", chooser(vec![worker(1), worker(2)]));
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        let lay = layout(OBJ, 130);
        // 4d.2: the get() hit below requires current-tag replicas.
        seed_sessions(&service, &[worker(1), worker(2)]);
        service
            .install_locations(OBJ, &lay, full_locations(&lay))
            .unwrap();

        assert_eq!(
            service.invalidate(7, 1, "/missing", 1, OBJ).unwrap(),
            CacheOpStatus::Superseded {
                expected: 2,
                current: 0
            }
        );
        assert!(!service.gc_has_work(OBJ), "no provenance, no GC");
        assert!(service.location_retained(OBJ), "live locations survive");
        assert!(service.get(1, "/k", true).unwrap().is_some());
    }

    /// One worker heartbeat through the real WorkerManager chain
    /// (`heartbeat(Running)` → `BlockMap::handle_heartbeat`), returning
    /// the DeleteBlock ids carried for that worker.
    fn heartbeat_deletes(
        wm: &ArcRwLock<WorkerManager>,
        cluster_id: &str,
        worker_id: u32,
    ) -> BTreeSet<i64> {
        let cmds = {
            let mut g = wm.write();
            g.heartbeat(
                cluster_id,
                HeartbeatStatus::Running,
                worker(worker_id),
                1,
                "session".into(),
                TransferWorkerCapabilities::default(),
                "test".into(),
                0,
                vec![],
                None,
            )
            .unwrap()
        };
        let mut ids = BTreeSet::new();
        for cmd in cmds {
            let WorkerCommand::DeleteBlock(c) = cmd;
            ids.extend(c.blocks);
        }
        ids
    }

    fn block_id_set(lay: &CacheBlockLayout, from: i64, to: i64) -> BTreeSet<i64> {
        (from..=to).map(|i| lay.block_id(i).unwrap()).collect()
    }

    fn report(id: i64, len: i64) -> BlockReportInfo {
        BlockReportInfo::new(id, BlockReportStatus::Finalized, StorageType::Disk, len)
    }

    fn assert_complete_entries(entries: &[BlockReportInfo], expected: &[(i64, i64)]) {
        let mut got: BTreeSet<(i64, i64)> = entries.iter().map(|b| (b.id, b.block_size)).collect();
        let want: BTreeSet<(i64, i64)> = expected.iter().map(|(id, len)| (*id, *len)).collect();
        assert_eq!(got, want);
        for b in entries {
            assert_eq!(b.status, BlockReportStatus::Finalized);
        }
        got.clear();
    }

    /// 4d R5/R7-2: the epoch fence cold-clears every volatile map on a
    /// leadership mismatch, while `next_tag` (tag uniqueness) survives.
    #[test]
    fn test_4d_sync_epoch_cold_clear() {
        let mut volatile = CacheVolatile::default();
        // Seed state across every domain.
        volatile
            .install_session(7, "s1".to_string(), worker(7))
            .unwrap();
        volatile.locations.insert(42, ObjectLocations::default());
        volatile.plans.insert(
            token(2, 1),
            LoadPlan {
                object_id: 1,
                generation: 1,
                file_len: 1,
                block_size: 1,
                replicas: 1,
                blocks: vec![],
                epoch: 0,
                fences: vec![],
            },
        );
        volatile.by_worker.entry(7).or_default().live_insert(42, 1);
        volatile
            .gc
            .enqueue(CacheGcWork {
                incarnation: 1,
                object_id: 42,
                len: 100,
                block_size: 64,
                next_seq: 1,
            })
            .unwrap();
        let tag1 = volatile.worker_sessions.get(&7).unwrap().tag;
        assert_eq!(tag1, 1, "first real tag is 1");

        // Same epoch: no clear.
        assert!(!volatile.sync_epoch(0));
        assert!(volatile.worker_sessions.contains_key(&7));

        // Epoch change: everything cold-cleared, next_tag preserved.
        assert!(volatile.sync_epoch(5));
        assert!(volatile.worker_sessions.is_empty());
        assert!(volatile.by_worker.is_empty());
        assert!(volatile.locations.is_empty());
        assert!(volatile.plans.is_empty());
        assert!(volatile.reconcile_gens.is_empty());
        assert!(volatile.gc.items.is_empty());
        assert!(volatile.gc.order.is_empty());
        assert_eq!(volatile.epoch, 5);
        assert_eq!(volatile.next_tag, 1, "tag issuer never rewinds");

        // A fresh session after the clear gets a NEW tag, never tag 1.
        let tag2 = volatile
            .install_session(7, "s2".to_string(), worker(7))
            .unwrap();
        assert_eq!(tag2, 2);

        // Re-binding to the same epoch is idempotent.
        assert!(!volatile.sync_epoch(5));
    }

    /// 4d R9-2/R9-3: install/retire are session-exact; tags are unique
    /// per Start; retirement moves the live reverse set and bumps the
    /// reconcile generation; a foreign session's retire is a no-op.
    #[test]
    fn test_4d_session_install_retire_exact() {
        let mut volatile = CacheVolatile::default();
        volatile
            .install_session(7, "s1".to_string(), worker(7))
            .unwrap();
        volatile.by_worker.entry(7).or_default().live_insert(100, 1);
        assert_eq!(
            volatile.reconcile_gens.get(&7).copied().unwrap_or(0),
            1,
            "install bumps the reconcile generation"
        );

        // A stale retire for a foreign session must not touch state:
        // registry intact, live set unmoved, nothing retired.
        assert!(!volatile.retire_session(7, "other-session"));
        assert!(volatile.worker_sessions.contains_key(&7));
        assert!(!volatile.by_worker.get(&7).unwrap().live.is_empty());
        assert!(volatile.by_worker.get(&7).unwrap().retired.is_empty());

        // Exact retire: registry row gone, live set moved to retired,
        // generation bumped again.
        assert!(volatile.retire_session(7, "s1"));
        assert!(volatile.worker_sessions.is_empty());
        let rev = volatile.by_worker.get(&7).unwrap();
        assert!(rev.live.is_empty());
        assert_eq!(rev.retired.len(), 1);
        assert_eq!(rev.retired[0].tag, 1);
        assert!(rev.retired[0]
            .entries
            .get(&100)
            .is_some_and(|s| s.contains(&1)));
        assert_eq!(volatile.reconcile_gens.get(&7).copied().unwrap_or(0), 2);

        // Retiring an already-retired session is a no-op (registry row
        // is gone).
        assert!(!volatile.retire_session(7, "s1"));

        // The worker re-registers (new Start, tag 2), publishes again,
        // and a THIRD Start (tag 3) supersedes: the second live set
        // moves to retired as well — tags never repeat.
        volatile
            .install_session(7, "s1-again".to_string(), worker(7))
            .unwrap();
        volatile.by_worker.get_mut(&7).unwrap().live_insert(200, 2);
        let tag = volatile
            .install_session(7, "s2".to_string(), worker(7))
            .unwrap();
        assert_eq!(tag, 3);
        let rev = volatile.by_worker.get(&7).unwrap();
        assert!(rev.live.is_empty());
        assert_eq!(rev.retired.len(), 2, "second retire session recorded");
        assert_eq!(rev.retired[1].tag, 2);
        assert!(rev.retired[1]
            .entries
            .get(&200)
            .is_some_and(|s| s.contains(&2)));
    }

    /// Final review `f14fa328`: the service-level cache End/lost retire
    /// is EXACT in BOTH domains — a hit retires registry/live AND
    /// terminally invalidates the same-session accumulator (an ended
    /// session can never accumulate to Complete); a stale retire has
    /// zero side effects on either domain.
    #[test]
    fn test_4d_service_retire_exact_terminalizes_accumulator() {
        let service = build_service("retire-terminal", chooser(vec![worker(1)]));
        service.begin_cache_session(1, "s1", &worker(1)).unwrap();

        // Partial accumulation for s1.
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 3, &[report(1, 64)]),
            CacheFullReportOutcome::Partial
        ));

        // Stale retire: registry keeps s1, the accumulator keeps
        // accepting pages of s1.
        assert!(!service.retire_worker_session(1, "other"));
        let spine = service.session_spine_snapshot(1);
        assert_eq!(spine.registry.map(|(s, _)| s).as_deref(), Some("s1"));
        assert_eq!(spine.accumulator, Some(("s1".to_string(), false)));
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 3, &[report(2, 64)]),
            CacheFullReportOutcome::Partial
        ));

        // Exact retire: registry gone AND accumulator terminal; late
        // pages of the ended session are Skipped forever.
        assert!(service.retire_worker_session(1, "s1"));
        let spine = service.session_spine_snapshot(1);
        assert!(spine.registry.is_none());
        assert_eq!(spine.accumulator, Some(("s1".to_string(), true)));
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 3, &[report(3, 64)]),
            CacheFullReportOutcome::Skipped
        ));

        // A fresh Start reopens with a fresh accumulator; the ended
        // session's pages stay foreign (Skipped).
        service.begin_cache_session(1, "s2", &worker(1)).unwrap();
        let spine = service.session_spine_snapshot(1);
        assert_eq!(spine.registry.map(|(s, _)| s).as_deref(), Some("s2"));
        assert_eq!(spine.accumulator, Some(("s2".to_string(), false)));
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 3, &[report(4, 64)]),
            CacheFullReportOutcome::Skipped
        ));
    }

    /// 4d R7-5: strict single-key accumulator lifecycle — partial pages
    /// accumulate, idempotent duplicates are free, completion hands out
    /// the exact entry set exactly once.
    #[test]
    fn test_4d_accumulator_lifecycle_and_completion() {
        let service = build_service("acc-lifecycle", chooser(vec![worker(1)]));
        service.begin_cache_session(1, "s1", &worker(1)).unwrap();

        // Foreign session pages are skipped entirely.
        assert!(matches!(
            service.cache_full_report_page(1, "not-mine", 3, &[report(1, 64)]),
            CacheFullReportOutcome::Skipped
        ));

        // Page 1 of 3.
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 3, &[report(1, 64)]),
            CacheFullReportOutcome::Partial
        ));
        // Idempotent duplicate of the same (id, status, len, storage).
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 3, &[report(1, 64)]),
            CacheFullReportOutcome::Partial
        ));
        // Page 2 (with a benign re-send of page 1's block).
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 3, &[report(1, 64), report(2, 64)]),
            CacheFullReportOutcome::Partial
        ));
        // Completing page.
        match service.cache_full_report_page(1, "s1", 3, &[report(3, 32)]) {
            CacheFullReportOutcome::Complete(entries, _) => {
                assert_complete_entries(&entries, &[(1, 64), (2, 64), (3, 32)]);
            }
            _ => panic!("expected Complete"),
        }
        // The accumulator is consumed: further pages of the same
        // session are skipped (no second reconcile).
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 3, &[report(1, 64)]),
            CacheFullReportOutcome::Skipped
        ));
    }

    /// 4d `0b900a2f` fixed-point: partial -> incremental Deleted ->
    /// terminal invalidation; replaying ALL original pages still
    /// reconciles NOTHING; only a new Start (new session) reopens the
    /// accumulator and a fresh full report completes.
    #[test]
    fn test_4d_accumulator_terminal_invalidation_new_start_recovers() {
        let service = build_service("acc-terminal", chooser(vec![worker(1)]));
        service.begin_cache_session(1, "s1", &worker(1)).unwrap();

        assert!(matches!(
            service.cache_full_report_page(1, "s1", 2, &[report(1, 64)]),
            CacheFullReportOutcome::Partial
        ));
        // An incremental F/W/Deleted lands for this worker.
        service.invalidate_report_session(1);
        // Replay ALL old pages (including a fresh full 2/2 set): every
        // page is permanently cache-skipped for this session.
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 2, &[report(1, 64)]),
            CacheFullReportOutcome::Skipped
        ));
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 2, &[report(1, 64), report(2, 64)]),
            CacheFullReportOutcome::Skipped
        ));

        // A new Start opens a fresh accumulator bound to the new
        // session; the full report runs and completes normally.
        service.begin_cache_session(1, "s2", &worker(1)).unwrap();
        assert!(matches!(
            service.cache_full_report_page(1, "s2", 2, &[report(1, 64)]),
            CacheFullReportOutcome::Partial
        ));
        match service.cache_full_report_page(1, "s2", 2, &[report(2, 64)]) {
            CacheFullReportOutcome::Complete(entries, _) => {
                assert_complete_entries(&entries, &[(1, 64), (2, 64)]);
            }
            _ => panic!("expected Complete after new Start"),
        }
    }

    /// 4d R7-5 terminal conflicts: total_len divergence, conflicting
    /// duplicate, overflow past the declared total, and the configured
    /// hard cap.
    #[test]
    fn test_4d_accumulator_conflicts_and_cap() {
        // total_len divergence between pages of one session.
        let service = build_service("acc-total-conflict", chooser(vec![worker(1)]));
        service.begin_cache_session(1, "s1", &worker(1)).unwrap();
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 3, &[report(1, 64)]),
            CacheFullReportOutcome::Partial
        ));
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 2, &[report(2, 64)]),
            CacheFullReportOutcome::Skipped
        ));
        // Terminal: even the original total cannot continue.
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 3, &[report(2, 64)]),
            CacheFullReportOutcome::Skipped
        ));

        // Conflicting duplicate (same id, different len).
        let service = build_service("acc-dup-conflict", chooser(vec![worker(1)]));
        service.begin_cache_session(1, "s1", &worker(1)).unwrap();
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 2, &[report(1, 64)]),
            CacheFullReportOutcome::Partial
        ));
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 2, &[report(1, 128)]),
            CacheFullReportOutcome::Skipped
        ));
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 2, &[report(2, 64)]),
            CacheFullReportOutcome::Skipped
        ));

        // Overflow: more unique ids than the declared total.
        let service = build_service("acc-overflow", chooser(vec![worker(1)]));
        service.begin_cache_session(1, "s1", &worker(1)).unwrap();
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 1, &[report(1, 64), report(2, 64)]),
            CacheFullReportOutcome::Skipped
        ));

        // Declared total above the configured hard cap (1_000_000 in
        // the test builder): terminal immediately.
        let service = build_service("acc-cap", chooser(vec![worker(1)]));
        service.begin_cache_session(1, "s1", &worker(1)).unwrap();
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 2_000_000, &[report(1, 64)]),
            CacheFullReportOutcome::Skipped
        ));
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 2, &[report(1, 64)]),
            CacheFullReportOutcome::Skipped
        ));
    }

    /// 4d R8-3 pre-propose fence: a plan validates while its epoch and
    /// every replica (worker, tag) still hold; a worker restart (new
    /// tag) or a leadership change breaches it.
    #[test]
    fn test_4d_plan_fence_prepropose() {
        let service = build_service("fence-prepropose", chooser(vec![worker(1), worker(2)]));
        service.begin_cache_session(1, "w1-s1", &worker(1)).unwrap();
        service.begin_cache_session(2, "w2-s1", &worker(2)).unwrap();
        let lay = layout(OBJ, 130);
        let mut plan = plan_for(&lay);
        plan.epoch = service.monitor.journal_epoch();
        {
            let volatile = service.state.lock().unwrap();
            let t1 = volatile.worker_sessions.get(&1).unwrap().tag;
            let t2 = volatile.worker_sessions.get(&2).unwrap().tag;
            plan.fences = plan
                .blocks
                .iter()
                .map(|_| {
                    let mut m = HashMap::new();
                    m.insert(WorkerIdent::of(&worker(1)), t1);
                    m.insert(WorkerIdent::of(&worker(2)), t2);
                    m
                })
                .collect();
        }
        service.install_plan(token(2, 1), plan.clone());
        assert!(service.validate_plan_fences(&plan).is_ok());

        // Worker 2 restarts: new session, new tag — the old plan's
        // fence for worker 2 no longer holds.
        service.begin_cache_session(2, "w2-s2", &worker(2)).unwrap();
        let err = service.validate_plan_fences(&plan).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("worker 2"), "{}", msg);

        // Leadership change: the volatile epoch moves; the plan's epoch
        // fence is breached (and the domain cold-cleared).
        service.monitor.journal_epoch.advance();
        let err = service.validate_plan_fences(&plan).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("epoch"), "{}", msg);
    }

    /// 4d R8-3 settle re-check: across the propose barrier the fences
    /// are re-verified under the volatile guard. A breach yields a loud
    /// terminal error with ZERO location publish and NO GC merge — even
    /// against an exact-Valid row; the plan is spent (exact allocate
    /// replay re-plans with fresh fences).
    #[test]
    fn test_4d_plan_fence_settle_lost() {
        // (a) fences hold: exact-Valid row publishes normally.
        let service = build_service("fence-settle-ok", chooser(vec![worker(1), worker(2)]));
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        service.begin_cache_session(1, "w1-s1", &worker(1)).unwrap();
        service.begin_cache_session(2, "w2-s1", &worker(2)).unwrap();
        let (t1, t2) = {
            let volatile = service.state.lock().unwrap();
            (
                volatile.worker_sessions.get(&1).unwrap().tag,
                volatile.worker_sessions.get(&2).unwrap().tag,
            )
        };
        let lay = layout(OBJ, 130);
        let live_fences: Vec<HashMap<WorkerIdent, u64>> = lay
            .block_ids()
            .map(|_| {
                let mut m = HashMap::new();
                m.insert(WorkerIdent::of(&worker(1)), t1);
                m.insert(WorkerIdent::of(&worker(2)), t2);
                m
            })
            .collect();
        assert_eq!(
            service
                .commit_barrier_settle(
                    &token(2, 1),
                    1,
                    "/k",
                    1,
                    OBJ,
                    130,
                    777,
                    0,
                    64,
                    service.monitor.journal_epoch(),
                    live_fences.clone(),
                    full_locations(&lay),
                )
                .unwrap(),
            CacheOpStatus::Applied
        );
        assert!(service.location_retained(OBJ));
        assert!(!service.gc_has_work(OBJ));

        // (b) worker 2 restarted mid-propose (its session tag advanced):
        // settle must fail loud, publish nothing, merge nothing, and
        // spend the plan.
        let service = build_service("fence-settle-lost", chooser(vec![worker(1), worker(2)]));
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        service.begin_cache_session(1, "w1-s1", &worker(1)).unwrap();
        service.begin_cache_session(2, "w2-s1", &worker(2)).unwrap();
        service.install_plan(token(2, 1), plan_for(&lay));
        let (t1, t2) = {
            let volatile = service.state.lock().unwrap();
            (
                volatile.worker_sessions.get(&1).unwrap().tag,
                volatile.worker_sessions.get(&2).unwrap().tag,
            )
        };
        let stale_fences: Vec<HashMap<WorkerIdent, u64>> = lay
            .block_ids()
            .map(|_| {
                let mut m = HashMap::new();
                m.insert(WorkerIdent::of(&worker(1)), t1);
                m.insert(WorkerIdent::of(&worker(2)), t2);
                m
            })
            .collect();
        // The restart lands between propose and settle.
        service.begin_cache_session(2, "w2-s2", &worker(2)).unwrap();
        let err = service
            .commit_barrier_settle(
                &token(2, 1),
                1,
                "/k",
                1,
                OBJ,
                130,
                777,
                0,
                64,
                service.monitor.journal_epoch(),
                stale_fences,
                full_locations(&lay),
            )
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("fence"), "{}", msg);
        assert!(
            !service.location_retained(OBJ),
            "zero old-location publish on fence breach"
        );
        assert!(!service.gc_has_work(OBJ), "no GC merge on fence breach");
        assert!(
            !service
                .state
                .lock()
                .unwrap()
                .plans
                .contains_key(&token(2, 1)),
            "the plan is spent (terminal for this commit)"
        );
    }

    /// 4d RC1 (gpt56 `7ceef2ff` item 1): production capture is
    /// FAIL-CLOSED — a chosen worker with no registered session, or one
    /// whose registry row was installed under a DIFFERENT full address,
    /// refuses the whole placement before any object id is issued.
    #[test]
    fn test_4d_capture_fail_closed() {
        let service = build_service("capture-fail-closed", chooser(vec![worker(1)]));
        let sets = vec![vec![worker(1)]];

        // No session at all: the legacy/pre-Start worker is refused
        // before any id is issued.
        let err = service.capture_plan_fences(&sets).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("no registered cache session"), "{}", msg);

        // Session installed under a drifted address (endpoint reuse of
        // the same worker_id): the fence capture refuses the drift.
        let mut drifted = worker(1);
        drifted.rpc_port += 1;
        service.begin_cache_session(1, "drifted", &drifted).unwrap();
        let err = service.capture_plan_fences(&sets).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("endpoint drift"), "{}", msg);

        // A proper session unblocks the capture and freezes the tag
        // under the worker's FULL identity.
        service.begin_cache_session(1, "s1", &worker(1)).unwrap();
        let (epoch, fences) = service.capture_plan_fences(&sets).unwrap();
        assert_eq!(epoch, service.monitor.journal_epoch());
        let tag = {
            let volatile = service.state.lock().unwrap();
            volatile.worker_sessions.get(&1).unwrap().tag
        };
        assert_eq!(fences[0].get(&WorkerIdent::of(&worker(1))), Some(&tag));
    }

    /// 4d RC3 (gpt56 `7ceef2ff` item 3): an EMPTY-session (legacy)
    /// Start fail-closes the cache domain — accumulator terminated,
    /// current session retired with its live set, nothing installed —
    /// and a later modern Start fully recovers.
    #[test]
    fn test_4d_empty_start_purges_cache_session() {
        let service = build_service("empty-start-purge", chooser(vec![worker(1)]));
        service.begin_cache_session(1, "s1", &worker(1)).unwrap();
        // Partial accumulator + a live reverse-set entry.
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 2, &[report(1, 64)]),
            CacheFullReportOutcome::Partial
        ));
        service
            .state
            .lock()
            .unwrap()
            .by_worker
            .entry(1)
            .or_default()
            .live_insert(OBJ, 1);

        // The legacy (empty-session) Start lands.
        service.purge_worker_cache_session(1);

        {
            let volatile = service.state.lock().unwrap();
            assert!(
                volatile.worker_sessions.is_empty(),
                "no session installed for the legacy Start"
            );
            let rev = volatile.by_worker.get(&1).unwrap();
            assert!(rev.live.is_empty(), "live set retired");
            assert_eq!(rev.retired.len(), 1, "retired drain holds the old tag");
            assert_eq!(
                volatile.reconcile_gens.get(&1).copied().unwrap_or(0),
                2,
                "reconcile generation bumped by install + purge"
            );
        }
        // The old session's pages stay permanently cache-skipped.
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 2, &[report(2, 64)]),
            CacheFullReportOutcome::Skipped
        ));

        // A modern Start reopens the domain and the full report
        // completes normally.
        service.begin_cache_session(1, "s2", &worker(1)).unwrap();
        assert!(matches!(
            service.cache_full_report_page(1, "s2", 2, &[report(1, 64)]),
            CacheFullReportOutcome::Partial
        ));
        match service.cache_full_report_page(1, "s2", 2, &[report(2, 64)]) {
            CacheFullReportOutcome::Complete(entries, _) => {
                assert_complete_entries(&entries, &[(1, 64), (2, 64)]);
            }
            _ => panic!("expected Complete after the modern Start"),
        }
    }

    /// 4d RC4 (gpt56 `7ceef2ff` item 4): fence tags are matched by
    /// FULL-endpoint identity, not position — subset and reordered
    /// evidence validate correctly (the old positional pairing would
    /// have checked worker A's tag against worker B); an unplanned
    /// worker identity, a restarted chosen worker, or registry address
    /// drift breaches.
    #[test]
    fn test_4d_plan_fence_evidence_identity() {
        let service = build_service("fence-identity", chooser(vec![worker(1), worker(2)]));
        service.begin_cache_session(1, "w1-s1", &worker(1)).unwrap();
        service.begin_cache_session(2, "w2-s1", &worker(2)).unwrap();
        let lay = layout(OBJ, 130);
        let (t1, t2) = {
            let volatile = service.state.lock().unwrap();
            (
                volatile.worker_sessions.get(&1).unwrap().tag,
                volatile.worker_sessions.get(&2).unwrap().tag,
            )
        };
        assert_ne!(t1, t2);
        let freeze = |plan: &mut LoadPlan| {
            plan.fences = plan
                .blocks
                .iter()
                .map(|_| {
                    let mut m = HashMap::new();
                    m.insert(WorkerIdent::of(&worker(1)), t1);
                    m.insert(WorkerIdent::of(&worker(2)), t2);
                    m
                })
                .collect();
        };

        // Subset evidence (only worker 2 replicated): holds.
        let mut subset = plan_for(&lay);
        freeze(&mut subset);
        for block in &mut subset.blocks {
            block.workers = vec![worker(2)];
        }
        assert!(service.validate_plan_fences(&subset).is_ok());

        // Reordered evidence ([w2, w1] instead of [w1, w2]): holds —
        // the old positional check would have paired t1 with worker 2.
        let mut reordered = plan_for(&lay);
        freeze(&mut reordered);
        for block in &mut reordered.blocks {
            block.workers = vec![worker(2), worker(1)];
        }
        assert!(service.validate_plan_fences(&reordered).is_ok());

        // An unplanned worker identity is a breach even though its
        // worker_id carries a live session.
        let mut unplanned = plan_for(&lay);
        freeze(&mut unplanned);
        for block in &mut unplanned.blocks {
            block.workers = vec![worker(3)];
        }
        let err = service.validate_plan_fences(&unplanned).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("not a fenced"), "{}", msg);

        // One chosen worker restarts: subset/reordered evidence naming
        // it breaches; evidence naming only the survivor still holds.
        service.begin_cache_session(2, "w2-s2", &worker(2)).unwrap();
        let err = service.validate_plan_fences(&subset).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("worker 2"), "{}", msg);
        let mut survivor = plan_for(&lay);
        freeze(&mut survivor);
        for block in &mut survivor.blocks {
            block.workers = vec![worker(1)];
        }
        assert!(service.validate_plan_fences(&survivor).is_ok());

        // Registry address drift (same worker_id re-registered at a
        // different endpoint) breaches the identity match.
        let mut drifted = worker(1);
        drifted.rpc_port += 1;
        service.begin_cache_session(1, "w1-s2", &drifted).unwrap();
        let err = service.validate_plan_fences(&survivor).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("worker 1"), "{}", msg);
    }

    /// 4d RC5 (gpt56 `7ceef2ff` item 5): the tag issuer is loud
    /// fail-closed at u64 exhaustion — never a wrapped (reused) tag.
    #[test]
    fn test_4d_tag_issuer_exhaustion_is_loud() {
        let mut volatile = CacheVolatile {
            next_tag: u64::MAX,
            ..Default::default()
        };
        let err = volatile
            .install_session(7, "s1".to_string(), worker(7))
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("exhausted"), "{}", msg);
        // Nothing was installed: the registry stays empty and the
        // issuer did not move (no wrap).
        assert!(volatile.worker_sessions.is_empty());
        assert_eq!(volatile.next_tag, u64::MAX);
    }

    /// Review `4b2e2a72` P0-2 + `6bc4f569` gate 4: the commit settlement.
    /// A load invalidated between propose and readback resolves terminal
    /// Superseded, its validated block evidence goes to GC (never a late
    /// publish), and the real heartbeat chain delivers the exact
    /// DeleteBlock ids to every replica worker.
    #[test]
    fn test_commit_settle_dead_handoff_race() {
        // (a) propose→readback race: the racing invalidate's tombstone is
        // already committed when the settlement reads the row.
        let service = build_service("settle-race", chooser(vec![worker(1), worker(2)]));
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store.cache.apply_remove(rocks, 1, "/k", 1, 2, OBJ).unwrap();
        }
        let lay = layout(OBJ, 130);
        assert_eq!(
            service
                .commit_barrier_settle(
                    &token(2, 1),
                    1,
                    "/k",
                    1,
                    OBJ,
                    130,
                    777,
                    0,
                    64,
                    0,
                    Vec::new(),
                    full_locations(&lay),
                )
                .unwrap(),
            CacheOpStatus::Superseded {
                expected: 1,
                current: 2
            }
        );
        assert!(!service
            .state
            .lock()
            .unwrap()
            .plans
            .contains_key(&token(2, 1)));
        assert!(service.gc_has_work(OBJ));
        assert!(service.location_retained(OBJ));

        // Full drain through the production tick + the real heartbeat
        // chain: each replica worker receives exactly the object's 3
        // block ids, and the completion clears the retained locations.
        let wm: ArcRwLock<WorkerManager> =
            ArcRwLock::new(WorkerManager::new(&ClusterConf::default()).unwrap());
        let cluster_id = wm.read().cluster_id.clone();
        service.gc_handoff_tick(&wm);
        let expected = block_id_set(&lay, 1, 3);
        assert_eq!(heartbeat_deletes(&wm, &cluster_id, 1), expected);
        assert_eq!(heartbeat_deletes(&wm, &cluster_id, 2), expected);
        assert!(!service.gc_has_work(OBJ), "3 blocks drain in one tick");
        assert!(!service.location_retained(OBJ));
    }

    /// Gate 4 determinism: the fenced-between-readback-and-publish race
    /// lands on the `publish_hook` seam; the fenced branch (a revoked
    /// namespace) wins even over an exact-Valid row (review `618498f7`);
    /// the exact-Valid live path publishes; an exact-identity field
    /// divergence stays loud.
    #[test]
    fn test_commit_settle_publish_hook_fence_and_divergence() {
        // (b) readback→publish race via the publish hook: the settlement
        // re-checks INSIDE the volatile lock and never publishes a row
        // the hook just tombstoned.
        let service = Arc::new(build_service(
            "settle-publish-hook",
            chooser(vec![worker(1), worker(2)]),
        ));
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        let hook_service = service.clone();
        service.set_publish_hook(Box::new(move || {
            let store = hook_service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store.cache.apply_remove(rocks, 1, "/k", 1, 2, OBJ).unwrap();
        }));
        let lay = layout(OBJ, 130);
        assert_eq!(
            service
                .commit_barrier_settle(
                    &token(2, 1),
                    1,
                    "/k",
                    1,
                    OBJ,
                    130,
                    777,
                    0,
                    64,
                    0,
                    Vec::new(),
                    full_locations(&lay),
                )
                .unwrap(),
            CacheOpStatus::Superseded {
                expected: 1,
                current: 2
            }
        );
        assert!(service.gc_has_work(OBJ), "raced evidence goes to GC");
        assert!(service.location_retained(OBJ));
        // The merged evidence drains to exactly the commit's workers.
        let batch = service.state.lock().unwrap().gc_take_batch().unwrap();
        let expected = block_id_set(&lay, 1, 3);
        assert_eq!(
            batch.iter().map(|&(w, b)| (w, b)).collect::<BTreeSet<_>>(),
            [(1u32, 0i64); 0]
                .into_iter()
                .chain(
                    expected
                        .iter()
                        .flat_map(|&b| [(1, b), (2, b)])
                        .collect::<Vec<_>>()
                )
                .collect::<BTreeSet<_>>()
        );
        assert!(!service.gc_has_work(OBJ));
        assert!(!service.location_retained(OBJ));

        // (c) fenced namespace beats an exact-Valid row (reviews
        // `618498f7` + `4dd264df` P0-1): the revoke commits via the
        // publish hook — deterministically between the barrier and the
        // settlement's single authoritative snapshot — while the entry
        // row stays exact-Valid. The snapshot reads namespace + entry
        // under ONE fs_dir guard, so the revoke cannot slip between the
        // two reads; the load is fenced and the evidence goes to GC.
        let service = Arc::new(build_service("settle-fenced", chooser(vec![worker(1)])));
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        let hook_service = service.clone();
        service.set_publish_hook(Box::new(move || {
            let store = hook_service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store.cache.apply_incarnation_revoke(rocks, 5, 1).unwrap();
        }));
        let lay = layout(OBJ, 130);
        let err = service
            .commit_barrier_settle(
                &token(2, 1),
                1,
                "/k",
                1,
                OBJ,
                130,
                777,
                0,
                64,
                0,
                Vec::new(),
                full_locations(&lay),
            )
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("terminal") && msg.contains("incarnation 1"),
            "{}",
            msg
        );
        assert!(
            service.gc_has_work(OBJ),
            "fenced load's evidence still goes to GC"
        );
        assert!(service.location_retained(OBJ));

        // (d) exact-Valid live row: publish, whole-object hit, no GC.
        // 4d.2: publish records each replica's session tag from the plan
        // fences, so the workers are sessioned and real fences are frozen
        // (tag-0 replicas would never be served by the current-tag get).
        let service = build_service("settle-applied", chooser(vec![worker(1), worker(2)]));
        mount_incarnation(&service, 1, 0);
        seed_sessions(&service, &[worker(1), worker(2)]);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        let lay = layout(OBJ, 130);
        let sets: Vec<Vec<WorkerAddress>> = full_locations(&lay)
            .into_iter()
            .map(|b| b.workers)
            .collect();
        let (_, fences) = service.capture_plan_fences(&sets).unwrap();
        assert_eq!(
            service
                .commit_barrier_settle(
                    &token(2, 1),
                    1,
                    "/k",
                    1,
                    OBJ,
                    130,
                    777,
                    0,
                    64,
                    0,
                    fences,
                    full_locations(&lay),
                )
                .unwrap(),
            CacheOpStatus::Applied
        );
        assert!(!service.gc_has_work(OBJ));
        assert!(service.location_retained(OBJ));
        assert!(service.get(1, "/k", true).unwrap().is_some());

        // (e) exact identity, divergent immutable field: loud divergence,
        // never a silent classification.
        let err = service
            .commit_barrier_settle(
                &token(2, 1),
                1,
                "/k",
                1,
                OBJ,
                131,
                777,
                0,
                64,
                0,
                Vec::new(),
                full_locations(&layout(OBJ, 131)),
            )
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("divergence"), "{}", msg);

        // (f) active + same-object Reserved@generation (review
        // `4dd264df` P0-2): our apply did not land and nothing fenced
        // the entry — NOT proven dead. Loud barrier failure, the plan is
        // retained for the exact allocate retry, and GC never sees the
        // object (a re-plan on the same object_id must not be
        // counter-deleted).
        let service = build_service("settle-reserved", chooser(vec![worker(1)]));
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
        service.install_plan(token(9, 1), plan_for(&lay));
        let err = service
            .commit_barrier_settle(
                &token(9, 1),
                1,
                "/k",
                1,
                OBJ,
                130,
                777,
                0,
                64,
                0,
                Vec::new(),
                full_locations(&lay),
            )
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("barrier readback failed"),
            "live Reserved is loud, not dead: {}",
            msg
        );
        assert!(
            service
                .state
                .lock()
                .unwrap()
                .plans
                .contains_key(&token(9, 1)),
            "plan retained for the exact retry"
        );
        assert!(!service.gc_has_work(OBJ), "no GC for a live Reserved load");
        assert!(!service.location_retained(OBJ), "no evidence merge");
    }

    /// Production heartbeat progress (review `327b30d2` item 1 +
    /// heartbeat-progress gate): two jobs of very different sizes drain
    /// through consecutive real heartbeats — the small job completes in
    /// the first tick alongside the large job's first quantum (fairness),
    /// the second tick finishes the large job, and every replica worker's
    /// DeleteBlock ids are exact.
    #[test]
    fn test_gc_handoff_tick_heartbeat_fairness() {
        let service = build_service("gc-heartbeat", chooser(vec![worker(1), worker(2)]));
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/small", OBJ, 130, 0);
        committed_entry(
            &service,
            token(3, 1),
            token(3, 2),
            "/large",
            OBJ + 1,
            300 * 64,
            0,
        );
        for (obj, len) in [(OBJ, 130i64), (OBJ + 1, 300 * 64)] {
            let lay = layout(obj, len);
            service
                .install_locations(obj, &lay, full_locations(&lay))
                .unwrap();
            service
                .retire_object_state(1, obj, Some((len, 64)))
                .unwrap();
        }

        let wm: ArcRwLock<WorkerManager> =
            ArcRwLock::new(WorkerManager::new(&ClusterConf::default()).unwrap());
        let cluster_id = wm.read().cluster_id.clone();
        let small = layout(OBJ, 130);
        let large = layout(OBJ + 1, 300 * 64);

        // Heartbeat 1: the small job's 3 blocks complete AND the large
        // job gets its first 256 — no starvation behind the big object.
        service.gc_handoff_tick(&wm);
        let got = heartbeat_deletes(&wm, &cluster_id, 1);
        assert_eq!(got.len(), 3 + 256);
        for id in block_id_set(&small, 1, 3) {
            assert!(got.contains(&id), "small job must progress in tick 1");
        }
        for id in block_id_set(&large, 1, 256) {
            assert!(got.contains(&id));
        }
        assert!(!got.contains(&large.block_id(257).unwrap()));
        // Ack worker 1's deletes like a block report would.
        {
            let mut g = wm.write();
            for id in &got {
                g.deleted_block(1, *id);
            }
        }
        assert!(!service.gc_has_work(OBJ), "small job done after tick 1");
        assert!(!service.location_retained(OBJ));

        // Heartbeat 2: the large job's remaining 44 blocks finish it, on
        // both replica workers.
        service.gc_handoff_tick(&wm);
        let tail = heartbeat_deletes(&wm, &cluster_id, 1);
        assert_eq!(tail, block_id_set(&large, 257, 300));
        let mut worker2_expected = block_id_set(&small, 1, 3);
        worker2_expected.extend(block_id_set(&large, 1, 300));
        let worker2_got = heartbeat_deletes(&wm, &cluster_id, 2);
        assert_eq!(
            worker2_got, worker2_expected,
            "worker 2 receives both ticks' replica sets on its first beat"
        );
        assert!(!service.gc_has_work(OBJ + 1));
        assert!(!service.location_retained(OBJ + 1));

        // Nothing left: ack both workers like block reports, then a
        // further tick + heartbeat carries no command.
        {
            let mut g = wm.write();
            for id in &tail {
                g.deleted_block(1, *id);
            }
            for id in &worker2_got {
                g.deleted_block(2, *id);
            }
        }
        service.gc_handoff_tick(&wm);
        assert!(heartbeat_deletes(&wm, &cluster_id, 1).is_empty());
    }

    /// Follower tick no-op (review `327b30d2` item 1): a non-leader's
    /// tick never enqueues deletes; the work survives for the leader.
    #[test]
    fn test_gc_tick_follower_noop() {
        let service = build_service("gc-follower", chooser(vec![worker(1)]));
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        let lay = layout(OBJ, 130);
        service
            .install_locations(OBJ, &lay, full_locations(&lay))
            .unwrap();
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store.cache.apply_remove(rocks, 1, "/k", 1, 2, OBJ).unwrap();
        }
        service.retire_object_state(1, OBJ, None).unwrap();
        assert!(service.gc_has_work(OBJ));

        let wm: ArcRwLock<WorkerManager> =
            ArcRwLock::new(WorkerManager::new(&ClusterConf::default()).unwrap());
        let cluster_id = wm.read().cluster_id.clone();
        service.monitor.journal_ctl.set_state(RoleState::Follower);
        service.gc_handoff_tick(&wm);
        assert!(
            heartbeat_deletes(&wm, &cluster_id, 1).is_empty(),
            "follower tick must not enqueue deletes"
        );
        assert!(service.gc_has_work(OBJ), "work survives for the leader");

        service.monitor.journal_ctl.set_state(RoleState::Leader);
        service.gc_handoff_tick(&wm);
        assert_eq!(
            heartbeat_deletes(&wm, &cluster_id, 1),
            block_id_set(&lay, 1, 3)
        );
    }

    // ---- 4d.2 incremental routing + tagged locations/reverse ----

    fn report_as(id: i64, len: i64, status: BlockReportStatus) -> BlockReportInfo {
        BlockReportInfo::new(id, status, StorageType::Disk, len)
    }

    /// 4d.2 R1 classification matrix + R3 exact-length checks, driven
    /// through `incr_block_report` under a real seeded session.
    #[test]
    fn test_4d2_incr_classification_and_length() {
        let service = build_service("4d2-classify", chooser(vec![worker(1), worker(2)]));
        seed_sessions(&service, &[worker(1), worker(2)]);

        // Valid: 150 bytes -> seq1 64, seq2 64, seq3 22.
        committed_entry(&service, token(2, 1), token(2, 2), "/valid", OBJ, 150, 0);
        // Reserved: allocate applied, commit not. The id segment
        // [OBJ, OBJ+100) was already reserved by `committed_entry`'s
        // token(1,1), so this entry only applies the allocate.
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            let mgr = &store.cache;
            let alloc = CacheEntry {
                generation: 1,
                state: CacheEntryState::Reserved,
                object_id: OBJ + 1,
                len: 0,
                ufs_mtime: 0,
                block_size: 64,
                expire_at: 0,
            };
            mgr.apply_allocate(rocks, token(3, 2), 1, "/reserved", 130, &alloc)
                .unwrap();
        }
        // Tombstoned: committed then removed.
        committed_entry(&service, token(4, 1), token(4, 2), "/dead", OBJ + 2, 130, 0);
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            store
                .cache
                .apply_remove(rocks, 1, "/dead", 1, 2, OBJ + 2)
                .unwrap();
        }

        let lay = layout(OBJ, 150);
        let b1 = lay.block_id(1).unwrap();
        let b2 = lay.block_id(2).unwrap();
        let b3 = lay.block_id(3).unwrap();
        let outcome = service
            .incr_block_report(
                1,
                "seed-1",
                &[
                    // Valid x Finalized x exact length -> publish.
                    report(b1, 64),
                    // Valid x Finalized on the tail with its exact 22 -> publish.
                    report(b3, 22),
                    // Valid x Writing -> defer.
                    report_as(b2, 64, BlockReportStatus::Writing),
                    // Reserved x Finalized -> defer.
                    report_as(
                        BlockIdCodec::encode_block_id(OBJ + 1, 1).unwrap(),
                        64,
                        BlockReportStatus::Finalized,
                    ),
                    // Tombstoned -> orphan.
                    report_as(
                        BlockIdCodec::encode_block_id(OBJ + 2, 1).unwrap(),
                        64,
                        BlockReportStatus::Finalized,
                    ),
                    // No object row -> orphan.
                    report_as(
                        BlockIdCodec::encode_block_id(OBJ + 50, 1).unwrap(),
                        64,
                        BlockReportStatus::Finalized,
                    ),
                    // Out-of-layout seqs (0 and past the block count) -> orphan.
                    report_as(
                        BlockIdCodec::encode_block_id(OBJ, 0).unwrap(),
                        64,
                        BlockReportStatus::Finalized,
                    ),
                    report_as(
                        BlockIdCodec::encode_block_id(OBJ, 9).unwrap(),
                        64,
                        BlockReportStatus::Finalized,
                    ),
                ],
            )
            .unwrap();

        let orphans: BTreeSet<i64> = outcome.remove_blocks.into_iter().collect();
        let expected_orphans: BTreeSet<i64> = [
            BlockIdCodec::encode_block_id(OBJ + 2, 1).unwrap(),
            BlockIdCodec::encode_block_id(OBJ + 50, 1).unwrap(),
            BlockIdCodec::encode_block_id(OBJ, 0).unwrap(),
            BlockIdCodec::encode_block_id(OBJ, 9).unwrap(),
        ]
        .into_iter()
        .collect();
        assert_eq!(orphans, expected_orphans);
        assert!(outcome.deleted_acks.is_empty());

        // Published exactly seq1 and seq3 for worker 1 under the current
        // session tag; seq2 deferred.
        {
            let volatile = service.state.lock().unwrap();
            let locs = volatile.locations.get(&OBJ).unwrap();
            assert_eq!(locs.len, 150);
            assert_eq!(locs.block_size, 64);
            assert_eq!(locs.blocks.len(), 2);
            let tag = volatile.worker_sessions[&1].tag;
            for seq in [1, 3] {
                let replicas = locs.blocks.get(&i64::from(seq)).unwrap();
                assert_eq!(replicas.len(), 1);
                assert_eq!(replicas[0].worker.worker_id, 1);
                assert_eq!(replicas[0].tag, tag);
            }
            let live = &volatile.by_worker[&1].live;
            assert!(
                (live
                    .get(&OBJ)
                    .is_some_and(|s| s.contains(&1) && s.contains(&3)))
            );
            assert!(!live.get(&OBJ).is_some_and(|s| s.contains(&2)));
            // Reconcile generation bumped past the install bump.
            assert!(*volatile.reconcile_gens.get(&1).unwrap() >= 2);
        }

        // Idempotent re-report of an already-published replica: no
        // duplicate row, no new live entry, no deletion.
        let again = service
            .incr_block_report(1, "seed-1", &[report(b1, 64)])
            .unwrap();
        assert!(again.remove_blocks.is_empty());
        {
            let volatile = service.state.lock().unwrap();
            assert_eq!(volatile.locations[&OBJ].blocks[&1].len(), 1);
        }

        // R3 corrupt page AFTER the exact publishes: short/oversize/
        // head/tail matrix on duplicate ids. The page fold keeps the
        // conservative terminal state — every dup id folds to orphan
        // (never back to publish), the published replicas are stripped
        // from the read path IMMEDIATELY (before any physical-delete
        // ack), and the identities are quarantined for this session.
        // (The whole-object get was already a miss here: seq2 never
        // published — Writing defers.)
        let corrupt = service
            .incr_block_report(
                1,
                "seed-1",
                &[
                    report_as(b1, 63, BlockReportStatus::Finalized),
                    report_as(b2, 0, BlockReportStatus::Finalized),
                    report_as(b3, 64, BlockReportStatus::Finalized),
                ],
            )
            .unwrap();
        let corrupt_orphans: BTreeSet<i64> = corrupt.remove_blocks.into_iter().collect();
        assert_eq!(
            corrupt_orphans,
            [b1, b2, b3].into_iter().collect::<BTreeSet<i64>>()
        );
        assert!(service.get(1, "/valid", true).unwrap().is_none());
        {
            let volatile = service.state.lock().unwrap();
            assert!(volatile.locations[&OBJ].blocks.is_empty());
            assert!(volatile.by_worker[&1].live.is_empty());
            let tag = volatile.worker_sessions[&1].tag;
            let quarantined = volatile.quarantine.get(&OBJ).unwrap();
            for seq in [1i64, 2, 3] {
                assert!(quarantined.get(&(1, tag)).is_some_and(|s| s.contains(&seq)));
            }
        }

        // Guard-first accumulator terminalization: the same-session
        // full-report accumulator is terminally invalid (late pages of
        // the session stay Skipped).
        let spine = service.session_spine_snapshot(1);
        assert_eq!(spine.accumulator, Some(("seed-1".to_string(), true)));
        match service.cache_full_report_page(1, "seed-1", 1, &[report(b1, 64)]) {
            CacheFullReportOutcome::Skipped => {}
            other => panic!(
                "late page of incremented session must be Skipped: {:?}",
                other
            ),
        }

        // Stale/foreign session: zero side effects on every domain.
        let gen_before = *service
            .state
            .lock()
            .unwrap()
            .reconcile_gens
            .get(&1)
            .unwrap();
        let stale = service
            .incr_block_report(1, "not-mine", &[report(b2, 64)])
            .unwrap();
        assert!(stale.remove_blocks.is_empty() && stale.deleted_acks.is_empty());
        assert_eq!(
            *service
                .state
                .lock()
                .unwrap()
                .reconcile_gens
                .get(&1)
                .unwrap(),
            gen_before
        );
        assert!(
            !service.state.lock().unwrap().locations[&OBJ]
                .blocks
                .contains_key(&2),
            "stale-session report must not publish"
        );

        // Empty (legacy) session: total no-op.
        let legacy = service.incr_block_report(1, "", &[report(b2, 64)]).unwrap();
        assert!(legacy.remove_blocks.is_empty() && legacy.deleted_acks.is_empty());
    }

    /// 4d.2 Deleted routing: volatile replica removal + BlockMap ack,
    /// never the inode chain; whole-object miss once the last replica
    /// of a block is gone.
    #[test]
    fn test_4d2_incr_deleted_routing() {
        let service = build_service("4d2-deleted", chooser(vec![worker(1), worker(2)]));
        seed_sessions(&service, &[worker(1), worker(2)]);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        let lay = layout(OBJ, 130);
        let b1 = lay.block_id(1).unwrap();

        // Publish seq1 on both workers via incremental Finalized.
        let outcome = service
            .incr_block_report(1, "seed-1", &[report(b1, 64)])
            .unwrap();
        assert!(outcome.remove_blocks.is_empty());
        let outcome = service
            .incr_block_report(2, "seed-2", &[report(b1, 64)])
            .unwrap();
        assert!(outcome.remove_blocks.is_empty());
        assert!(
            service.get(1, "/k", true).unwrap().is_none(),
            "only seq1 of 3 published"
        );

        // Worker 1 deletes its replica: row removed for worker 1 only,
        // worker 2's replica survives, an ack is returned.
        let outcome = service
            .incr_block_report(1, "seed-1", &[BlockReportInfo::with_deleted(b1, 64)])
            .unwrap();
        assert!(outcome.remove_blocks.is_empty());
        assert_eq!(outcome.deleted_acks, vec![b1]);
        {
            let volatile = service.state.lock().unwrap();
            let replicas = volatile.locations[&OBJ].blocks[&1].clone();
            assert_eq!(replicas.len(), 1);
            assert_eq!(replicas[0].worker.worker_id, 2);
            assert!(!volatile.by_worker[&1].live_contains(OBJ, 1));
        }

        // Idempotent re-Delete: no error, ack again (BlockMap's own ack
        // is idempotent), row state unchanged.
        let outcome = service
            .incr_block_report(1, "seed-1", &[BlockReportInfo::with_deleted(b1, 64)])
            .unwrap();
        assert_eq!(outcome.deleted_acks, vec![b1]);
        assert_eq!(
            service.state.lock().unwrap().locations[&OBJ].blocks[&1].len(),
            1
        );

        // Worker 2 also deletes: the block's replica set empties and the
        // row is dropped.
        let outcome = service
            .incr_block_report(2, "seed-2", &[BlockReportInfo::with_deleted(b1, 64)])
            .unwrap();
        assert_eq!(outcome.deleted_acks, vec![b1]);
        assert!(
            !service.state.lock().unwrap().locations[&OBJ]
                .blocks
                .contains_key(&1),
            "empty replica set removes the block row"
        );
    }

    // ---- 4d.3 full-report reconcile ----

    /// 4d.3 deterministic race matrix (gpt56 `8a9e5261` point 4): the
    /// FULL_RECONCILE_SEAM fires between the fence capture (phase A)
    /// and the final recheck guard (phase B). A Start (new session +
    /// tag + gen bump), an exact lost/End retire, an incremental
    /// F/W/Deleted (gen bump), or an epoch flip (cold clear) in that
    /// window makes the ENTIRE reconcile a no-op — nothing a
    /// superseded snapshot decided is applied (revival forbidden). The
    /// control branch (no race) applies the exact replace.
    #[test]
    fn test_4d3_reconcile_fence_race_matrix() {
        // One branch = one isolated service, worker 1 holding b1+b2
        // under its current session; the reconciling snapshot reports
        // ONLY b1, so an APPLIED reconcile strips b2.
        let branch = |name: &str, hook: Box<dyn Fn(&CacheService) + Send + Sync>| {
            let service = Arc::new(build_service(name, chooser(vec![worker(1), worker(2)])));
            seed_sessions(&service, &[worker(1), worker(2)]);
            committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
            let lay = layout(OBJ, 130);
            let b1 = lay.block_id(1).unwrap();
            let b2 = lay.block_id(2).unwrap();
            service
                .incr_block_report(1, "seed-1", &[report(b1, 64), report(b2, 64)])
                .unwrap();
            // The incr seed terminalized the accumulator row; model the
            // fresh row a new Start installs before the full report,
            // then its production checkout (Reconciling + ticket).
            service.reset_accumulator_for_full_test(1);
            let ticket = service.checkout_ticket_for_full_test(1).unwrap();
            {
                let s = service.clone();
                *FULL_RECONCILE_SEAM.lock().unwrap() = Some(Box::new(move || hook(&s)));
            }
            let outcome = service
                .reconcile_cache_full_report(1, "seed-1", ticket, &[report(b1, 64)])
                .unwrap();
            *FULL_RECONCILE_SEAM.lock().unwrap() = None;
            (outcome, service)
        };
        let assert_noop = |outcome: &crate::cache::cache_service::CacheIncrOutcome| {
            assert_eq!(outcome.session_tag, 0, "fence failed: default outcome");
            assert!(outcome.remove_blocks.is_empty() && outcome.deleted_acks.is_empty());
        };

        // Control (no race): the missing identity b2 is stripped from
        // both the locations row and the live reverse trace; the
        // reported b1 stays published.
        {
            let service = build_service("4d3-control", chooser(vec![worker(1), worker(2)]));
            seed_sessions(&service, &[worker(1), worker(2)]);
            committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
            let lay = layout(OBJ, 130);
            let b1 = lay.block_id(1).unwrap();
            let b2 = lay.block_id(2).unwrap();
            service
                .incr_block_report(1, "seed-1", &[report(b1, 64), report(b2, 64)])
                .unwrap();
            service.reset_accumulator_for_full_test(1);
            let ticket = service.checkout_ticket_for_full_test(1).unwrap();
            let outcome = service
                .reconcile_cache_full_report(1, "seed-1", ticket, &[report(b1, 64)])
                .unwrap();
            assert_ne!(outcome.session_tag, 0);
            assert!(outcome.remove_blocks.is_empty() && outcome.deleted_acks.is_empty());
            let volatile = service.state.lock().unwrap();
            let tag = volatile.worker_sessions[&1].tag;
            assert!(!volatile.locations[&OBJ].blocks.contains_key(&2));
            assert!(!volatile.by_worker[&1].live_contains(OBJ, 2));
            assert!(volatile.by_worker[&1].live_contains(OBJ, 1));
            assert!(volatile.locations[&OBJ].blocks[&1]
                .iter()
                .any(|r| r.worker.worker_id == 1 && r.tag == tag));
        }

        // Start race: the new session swaps tag + bumps gen. The raced
        // reconcile no-ops; install's retire moved the live set into
        // the retired drain (NOT a strip), and the old-tag b2 replica
        // row survives untouched.
        let (outcome, service) = branch(
            "4d3-race-start",
            Box::new(|s| {
                s.begin_cache_session(1, "s2", &worker(1)).unwrap();
            }),
        );
        assert_noop(&outcome);
        {
            let volatile = service.state.lock().unwrap();
            assert!(
                volatile.by_worker[&1].live.is_empty(),
                "retired, not stripped"
            );
            let tag_a = tag_a_of(&volatile, 1);
            assert!(
                volatile.locations[&OBJ].blocks[&2]
                    .iter()
                    .any(|r| r.worker.worker_id == 1 && r.tag == tag_a),
                "raced reconcile must not strip the missing identity"
            );
        }

        // Lost race: the exact retire removes the registry row; the
        // reconcile's phase-B session gate fails. Same shape as Start.
        let (outcome, service) = branch(
            "4d3-race-lost",
            Box::new(|s| {
                assert!(s.retire_worker_session(1, "seed-1"));
            }),
        );
        assert_noop(&outcome);
        {
            let volatile = service.state.lock().unwrap();
            assert!(
                volatile.by_worker[&1].live.is_empty(),
                "retired, not stripped"
            );
            let tag_a = tag_a_of(&volatile, 1);
            assert!(volatile.locations[&OBJ].blocks[&2]
                .iter()
                .any(|r| r.worker.worker_id == 1 && r.tag == tag_a));
        }

        // Incremental race: a same-session incremental bumps the
        // reconcile generation (and terminalizes the accumulator). The
        // reconcile's gen recheck fails; b2 stays live + published.
        let (outcome, service) = branch(
            "4d3-race-incr",
            Box::new(|s| {
                let lay = layout(OBJ, 130);
                let b1 = lay.block_id(1).unwrap();
                s.incr_block_report(
                    1,
                    "seed-1",
                    &[report_as(b1, 64, BlockReportStatus::Writing)],
                )
                .unwrap();
            }),
        );
        assert_noop(&outcome);
        {
            let volatile = service.state.lock().unwrap();
            let tag = volatile.worker_sessions[&1].tag;
            assert!(volatile.by_worker[&1].live_contains(OBJ, 2));
            assert!(volatile.locations[&OBJ].blocks[&2]
                .iter()
                .any(|r| r.worker.worker_id == 1 && r.tag == tag));
        }

        // Epoch race: the leadership epoch moves in the window; phase
        // B's lock_volatile cold-clears the whole volatile domain
        // (registry gone) and the fence fails as a session miss.
        let (outcome, service) = branch(
            "4d3-race-epoch",
            Box::new(|s| {
                s.monitor.journal_epoch.advance();
            }),
        );
        assert_noop(&outcome);
        {
            let volatile = service.state.lock().unwrap();
            assert!(volatile.worker_sessions.is_empty(), "cold clear ran");
            assert!(volatile.locations.is_empty());
        }
    }

    /// Helper for the race matrix: the FIRST retired tag of a worker
    /// (the pre-restart session's tag), i.e. the tag its old replica
    /// rows still carry after a Start/lost retire.
    fn tag_a_of(volatile: &CacheVolatile, worker_id: u32) -> u64 {
        volatile.by_worker[&worker_id]
            .retired
            .front()
            .map(|r| r.tag)
            .unwrap_or(0)
    }

    /// 4d.3 current-tag exact replace (gpt56 `8a9e5261` point 2): a
    /// snapshot reporting b1 (already published under the current tag),
    /// a Deleted b2 (an old-tag retired replica of this worker), and an
    /// unknown-object orphan — while b3 (published under the CURRENT
    /// tag) is MISSING from the snapshot:
    /// - the missing current-tag b3 replica + reverse trace is stripped;
    /// - the Deleted removes the worker's ANY-tag b2 replica and acks;
    /// - the orphan is quarantined (exact worker+tag) and scheduled;
    /// - the OTHER worker's replicas/live and the retired drain record
    ///   are untouched.
    #[test]
    fn test_4d3_reconcile_exact_replace_matrix() {
        let service = build_service("4d3-exact-replace", chooser(vec![worker(1), worker(2)]));
        seed_sessions(&service, &[worker(1), worker(2)]);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        let lay = layout(OBJ, 130);
        let b1 = lay.block_id(1).unwrap();
        let b2 = lay.block_id(2).unwrap();
        let b3 = lay.block_id(3).unwrap();
        let orphan = BlockIdCodec::encode_block_id(OBJ + 50, 1).unwrap();

        // Session A: worker 1 holds b1+b2, worker 2 holds b1.
        service
            .incr_block_report(1, "seed-1", &[report(b1, 64), report(b2, 64)])
            .unwrap();
        service
            .incr_block_report(2, "seed-2", &[report(b1, 64)])
            .unwrap();

        // Worker 1 restarts (session B): A's live set retires; the b1/b2
        // location rows keep tag A. Under B the worker re-publishes b1
        // (re-tagged) and b3.
        service.begin_cache_session(1, "B", &worker(1)).unwrap();
        let tag_b = service.cache_session_tag(1).unwrap();
        service
            .incr_block_report(1, "B", &[report(b1, 64), report(b3, 2)])
            .unwrap();
        // The incr seed terminalized B's accumulator row; model the
        // fresh row the NEXT Start installs before the full report,
        // then its production checkout (Reconciling + ticket).
        service.reset_accumulator_for_full_test(1);
        let ticket = service.checkout_ticket_for_full_test(1).unwrap();

        // The full-report snapshot: b1 reported, b2 Deleted, orphan
        // reported, b3 MISSING.
        let outcome = service
            .reconcile_cache_full_report(
                1,
                "B",
                ticket,
                &[
                    report(b1, 64),
                    BlockReportInfo::with_deleted(b2, 64),
                    report(orphan, 64),
                ],
            )
            .unwrap();
        assert_eq!(outcome.session_tag, tag_b);
        assert_eq!(outcome.remove_blocks, vec![orphan]);
        assert_eq!(outcome.deleted_acks, vec![b2]);

        {
            let volatile = service.state.lock().unwrap();
            // seq1: worker 1 (tag B) + worker 2 (tag seed-2) — both
            // intact, no duplicate rows.
            assert_eq!(volatile.locations[&OBJ].blocks[&1].len(), 2);
            // seq2: the worker's old-tag (retired) replica removed by
            // the Deleted; no other holder — row gone.
            assert!(!volatile.locations[&OBJ].blocks.contains_key(&2));
            // seq3: the MISSING current-tag replica stripped.
            assert!(!volatile.locations[&OBJ].blocks.contains_key(&3));
            // Reverse traces: worker 1 keeps only the reported b1;
            // worker 2 untouched.
            assert!(!volatile.by_worker[&1].live_contains(OBJ, 2));
            assert!(!volatile.by_worker[&1].live_contains(OBJ, 3));
            assert!(volatile.by_worker[&1].live_contains(OBJ, 1));
            assert!(volatile.by_worker[&2].live_contains(OBJ, 1));
            // The retired drain record for tag A is untouched (its
            // reclamation is the bounded drain's job, never the
            // reconcile's).
            let rev = &volatile.by_worker[&1];
            let tag_a = rev.retired.front().map(|r| r.tag).unwrap();
            assert_ne!(tag_a, tag_b);
            assert!(rev
                .retired
                .front()
                .unwrap()
                .entries
                .get(&OBJ)
                .is_some_and(|s| s.contains(&1) && s.contains(&2)));
            // The orphan is quarantined under the exact (worker, tag)
            // and indexed for the directed purge.
            assert!(volatile
                .quarantine
                .get(&(OBJ + 50))
                .and_then(|row| row.get(&(1, tag_b)))
                .is_some_and(|s| s.contains(&1)));
            assert!(volatile
                .quarantine_index
                .get(&1)
                .and_then(|m| m.get(&tag_b))
                .is_some_and(|objs| objs.contains(&(OBJ + 50))));
        }
    }

    /// RC2 P0-1 (gpt56 `53516250` window 1): the atomic final fence. A
    /// same-session incremental that has ALREADY terminalized the
    /// accumulator row (invalid written) pauses in the
    /// INCR_TERMINALIZE_SEAM — the exact production window the
    /// guard-hold closes ("invalid written, generation not yet
    /// bumped") — while a competing reconcile for the OLD checked-out
    /// snapshot runs on another thread. The incremental holds the
    /// accumulator map lock across its volatile section, so the
    /// reconcile can only complete AFTER the whole incremental won:
    /// its old snapshot must produce ZERO volatile mutation (the
    /// incremental's item defers, so b1 AND b2 both stay live +
    /// published — any strip is attributable to the raced reconcile),
    /// and the row stays terminal.
    #[test]
    fn test_4d3_rc2_atomic_fence_incr_terminalize_wins() {
        let service = Arc::new(build_service(
            "4d3-rc2-fence",
            chooser(vec![worker(1), worker(2)]),
        ));
        seed_sessions(&service, &[worker(1), worker(2)]);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        let lay = layout(OBJ, 130);
        let b1 = lay.block_id(1).unwrap();
        let b2 = lay.block_id(2).unwrap();
        // Worker 1 holds b1+b2 live under its current session tag.
        service
            .incr_block_report(1, "seed-1", &[report(b1, 64), report(b2, 64)])
            .unwrap();
        // Model the fresh row a new Start installs, then its production
        // checkout: the OLD snapshot reports ONLY b1 (an applied
        // reconcile would strip b2).
        service.reset_accumulator_for_full_test(1);
        let ticket = service.checkout_ticket_for_full_test(1).unwrap();
        let tag = service.cache_session_tag(1).unwrap();

        // The competing reconcile runs on ANOTHER thread (the paused
        // incremental holds the accumulator map lock — a same-thread
        // call would self-deadlock, which is exactly the serialization
        // under test).
        let (tx, rx) = std::sync::mpsc::channel();
        {
            let s = service.clone();
            *INCR_TERMINALIZE_SEAM.lock().unwrap() = Some(Box::new(move || {
                let s2 = s.clone();
                let tx2 = tx.clone();
                std::thread::spawn(move || {
                    let out = s2
                        .reconcile_cache_full_report(1, "seed-1", ticket, &[report(b1, 64)])
                        .unwrap();
                    let _ = tx2.send(out);
                });
            }));
        }
        // Same-session incremental whose item classifies as DEFER
        // (Valid × Writing): terminalizes the row and bumps the
        // generation while making ZERO volatile mutations of its own —
        // so ANY mutation below is attributable to the raced reconcile.
        let outcome = service
            .incr_block_report(
                1,
                "seed-1",
                &[report_as(b2, 64, BlockReportStatus::Writing)],
            )
            .unwrap();
        assert!(outcome.remove_blocks.is_empty() && outcome.deleted_acks.is_empty());
        *INCR_TERMINALIZE_SEAM.lock().unwrap() = None;

        // The competing old-snapshot reconcile completed AFTER the
        // winner: default outcome, zero volatile mutation of its own.
        let raced = rx.recv_timeout(std::time::Duration::from_secs(30)).unwrap();
        assert_eq!(raced.session_tag, 0, "old snapshot dropped by the fence");
        assert!(raced.remove_blocks.is_empty() && raced.deleted_acks.is_empty());
        {
            let volatile = service.state.lock().unwrap();
            // The old snapshot's exact-strip never ran: BOTH b1 and b2
            // stay live and published under the current tag.
            assert!(volatile.by_worker[&1].live_contains(OBJ, 1));
            assert!(volatile.by_worker[&1].live_contains(OBJ, 2));
            assert!(volatile.locations[&OBJ].blocks[&2]
                .iter()
                .any(|r| r.worker.worker_id == 1 && r.tag == tag));
            // The winner quarantined nothing; the raced reconcile
            // stripped nothing.
            assert!(!volatile.quarantine.contains_key(&OBJ));
        }
        // The row stayed terminal through the whole flight.
        assert_eq!(
            service.session_spine_snapshot(1).accumulator,
            Some(("seed-1".to_string(), true))
        );
    }

    /// 4d.3 snapshot lifecycle / RC1 P0-1 (gpt56 `d2546338` item 1) /
    /// RC2 P0-1 (gpt56 `53516250` window 2): the checkout transitions
    /// the row IN PLACE to Reconciling(ticket) — never a remove — so a
    /// mid-flight terminalization still reaches the row; the finish is
    /// an exact `(session, tag, attempt)` CAS back to Accumulating; a
    /// terminal row is never resurrected (`0b900a2f`); an absent row is
    /// never blindly inserted — only a new Start opens one; and a
    /// same-wire-session Start RETRY (fresh tag, fresh row) is never
    /// consumable or releasable by the OLD tag's ticket.
    #[test]
    fn test_4d3_snapshot_take_release_and_terminal() {
        let service = build_service("4d3-take", chooser(vec![worker(1)]));
        service.begin_cache_session(1, "s1", &worker(1)).unwrap();
        let tag = service.cache_session_tag(1).unwrap();

        // A mixed worker's partial accumulation (2 of declared 5).
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 5, &[report(700, 64)]),
            CacheFullReportOutcome::Partial
        ));
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 5, &[report(701, 64)]),
            CacheFullReportOutcome::Partial
        ));
        let (snap, ticket) = service.take_cache_full_snapshot(1, "s1", tag).unwrap();
        assert_complete_entries(&snap, &[(700, 64), (701, 64)]);
        assert_eq!(ticket.tag, tag);
        assert_eq!(ticket.attempt, 1);

        // In flight (Reconciling): late same-session full pages are
        // Skipped, and a SECOND checkout of the same row is None.
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 5, &[report(702, 64)]),
            CacheFullReportOutcome::Skipped
        ));
        assert!(service.take_cache_full_snapshot(1, "s1", tag).is_none());

        // A WRONG attempt is not a release (attempt CAS fails).
        service.release_full_accumulator(1, "s1", tag, ticket.attempt + 1);
        assert!(service.take_cache_full_snapshot(1, "s1", tag).is_none());
        // A foreign session is not a release either.
        service.release_full_accumulator(1, "other", tag, ticket.attempt);
        assert!(service.take_cache_full_snapshot(1, "s1", tag).is_none());

        // The exact CAS returns the row to Accumulating, fresh.
        service.release_full_accumulator(1, "s1", tag, ticket.attempt);
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 5, &[report(702, 64)]),
            CacheFullReportOutcome::Partial
        ));

        // A foreign session's take is None and leaves the row intact.
        assert!(service.take_cache_full_snapshot(1, "other", tag).is_none());
        assert_eq!(
            service.session_spine_snapshot(1).accumulator,
            Some(("s1".to_string(), false))
        );

        // An incremental terminalizes the row MID-FLIGHT: take -> None;
        // even the exact-attempt release must NOT clear the terminal
        // flag (`0b900a2f`); the session's pages stay Skipped.
        let (_, ticket2) = service.take_cache_full_snapshot(1, "s1", tag).unwrap();
        assert_eq!(ticket2.attempt, 2);
        service.invalidate_report_session(1);
        assert!(service.take_cache_full_snapshot(1, "s1", tag).is_none());
        service.release_full_accumulator(1, "s1", tag, ticket2.attempt);
        assert_eq!(
            service.session_spine_snapshot(1).accumulator,
            Some(("s1".to_string(), true))
        );
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 5, &[report(703, 64)]),
            CacheFullReportOutcome::Skipped
        ));

        // A same-wire-session Start RETRY (RC2 P0-1): the fresh row has
        // a NEW tag — the OLD tag's take is None, the old ticket's
        // release never touches the fresh row, and the fresh row
        // accumulates normally under its own tag.
        service.begin_cache_session(1, "s1", &worker(1)).unwrap();
        let tag_retry = service.cache_session_tag(1).unwrap();
        assert_ne!(tag_retry, tag);
        assert!(service.take_cache_full_snapshot(1, "s1", tag).is_none());
        service.release_full_accumulator(1, "s1", tag, 1);
        assert_eq!(
            service.session_spine_snapshot(1).accumulator,
            Some(("s1".to_string(), false)),
            "old ticket's release did not touch the retried row"
        );
        assert!(matches!(
            service.cache_full_report_page(1, "s1", 5, &[report(704, 64)]),
            CacheFullReportOutcome::Partial
        ));
        let (_, ticket3) = service
            .take_cache_full_snapshot(1, "s1", tag_retry)
            .unwrap();
        assert_eq!(ticket3.tag, tag_retry);

        // An absent row (worker with no cache page this report) is
        // also None, and release is a NO-OP on it — no blind insert;
        // only a new Start opens the worker's accumulator.
        assert!(service.take_cache_full_snapshot(2, "s2", 0).is_none());
        service.release_full_accumulator(2, "s2", 0, 1);
        assert!(service.session_spine_snapshot(2).accumulator.is_none());
        assert!(matches!(
            service.cache_full_report_page(2, "s2", 1, &[report(800, 64)]),
            CacheFullReportOutcome::Skipped
        ));
        service.begin_cache_session(2, "s2", &worker(2)).unwrap();
        let tag2 = service.cache_session_tag(2).unwrap();
        assert!(matches!(
            service.cache_full_report_page(2, "s2", 1, &[report(800, 64)]),
            CacheFullReportOutcome::Complete(..)
        ));
        // The self-Complete ticket is exact on the retried Start
        // identity too.
        let tag2_next = service.cache_session_tag(2).unwrap();
        assert_eq!(tag2, tag2_next);
    }

    /// 4d.3 page permutation/duplicate equivalence: the same report
    /// content split into different page orders (with an idempotent
    /// duplicate folded in) accumulates to the same Complete snapshot
    /// and reconciles to the same published state.
    #[test]
    fn test_4d3_page_permutation_equivalence() {
        let lay = layout(OBJ, 130);
        let b1 = lay.block_id(1).unwrap();
        let b2 = lay.block_id(2).unwrap();
        let b3 = lay.block_id(3).unwrap();
        let run = |name: &str, pages: Vec<Vec<BlockReportInfo>>| {
            let service = build_service(name, chooser(vec![worker(1)]));
            service.begin_cache_session(1, "s1", &worker(1)).unwrap();
            committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
            let mut complete = None;
            for page in pages {
                if let CacheFullReportOutcome::Complete(entries, ticket) =
                    service.cache_full_report_page(1, "s1", 3, &page)
                {
                    complete = Some((entries, ticket));
                }
            }
            let (entries, ticket) = complete.expect("report completed");
            let outcome = service
                .reconcile_cache_full_report(1, "s1", ticket, &entries)
                .unwrap();
            (service, outcome)
        };

        let (svc_a, out_a) = run(
            "4d3-perm-a",
            vec![vec![report(b1, 64)], vec![report(b2, 64), report(b3, 2)]],
        );
        let (svc_b, out_b) = run(
            "4d3-perm-b",
            vec![
                vec![report(b3, 2), report(b2, 64), report(b1, 64)],
                vec![report(b2, 64)],
            ],
        );
        assert_eq!(out_a.remove_blocks, out_b.remove_blocks);
        assert_eq!(out_a.deleted_acks, out_b.deleted_acks);
        assert_eq!(out_a.session_tag, out_b.session_tag);
        for svc in [&svc_a, &svc_b] {
            let hit = svc.get(1, "/k", true).unwrap().expect("all published");
            assert_eq!(hit.blocks.len(), 3);
            assert_eq!(hit.blocks[2].block_len, 2);
            let volatile = svc.state.lock().unwrap();
            let tag = volatile.worker_sessions[&1].tag;
            for seq in [1i64, 2, 3] {
                assert_eq!(volatile.locations[&OBJ].blocks[&seq].len(), 1);
                assert_eq!(volatile.locations[&OBJ].blocks[&seq][0].tag, tag);
            }
        }
    }

    /// 4d.2 current-tag read semantics + exact End/lost retire into the
    /// retired drain + re-publish re-tagging under a new session.
    #[test]
    fn test_4d2_current_tag_end_retire_and_republish() {
        let service = build_service("4d2-endtag", chooser(vec![worker(1)]));
        // `full_locations` puts two replicas per block (workers 1 and 2),
        // and the production settle now fail-closes on unregistered
        // sessions, so both must be seeded before fence capture.
        seed_sessions(&service, &[worker(1), worker(2)]);
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        let lay = layout(OBJ, 130); // 64, 64, 2

        // Real publish through the settle path: replicas carry the plan
        // fence tag and the reverse live set is fed.
        let sets: Vec<Vec<WorkerAddress>> = full_locations(&lay)
            .into_iter()
            .map(|b| b.workers)
            .collect();
        let (_, fences) = service.capture_plan_fences(&sets).unwrap();
        assert_eq!(
            service
                .commit_barrier_settle(
                    &token(2, 1),
                    1,
                    "/k",
                    1,
                    OBJ,
                    130,
                    777,
                    0,
                    64,
                    0,
                    fences,
                    full_locations(&lay),
                )
                .unwrap(),
            CacheOpStatus::Applied
        );
        assert!(service.get(1, "/k", true).unwrap().is_some());
        {
            let volatile = service.state.lock().unwrap();
            let live = &volatile.by_worker[&1].live;
            let seqs = live.get(&OBJ).unwrap();
            for seq in 1..=3 {
                assert!(seqs.contains(&seq));
            }
        }

        // GC drains ALL retained replicas — including ones the current
        // read path would filter.
        {
            let mut volatile = service.state.lock().unwrap();
            volatile
                .gc
                .enqueue(CacheGcWork {
                    incarnation: 1,
                    object_id: OBJ,
                    len: 130,
                    block_size: 64,
                    next_seq: 1,
                })
                .unwrap();
            let batch = volatile.gc_take_batch().unwrap();
            let ids: BTreeSet<i64> = batch.iter().map(|&(_, b)| b).collect();
            assert_eq!(ids, block_id_set(&lay, 1, 3));
            // Not completed in one tick (quantum 256 > 3 -> completed):
            // the completion removed the locations entry; re-install for
            // the retire phase below.
            assert!(!volatile.locations.contains_key(&OBJ));
        }
        let sets: Vec<Vec<WorkerAddress>> = full_locations(&lay)
            .into_iter()
            .map(|b| b.workers)
            .collect();
        let (_, fences) = service.capture_plan_fences(&sets).unwrap();
        assert_eq!(
            service
                .commit_barrier_settle(
                    &token(2, 1),
                    1,
                    "/k",
                    1,
                    OBJ,
                    130,
                    777,
                    0,
                    64,
                    0,
                    fences,
                    full_locations(&lay),
                )
                .unwrap(),
            CacheOpStatus::Applied
        );

        // Exact End for worker 1: its registry row retires and its live
        // set moves to the retired queue; worker 2's replica still serves
        // the read, until worker 2's own exact End retires it too.
        assert!(service.retire_worker_session(1, "seed-1"));
        {
            let volatile = service.state.lock().unwrap();
            assert!(!volatile.worker_sessions.contains_key(&1));
            let rev = volatile.by_worker.get(&1).unwrap();
            assert!(rev.live.is_empty());
            assert_eq!(rev.retired.len(), 1);
            assert_eq!(rev.retired[0].entries_total(), 3);
        }
        assert!(service.get(1, "/k", true).unwrap().is_some());
        assert!(service.retire_worker_session(2, "seed-2"));
        assert!(service.get(1, "/k", true).unwrap().is_none());
        // Repeated (stale) Ends are no-ops.
        assert!(!service.retire_worker_session(1, "seed-1"));
        assert!(!service.retire_worker_session(2, "seed-2"));

        // New session (new tag): old-tag replicas stay unpublished, but
        // an incremental Finalized re-report re-tags them — the read
        // path serves the object again.
        service.begin_cache_session(1, "fresh", &worker(1)).unwrap();
        assert!(
            service.get(1, "/k", true).unwrap().is_none(),
            "old-tag replicas are not served under the new session"
        );
        let outcome = service
            .incr_block_report(
                1,
                "fresh",
                &[
                    report(lay.block_id(1).unwrap(), 64),
                    report(lay.block_id(2).unwrap(), 64),
                    report(lay.block_id(3).unwrap(), 2),
                ],
            )
            .unwrap();
        assert!(outcome.remove_blocks.is_empty());
        let hit = service.get(1, "/k", true).unwrap().expect("re-tagged hit");
        assert_eq!(hit.blocks.len(), 3);

        // The retired-session drain removes ONLY the old-tag rows: the
        // re-tagged replicas survive and keep serving.
        let removed = {
            let mut volatile = service.state.lock().unwrap();
            volatile.drain_retired()
        };
        assert!(removed >= 3);
        assert!(service.get(1, "/k", true).unwrap().is_some());
        {
            let volatile = service.state.lock().unwrap();
            assert!(volatile.by_worker[&1].retired.is_empty());
            for seq in 1..=3 {
                assert_eq!(volatile.locations[&OBJ].blocks[&seq].len(), 1);
            }
        }
    }

    /// 4d.2 retired-drain bounded cap + same-key new-tag survival.
    #[test]
    fn test_4d2_retired_drain_cap_and_tag_survival() {
        let mut volatile = CacheVolatile::default();
        let total = RETIRED_DRAIN_PER_TICK + 5;
        let mut locs = ObjectLocations {
            len: 64 * total as i64,
            block_size: 64,
            blocks: HashMap::new(),
        };
        let mut entries: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
        for seq in 1..=total {
            locs.blocks.insert(
                seq as i64,
                vec![Replica {
                    worker: worker(7),
                    tag: 7,
                }],
            );
            entries.entry(OBJ).or_default().insert(seq as i64);
        }
        volatile.locations.insert(OBJ, locs);
        volatile.by_worker.insert(
            7,
            WorkerRev {
                live: BTreeMap::new(),
                retired: VecDeque::from([RetiredSession { tag: 7, entries }]),
            },
        );
        // Production enqueues via `retire_live`; this hand-built state
        // must register the pending-retired round-robin itself.
        volatile.retired_rr.push_back(7);
        volatile.retired_rr_set.insert(7);

        // First drain: exactly the cap, record stays at the front.
        assert_eq!(volatile.drain_retired(), RETIRED_DRAIN_PER_TICK);
        assert_eq!(volatile.by_worker[&7].retired.len(), 1);
        assert_eq!(
            volatile.by_worker[&7].retired[0].entries_total(),
            5,
            "record partially drained stays queued"
        );
        {
            // Drained identities lost their only (old-tag) replica and the
            // block rows are gone; the undrained 5 remain (HashSet order
            // is arbitrary, so assert by count).
            assert_eq!(volatile.locations[&OBJ].blocks.len(), 5);
        }

        // A new-session replica published for one STILL-QUEUED identity
        // shares that block row with the old-tag replica; the remaining
        // drain must remove only the old-tag one (R7-3 conditional).
        let still_queued = *volatile.locations[&OBJ].blocks.keys().next().unwrap();
        volatile
            .locations
            .get_mut(&OBJ)
            .unwrap()
            .blocks
            .get_mut(&still_queued)
            .unwrap()
            .push(Replica {
                worker: worker(7),
                tag: 8,
            });

        assert_eq!(volatile.drain_retired(), 5);
        assert!(volatile.by_worker[&7].retired.is_empty());
        {
            let locs = &volatile.locations[&OBJ];
            // Only the new-tag row survives — the drain removed its old-tag
            // sibling from the same block and every other identity.
            assert_eq!(locs.blocks.len(), 1);
            let row = &locs.blocks[&still_queued];
            assert_eq!(row.len(), 1);
            assert_eq!((row[0].worker.worker_id, row[0].tag), (7, 8));
        }
    }

    /// 4d.2 RC1 (gpt56 `f549118c` item 1): the R1 exact mapping must
    /// include OBJECT IDENTITY. A same-key, same-generation entry bound
    /// to a different object than the reported block id decodes to is a
    /// divergent mapping — orphan, never a publish.
    #[test]
    fn test_4d2_rc1_divergent_object_same_generation() {
        let service = build_service("4d2-rc1", chooser(vec![worker(1)]));
        seed_sessions(&service, &[worker(1)]);
        // A real committed entry for OBJ+1 under "/div" (generation 1).
        committed_entry(&service, token(2, 1), token(2, 2), "/div", OBJ + 1, 150, 0);

        // Divergent hand-written ObjectRow: OBJ claims the SAME key,
        // incarnation, and generation — only the entry's object_id
        // (OBJ+1) betrays the mismatch.
        {
            let store = service.fs_dir.read();
            let rocks = store.get_rocks_store();
            let row = crate::master::meta::cache::entry::ObjectRow {
                incarnation: 1,
                key: "/div".into(),
                generation: 1,
            };
            let mut w = rocks.cache_write();
            w.put_object(OBJ, &row).unwrap();
        }

        let lay = layout(OBJ, 150);
        let b1 = lay.block_id(1).unwrap();
        let outcome = service
            .incr_block_report(1, "seed-1", &[report(b1, 64)])
            .unwrap();
        assert_eq!(
            outcome.remove_blocks,
            vec![b1],
            "divergent object mapping is an orphan"
        );
        assert!(!service.state.lock().unwrap().locations.contains_key(&OBJ));

        // Control: the entry's OWN object (OBJ+1) with the same
        // generation publishes — the row/entry were otherwise exact.
        let control = BlockIdCodec::encode_block_id(OBJ + 1, 1).unwrap();
        let outcome = service
            .incr_block_report(1, "seed-1", &[report(control, 64)])
            .unwrap();
        assert!(outcome.remove_blocks.is_empty());
        assert!(service
            .state
            .lock()
            .unwrap()
            .locations
            .contains_key(&(OBJ + 1)));
    }

    /// 4d.2 RC2 (gpt56 `f549118c` item 2 + `7cc7295c`): page-fold
    /// conservative terminal state in BOTH duplicate orders, immediate
    /// location/reverse strip on orphan, and the session-tag quarantine
    /// release matrix (new session / Deleted ack).
    #[test]
    fn test_4d2_rc2_fold_strip_and_quarantine() {
        let service = build_service("4d2-rc2", chooser(vec![worker(1)]));
        seed_sessions(&service, &[worker(1)]);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        let lay = layout(OBJ, 130); // 64 / 64 / 2
        let b1 = lay.block_id(1).unwrap();
        let b2 = lay.block_id(2).unwrap();
        let b3 = lay.block_id(3).unwrap();

        // Duplicate order 1: exact THEN corrupt folds to orphan — the
        // identity is in the delete set and NOT published.
        let out = service
            .incr_block_report(
                1,
                "seed-1",
                &[
                    report(b1, 64),
                    report_as(b1, 63, BlockReportStatus::Finalized),
                ],
            )
            .unwrap();
        assert_eq!(out.remove_blocks, vec![b1]);
        {
            let volatile = service.state.lock().unwrap();
            assert!(!volatile.locations.contains_key(&OBJ));
            assert!(volatile
                .by_worker
                .get(&1)
                .is_none_or(|rev| rev.live.is_empty()));
        }

        // Duplicate order 2: corrupt THEN exact folds to the SAME
        // terminal state — order independence.
        let out = service
            .incr_block_report(
                1,
                "seed-1",
                &[
                    report_as(b2, 0, BlockReportStatus::Finalized),
                    report(b2, 64),
                ],
            )
            .unwrap();
        assert_eq!(out.remove_blocks, vec![b2]);
        assert!(!service.state.lock().unwrap().locations.contains_key(&OBJ));

        // Published hit -> corrupt report -> IMMEDIATE miss (the strip
        // happens in the volatile domain, before any physical ack),
        // exercised on a fresh object outside the quarantine under test.
        committed_entry(&service, token(5, 1), token(5, 2), "/k2", OBJ + 5, 130, 0);
        let lay2 = layout(OBJ + 5, 130);
        let c1 = lay2.block_id(1).unwrap();
        let c2 = lay2.block_id(2).unwrap();
        let c3 = lay2.block_id(3).unwrap();
        service
            .incr_block_report(
                1,
                "seed-1",
                &[report(c1, 64), report(c2, 64), report(c3, 2)],
            )
            .unwrap();
        assert!(service.get(1, "/k2", true).unwrap().is_some());
        let out = service
            .incr_block_report(
                1,
                "seed-1",
                &[report_as(c2, 0, BlockReportStatus::Finalized)],
            )
            .unwrap();
        assert_eq!(out.remove_blocks, vec![c2]);
        assert!(service.get(1, "/k2", true).unwrap().is_none());

        // Quarantine b3 on the primary object too (for the release
        // matrix below).
        let out = service
            .incr_block_report(
                1,
                "seed-1",
                &[report_as(b3, 64, BlockReportStatus::Finalized)],
            )
            .unwrap();
        assert_eq!(out.remove_blocks, vec![b3]);
        assert!(service.get(1, "/k", true).unwrap().is_none());

        // Delete-pending no-resurrection: same-session Finalized
        // re-reports of the quarantined identities all defer.
        for (id, len) in [(b1, 64), (b2, 64), (b3, 2)] {
            let out = service
                .incr_block_report(1, "seed-1", &[report(id, len)])
                .unwrap();
            assert!(
                out.remove_blocks.is_empty(),
                "quarantined {} must defer",
                id
            );
        }
        assert!(service.get(1, "/k", true).unwrap().is_none());
        {
            let volatile = service.state.lock().unwrap();
            let live = &volatile.by_worker[&1].live;
            for seq in 1..=3i64 {
                assert!(
                    !live.get(&OBJ).is_some_and(|s| s.contains(&seq)),
                    "stripped {} must not be live",
                    seq
                );
            }
        }

        // Deleted-ack release: a Deleted report acks the physical
        // delete, the handler-side ack releases the quarantine, and a
        // subsequent same-session Finalized may publish again.
        let out = service
            .incr_block_report(1, "seed-1", &[BlockReportInfo::with_deleted(b1, 64)])
            .unwrap();
        assert_eq!(out.deleted_acks, vec![b1]);
        service.ack_cache_deleted(1, out.session_tag, b1);
        let out = service
            .incr_block_report(1, "seed-1", &[report(b1, 64)])
            .unwrap();
        assert!(out.remove_blocks.is_empty());
        {
            // b1's replica row is back for the current tag (per-identity
            // release; b2/b3 stay quarantined so the whole object still
            // misses).
            let volatile = service.state.lock().unwrap();
            assert!(volatile.by_worker[&1].live_contains(OBJ, 1));
        }
        assert!(service.get(1, "/k", true).unwrap().is_none());

        // New-session release: old-tag quarantine is purged wholesale, and
        // re-reports under the fresh tag publish again (the old-tag b1
        // replica cannot serve, so all three are re-reported).
        service.begin_cache_session(1, "fresh", &worker(1)).unwrap();
        let out = service
            .incr_block_report(1, "fresh", &[report(b1, 64), report(b2, 64), report(b3, 2)])
            .unwrap();
        assert!(out.remove_blocks.is_empty());
        let hit = service
            .get(1, "/k", true)
            .unwrap()
            .expect("fresh-tag republish hit");
        assert_eq!(hit.blocks.len(), 3);
    }

    /// 4d.2 RC3 (gpt56 `f549118c` item 3): GC completion and the
    /// no-geometry retire drop clear the reverse index (and quarantine)
    /// in the same critical section, and the retired drain is a fair
    /// bounded round-robin.
    #[test]
    fn test_4d2_rc3_reverse_cleanup_and_drain_fairness() {
        // -- Part 1: GC completion clears live/retired/quarantine. --
        let service = build_service("4d2-rc3a", chooser(vec![worker(1)]));
        seed_sessions(&service, &[worker(1)]);
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        let lay = layout(OBJ, 130);
        service
            .incr_block_report(1, "seed-1", &[report(lay.block_id(1).unwrap(), 64)])
            .unwrap();
        {
            let mut volatile = service.state.lock().unwrap();
            volatile
                .gc
                .enqueue(CacheGcWork {
                    incarnation: 1,
                    object_id: OBJ,
                    len: 130,
                    block_size: 64,
                    next_seq: 1,
                })
                .unwrap();
            let batch = volatile.gc_take_batch().unwrap();
            assert_eq!(batch.len(), 1, "quantum 256 > 3 blocks: completes");
        }
        {
            let volatile = service.state.lock().unwrap();
            assert!(!volatile.locations.contains_key(&OBJ));
            assert!(
                volatile.by_worker[&1].live.is_empty(),
                "GC completion must clear the reverse live set"
            );
            assert!(volatile.by_worker[&1].retired.is_empty());
            assert!(!volatile.quarantine.contains_key(&OBJ));
        }

        // -- Part 2: no-geometry retire drop clears quarantine. --
        let service = build_service("4d2-rc3b", chooser(vec![worker(1)]));
        seed_sessions(&service, &[worker(1)]);
        mount_incarnation(&service, 1, 0);
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, 130, 0);
        // Quarantine the only identity: publish then corrupt-strip.
        service
            .incr_block_report(1, "seed-1", &[report(lay.block_id(1).unwrap(), 64)])
            .unwrap();
        service
            .incr_block_report(
                1,
                "seed-1",
                &[report_as(
                    lay.block_id(1).unwrap(),
                    63,
                    BlockReportStatus::Finalized,
                )],
            )
            .unwrap();
        assert!(service.state.lock().unwrap().quarantine.contains_key(&OBJ));
        // Degenerate locations (blocks empty, no geometry passed) hit
        // the no-geometry branch, which must drop the quarantine row.
        service.retire_object_state(1, OBJ, None).unwrap();
        {
            let volatile = service.state.lock().unwrap();
            assert!(!volatile.locations.contains_key(&OBJ));
            assert!(!volatile.quarantine.contains_key(&OBJ));
            assert!(volatile.by_worker[&1].live.is_empty());
        }

        // -- Part 3: fair bounded round-robin drain. --
        let mut volatile = CacheVolatile::default();
        let big = RETIRED_DRAIN_PER_TICK + 5;
        let mut big_locs = ObjectLocations {
            len: 64 * big as i64,
            block_size: 64,
            blocks: HashMap::new(),
        };
        for seq in 1..=big {
            big_locs.blocks.insert(
                seq as i64,
                vec![Replica {
                    worker: worker(7),
                    tag: 7,
                }],
            );
        }
        let mut small_locs = ObjectLocations {
            len: 64 * 3,
            block_size: 64,
            blocks: HashMap::new(),
        };
        for seq in 1..=3 {
            small_locs.blocks.insert(
                seq as i64,
                vec![Replica {
                    worker: worker(8),
                    tag: 8,
                }],
            );
        }
        volatile.locations.insert(OBJ, big_locs);
        volatile.locations.insert(OBJ + 1, small_locs);
        {
            let rev = volatile.by_worker.entry(7).or_default();
            let seqs: BTreeSet<i64> = (1..=big).map(|seq| seq as i64).collect();
            rev.live.insert(OBJ, seqs);
        }
        {
            let rev = volatile.by_worker.entry(8).or_default();
            let seqs: BTreeSet<i64> = (1..=3).map(|seq| seq as i64).collect();
            rev.live.insert(OBJ + 1, seqs);
        }
        volatile.retire_live(7, 7);
        volatile.retire_live(8, 8);
        assert_eq!(volatile.retired_rr.len(), 2);

        // One tick: BOTH queues advance. The small queue finishes (3),
        // the big one drains the rest of the budget; neither starves.
        let removed = volatile.drain_retired();
        assert_eq!(removed, RETIRED_DRAIN_PER_TICK);
        assert!(
            volatile.by_worker[&8].retired.is_empty(),
            "small queue fully drains in tick 1"
        );
        assert!(!volatile.retired_rr.contains(&8));
        let big_left = volatile.by_worker[&7].retired[0].entries_total();
        assert_eq!(big_left, big - (RETIRED_DRAIN_PER_TICK - 3));
        assert!(volatile.retired_rr.contains(&7));
        // Small object's rows are all gone after its drain (the empty
        // outer locations row itself is reclaimed by GC completion).
        assert!(volatile.locations[&(OBJ + 1)].blocks.is_empty());
    }

    /// P0-2 (gpt56 `25d4b51e` item 2): a QUARANTINE-ONLY object — no
    /// locations row ever published — must lose its quarantine row at
    /// the no-geometry retire (the pre-fix locations-None early-return
    /// leaked it forever), together with its directed-index trace.
    #[test]
    fn test_4d2_p02_quarantine_only_no_geometry_drop() {
        let service = build_service("4d2-p02a", chooser(vec![worker(1)]));
        seed_sessions(&service, &[worker(1)]);
        let lay = layout(OBJ, 130);
        let b1 = lay.block_id(1).unwrap();

        // No committed entry: the report is a proven orphan and NO
        // locations row is ever created — the object is quarantine-only.
        let out = service
            .incr_block_report(1, "seed-1", &[report(b1, 64)])
            .unwrap();
        assert_eq!(out.remove_blocks, vec![b1]);
        {
            let volatile = service.state.lock().unwrap();
            assert!(volatile.quarantine.contains_key(&OBJ));
            assert!(!volatile.locations.contains_key(&OBJ));
            assert!(volatile.quarantine_index.contains_key(&1));
        }

        // No-geometry retire (nothing published, no committed row): the
        // bounded drop MUST clear the quarantine row even without a
        // locations row, and clean the directed index.
        service.retire_object_state(1, OBJ, None).unwrap();
        {
            let volatile = service.state.lock().unwrap();
            assert!(
                !volatile.quarantine.contains_key(&OBJ),
                "quarantine-only object must not leak its row"
            );
            assert!(!volatile.quarantine_index.contains_key(&1));
            assert!(!volatile.location_holders.contains_key(&OBJ));
        }
    }

    /// P0-2 (gpt56 `25d4b51e` item 2): the GC handoff stays
    /// quantum-capped for a large layout (no per-tick unbounded work),
    /// and the completion drop clears every trace via the holders
    /// index; a Start purge resolves through the directed quarantine
    /// index — only that worker's own identities are touched — and the
    /// Deleted-ack release is tag-exact.
    #[test]
    fn test_4d2_p02_bounded_gc_and_directed_purge() {
        // -- Part A: bounded drain + completion drop. --
        let service = build_service("4d2-p02b", chooser(vec![worker(1)]));
        seed_sessions(&service, &[worker(1)]);
        mount_incarnation(&service, 1, 0);
        let len = 600 * 64; // 600 blocks, all exactly 64
        committed_entry(&service, token(2, 1), token(2, 2), "/k", OBJ, len, 0);
        let lay = layout(OBJ, len);
        let items: Vec<BlockReportInfo> = (1..=600i64)
            .map(|i| report(lay.block_id(i).unwrap(), 64))
            .collect();
        let out = service.incr_block_report(1, "seed-1", &items).unwrap();
        assert!(out.remove_blocks.is_empty());
        {
            let volatile = service.state.lock().unwrap();
            assert_eq!(volatile.by_worker[&1].live_len(), 600);
            assert!(volatile.location_holders.contains_key(&OBJ));
        }
        {
            let mut volatile = service.state.lock().unwrap();
            volatile
                .gc
                .enqueue(CacheGcWork {
                    incarnation: 1,
                    object_id: OBJ,
                    len,
                    block_size: 64,
                    next_seq: 1,
                })
                .unwrap();
            // Ticks 1-2: quantum-capped, no completion, traces intact.
            assert_eq!(volatile.gc_take_batch().unwrap().len(), 256);
            assert_eq!(volatile.gc_take_batch().unwrap().len(), 256);
            assert!(volatile.locations.contains_key(&OBJ));
            assert_eq!(volatile.by_worker[&1].live_len(), 600);
            // Tick 3: the 88-block tail completes; the O(#holders) drop
            // clears locations/live/holders in the same critical
            // section — no identity walk, no materialized seq list.
            assert_eq!(volatile.gc_take_batch().unwrap().len(), 88);
            assert!(!volatile.locations.contains_key(&OBJ));
            assert!(volatile.by_worker.get(&1).is_none_or(|r| r.live_len() == 0));
            assert!(!volatile.location_holders.contains_key(&OBJ));
        }

        // -- Part B: directed Start purge + tag-exact ack. --
        let service = build_service("4d2-p02c", chooser(vec![worker(1), worker(2)]));
        seed_sessions(&service, &[worker(1), worker(2)]);
        let b1 = layout(OBJ, 130).block_id(1).unwrap();
        let c1 = BlockIdCodec::encode_block_id(OBJ + 1, 1).unwrap();
        service
            .incr_block_report(1, "seed-1", &[report(b1, 64)])
            .unwrap();
        service
            .incr_block_report(2, "seed-2", &[report(c1, 64)])
            .unwrap();
        let t1 = service.cache_session_tag(1).unwrap();
        let t2 = service.cache_session_tag(2).unwrap();

        // An ack carrying the WRONG tag releases nothing.
        service.ack_cache_deleted(1, t1 + 100, b1);
        assert!(service.quarantine_contains(OBJ, 1, t1, 1));

        // Worker 1's Start purges only ITS old-tag entries — worker 2's
        // quarantine (different worker, different object) is untouched.
        service
            .begin_cache_session(1, "fresh-1", &worker(1))
            .unwrap();
        assert!(!service.quarantine_contains(OBJ, 1, t1, 1));
        assert!(service.quarantine_contains(OBJ + 1, 2, t2, 1));
        {
            let volatile = service.state.lock().unwrap();
            assert!(!volatile.quarantine.contains_key(&OBJ));
            assert!(volatile.quarantine.contains_key(&(OBJ + 1)));
            assert!(!volatile.quarantine_index.contains_key(&1));
            assert!(volatile.quarantine_index.contains_key(&2));
        }
    }

    /// Round-3 P0-2 (gpt56 `f5980e03` item 2): a drain tick must be
    /// bounded in TRAVERSAL, not just in returned identities. Degenerate
    /// EMPTY leading records (which the live-only object drop can leave
    /// behind) each cost one budget unit, so one tick cannot walk an
    /// arbitrarily long empty history for free; and a many-object /
    /// single-seq front record drains exactly `min(budget, quantum)`
    /// identities per visit with no full pre-count.
    #[test]
    fn test_4d2_r3_bounded_drain_traversal() {
        // -- Part 1: empty-record popping consumes budget. --
        let mut volatile = CacheVolatile::default();
        const EMPTY_RECORDS: usize = 5000;
        {
            let rev = volatile.by_worker.entry(9).or_default();
            for _ in 0..EMPTY_RECORDS {
                rev.retired.push_back(RetiredSession {
                    tag: 1,
                    entries: BTreeMap::new(),
                });
            }
            let mut tail: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
            tail.entry(OBJ).or_default().insert(1);
            rev.retired.push_back(RetiredSession {
                tag: 1,
                entries: tail,
            });
        }
        volatile.retired_rr.push_back(9);
        volatile.retired_rr_set.insert(9);

        // One tick: at most RETIRED_DRAIN_PER_TICK budget units are
        // spent. The pre-fix shape popped every empty record without
        // deducting budget (a single tick could scan the entire
        // history); now the walk stops when the budget does.
        let removed = volatile.drain_retired();
        assert_eq!(removed, 0, "identity drain only reached after pops");
        let records_left = volatile.by_worker[&9].retired.len();
        assert_eq!(
            records_left,
            EMPTY_RECORDS + 1 - RETIRED_DRAIN_PER_TICK,
            "empty-record traversal is budget-capped per tick"
        );
        // Repeated ticks eventually clear the history and dequeue the
        // worker (bounded self-heal, never an unbounded single pass).
        for _ in 0..10 {
            volatile.drain_retired();
        }
        assert!(volatile.by_worker[&9].retired.is_empty());
        assert!(!volatile.retired_rr.contains(&9));
        assert!(!volatile.retired_rr_set.contains(&9));

        // -- Part 2: many-object single-seq front record — the visit
        // takes min(budget, quantum) identities per worker turn without
        // counting the whole record first. Round-4 (gpt56 `36f4e28b`
        // P0-2): the drain advances by POP (first-entry / pop-first), so
        // every consumed budget unit is a concrete slot removal — the
        // object-key count of the record shrinks by EXACTLY the number
        // of identities drained per tick (a scouting-then-restart
        // traversal could not guarantee that against a shrinking
        // structure). --
        let mut volatile = CacheVolatile::default();
        const OBJS: i64 = 3000;
        {
            let rev = volatile.by_worker.entry(9).or_default();
            let mut entries = BTreeMap::new();
            for o in 0..OBJS {
                entries
                    .entry(OBJ + o)
                    .or_insert_with(BTreeSet::new)
                    .insert(1);
            }
            rev.retired.push_back(RetiredSession { tag: 1, entries });
        }
        volatile.retired_rr.push_back(9);
        volatile.retired_rr_set.insert(9);
        let keys_before = volatile.by_worker[&9].retired[0].entries.len();
        let removed = volatile.drain_retired();
        assert_eq!(removed, RETIRED_DRAIN_PER_TICK);
        let left = volatile.by_worker[&9].retired[0].entries_total();
        assert_eq!(left, (OBJS as usize) - RETIRED_DRAIN_PER_TICK);
        // Visited-slot accounting: single-seq-per-object keys removed ==
        // identities drained — each budget unit popped one slot.
        let keys_after = volatile.by_worker[&9].retired[0].entries.len();
        assert_eq!(
            keys_before - keys_after,
            removed,
            "per-tick object-key removals must equal identities drained"
        );
        // Later ticks drain the remainder the same way — every tick pops
        // exactly what it budgets, until the record is gone.
        let mut ticks = 0;
        loop {
            if volatile.by_worker[&9].retired.is_empty() {
                break;
            }
            let front = volatile.by_worker[&9].retired.front().unwrap();
            if front.entries.is_empty() {
                break;
            }
            let keys_before = front.entries.len();
            let removed_n = volatile.drain_retired();
            let keys_after = volatile.by_worker[&9]
                .retired
                .front()
                .map(|r| r.entries.len())
                .unwrap_or(0);
            assert_eq!(
                keys_before - keys_after,
                removed_n,
                "each tick pops exactly what it budgets"
            );
            ticks += 1;
            assert!(ticks < 10, "drain must converge");
        }
        assert!(volatile.by_worker[&9]
            .retired
            .iter()
            .all(|r| r.entries.is_empty()));
        volatile.drain_retired();
        assert!(
            volatile.by_worker[&9].retired.is_empty(),
            "fully drained record is popped"
        );
        assert!(!volatile.retired_rr_set.contains(&9));
    }

    /// Round-3 P0-3 (gpt56 `f5980e03` item 3): `drop_object_state` is
    /// LIVE-ONLY for the reverse view — it never scans the worker's
    /// retired generations (unbounded under rapid restarts). The stale
    /// identities left in retired records self-heal through the bounded
    /// RR drain: the locations row is already gone, so the drain's
    /// replica strip is a no-op map miss and only the record entries go.
    #[test]
    fn test_4d2_r3_drop_live_only_and_retired_self_heal() {
        const GENERATIONS: usize = 100;
        let mut volatile = CacheVolatile::default();
        volatile.location_holders.insert(OBJ, HashSet::from([1]));
        {
            let rev = volatile.by_worker.entry(1).or_default();
            rev.live.insert(OBJ, BTreeSet::from([1, 2, 3]));
            for gen in 0..GENERATIONS {
                let mut entries = BTreeMap::new();
                entries.insert(OBJ, BTreeSet::from([1, 2, 3]));
                rev.retired.push_back(RetiredSession {
                    tag: 10 + gen as u64,
                    entries,
                });
            }
        }
        volatile.retired_rr.push_back(1);
        volatile.retired_rr_set.insert(1);

        // The drop clears the synchronous traces only: live row +
        // holders index (plus locations/quarantine, empty here).
        volatile.drop_object_state(OBJ);
        assert!(!volatile.location_holders.contains_key(&OBJ));
        assert!(
            volatile.by_worker[&1].live.is_empty(),
            "live object row drops synchronously"
        );
        // ...and deliberately does NOT touch the retired generations.
        assert_eq!(volatile.by_worker[&1].retired.len(), GENERATIONS);
        assert_eq!(volatile.by_worker[&1].retired[0].entries_total(), 3);

        // Bounded self-heal: every drain tick stays under the per-tick
        // cap, the (already-dropped) locations strip is a no-op, and
        // the stale identities clear record by record until the worker
        // leaves the round-robin.
        let mut total = 0usize;
        loop {
            let removed = volatile.drain_retired();
            assert!(removed <= RETIRED_DRAIN_PER_TICK);
            total += removed;
            if removed == 0 && volatile.by_worker[&1].retired.is_empty() {
                break;
            }
            if total > GENERATIONS * 3 + RETIRED_DRAIN_PER_TICK {
                panic!("self-heal drain did not converge");
            }
        }
        assert_eq!(total, GENERATIONS * 3);
        assert!(volatile.by_worker[&1].retired.is_empty());
        assert!(!volatile.retired_rr_set.contains(&1));
    }

    /// Round-3 P1 (gpt56 `f5980e03` item 4): the directed quarantine
    /// index is pruned as soon as THIS reporter's exact `(worker, tag)`
    /// subrow for the object empties — independently of other reporters
    /// still holding quarantine rows for the same object. Waiting for
    /// the whole object row leaks a stale `quarantine_index` entry.
    #[test]
    fn test_4d2_r3_ack_prunes_directed_index_per_reporter() {
        let service = build_service("4d2-r3p1", chooser(vec![worker(1), worker(2)]));
        seed_sessions(&service, &[worker(1), worker(2)]);
        let b1 = layout(OBJ, 130).block_id(1).unwrap();

        // Both workers orphan-report the same block: two exact
        // quarantine identities on ONE object row.
        service
            .incr_block_report(1, "seed-1", &[report(b1, 64)])
            .unwrap();
        service
            .incr_block_report(2, "seed-2", &[report(b1, 64)])
            .unwrap();
        let t1 = service.cache_session_tag(1).unwrap();
        let t2 = service.cache_session_tag(2).unwrap();
        {
            let volatile = service.state.lock().unwrap();
            assert!(volatile.quarantine[&OBJ].contains_key(&(1, t1)));
            assert!(volatile.quarantine[&OBJ].contains_key(&(2, t2)));
            assert!(volatile.quarantine_index.contains_key(&1));
            assert!(volatile.quarantine_index.contains_key(&2));
        }

        // Ack worker 1's identity: the object row survives (worker 2
        // still quarantined) but worker 1's directed-index trace MUST
        // go now — the subrow `(1, t1)` is empty.
        service.ack_cache_deleted(1, t1, b1);
        {
            let volatile = service.state.lock().unwrap();
            assert!(
                volatile.quarantine.contains_key(&OBJ),
                "worker 2's quarantine row survives"
            );
            assert!(
                !volatile.quarantine_index.contains_key(&1),
                "reporter-empty subrow prunes the directed index immediately"
            );
            assert!(volatile.quarantine_index.contains_key(&2));
        }

        // Acking worker 2 clears the last row and the last trace.
        service.ack_cache_deleted(2, t2, b1);
        {
            let volatile = service.state.lock().unwrap();
            assert!(!volatile.quarantine.contains_key(&OBJ));
            assert!(!volatile.quarantine_index.contains_key(&2));
        }
    }
}
