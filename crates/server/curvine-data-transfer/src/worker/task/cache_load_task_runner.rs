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

//! Phase 3 (dual-mode metadata split) cache-mode load runner.
//!
//! Executes the shortest closed loop against the cache index ONLY —
//! UFS status/open → CacheAllocate (durable load token) → write the
//! master-planned blocks on the planned workers → self-contained
//! CacheCommit. It never touches the inode tree: no create_parent, no
//! create, no rename, no set_attr(ufs_mtime).
//!
//! Hard gates (task #5):
//! 1. zero-length: Allocate(len=0) → zero worker I/O → Commit(blocks=[]).
//! 2. any worker write/complete failure aborts the WHOLE task with no
//!    Commit — BlockWriter is all-or-nothing per planned block, so a
//!    partial replica never reaches the commit. Written-not-committed
//!    blocks are orphans reclaimed by the 4d.3 reconcile.
//! 3. both op tokens come from the master-minted `CacheLoadSpec` and are
//!    replayed verbatim on every retry — a commit response loss followed
//!    by a same-token retry returns the same durable outcome.
//! 4. crash/restart: the commit is self-contained (token + identity +
//!    len + ufs_mtime + succeeded locations), no worker-resident state.
//!    If the master lost the volatile plan (restart) or invalidated its
//!    fences (worker session/epoch change), the commit comes back as a
//!    typed REPLAN_NEEDED (RC `40e47dcb` + `3d91a095`) — the runner
//!    replays the EXACT allocate (same load token, same identity, fresh
//!    plan), rewrites every block per the NEW placements, rebuilds the
//!    ACK evidence, and re-commits, all inside the task timeout budget.
//!    An already-applied commit is never re-allocated: its recovery
//!    path is commit-side only.
//! 5. a superseded commit (fresher concurrent winner) is a terminal
//!    success for this task: the loser's row is reclaimed by the master.

use crate::common::UfsFactory;
use crate::worker::task::TaskContext;
use curvine_client_core::block::BlockWriter;
use curvine_client_core::file::CurvineFileSystem;
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path, Reader};
use curvine_job_client::JobMasterClient;
use curvine_model::{
    CacheLoadSpec, ExtendedBlock, FileType, JobTaskProgress, JobTaskState, LocatedBlock, ProtoUtils,
};
use curvine_proto::CacheOpStatusProto;
use curvine_runtime::common::LocalTime;
use curvine_unified_fs::{UfsFileSystem, UnifiedReader};
use log::{error, info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const READ_CHUNK_BYTES: i64 = 16 * 1024 * 1024;

/// Backoff between verbatim CacheCommit retries (hard gate 3).
const COMMIT_RETRY_BACKOFF_MS: u64 = 1_000;

/// Best-effort CacheAbort budget after a failed/canceled run whose
/// commit was never issued (gate-2 escape, gpt56 `fca627f5`).
const ABORT_RETRY_ATTEMPTS: u32 = 3;
const ABORT_RETRY_BACKOFF_MS: u64 = 500;

pub struct CacheLoadTaskRunner {
    task: Arc<TaskContext>,
    fs: CurvineFileSystem,
    factory: Arc<UfsFactory>,
    master_client: JobMasterClient,
    progress_interval_ms: u64,
    task_timeout_ms: u64,
}

impl CacheLoadTaskRunner {
    pub fn new(
        task: Arc<TaskContext>,
        fs: CurvineFileSystem,
        factory: Arc<UfsFactory>,
        progress_interval_ms: u64,
        task_timeout_ms: u64,
    ) -> Self {
        let master_client = JobMasterClient::new(fs.fs_client());
        Self {
            task,
            fs,
            factory,
            master_client,
            progress_interval_ms,
            task_timeout_ms,
        }
    }

    fn get_ufs(&self) -> FsResult<UfsFileSystem> {
        self.factory.get_ufs(&self.task.info.job.mount_info)
    }

    fn log_context(&self) -> String {
        format!(
            "job={} task={} source={} cache_key={:?}",
            self.task.info.job.job_id,
            self.task.info.task_id,
            self.task.info.source_path,
            self.task.info.cache.as_ref().map(|spec| spec.key.clone())
        )
    }

    pub async fn run(&self) -> bool {
        let remove_task = match self.run0().await {
            Ok(remove_task) => remove_task,
            Err(e) => {
                if self.task.is_cancel() {
                    info!(
                        "cache load task stopped after cancellation request: {} err={}",
                        self.log_context(),
                        e
                    );
                    return self.finish_canceled().await.unwrap_or_else(|err| {
                        error!(
                            "cache load task cancellation finalization failed: {} err={}",
                            self.log_context(),
                            err
                        );
                        true
                    });
                }
                // No Commit was issued (or it failed closed): the task
                // fails loudly and any written blocks stay orphans for
                // the 4d.3 reconcile to reclaim.
                error!("cache load task failed: {} err={}", self.log_context(), e);
                let progress = self.task.set_failed(format!("cache load failed: {}", e));
                if let Err(err) = self.report_progress(progress).await {
                    warn!(
                        "cache load task failure report failed: {} err={}",
                        self.log_context(),
                        err
                    );
                }
                true
            }
        };

        remove_task
    }

    async fn run0(&self) -> FsResult<bool> {
        if self.task.is_cancel() {
            info!(
                "cache load task canceled before starting: {}",
                self.log_context()
            );
            return self.finish_canceled().await;
        }

        let spec = self.require_cache_spec()?;
        self.task
            .update_state(JobTaskState::Loading, "cache load task started");

        // Observe the UFS identity (len + mtime) before any byte is read.
        // The mtime travels inside the self-contained commit; the len is
        // what the allocate plan is derived from.
        let source_path = Path::from_str(&self.task.info.source_path)?;
        let ufs = self.get_ufs()?;
        let status = ufs.get_status(&source_path).await?;
        if status.is_dir {
            return err_box!(
                "cache load source {} is a directory",
                source_path.full_path()
            );
        }
        let file_len = status.len;
        let ufs_mtime = status.mtime;

        // Two-state closed loop (hard gates 3+4, RC `40e47dcb` +
        // `3d91a095`), driven end-to-end by `drive_cache_load`:
        //
        // - Allocate with the master-minted retry-stable load token
        //   (zero-length files do NOT skip it: the service accepts len 0
        //   and returns an empty plan, hard gate 1), write the planned
        //   blocks recording the locations that ACTUALLY completed, then
        //   commit self-contained evidence with a verbatim retry.
        // - REPLAN_NEEDED (volatile plan lost to a master restart, or
        //   plan fences invalidated by a worker session/epoch change
        //   after the writes; the commit has NOT applied): replay the
        //   EXACT allocate — same load token, same identity, fresh plan —
        //   rewrite every block per the NEW placements, rebuild the ACK
        //   evidence, and re-commit, bounded by the task timeout budget.
        // - An applied commit (response loss) is NEVER re-allocated: the
        //   master's per-client watermark (2) makes the load token (1)
        //   evictable by the bounded outcome window, so recovery rides
        //   the commit-side path alone.
        let client = self.fs.fs_client();

        // gpt56 `e9c554b2`: ONE absolute, saturating deadline computed
        // BEFORE the first Allocate and shared by every phase — each block
        // write, every replan round, and every commit retry checks this
        // same instant; no phase ever resets it. This makes the master's
        // Reserved lease (task_timeout + fixed grace, `reserved_lease_ms`)
        // provably cover the whole live task: the runner can never
        // legally run past this deadline, so a live row can never be past
        // its lease while its runner still commits.
        let deadline_ms = LocalTime::mills().saturating_add(self.task_timeout_ms);

        let allocate = {
            let client = client.clone();
            let spec = spec.clone();
            move || {
                let client = client.clone();
                let spec = spec.clone();
                async move {
                    client
                        .cache_allocate(
                            spec.load_token,
                            spec.incarnation,
                            &spec.key,
                            file_len,
                            self.task.info.job.block_size,
                        )
                        .await
                }
            }
        };
        let write = |alloc: curvine_proto::CacheAllocateResponse| {
            // `alloc` must be owned by the future (a replan round gets a
            // fresh plan value); `self`/`ufs`/`source_path` are borrows
            // of this fn's scope, which outlives every round, so the
            // async block only moves Copy references.
            let ufs = &ufs;
            let source_path = &source_path;
            async move {
                self.write_round(ufs, source_path, file_len, &alloc, deadline_ms)
                    .await
            }
        };
        let commit = {
            let client = client.clone();
            let spec = spec.clone();
            move |evidence: CommitEvidence| {
                let client = client.clone();
                let spec = spec.clone();
                async move {
                    client
                        .cache_commit(
                            spec.commit_token,
                            spec.load_token,
                            spec.incarnation,
                            &spec.key,
                            evidence.generation,
                            evidence.object_id,
                            file_len,
                            ufs_mtime,
                            evidence.blocks,
                        )
                        .await
                }
            }
        };

        // gpt56 `21bb7129`: set by `drive_cache_load` BEFORE the first
        // commit send and never reset. Once a commit may have reached
        // the master its outcome is unknown and the failure path MUST
        // NOT abort the load's Reserved row (the server-side
        // commit-token first-winner guard is the backstop).
        let commit_issued = AtomicBool::new(false);
        let (commit, evidence) = match drive_cache_load(
            || self.task.is_cancel(),
            deadline_ms,
            COMMIT_RETRY_BACKOFF_MS,
            &commit_issued,
            allocate,
            write,
            commit,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                self.abort_reserved_row(&client, &spec, &commit_issued)
                    .await;
                return Err(e);
            }
        };
        match cache_op_status(commit.status) {
            Some(CacheOpStatusProto::Applied) | Some(CacheOpStatusProto::AlreadyApplied) => {}
            Some(CacheOpStatusProto::Superseded) => {
                // A fresher concurrent winner owns the entry; this load's
                // row is reclaimed by the master. Terminal success for
                // the task (hard gate 5).
                info!(
                    "cache load commit superseded (current generation {:?}): {}",
                    commit.current_generation,
                    self.log_context()
                );
            }
            // drive_cache_load loops on ReplanNeeded and never surfaces
            // it; treat a leak-through as uninterpretable, fail closed.
            Some(CacheOpStatusProto::ReplanNeeded) => {
                self.abort_reserved_row(&client, &spec, &commit_issued)
                    .await;
                return err_box!(
                    "cache commit returned REPLAN_NEEDED past the recovery loop: {}",
                    self.log_context()
                );
            }
            // Missing or unknown discriminator: the commit outcome is not
            // interpretable — fail closed instead of assuming Applied.
            None => {
                // Uninterpretable outcome: `commit_issued` is already
                // set, so this never aborts — recorded for symmetry.
                self.abort_reserved_row(&client, &spec, &commit_issued)
                    .await;
                return err_box!(
                    "cache commit returned unrecognized status {:?}: {}",
                    commit.status,
                    self.log_context()
                );
            }
        }

        self.update_progress(file_len, file_len, true).await;
        info!(
            "cache load task completed: {} object_id={} generation={} file_len={} ufs_mtime={} committed_blocks={}",
            self.log_context(),
            evidence.object_id,
            evidence.generation,
            file_len,
            ufs_mtime,
            evidence.blocks.len()
        );

        Ok(true)
    }

    /// One full write round against a plan: opens a FRESH reader (a
    /// replan round rewrites from offset 0 per the NEW placements), walks
    /// every planned block with the all-or-nothing `BlockWriter`, and
    /// returns the commit evidence built from the writer's ACTUAL
    /// `CommitBlock` ACKs (never the bare plan). Any failure aborts the
    /// whole task — no Commit is issued for this round.
    async fn write_round(
        &self,
        ufs: &UfsFileSystem,
        source_path: &Path,
        file_len: i64,
        alloc: &curvine_proto::CacheAllocateResponse,
        deadline_ms: u64,
    ) -> FsResult<CommitEvidence> {
        // Plan shape must match the observed file length — both for the
        // initial plan and for every replan round (hard gate 1 shape).
        if file_len == 0 && !alloc.blocks.is_empty() {
            return err_box!(
                "cache allocate returned {} blocks for a zero-length object",
                alloc.blocks.len()
            );
        }
        if file_len > 0 && alloc.blocks.is_empty() {
            return err_box!(
                "cache allocate returned an empty plan for a {}-byte object",
                file_len
            );
        }

        let mut committed_blocks = Vec::with_capacity(alloc.blocks.len());
        if file_len > 0 {
            let mut reader = ufs.open(source_path).await?;
            if reader.len() != file_len {
                return err_box!(
                    "cache load source {} changed length after status (expected {}, reader {})",
                    source_path.full_path(),
                    file_len,
                    reader.len()
                );
            }

            let mut written: i64 = 0;
            for block in &alloc.blocks {
                let (loaded, completed) = self
                    .write_planned_block(&mut reader, block, written, file_len, deadline_ms)
                    .await?;
                written = loaded;
                committed_blocks.push(completed);
            }
            reader.complete().await?;
        }

        Ok(CommitEvidence {
            generation: alloc.generation,
            object_id: alloc.object_id,
            blocks: committed_blocks,
        })
    }

    /// Writes one master-planned block on its planned workers using the
    /// reusable all-or-nothing `BlockWriter`. Any failure aborts the
    /// whole task (the caller returns Err → no Commit); the writer is
    /// canceled so the worker-side partial block is dropped. On success
    /// returns `(loaded_after, completed)` where `completed` is built
    /// from the writer's ACTUAL `CommitBlock` ACKs (never the bare plan):
    /// completed locations are mapped back to the planned
    /// `WorkerAddressProto`s by worker id, so an unplanned ACK is loud.
    async fn write_planned_block(
        &self,
        reader: &mut UnifiedReader,
        block: &curvine_proto::CacheBlockLocationProto,
        loaded_before: i64,
        file_len: i64,
        deadline_ms: u64,
    ) -> FsResult<(i64, curvine_proto::CacheBlockLocationProto)> {
        let block_len = block.block_len;
        if block_len <= 0 {
            return err_box!(
                "planned cache block {} has non-positive len",
                block.block_id
            );
        }

        let locs: Vec<_> = block
            .workers
            .iter()
            .map(ProtoUtils::worker_address_from_pb)
            .collect();
        if locs.is_empty() {
            return err_box!(
                "planned cache block {} has no worker placement",
                block.block_id
            );
        }
        let extended = ExtendedBlock::new(
            block.block_id,
            block_len,
            self.task.info.job.storage_type,
            FileType::File,
        );
        let locate = LocatedBlock::new(extended, locs);

        let mut writer = BlockWriter::new(self.fs.fs_context(), locate, 0, block_len).await?;
        let mut written_block: i64 = 0;
        let mut last_progress_time = LocalTime::mills();

        let outcome = loop {
            if self.task.is_cancel() {
                break Err(curvine_error::FsError::common("cache load task canceled"));
            }
            // gpt56 `e9c554b2`: the whole-task absolute deadline — shared
            // with every other phase and never reset per block — so N slow
            // blocks consume ONE budget, not N.
            if deadline_exceeded(deadline_ms) {
                break err_box!(
                    "Task {} exceeded global deadline {} ms in cache block {}",
                    self.task.info.task_id,
                    deadline_ms,
                    block.block_id
                );
            }
            // Block full: complete before the loop would issue an
            // empty-bounded read (which reads as a short read).
            if written_block == block_len {
                break writer.complete().await;
            }

            let want = (block_len - written_block).min(READ_CHUNK_BYTES) as usize;
            let chunk = match reader.async_read(Some(want)).await {
                Ok(chunk) => chunk,
                Err(e) => break Err(e),
            };
            if chunk.is_empty() {
                break err_box!(
                    "short read on cache block {} ({} of {} bytes)",
                    block.block_id,
                    written_block,
                    block_len
                );
            }
            written_block += chunk.len() as i64;

            if let Err(e) = writer.write(chunk).await {
                break Err(e);
            }

            if LocalTime::mills() > last_progress_time + self.progress_interval_ms {
                last_progress_time = LocalTime::mills();
                self.update_progress(loaded_before + written_block, file_len, false)
                    .await;
            }
        };

        let commit_block = match outcome {
            Ok(commit_block) => commit_block,
            Err(e) => {
                if let Err(cancel_err) = writer.cancel().await {
                    warn!(
                        "cancel failed cache block {} writer: {} err={}",
                        block.block_id,
                        self.log_context(),
                        cancel_err
                    );
                }
                return Err(e);
            }
        };

        let completed = completed_location(block, commit_block)?;
        Ok((loaded_before + written_block, completed))
    }

    fn require_cache_spec(&self) -> FsResult<CacheLoadSpec> {
        match self.task.info.cache.clone() {
            Some(spec) => Ok(spec),
            None => err_box!(
                "cache load task {} has no cache spec (master must inject CacheLoadSpec)",
                self.task.info.task_id
            ),
        }
    }

    pub async fn update_progress(&self, loaded_size: i64, total_size: i64, is_last: bool) {
        if let Err(e) = self
            .update_progress0(loaded_size, total_size, is_last)
            .await
        {
            warn!(
                "cache load task progress report failed: {} err={}",
                self.log_context(),
                e
            );
        }
    }

    pub async fn update_progress0(
        &self,
        loaded_size: i64,
        total_size: i64,
        is_last: bool,
    ) -> FsResult<()> {
        let progress = self.task.update_progress(loaded_size, total_size, is_last);
        self.report_progress(progress).await
    }

    async fn report_progress(&self, progress: JobTaskProgress) -> FsResult<()> {
        let task = &self.task.info;
        self.master_client
            .report_task(&task.job.job_id, &task.task_id, progress)
            .await
    }

    /// Durable escape for the gate-2 wedge (gpt56 `fca627f5`): a failed
    /// or canceled run may leave the master's cache row Reserved under
    /// this load's token, permanently refusing new allocations of the
    /// key. When — and ONLY when — no commit was ever issued, tombstone
    /// the row via CacheAbort (bounded retries, log-only failure). Once
    /// `commit_issued` is set this is a no-op: the server-side
    /// commit-token first-winner guard (`21bb7129`) is the backstop, the
    /// flag is the primary fence.
    async fn abort_reserved_row(
        &self,
        client: &curvine_client_core::file::FsClient,
        spec: &CacheLoadSpec,
        commit_issued: &AtomicBool,
    ) {
        let context = self.log_context();
        let outcome = best_effort_abort_after_failure(
            commit_issued,
            ABORT_RETRY_ATTEMPTS,
            ABORT_RETRY_BACKOFF_MS,
            || async {
                let resp = client
                    .cache_abort(
                        spec.load_token,
                        spec.commit_token,
                        spec.incarnation,
                        &spec.key,
                    )
                    .await?;
                abort_outcome(&resp)
            },
        )
        .await;
        match outcome {
            AbortOutcome::Aborted => info!(
                "cache load Reserved row aborted after failed run: {}",
                context
            ),
            AbortOutcome::Forbidden => info!(
                "cache load commit outcome unknown; abort of Reserved row forbidden: {}",
                context
            ),
            AbortOutcome::Exhausted => error!(
                "cache load abort retries exhausted; Reserved row left for TTL sweep/manual reclaim: {}",
                context
            ),
        }
    }

    async fn finish_canceled(&self) -> FsResult<bool> {
        let progress = self.task.set_canceled("task canceled");
        if let Err(err) = self.report_progress(progress).await {
            info!(
                "canceled cache load task report was not accepted, remove local task anyway: {} err={}",
                self.log_context(),
                err
            );
        }
        Ok(true)
    }
}

/// The self-contained commit evidence of ONE write round: the allocate
/// identity plus the locations that ACTUALLY completed (worker ACKs).
/// Replayed verbatim on commit retries; rebuilt from scratch after a
/// REPLAN round (old placements only count as orphans).
#[derive(Clone, Debug)]
struct CommitEvidence {
    generation: u64,
    object_id: i64,
    blocks: Vec<curvine_proto::CacheBlockLocationProto>,
}

/// Drives the two-state closed loop (hard gates 3+4, RC `40e47dcb` +
/// `3d91a095`):
///
/// allocate → write round → commit (verbatim retry). A commit that
/// returns REPLAN_NEEDED has NOT applied: its plan is unusable (volatile
/// plan lost to a master restart, or fences invalidated by a worker
/// session/epoch change), so the loop replays the EXACT allocate — the
/// FSM replays the same identity and installs a fresh plan — rewrites
/// every block per the NEW placements, rebuilds the evidence, and
/// re-commits, bounded by the whole-task absolute deadline shared with
/// the write phases (gpt56 `e9c554b2`). Any other commit
/// outcome is terminal (Applied/AlreadyApplied/Superseded) or fails
/// closed (missing/unknown discriminator). Cancellation is loud at every
/// state boundary.
async fn drive_cache_load<C, A, AFut, W, WFut, K, KFut>(
    mut is_cancel: C,
    deadline_ms: u64,
    backoff_ms: u64,
    commit_issued: &AtomicBool,
    mut allocate: A,
    mut write_round: W,
    mut commit: K,
) -> FsResult<(curvine_proto::CacheCommitResponse, CommitEvidence)>
where
    C: FnMut() -> bool,
    A: FnMut() -> AFut,
    AFut: std::future::Future<Output = FsResult<curvine_proto::CacheAllocateResponse>>,
    W: FnMut(curvine_proto::CacheAllocateResponse) -> WFut,
    WFut: std::future::Future<Output = FsResult<CommitEvidence>>,
    K: FnMut(CommitEvidence) -> KFut,
    KFut: std::future::Future<Output = FsResult<curvine_proto::CacheCommitResponse>>,
{
    let mut evidence = write_round(allocate().await?).await?;
    loop {
        if is_cancel() {
            return err_box!("cache load aborted by task cancellation before commit");
        }
        // gpt56 `e9c554b2`: the whole-task absolute deadline, checked
        // BEFORE `commit_issued` is set — a task past its global budget
        // never sends a Commit, and the ambiguity flag stays in the
        // state that lets the failure path take the SAFE abort.
        if deadline_exceeded(deadline_ms) {
            return err_box!(
                "cache load global deadline {} ms exceeded before commit",
                deadline_ms
            );
        }
        let commit_resp = {
            // Mark BEFORE the first send and never reset (gpt56
            // `21bb7129`): a transport error after this point leaves
            // the commit outcome unknown — aborting the Reserved row
            // could tombstone a row the commit just published over.
            commit_issued.store(true, Ordering::Release);
            let evidence = evidence.clone();
            commit_with_retry(&mut is_cancel, deadline_ms, backoff_ms, || {
                commit(evidence.clone())
            })
            .await?
        };
        match cache_op_status(commit_resp.status) {
            Some(CacheOpStatusProto::Applied)
            | Some(CacheOpStatusProto::AlreadyApplied)
            | Some(CacheOpStatusProto::Superseded) => return Ok((commit_resp, evidence)),
            Some(CacheOpStatusProto::ReplanNeeded) => {
                // The commit did NOT apply and this round's writes only
                // count as orphans: re-plan, rewrite, rebuild evidence.
                //
                // Typed REPLAN_NEEDED definitively resolves the commit
                // ambiguity (the master asserted the commit did NOT
                // apply), so the failure path may abort again if the
                // NEXT round fails before issuing another commit (gpt56
                // `cfa2f0d7` blocker 1). Transport errors never clear
                // the flag — only this typed outcome does. Clear FIRST so
                // a deadline exhaustion here also takes the safe abort.
                commit_issued.store(false, Ordering::Release);
                if deadline_exceeded(deadline_ms) {
                    return err_box!(
                        "cache load replan budget exhausted (global deadline {} ms) after REPLAN_NEEDED",
                        deadline_ms
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                evidence = write_round(allocate().await?).await?;
            }
            None => {
                return err_box!(
                    "cache commit returned unrecognized status {:?}",
                    commit_resp.status
                );
            }
        }
    }
}

/// Outcome of the post-failure best-effort abort decision.
#[derive(Debug, PartialEq, Eq)]
enum AbortOutcome {
    /// Abort was warranted (no commit ever issued) and the master
    /// accepted it (Applied / AlreadyApplied / Superseded).
    Aborted,
    /// A commit was issued at least once — its outcome may be unknown;
    /// aborting is forbidden (gpt56 `fca627f5` / `21bb7129`).
    Forbidden,
    /// Abort was warranted but every bounded attempt failed; the row is
    /// left for the TTL sweep / manual reclaim (loud, never silent).
    Exhausted,
}

/// Decision core for the gate-2 durable escape: attempt the abort ONLY
/// while no commit was ever issued, with a small bounded retry budget.
/// Closure-generic so the runner seam (abort iff `!commit_issued`,
/// including after a commit transport error) is unit-testable.
async fn best_effort_abort_after_failure<F, Fut>(
    commit_issued: &AtomicBool,
    attempts: u32,
    backoff_ms: u64,
    mut send: F,
) -> AbortOutcome
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = FsResult<()>>,
{
    if commit_issued.load(Ordering::Acquire) {
        return AbortOutcome::Forbidden;
    }
    for attempt in 1..=attempts {
        match send().await {
            Ok(()) => return AbortOutcome::Aborted,
            Err(_) if attempt == attempts => return AbortOutcome::Exhausted,
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            }
        }
    }
    unreachable!("bounded loop always returns")
}

/// Assembles the commit evidence for one block from the writer's ACTUAL
/// `CommitBlock` ACK: each ACKed `worker_id` is mapped back to its
/// planned `WorkerAddressProto` (full five-field endpoint identity — the
/// master's commit validator accepts planned workers only). An ACK from
/// a worker that was never planned is a loud error, not a silent pass.
fn completed_location(
    planned: &curvine_proto::CacheBlockLocationProto,
    acked: curvine_model::CommitBlock,
) -> FsResult<curvine_proto::CacheBlockLocationProto> {
    let mut workers = Vec::with_capacity(acked.locations.len());
    for loc in &acked.locations {
        let worker = planned
            .workers
            .iter()
            .find(|w| w.worker_id == loc.worker_id);
        match worker {
            Some(w) => workers.push(w.clone()),
            None => {
                return err_box!(
                    "completed cache block {} ACKed unplanned worker {}",
                    acked.block_id,
                    loc.worker_id
                );
            }
        }
    }
    Ok(curvine_proto::CacheBlockLocationProto {
        block_id: acked.block_id,
        block_len: acked.block_len,
        workers,
    })
}

/// Bounded verbatim retry of the self-contained CacheCommit RPC (hard
/// gate 3, RC `4ebcff5a`): retries transient RPC errors until the
/// task-level timeout budget is spent or the task is canceled. The
/// request is never mutated across attempts, so the master resolves an
/// applied-but-response-lost commit as AlreadyApplied (or Superseded)
/// from its durable outcome — never a second execution.
async fn commit_with_retry<C, F, Fut>(
    mut is_cancel: C,
    deadline_ms: u64,
    backoff_ms: u64,
    mut attempt: F,
) -> FsResult<curvine_proto::CacheCommitResponse>
where
    C: FnMut() -> bool,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = FsResult<curvine_proto::CacheCommitResponse>>,
{
    // gpt56 `e9c554b2`: the deadline is the task's WHOLE-budget absolute
    // instant (shared with the write/replan phases), not a fresh
    // per-commit budget.
    loop {
        match attempt().await {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                if is_cancel() {
                    return err_box!(
                        "cache commit retry aborted by task cancellation, last err: {}",
                        e
                    );
                }
                if deadline_exceeded(deadline_ms) {
                    return Err(e);
                }
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            }
        }
    }
}

/// Whole-task deadline predicate (gpt56 `e9c554b2`): every phase — each
/// block write, every replan round, every commit retry — checks the SAME
/// absolute instant, computed once before the first Allocate, against the
/// current clock. No phase computes its own budget.
fn deadline_exceeded(deadline_ms: u64) -> bool {
    LocalTime::mills() > deadline_ms
}

/// Fail-closed decode of the commit op status. The repo's prost codegen
/// emits no `TryFrom<i32>` for proto2 enums, so this discriminated match
/// restores that contract: a missing or unknown discriminator decodes to
/// `None` and the caller must treat the commit outcome as
/// uninterpretable.
/// Terminal-status gate for a CacheAbort response (gpt56 `cfa2f0d7`
/// blocker 2): ONLY Applied / AlreadyApplied / Superseded mean the
/// Reserved row is released (or owned by a fresher winner). REPLAN_NEEDED
/// (impossible today but wire-decodable), a missing discriminator, or an
/// unknown value is a FAILURE that keeps the bounded retry going — never
/// a silent "any Ok response means cleaned".
fn abort_outcome(resp: &curvine_proto::CacheAbortResponse) -> FsResult<()> {
    match cache_op_status(resp.status) {
        Some(CacheOpStatusProto::Applied)
        | Some(CacheOpStatusProto::AlreadyApplied)
        | Some(CacheOpStatusProto::Superseded) => Ok(()),
        other => err_box!(
            "cache abort returned non-terminal status {:?} (raw {:?})",
            other,
            resp.status
        ),
    }
}

fn cache_op_status(v: Option<i32>) -> Option<CacheOpStatusProto> {
    match v {
        Some(s) if s == CacheOpStatusProto::Applied as i32 => Some(CacheOpStatusProto::Applied),
        Some(s) if s == CacheOpStatusProto::AlreadyApplied as i32 => {
            Some(CacheOpStatusProto::AlreadyApplied)
        }
        Some(s) if s == CacheOpStatusProto::Superseded as i32 => {
            Some(CacheOpStatusProto::Superseded)
        }
        Some(s) if s == CacheOpStatusProto::ReplanNeeded as i32 => {
            Some(CacheOpStatusProto::ReplanNeeded)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{cache_op_status, commit_with_retry, completed_location, deadline_exceeded};
    use curvine_error::FsError;
    use curvine_model::{BlockLocation, CommitBlock, StorageType};
    use curvine_proto::{
        CacheBlockLocationProto, CacheCommitResponse, CacheOpStatusProto, WorkerAddressProto,
    };
    use curvine_runtime::common::LocalTime;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    fn worker(id: u32) -> WorkerAddressProto {
        WorkerAddressProto {
            worker_id: id,
            hostname: format!("w{}", id),
            ip_addr: format!("10.0.0.{}", id),
            rpc_port: 9000,
            web_port: 9001,
        }
    }

    #[test]
    fn cache_op_status_decodes_known_discriminators() {
        assert_eq!(cache_op_status(Some(1)), Some(CacheOpStatusProto::Applied));
        assert_eq!(
            cache_op_status(Some(2)),
            Some(CacheOpStatusProto::AlreadyApplied)
        );
        assert_eq!(
            cache_op_status(Some(3)),
            Some(CacheOpStatusProto::Superseded)
        );
        assert_eq!(
            cache_op_status(Some(4)),
            Some(CacheOpStatusProto::ReplanNeeded)
        );
    }

    #[test]
    fn cache_op_status_fails_closed_on_missing_or_unknown() {
        assert_eq!(cache_op_status(None), None);
        assert_eq!(cache_op_status(Some(0)), None);
        assert_eq!(cache_op_status(Some(5)), None);
        assert_eq!(cache_op_status(Some(-1)), None);
    }

    #[test]
    fn completed_location_maps_acks_back_to_planned_workers() {
        let planned = CacheBlockLocationProto {
            block_id: 7,
            block_len: 128,
            workers: vec![worker(1), worker(2)],
        };
        let acked = CommitBlock {
            block_id: 7,
            block_len: 128,
            locations: vec![
                BlockLocation::new(2, StorageType::Mem),
                BlockLocation::new(1, StorageType::Mem),
            ],
        };

        let completed = completed_location(&planned, acked).unwrap();
        assert_eq!(completed.block_id, 7);
        assert_eq!(completed.block_len, 128);
        assert_eq!(completed.workers.len(), 2);
        // ACK order is preserved, identities come from the plan.
        assert_eq!(completed.workers[0].worker_id, 2);
        assert_eq!(completed.workers[0].hostname, "w2");
        assert_eq!(completed.workers[1].worker_id, 1);
    }

    #[test]
    fn completed_location_rejects_unplanned_ack_loudly() {
        let planned = CacheBlockLocationProto {
            block_id: 7,
            block_len: 128,
            workers: vec![worker(1)],
        };
        let acked = CommitBlock {
            block_id: 7,
            block_len: 128,
            locations: vec![BlockLocation::new(99, StorageType::Mem)],
        };

        let err = completed_location(&planned, acked).unwrap_err().to_string();
        assert!(err.contains("unplanned worker 99"), "err: {}", err);
    }

    // ---- commit_with_retry fault seam (hard gate 3, RC `4ebcff5a`) ----
    // A response-loss / transient RPC error must NOT fail the task while
    // the retry budget lasts; the verbatim replay converges on the
    // master's durable outcome.

    #[tokio::test]
    async fn commit_retry_converges_after_transient_failures() {
        let attempts = AtomicU32::new(0);
        let resp = commit_with_retry(
            || false,
            LocalTime::mills() + 10_000,
            1,
            || {
                let n = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n < 2 {
                        Err(FsError::common("injected transient rpc error"))
                    } else {
                        Ok(CacheCommitResponse::default())
                    }
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(resp, CacheCommitResponse::default());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn commit_retry_fails_loudly_after_budget() {
        let attempts = AtomicU32::new(0);
        let err = commit_with_retry(
            || false,
            LocalTime::mills() + 20,
            5,
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Err(FsError::common("commit endpoint down")) }
            },
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("commit endpoint down"),
            "err: {}",
            err
        );
        assert!(
            attempts.load(Ordering::SeqCst) >= 2,
            "budget must cover more than one attempt"
        );
    }

    #[tokio::test]
    async fn commit_retry_stops_on_cancellation() {
        let attempts = AtomicU32::new(0);
        let err = commit_with_retry(
            || true,
            LocalTime::mills() + 10_000,
            1,
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Err(FsError::common("net")) }
            },
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("cancellation"), "err: {}", err);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    // ---- drive_cache_load two-state recovery seam (hard gate 4, RC ----
    // `40e47dcb` + `3d91a095`): a REPLAN_NEEDED commit has NOT applied,
    // so the loop must replay the exact allocate, rewrite per the NEW
    // plan, and rebuild the evidence; an applied commit (response loss)
    // must converge commit-side only and NEVER re-allocate.

    use super::{
        abort_outcome, best_effort_abort_after_failure, drive_cache_load, AbortOutcome,
        CommitEvidence,
    };
    use curvine_proto::CacheAllocateResponse;

    fn resp_with_status(s: i32) -> CacheCommitResponse {
        CacheCommitResponse {
            status: Some(s),
            ..Default::default()
        }
    }

    fn round_evidence(round: i64) -> CommitEvidence {
        // The round number rides in block_id so a test can prove WHICH
        // write round a commit actually carried.
        CommitEvidence {
            generation: 1,
            object_id: 5,
            blocks: vec![CacheBlockLocationProto {
                block_id: round,
                block_len: 64,
                workers: vec![worker(1)],
            }],
        }
    }

    #[tokio::test]
    async fn drive_replans_rewrites_and_recommits_after_plan_loss() {
        let allocs = AtomicU32::new(0);
        let rounds = AtomicU32::new(0);
        let commits = AtomicU32::new(0);
        let committed_rounds = std::sync::Mutex::new(Vec::new());

        let commit_issued = AtomicBool::new(false);
        let (resp, evidence) = drive_cache_load(
            || false,
            LocalTime::mills() + 10_000,
            1,
            &commit_issued,
            || {
                allocs.fetch_add(1, Ordering::SeqCst);
                async {
                    Ok(CacheAllocateResponse {
                        object_id: 5,
                        generation: 1,
                        blocks: vec![CacheBlockLocationProto {
                            block_id: 1,
                            block_len: 64,
                            workers: vec![worker(1)],
                        }],
                    })
                }
            },
            |_alloc| {
                let round = 1 + rounds.fetch_add(1, Ordering::SeqCst);
                async move { Ok(round_evidence(round as i64)) }
            },
            |ev| {
                let n = 1 + commits.fetch_add(1, Ordering::SeqCst);
                committed_rounds.lock().unwrap().push(ev.blocks[0].block_id);
                async move {
                    // First commit loses the volatile plan (master
                    // restart / session-tag change); the second applies.
                    if n == 1 {
                        Ok(resp_with_status(4))
                    } else {
                        Ok(resp_with_status(1))
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(
            cache_op_status(resp.status),
            Some(CacheOpStatusProto::Applied)
        );
        // The re-plan ran a FULL second round: allocate, rewrite, and a
        // commit carrying the round-2 evidence (not the stale round-1
        // placements).
        assert_eq!(allocs.load(Ordering::SeqCst), 2);
        assert_eq!(rounds.load(Ordering::SeqCst), 2);
        assert_eq!(commits.load(Ordering::SeqCst), 2);
        assert_eq!(evidence.blocks[0].block_id, 2);
        assert_eq!(*committed_rounds.lock().unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn drive_applied_response_loss_never_reallocates() {
        let allocs = AtomicU32::new(0);
        let rounds = AtomicU32::new(0);
        let attempts = AtomicU32::new(0);

        let commit_issued = AtomicBool::new(false);
        let (resp, _evidence) = drive_cache_load(
            || false,
            LocalTime::mills() + 10_000,
            1,
            &commit_issued,
            || {
                allocs.fetch_add(1, Ordering::SeqCst);
                async {
                    Ok(CacheAllocateResponse {
                        object_id: 5,
                        generation: 1,
                        blocks: Vec::new(),
                    })
                }
            },
            |_alloc| {
                rounds.fetch_add(1, Ordering::SeqCst);
                async move { Ok(round_evidence(1)) }
            },
            |_ev| {
                let n = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    // Applied on the master, response lost twice: the
                    // verbatim replay converges to AlreadyApplied.
                    if n < 2 {
                        Err(FsError::common("injected commit response loss"))
                    } else {
                        Ok(resp_with_status(2))
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(
            cache_op_status(resp.status),
            Some(CacheOpStatusProto::AlreadyApplied)
        );
        assert_eq!(
            allocs.load(Ordering::SeqCst),
            1,
            "an applied commit must recover commit-side only, never re-allocate"
        );
        assert_eq!(rounds.load(Ordering::SeqCst), 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn drive_replan_budget_exhaustion_is_loud() {
        let allocs = AtomicU32::new(0);
        let commit_issued = AtomicBool::new(false);
        let err = drive_cache_load(
            || false,
            LocalTime::mills() + 30,
            1,
            &commit_issued,
            || {
                allocs.fetch_add(1, Ordering::SeqCst);
                async {
                    Ok(CacheAllocateResponse {
                        object_id: 5,
                        generation: 1,
                        blocks: Vec::new(),
                    })
                }
            },
            |_alloc| async move { Ok(round_evidence(1)) },
            |_ev| async move { Ok(resp_with_status(4)) },
        )
        .await
        .unwrap_err();
        // Either deadline surface fires loud: the loop-top pre-commit
        // check ("global deadline ... before commit") or the post-REPLAN
        // branch ("replan budget exhausted ... global deadline").
        let msg = err.to_string();
        assert!(
            msg.contains("replan budget") || msg.contains("global deadline"),
            "err: {}",
            msg
        );
        assert!(
            allocs.load(Ordering::SeqCst) >= 2,
            "at least one full replan round before the budget error"
        );
        assert!(
            !commit_issued.load(Ordering::Acquire),
            "REPLAN_NEEDED resolved the ambiguity: the exhausted budget takes the safe abort"
        );
    }

    // gpt56 `e9c554b2` whole-task absolute deadline seams: a task past its
    // global deadline must NEVER send a Commit, and the failure must leave
    // the ambiguity flag in the state that allows the SAFE abort. The
    // deadline is one absolute instant shared by every phase (block
    // writes, replan rounds, commit retries) — no per-phase budget.

    #[test]
    fn deadline_predicate_compares_absolute_instant() {
        assert!(deadline_exceeded(LocalTime::mills().saturating_sub(1)));
        assert!(!deadline_exceeded(LocalTime::mills() + 60_000));
    }

    #[tokio::test]
    async fn drive_global_deadline_blocks_commit_after_slow_first_round() {
        let commits = AtomicU32::new(0);
        let commit_issued = AtomicBool::new(false);
        let err = drive_cache_load(
            || false,
            LocalTime::mills() + 20,
            1,
            &commit_issued,
            || async {
                Ok(CacheAllocateResponse {
                    object_id: 5,
                    generation: 1,
                    blocks: Vec::new(),
                })
            },
            |_alloc| async move {
                // The first write round alone outlives the whole task —
                // N slow blocks consume ONE budget, not N.
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                Ok(round_evidence(1))
            },
            |_ev| {
                commits.fetch_add(1, Ordering::SeqCst);
                async move { Ok(resp_with_status(1)) }
            },
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("global deadline"), "err: {}", err);
        assert_eq!(
            commits.load(Ordering::SeqCst),
            0,
            "no commit may be sent past the global deadline"
        );
        assert!(
            !commit_issued.load(Ordering::Acquire),
            "flag never set: the safe abort remains allowed"
        );
    }

    #[tokio::test]
    async fn drive_global_deadline_blocks_second_commit_after_replan() {
        let commits = AtomicU32::new(0);
        let commit_issued = AtomicBool::new(false);
        let err = drive_cache_load(
            || false,
            LocalTime::mills() + 40,
            // The replan backoff alone carries the task past its deadline.
            45,
            &commit_issued,
            || async {
                Ok(CacheAllocateResponse {
                    object_id: 5,
                    generation: 1,
                    blocks: Vec::new(),
                })
            },
            |_alloc| async move { Ok(round_evidence(1)) },
            |_ev| {
                let n = 1 + commits.fetch_add(1, Ordering::SeqCst);
                async move { Ok(resp_with_status(if n == 1 { 4 } else { 1 })) }
            },
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("global deadline"), "err: {}", err);
        assert_eq!(
            commits.load(Ordering::SeqCst),
            1,
            "round 1 committed; the expired round 2 must not"
        );
        assert!(
            !commit_issued.load(Ordering::Acquire),
            "REPLAN_NEEDED resolved round 1's ambiguity before the deadline error: safe abort allowed"
        );
    }

    // gpt56 `fca627f5` gate-2 runner seam: abort is warranted ONLY when
    // no commit was ever issued — including after a commit transport
    // error, where the outcome is unknown and aborting could tombstone a
    // row the (lost-response) commit just published over.

    #[tokio::test]
    async fn drive_marks_commit_issued_before_first_send_even_on_transport_err() {
        let commit_issued = AtomicBool::new(false);
        let flag_at_entry = std::sync::Mutex::new(Vec::new());
        let err = drive_cache_load(
            || false,
            LocalTime::mills() + 20,
            1,
            &commit_issued,
            || async {
                Ok(CacheAllocateResponse {
                    object_id: 5,
                    generation: 1,
                    blocks: Vec::new(),
                })
            },
            |_alloc| async move { Ok(round_evidence(1)) },
            |_ev| {
                flag_at_entry
                    .lock()
                    .unwrap()
                    .push(commit_issued.load(Ordering::Acquire));
                async move { Err(FsError::common("injected commit transport loss")) }
            },
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("commit transport loss"),
            "err: {}",
            err
        );
        assert!(
            !flag_at_entry.lock().unwrap().is_empty(),
            "commit was attempted"
        );
        assert!(
            flag_at_entry.lock().unwrap().iter().all(|v| *v),
            "flag must be set BEFORE every commit send, observed: {:?}",
            flag_at_entry.lock().unwrap()
        );
        // After the transport error the flag stays set: abort forbidden.
        let sends = AtomicU32::new(0);
        let outcome = best_effort_abort_after_failure(&commit_issued, 3, 1, || {
            sends.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        })
        .await;
        assert_eq!(outcome, AbortOutcome::Forbidden);
        assert_eq!(sends.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn abort_is_attempted_after_write_failure_with_no_commit_issued() {
        let commit_issued = AtomicBool::new(false);
        let err = drive_cache_load(
            || false,
            LocalTime::mills() + 10_000,
            1,
            &commit_issued,
            || async {
                Ok(CacheAllocateResponse {
                    object_id: 5,
                    generation: 1,
                    blocks: Vec::new(),
                })
            },
            |_alloc| async move { Err(FsError::common("injected worker write failure")) },
            |_ev| async move { Ok(resp_with_status(1)) },
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("worker write failure"),
            "err: {}",
            err
        );
        assert!(!commit_issued.load(Ordering::Acquire));
        let sends = AtomicU32::new(0);
        let outcome = best_effort_abort_after_failure(&commit_issued, 3, 1, || {
            sends.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        })
        .await;
        assert_eq!(outcome, AbortOutcome::Aborted);
        assert_eq!(sends.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn abort_retry_budget_is_bounded_and_loud() {
        let commit_issued = AtomicBool::new(false);
        let sends = AtomicU32::new(0);
        let outcome = best_effort_abort_after_failure(&commit_issued, 3, 1, || {
            sends.fetch_add(1, Ordering::SeqCst);
            async { Err(FsError::common("master unreachable")) }
        })
        .await;
        assert_eq!(outcome, AbortOutcome::Exhausted);
        assert_eq!(sends.load(Ordering::SeqCst), 3);
    }

    // gpt56 `cfa2f0d7` blocker 1 seam: a REPLAN_NEEDED resolves the
    // commit ambiguity (the master asserted the commit did NOT apply),
    // so a failure in the SECOND write round may still abort the
    // Reserved row — the flag is cleared only by this typed outcome,
    // never by a transport error.

    #[tokio::test]
    async fn replan_resolves_commit_ambiguity_so_second_round_failure_aborts() {
        let commit_issued = AtomicBool::new(false);
        let rounds = AtomicU32::new(0);
        let err = drive_cache_load(
            || false,
            LocalTime::mills() + 10_000,
            1,
            &commit_issued,
            || async {
                Ok(CacheAllocateResponse {
                    object_id: 5,
                    generation: 1,
                    blocks: Vec::new(),
                })
            },
            |_alloc| {
                let round = 1 + rounds.fetch_add(1, Ordering::SeqCst);
                async move {
                    if round == 1 {
                        Ok(round_evidence(1))
                    } else {
                        Err(FsError::common("injected second-round write failure"))
                    }
                }
            },
            |_ev| async move { Ok(resp_with_status(4)) },
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("second-round write failure"),
            "err: {}",
            err
        );
        // The typed REPLAN_NEEDED cleared the ambiguity flag: the abort
        // is warranted again for the still-Reserved row.
        assert!(!commit_issued.load(Ordering::Acquire));
        let sends = AtomicU32::new(0);
        let outcome = best_effort_abort_after_failure(&commit_issued, 3, 1, || {
            sends.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        })
        .await;
        assert_eq!(outcome, AbortOutcome::Aborted);
        assert_eq!(sends.load(Ordering::SeqCst), 1);
    }

    // gpt56 `cfa2f0d7` blocker 2 seam: only terminal statuses count as a
    // cleaned row; anything else is a retryable failure.
    #[test]
    fn abort_outcome_accepts_only_terminal_statuses() {
        let mk = |status: Option<i32>| curvine_proto::CacheAbortResponse {
            status,
            current_generation: Some(0),
        };
        for terminal in [1i32, 2, 3] {
            abort_outcome(&mk(Some(terminal))).unwrap();
        }
        for non_terminal in [Some(4i32), None, Some(99)] {
            let err = abort_outcome(&mk(non_terminal)).unwrap_err();
            assert!(format!("{}", err).contains("non-terminal"), "err: {}", err);
        }
    }

    #[tokio::test]
    async fn abort_retries_on_non_terminal_status_then_converges() {
        let commit_issued = AtomicBool::new(false);
        let sends = AtomicU32::new(0);
        let outcome = best_effort_abort_after_failure(&commit_issued, 3, 1, || {
            let n = sends.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(FsError::common("abort returned non-terminal status"))
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert_eq!(outcome, AbortOutcome::Aborted);
        assert_eq!(sends.load(Ordering::SeqCst), 3);
    }
}
