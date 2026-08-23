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
use std::sync::Arc;

const READ_CHUNK_BYTES: i64 = 16 * 1024 * 1024;

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

        // CacheAllocate with the master-minted retry-stable load token.
        // Zero-length files do NOT skip this: the service accepts len 0
        // and returns an empty plan (hard gate 1).
        let client = self.fs.fs_client();
        let alloc = client
            .cache_allocate(
                spec.load_token,
                spec.incarnation,
                &spec.key,
                file_len,
                self.task.info.job.block_size,
            )
            .await?;

        let mut written: i64 = 0;
        let mut committed_blocks = Vec::with_capacity(alloc.blocks.len());
        if file_len > 0 {
            let mut reader = ufs.open(&source_path).await?;
            if reader.len() != file_len {
                return err_box!(
                    "cache load source {} changed length after status (expected {}, reader {})",
                    source_path.full_path(),
                    file_len,
                    reader.len()
                );
            }

            for block in &alloc.blocks {
                let (loaded, completed) = self
                    .write_planned_block(&mut reader, block, written, file_len)
                    .await?;
                written = loaded;
                committed_blocks.push(completed);
            }
            reader.complete().await?;
        } else if !alloc.blocks.is_empty() {
            return err_box!(
                "cache allocate returned {} blocks for a zero-length object",
                alloc.blocks.len()
            );
        }

        if self.task.is_cancel() {
            info!(
                "cache load task canceled before committing: {}",
                self.log_context()
            );
            return self.finish_canceled().await;
        }

        // Self-contained commit carrying the locations that ACTUALLY
        // completed (worker ACKs), never the bare plan.
        let commit = client
            .cache_commit(
                spec.commit_token,
                spec.load_token,
                spec.incarnation,
                &spec.key,
                alloc.generation,
                alloc.object_id,
                file_len,
                ufs_mtime,
                committed_blocks,
            )
            .await?;
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
            // Missing or unknown discriminator: the commit outcome is not
            // interpretable — fail closed instead of assuming Applied.
            None => {
                return err_box!(
                    "cache commit returned unrecognized status {:?}: {}",
                    commit.status,
                    self.log_context()
                );
            }
        }

        self.update_progress(file_len, file_len, true).await;
        info!(
            "cache load task completed: {} object_id={} generation={} file_len={} ufs_mtime={} blocks={}",
            self.log_context(),
            alloc.object_id,
            alloc.generation,
            file_len,
            ufs_mtime,
            alloc.blocks.len()
        );

        Ok(true)
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
        let start_ms = LocalTime::mills();

        let outcome = loop {
            if self.task.is_cancel() {
                break Err(curvine_error::FsError::common("cache load task canceled"));
            }
            if LocalTime::mills() - start_ms > self.task_timeout_ms {
                break err_box!(
                    "Task {} exceed timeout {} ms",
                    self.task.info.task_id,
                    self.task_timeout_ms
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

/// Fail-closed decode of the commit op status. The repo's prost codegen
/// emits no `TryFrom<i32>` for proto2 enums, so this discriminated match
/// restores that contract: a missing or unknown discriminator decodes to
/// `None` and the caller must treat the commit outcome as
/// uninterpretable.
fn cache_op_status(v: Option<i32>) -> Option<CacheOpStatusProto> {
    match v {
        Some(s) if s == CacheOpStatusProto::Applied as i32 => Some(CacheOpStatusProto::Applied),
        Some(s) if s == CacheOpStatusProto::AlreadyApplied as i32 => {
            Some(CacheOpStatusProto::AlreadyApplied)
        }
        Some(s) if s == CacheOpStatusProto::Superseded as i32 => {
            Some(CacheOpStatusProto::Superseded)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{cache_op_status, completed_location};
    use curvine_model::{BlockLocation, CommitBlock, StorageType};
    use curvine_proto::{CacheBlockLocationProto, CacheOpStatusProto, WorkerAddressProto};

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
    }

    #[test]
    fn cache_op_status_fails_closed_on_missing_or_unknown() {
        assert_eq!(cache_op_status(None), None);
        assert_eq!(cache_op_status(Some(0)), None);
        assert_eq!(cache_op_status(Some(4)), None);
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
}
