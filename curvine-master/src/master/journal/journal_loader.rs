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

#![allow(clippy::needless_range_loop)]

use crate::master::fs::MasterFilesystem;
use crate::master::journal::*;
use crate::master::meta::inode::InodeView::File;
use crate::master::meta::inode::{InodePath, InodeView};
use crate::master::meta::InodeId;
use crate::master::{JobManager, Master, MasterMetrics, MountManager, SyncFsDir};
use curvine_config::JournalConf;
use curvine_core_error::{err_box, ternary, CommonResult};
use curvine_error::FsError;
use curvine_model::RenameFlags;
use curvine_raft::conf::JournalConfExt;
use curvine_raft::proto::raft::{AppliedIndex, FsmState, SnapshotData};
use curvine_raft::raft::storage::{AppStorage, ApplyMsg, LogStorage, RocksLogStorage};
use curvine_raft::raft::{RaftClient, RaftResult, RaftUtils};
use curvine_runtime::common::{FileUtils, TimeSpent};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel};
use log::{debug, error, info, warn};
use raft::eraftpb::{Entry, EntryType};
use raft::StateRole;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use std::{fs, mem};

// Replay the master metadata operation log.
#[derive(Clone)]
pub struct JournalLoader {
    node_id: u64,
    fs_dir: SyncFsDir,
    mnt_mgr: Arc<MountManager>,
    journal_writer: Arc<JournalWriter>,
    ufs_loader: UfsLoader,
    log_store: RocksLogStorage,
    sender: AsyncSender<ApplyMsg>,
    fsm_state: Arc<Mutex<FsmState>>,
    retain_checkpoint_num: usize,
    ignore_reply_error: bool,
    max_retry_num: u64,
    skip_failed_ufs_replay_after_retry: bool,
    batch_size: u64,
    retry_interval: Duration,
    metrics: &'static MasterMetrics,
    has_apply_worker: bool,
}

impl JournalLoader {
    pub fn new_replay_loader(
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
    ) -> CommonResult<Self> {
        let rt = conf.create_runtime();
        let client = RaftClient::from_conf(rt.clone(), conf);
        let journal_writer = Arc::new(JournalWriter::new(true, client, conf)?);
        let log_store = RocksLogStorage::from_conf(conf, false);
        Self::build(
            rt,
            fs_dir,
            mnt_mgr,
            conf,
            job_manager,
            log_store,
            journal_writer,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        rt: Arc<Runtime>,
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
        log_store: RocksLogStorage,
        journal_writer: Arc<JournalWriter>,
    ) -> CommonResult<Self> {
        Self::build(
            rt,
            fs_dir,
            mnt_mgr,
            conf,
            job_manager,
            log_store,
            journal_writer,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        rt: Arc<Runtime>,
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
        log_store: RocksLogStorage,
        journal_writer: Arc<JournalWriter>,
        testing: bool,
    ) -> CommonResult<Self> {
        let ufs_loader = UfsLoader::new(job_manager, conf);
        let (sender, receiver) = AsyncChannel::new(conf.writer_channel_size).split();
        let loader = Self {
            node_id: conf.node_id()?,
            fs_dir,
            mnt_mgr,
            journal_writer,
            ufs_loader,
            log_store,
            sender,
            fsm_state: Arc::new(Mutex::new(FsmState::default())),
            retain_checkpoint_num: 3.max(conf.retain_checkpoint_num),
            ignore_reply_error: conf.ignore_reply_error,
            max_retry_num: conf.max_retry_num,
            skip_failed_ufs_replay_after_retry: conf.skip_failed_ufs_replay_after_retry,
            batch_size: conf.scan_batch_size,
            retry_interval: Duration::from_secs(conf.retry_interval_secs),
            metrics: Master::get_metrics()?,
            has_apply_worker: !testing,
        };

        if !testing {
            let loader1 = loader.clone();
            rt.spawn(async move {
                Self::run_apply(loader1, receiver).await;
            });
        }

        Ok(loader)
    }

    fn fsm_state(&self) -> CommonResult<MutexGuard<'_, FsmState>> {
        match self.fsm_state.lock() {
            Ok(state) => Ok(state),
            Err(e) => err_box!("fsm_state lock poisoned: {}", e),
        }
    }

    fn fsm_state_snapshot(&self) -> CommonResult<FsmState> {
        Ok(self.fsm_state()?.clone())
    }

    fn get_ufs_applied(&self) -> CommonResult<AppliedIndex> {
        Ok(self.fsm_state()?.ufs_applied.clone())
    }

    fn abort_on_fatal_apply_error(message: impl AsRef<str>) -> ! {
        error!(
            "fatal journal apply error: {}; aborting master to avoid serving inconsistent metadata",
            message.as_ref()
        );
        std::process::abort();
    }

    fn set_applied(
        &self,
        is_leader: bool,
        applied: AppliedIndex,
        has_ufs_affecting: bool,
    ) -> CommonResult<()> {
        if is_leader && has_ufs_affecting {
            self.journal_writer
                .log_ufs_applied(applied.op_id, applied.term, applied.index)?;
        }

        let mut state = self.fsm_state()?;
        if is_leader {
            state.ufs_applied = applied.clone();
            state.applied = applied;
        } else {
            state.applied = applied;
        }

        self.metrics.journal_applied.set(state.applied.index as i64);
        self.metrics
            .journal_ufs_applied
            .set(state.ufs_applied.index as i64);
        drop(state);

        let state = self.log_store.hard_state();
        self.metrics.journal_committed.set(state.commit as i64);
        self.metrics.journal_term.set(state.term as i64);

        Ok(())
    }

    fn build_applied(entry: &Entry) -> AppliedIndex {
        AppliedIndex {
            term: entry.term,
            index: entry.index,
            ..Default::default()
        }
    }

    async fn apply0(
        &self,
        is_leader: bool,
        entry: &Entry,
        skip_ufs_error: bool,
    ) -> CommonResult<()> {
        let cur = self.fsm_state_snapshot()?;
        let role_applied = ternary!(is_leader, cur.ufs_applied.index, cur.applied.index);
        if entry.index <= role_applied {
            info!(
                "skip entry index {}, term {}, fsm_state {:?}",
                entry.index, entry.term, cur
            );
            return Ok(());
        }

        // Empty leader no-ops and configuration entries have no metadata mutation,
        // but still advance the committed apply high-water mark.
        if entry.get_entry_type() != EntryType::EntryNormal || entry.data.is_empty() {
            let mut applied = if is_leader {
                cur.ufs_applied
            } else {
                cur.applied
            };
            applied.term = entry.term;
            applied.index = entry.index;
            self.set_applied(is_leader, applied, false)?;
            return Ok(());
        }

        let batch = JournalBatch::deserialize_compat(&entry.data)?;
        let batch_len = batch.len();
        let mut snapshot = None;
        let mut applied = Self::build_applied(entry);
        let mut has_ufs_affecting = false;

        for (seq, op_entry) in batch.batch.into_iter().enumerate() {
            applied.op_id = op_entry.op_id();
            applied.rpc_id = op_entry.rpc_id();

            match op_entry {
                JournalEntry::Snapshot(e) if is_leader && e.node_id == self.node_id => {
                    if seq + 1 != batch_len {
                        return err_box!("snapshot should be the last entry");
                    }
                    snapshot.replace(e);
                    continue;
                }

                JournalEntry::CacheInvalidation(_)
                | JournalEntry::UfsApplied(_)
                | JournalEntry::CacheIdReserve(_)
                | JournalEntry::CacheIncarnationAllocate(_)
                | JournalEntry::CacheIncarnationRevoke(_)
                | JournalEntry::CacheAllocate(_)
                | JournalEntry::CacheCommit(_)
                | JournalEntry::CacheRemove(_) => (),

                _ => has_ufs_affecting = true,
            }

            {
                let fs_dir = self.fs_dir.read();
                if !is_leader {
                    if let Some(inode_id) = op_entry.allocated_inode_id() {
                        let last_inode_id = fs_dir.last_inode_id();
                        if inode_id <= last_inode_id {
                            return err_box!(
                                "refusing duplicate inode allocation during follower replay: inode_id={}, last_inode_id={}, journal={:?}",
                                inode_id,
                                last_inode_id,
                                op_entry
                            );
                        }
                    }
                }
                fs_dir.update_op_id(op_entry.op_id());
                if let Some(inode_id) = op_entry.inode_id() {
                    fs_dir.update_last_inode_id(inode_id)?;
                }
            }

            let res = if op_entry.is_cache_entry() {
                // Cache-mode entries apply through the single committed
                // CacheManager path on leader AND follower — never the
                // leader UFS loader (no pre-apply, no UFS side effects).
                let fs_dir = self.fs_dir.read();
                fs_dir.apply_cache_journal_entry(&op_entry)
            } else if is_leader {
                self.ufs_loader.apply_entry(&op_entry).await
            } else {
                self.apply_entry(op_entry.clone())
            };

            if let Err(e) = res {
                // The UFS retry-skip exemption exists only for the legacy
                // UFS-affecting replay path. A failed cache-mode apply means
                // the authoritative RocksDB state diverged from the journal:
                // acknowledging it (advancing applied) would ACK a command
                // whose durable state was never written. Always fatal.
                if is_leader && skip_ufs_error && !op_entry.is_cache_entry() {
                    error!(
                        "skip failed UFS replay after retries, entry index={}, term={}, journal={:?}, error={}",
                        entry.index, entry.term, op_entry, e
                    );
                    continue;
                }

                return err_box!("failed to apply journal: {:?}: {}", op_entry, e);
            }
        }

        self.set_applied(is_leader, applied, has_ufs_affecting)?;

        if let Some(e) = snapshot {
            let snap_data = self.create_snapshot0(Some(e.dir.to_string()))?;

            self.log_store.create_snapshot(snap_data.clone())?;
            self.log_store.compact(snap_data.fsm_state.compact())?;

            info!(
                "create leader snapshot, dir={}, fsm_state={:?}",
                e.dir, snap_data.fsm_state
            );
        }

        Ok(())
    }

    async fn apply_msg(
        &self,
        is_leader: bool,
        msg: &ApplyMsg,
        skip_ufs_error: bool,
    ) -> CommonResult<()> {
        match msg {
            ApplyMsg::Entry(entry) | ApplyMsg::EntryWithAck((entry, _)) => {
                self.apply0(is_leader, entry, skip_ufs_error).await?;
                Ok(())
            }

            ApplyMsg::Scan(applied_index) => {
                let mut last_applied = applied_index.index;
                if is_leader && skip_ufs_error {
                    last_applied = last_applied.max(self.fsm_state_snapshot()?.ufs_applied.index);
                }

                let commit_index = self.log_store.hard_state().commit;
                loop {
                    if last_applied >= commit_index {
                        return Ok(());
                    }

                    let high = (last_applied + self.batch_size).min(commit_index + 1);
                    let list = self.log_store.scan_entries(last_applied + 1, high)?;

                    if list.is_empty() {
                        return Ok(());
                    };

                    info!(
                        "replay-scan, start_index: {}, entries: {}, commit_index: {}",
                        last_applied + 1,
                        list.len(),
                        commit_index
                    );

                    for entry in list {
                        self.apply0(is_leader, &entry, skip_ufs_error).await?;
                        last_applied = entry.index;
                        if skip_ufs_error {
                            return Ok(());
                        }
                    }
                }
            }

            _ => err_box!("unsupported apply message in journal loader apply_msg"),
        }
    }

    fn complete_entry_ack(msg: ApplyMsg, result: RaftResult<()>) {
        if let ApplyMsg::EntryWithAck((_, tx)) = msg {
            if let Err(e) = tx.send(result) {
                warn!("send journal entry apply acknowledgement failed: {}", e);
            }
        }
    }

    async fn catch_up_committed_metadata(&self) -> RaftResult<()> {
        let committed = self.log_store.hard_state().commit;
        let applied = self.fsm_state_snapshot()?.applied;
        self.apply_msg(false, &ApplyMsg::new_scan(applied), false)
            .await?;

        let metadata_applied = self.fsm_state_snapshot()?.applied.index;
        if metadata_applied < committed {
            return err_box!(
                "metadata catch-up stopped before committed raft index: applied={}, commit={}",
                metadata_applied,
                committed
            );
        }
        Ok(())
    }
    async fn next_apply_msg(
        &self,
        receiver: &mut AsyncReceiver<ApplyMsg>,
        retry_msg: &mut Option<ApplyMsg>,
    ) -> Option<ApplyMsg> {
        match retry_msg.take() {
            Some(msg) => {
                tokio::time::sleep(self.retry_interval).await;
                Some(msg)
            }
            None => receiver.recv().await,
        }
    }

    async fn run_apply(self, mut receiver: AsyncReceiver<ApplyMsg>) {
        let mut retry_msg: Option<ApplyMsg> = None;
        let mut retry_num: u64 = 0;
        let mut is_leader = false;

        loop {
            let apply_msg = match self.next_apply_msg(&mut receiver, &mut retry_msg).await {
                Some(v) => v,
                None => break,
            };

            match apply_msg {
                ApplyMsg::CreateSnapshot(tx) => {
                    if let Err(e) = tx.send(self.create_snapshot0(None)) {
                        warn!("send create snapshot result failed: {}", e);
                    }
                    retry_num = 0;
                }

                ApplyMsg::ApplySnapshot((tx, snapshot)) => {
                    if let Err(e) = tx.send(self.apply_snapshot0(snapshot)) {
                        warn!("send apply snapshot result failed: {}", e);
                    }
                    retry_num = 0;
                }

                ApplyMsg::RoleChange((role, tx)) => {
                    let result = async {
                        if role == StateRole::Leader {
                            // Publish leadership only after committed namespace mutations
                            // have advanced local metadata high-water marks.
                            self.catch_up_committed_metadata().await?;
                            is_leader = true;
                            let ufs_applied = self.get_ufs_applied()?;
                            info!("metadata caught up for leader promotion, scheduling UFS replay from {:?}", ufs_applied);
                            retry_msg.replace(ApplyMsg::new_scan(ufs_applied));
                        } else {
                            is_leader = false;
                        }
                        Ok(())
                    }
                    .await;

                    if tx.send(result).is_err() {
                        warn!("leader role-change acknowledgement receiver dropped");
                    }
                }

                ApplyMsg::Shutdown(tx) => {
                    let _ = tx.send(());
                    break;
                }

                msg => match self.apply_msg(is_leader, &msg, false).await {
                    Ok(_) => {
                        retry_num = 0;
                        Self::complete_entry_ack(msg, Ok(()));
                    }

                    Err(error) => {
                        if self.ignore_reply_error {
                            error!("apply entry failed(skip): {}", error);
                            Self::complete_entry_ack(msg, Ok(()));
                        } else if is_leader {
                            retry_num += 1;

                            if retry_num >= self.max_retry_num {
                                if self.skip_failed_ufs_replay_after_retry {
                                    error!(
                                        "apply entry failed(retry_num={}), skipping failed UFS replay to keep master alive: {}",
                                        retry_num, error
                                    );
                                    let continue_scan = matches!(&msg, ApplyMsg::Scan(_));
                                    if let Err(skip_error) =
                                        self.apply_msg(is_leader, &msg, true).await
                                    {
                                        Self::abort_on_fatal_apply_error(format!(
                                            "apply entry failed while skipping failed UFS replay: {}",
                                            skip_error
                                        ));
                                    }
                                    retry_num = 0;
                                    if continue_scan {
                                        retry_msg.replace(msg);
                                    } else {
                                        Self::complete_entry_ack(msg, Ok(()));
                                    }
                                } else {
                                    Self::abort_on_fatal_apply_error(format!(
                                        "apply entry failed(retry_num={}): {}",
                                        retry_num, error
                                    ));
                                }
                            } else {
                                error!("apply entry failed(retry_num={}): {}", retry_num, error);
                                retry_msg.replace(msg);
                            }
                        } else {
                            Self::abort_on_fatal_apply_error(format!(
                                "apply entry failed on follower: {}",
                                error
                            ));
                        }
                    }
                },
            }
        }
    }

    fn create_snapshot0(&self, dir_option: Option<String>) -> RaftResult<SnapshotData> {
        let fsm_state = self.fsm_state_snapshot()?;
        let fs_dir = self.fs_dir.read();
        let dir = match dir_option {
            Some(dir) => dir,
            None => fs_dir.create_checkpoint(fsm_state.applied.index)?,
        };

        let data = RaftUtils::create_file_snapshot(&dir, self.node_id, fsm_state)?;

        if let Err(e) = self.purge_checkpoint(&dir) {
            warn!("purge checkpoint: {}", e);
        }

        Ok(data)
    }

    fn apply_snapshot0(&self, snapshot: SnapshotData) -> RaftResult<()> {
        let mut spend = TimeSpent::new();

        // Raft uses the default value as a no-snapshot placeholder. It carries no
        // filesystem state and must not create a restore directory or replace metadata.
        let is_empty_placeholder_snapshot = snapshot.snapshot_id == 0
            && snapshot.files_data.is_none()
            && snapshot.bytes_data.is_none();
        if is_empty_placeholder_snapshot {
            warn!("skip zero-index empty placeholder snapshot");
            return Ok(());
        }

        let restore_path = match &snapshot.files_data {
            Some(data) => data.dir.clone(),
            None => {
                return err_box!(
                    "refusing to apply empty snapshot {} without checkpoint files",
                    snapshot.snapshot_id
                )
            }
        };
        let actual_size = FileUtils::dir_size(&restore_path).unwrap_or_else(|e| {
            warn!(
                "failed to compute checkpoint size for {}: {}",
                restore_path, e
            );
            0
        });
        let checkpoint_size = if log::log_enabled!(log::Level::Info) {
            actual_size
        } else {
            0
        };

        // Never wipe populated metadata with an empty checkpoint. Harness
        // daily failures showed FileNotFound after restore from checkpoint_size=0
        // following raft quorum loss (CurvineIO/curvine#1207).
        // get_file_counts() returns (dir_count, file_count).
        {
            let fs_dir = self.fs_dir.read();
            let (dir_count, file_count) = fs_dir.get_file_counts();
            if actual_size == 0 && (dir_count > 0 || file_count > 0) {
                return err_box!(
                    "refusing to apply empty snapshot {} at {} ({} bytes) over filesystem with {} files and {} dirs",
                    snapshot.snapshot_id,
                    restore_path,
                    actual_size,
                    file_count,
                    dir_count
                );
            }
        }

        let mut fs_dir = self.fs_dir.write();
        fs_dir.restore(&restore_path, checkpoint_size)?;
        fs_dir.update_op_id(snapshot.fsm_state.op_id());
        drop(fs_dir);
        let restore_ms = spend.used_ms();
        spend.reset();

        self.mnt_mgr.restore_best_effort();
        let mount_ms = spend.used_ms();

        *self.fsm_state()? = snapshot.fsm_state;

        info!(
            "apply_snapshot: fs_dir_restore={} ms, mount_restore={} ms, total={} ms",
            restore_ms,
            mount_ms,
            restore_ms + mount_ms
        );

        Ok(())
    }

    pub fn apply_entry(&self, entry: JournalEntry) -> CommonResult<()> {
        debug!("replay entry: {:?}", entry);

        match entry {
            JournalEntry::Mkdir(e) => self.mkdir(e),

            JournalEntry::CreateFile(e) => self.create_file(e),

            JournalEntry::OverWriteFile(e) => self.overwrite_file(e),

            JournalEntry::AddBlock(e) => self.add_block(e),

            JournalEntry::CompleteFile(e) => self.complete_file(e),

            JournalEntry::Rename(e) => self.rename(e),

            JournalEntry::Delete(e) => self.delete(e),

            JournalEntry::Free(e) => self.free(e),

            JournalEntry::CacheInvalidation(e) => self.cache_invalidation(e),

            JournalEntry::ReopenFile(e) => self.reopen_file(e),

            JournalEntry::Mount(e) => self.mount(e),

            JournalEntry::UnMount(e) => self.unmount(e),

            JournalEntry::SetAttr(e) => self.set_attr(e),

            JournalEntry::Symlink(e) => self.symlink(e),

            JournalEntry::Link(e) => self.link(e),

            JournalEntry::SetLocks(e) => self.set_locks(e),

            JournalEntry::UfsApplied(e) => self.ufs_applied(e),

            _ => Ok(()),
        }
    }

    fn cache_invalidation(&self, entry: CacheInvalidationEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        fs_dir.store.apply_cache_invalidations(entry.inodes)
    }

    fn mkdir(&self, entry: MkdirEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), entry.path, &fs_dir.store)?;
        let name = inp.name().to_string();
        let _ = fs_dir.add_last_inode(inp, InodeView::new_dir(name, entry.dir))?;
        Ok(())
    }

    fn create_file(&self, entry: CreateFileEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        if inp.is_full() {
            warn!("create_file: file already exists: {:?}", entry);
            return Ok(());
        }
        let name = inp.name().to_string();
        let _ = fs_dir.add_last_inode(inp, InodeView::new_file(name, entry.file))?;
        Ok(())
    }

    fn reopen_file(&self, entry: ReopenFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("reopen_file: file not found: {:?}", entry);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let _ = mem::replace(file, entry.file);

        fs_dir.store.apply_reopen_file(inode.as_ref())?;

        Ok(())
    }

    fn overwrite_file(&self, entry: OverWriteFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("overwrite_file: file not found: {:?}", entry);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let _ = mem::replace(file, entry.file);

        fs_dir.store.apply_overwrite_file(inode.as_ref())?;

        Ok(())
    }

    fn add_block(&self, entry: AddBlockEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();

        let inode_id = entry.blocks.first().map(|v| InodeId::get_id(v.id));

        let mut inode = match MasterFilesystem::resolve_file_inode(&fs_dir, &entry.path, inode_id) {
            Ok(v) => v,
            Err(e) => {
                warn!("add_block: file not found: {:?} {}", entry, e);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let _ = mem::replace(&mut file.blocks, entry.blocks);
        fs_dir
            .store
            .apply_new_block(inode.as_ref(), &entry.commit_block)?;

        Ok(())
    }

    fn complete_file(&self, entry: CompleteFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();

        let mut inode =
            match MasterFilesystem::resolve_file_inode(&fs_dir, &entry.path, Some(entry.file.id)) {
                Ok(v) => v,
                Err(e) => {
                    warn!("complete_file: file not found: {:?} {}", entry, e);
                    return Ok(());
                }
            };
        let file = inode.as_file_mut()?;

        let _ = mem::replace(file, entry.file);
        // Update block location
        fs_dir
            .store
            .apply_complete_file(inode.as_ref(), &entry.commit_blocks)?;

        Ok(())
    }
    pub fn rename(&self, entry: RenameEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let entry_src = entry.src;
        let src_inp = InodePath::resolve(fs_dir.root_ptr(), &entry_src, &fs_dir.store)?;
        let dst_inp = InodePath::resolve(fs_dir.root_ptr(), entry.dst, &fs_dir.store)?;
        if src_inp.get_last_inode().is_none() {
            warn!("Rename: source path not found: {}", entry_src);
            return Ok(());
        }
        fs_dir.unprotected_rename(
            &src_inp,
            &dst_inp,
            entry.mtime,
            RenameFlags::new(entry.flags),
            if RenameFlags::new(entry.flags).exchange_mode()
                && entry.src_inode_id != 0
                && entry.dst_inode_id != 0
            {
                Some((entry.src_inode_id, entry.dst_inode_id))
            } else {
                None
            },
        )?;

        Ok(())
    }

    pub fn delete(&self, entry: DeleteEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let entry_path = entry.path;
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry_path, &fs_dir.store)?;
        if inp.get_last_inode().is_none() {
            warn!("Delete: path not found: {}", entry_path);
            return Ok(());
        }
        fs_dir.unprotected_delete(&inp, entry.mtime)?;
        Ok(())
    }

    pub fn free(&self, entry: FreeEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;
        let Some(inode) = inp.get_last_inode() else {
            warn!("Free: path not found: {:?}", entry);
            return Ok(());
        };
        fs_dir.unprotected_free(inode, entry.mtime, entry.recursive)?;
        Ok(())
    }

    pub fn mount(&self, entry: MountEntry) -> CommonResult<()> {
        self.mnt_mgr.unprotected_add_mount(entry.info.clone())?;

        let mut fs_dir = self.fs_dir.write();
        fs_dir.unprotected_store_mount(entry.info)?;
        Ok(())
    }

    pub fn unmount(&self, entry: UnMountEntry) -> CommonResult<()> {
        if !self.mnt_mgr.has_mounted(entry.id)? {
            warn!("Unmount: id already unmounted: {:?}", entry);
            return Ok(());
        }
        self.mnt_mgr.unprotected_umount_by_id(entry.id)?;
        let mut fs_dir = self.fs_dir.write();
        fs_dir.unprotected_unmount(entry.id)?;
        Ok(())
    }

    pub fn set_attr(&self, entry: SetAttrEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;
        let last_inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("SetAttr: path not found: {:?}", entry);
                return Ok(());
            }
        };

        fs_dir.unprotected_set_attr(last_inode, entry.opts)?;
        Ok(())
    }

    pub fn symlink(&self, entry: SymlinkEntry) -> CommonResult<()> {
        let link_path = entry.link;
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &link_path, &fs_dir.store)?;
        match fs_dir.unprotected_symlink(inp, entry.new_inode, entry.force) {
            Ok(_) => Ok(()),
            Err(FsError::FileAlreadyExists(_)) => {
                warn!("Symlink: file already exists: {:?}", link_path);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn link(&self, entry: LinkEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let old_path = InodePath::resolve(fs_dir.root_ptr(), &entry.src_path, &fs_dir.store)?;
        let new_path = InodePath::resolve(fs_dir.root_ptr(), &entry.dst_path, &fs_dir.store)?;

        // Get the original inode ID
        let original_inode_id = match old_path.get_last_inode() {
            Some(inode) => inode.id(),
            None => {
                warn!("Link: source path not found: {:?}", entry);
                return Ok(());
            }
        };

        if let Some(mut inode_ptr) = old_path.get_last_inode() {
            if let File(_) = inode_ptr.as_mut() {
                inode_ptr.incr_nlink(entry.mtime);
            }
        }

        match fs_dir.unprotected_link(new_path, original_inode_id, entry.mtime as u64) {
            Ok(_) => Ok(()),
            Err(FsError::FileAlreadyExists(_)) => {
                warn!("Link: dst_path already exists: {:?}", entry);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_locks(&self, entry: SetLocksEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        fs_dir.store.apply_set_locks(entry.ino, &entry.locks)
    }

    pub fn ufs_applied(&self, entry: UfsAppliedEntry) -> CommonResult<()> {
        let mut lock = self.fsm_state()?;
        lock.ufs_applied = AppliedIndex {
            op_id: entry.op_id,
            rpc_id: entry.rpc_id,
            term: entry.term,
            index: entry.index,
        };
        Ok(())
    }

    // Clean up expired checkpoints.
    pub fn purge_checkpoint(&self, current_ck: impl AsRef<str>) -> CommonResult<()> {
        let current_ck = current_ck.as_ref();
        let ck_dir = match Path::new(current_ck).parent() {
            None => return Ok(()),
            Some(v) => v,
        };

        let current_mtime = match Path::new(current_ck).metadata() {
            Ok(meta) => FileUtils::mtime(&meta)?,
            Err(_) => return Ok(()),
        };

        let mut vec = vec![];
        for entry in fs::read_dir(ck_dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            let mtime = FileUtils::mtime(&meta)?;
            if mtime < current_mtime {
                vec.push((mtime, entry.path()));
            }
        }

        // Sort oldest-first and keep at most (retain_checkpoint_num - 1) older
        // checkpoints so that together with current_ck the total is retain_checkpoint_num.
        vec.sort_by_key(|x| x.0);
        let keep = self.retain_checkpoint_num.saturating_sub(1);
        let del_num = vec.len().saturating_sub(keep);

        for (_, path) in vec.iter().take(del_num) {
            let path = path.as_path();
            FileUtils::delete_path(path, true)?;
            info!("delete expired checkpoint: {}", path.to_string_lossy());
        }

        Ok(())
    }

    pub async fn shutdown(&self) -> RaftResult<()> {
        let (tx, rx) = CallChannel::channel();
        self.sender.send(ApplyMsg::Shutdown(tx)).await?;
        rx.receive().await?;
        Ok(())
    }

    async fn apply_direct(&self, msg: ApplyMsg) -> RaftResult<()> {
        let result = if let Err(e) = self.apply_msg(false, &msg, false).await {
            if self.ignore_reply_error {
                error!("apply entry failed: {}", e);
                Ok(())
            } else {
                Err(e.into())
            }
        } else {
            Ok(())
        };

        if matches!(&msg, ApplyMsg::EntryWithAck(_)) {
            Self::complete_entry_ack(msg, result);
            Ok(())
        } else {
            result
        }
    }
}

impl AppStorage for JournalLoader {
    async fn apply(&self, wait: bool, msg: ApplyMsg) -> RaftResult<()> {
        if !self.has_apply_worker {
            return self.apply_direct(msg).await;
        }

        if matches!(&msg, ApplyMsg::EntryWithAck(_)) {
            self.sender.send(msg).await?;
            return Ok(());
        }

        if wait {
            return self.apply_direct(msg).await;
        }

        self.sender.send(msg).await?;
        Ok(())
    }

    fn get_fsm_state(&self) -> FsmState {
        match self.fsm_state.lock() {
            Ok(state) => state.clone(),
            Err(e) => {
                error!("fatal fsm_state lock poisoned: {}", e);
                std::process::abort();
            }
        }
    }

    async fn role_change(&self, role: StateRole) -> RaftResult<()> {
        if !self.has_apply_worker {
            if role == StateRole::Leader {
                self.catch_up_committed_metadata().await?;
            }
            return Ok(());
        }
        let (tx, rx) = CallChannel::channel();
        self.sender.send(ApplyMsg::RoleChange((role, tx))).await?;
        rx.receive().await?
    }

    async fn create_snapshot(&self) -> RaftResult<SnapshotData> {
        if !self.has_apply_worker {
            return self.create_snapshot0(None);
        }
        let (tx, rx) = CallChannel::channel();
        let msg = ApplyMsg::CreateSnapshot(tx);

        self.sender.send(msg).await?;
        rx.receive().await?
    }

    async fn apply_snapshot(&self, snapshot: SnapshotData) -> RaftResult<()> {
        if !self.has_apply_worker {
            return self.apply_snapshot0(snapshot);
        }
        let (tx, rx) = CallChannel::channel();
        let msg = ApplyMsg::ApplySnapshot((tx, snapshot));

        self.sender.send(msg).await?;
        rx.receive().await?
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String> {
        let fs_dir = self.fs_dir.read();
        Ok(fs_dir.get_checkpoint_path(snapshot_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::fs::{MasterFilesystem, WorkerManager};
    use crate::master::meta::cache::{
        state_tags, BlockIdCodec, LocalCacheIndexStore, OpOutcome, OpToken,
    };
    use crate::master::meta::inode::ttl::TtlBucketList;
    use crate::master::meta::store::RocksInodeStore;
    use crate::master::meta::FsDir;
    use crate::master::quota::eviction::evictor::{Evictor, LRUEvictor};
    use crate::master::quota::eviction::EvictionConf;
    use crate::master::{MasterMonitor, MetaRaftJournal, SyncFsDir, SyncWorkerManager};
    use curvine_config::{ClusterConf, JournalConf, MasterConf};
    use curvine_raft::raft::RoleMonitor;
    use curvine_runtime::common::{FileUtils, SerdeUtils, Utils};
    use curvine_runtime::sync::StateCtl;
    use std::sync::Arc;

    fn test_conf(name: &str) -> ClusterConf {
        let mut conf = ClusterConf {
            testing: true,
            format_master: true,
            journal: JournalConf {
                enable: true,
                ..Default::default()
            },
            ..Default::default()
        };
        conf.change_test_meta_dir(name);
        conf
    }

    fn build_loader(conf: &ClusterConf) -> JournalLoader {
        Master::init_test_metrics();

        let rt = conf.journal.create_runtime();
        let client = RaftClient::from_conf(rt.clone(), &conf.journal);
        let journal_writer = Arc::new(JournalWriter::new(true, client, &conf.journal).unwrap());

        let ttl_bucket_list =
            Arc::new(TtlBucketList::new(conf.master.ttl_bucket_interval_ms() as i64).unwrap());
        let eviction_conf = EvictionConf::from_conf(conf);
        let evictor: Arc<dyn Evictor> = Arc::new(LRUEvictor::new(eviction_conf.clone()));
        let fs_dir =
            SyncFsDir::new(FsDir::new(conf, journal_writer, ttl_bucket_list, evictor).unwrap());

        let master_monitor = MasterMonitor::new(StateCtl::new(0), StateCtl::new(0));
        let fs = MasterFilesystem::new(
            conf,
            fs_dir.clone(),
            SyncWorkerManager::new(WorkerManager::new(conf).unwrap()),
            master_monitor,
        );
        let mount_manager = Arc::new(MountManager::new(fs.clone()));
        let job_manager = Arc::new(JobManager::from_cluster_conf(
            fs,
            mount_manager.clone(),
            rt,
            conf,
        ));

        JournalLoader::new_replay_loader(fs_dir, mount_manager, &conf.journal, job_manager).unwrap()
    }

    fn cache_commit_entry() -> JournalEntry {
        // A commit for a key with no entry: legal journal bytes, but the
        // committed apply fails loudly (missing entry CAS).
        JournalEntry::CacheCommit(CacheCommitEntry {
            op_id: 1,
            rpc_id: 0,
            incarnation: 1,
            key: "/missing".into(),
            generation: 1,
            expected_object_id: BlockIdCodec::CACHE_OBJECT_MIN,
            len: 1,
            ufs_mtime: 1,
            expire_at: 0,
        })
    }

    fn raft_entry(index: u64, entry: JournalEntry) -> Entry {
        let mut batch = JournalBatch::new(0);
        batch.push(entry);
        let bytes = SerdeUtils::serialize(&batch).unwrap();
        let mut e = Entry::default();
        e.set_entry_type(EntryType::EntryNormal);
        e.term = 1;
        e.index = index;
        e.set_data(bytes);
        e
    }

    /// A failed cache-mode apply must be fatal even when the leader replay
    /// runs with skip_failed_ufs_replay_after_retry: acknowledging it would
    /// advance applied past a command whose authoritative state was never
    /// written (gpt56 snapshot review, blocker 3).
    #[test]
    fn cache_apply_error_is_fatal_even_with_ufs_skip() {
        let conf = test_conf("cache-apply-fatal");
        let loader = build_loader(&conf);
        let rt = conf.journal.create_runtime();

        let result = rt.block_on(async {
            loader
                .apply_msg(
                    true,
                    &ApplyMsg::new_entry(raft_entry(1, cache_commit_entry())),
                    true, // skip_ufs_error: must NOT exempt cache entries
                )
                .await
        });

        assert!(result.is_err(), "malformed cache entry must fail the apply");
        let state = loader.fsm_state_snapshot().unwrap();
        assert_eq!(
            state.applied.index, 0,
            "applied must not advance past a failed cache apply"
        );
        assert_eq!(
            state.ufs_applied.index, 0,
            "ufs_applied must not advance past a failed cache apply"
        );
    }

    /// The happy-path cache entry applies through the same committed path
    /// with skip enabled and does advance applied.
    #[test]
    fn cache_entry_apply_advances_applied() {
        let conf = test_conf("cache-apply-ok");
        let loader = build_loader(&conf);
        let rt = conf.journal.create_runtime();

        let entry = JournalEntry::CacheIdReserve(CacheIdReserveEntry {
            op_id: 1,
            rpc_id: 0,
            token: OpToken {
                client_id: 7,
                op_seq: 1,
            },
            start: crate::master::meta::cache::BlockIdCodec::CACHE_OBJECT_MIN,
            end: crate::master::meta::cache::BlockIdCodec::CACHE_OBJECT_MIN + 10,
        });

        let result = rt.block_on(async {
            loader
                .apply_msg(true, &ApplyMsg::new_entry(raft_entry(1, entry)), true)
                .await
        });
        result.unwrap();

        let state = loader.fsm_state_snapshot().unwrap();
        assert_eq!(state.applied.index, 1);
    }

    /// Fake-ACK crash boundary (contract §7): the leader applies a cache
    /// command (the point where the RPC would be ACKed), then the process
    /// crashes before the client observes the ACK. On restart the journal
    /// replay of the same entry must (a) succeed and (b) NOT execute the
    /// command a second time — the persisted idempotency outcome is the
    /// single source of identity. A follower replaying the same entry takes
    /// the identical committed path and reaches the identical state.
    #[test]
    fn fake_ack_crash_then_restart_replay_does_not_reexecute() {
        let name = format!("fake-ack-{}", Utils::rand_str(6));
        let conf = test_conf(&name);

        let id_reserve = || {
            JournalEntry::CacheIdReserve(CacheIdReserveEntry {
                op_id: 1,
                rpc_id: 0,
                token: OpToken {
                    client_id: 7,
                    op_seq: 1,
                },
                start: BlockIdCodec::CACHE_OBJECT_MIN,
                end: BlockIdCodec::CACHE_OBJECT_MIN + 10,
            })
        };

        // Leader lifetime: apply the entry (this is the ACK boundary).
        let durable_after_leader;
        {
            let loader = build_loader(&conf);
            let rt = conf.journal.create_runtime();
            rt.block_on(async {
                loader
                    .apply_msg(
                        true,
                        &ApplyMsg::new_entry(raft_entry(1, id_reserve())),
                        false,
                    )
                    .await
            })
            .unwrap();
            assert_eq!(loader.fsm_state_snapshot().unwrap().applied.index, 1);

            let store = loader.fs_dir.read();
            let rocks = store.get_rocks_store();
            durable_after_leader = (
                rocks.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
                rocks
                    .cache_get_outcome(OpToken {
                        client_id: 7,
                        op_seq: 1,
                    })
                    .unwrap(),
            );
        }
        // (loader + FsDir dropped: simulated process crash)

        assert_eq!(
            durable_after_leader.0,
            Some(BlockIdCodec::CACHE_OBJECT_MIN + 9),
            "leader apply must have durably advanced the watermark"
        );

        // Restart: a fresh FsDir over the SAME RocksDB dir replays the same
        // journal entry — both as leader-replay and as follower — and must
        // converge without re-executing (outcome and watermark unchanged).
        for is_leader in [true, false] {
            let mut conf2 = test_conf(&name);
            conf2.format_master = false;
            let loader2 = build_loader(&conf2);
            let rt2 = conf2.journal.create_runtime();

            rt2.block_on(async {
                loader2
                    .apply_msg(
                        is_leader,
                        &ApplyMsg::new_entry(raft_entry(1, id_reserve())),
                        false,
                    )
                    .await
            })
            .unwrap_or_else(|e| {
                panic!("restart replay (leader={}) must converge: {}", is_leader, e)
            });

            let store = loader2.fs_dir.read();
            let rocks = store.get_rocks_store();
            assert_eq!(
                rocks.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
                durable_after_leader.0,
                "restart replay (leader={}) must not move the watermark",
                is_leader
            );
            assert_eq!(
                rocks
                    .cache_get_outcome(OpToken {
                        client_id: 7,
                        op_seq: 1
                    })
                    .unwrap(),
                durable_after_leader.1,
                "restart replay (leader={}) must not rewrite the outcome",
                is_leader
            );
        }
    }

    /// Real single-voter Raft + real JournalWriter fake-ACK fault test
    /// (1c review blocker 6). The full production chain is exercised:
    /// `sync_propose_cache` → RaftClient RPC → raft commit → apply worker →
    /// `EntryWithAck` ack → `ProposeResponse{applied_index}`. The raft run
    /// task holds the runtime (and through the loader, the metadata
    /// RocksDB) until process exit, so the crash is simulated the faithful
    /// way: the leader stack runs in a child process that simply exits
    /// right after the barrier response — no graceful close whatsoever.
    /// The parent then reopens the same RocksDB and the committed raft log
    /// as the restarting master: the outcome and watermark must already be
    /// durable, and replaying the committed entry (leader AND follower)
    /// must converge without re-executing the command.
    #[test]
    fn real_raft_sync_propose_barrier_survives_leader_crash() {
        let leader_mode_env = "CURVINE_TEST_CACHE_FAKE_ACK_LEADER";
        let meta_dir_env = "CURVINE_TEST_CACHE_FAKE_ACK_META_DIR";
        let journal_dir_env = "CURVINE_TEST_CACHE_FAKE_ACK_JOURNAL_DIR";

        if std::env::var(leader_mode_env).is_ok() {
            // Child: the leader lifetime. Exits right after the barrier ACK
            // — this process exit IS the crash under test.
            real_raft_leader_lifetime(meta_dir_env, journal_dir_env);
            return;
        }

        // Parent: fresh dirs, spawn the leader process on them, wait.
        Master::init_test_metrics();
        let base = Utils::cur_dir_sub(format!(
            "../target/testing/real-raft-fake-ack-{}-{}",
            std::process::id(),
            Utils::rand_str(6)
        ));
        let meta_dir = format!("{}/meta", base);
        let journal_dir = format!("{}/journal", base);
        let _ = FileUtils::delete_path(&base, true);

        let exe = std::env::current_exe().unwrap();
        let output = std::process::Command::new(exe)
            .arg("--exact")
            .arg("master::journal::journal_loader::tests::real_raft_sync_propose_barrier_survives_leader_crash")
            .env(leader_mode_env, "1")
            .env(meta_dir_env, &meta_dir)
            .env(journal_dir_env, &journal_dir)
            .output()
            .expect("spawn leader test process");
        assert!(
            output.status.success(),
            "leader lifetime failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let token = OpToken {
            client_id: 7,
            op_seq: 1,
        };
        let conf = || {
            let mut journal = JournalConf::with_test();
            journal.enable = true;
            journal.journal_dir = journal_dir.clone();
            ClusterConf {
                testing: true,
                format_master: false,
                journal,
                master: MasterConf {
                    meta_dir: meta_dir.clone(),
                    ..Default::default()
                },
                ..Default::default()
            }
        };

        // The committed raft log survived the crash: read the one data
        // entry back from the journal RocksDB.
        let committed_entry = {
            let log_store = RocksLogStorage::from_conf(&conf().journal, false);
            let last = log_store.read().last_index();
            assert!(last >= 1, "journal log must exist after leader crash");
            let entries = log_store.scan_entries(1, last + 1).unwrap();
            drop(log_store);
            let data_entries: Vec<Entry> = entries
                .into_iter()
                .filter(|e| e.get_entry_type() == EntryType::EntryNormal && !e.data.is_empty())
                .collect();
            assert_eq!(
                data_entries.len(),
                1,
                "exactly one committed data entry expected"
            );
            data_entries.into_iter().next().unwrap()
        };

        // All-or-nothing after abrupt exit (round-2 review): the leader
        // died via std::process::exit right after the barrier ACK — no
        // graceful close, no destructors. Cache identity writes commit with
        // per-write WAL + fsync (write_batch_durable), overriding the meta
        // DB's disable_wal default, so reopening the meta RocksDB BEFORE
        // any journal replay must show the whole identity batch: reserve
        // watermark, token outcome, and client watermark together, or none
        // of them. A partial batch (e.g. only the client watermark) would
        // make the replayed Expired no-op forever unable to rebuild the
        // identity.
        {
            let rocks = RocksInodeStore::new(conf().db_conf(), false).unwrap();
            assert_eq!(
                rocks.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
                Some(BlockIdCodec::CACHE_OBJECT_MIN + 9),
                "pre-replay: the durable reserve watermark must have survived the abrupt exit"
            );
            assert_eq!(
                rocks.cache_get_outcome(token).unwrap(),
                Some(OpOutcome::Reserved {
                    start: BlockIdCodec::CACHE_OBJECT_MIN,
                    end: BlockIdCodec::CACHE_OBJECT_MIN + 10,
                }),
                "pre-replay: the token outcome must have survived the abrupt exit"
            );
            assert_eq!(
                rocks.cache_client_watermark(7).unwrap(),
                Some(1),
                "pre-replay: the client watermark must have survived the abrupt exit"
            );
        }

        // Replaying the committed entry must (re)derive the exact single
        // identity, and every further replay — the recovering leader, a
        // follower, and a post-recovery client retry of the same token —
        // must converge on it without minting a second identity.
        for (round, is_leader) in [(0, true), (1, false), (2, true)] {
            let loader = build_loader(&conf());
            let rt = conf().journal.create_runtime();
            rt.block_on(async {
                loader
                    .apply_msg(
                        is_leader,
                        &ApplyMsg::new_entry(committed_entry.clone()),
                        false,
                    )
                    .await
            })
            .unwrap_or_else(|e| {
                panic!(
                    "restart replay (round {}, leader={}) must converge: {}",
                    round, is_leader, e
                )
            });

            let store = loader.fs_dir.read();
            let rocks = store.get_rocks_store();
            assert_eq!(
                rocks.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
                Some(BlockIdCodec::CACHE_OBJECT_MIN + 9),
                "round {}: the single segment identity must be [OBJ, OBJ+10), never re-minted",
                round
            );
            assert_eq!(
                rocks.cache_get_outcome(token).unwrap(),
                Some(OpOutcome::Reserved {
                    start: BlockIdCodec::CACHE_OBJECT_MIN,
                    end: BlockIdCodec::CACHE_OBJECT_MIN + 10,
                }),
                "round {}: the token must resolve to exactly one outcome",
                round
            );
            assert_eq!(rocks.cache_client_watermark(7).unwrap(), Some(1));
        }

        let _ = FileUtils::delete_path(&base, true);
    }

    /// The leader lifetime for the fake-ACK fault test: a REAL single-voter
    /// raft node with the production (non-testing) JournalWriter and the
    /// production JournalLoader (apply worker enabled). One cache id-reserve
    /// command goes through `sync_propose_cache`, i.e. the full RPC →
    /// commit → EntryWithAck-apply → ProposeResponse barrier.
    fn real_raft_leader_lifetime(meta_dir_env: &str, journal_dir_env: &str) {
        Master::init_test_metrics();
        let meta_dir = std::env::var(meta_dir_env).unwrap();
        let journal_dir = std::env::var(journal_dir_env).unwrap();

        let mut journal = JournalConf::with_test();
        journal.enable = true;
        journal.journal_dir = journal_dir;
        let conf = ClusterConf {
            testing: true,
            format_master: true,
            journal,
            master: MasterConf {
                meta_dir,
                ..Default::default()
            },
            ..Default::default()
        };

        let rt = conf.journal.create_runtime();
        let log_store = RocksLogStorage::from_conf(&conf.journal, true);
        let role_monitor = RoleMonitor::new();
        let master_monitor = MasterMonitor::new(role_monitor.read_ctl(), StateCtl::new(0));

        // Production writer: testing=false keeps the RaftClient and the
        // real sync-propose barrier; the loader's apply worker runs.
        let client = RaftClient::from_conf(rt.clone(), &conf.journal);
        let journal_writer = Arc::new(JournalWriter::new(false, client, &conf.journal).unwrap());

        let ttl_bucket_list =
            Arc::new(TtlBucketList::new(conf.master.ttl_bucket_interval_ms() as i64).unwrap());
        let eviction_conf = EvictionConf::from_conf(&conf);
        let evictor: Arc<dyn Evictor> = Arc::new(LRUEvictor::new(eviction_conf.clone()));
        let fs_dir = SyncFsDir::new(
            FsDir::new(&conf, journal_writer.clone(), ttl_bucket_list, evictor).unwrap(),
        );
        let fs = MasterFilesystem::new(
            &conf,
            fs_dir.clone(),
            SyncWorkerManager::new(WorkerManager::new(&conf).unwrap()),
            master_monitor,
        );
        let mount_manager = Arc::new(MountManager::new(fs.clone()));
        let job_manager = Arc::new(JobManager::from_cluster_conf(
            fs,
            mount_manager.clone(),
            rt.clone(),
            &conf,
        ));

        let loader = JournalLoader::new(
            rt.clone(),
            fs_dir.clone(),
            mount_manager,
            &conf.journal,
            job_manager,
            log_store.clone(),
            journal_writer.clone(),
        )
        .unwrap();
        let raft = MetaRaftJournal::new(
            rt.clone(),
            log_store,
            loader.clone(),
            conf.journal.clone(),
            role_monitor,
        );
        let mut listener = rt.block_on(raft.run()).unwrap();
        rt.block_on(listener.wait_leader()).unwrap();

        let token = OpToken {
            client_id: 7,
            op_seq: 1,
        };
        let entry = JournalEntry::CacheIdReserve(CacheIdReserveEntry {
            op_id: 1,
            rpc_id: 0,
            token,
            start: BlockIdCodec::CACHE_OBJECT_MIN,
            end: BlockIdCodec::CACHE_OBJECT_MIN + 10,
        });

        // The full barrier path: this returns only after the committed
        // entry has been applied by the FSM (EntryWithAck).
        let index = journal_writer.sync_propose_cache(entry).unwrap();
        assert!(index > 0, "sync barrier must return the applied raft index");
        assert!(
            loader.fsm_state_snapshot().unwrap().applied.index >= index,
            "FSM applied index must have reached the barrier index"
        );

        // The committed state this leader would ACK against.
        let store = fs_dir.read();
        let rocks = store.get_rocks_store();
        assert_eq!(
            rocks.cache_get_state(state_tags::CACHE_OBJECT_ID).unwrap(),
            Some(BlockIdCodec::CACHE_OBJECT_MIN + 9)
        );
        assert_eq!(
            rocks.cache_get_outcome(token).unwrap(),
            Some(OpOutcome::Reserved {
                start: BlockIdCodec::CACHE_OBJECT_MIN,
                end: BlockIdCodec::CACHE_OBJECT_MIN + 10,
            })
        );

        // Abrupt exit right after the barrier asserts: returning would run
        // destructors (loader/raft runtime/RocksDB), which is a graceful
        // shutdown, not the crash this fault test simulates. The crash point
        // is "immediately after the barrier response, before anything else".
        std::process::exit(0);
    }
}
