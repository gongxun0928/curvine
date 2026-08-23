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

use crate::master::fs::context::ValidateAddBlock;
use crate::master::fs::policy::ChooseContext;
use crate::master::journal::JournalSystem;
use crate::master::meta::inode::{InodeFile, InodePath, InodePtr, InodeView, PATH_SEPARATOR};
use crate::master::meta::{BlockIdCodec, CacheInvalidationResult, FsDir};

use crate::master::fs::DeleteResult;
use crate::master::meta::parse_glob_pattern;
use crate::master::replication::master_replication_handler::MasterReplicationHandler;
use crate::master::{Master, MasterMonitor, SyncFsDir, SyncWorkerManager};
use curvine_config::{ClusterConf, MasterConf};
use curvine_core_error::{err_box, err_ext, try_option, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::*;
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::GroupExecutor;
use curvine_runtime::sync::ArcRwLock;
use log::{error, info, warn};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

pub struct CvMetadataSnapshotEntry {
    pub status: FileStatus,
    pub blocks: Option<FileBlocks>,
}

pub struct CvMetadataSnapshotPage {
    pub entries: Vec<CvMetadataSnapshotEntry>,
    pub next_page_token: Option<String>,
    pub epoch: u64,
}

pub struct CvMetadataDeltaEntry {
    pub path: String,
    pub entry: Option<CvMetadataSnapshotEntry>,
}

pub struct CvMetadataDeltaPage {
    pub entries: Vec<CvMetadataDeltaEntry>,
    pub next_page_token: Option<String>,
    pub from_epoch: u64,
    pub to_epoch: u64,
    pub full_snapshot_required: bool,
}

struct CompleteFileOptions {
    only_flush: bool,
    return_file_blocks: bool,
    set_attr_opts: Option<SetAttrOpts>,
}

#[derive(Clone)]
pub struct MasterFilesystem {
    pub fs_dir: SyncFsDir,
    pub worker_manager: SyncWorkerManager,
    pub master_monitor: MasterMonitor,
    pub conf: Arc<MasterConf>,
    pub cache_service: Arc<crate::master::cache::CacheService>,
    full_block_reports: Arc<Mutex<HashMap<u32, FullBlockReportState>>>,
    full_block_reconciles: Arc<Mutex<HashMap<u32, FullBlockReconcileState>>>,
    full_block_reconcile_executor: Arc<GroupExecutor>,
    /// 4d (R8-1): serializes a Start heartbeat's whole critical
    /// section — WorkerManager validation/clear, accumulator reset,
    /// volatile session install — so dual or reordered Starts cannot
    /// interleave their state transitions. Declared lock order:
    /// `start_gate → WM write → accumulator control → CacheVolatile`.
    pub(crate) start_gate: Arc<Mutex<()>>,
}

pub struct BlockReportResult {
    pub delete_blocks: Vec<i64>,
}

#[derive(Default)]
pub struct LostWorkerLocationCleanup {
    pub removed_block_ids: Vec<i64>,
    pub replication_block_ids: Vec<i64>,
}

pub(crate) enum BlockInodeState {
    File,
    Missing,
    NotFile,
}

fn child_snapshot_path(parent: &str, child_name: &str) -> String {
    if parent == "/" {
        format!("/{child_name}")
    } else {
        format!("{parent}/{child_name}")
    }
}

fn snapshot_token_in_subtree(token: &str, subtree_path: &str) -> bool {
    token == subtree_path
        || (subtree_path != "/"
            && token
                .strip_prefix(subtree_path)
                .map(|rest| rest.starts_with(PATH_SEPARATOR))
                .unwrap_or(false))
}

struct FullBlockReportState {
    /// RC1 P0-3 (gpt56 `d2546338` item 3 / `2b83f05d`): the wire session
    /// every page of this accumulation was authorized against. A Start
    /// resets the row; a page from any OTHER session is zero-effect
    /// (zero-create, zero-clear, zero-count, zero-trigger) BEFORE it
    /// reaches this struct — see `collect_full_block_report`.
    session: String,
    /// RC2 P0-1 (gpt56 `53516250` window 2): the registry tag captured
    /// at the FIRST authorized page — the Start identity this trigger
    /// belongs to. The eventual cache snapshot checkout is exact on it:
    /// a same-wire-session Start RETRY (fresh tag, fresh cache row)
    /// makes the old trigger's take a None — the old snapshot can
    /// never exact-strip the new Start's locations.
    tag: u64,
    total_len: u64,
    update_time_ms: u64,
    reported_blocks: HashSet<i64>,
    invalidated: bool,
}

struct FullBlockReconcileState {
    running: bool,
    generation: u64,
    pending: Option<FullBlockReconcileJob>,
}

struct FullBlockReconcileJob {
    generation: u64,
    reported_blocks: HashSet<i64>,
}

const FULL_BLOCK_REPORT_TTL_MS: u64 = 60 * 60 * 1000;
const FULL_BLOCK_RECONCILE_THREADS: usize = 2;
const FULL_BLOCK_RECONCILE_QUEUE_SIZE: usize = 128;

impl MasterFilesystem {
    // Max block-report location updates applied under a single fs_dir write lock.
    const BLOCK_REPORT_WRITE_CHUNK: usize = 4096;
    // Max lost-worker block ids inspected under a single fs_dir write lock.
    const LOST_WORKER_INVALIDATION_CHUNK: usize = Self::BLOCK_REPORT_WRITE_CHUNK;

    fn validate_alloc_capacity(
        current_len: i64,
        replicas: u8,
        opts: &FileAllocOpts,
        available: i64,
    ) -> FsResult<()> {
        if opts.truncate || opts.len <= current_len {
            return Ok(());
        }

        let logical_growth = opts.len - current_len;
        let required = logical_growth.saturating_mul(i64::from(replicas));
        if required > available {
            return err_ext!(FsError::disk_out_of_space(format!(
                "fallocate requires {} bytes for {} replicas, but only {} bytes are available",
                logical_growth, replicas, available
            )));
        }

        Ok(())
    }

    /// Production cache placement chooser: the cluster worker policy at
    /// the server-configured cache replication factor. The factor must be
    /// a valid replication inside the master's min/max bounds AND no more
    /// than `MAX_LOCATIONS_PER_BLOCK` — a misconfigured cluster fails
    /// closed at construction instead of planning under-replicated or
    /// over-cap placements (an over-cap plan would be truncated at commit
    /// time and could then never be satisfied).
    fn cache_chooser(
        conf: &ClusterConf,
        workers: &SyncWorkerManager,
    ) -> FsResult<Arc<crate::master::cache::PolicyWorkerChooser>> {
        let raw = conf.client.replicas;
        let replicas = match u16::try_from(raw) {
            Ok(r)
                if r >= conf.master.min_replication
                    && r <= conf.master.max_replication
                    && (r as usize) <= crate::master::cache::MAX_LOCATIONS_PER_BLOCK =>
            {
                r
            }
            _ => {
                return err_ext!(FsError::invalid_argument(format!(
                    "cache placement replicas {} is outside the master replication bounds [{}, {}] or above the per-block location cap {}",
                    raw,
                    conf.master.min_replication,
                    conf.master.max_replication,
                    crate::master::cache::MAX_LOCATIONS_PER_BLOCK
                )))
            }
        };
        Ok(Arc::new(crate::master::cache::PolicyWorkerChooser::new(
            workers.clone(),
            replicas,
        )))
    }

    pub fn new(
        conf: &ClusterConf,
        fs_dir: SyncFsDir,
        worker_manager: SyncWorkerManager,
        master_monitor: MasterMonitor,
    ) -> FsResult<Self> {
        let journal_writer = fs_dir.read().journal_writer.clone();
        let chooser = Self::cache_chooser(conf, &worker_manager)?;
        Ok(Self {
            cache_service: Arc::new(crate::master::cache::CacheService::new(
                fs_dir.clone(),
                journal_writer,
                master_monitor.clone(),
                chooser,
                conf.master.cache_metadata_enabled,
                conf.master.cache_report_total_cap,
            )),
            fs_dir,
            worker_manager,
            master_monitor,
            conf: Arc::new(conf.master.clone()),
            full_block_reports: Default::default(),
            start_gate: Default::default(),
            full_block_reconciles: Default::default(),
            full_block_reconcile_executor: Arc::new(GroupExecutor::new(
                "master-full-block-reconcile",
                FULL_BLOCK_RECONCILE_THREADS,
                FULL_BLOCK_RECONCILE_QUEUE_SIZE,
            )),
        })
    }

    pub fn with_js(conf: &ClusterConf, js: &JournalSystem) -> FsResult<Self> {
        let fs_dir = js.fs().fs_dir.clone();
        let journal_writer = fs_dir.read().journal_writer.clone();
        let worker_manager = js.worker_manager();
        let chooser = Self::cache_chooser(conf, &worker_manager)?;
        Ok(Self {
            cache_service: Arc::new(crate::master::cache::CacheService::new(
                fs_dir.clone(),
                journal_writer,
                js.master_monitor(),
                chooser,
                conf.master.cache_metadata_enabled,
                conf.master.cache_report_total_cap,
            )),
            fs_dir,
            worker_manager,
            master_monitor: js.master_monitor(),
            conf: Arc::new(conf.master.clone()),
            full_block_reports: Default::default(),
            start_gate: Default::default(),
            full_block_reconciles: Default::default(),
            full_block_reconcile_executor: Arc::new(GroupExecutor::new(
                "master-full-block-reconcile",
                FULL_BLOCK_RECONCILE_THREADS,
                FULL_BLOCK_RECONCILE_QUEUE_SIZE,
            )),
        })
    }

    pub fn check_parent(path: &InodePath) -> FsResult<()> {
        // The root directory must exist.All /a does not require verification
        if path.len() > 2 {
            if let Some(v) = path.get_inode(-2) {
                if !v.is_dir() {
                    err_box!(
                        "Parent path is not a directory:: {}",
                        path.get_parent_path()
                    )
                } else {
                    Ok(())
                }
            } else {
                err_box!("Parent directory doesn't exist: {}", path.get_parent_path())
            }
        } else {
            Ok(())
        }
    }

    pub fn print_tree(&self) {
        let fs_dir = self.fs_dir.read();
        fs_dir.print_tree();
    }

    pub fn mkdir_with_opts<T: AsRef<str>>(&self, path: T, opts: MkdirOpts) -> FsResult<FileStatus> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;

        // Creation of root directory is not allowed
        if inp.is_root() {
            return err_box!("Not allowed to create existing root path: {}", inp.path());
        }

        if inp.is_full() {
            if opts.create_parent {
                if let Some(last_inode) = inp.get_last_inode() {
                    if last_inode.is_dir() {
                        let status = last_inode.to_file_status(inp.path())?;
                        return Ok(status);
                    }
                }
            }
            return err_ext!(FsError::file_exists(inp.path()));
        }

        // Check whether the directory can be created recursively.
        if !opts.create_parent {
            Self::check_parent(&inp)?;
        }

        let inp = fs_dir.mkdir(inp, opts)?;
        let last = try_option!(
            inp.get_last_inode(),
            "Path {} has no inode after mkdir",
            inp.path()
        );
        let status = last.to_file_status(inp.path())?;
        Ok(status)
    }

    pub fn mkdir<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let opts = MkdirOpts::with_create(create_parent);
        self.mkdir_with_opts(path, opts)
    }

    pub fn delete<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<DeleteResult> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;

        let mut delete_res = fs_dir.delete(&inp, recursive)?;
        drop(fs_dir);

        let mut worker_manager = self.worker_manager.write();
        worker_manager.remove_blocks(&DeleteResult {
            inodes: 0,
            bytes: 0,
            blocks: std::mem::take(&mut delete_res.blocks),
        });

        Ok(delete_res)
    }

    pub fn free<T: AsRef<str>>(&self, path: T, recursive: bool) -> FsResult<FreeResult> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;

        let mut free_res = fs_dir.free(&inp, recursive)?;
        drop(fs_dir);

        let mut worker_manager = self.worker_manager.write();
        worker_manager.remove_blocks(&DeleteResult {
            inodes: 0,
            bytes: 0,
            blocks: std::mem::take(&mut free_res.blocks),
        });

        Ok(free_res)
    }

    pub fn rename<T: AsRef<str>>(&self, src: T, dst: T, flags: RenameFlags) -> FsResult<bool> {
        let src = src.as_ref();
        let dst = dst.as_ref();

        let mut fs_dir = self.fs_dir.write();
        let src_inp = Self::resolve_path(&fs_dir, src)?;
        let dst_inp = Self::resolve_path(&fs_dir, dst)?;

        if src_inp.is_root() {
            return err_box!("Cannot rename root path");
        }

        if src == dst {
            return Ok(false);
        }

        // dst cannot be in the src directory, /a/b -> /a/b/c is not allowed (POSIX EINVAL).
        if let Some(rest) = dst.strip_prefix(src) {
            if rest.starts_with(PATH_SEPARATOR) {
                return err_ext!(FsError::invalid_argument(format!(
                    "cannot rename {} to {}: destination is under source",
                    src, dst
                )));
            }
        }

        // EXCHANGE also rejects src under dst (/a/b <-> /a would make /a its own descendant).
        if flags.exchange_mode() {
            if let Some(rest) = src.strip_prefix(dst) {
                if rest.starts_with(PATH_SEPARATOR) {
                    return err_ext!(FsError::invalid_argument(format!(
                        "cannot exchange {} with {}: source is under destination",
                        src, dst
                    )));
                }
            }
        }

        if let Some(del_res) = fs_dir.rename(&src_inp, &dst_inp, flags)? {
            let mut worker_manager = self.worker_manager.write();
            worker_manager.remove_blocks(&del_res);
        }

        Ok(true)
    }

    pub fn create<T: AsRef<str>>(&self, path: T, create_parent: bool) -> FsResult<FileStatus> {
        let ctx = CreateFileOpts::with_create(create_parent);
        self.create_with_opts(path, ctx, OpenFlags::new_create().set_overwrite(true))
    }

    fn truncate(&self, fs_dir: &mut FsDir, inp: &InodePath, opts: CreateFileOpts) -> FsResult<()> {
        let clean_result = fs_dir.overwrite_file(inp, opts)?;
        if !clean_result.blocks.is_empty() {
            let mut worker_manager = self.worker_manager.write();
            worker_manager.remove_blocks(&clean_result);
        }
        Ok(())
    }

    pub fn create_with_opts<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileStatus> {
        if !flags.create() {
            return err_box!("create_with_opts requires O_CREAT flag");
        }
        let path = path.as_ref();

        // Check the path length
        self.check_path_length(path)?;

        if opts.replicas < self.conf.min_replication || opts.replicas >= self.conf.max_replication {
            return err_box!(
                "The replica number {} needs to be between {} and {}",
                opts.replicas,
                self.conf.min_replication,
                self.conf.max_replication
            );
        }

        if opts.block_size < self.conf.min_block_size || opts.block_size >= self.conf.max_block_size
        {
            return err_box!(
                "Block size needs to be between {} and {}",
                self.conf.min_block_size,
                self.conf.max_block_size
            );
        }

        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let last_inode = inp.get_last_inode();
        if let Some(inode) = &last_inode {
            if inode.is_dir() {
                return err_box!("{}  already exists as a dir", inp.path());
            }

            if flags.exclusive() {
                return err_ext!(FsError::file_exists(inp.path()));
            }
        }

        if !opts.create_parent {
            Self::check_parent(&inp)?;
        }

        let inp = if last_inode.is_some() {
            if flags.overwrite() {
                self.truncate(&mut fs_dir, &inp, opts)?;
            } else {
                return err_ext!(FsError::file_exists(inp.path()));
            }
            inp
        } else {
            fs_dir.create_file(inp, opts)?
        };

        let status = fs_dir.file_status(&inp)?;

        Ok(status)
    }

    pub fn open_file<T: AsRef<str>>(
        &self,
        path: T,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FileBlocks> {
        let path = path.as_ref();

        if flags.read_only() {
            if flags.truncate() {
                return err_box!("cannot combine O_RDONLY with O_TRUNC");
            }
            if flags.create() {
                return err_box!("cannot combine O_RDONLY with O_CREAT");
            }
            return self.get_block_locations(path);
        }

        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let inode = match inp.get_last_inode() {
            None => {
                return if flags.create() {
                    drop(fs_dir);
                    let status = self.create_with_opts(path, opts, flags)?;
                    Ok(FileBlocks::new(status, vec![]))
                } else {
                    err_ext!(FsError::file_not_found(inp.path()))
                }
            }

            Some(inode) => {
                if inode.is_dir() {
                    return err_box!("{} is a directory", inp.path());
                }
                inode
            }
        };

        if flags.truncate() {
            self.truncate(&mut fs_dir, &inp, opts)?;
            let status = fs_dir.file_status(&inp)?;
            return Ok(FileBlocks::new(status, vec![]));
        }

        let status = fs_dir.reopen_file(&inp, opts.client_name)?;
        let file = inode.as_file_ref()?;
        let blocks = if !file.blocks.is_empty() {
            self.get_block_locs(path, &fs_dir, file)?
        } else {
            vec![]
        };
        Ok(FileBlocks::new(status, blocks))
    }

    pub fn file_status<T: AsRef<str>>(&self, path: T) -> FsResult<FileStatus> {
        let fs_dir = self.fs_dir.read();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        let status = fs_dir.file_status(&inp)?;
        Ok(status)
    }

    pub fn exists<T: AsRef<str>>(&self, path: T) -> FsResult<bool> {
        let fs_dir = self.fs_dir.read();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        Ok(inp.get_last_inode().is_some())
    }

    pub fn list_status<T: AsRef<str>>(&self, path: T) -> FsResult<Vec<FileStatus>> {
        let fs_dir = self.fs_dir.read();
        let (is_glob_pattern, _) = parse_glob_pattern(path.as_ref());
        if is_glob_pattern {
            let paths = Self::resolve_path_by_glob_pattern(&fs_dir, path.as_ref())?;
            let mut all_statuses = Vec::new();
            for path in &paths {
                let statuses = fs_dir.list_status(path)?;
                all_statuses.extend(statuses);
            }
            Ok(all_statuses)
        } else {
            let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
            fs_dir.list_status(&inp)
        }
    }

    pub fn list_options<T: AsRef<str>>(
        &self,
        path: T,
        opts: ListOptions,
    ) -> FsResult<Vec<FileStatus>> {
        let path = path.as_ref();
        let fs_dir = self.fs_dir.read();
        let (is_glob_pattern, _) = parse_glob_pattern(path);
        if is_glob_pattern {
            err_box!("list_options does not support glob pattern, path {}", path)
        } else {
            let inp = Self::resolve_path(&fs_dir, path)?;
            fs_dir.list_options(&inp, &opts)
        }
    }

    fn resolve_path(fs_dir: &FsDir, path: &str) -> CommonResult<InodePath> {
        InodePath::resolve(fs_dir.root_ptr(), path, &fs_dir.store)
    }

    fn resolve_path_by_glob_pattern(fs_dir: &FsDir, path: &str) -> CommonResult<Vec<InodePath>> {
        InodePath::resolve_for_glob_pattern(fs_dir.root_ptr(), path, &fs_dir.store)
    }

    pub fn check_path_length(&self, path: &str) -> CommonResult<()> {
        if path.len() > self.conf.max_path_len {
            return err_box!(
                "create: Path too long, limit {} characters",
                self.conf.max_path_len
            );
        }

        let depth = path.split(PATH_SEPARATOR).count();
        if depth > self.conf.max_path_depth {
            return err_box!(
                "create: Path too long, limit {} levels",
                self.conf.max_path_depth
            );
        }

        Ok(())
    }

    pub fn validate_add_block(
        file: &InodeFile,
        client_addr: &ClientAddress,
        previous: Option<&CommitBlock>,
    ) -> FsResult<ValidateAddBlock> {
        if let Some(v) = previous {
            if v.block_len != file.block_size as i64 {
                return err_box!(
                    "The block size is incorrect, block size: {}, commit block length: {}",
                    file.block_size,
                    v.block_len
                );
            }
        }

        let res = ValidateAddBlock {
            replicas: file.replicas as u16,
            block_size: file.block_size as i64,
            storage_policy: file.storage_policy.clone(),
            client_host: client_addr.hostname.clone(),
        };

        Ok(res)
    }

    pub fn choose_worker(
        &self,
        inp: &InodePath,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<Vec<WorkerAddress>> {
        let mut inode = try_option!(inp.get_last_inode(), "File {} not exists", inp.path());
        let file = inode.as_file_mut()?;
        self.choose_worker_for_file(file, client_addr, exclude_workers)
    }

    pub fn choose_worker_for_file(
        &self,
        file: &InodeFile,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<Vec<WorkerAddress>> {
        let wm = self.worker_manager.read();
        let validate_block = Self::validate_add_block(file, &client_addr, None)?;
        let choose_ctx = ChooseContext::with_block(validate_block, exclude_workers);
        Ok(wm.choose_worker(choose_ctx)?)
    }

    pub fn create_locate_block(
        &self,
        path: impl AsRef<str>,
        block: ExtendedBlock,
        locs: &[BlockLocation],
    ) -> FsResult<LocatedBlock> {
        self.worker_manager
            .read()
            .create_locate_block(path, block, locs)
    }

    pub fn resolve_file_inode(
        fs_dir: &FsDir,
        path: &str,
        inode_id: Option<i64>,
    ) -> FsResult<InodePtr> {
        match inode_id {
            Some(v) if v > 0 => match fs_dir.store.get_inode(v, None)? {
                Some(view) => Ok(InodePtr::from_owned(view)),
                None => err_ext!(FsError::file_not_found(path).ctx(format!("inode_id={}", v))),
            },

            _ => {
                let inp = Self::resolve_path(fs_dir, path)?;
                match inp.task_last() {
                    Some(ptr) => Ok(ptr),
                    None => err_ext!(FsError::file_not_found(path)),
                }
            }
        }
    }

    /// Document application to allocate a new block.
    #[allow(clippy::too_many_arguments)]
    pub fn add_block<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        client_addr: ClientAddress,
        commit_blocks: Vec<CommitBlock>,
        exclude_workers: Vec<u32>,
        file_len: i64,
        last_block: Option<ExtendedBlock>,
    ) -> FsResult<LocatedBlock> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
        let file = inode.as_file_ref()?;

        // File allows concurrent writes, 'previous' is the previous block,
        // need to check if the next block has already been allocated。
        // If it has been allocated, return that block
        if let Some(next) = file.search_next_block(last_block.map(|v| v.id)) {
            let locs = fs_dir.get_block_locations(next.id)?;
            let extend_block = ExtendedBlock {
                id: next.id,
                len: next.len(),
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
                alloc_opts: next.alloc_opts.clone(),
            };

            return self.create_locate_block(path, extend_block, &locs);
        }

        let choose_workers = self.choose_worker_for_file(file, client_addr, exclude_workers)?;
        let has_spdk = {
            let wm = self.worker_manager.read();
            wm.workers_have_spdk(&choose_workers)
        };
        let block =
            fs_dir.acquire_new_block(path, inode, commit_blocks, &choose_workers, file_len)?;
        let located = LocatedBlock {
            block,
            locs: choose_workers,
            has_spdk,
        };

        Ok(located)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_file<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
        only_flush: bool,
        set_attr_opts: Option<SetAttrOpts>,
    ) -> FsResult<Option<FileBlocks>> {
        self.complete_file0(
            path,
            inode_id,
            len,
            commit_blocks,
            client_name,
            CompleteFileOptions {
                only_flush,
                return_file_blocks: true,
                set_attr_opts,
            },
        )
    }

    /// Flushes file metadata without building the full block-location snapshot.
    pub fn flush_file<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
    ) -> FsResult<()> {
        self.complete_file0(
            path,
            inode_id,
            len,
            commit_blocks,
            client_name,
            CompleteFileOptions {
                only_flush: true,
                return_file_blocks: false,
                set_attr_opts: None,
            },
        )
        .map(|_| ())
    }

    fn complete_file0<T: AsRef<str>>(
        &self,
        path: T,
        inode_id: Option<i64>,
        len: i64,
        commit_blocks: Vec<CommitBlock>,
        client_name: T,
        options: CompleteFileOptions,
    ) -> FsResult<Option<FileBlocks>> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let mut inode = Self::resolve_file_inode(&fs_dir, path, inode_id)?;
        fs_dir.complete_file(
            path,
            &mut inode,
            len,
            commit_blocks,
            client_name,
            options.only_flush,
            options.set_attr_opts,
        )?;

        if options.only_flush && options.return_file_blocks {
            let file = inode.as_file_ref()?;
            let locs = self.get_block_locs(path, &fs_dir, file)?;
            let status = inode.to_file_status(path)?;
            return Ok(Some(FileBlocks::new(status, locs)));
        }

        Ok(None)
    }

    pub fn get_file_blocks(
        &self,
        path: &str,
        fs_dir: &FsDir,
        inp: &InodePath,
    ) -> FsResult<FileBlocks> {
        let inode = try_option!(inp.get_last_inode(), "File {} not exists", path);
        let file = inode.as_file_ref()?;
        let blocks = self.get_block_locs(path, fs_dir, file)?;
        Ok(FileBlocks::new(inode.to_file_status(path)?, blocks))
    }

    fn get_block_locs(
        &self,
        path: &str,
        fs_dir: &FsDir,
        file: &InodeFile,
    ) -> FsResult<Vec<LocatedBlock>> {
        let wm = self.worker_manager.read();
        let file_locs = fs_dir.get_file_locations(file)?;
        let mut block_locs = Vec::with_capacity(file_locs.len());

        for (index, meta) in file.blocks.iter().enumerate() {
            if index + 1 < file.blocks.len() && meta.len() != file.block_size as i64 {
                return err_box!(
                    "block status abnormal, block id {}, block len {}, expected block size {}",
                    meta.id,
                    meta.len(),
                    file.block_size
                );
            }

            let extend_block = ExtendedBlock {
                id: meta.id,
                len: meta.len(),
                storage_type: file.storage_policy.storage_type,
                file_type: file.file_type,
                alloc_opts: meta.alloc_opts.clone(),
            };

            let lc = try_option!(
                file_locs.get(&meta.id),
                "File {}, block {} Lost (no worker can read)",
                path,
                meta.id
            );
            let lb = wm.create_locate_block(path, extend_block, lc)?;
            block_locs.push(lb);
        }

        Ok(block_locs)
    }

    pub fn get_block_locations<T: AsRef<str>>(&self, path: T) -> FsResult<FileBlocks> {
        let fs_dir = self.fs_dir.read();
        let path = path.as_ref();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let inode = match inp.get_last_inode() {
            Some(v) => v,
            None => return err_ext!(FsError::file_not_found(path)),
        };
        let file = inode.as_file_ref()?;
        let block_locs = self.get_block_locs(path, &fs_dir, file)?;
        let locate_blocks = FileBlocks {
            status: inode.to_file_status(path)?,
            block_locs,
        };

        Ok(locate_blocks)
    }

    pub fn cv_metadata_snapshot_page(
        &self,
        page_token: Option<String>,
        page_size: usize,
    ) -> FsResult<CvMetadataSnapshotPage> {
        if page_size == 0 {
            return err_box!("cv metadata snapshot page_size must be greater than 0");
        }

        let fs_dir = self.fs_dir.read();
        let epoch = fs_dir.op_id.get();
        let start_after = page_token.filter(|token| !token.is_empty());
        let mut entries = Vec::with_capacity(page_size.saturating_add(1));
        self.collect_cv_metadata_snapshot_page(
            &fs_dir,
            fs_dir.root_dir(),
            "/",
            start_after.as_deref(),
            page_size.saturating_add(1),
            &mut entries,
        )?;

        let next_page_token = if entries.len() > page_size {
            let token = entries
                .get(page_size.saturating_sub(1))
                .map(|entry| entry.status.path.clone());
            entries.truncate(page_size);
            token
        } else {
            None
        };

        Ok(CvMetadataSnapshotPage {
            entries,
            next_page_token,
            epoch,
        })
    }

    pub fn cv_metadata_delta_page(
        &self,
        from_epoch: u64,
        target_epoch: Option<u64>,
        page_token: Option<String>,
        page_size: usize,
    ) -> FsResult<CvMetadataDeltaPage> {
        if page_size == 0 {
            return err_box!("cv metadata delta page_size must be greater than 0");
        }

        let fs_dir = self.fs_dir.read();
        let current_epoch = fs_dir.op_id.get();
        let to_epoch = target_epoch.unwrap_or(current_epoch);
        if to_epoch > current_epoch {
            return err_box!(
                "cv metadata delta target_epoch {} is newer than current epoch {}",
                to_epoch,
                current_epoch
            );
        }
        if from_epoch > to_epoch {
            return err_box!(
                "cv metadata delta from_epoch {} is newer than target epoch {}",
                from_epoch,
                to_epoch
            );
        }
        if target_epoch.is_some() && to_epoch < current_epoch {
            return Ok(CvMetadataDeltaPage {
                entries: Vec::new(),
                next_page_token: None,
                from_epoch,
                to_epoch,
                full_snapshot_required: true,
            });
        }
        if from_epoch == to_epoch {
            return Ok(CvMetadataDeltaPage {
                entries: Vec::new(),
                next_page_token: None,
                from_epoch,
                to_epoch,
                full_snapshot_required: false,
            });
        }

        let Some(changes) = fs_dir
            .journal_writer
            .cv_metadata_changes_since(from_epoch, to_epoch)
        else {
            return Ok(CvMetadataDeltaPage {
                entries: Vec::new(),
                next_page_token: None,
                from_epoch,
                to_epoch,
                full_snapshot_required: true,
            });
        };

        let mut changed_paths = BTreeMap::new();
        for change in changes {
            changed_paths
                .entry(change.path)
                .and_modify(|include_subtree| *include_subtree |= change.include_subtree)
                .or_insert(change.include_subtree);
        }

        let mut delta_entries = BTreeMap::new();
        for (path, include_subtree) in changed_paths {
            if include_subtree {
                self.collect_cv_metadata_delta_subtree(&fs_dir, &path, &mut delta_entries)?;
            } else {
                let entry = self.cv_metadata_entry_for_path(&fs_dir, &path)?;
                delta_entries.insert(path, entry);
            }
        }

        let start_after = page_token.filter(|token| !token.is_empty());
        let mut page_entries = Vec::with_capacity(page_size.saturating_add(1));
        for (path, entry) in delta_entries {
            if start_after
                .as_deref()
                .map(|token| path.as_str() <= token)
                .unwrap_or(false)
            {
                continue;
            }
            page_entries.push(CvMetadataDeltaEntry { path, entry });
            if page_entries.len() > page_size {
                break;
            }
        }

        let next_page_token = if page_entries.len() > page_size {
            let token = page_entries
                .get(page_size.saturating_sub(1))
                .map(|entry| entry.path.clone());
            page_entries.truncate(page_size);
            token
        } else {
            None
        };

        Ok(CvMetadataDeltaPage {
            entries: page_entries,
            next_page_token,
            from_epoch,
            to_epoch,
            full_snapshot_required: false,
        })
    }

    fn collect_cv_metadata_delta_subtree(
        &self,
        fs_dir: &FsDir,
        path: &str,
        entries: &mut BTreeMap<String, Option<CvMetadataSnapshotEntry>>,
    ) -> FsResult<()> {
        let Some(entry) = self.cv_metadata_entry_for_path(fs_dir, path)? else {
            entries.insert(path.to_string(), None);
            return Ok(());
        };

        let is_dir = entry.status.is_dir;
        entries.insert(path.to_string(), Some(entry));
        if !is_dir {
            return Ok(());
        }

        let inp = Self::resolve_path(fs_dir, path)?;
        let Some(inode) = inp.get_last_inode() else {
            return Ok(());
        };
        let resolved = self.resolve_snapshot_inode(fs_dir, &inode)?;
        if let InodeView::Dir(dir) = resolved {
            for child in dir.children_iter() {
                let child_path = child_snapshot_path(path, child.name());
                self.collect_cv_metadata_delta_subtree(fs_dir, &child_path, entries)?;
            }
        }
        Ok(())
    }

    fn cv_metadata_entry_for_path(
        &self,
        fs_dir: &FsDir,
        path: &str,
    ) -> FsResult<Option<CvMetadataSnapshotEntry>> {
        let inp = match Self::resolve_path(fs_dir, path) {
            Ok(inp) => inp,
            Err(_) => return Ok(None),
        };
        let Some(inode) = inp.get_last_inode() else {
            return Ok(None);
        };
        let resolved = self.resolve_snapshot_inode(fs_dir, &inode)?;
        let status = resolved.to_file_status(path)?;
        let blocks = if let Ok(file) = resolved.as_file_ref() {
            Some(FileBlocks::new(
                status.clone(),
                self.get_block_locs(path, fs_dir, file)?,
            ))
        } else {
            None
        };
        Ok(Some(CvMetadataSnapshotEntry { status, blocks }))
    }

    fn collect_cv_metadata_snapshot_page(
        &self,
        fs_dir: &FsDir,
        inode: &InodeView,
        path: &str,
        start_after: Option<&str>,
        limit: usize,
        entries: &mut Vec<CvMetadataSnapshotEntry>,
    ) -> FsResult<()> {
        if entries.len() >= limit {
            return Ok(());
        }
        if let Some(token) = start_after {
            if path != "/" && token > path && !snapshot_token_in_subtree(token, path) {
                return Ok(());
            }
        }

        let resolved = self.resolve_snapshot_inode(fs_dir, inode)?;
        if start_after.map(|token| path > token).unwrap_or(true) {
            let status = resolved.to_file_status(path)?;
            let blocks = if let Ok(file) = resolved.as_file_ref() {
                Some(FileBlocks::new(
                    status.clone(),
                    self.get_block_locs(path, fs_dir, file)?,
                ))
            } else {
                None
            };
            entries.push(CvMetadataSnapshotEntry { status, blocks });
            if entries.len() >= limit {
                return Ok(());
            }
        }

        if let InodeView::Dir(dir) = resolved {
            for child in dir.children_iter() {
                let child_path = child_snapshot_path(path, child.name());
                self.collect_cv_metadata_snapshot_page(
                    fs_dir,
                    child,
                    &child_path,
                    start_after,
                    limit,
                    entries,
                )?;
                if entries.len() >= limit {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn resolve_snapshot_inode(&self, fs_dir: &FsDir, inode: &InodeView) -> FsResult<InodeView> {
        if let InodeView::FileEntry(entry) = inode {
            return fs_dir
                .store
                .get_inode(entry.id, Some(&entry.name))?
                .ok_or_else(|| FsError::file_not_found(entry.name.clone()));
        }
        Ok(inode.clone())
    }

    pub fn filesystem_info(&self) -> FsResult<FilesystemInfo> {
        let metrics = Master::get_metrics()?;
        let mut info = FilesystemInfo {
            inode_dir_num: metrics.inode_dir_num.get(),
            inode_file_num: metrics.inode_file_num.get(),
            ..Default::default()
        };

        let wm = self.worker_manager.read();

        // Requests can only reach active master
        info.active_master = wm.conf.master_addr().to_string();
        for peer in &wm.conf.journal.journal_addrs {
            info.journal_nodes.push(peer.to_string())
        }

        for (_, worker) in wm.worker_map.workers() {
            info.capacity += worker.capacity;
            info.available += worker.available;
            info.fs_used += worker.fs_used;
            info.non_fs_used += worker.non_fs_used;
            info.reserved_bytes += worker.reserved_bytes;
            info.block_num += worker.block_num;

            match worker.status {
                WorkerStatus::Live => {
                    info.live_workers.push(worker.clone());
                    // Only Live workers are eligible for new allocations, so the
                    // allocatable view mirrors the allocation policy. Failed
                    // storage dirs are already excluded from worker.capacity.
                    info.allocatable_capacity += worker.capacity;
                    info.allocatable_available += worker.available;
                }
                WorkerStatus::Blacklist => info.blacklist_workers.push(worker.clone()),
                WorkerStatus::Decommission => info.decommission_workers.push(worker.clone()),
                _ => (),
            }
        }

        for (_, worker) in wm.worker_map.lost_workers() {
            info.lost_workers.push(worker.clone());
        }

        Ok(info)
    }

    pub fn fs_dir(&self) -> ArcRwLock<FsDir> {
        self.fs_dir.clone()
    }

    // Add a test worker and unit tests will use it.
    pub fn add_test_worker(&self, worker: WorkerInfo) {
        let mut wm = self.worker_manager.write();
        wm.add_test_worker(worker);
    }

    pub fn sum_hash(&self) -> CommonResult<u128> {
        let fs_dir = self.fs_dir.read();
        fs_dir.sum_hash()
    }

    pub fn last_inode_id(&self) -> i64 {
        let fs_dir = self.fs_dir.read();
        fs_dir.last_inode_id()
    }

    pub fn get_file_counts(&self) -> (i64, i64) {
        let fs_dir = self.fs_dir.read();
        fs_dir.get_file_counts()
    }

    // Create a directory number based on rocksdb data for testing.
    pub fn create_tree(&self) -> CommonResult<InodeView> {
        let fs_dir = self.fs_dir.read();
        fs_dir.create_tree()
    }

    // Restore in-memory tree from RocksDB (for testing without Raft).
    // In production, Raft automatically restores via apply_snapshot().
    pub fn restore_from_rocksdb(&self) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        fs_dir.restore_from_rocksdb()
    }

    fn block_inode_state(&self, id: i64) -> FsResult<BlockInodeState> {
        let fs_dir = self.fs_dir.read();
        fs_dir.block_inode_state(id)
    }

    fn collect_full_block_report(&self, list: &BlockReportList) -> Option<(HashSet<i64>, u64)> {
        if !list.full_report {
            return None;
        }

        // RC1 P0-3 (gpt56 `d2546338` item 3, tightened `2b83f05d`):
        // EVERY full page is authorized against the CURRENT registry
        // wire session BEFORE it can create, switch, clear, or count the
        // FS trigger row — a stale/foreign page is 零创建、零清空、零计数、
        // 零触发. Match rules: the page session equals the current
        // registry session; with no live cache registration only a
        // legacy EMPTY-session page accumulates; with the cache domain
        // disabled/inactive the authorization is vacuous (the FS
        // accumulator must keep working on cache-disabled clusters,
        // exactly the pre-4d.3 behavior). A stale s1 page after
        // Start(s2) can therefore neither create the row before s2's
        // first page nor disturb s2's progress after it.
        // RC2 P0-1 (gpt56 `53516250` window 2): on success the CURRENT
        // registry TAG comes back with the authorization and is bound
        // into the row — the eventual checkout is exact on this Start
        // identity.
        let page_session = list.worker_session_id.clone().unwrap_or_default();
        let page_tag = match self
            .cache_service
            .authorize_full_report_page(list.worker_id, &page_session)
        {
            Some(tag) => tag,
            None => {
                warn!(
                    "full block report page for worker {} skipped: session {:?} is not the current registry session",
                    list.worker_id, page_session
                );
                return None;
            }
        };

        let now = LocalTime::mills();
        let mut reports = self.full_block_reports.lock();
        reports.retain(|_, report| {
            now.saturating_sub(report.update_time_ms) <= FULL_BLOCK_REPORT_TTL_MS
        });
        // A prior incremental report may have invalidated the session. Drop it so
        // this full report can start a fresh accumulation instead of being ignored.
        if reports
            .get(&list.worker_id)
            .map(|report| report.invalidated)
            .unwrap_or(false)
        {
            reports.remove(&list.worker_id);
        }
        // An existing row bound to a DIFFERENT session (or a different
        // Start identity — tag — under the same wire session, i.e. a
        // Start retry the defensive path missed) can only survive when
        // the registry moved without the Start reset path removing it:
        // restart the accumulation bound to THIS (authorized) page
        // session/tag — old-identity state never carries over.
        if reports
            .get(&list.worker_id)
            .is_some_and(|row| row.session != page_session || row.tag != page_tag)
        {
            warn!(
                "full block report for worker {} restarted: row identity (session {:?}, tag {}) superseded by (session {:?}, tag {})",
                list.worker_id,
                reports.get(&list.worker_id).map(|r| r.session.clone()),
                reports.get(&list.worker_id).map(|r| r.tag).unwrap_or(0),
                page_session,
                page_tag
            );
            reports.remove(&list.worker_id);
        }

        let report = reports
            .entry(list.worker_id)
            .or_insert_with(|| FullBlockReportState {
                session: page_session.clone(),
                tag: page_tag,
                total_len: list.total_len,
                update_time_ms: now,
                reported_blocks: HashSet::with_capacity(list.total_len as usize),
                invalidated: false,
            });

        if report.total_len != list.total_len {
            warn!(
                "full block report for worker {} restarted because total_len changed from {} to {}; discarding {} accumulated block ids",
                list.worker_id,
                report.total_len,
                list.total_len,
                report.reported_blocks.len()
            );
            report.total_len = list.total_len;
            report.reported_blocks.clear();
            report.reported_blocks.reserve(list.total_len as usize);
            report.invalidated = false;
        }
        report.update_time_ms = now;

        for block in &list.blocks {
            report.reported_blocks.insert(block.id);
        }

        if report.reported_blocks.len() as u64 >= report.total_len {
            reports
                .remove(&list.worker_id)
                .map(|report| (report.reported_blocks, report.tag))
        } else {
            None
        }
    }

    pub fn reset_full_block_report(&self, worker_id: u32) {
        self.full_block_reports.lock().remove(&worker_id);
        self.invalidate_full_block_reconcile(worker_id);
    }

    /// 4d (R8-1/R9-3): a Start heartbeat's cache-domain acceptance.
    /// Called from the heartbeat handler with the start_gate held, only
    /// AFTER the WorkerManager validated the registration (R7-1 order
    /// fix: no accumulator or session state moves on a rejected Start).
    /// Declared order inside the critical section: accumulator control
    /// first, then the volatile guard. A legacy worker (EMPTY session
    /// id) fail-closes the cache domain instead of skipping it (RC3,
    /// gpt56 `7ceef2ff` item 3): its accumulator is terminated and any
    /// current cache session retired — nothing installed in its place —
    /// so a legacy Start can never inherit or revive a predecessor's
    /// cache session, and no cache plan/location can reference the
    /// worker until a non-empty Start reopens.
    /// RC2-2 (gpt56 `aa41c780` item 2): an install refusal is RETURNED
    /// (the handler propagates it to the heartbeat RPC) — by then both
    /// domains have already failed closed atomically.
    pub fn begin_worker_session(&self, address: &WorkerAddress, session: &str) -> CommonResult<()> {
        // Accumulator domain first (FS full-report accumulation + any
        // running reconcile for this worker).
        self.reset_full_block_report(address.worker_id);
        if session.is_empty() {
            self.cache_service
                .purge_worker_cache_session(address.worker_id);
            return Ok(());
        }
        // 4d R9-3: atomic cache-session install — accumulator guard and
        // volatile guard held simultaneously (accumulator first): fresh
        // registry row + fresh accumulator bound to the new session.
        // RC5/RC2-2 (gpt56 `aa41c780` item 2): a tag-issuer exhaustion
        // is a loud refusal — nothing is installed, the OLD accumulator
        // is terminally invalidated inside begin_cache_session, and the
        // error propagates to the heartbeat RPC instead of being
        // logged and swallowed.
        self.cache_service
            .begin_cache_session(address.worker_id, session, address)
    }

    /// 4d (R9-2 + final-review `f14fa328`): an End heartbeat's — and the
    /// lost-worker callback's, via the SAME exact primitive — cache-domain
    /// retirement: session-exact against the volatile registry AND the
    /// accumulator (a hit retires registry/live and terminalizes the
    /// same-session accumulator); a no-op with zero side effects on both
    /// domains when a newer Start already replaced the session.
    pub fn end_worker_session(&self, worker_id: u32, session: &str) {
        if session.is_empty() {
            return;
        }
        self.cache_service.retire_worker_session(worker_id, session);
    }

    /// 4d.2 round-4 (gpt56 `36f4e28b` P0-1): the CONTIGUOUS lost-worker
    /// transition — the WM expired removal AND the exact cache-session
    /// retire (accumulator + volatile domains) complete inside ONE
    /// `start_gate` hold, with the WM write guard held throughout, so an
    /// incremental-report outcome's tag recheck and its WM/ack side
    /// effects are linearized against the WHOLE transition (either the
    /// transition wins first and the outcome's recheck sees the tag gone,
    /// or the outcome wins first and the transition blocks on the gate
    /// until the side effects land). The caller's expiry scan ran under
    /// an earlier gate hold, so the row is re-verified here: it must
    /// still be the exact session the scan flagged AND still be expired
    /// — a Start/Running that re-registered in between survives
    /// untouched. Only the long FS location cleanup + replication report
    /// are left async by the caller. Returns the removed worker on an
    /// actual removal.
    pub fn lost_worker_transition(
        &self,
        worker_id: u32,
        session: &str,
        lost_ms: u64,
    ) -> Option<WorkerInfo> {
        let _gate = self.start_gate.lock();
        let mut wm = self.worker_manager.write();
        // Exact-row re-verification: session mismatch = the flagged row
        // was already superseded (Start + Running re-registration).
        let row = wm.get_worker(worker_id)?;
        if row.worker_session_id != session {
            return None;
        }
        // Expiry re-verification under the gate: a fresh Running beat
        // since the scan refreshes last_update and must survive.
        if LocalTime::mills() <= row.last_update + lost_ms {
            return None;
        }
        let worker = wm.remove_expired_worker(worker_id)?;
        // Inline retire INSIDE the same gate hold (round-4 P0-1): the
        // session snapshot comes from the row just removed, so the cache
        // registry retire is exact — a worker that restarted in between
        // keeps its new session untouched (retire_worker_session no-ops
        // on a session mismatch).
        self.end_worker_session(worker_id, &worker.worker_session_id);
        Some(worker)
    }

    fn invalidate_full_block_report_session(&self, worker_id: u32) {
        let now = LocalTime::mills();
        let mut reports = self.full_block_reports.lock();
        // Only invalidate an in-flight session. Inserting a stub invalidated
        // entry would make the next full report return None forever.
        if let Some(report) = reports.get_mut(&worker_id) {
            report.update_time_ms = now;
            report.reported_blocks.clear();
            report.invalidated = true;
        }
    }

    fn invalidate_full_block_state(&self, worker_id: u32) {
        self.invalidate_full_block_report_session(worker_id);
        self.invalidate_full_block_reconcile(worker_id);
    }

    fn invalidate_full_block_reconcile(&self, worker_id: u32) {
        let mut reconciles = self.full_block_reconciles.lock();
        if let Some(state) = reconciles.get_mut(&worker_id) {
            state.generation = state.generation.saturating_add(1);
            state.pending = None;
            if !state.running {
                reconciles.remove(&worker_id);
            }
        }
    }

    /// P0-1 (gpt56 `25d4b51e` item 1) / RC1 P0-2 (gpt56 `d2546338`
    /// item 2): apply one report outcome's WorkerManager side effects
    /// under the transition fence. The outcome carries the registry tag
    /// AND the reconcile generation its volatile mutations were applied
    /// under; the tag+generation recheck AND the WM remove/ack +
    /// quarantine-release effects all run inside
    /// `apply_outcome_fenced`, which holds the VOLATILE guard across
    /// the whole apply — so neither a Start/End/lost transition (needs
    /// `start_gate`, held here) nor a same-session report (needs the
    /// volatile lock) can interleave between the recheck and the side
    /// effects. An outcome superseded by a newer same-session report
    /// (generation bumped) is a LOUD drop: its WM effects can never
    /// re-order after the newer report's (an old `remove_block` would
    /// clear a fresh delete queue entry; an old `deleted_block` ack
    /// would release a fresh quarantine).
    fn apply_cache_incr_outcome(
        &self,
        worker_id: u32,
        outcome: crate::cache::cache_service::CacheIncrOutcome,
    ) {
        if outcome.session_tag == 0
            || (outcome.remove_blocks.is_empty() && outcome.deleted_acks.is_empty())
        {
            return;
        }
        let _gate = self.start_gate.lock();
        let mut wm = self.worker_manager.write();
        // #[cfg(test)] deterministic seam (4d.2 round-3, gpt56 `f5980e03`
        // P0-1 / `48dec504` outcome-wins branch): fires at ENTRY of the
        // fenced transition — `start_gate` and the WM write guard are
        // held, the volatile lock is NOT (existing hooks call
        // volatile-taking methods such as `cache_session_tag`, which
        // would self-deadlock under the fenced apply's guard). The
        // lost-worker transition also takes `start_gate`, so a lost
        // retire arriving in this window must BLOCK — tests prove it via
        // `start_gate.try_lock()` failing here. Compiled out outside
        // cfg(test); never set in production.
        #[cfg(test)]
        if let Some(hook) = crate::master::master_handler::INCR_OUTCOME_SEAM
            .lock()
            .unwrap()
            .as_ref()
        {
            hook();
        }
        // RC1 P0-2: tag AND applied-generation recheck plus the WM
        // remove/ack + quarantine release, ALL under the volatile guard
        // held by the fenced apply (lock order matches the heartbeat
        // path: start_gate → WM → volatile).
        self.cache_service
            .apply_outcome_fenced(worker_id, &outcome, |eff| match eff {
                crate::cache::cache_service::WmEffect::RemoveBlock(id) => {
                    wm.remove_block(worker_id, id)
                }
                crate::cache::cache_service::WmEffect::DeletedAck(id) => {
                    wm.deleted_block(worker_id, id)
                }
            });
    }

    /// Process block reports
    pub fn block_report(
        &self,
        list: BlockReportList,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<BlockReportResult> {
        // @todo check cluster.
        let invalidate_full_reconcile = !list.full_report
            && list.blocks.iter().any(|block| {
                matches!(
                    block.status,
                    BlockReportStatus::Finalized | BlockReportStatus::Writing
                )
            });
        if invalidate_full_reconcile {
            self.invalidate_full_block_state(list.worker_id);
        }

        let full_reported_blocks = self.collect_full_block_report(&list);
        if list.blocks.is_empty() && full_reported_blocks.is_none() {
            return Ok(BlockReportResult {
                delete_blocks: Vec::new(),
            });
        }

        // 4d.2 cache-before-FS diversion: cache-domain block ids NEVER
        // enter the FS classify/apply/reconcile chain below (zero
        // penetration, including Deleted — a cache Deleted is a BlockMap
        // ack + volatile replica removal, never an inode-chain delete).
        // The FS full-report accumulator above still counts ALL ids so
        // its declared total stays correct; only the classification and
        // the final reconcile set are domain-split.
        let mut cache_items: Vec<BlockReportInfo> = Vec::new();
        let mut fs_blocks: Vec<BlockReportInfo> = Vec::with_capacity(list.blocks.len());
        // 4d.3 / RC2 P0-1: a cache-only worker's self-Complete snapshot,
        // stashed WITH its exact checkout ticket (carried out
        // atomically at the in-place transition — never re-read) until
        // the FS accumulator's end-of-report trigger fires.
        let mut cache_complete: Option<(
            Vec<BlockReportInfo>,
            crate::cache::cache_service::FullSnapshotTicket,
        )> = None;
        for item in list.blocks {
            match BlockIdCodec::is_cache_block_id(item.id) {
                Ok(true) => cache_items.push(item),
                // A decode failure is not provably a cache id: keep it
                // on the FS path, whose classify logs and skips it.
                _ => fs_blocks.push(item),
            }
        }
        if !cache_items.is_empty() {
            if list.full_report {
                // #[cfg(test)] deterministic seam (RC2 gpt56 `e6207e1d`
                // focused check): the FS accumulator has authorized the
                // page and bound the trigger's Start-identity tag, but the
                // cache-domain page has NOT been processed. A hook may run
                // a same-wire-session Start RETRY here — the fresh row may
                // then swallow this RPC's page and self-Complete with a
                // NEW-tag ticket, which the trigger below must refuse.
                // Compiled out outside cfg(test); never set in production.
                #[cfg(test)]
                if let Some(hook) = crate::master::master_handler::FULL_PAGE_SEAM
                    .lock()
                    .unwrap()
                    .as_ref()
                {
                    hook();
                }
                // 4d.3: full-report cache pages feed the cache
                // accumulator (cache-before-FS: they never reach the FS
                // chain below). The declared total passed here is the
                // report's FULL total (cache + FS ids), so a MIXED
                // worker's cache side can never self-Complete — only a
                // cache-only worker's can; the FS accumulator reaching
                // its total below is the single authoritative
                // end-of-report trigger for everyone.
                let session = list.worker_session_id.clone().unwrap_or_default();
                match self.cache_service.cache_full_report_page(
                    list.worker_id,
                    &session,
                    list.total_len,
                    &cache_items,
                ) {
                    crate::cache::cache_service::CacheFullReportOutcome::Complete(
                        entries,
                        ticket,
                    ) => {
                        cache_complete = Some((entries, ticket));
                    }
                    crate::cache::cache_service::CacheFullReportOutcome::Partial
                    | crate::cache::cache_service::CacheFullReportOutcome::Skipped => {}
                }
            } else {
                let session = list.worker_session_id.as_deref().unwrap_or("");
                let outcome =
                    self.cache_service
                        .incr_block_report(list.worker_id, session, &cache_items)?;
                self.apply_cache_incr_outcome(list.worker_id, outcome);
            }
        }

        //(Whether to increase, block id, block location)
        let mut checked = Vec::with_capacity(fs_blocks.len());
        let mut delete_blocks = Vec::new();
        let mut missing_blocks = 0usize;
        let mut not_file_blocks = 0usize;
        for item in fs_blocks {
            match item.status {
                BlockReportStatus::Finalized | BlockReportStatus::Writing => {
                    let defer_writing_delete =
                        item.status == BlockReportStatus::Writing && !list.full_report;
                    let state = match self.block_inode_state(item.id) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!("block_report {item:?}: {e}");
                            continue;
                        }
                    };
                    match state {
                        BlockInodeState::File => checked.push((item, Some(BlockInodeState::File))),
                        BlockInodeState::Missing if defer_writing_delete => {
                            warn!(
                                "block_report deferred deletion for writing block {} on worker {} because its inode is missing",
                                item.id, list.worker_id
                            );
                        }
                        BlockInodeState::NotFile if defer_writing_delete => {
                            warn!(
                                "block_report deferred deletion for writing block {} on worker {} because its inode is not a file",
                                item.id, list.worker_id
                            );
                        }
                        BlockInodeState::Missing => {
                            missing_blocks += 1;
                            delete_blocks.push(item.id);
                            checked.push((item, Some(BlockInodeState::Missing)));
                        }
                        BlockInodeState::NotFile => {
                            not_file_blocks += 1;
                            delete_blocks.push(item.id);
                            checked.push((item, Some(BlockInodeState::NotFile)));
                        }
                    }
                }
                BlockReportStatus::Deleted => checked.push((item, None)),
            }
        }
        if missing_blocks > 0 || not_file_blocks > 0 {
            warn!(
                "block_report found {} missing-inode and {} non-file-inode blocks for worker {}; scheduling worker deletion",
                missing_blocks, not_file_blocks, list.worker_id
            );
        }

        let mut batch: Vec<(bool, i64, BlockLocation)> = vec![];
        let mut wm = self.worker_manager.write();
        for (item, exists) in checked {
            let loc = BlockLocation::new(list.worker_id, item.storage_type);
            match item.status {
                BlockReportStatus::Finalized | BlockReportStatus::Writing => {
                    let state = match exists {
                        Some(v) => v,
                        None => {
                            warn!(
                                "block_report invariant violated: missing inode state for block {}",
                                item.id
                            );
                            continue;
                        }
                    };

                    match state {
                        BlockInodeState::File => batch.push((true, item.id, loc)),
                        BlockInodeState::Missing | BlockInodeState::NotFile => {
                            batch.push((false, item.id, loc));
                            wm.remove_block(list.worker_id, item.id);
                        }
                    }
                }
                BlockReportStatus::Deleted => {
                    batch.push((false, item.id, loc));
                    wm.deleted_block(list.worker_id, item.id);
                }
            }
        }
        drop(wm);

        if let Some((reported_blocks, trigger_tag)) = full_reported_blocks {
            // 4d.3: end-of-report trigger for the CACHE domain — the
            // single authoritative signal (the FS accumulator counts
            // ALL ids, cache + FS). Snapshot = the self-Complete stash
            // with its atomic ticket (cache-only worker) or the worker's
            // still-Partial same-session accumulator consumed here
            // (mixed worker, checkout EXACT on the trigger's bound
            // Start-identity tag — RC2 P0-1: a same-session Start retry
            // installed a fresh row the old trigger can never consume);
            // a terminal/absent/retried row yields None and the
            // reconcile is skipped (no authoritative snapshot,
            // 复活禁止). The reconcile itself is fenced on the exact
            // (epoch, session, tag, attempt, generation) ticket; its WM
            // side effects ride the same fenced transition as an
            // incremental's.
            let session = list.worker_session_id.clone().unwrap_or_default();
            // #[cfg(test)] deterministic seam (RC2 P0-1, gpt56
            // `53516250` window 2, mixed variant): the FS trigger has
            // completed (reported set + bound tag in hand), the cache
            // snapshot has NOT been taken yet. A hook may run a
            // same-wire-session Start RETRY here — the take below must
            // then return None (fresh row under a new tag) and the old
            // trigger must have zero effect. Compiled out outside
            // cfg(test); never set in production.
            #[cfg(test)]
            if let Some(hook) = crate::master::master_handler::FULL_TAKE_SEAM
                .lock()
                .unwrap()
                .as_ref()
            {
                hook();
            }
            // RC1 P0-1 / RC2 P0-1: BOTH Complete paths share the ONE
            // checkout state machine and hand out the exact
            // (tag, attempt) ticket atomically — the self-Complete
            // stash (cache-only worker) carries it from the in-place
            // transition, the mixed path consumes the still-Partial row
            // via the same transition, exact on the trigger's tag.
            // RC2 focused check (gpt56 `e6207e1d`): the stash is ONLY
            // consumable when its ticket binds the SAME Start tag the
            // FS trigger captured — a same-wire-session Start retry
            // landing between the authorization and the cache page
            // processing lets the fresh row swallow this RPC's page and
            // self-Complete with a NEW-tag ticket; that ticket is
            // dropped here (no reconcile, and crucially NO release of
            // the retried row, which stays Reconciling until a new
            // Start reopens it).
            let snapshot = cache_complete
                .take()
                .filter(|(_, ticket)| ticket.tag == trigger_tag)
                .or_else(|| {
                    self.cache_service.take_cache_full_snapshot(
                        list.worker_id,
                        &session,
                        trigger_tag,
                    )
                });
            if let Some((entries, ticket)) = snapshot {
                // #[cfg(test)] deterministic seam (RC1 P0-1, gpt56
                // `d2546338` item 1): the checkout window — the row is
                // Reconciling (ticket checked out), the reconcile and
                // the exact-CAS release have not run. A hook may run an
                // incremental report or an End/lost retire and WIN the
                // row (terminalize it in place), or a same-session
                // Start retry (the ticket's exact match must then fail);
                // the release CAS must fail and the row must stay
                // terminal / belong to the retry. Compiled out outside
                // cfg(test); never set in production.
                #[cfg(test)]
                if let Some(hook) = crate::master::master_handler::FULL_TRIGGER_SEAM
                    .lock()
                    .unwrap()
                    .as_ref()
                {
                    hook();
                }
                let outcome = self.cache_service.reconcile_cache_full_report(
                    list.worker_id,
                    &session,
                    ticket,
                    &entries,
                )?;
                self.apply_cache_incr_outcome(list.worker_id, outcome);
                // RC1 P0-1 (gpt56 `d2546338` item 1): finish the checkout
                // — the ONLY path back to Accumulating is an exact
                // `(session, tag, attempt)` CAS on the in-place row. A
                // row TERMINALIZED mid-flight (incremental / End / lost
                // raced the flight) stays terminal (`0b900a2f`); a row a
                // newer Start installed stays untouched. No
                // remove-then-blind-insert anywhere in the lifecycle.
                self.cache_service.release_full_accumulator(
                    list.worker_id,
                    &session,
                    ticket.tag,
                    ticket.attempt,
                );
            }

            // 4d.2: the FS reconcile set covers FS blocks only — cache
            // ids in the accumulated total are excluded here (the 4d.3
            // full-report reconcile owns the cache-domain exact set).
            let fs_reported: HashSet<i64> = reported_blocks
                .into_iter()
                .filter(|id| !BlockIdCodec::is_cache_block_id(*id).unwrap_or(false))
                .collect();
            self.submit_full_block_reconcile(list.worker_id, fs_reported, replication_handler)?;
        }

        self.apply_block_report_batch(batch)?;

        Ok(BlockReportResult { delete_blocks })
    }

    fn submit_full_block_reconcile(
        &self,
        worker_id: u32,
        reported_blocks: HashSet<i64>,
        replication_handler: Option<MasterReplicationHandler>,
    ) -> FsResult<()> {
        let should_spawn = {
            let mut reconciles = self.full_block_reconciles.lock();
            let state = reconciles
                .entry(worker_id)
                .or_insert_with(|| FullBlockReconcileState {
                    running: false,
                    generation: 0,
                    pending: None,
                });
            state.generation = state.generation.saturating_add(1);
            let generation = state.generation;
            state.pending = Some(FullBlockReconcileJob {
                generation,
                reported_blocks,
            });
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };

        if !should_spawn {
            return Ok(());
        }

        let fs = self.clone();
        let res = self
            .full_block_reconcile_executor
            .fixed_spawn(worker_id as i64, move || {
                fs.run_full_block_reconcile(worker_id, replication_handler);
                // RC1 (gpt56 `d2546338` join-panic note): this task may
                // hold the LAST `MasterFilesystem` clone, whose Drop
                // chain releases the last Arc to the executor — and the
                // executor's Drop JOINS its pool threads. Dropping that
                // on THIS pool thread is a pthread self-join
                // (`failed to join thread: Resource deadlock avoided`).
                // Hand the final clones to a detached thread so the
                // join happens off-pool. (Task-local drops are moved
                // into the closure together with `fs`.)
                std::thread::spawn(move || drop(fs));
            });
        if let Err(e) = &res {
            self.full_block_reconciles.lock().remove(&worker_id);
            error!("submit full block report reconcile for worker {worker_id} failed: {e}");
        }
        res?;
        Ok(())
    }

    fn run_full_block_reconcile(
        &self,
        worker_id: u32,
        replication_handler: Option<MasterReplicationHandler>,
    ) {
        loop {
            let job = {
                let mut reconciles = self.full_block_reconciles.lock();
                match reconciles.get_mut(&worker_id) {
                    Some(state) => match state.pending.take() {
                        Some(v) => v,
                        None => {
                            reconciles.remove(&worker_id);
                            return;
                        }
                    },
                    None => return,
                }
            };

            if !self.is_full_block_reconcile_current(worker_id, job.generation) {
                info!(
                    "skip stale full block report reconcile for worker {}, generation {}",
                    worker_id, job.generation
                );
                continue;
            }

            match self.reconcile_full_block_report(worker_id, job.generation, job.reported_blocks) {
                Ok(stale_block_ids) => {
                    let stale_block_count = stale_block_ids.len();
                    if stale_block_count > 0 {
                        info!(
                            "full block report reconciled {} stale block locations for worker {}",
                            stale_block_count, worker_id
                        );
                        if let Some(replication_handler) = &replication_handler {
                            if let Err(e) = replication_handler
                                .report_under_replicated_blocks(worker_id, stale_block_ids)
                            {
                                error!(
                                    "Errors on reporting under-replicated {} blocks from full block report reconciliation. err: {:?}",
                                    stale_block_count, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "full block report reconcile for worker {} failed: {}",
                        worker_id, e
                    );
                }
            }
        }
    }

    fn reconcile_full_block_report(
        &self,
        worker_id: u32,
        generation: u64,
        reported_blocks: HashSet<i64>,
    ) -> FsResult<Vec<i64>> {
        let existing_blocks = {
            let fs_dir = self.fs_dir.read();
            fs_dir.get_worker_block_ids(worker_id)?
        };

        let mut stale_block_ids = Vec::new();
        let mut batch = Vec::new();
        for block_id in existing_blocks {
            if !reported_blocks.contains(&block_id) {
                batch.push((false, block_id, BlockLocation::with_id(worker_id)));
                stale_block_ids.push(block_id);
            }
        }

        if !batch.is_empty() {
            let reconciles = self.full_block_reconciles.lock();
            if !reconciles
                .get(&worker_id)
                .map(|state| state.generation == generation)
                .unwrap_or(false)
            {
                info!(
                    "skip stale full block report reconcile apply for worker {}, generation {}",
                    worker_id, generation
                );
                return Ok(Vec::new());
            }
            self.apply_block_report_batch(batch)?;
        }

        Ok(stale_block_ids)
    }

    /// Applies block-report location updates in bounded chunks so the global
    /// fs_dir write lock is held only briefly per chunk. Each entry is an
    /// independent add/remove for one block location, so chunk boundaries do
    /// not break cross-entry invariants.
    fn apply_block_report_batch(&self, batch: Vec<(bool, i64, BlockLocation)>) -> FsResult<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut iter = batch.into_iter();
        loop {
            let chunk: Vec<_> = iter.by_ref().take(Self::BLOCK_REPORT_WRITE_CHUNK).collect();
            if chunk.is_empty() {
                break;
            }
            let mut fs_dir = self.fs_dir.write();
            fs_dir.block_report(chunk)?;
        }
        Ok(())
    }

    fn is_full_block_reconcile_current(&self, worker_id: u32, generation: u64) -> bool {
        self.full_block_reconciles
            .lock()
            .get(&worker_id)
            .map(|state| state.generation == generation)
            .unwrap_or(false)
    }

    pub fn delete_locations(&self, worker_id: u32) -> FsResult<LostWorkerLocationCleanup> {
        let removed_block_ids = {
            let fs_dir = self.fs_dir.write();
            fs_dir.delete_locations(worker_id)?
        };
        let mut invalidated = CacheInvalidationResult::default();

        for chunk in removed_block_ids.chunks(Self::LOST_WORKER_INVALIDATION_CHUNK) {
            let result = {
                let mut fs_dir = self.fs_dir.write();
                fs_dir.invalidate_lost_cache_files(chunk)
            };
            match result {
                Ok(result) => invalidated.extend(result),
                Err(e) => warn!(
                    "failed to invalidate lost cache files for worker {} ({} block ids); \\
                     continuing with normal replica recovery: {}",
                    worker_id,
                    chunk.len(),
                    e
                ),
            }
        }

        let replication_block_ids = removed_block_ids
            .iter()
            .copied()
            .filter(|block_id| !invalidated.invalidated_block_ids.contains(block_id))
            .collect();

        if !invalidated.delete_result.blocks.is_empty() {
            self.worker_manager
                .write()
                .remove_blocks(&invalidated.delete_result);
        }

        Ok(LostWorkerLocationCleanup {
            removed_block_ids,
            replication_block_ids,
        })
    }

    pub fn set_attr<T: AsRef<str>>(&self, path: T, opts: SetAttrOpts) -> FsResult<FileStatus> {
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path.as_ref())?;
        fs_dir.set_attr(inp, opts)
    }

    pub fn symlink<T: AsRef<str>>(
        &self,
        target: T,
        link: T,
        force: bool,
        mode: u32,
    ) -> FsResult<()> {
        self.symlink_with_owner_group(target, link, force, mode, None, None)
    }

    pub fn symlink_with_owner_group<T: AsRef<str>>(
        &self,
        target: T,
        link: T,
        force: bool,
        mode: u32,
        owner: Option<String>,
        group: Option<String>,
    ) -> FsResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let target = target.as_ref().to_string();
        let link = Self::resolve_path(&fs_dir, link.as_ref())?;
        fs_dir.symlink(target, link, force, mode, owner, group)
    }

    pub fn link<T: AsRef<str>>(&self, src_path: T, dst_path: T) -> FsResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let src_path = Self::resolve_path(&fs_dir, src_path.as_ref())?;
        let dst_path = Self::resolve_path(&fs_dir, dst_path.as_ref())?;
        fs_dir.link(src_path, dst_path)
    }

    pub fn resize<T: AsRef<str>>(&self, path: T, opts: FileAllocOpts) -> FsResult<FileBlocks> {
        opts.validate()?;

        let path = path.as_ref();
        // This snapshot only rejects individually impossible requests; it is not a
        // reservation, so concurrent fallocates may observe the same capacity.
        // Worker-side block allocation remains the hard enforcement point.
        let available = if opts.truncate {
            i64::MAX
        } else {
            self.worker_manager.read().available_bytes()
        };
        let (del_res, inode_id) = {
            let mut fs_dir = self.fs_dir.write();
            let inp = Self::resolve_path(&fs_dir, path)?;
            let inode = try_option!(inp.get_last_inode(), "File {} not exists", path);
            let file = inode.as_file_ref()?;
            Self::validate_alloc_capacity(file.len, file.replicas, &opts, available)?;
            let inode_id = inode.id();
            let del_res = fs_dir.resize(&inp, opts)?;
            (del_res, inode_id)
        };

        if !del_res.blocks.is_empty() {
            self.worker_manager.write().remove_blocks(&del_res);
        }

        let blocks = self.get_block_locations(path)?;
        if blocks.status.id != inode_id {
            return err_box!(
                "Path {} resolved to different inode after resize, expected {}, got {}",
                path,
                inode_id,
                blocks.status.id
            );
        }

        Ok(blocks)
    }

    pub fn assign_worker<T: AsRef<str>>(
        &self,
        path: T,
        block: ExtendedBlock,
        client_addr: ClientAddress,
        exclude_workers: Vec<u32>,
    ) -> FsResult<LocatedBlock> {
        let path = path.as_ref();
        let mut fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        let choose_workers = self.choose_worker(&inp, client_addr, exclude_workers)?;
        let has_spdk = {
            let wm = self.worker_manager.read();
            wm.workers_have_spdk(&choose_workers)
        };
        let block = fs_dir.assign_worker(inp, block.id, &choose_workers)?;

        Ok(LocatedBlock {
            block,
            locs: choose_workers,
            has_spdk,
        })
    }

    pub fn get_lock<T: AsRef<str>>(&self, path: T, lock: FileLock) -> FsResult<Option<FileLock>> {
        let path = path.as_ref();

        let fs_dir = self.fs_dir.read();
        let inp = Self::resolve_path(&fs_dir, path)?;
        let expire_ms = self.conf.lock_expire_time_ms();

        fs_dir.get_lock(inp, &lock, expire_ms)
    }

    pub fn set_lock<T: AsRef<str>>(&self, path: T, lock: FileLock) -> FsResult<Option<FileLock>> {
        let path = path.as_ref();

        let fs_dir = self.fs_dir.write();
        let inp = Self::resolve_path(&fs_dir, path)?;

        fs_dir.set_lock(inp, lock, self.conf.lock_expire_time_ms())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_core_error::ErrorExt;
    use curvine_error::ErrorKind;
    use curvine_runtime::common::Utils;

    fn test_fs(name: &str) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "master-fs-resolve-test/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "master-fs-resolve-test/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        JournalSystem::fs_only_for_test(&conf).unwrap()
    }

    fn assert_file_not_found_roundtrip(err: &FsError) {
        assert!(
            matches!(err.kind(), ErrorKind::FileNotFound),
            "expected FileNotFound, got {:?}",
            err.kind()
        );
        let decoded = FsError::decode(err.encode());
        assert!(
            matches!(decoded.kind(), ErrorKind::FileNotFound),
            "expected FileNotFound after encode/decode, got {:?}",
            decoded.kind()
        );
        assert!(
            matches!(decoded, FsError::FileNotFound(_)),
            "decoded error collapsed away from FileNotFound: {}",
            decoded
        );
    }

    #[test]
    fn fallocate_rejects_growth_larger_than_available_capacity() {
        let opts = FileAllocOpts::with_alloc(200, FileAllocMode::DEFAULT);
        let err = MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 359).unwrap_err();
        assert!(matches!(err, FsError::DiskOutOfSpace(_)));
    }

    /// P1-6: the production cache chooser fails closed at construction
    /// when `client.replicas` exceeds the per-block location cap — the
    /// replication bounds alone would let a 17-replica plan be built and
    /// then truncated to 16, making the commit's replica-policy check
    /// permanently unsatisfiable.
    #[test]
    fn cache_chooser_rejects_replicas_above_location_cap() {
        Master::init_test_metrics();
        let workers = SyncWorkerManager::new(
            crate::master::fs::WorkerManager::new(&ClusterConf::default()).unwrap(),
        );
        let mut conf = ClusterConf::format();
        // Inside the default replication bounds [1, 100], but above the
        // per-block cap of MAX_LOCATIONS_PER_BLOCK (16).
        conf.client.replicas = (crate::master::cache::MAX_LOCATIONS_PER_BLOCK + 1) as i32;
        let err = match MasterFilesystem::cache_chooser(&conf, &workers) {
            Err(e) => e,
            Ok(_) => panic!("replicas above the location cap must fail closed"),
        };
        let msg = format!("{}", err);
        assert!(
            msg.contains("per-block location cap"),
            "expected the location-cap rejection, got: {}",
            msg
        );

        // Exactly at the cap is accepted.
        conf.client.replicas = crate::master::cache::MAX_LOCATIONS_PER_BLOCK as i32;
        assert!(MasterFilesystem::cache_chooser(&conf, &workers).is_ok());
    }

    #[test]
    fn fallocate_accepts_exact_available_capacity() {
        let opts = FileAllocOpts::with_alloc(200, FileAllocMode::DEFAULT);
        assert!(MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 360).is_ok());
    }

    #[test]
    fn truncate_growth_does_not_require_physical_capacity() {
        let opts = FileAllocOpts::with_truncate(200);
        assert!(MasterFilesystem::validate_alloc_capacity(20, 2, &opts, 0).is_ok());
    }

    #[test]
    fn resolve_file_inode_missing_inode_id_returns_file_not_found() {
        let fs = test_fs("missing-inode-id");
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.read();
        let missing_id = 9_999_999_i64;
        let err = MasterFilesystem::resolve_file_inode(&fs_dir, "/missing", Some(missing_id))
            .unwrap_err();

        assert_file_not_found_roundtrip(&err);
        let msg = err.to_string();
        assert!(
            msg.contains(&format!("inode_id={}", missing_id)),
            "expected inode_id in error context, got: {}",
            msg
        );
    }

    #[test]
    fn resolve_file_inode_unresolved_path_returns_file_not_found() {
        let fs = test_fs("unresolved-path");
        let sync_fs_dir = fs.fs_dir();
        let fs_dir = sync_fs_dir.read();
        let path = "/does/not/exist";
        let err = MasterFilesystem::resolve_file_inode(&fs_dir, path, None).unwrap_err();

        assert_file_not_found_roundtrip(&err);
        assert!(
            err.to_string().contains(path),
            "expected path in error, got: {}",
            err
        );
    }

    fn worker_with_status(
        id: u32,
        status: WorkerStatus,
        capacity: i64,
        available: i64,
    ) -> WorkerInfo {
        let mut worker = WorkerInfo::new(
            WorkerAddress {
                worker_id: id,
                ..Default::default()
            },
            1,
        );
        worker.status = status;
        worker.capacity = capacity;
        worker.available = available;
        worker
    }

    #[test]
    fn filesystem_info_allocatable_only_counts_live_workers() {
        // See issue #1460: statfs must report capacity eligible for new writes,
        // i.e. only Live workers. Blacklist/Decommission workers must not
        // contribute to the allocatable view (they still count toward the total
        // physical capacity).
        let fs = test_fs("allocatable-live-only");
        fs.add_test_worker(worker_with_status(1, WorkerStatus::Live, 1000, 800));
        fs.add_test_worker(worker_with_status(2, WorkerStatus::Blacklist, 2000, 1500));
        fs.add_test_worker(worker_with_status(
            3,
            WorkerStatus::Decommission,
            3000,
            2500,
        ));

        let info = fs.filesystem_info().unwrap();

        // Total physical capacity across all non-lost worker states.
        assert_eq!(info.capacity, 6000, "total capacity should sum all workers");
        assert_eq!(
            info.available, 4800,
            "total available should sum all workers"
        );
        assert_eq!(info.live_workers.len(), 1);
        assert_eq!(info.blacklist_workers.len(), 1);
        assert_eq!(info.decommission_workers.len(), 1);

        // Allocatable view mirrors allocation eligibility: Live workers only.
        assert_eq!(
            info.allocatable_capacity, 1000,
            "allocatable capacity must be Live workers only"
        );
        assert_eq!(
            info.allocatable_available, 800,
            "allocatable available must be Live workers only"
        );
    }

    #[test]
    fn filesystem_info_allocatable_excludes_failed_storage_dirs() {
        // A Live worker with a healthy dir plus a failed dir: add_storage already
        // skips failed dirs, so neither total nor allocatable capacity should
        // include the failed dir's bytes.
        let fs = test_fs("allocatable-excludes-failed");
        let mut worker = WorkerInfo::new(
            WorkerAddress {
                worker_id: 10,
                ..Default::default()
            },
            1,
        );
        worker.status = WorkerStatus::Live;
        worker.add_storage(StorageInfo {
            dir_id: 1,
            storage_id: "healthy".into(),
            failed: false,
            capacity: 1000,
            available: 800,
            ..Default::default()
        });
        worker.add_storage(StorageInfo {
            dir_id: 2,
            storage_id: "failed".into(),
            failed: true,
            capacity: 500,
            available: 400,
            ..Default::default()
        });
        fs.add_test_worker(worker);

        let info = fs.filesystem_info().unwrap();

        assert_eq!(
            info.capacity, 1000,
            "failed dir must not count toward total"
        );
        assert_eq!(info.available, 800);
        assert_eq!(info.allocatable_capacity, 1000);
        assert_eq!(info.allocatable_available, 800);
    }

    #[test]
    fn filesystem_info_allocatable_excludes_lost_workers() {
        // Lost workers live in a separate map and must not contribute to either
        // the total or the allocatable view. Insert a lost worker directly into
        // the lost map (add_test_worker only touches the live map) and assert its
        // capacity never leaks into aggregation.
        let fs = test_fs("allocatable-excludes-lost");
        fs.add_test_worker(worker_with_status(1, WorkerStatus::Live, 1000, 800));
        fs.worker_manager
            .write()
            .worker_map
            .lost_workers
            .insert(99, worker_with_status(99, WorkerStatus::Lost, 9999, 9999));

        let info = fs.filesystem_info().unwrap();

        // The lost worker is reported but its bytes stay out of both views.
        assert_eq!(info.lost_workers.len(), 1);
        assert_eq!(
            info.capacity, 1000,
            "lost worker capacity must not leak into total"
        );
        assert_eq!(info.available, 800);
        assert_eq!(
            info.allocatable_capacity, 1000,
            "lost worker capacity must not leak into allocatable"
        );
        assert_eq!(info.allocatable_available, 800);
    }

    fn token(client: u64, seq: u64) -> crate::master::meta::cache::entry::OpToken {
        crate::master::meta::cache::entry::OpToken {
            client_id: client,
            op_seq: seq,
        }
    }

    /// 4d.2 cache-before-FS diversion at the block_report boundary: a
    /// mixed page (fs ids + cache ids) routes cache items into the cache
    /// domain only — they never reach the FS classify loop (no
    /// missing-inode deletion for cache ids, `delete_blocks` carries FS
    /// ids only) — while a complete Valid×Finalized cache publish makes
    /// the whole-object read path serve the worker, and a cache Deleted
    /// removes the replica with zero FS effect.
    #[test]
    fn test_4d2_block_report_mixed_page_zero_penetration() {
        use curvine_raft::raft::RoleState;

        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.cache_metadata_enabled = true;
        conf.master.meta_dir =
            Utils::test_sub_dir(format!("master-fs-4d2-mixed/meta-{}", Utils::rand_str(6)));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "master-fs-4d2-mixed/journal-{}",
            Utils::rand_str(6)
        ));
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);

        let addr = WorkerAddress {
            worker_id: 1,
            hostname: "mixed-1".into(),
            ip_addr: "10.0.0.1".into(),
            rpc_port: 8200,
            web_port: 8300,
        };
        fs.begin_worker_session(&addr, "s1").unwrap();

        // Committed Valid cache entry: OBJ, len 150 -> 64/64/22.
        let obj = BlockIdCodec::CACHE_OBJECT_MIN;
        let lay = crate::master::meta::CacheBlockLayout::derive(obj, 150, 64).unwrap();
        {
            let store = fs.fs_dir.read();
            let rocks = store.get_rocks_store();
            let mgr = &store.cache;
            mgr.apply_incarnation_allocate_v2(rocks, token(91, 1), 5, 1, 0)
                .unwrap();
            mgr.apply_id_reserve(rocks, token(1, 1), obj, obj + 100)
                .unwrap();
            let alloc = crate::master::meta::cache::entry::CacheEntry {
                generation: 1,
                state: crate::master::meta::cache::entry::CacheEntryState::Reserved,
                object_id: obj,
                len: 0,
                ufs_mtime: 0,
                block_size: 64,
                expire_at: 0,
            };
            mgr.apply_allocate(rocks, token(2, 1), 1, "/k", 150, &alloc)
                .unwrap();
            mgr.apply_commit(
                rocks,
                token(2, 1),
                token(2, 2),
                1,
                "/k",
                1,
                obj,
                150,
                777,
                0,
            )
            .unwrap();
        }

        // One mixed INCREMENTAL page: an fs id whose inode is missing
        // (existing FS behavior: scheduled deletion) plus the three
        // cache block ids with exact lengths.
        let list = BlockReportList {
            cluster_id: conf.cluster_id.clone(),
            worker_id: 1,
            full_report: false,
            total_len: 0,
            blocks: vec![
                BlockReportInfo::new(500, BlockReportStatus::Finalized, StorageType::Disk, 64),
                BlockReportInfo::new(
                    lay.block_id(1).unwrap(),
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    64,
                ),
                BlockReportInfo::new(
                    lay.block_id(2).unwrap(),
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    64,
                ),
                BlockReportInfo::new(
                    lay.block_id(3).unwrap(),
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    22,
                ),
            ],
            worker_session_id: Some("s1".into()),
        };
        let res = fs.block_report(list, None).unwrap();
        assert_eq!(
            res.delete_blocks,
            vec![500],
            "cache ids never leak into the FS delete set"
        );

        // The cache publish completed the whole-object location set.
        let hit = fs
            .cache_service
            .get(1, "/k", true)
            .unwrap()
            .expect("cache blocks published through the routed incremental");
        assert_eq!(hit.blocks.len(), 3);
        assert_eq!(hit.blocks[2].block_len, 22);

        // A cache Deleted page: replica removed, zero FS effect.
        let list = BlockReportList {
            cluster_id: conf.cluster_id.clone(),
            worker_id: 1,
            full_report: false,
            total_len: 0,
            blocks: vec![BlockReportInfo::with_deleted(lay.block_id(2).unwrap(), 64)],
            worker_session_id: Some("s1".into()),
        };
        let res = fs.block_report(list, None).unwrap();
        assert!(res.delete_blocks.is_empty());
        assert!(
            fs.cache_service.get(1, "/k", true).unwrap().is_none(),
            "losing one block's only replica is a whole-object miss"
        );
    }

    /// 4d.3 full-report reconcile end-to-end at the block_report
    /// boundary: the FS accumulator (counting ALL ids, cache + FS) is
    /// the single end-of-report trigger; cache pages accumulate
    /// cache-side only (zero FS penetration — `delete_blocks` never
    /// carries a cache id); a MIXED report reconciles its cache
    /// snapshot at the final page (the accumulator reopen lets the
    /// NEXT periodic full report accumulate again, including the
    /// self-Complete path), and a Deleted-only cache full report is an
    /// exact replace: the missing identities strip, the Deleted acks,
    /// and the FS delete set stays empty.
    #[test]
    fn test_4d3_block_report_full_reconcile_end_to_end() {
        use curvine_raft::raft::RoleState;

        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.cache_metadata_enabled = true;
        conf.master.meta_dir =
            Utils::test_sub_dir(format!("master-fs-4d3-full/meta-{}", Utils::rand_str(6)));
        conf.journal.journal_dir =
            Utils::test_sub_dir(format!("master-fs-4d3-full/journal-{}", Utils::rand_str(6)));
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);

        let addr = WorkerAddress {
            worker_id: 1,
            hostname: "full-1".into(),
            ip_addr: "10.0.0.1".into(),
            rpc_port: 8200,
            web_port: 8300,
        };
        fs.begin_worker_session(&addr, "s1").unwrap();

        // Committed Valid cache entry: OBJ, len 150 -> 64/64/22.
        let obj = BlockIdCodec::CACHE_OBJECT_MIN;
        let lay = crate::master::meta::CacheBlockLayout::derive(obj, 150, 64).unwrap();
        let b1 = lay.block_id(1).unwrap();
        let b2 = lay.block_id(2).unwrap();
        let b3 = lay.block_id(3).unwrap();
        {
            let store = fs.fs_dir.read();
            let rocks = store.get_rocks_store();
            let mgr = &store.cache;
            mgr.apply_incarnation_allocate_v2(rocks, token(91, 1), 5, 1, 0)
                .unwrap();
            mgr.apply_id_reserve(rocks, token(1, 1), obj, obj + 100)
                .unwrap();
            let alloc = crate::master::meta::cache::entry::CacheEntry {
                generation: 1,
                state: crate::master::meta::cache::entry::CacheEntryState::Reserved,
                object_id: obj,
                len: 0,
                ufs_mtime: 0,
                block_size: 64,
                expire_at: 0,
            };
            mgr.apply_allocate(rocks, token(2, 1), 1, "/k", 150, &alloc)
                .unwrap();
            mgr.apply_commit(
                rocks,
                token(2, 1),
                token(2, 2),
                1,
                "/k",
                1,
                obj,
                150,
                777,
                0,
            )
            .unwrap();
        }

        let list = |full: bool, total: u64, blocks: Vec<BlockReportInfo>| BlockReportList {
            cluster_id: conf.cluster_id.clone(),
            worker_id: 1,
            full_report: full,
            total_len: total,
            blocks,
            worker_session_id: Some("s1".into()),
        };
        let cfinal = |id: i64, len: i64| {
            BlockReportInfo::new(id, BlockReportStatus::Finalized, StorageType::Disk, len)
        };

        // Page 1 of a 5-id MIXED full report: 2 FS ids (missing inodes
        // — the FS domain schedules their deletion immediately) + 2
        // cache ids (accumulated cache-side only).
        let res = fs
            .block_report(
                list(
                    true,
                    5,
                    vec![
                        cfinal(500, 64),
                        cfinal(501, 64),
                        cfinal(b1, 64),
                        cfinal(b2, 64),
                    ],
                ),
                None,
            )
            .unwrap();
        assert_eq!(res.delete_blocks, vec![500, 501]);
        assert!(
            fs.cache_service.get(1, "/k", true).unwrap().is_none(),
            "cache side still accumulating"
        );

        // Final page: the FS accumulator completes (5/5 ids counted,
        // cache + FS) — the end-of-report trigger fires the 4d.3 cache
        // reconcile. Cache ids never leak into the FS delete set.
        let res = fs
            .block_report(list(true, 5, vec![cfinal(b3, 22)]), None)
            .unwrap();
        assert!(res.delete_blocks.is_empty());
        let hit = fs
            .cache_service
            .get(1, "/k", true)
            .unwrap()
            .expect("cache snapshot reconciled at end-of-report");
        assert_eq!(hit.blocks.len(), 3);
        assert_eq!(hit.blocks[2].block_len, 22);

        // The accumulator was reopened: the NEXT periodic full report
        // (this time cache-only) accumulates again and self-Completes;
        // its reconcile is idempotent (still served, no dup rows).
        let res = fs
            .block_report(
                list(
                    true,
                    3,
                    vec![cfinal(b1, 64), cfinal(b2, 64), cfinal(b3, 22)],
                ),
                None,
            )
            .unwrap();
        assert!(res.delete_blocks.is_empty());
        let hit = fs.cache_service.get(1, "/k", true).unwrap().unwrap();
        assert_eq!(hit.blocks.len(), 3);

        // A Deleted-only cache full report is an EXACT replace: the
        // two missing identities strip, the Deleted acks through the
        // fenced transition, and the FS delete set stays empty
        // (Deleted zero-penetration).
        let res = fs
            .block_report(
                list(true, 1, vec![BlockReportInfo::with_deleted(b2, 64)]),
                None,
            )
            .unwrap();
        assert!(
            res.delete_blocks.is_empty(),
            "cache Deleted never penetrates the FS delete set"
        );
        assert!(
            fs.cache_service.get(1, "/k", true).unwrap().is_none(),
            "exact replace: missing + deleted identities removed the object"
        );
    }

    /// RC1 P0-1 (gpt56 `d2546338` item 1): the end-of-report checkout
    /// keeps the accumulator row IN PLACE (Reconciling + attempt) — an
    /// incremental or an exact End that WINS the checkout window can
    /// still terminalize the row, the reconcile no-ops, the release CAS
    /// never resurrects the terminal row, late same-session full pages
    /// stay Skipped, and only a new Start reopens the worker.
    #[test]
    fn test_4d3_rc1_p01_checkout_window_winners() {
        let build = |tag: &str| {
            Master::init_test_metrics();
            let mut conf = ClusterConf::format();
            conf.testing = true;
            conf.journal.enable = false;
            conf.master.cache_metadata_enabled = true;
            conf.master.meta_dir = Utils::test_sub_dir(format!(
                "master-fs-4d3-rc1win/meta-{}-{}",
                tag,
                Utils::rand_str(6)
            ));
            conf.journal.journal_dir = Utils::test_sub_dir(format!(
                "master-fs-4d3-rc1win/journal-{}-{}",
                tag,
                Utils::rand_str(6)
            ));
            let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
            fs.master_monitor
                .journal_ctl
                .set_state(curvine_raft::raft::RoleState::Leader);
            let addr = WorkerAddress {
                worker_id: 1,
                hostname: format!("rc1win-{}", tag),
                ip_addr: "10.0.0.1".into(),
                rpc_port: 8200,
                web_port: 8300,
            };
            fs.begin_worker_session(&addr, "s1").unwrap();
            (fs, conf, addr)
        };
        let commit_entry = |fs: &MasterFilesystem| {
            let obj = BlockIdCodec::CACHE_OBJECT_MIN;
            let lay = crate::master::meta::CacheBlockLayout::derive(obj, 150, 64).unwrap();
            {
                let store = fs.fs_dir.read();
                let rocks = store.get_rocks_store();
                let mgr = &store.cache;
                mgr.apply_incarnation_allocate_v2(rocks, token(91, 1), 5, 1, 0)
                    .unwrap();
                mgr.apply_id_reserve(rocks, token(1, 1), obj, obj + 100)
                    .unwrap();
                let alloc = crate::master::meta::cache::entry::CacheEntry {
                    generation: 1,
                    state: crate::master::meta::cache::entry::CacheEntryState::Reserved,
                    object_id: obj,
                    len: 0,
                    ufs_mtime: 0,
                    block_size: 64,
                    expire_at: 0,
                };
                mgr.apply_allocate(rocks, token(2, 1), 1, "/k", 150, &alloc)
                    .unwrap();
                mgr.apply_commit(
                    rocks,
                    token(2, 1),
                    token(2, 2),
                    1,
                    "/k",
                    1,
                    obj,
                    150,
                    777,
                    0,
                )
                .unwrap();
            }
            (
                obj,
                lay.block_id(1).unwrap(),
                lay.block_id(2).unwrap(),
                lay.block_id(3).unwrap(),
            )
        };
        let cfinal = |id: i64, len: i64| {
            BlockReportInfo::new(id, BlockReportStatus::Finalized, StorageType::Disk, len)
        };

        // Branch 1 — an incremental WINS the window: the corrupt b2
        // report terminalizes the in-place row; the pending snapshot
        // reconcile must no-op and its release must not resurrect.
        {
            let (fs, conf, addr) = build("incr");
            let (obj, b1, b2, b3) = commit_entry(&fs);
            let list = |session: &str, total: u64, blocks: Vec<BlockReportInfo>| BlockReportList {
                cluster_id: conf.cluster_id.clone(),
                worker_id: 1,
                full_report: true,
                total_len: total,
                blocks,
                worker_session_id: Some(session.into()),
            };

            fs.block_report(list("s1", 3, vec![cfinal(b1, 64)]), None)
                .unwrap();
            assert!(
                fs.cache_service.get(1, "/k", true).unwrap().is_none(),
                "still accumulating"
            );

            {
                let fs2 = fs.clone();
                *crate::master::master_handler::FULL_TRIGGER_SEAM
                    .lock()
                    .unwrap() = Some(Box::new(move || {
                    // Same-session corrupt incremental, applied inside
                    // the checkout window.
                    let out = fs2
                        .cache_service
                        .incr_block_report(
                            1,
                            "s1",
                            &[BlockReportInfo::new(
                                b2,
                                BlockReportStatus::Finalized,
                                StorageType::Disk,
                                0,
                            )],
                        )
                        .unwrap();
                    assert_eq!(out.remove_blocks, vec![b2]);
                    fs2.apply_cache_incr_outcome(1, out);
                }));
            }
            fs.block_report(list("s1", 3, vec![cfinal(b2, 64), cfinal(b3, 22)]), None)
                .unwrap();
            *crate::master::master_handler::FULL_TRIGGER_SEAM
                .lock()
                .unwrap() = None;

            // The reconciled snapshot was a superseded flight: nothing
            // published, and the WINNER's effects landed instead.
            assert!(
                fs.cache_service.get(1, "/k", true).unwrap().is_none(),
                "terminalized flight reconciles to a no-op"
            );
            let tag = fs.cache_service.cache_session_tag(1).unwrap();
            assert!(
                fs.cache_service.quarantine_contains(obj, 1, tag, 2),
                "the winning incremental's quarantine survives"
            );

            // The row stayed terminal through the release CAS; a late
            // same-session full page stays Skipped.
            assert_eq!(
                fs.cache_service.session_spine_snapshot(1).accumulator,
                Some(("s1".to_string(), true)),
                "release did not resurrect the terminal row"
            );
            fs.block_report(list("s1", 3, vec![cfinal(b1, 64)]), None)
                .unwrap();
            assert_eq!(
                fs.cache_service.session_spine_snapshot(1).accumulator,
                Some(("s1".to_string(), true))
            );

            // Only a NEW Start reopens the worker's accumulator.
            fs.begin_worker_session(&addr, "s2").unwrap();
            fs.block_report(
                list(
                    "s2",
                    3,
                    vec![cfinal(b1, 64), cfinal(b2, 64), cfinal(b3, 22)],
                ),
                None,
            )
            .unwrap();
            assert_eq!(
                fs.cache_service.session_spine_snapshot(1).accumulator,
                Some(("s2".to_string(), false)),
                "new Start reopens the accumulator"
            );
        }

        // Branch 2 — an exact End WINS the window: registry retired, row
        // terminal, reconcile no-op, only a new Start reopens.
        {
            let (fs, conf, addr) = build("end");
            let (_obj, b1, b2, b3) = commit_entry(&fs);
            let list = |session: &str, total: u64, blocks: Vec<BlockReportInfo>| BlockReportList {
                cluster_id: conf.cluster_id.clone(),
                worker_id: 1,
                full_report: true,
                total_len: total,
                blocks,
                worker_session_id: Some(session.into()),
            };

            fs.block_report(list("s1", 3, vec![cfinal(b1, 64)]), None)
                .unwrap();
            {
                let fs2 = fs.clone();
                *crate::master::master_handler::FULL_TRIGGER_SEAM
                    .lock()
                    .unwrap() = Some(Box::new(move || {
                    fs2.end_worker_session(1, "s1");
                }));
            }
            fs.block_report(list("s1", 3, vec![cfinal(b2, 64), cfinal(b3, 22)]), None)
                .unwrap();
            *crate::master::master_handler::FULL_TRIGGER_SEAM
                .lock()
                .unwrap() = None;

            assert!(
                fs.cache_service.get(1, "/k", true).unwrap().is_none(),
                "ended session's flight reconciles to a no-op"
            );
            assert_eq!(
                fs.cache_service.cache_session_tag(1),
                None,
                "exact End retired the registry row"
            );
            assert_eq!(
                fs.cache_service.session_spine_snapshot(1).accumulator,
                Some(("s1".to_string(), true)),
                "row terminal, release did not resurrect"
            );

            // Late same-session page: zero cache effect; only a new
            // Start reopens.
            fs.block_report(list("s1", 3, vec![cfinal(b1, 64)]), None)
                .unwrap();
            assert_eq!(
                fs.cache_service.session_spine_snapshot(1).accumulator,
                Some(("s1".to_string(), true))
            );
            fs.begin_worker_session(&addr, "s2").unwrap();
            fs.block_report(
                list(
                    "s2",
                    3,
                    vec![cfinal(b1, 64), cfinal(b2, 64), cfinal(b3, 22)],
                ),
                None,
            )
            .unwrap();
            let hit = fs
                .cache_service
                .get(1, "/k", true)
                .unwrap()
                .expect("reopened session publishes");
            assert_eq!(hit.blocks.len(), 3);
        }
    }

    /// RC1 P0-2 (gpt56 `d2546338` item 2): an OLD full-report outcome
    /// (a Deleted ack) paused at its WM apply must be DROPPED when a NEW
    /// same-session corrupt incremental completed and applied inside the
    /// pause — the old ack can neither clear the fresh BlockMap delete
    /// queue entry nor release the fresh quarantine (no re-ordering).
    #[test]
    fn test_4d3_rc1_p02_outcome_generation_reorder() {
        use curvine_model::{HeartbeatStatus, WorkerCommand};

        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.cache_metadata_enabled = true;
        conf.master.meta_dir =
            Utils::test_sub_dir(format!("master-fs-4d3-rc1gen/meta-{}", Utils::rand_str(6)));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "master-fs-4d3-rc1gen/journal-{}",
            Utils::rand_str(6)
        ));
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        fs.master_monitor
            .journal_ctl
            .set_state(curvine_raft::raft::RoleState::Leader);

        let addr = WorkerAddress {
            worker_id: 1,
            hostname: "rc1gen-1".into(),
            ip_addr: "10.0.0.1".into(),
            rpc_port: 8200,
            web_port: 8300,
        };
        fs.begin_worker_session(&addr, "s1").unwrap();

        let obj = BlockIdCodec::CACHE_OBJECT_MIN;
        let lay = crate::master::meta::CacheBlockLayout::derive(obj, 150, 64).unwrap();
        let b1 = lay.block_id(1).unwrap();
        let b2 = lay.block_id(2).unwrap();
        let b3 = lay.block_id(3).unwrap();
        {
            let store = fs.fs_dir.read();
            let rocks = store.get_rocks_store();
            let mgr = &store.cache;
            mgr.apply_incarnation_allocate_v2(rocks, token(91, 1), 5, 1, 0)
                .unwrap();
            mgr.apply_id_reserve(rocks, token(1, 1), obj, obj + 100)
                .unwrap();
            let alloc = crate::master::meta::cache::entry::CacheEntry {
                generation: 1,
                state: crate::master::meta::cache::entry::CacheEntryState::Reserved,
                object_id: obj,
                len: 0,
                ufs_mtime: 0,
                block_size: 64,
                expire_at: 0,
            };
            mgr.apply_allocate(rocks, token(2, 1), 1, "/k", 150, &alloc)
                .unwrap();
            mgr.apply_commit(
                rocks,
                token(2, 1),
                token(2, 2),
                1,
                "/k",
                1,
                obj,
                150,
                777,
                0,
            )
            .unwrap();
        }

        let list = |session: &str, total: u64, blocks: Vec<BlockReportInfo>| BlockReportList {
            cluster_id: conf.cluster_id.clone(),
            worker_id: 1,
            full_report: true,
            total_len: total,
            blocks,
            worker_session_id: Some(session.into()),
        };
        let cfinal = |id: i64, len: i64| {
            BlockReportInfo::new(id, BlockReportStatus::Finalized, StorageType::Disk, len)
        };

        // Publish b1..b3 with a full report.
        fs.block_report(
            list(
                "s1",
                3,
                vec![cfinal(b1, 64), cfinal(b2, 64), cfinal(b3, 22)],
            ),
            None,
        )
        .unwrap();
        assert_eq!(
            fs.cache_service
                .get(1, "/k", true)
                .unwrap()
                .expect("published")
                .blocks
                .len(),
            3
        );

        // The OLD full outcome (a Deleted-only snapshot → Deleted ack for
        // b2). Its WM apply pauses at the INCR_OUTCOME_SEAM; inside the
        // pause a NEW same-session corrupt incremental completes (gen
        // bump + quarantine + delete-queue decision). The fresh
        // outcome's own WM apply is DEFERRED until after the outer
        // block_report returns — the seam fires with the outer
        // `start_gate` held, so re-entering apply here would self-
        // deadlock; only the ordering that matters (the new report
        // landing BEFORE the old outcome's fence recheck) is inside the
        // pause.
        let fresh_outcome: Arc<
            std::sync::Mutex<Option<crate::cache::cache_service::CacheIncrOutcome>>,
        > = Arc::new(std::sync::Mutex::new(None));
        {
            let fs2 = fs.clone();
            let fresh_outcome = fresh_outcome.clone();
            *crate::master::master_handler::INCR_OUTCOME_SEAM
                .lock()
                .unwrap() = Some(Box::new(move || {
                let out2 = fs2
                    .cache_service
                    .incr_block_report(
                        1,
                        "s1",
                        &[BlockReportInfo::new(
                            b2,
                            BlockReportStatus::Finalized,
                            StorageType::Disk,
                            0, // corrupt length → orphan
                        )],
                    )
                    .unwrap();
                assert_eq!(out2.remove_blocks, vec![b2]);
                *fresh_outcome.lock().unwrap() = Some(out2);
            }));
        }
        fs.block_report(
            list("s1", 1, vec![BlockReportInfo::with_deleted(b2, 64)]),
            None,
        )
        .unwrap();
        // Clear the seam OUTSIDE the hook: the firing `if let` holds the
        // seam's own guard for the hook's whole body, so re-locking it
        // inside would self-deadlock.
        *crate::master::master_handler::INCR_OUTCOME_SEAM
            .lock()
            .unwrap() = None;
        let out2 = fresh_outcome.lock().unwrap().take().unwrap();
        fs.apply_cache_incr_outcome(1, out2);

        // The old outcome was dropped by the generation fence: b2 is
        // STILL in the worker's delete queue (heartbeat re-delivers it).
        let cmds = fs
            .worker_manager
            .write()
            .heartbeat(
                &conf.cluster_id,
                HeartbeatStatus::Running,
                addr.clone(),
                1,
                "s1".to_string(),
                Default::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        let mut queued: Vec<i64> = Vec::new();
        for cmd in cmds {
            let WorkerCommand::DeleteBlock(c) = cmd;
            queued.extend(c.blocks);
        }
        assert_eq!(queued, vec![b2], "old ack did not clear the fresh delete");

        // The fresh quarantine survives — no ack inversion.
        let tag = fs.cache_service.cache_session_tag(1).unwrap();
        assert!(
            fs.cache_service.quarantine_contains(obj, 1, tag, 2),
            "old ack did not release the fresh quarantine"
        );
    }

    /// RC1 P0-3 (gpt56 `d2546338` item 3 / tightening `2b83f05d`): a
    /// stale old-session full page is 零创建、零清空、零计数、零触发 on
    /// the FS trigger row — before OR after the new session's first
    /// page — and the new session's own pages complete and reconcile
    /// correctly.
    #[test]
    fn test_4d3_rc1_p03_stale_page_after_restart() {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.cache_metadata_enabled = true;
        conf.master.meta_dir =
            Utils::test_sub_dir(format!("master-fs-4d3-rc1s3/meta-{}", Utils::rand_str(6)));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "master-fs-4d3-rc1s3/journal-{}",
            Utils::rand_str(6)
        ));
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        fs.master_monitor
            .journal_ctl
            .set_state(curvine_raft::raft::RoleState::Leader);

        let addr = WorkerAddress {
            worker_id: 1,
            hostname: "rc1s3-1".into(),
            ip_addr: "10.0.0.1".into(),
            rpc_port: 8200,
            web_port: 8300,
        };
        fs.begin_worker_session(&addr, "s1").unwrap();

        let obj = BlockIdCodec::CACHE_OBJECT_MIN;
        let lay = crate::master::meta::CacheBlockLayout::derive(obj, 150, 64).unwrap();
        let b1 = lay.block_id(1).unwrap();
        let b2 = lay.block_id(2).unwrap();
        let b3 = lay.block_id(3).unwrap();
        {
            let store = fs.fs_dir.read();
            let rocks = store.get_rocks_store();
            let mgr = &store.cache;
            mgr.apply_incarnation_allocate_v2(rocks, token(91, 1), 5, 1, 0)
                .unwrap();
            mgr.apply_id_reserve(rocks, token(1, 1), obj, obj + 100)
                .unwrap();
            let alloc = crate::master::meta::cache::entry::CacheEntry {
                generation: 1,
                state: crate::master::meta::cache::entry::CacheEntryState::Reserved,
                object_id: obj,
                len: 0,
                ufs_mtime: 0,
                block_size: 64,
                expire_at: 0,
            };
            mgr.apply_allocate(rocks, token(2, 1), 1, "/k", 150, &alloc)
                .unwrap();
            mgr.apply_commit(
                rocks,
                token(2, 1),
                token(2, 2),
                1,
                "/k",
                1,
                obj,
                150,
                777,
                0,
            )
            .unwrap();
        }

        let list = |session: &str, total: u64, blocks: Vec<BlockReportInfo>| BlockReportList {
            cluster_id: conf.cluster_id.clone(),
            worker_id: 1,
            full_report: true,
            total_len: total,
            blocks,
            worker_session_id: Some(session.into()),
        };
        let cfinal = |id: i64, len: i64| {
            BlockReportInfo::new(id, BlockReportStatus::Finalized, StorageType::Disk, len)
        };

        // s1 begins a full report (1 of 3), then the worker restarts.
        fs.block_report(list("s1", 3, vec![cfinal(b1, 64)]), None)
            .unwrap();
        assert!(fs.cache_service.get(1, "/k", true).unwrap().is_none());
        fs.begin_worker_session(&addr, "s2").unwrap();
        assert!(
            fs.full_block_reports.lock().is_empty(),
            "Start resets the FS trigger row"
        );

        // A COMPLETING stale s1 page arrives: zero creation, zero
        // counting, zero triggering on the FS accumulator; the cache
        // accumulator (bound to s2) skips it; nothing publishes.
        fs.block_report(list("s1", 3, vec![cfinal(b2, 64), cfinal(b3, 22)]), None)
            .unwrap();
        assert!(
            fs.full_block_reports.lock().is_empty(),
            "stale page: zero-create on the FS trigger row"
        );
        assert!(fs.cache_service.get(1, "/k", true).unwrap().is_none());

        // s2's first page begins ITS accumulation.
        fs.block_report(list("s2", 3, vec![cfinal(b1, 64)]), None)
            .unwrap();
        {
            let reports = fs.full_block_reports.lock();
            let row = reports.get(&1).expect("s2 row");
            assert_eq!(row.session, "s2");
            assert_eq!(row.reported_blocks.len(), 1);
        }

        // Another stale s1 page lands BETWEEN s2's pages: it must not
        // restart, clear, or advance s2's progress.
        fs.block_report(list("s1", 3, vec![cfinal(b1, 64)]), None)
            .unwrap();
        {
            let reports = fs.full_block_reports.lock();
            let row = reports.get(&1).expect("s2 row survives");
            assert_eq!(row.session, "s2", "stale page did not restart the row");
            assert_eq!(
                row.reported_blocks.len(),
                1,
                "stale page did not count or clear"
            );
        }

        // s2's completing page triggers the reconcile and publishes
        // exactly s2's snapshot.
        fs.block_report(list("s2", 3, vec![cfinal(b2, 64), cfinal(b3, 22)]), None)
            .unwrap();
        let hit = fs
            .cache_service
            .get(1, "/k", true)
            .unwrap()
            .expect("s2 report published");
        assert_eq!(hit.blocks.len(), 3);
        assert_eq!(hit.blocks[2].block_len, 22);
    }

    /// Shared scaffolding for the RC2 Start-retry races (gpt56
    /// `53516250` window 2): one isolated leader filesystem with a
    /// committed cache entry and worker 1 opened under wire session
    /// "s1".
    fn build_rc2_retry_fs(tag: &str) -> (MasterFilesystem, WorkerAddress, [i64; 3]) {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.cache_metadata_enabled = true;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "master-fs-4d3-rc2sr/meta-{}-{}",
            tag,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "master-fs-4d3-rc2sr/journal-{}-{}",
            tag,
            Utils::rand_str(6)
        ));
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        fs.master_monitor
            .journal_ctl
            .set_state(curvine_raft::raft::RoleState::Leader);
        let addr = WorkerAddress {
            worker_id: 1,
            hostname: format!("rc2sr-{}", tag),
            ip_addr: "10.0.0.1".into(),
            rpc_port: 8200,
            web_port: 8300,
        };
        fs.begin_worker_session(&addr, "s1").unwrap();

        let obj = BlockIdCodec::CACHE_OBJECT_MIN;
        let lay = crate::master::meta::CacheBlockLayout::derive(obj, 150, 64).unwrap();
        let blocks = [
            lay.block_id(1).unwrap(),
            lay.block_id(2).unwrap(),
            lay.block_id(3).unwrap(),
        ];
        {
            let store = fs.fs_dir.read();
            let rocks = store.get_rocks_store();
            let mgr = &store.cache;
            mgr.apply_incarnation_allocate_v2(rocks, token(91, 1), 5, 1, 0)
                .unwrap();
            mgr.apply_id_reserve(rocks, token(1, 1), obj, obj + 100)
                .unwrap();
            let alloc = crate::master::meta::cache::entry::CacheEntry {
                generation: 1,
                state: crate::master::meta::cache::entry::CacheEntryState::Reserved,
                object_id: obj,
                len: 0,
                ufs_mtime: 0,
                block_size: 64,
                expire_at: 0,
            };
            mgr.apply_allocate(rocks, token(2, 1), 1, "/k", 150, &alloc)
                .unwrap();
            mgr.apply_commit(
                rocks,
                token(2, 1),
                token(2, 2),
                1,
                "/k",
                1,
                obj,
                150,
                777,
                0,
            )
            .unwrap();
        }
        (fs, addr, blocks)
    }

    /// RC2 P0-1 (gpt56 `53516250` window 2, cache-only variant): the
    /// worker's full report self-Completes (ticket handed out under the
    /// OLD tag), then a same-WIRE-session Start RETRY lands inside the
    /// checkout window (FULL_TRIGGER_SEAM) — fresh registry tag, fresh
    /// accumulator row. The old snapshot's reconcile must fail the
    /// exact-ticket fence (zero publish, zero strip), the old ticket's
    /// release must not touch the retried row, and the retried Start's
    /// own full report must then publish normally.
    #[test]
    fn test_4d3_rc2_cache_only_start_retry_race() {
        let (fs, _addr, [b1, b2, b3]) = build_rc2_retry_fs("cacheonly");
        let old_tag = fs.cache_service.cache_session_tag(1).unwrap();
        let list = |session: &str, total: u64, blocks: Vec<BlockReportInfo>| BlockReportList {
            cluster_id: fs.worker_manager.read().conf.cluster_id.to_string(),
            worker_id: 1,
            full_report: true,
            total_len: total,
            blocks,
            worker_session_id: Some(session.into()),
        };
        let cfinal = |id: i64, len: i64| {
            BlockReportInfo::new(id, BlockReportStatus::Finalized, StorageType::Disk, len)
        };

        {
            let fs2 = fs.clone();
            *crate::master::master_handler::FULL_TRIGGER_SEAM
                .lock()
                .unwrap() = Some(Box::new(move || {
                // A same-wire-session Start RETRY inside the checkout
                // window: fresh tag, fresh accumulator row.
                fs2.begin_worker_session(&addr_retry(), "s1").unwrap();
            }));
        }
        // A helper so the closure above can borrow nothing: re-create
        // the address (identity fields are what matter to the session
        // domain; worker_id is the key).
        fn addr_retry() -> WorkerAddress {
            WorkerAddress {
                worker_id: 1,
                ..Default::default()
            }
        }

        // The cache-only worker's single completing page: cache
        // self-Complete + FS trigger in one call, with the retry landing
        // in between.
        fs.block_report(
            list(
                "s1",
                3,
                vec![cfinal(b1, 64), cfinal(b2, 64), cfinal(b3, 22)],
            ),
            None,
        )
        .unwrap();
        *crate::master::master_handler::FULL_TRIGGER_SEAM
            .lock()
            .unwrap() = None;

        // The old snapshot never acted on the retried Start's tag.
        assert!(
            fs.cache_service.get(1, "/k", true).unwrap().is_none(),
            "old ticket reconciled to a no-op"
        );
        let new_tag = fs.cache_service.cache_session_tag(1).unwrap();
        assert_ne!(new_tag, old_tag, "the retry issued a fresh tag");
        assert_eq!(
            fs.cache_service.session_spine_snapshot(1).accumulator,
            Some(("s1".to_string(), false)),
            "retried row untouched: old ticket's release was a no-op"
        );

        // The retried Start's own full report publishes normally under
        // the new tag.
        fs.block_report(
            list(
                "s1",
                3,
                vec![cfinal(b1, 64), cfinal(b2, 64), cfinal(b3, 22)],
            ),
            None,
        )
        .unwrap();
        let hit = fs
            .cache_service
            .get(1, "/k", true)
            .unwrap()
            .expect("retried Start's report published");
        assert_eq!(hit.blocks.len(), 3);
        assert_eq!(hit.blocks[2].block_len, 22);
    }

    /// RC2 P0-1 (gpt56 `53516250` window 2, mixed variant): the FS
    /// trigger has completed (reported set + bound tag in hand) when a
    /// same-wire-session Start RETRY lands BEFORE the cache snapshot
    /// take (FULL_TAKE_SEAM). The take is exact on the trigger's OLD
    /// tag, so the retried Start's fresh row is never consumable: take
    /// → None, zero reconcile, zero strip, and the retried Start's own
    /// report later publishes.
    #[test]
    fn test_4d3_rc2_mixed_start_retry_before_take_race() {
        let (fs, addr, [b1, b2, b3]) = build_rc2_retry_fs("mixed");
        let old_tag = fs.cache_service.cache_session_tag(1).unwrap();
        let list = |session: &str, total: u64, blocks: Vec<BlockReportInfo>| BlockReportList {
            cluster_id: fs.worker_manager.read().conf.cluster_id.to_string(),
            worker_id: 1,
            full_report: true,
            total_len: total,
            blocks,
            worker_session_id: Some(session.into()),
        };
        let cfinal = |id: i64, len: i64| {
            BlockReportInfo::new(id, BlockReportStatus::Finalized, StorageType::Disk, len)
        };

        {
            let fs2 = fs.clone();
            *crate::master::master_handler::FULL_TAKE_SEAM
                .lock()
                .unwrap() = Some(Box::new(move || {
                fs2.begin_worker_session(&addr, "s1").unwrap();
            }));
        }

        // MIXED report: 3 cache ids + 2 FS ids against a declared total
        // of 5 — the cache side can never self-Complete; the FS
        // accumulator's completion is the trigger, and the retry lands
        // between the trigger and the take.
        fs.block_report(
            list(
                "s1",
                5,
                vec![
                    cfinal(b1, 64),
                    cfinal(b2, 64),
                    cfinal(b3, 22),
                    cfinal(500001, 64),
                    cfinal(500002, 64),
                ],
            ),
            None,
        )
        .unwrap();
        *crate::master::master_handler::FULL_TAKE_SEAM
            .lock()
            .unwrap() = None;

        // The old trigger consumed nothing from the retried Start.
        assert!(
            fs.cache_service.get(1, "/k", true).unwrap().is_none(),
            "old trigger's take returned None — zero reconcile"
        );
        let new_tag = fs.cache_service.cache_session_tag(1).unwrap();
        assert_ne!(new_tag, old_tag, "the retry issued a fresh tag");
        assert_eq!(
            fs.cache_service.session_spine_snapshot(1).accumulator,
            Some(("s1".to_string(), false)),
            "retried row never checked out by the old trigger"
        );

        // The retried Start's own mixed report takes + reconciles
        // under the NEW tag.
        fs.block_report(
            list(
                "s1",
                5,
                vec![
                    cfinal(b1, 64),
                    cfinal(b2, 64),
                    cfinal(b3, 22),
                    cfinal(500003, 64),
                    cfinal(500004, 64),
                ],
            ),
            None,
        )
        .unwrap();
        let hit = fs
            .cache_service
            .get(1, "/k", true)
            .unwrap()
            .expect("retried Start's mixed report published");
        assert_eq!(hit.blocks.len(), 3);
        assert_eq!(hit.blocks[2].block_len, 22);
    }

    /// RC2 focused check (gpt56 `e6207e1d`): the collect→page fence. The
    /// FS accumulator authorizes the page and binds the trigger's
    /// Start-identity tag A, THEN a same-wire-session Start RETRY lands
    /// before the cache-domain page is processed (FULL_PAGE_SEAM) — fresh
    /// registry tag B, fresh accumulator row. The new row swallows this
    /// RPC's page and self-Completes with a ticket bound to tag B; the
    /// trigger must refuse that ticket (tag ≠ trigger tag → drop, zero
    /// reconcile) and must NOT release the retried row (it stays
    /// Reconciling until a new Start reopens it).
    #[test]
    fn test_4d3_rc2_collect_page_start_retry_race() {
        let (fs, addr, [b1, b2, b3]) = build_rc2_retry_fs("collectpage");
        let old_tag = fs.cache_service.cache_session_tag(1).unwrap();
        let list = |session: &str, total: u64, blocks: Vec<BlockReportInfo>| BlockReportList {
            cluster_id: fs.worker_manager.read().conf.cluster_id.to_string(),
            worker_id: 1,
            full_report: true,
            total_len: total,
            blocks,
            worker_session_id: Some(session.into()),
        };
        let cfinal = |id: i64, len: i64| {
            BlockReportInfo::new(id, BlockReportStatus::Finalized, StorageType::Disk, len)
        };

        {
            let fs2 = fs.clone();
            *crate::master::master_handler::FULL_PAGE_SEAM
                .lock()
                .unwrap() = Some(Box::new(move || {
                // Same-wire-session Start RETRY between the FS
                // authorization (trigger tag A bound) and the cache page
                // processing: fresh tag B, fresh accumulator row.
                fs2.begin_worker_session(&addr, "s1").unwrap();
            }));
        }

        // Single completing cache-only page: the FS trigger completes and
        // captures tag A, the retry installs tag B mid-window, and the
        // fresh row swallows the page and self-Completes with ticket B.
        fs.block_report(
            list(
                "s1",
                3,
                vec![cfinal(b1, 64), cfinal(b2, 64), cfinal(b3, 22)],
            ),
            None,
        )
        .unwrap();
        *crate::master::master_handler::FULL_PAGE_SEAM
            .lock()
            .unwrap() = None;

        // The old page never published onto the retried Start's tag.
        assert!(
            fs.cache_service.get(1, "/k", true).unwrap().is_none(),
            "new-tag self-Complete ticket refused by the trigger"
        );
        let new_tag = fs.cache_service.cache_session_tag(1).unwrap();
        assert_ne!(new_tag, old_tag, "the retry issued a fresh tag");
        assert_eq!(
            fs.cache_service.session_spine_snapshot(1).accumulator,
            Some(("s1".to_string(), false)),
            "retried row is non-terminal"
        );
        // The trigger did NOT release the retried row: it stays
        // Reconciling (checkout consumed by the swallowed page), so a
        // later same-session page is Skipped — only a new Start reopens.
        assert!(matches!(
            fs.cache_service
                .cache_full_report_page(1, "s1", 3, &[cfinal(b1, 64)]),
            crate::cache::cache_service::CacheFullReportOutcome::Skipped
        ));
    }

    /// P0-1 (gpt56 `25d4b51e` item 1): an incremental outcome's
    /// WorkerManager side effects are fenced by the registry tag the
    /// decisions were made under. A paused stale outcome (computed
    /// before a Start swapped the session) must be a LOUD no-op — no
    /// BlockMap delete queue entry, no quarantine release against the
    /// NEW session — while a fresh outcome applies normally.
    #[test]
    fn test_4d2_p01_outcome_fence_over_start() {
        use curvine_model::{HeartbeatStatus, WorkerCommand};

        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.cache_metadata_enabled = true;
        conf.master.meta_dir =
            Utils::test_sub_dir(format!("master-fs-4d2-fence/meta-{}", Utils::rand_str(6)));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "master-fs-4d2-fence/journal-{}",
            Utils::rand_str(6)
        ));
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        fs.master_monitor
            .journal_ctl
            .set_state(curvine_raft::raft::RoleState::Leader);
        let cluster_id = conf.cluster_id.clone();

        let addr = WorkerAddress {
            worker_id: 1,
            hostname: "fence-1".into(),
            ip_addr: "10.0.0.1".into(),
            rpc_port: 8200,
            web_port: 8300,
        };
        fs.begin_worker_session(&addr, "s1").unwrap();

        // Committed Valid cache entry: OBJ, len 150 -> 64/64/22.
        let obj = BlockIdCodec::CACHE_OBJECT_MIN;
        let lay = crate::master::meta::CacheBlockLayout::derive(obj, 150, 64).unwrap();
        let b1 = lay.block_id(1).unwrap();
        let b2 = lay.block_id(2).unwrap();
        {
            let store = fs.fs_dir.read();
            let rocks = store.get_rocks_store();
            let mgr = &store.cache;
            mgr.apply_incarnation_allocate_v2(rocks, token(91, 1), 5, 1, 0)
                .unwrap();
            mgr.apply_id_reserve(rocks, token(1, 1), obj, obj + 100)
                .unwrap();
            let alloc = crate::master::meta::cache::entry::CacheEntry {
                generation: 1,
                state: crate::master::meta::cache::entry::CacheEntryState::Reserved,
                object_id: obj,
                len: 0,
                ufs_mtime: 0,
                block_size: 64,
                expire_at: 0,
            };
            mgr.apply_allocate(rocks, token(2, 1), 1, "/k", 150, &alloc)
                .unwrap();
            mgr.apply_commit(
                rocks,
                token(2, 1),
                token(2, 2),
                1,
                "/k",
                1,
                obj,
                150,
                777,
                0,
            )
            .unwrap();
        }

        // PAUSED STALE OUTCOME: a corrupt report under s1 decides
        // orphan for b1 (captured tag = s1's tag). It is NOT applied.
        let stale = fs
            .cache_service
            .incr_block_report(
                1,
                "s1",
                &[BlockReportInfo::new(
                    b1,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    63, // corrupt length
                )],
            )
            .unwrap();
        assert_eq!(stale.remove_blocks.clone(), vec![b1]);
        assert_ne!(stale.session_tag, 0);

        // A Start swaps the session BEFORE the stale outcome reaches
        // the WorkerManager.
        fs.begin_worker_session(&addr, "s2").unwrap();

        // The NEW session builds its own quarantine (corrupt b2 under
        // s2) and its outcome applies through the fence.
        let fresh = fs
            .cache_service
            .incr_block_report(
                1,
                "s2",
                &[BlockReportInfo::new(
                    b2,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    0, // corrupt length
                )],
            )
            .unwrap();
        assert_eq!(fresh.remove_blocks.clone(), vec![b2]);
        assert_ne!(fresh.session_tag, stale.session_tag);
        let fresh_tag = fresh.session_tag;
        fs.apply_cache_incr_outcome(1, fresh);

        // Release the paused stale outcome: it must be a no-op against
        // the new session — b1 never reaches the BlockMap delete queue
        // (heartbeat re-delivers every pending delete), and the s2
        // quarantine for b2 survives untouched.
        fs.apply_cache_incr_outcome(1, stale);
        let cmds = fs
            .worker_manager
            .write()
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr.clone(),
                1,
                "s2".to_string(),
                Default::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        let mut queued: Vec<i64> = Vec::new();
        for cmd in cmds {
            let WorkerCommand::DeleteBlock(c) = cmd;
            queued.extend(c.blocks);
        }
        assert_eq!(queued, vec![b2], "stale outcome dropped, fresh applied");

        let tag2 = fs.cache_service.cache_session_tag(1).unwrap();
        assert_eq!(tag2, fresh_tag);
        assert!(
            fs.cache_service.quarantine_contains(obj, 1, fresh_tag, 2),
            "new-session quarantine survives the stale outcome release"
        );

        // Positive control — the Deleted ack releases the exact-tag
        // quarantine and a same-tag Finalized re-report publishes again.
        let acked = fs
            .cache_service
            .incr_block_report(1, "s2", &[BlockReportInfo::with_deleted(b2, 64)])
            .unwrap();
        assert_eq!(acked.deleted_acks, vec![b2]);
        fs.apply_cache_incr_outcome(1, acked);
        assert!(
            !fs.cache_service.quarantine_contains(obj, 1, fresh_tag, 2),
            "exact-tag ack releases the quarantine identity"
        );
        let republish = fs
            .cache_service
            .incr_block_report(
                1,
                "s2",
                &[BlockReportInfo::new(
                    b2,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    64,
                )],
            )
            .unwrap();
        assert!(republish.remove_blocks.is_empty());
        assert!(fs.cache_service.live_contains(1, obj, 2));
    }

    /// Round-4 P0-1 (gpt56 `36f4e28b`): the lost-worker transition runs
    /// as ONE contiguous production primitive (`lost_worker_transition`
    /// — WM expired removal + exact cache retire inside a single
    /// `start_gate` hold), so it and the fenced outcome apply are
    /// LINEARIZED. Real threads cover BOTH winners:
    ///
    /// - lost-wins: a real thread runs the production primitive to
    ///   completion BEFORE the apply takes the gate; the apply's tag
    ///   recheck sees the registry gone and drops the outcome with zero
    ///   WM/ack side effects.
    /// - outcome-wins: the apply (main thread) holds the gate and has
    ///   passed the recheck when a real thread enters the primitive;
    ///   the thread BLOCKS on the gate (proved: it has not finished
    ///   while the hook holds the gate); the side effects land; only
    ///   after the apply releases the gate does the primitive complete.
    /// - re-verification: the primitive refuses a row whose session no
    ///   longer matches the scan's snapshot, and refuses a row that is
    ///   no longer expired — nothing is removed on either refusal.
    #[test]
    fn test_4d2_r4_lost_fence_dual_branch_contiguous() {
        use curvine_model::{HeartbeatStatus, WorkerCommand};

        // Shared scaffold: fs with a Leader monitor, worker 1 session
        // "s1", and a committed Valid cache entry (OBJ, len 150 ->
        // 64/64/22) so a corrupt-length report decides orphan removals.
        let build = |tag: &str| {
            Master::init_test_metrics();
            let mut conf = ClusterConf::format();
            conf.testing = true;
            conf.journal.enable = false;
            conf.master.cache_metadata_enabled = true;
            conf.master.meta_dir = Utils::test_sub_dir(format!(
                "master-fs-4d2-lostfence/meta-{}",
                Utils::rand_str(6)
            ));
            conf.journal.journal_dir = Utils::test_sub_dir(format!(
                "master-fs-4d2-lostfence/journal-{}",
                Utils::rand_str(6)
            ));
            let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
            fs.master_monitor
                .journal_ctl
                .set_state(curvine_raft::raft::RoleState::Leader);
            let addr = WorkerAddress {
                worker_id: 1,
                hostname: format!("lostfence-{}", tag),
                ip_addr: "10.0.0.1".into(),
                rpc_port: 8200,
                web_port: 8300,
            };
            fs.begin_worker_session(&addr, "s1").unwrap();

            let obj = BlockIdCodec::CACHE_OBJECT_MIN;
            let lay = crate::master::meta::CacheBlockLayout::derive(obj, 150, 64).unwrap();
            let b1 = lay.block_id(1).unwrap();
            {
                let store = fs.fs_dir.read();
                let rocks = store.get_rocks_store();
                let mgr = &store.cache;
                mgr.apply_incarnation_allocate_v2(rocks, token(91, 1), 5, 1, 0)
                    .unwrap();
                mgr.apply_id_reserve(rocks, token(1, 1), obj, obj + 100)
                    .unwrap();
                let alloc = crate::master::meta::cache::entry::CacheEntry {
                    generation: 1,
                    state: crate::master::meta::cache::entry::CacheEntryState::Reserved,
                    object_id: obj,
                    len: 0,
                    ufs_mtime: 0,
                    block_size: 64,
                    expire_at: 0,
                };
                mgr.apply_allocate(rocks, token(2, 1), 1, "/k", 150, &alloc)
                    .unwrap();
                mgr.apply_commit(
                    rocks,
                    token(2, 1),
                    token(2, 2),
                    1,
                    "/k",
                    1,
                    obj,
                    150,
                    777,
                    0,
                )
                .unwrap();
            }
            (fs, conf, addr, b1)
        };

        // Installs the WM row for worker 1 under session `session` (the
        // Running heartbeat arm), so the production lost primitive has a
        // row to re-verify against.
        let insert_row =
            |fs: &MasterFilesystem, cluster_id: &str, addr: &WorkerAddress, session: &str| {
                fs.worker_manager
                    .write()
                    .heartbeat(
                        cluster_id,
                        HeartbeatStatus::Running,
                        addr.clone(),
                        1,
                        session.to_string(),
                        Default::default(),
                        String::new(),
                        0,
                        vec![],
                        None,
                    )
                    .unwrap();
                // The primitive re-verifies `now > last_update + lost_ms`
                // with lost_ms = 0 — guarantee the millisecond has ticked.
                std::thread::sleep(std::time::Duration::from_millis(2));
            };

        // -- Branch A (lost-wins): a REAL thread runs the production
        // primitive to completion before the apply takes the gate. --
        let (fs, conf, addr, b1) = build("a");
        let cluster_id = conf.cluster_id.clone();
        insert_row(&fs, &cluster_id, &addr, "s1");
        let stale = fs
            .cache_service
            .incr_block_report(
                1,
                "s1",
                &[BlockReportInfo::new(
                    b1,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    63, // corrupt length -> orphan removal under s1's tag
                )],
            )
            .unwrap();
        assert_eq!(stale.remove_blocks.clone(), vec![b1]);

        // The contiguous lost transition (WM removal + exact cache
        // retire under ONE gate hold) completes on a separate thread
        // before the outcome reaches the gate.
        let fs_a = fs.clone();
        let lost_a = std::thread::spawn(move || fs_a.lost_worker_transition(1, "s1", 0));
        let removed_worker = lost_a.join().unwrap();
        assert!(
            removed_worker.is_some(),
            "contiguous primitive removed the row"
        );
        assert!(
            fs.cache_service.cache_session_tag(1).is_none(),
            "lost retire removed the registry row"
        );

        // The gated apply now rechecks against a GONE tag: loud drop,
        // zero side effects — b1 never reaches the BlockMap delete queue.
        fs.apply_cache_incr_outcome(1, stale);
        let cmds = fs
            .worker_manager
            .write()
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                "s1".to_string(),
                Default::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        let mut queued: Vec<i64> = Vec::new();
        for cmd in cmds {
            let WorkerCommand::DeleteBlock(c) = cmd;
            queued.extend(c.blocks);
        }
        assert!(
            queued.is_empty(),
            "lost-wins: stale outcome must have zero WM side effects"
        );

        // -- Branch B (outcome-wins): the apply (main thread) is inside
        // the gate past the recheck when a REAL thread enters the
        // production primitive; the primitive must BLOCK on the gate
        // until the side effects land. --
        let (fs, conf, addr, b1) = build("b");
        let cluster_id = conf.cluster_id.clone();
        insert_row(&fs, &cluster_id, &addr, "s1");
        let fresh = fs
            .cache_service
            .incr_block_report(
                1,
                "s1",
                &[BlockReportInfo::new(
                    b1,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    63,
                )],
            )
            .unwrap();
        assert_eq!(fresh.remove_blocks.clone(), vec![b1]);
        let fresh_tag = fresh.session_tag;

        // The seam fires between the passed recheck and the WM side
        // effects, with start_gate and the WM write guard held. Inside
        // the seam a real thread enters `lost_worker_transition` — it
        // can only BLOCK on the gate. It must NOT have finished while
        // the apply still holds the gate.
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let lost_done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let lost_handle: std::sync::Arc<std::sync::Mutex<Option<std::thread::JoinHandle<()>>>> =
            std::sync::Arc::new(std::sync::Mutex::new(None));
        {
            let fired = fired.clone();
            let lost_done = lost_done.clone();
            let lost_handle = lost_handle.clone();
            let fs_lost = fs.clone();
            *crate::master::master_handler::INCR_OUTCOME_SEAM
                .lock()
                .unwrap() = Some(Box::new(move || {
                fired.store(true, std::sync::atomic::Ordering::SeqCst);
                assert!(
                    fs_lost.start_gate.try_lock().is_none(),
                    "outcome-wins: the apply holds the transition gate"
                );
                assert_eq!(
                    fs_lost.cache_service.cache_session_tag(1),
                    Some(fresh_tag),
                    "no session swap between recheck and side effects"
                );
                // Real thread through the production primitive — the
                // same contiguous code path the heartbeat checker runs.
                let fs_t = fs_lost.clone();
                let lost_done_t = lost_done.clone();
                *lost_handle.lock().unwrap() = Some(std::thread::spawn(move || {
                    let removed = fs_t.lost_worker_transition(1, "s1", 0);
                    assert!(
                        removed.is_some(),
                        "blocked transition completes once the gate frees"
                    );
                    lost_done_t.store(true, std::sync::atomic::Ordering::SeqCst);
                }));
                // The blocked primitive cannot finish while the apply
                // still holds the gate (generous window: it would only
                // need microseconds to run if the gate were free).
                std::thread::sleep(std::time::Duration::from_millis(200));
                assert!(
                    !lost_done.load(std::sync::atomic::Ordering::SeqCst),
                    "lost transition must BLOCK on the gate until the apply completes"
                );
            }));
        }
        fs.apply_cache_incr_outcome(1, fresh);
        *crate::master::master_handler::INCR_OUTCOME_SEAM
            .lock()
            .unwrap() = None;
        assert!(fired.load(std::sync::atomic::Ordering::SeqCst));
        let handle = lost_handle.lock().unwrap().take().unwrap();
        handle.join().unwrap();
        assert!(
            lost_done.load(std::sync::atomic::Ordering::SeqCst),
            "the primitive completed after the apply released the gate"
        );

        // The side effects landed under the still-live tag BEFORE the
        // retire could proceed: b1 reached the BlockMap delete queue
        // (heartbeat re-delivers pending)...
        assert_eq!(fs.cache_service.cache_session_tag(1), None);
        let cmds = fs
            .worker_manager
            .write()
            .heartbeat(
                &conf.cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                "s1".to_string(),
                Default::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        let mut queued: Vec<i64> = Vec::new();
        for cmd in cmds {
            let WorkerCommand::DeleteBlock(c) = cmd;
            queued.extend(c.blocks);
        }
        assert_eq!(queued, vec![b1], "outcome-wins: side effects completed");

        // -- Branch C (re-verification): the primitive refuses a
        // superseded or no-longer-expired row — nothing is removed. --
        let (fs, _conf, addr, _b1) = build("c");
        let cluster_id = _conf.cluster_id.clone();
        insert_row(&fs, &cluster_id, &addr, "s1");

        // Session mismatch (the scan's snapshot is stale — a Start +
        // Running re-registered in between): refused, row survives.
        assert!(fs.lost_worker_transition(1, "s0", 0).is_none());
        assert!(fs.worker_manager.read().get_worker(1).is_some());
        assert!(fs.cache_service.cache_session_tag(1).is_some());

        // No longer expired (lost_ms far in the future): refused, row
        // survives, registry untouched.
        assert!(fs.lost_worker_transition(1, "s1", 10_000).is_none());
        assert!(fs.worker_manager.read().get_worker(1).is_some());
        assert!(fs.cache_service.cache_session_tag(1).is_some());

        // The exact, still-expired row: removed and retired.
        assert!(fs.lost_worker_transition(1, "s1", 0).is_some());
        assert!(fs.worker_manager.read().get_worker(1).is_none());
        assert!(fs.cache_service.cache_session_tag(1).is_none());
    }
}
