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

use crate::{
    FallbackFsReader, MountCache, MountValue, UnifiedReader, UnifiedWriter, WriteCacheWriter,
};
use bytes::BytesMut;
use curvine_client_core::file::{
    CurvineFileSystem, FsClient, FsContext, FsReader, MasterHandshake,
};
use curvine_client_core::ClientMetrics;
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, err_ext};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, FsKind, ListStream, Path, Reader, RpcCode, Writer};
use curvine_job_client::{JobMasterClient, TransferClient};
use curvine_model::{
    CreateFileOpts, DeleteResult, ExtendedBlock, FileAllocOpts, FileBlocks, FileLock, FileStatus,
    FileType, FilesystemInfo, FreeResult, JobStatus, ListOptions, LoadJobCommand, LocatedBlock,
    MkdirOpts, MkdirOptsBuilder, MountInfo, MountOptions, OpenFlags, ProtoUtils, RenameFlags,
    SetAttrOpts, StorageType, TransferCommand, TransferKind, TransferState, UFS_INODE_ID,
};
use curvine_runtime::common::LocalTime;
use curvine_runtime::common::TimeSpent;
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::FastMutex;
use log::{debug, error, info, warn};
use std::borrow::Cow;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time;

const TRANSFER_SUBMIT_MAX_ATTEMPTS: usize = 3;

/// gpt56 `89ad4667` two-stage rename seam (cfg(test) ONLY — invisible in
/// production builds): a closure armed by a test fires between the src
/// and dst bound purges inside `rename_with_flags`. The closure returns
/// a future that flips REAL master state (e.g. umount+remount) so the
/// dst purge hits the REAL typed CacheIncarnationFenced terminal from
/// the server, not a fabricated error.
#[cfg(test)]
type RenamePurgeFault =
    Box<dyn FnOnce() -> std::pin::Pin<Box<dyn Future<Output = FsResult<()>> + Send>> + Send>;

#[cfg(test)]
static RENAME_PURGE_FAULT: std::sync::Mutex<Option<RenamePurgeFault>> = std::sync::Mutex::new(None);

/// gpt56 `89ad4667`: counts actual UFS rename calls made by
/// `rename_with_flags` (cfg(test) ONLY). After the dst purge FENCED, the
/// count must be 0 — purge-before-UFS means a fenced second stage never
/// mutates the backend.
#[cfg(test)]
static UFS_RENAME_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Task #6 P4-1 (gpt56 `88cda9cf` point 3): typed public results — a
/// boolean/unit return would collapse the miss/applied/superseded
/// distinction the contract needs.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheEntryStatus {
    /// No valid entry under an ACTIVE incarnation (row missing,
    /// tombstoned, expired, or location-incomplete). The caller may fall
    /// back to the UFS — unlike a FENCE, which is a loud error.
    Miss,
    Hit {
        object_id: i64,
        len: i64,
        block_size: i64,
        generation: u64,
        ufs_mtime: i64,
        expire_at: i64,
    },
}

/// Typed outcome of the composite public Invalidate. `Miss` is only ever
/// returned under an ACTIVE incarnation.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheInvalidateResult {
    Miss,
    Applied,
    AlreadyApplied,
    Superseded { current_generation: u64 },
}

/// Typed FENCE discriminator shared by the P4-1 public entries: branch on
/// the machine-readable ErrorKind, never on the message string.
fn is_incarnation_fenced(e: &FsError) -> bool {
    matches!(e.kind(), curvine_error::ErrorKind::CacheIncarnationFenced)
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
enum CacheValidity {
    Valid,
    Invalid(Option<FileStatus>),
}

#[derive(Clone)]
struct AsyncCachePending {
    paths: Arc<FastMutex<HashSet<String>>>,
    capacity: usize,
    submit_slots: Arc<Semaphore>,
}

enum AsyncCacheAdmission {
    Accepted(AsyncCachePermit),
    AlreadyPending,
    Overloaded,
}

struct AsyncCachePermit {
    path: String,
    paths: Arc<FastMutex<HashSet<String>>>,
}

impl AsyncCachePending {
    fn new(capacity: usize, submit_concurrency: usize) -> Self {
        Self {
            paths: Arc::new(FastMutex::new(HashSet::new())),
            capacity,
            submit_slots: Arc::new(Semaphore::new(submit_concurrency)),
        }
    }

    fn try_admit(&self, path: String) -> AsyncCacheAdmission {
        let mut paths = self.paths.lock();
        if paths.contains(&path) {
            return AsyncCacheAdmission::AlreadyPending;
        }
        if paths.len() >= self.capacity {
            return AsyncCacheAdmission::Overloaded;
        }
        paths.insert(path.clone());

        AsyncCacheAdmission::Accepted(AsyncCachePermit {
            path,
            paths: self.paths.clone(),
        })
    }
}

impl Drop for AsyncCachePermit {
    fn drop(&mut self) {
        self.paths.lock().remove(&self.path);
    }
}

#[derive(Clone)]
pub struct UnifiedFileSystem {
    cv: CurvineFileSystem,
    mount_cache: Arc<MountCache>,
    enable_unified: bool,
    enable_read_ufs: bool,
    audit_logging_enabled: bool,
    async_cache_pending: AsyncCachePending,
    metrics: &'static ClientMetrics,
}

impl UnifiedFileSystem {
    pub fn with_rt(conf: impl Into<ClusterConf>, rt: Arc<Runtime>) -> FsResult<Self> {
        let conf = conf.into();
        let update_interval_ms = conf.client.mount_update_ttl_ms;
        let enable_unified = conf.client.enable_unified_fs;
        let enable_read_ufs = conf.client.enable_rust_read_ufs;
        let audit_logging_enabled = conf.client.audit_logging_enabled;
        let async_cache_pending_capacity = conf.transfer.client_pending_queue_size();
        let async_cache_submit_concurrency = conf.transfer.client_submit_concurrency();

        let cv = CurvineFileSystem::with_rt(conf, rt.clone())?;
        let fs = UnifiedFileSystem {
            cv,
            mount_cache: Arc::new(MountCache::new(update_interval_ms)),
            enable_unified,
            enable_read_ufs,
            audit_logging_enabled,
            async_cache_pending: AsyncCachePending::new(
                async_cache_pending_capacity,
                async_cache_submit_concurrency,
            ),
            metrics: FsContext::get_metrics(),
        };

        Ok(fs)
    }

    fn audit<T>(
        &self,
        cmd: &str,
        src: &str,
        dst: &str,
        res: FsResult<T>,
        used_us: u64,
    ) -> FsResult<T> {
        if self.audit_logging_enabled {
            let err_suffix: Cow<'_, str> = match &res {
                Err(e) => Cow::Owned(format!(" err={:?}", e.kind())),
                Ok(_) => Cow::Borrowed(""),
            };
            info!(
                target: "audit",
                "cmd={} ok={} src={} dst={} usedUs={}{}",
                cmd,
                res.is_ok(),
                src,
                dst,
                used_us,
                err_suffix,
            );
        }

        res
    }

    fn op_metric(&self, cmd: &str, used_us: u64) {
        self.metrics
            .metadata_operation_duration
            .with_label_values(&[cmd])
            .observe(used_us as f64);
    }

    async fn track<F, T>(&self, cmd: &str, src: &str, dst: &str, fut: F) -> FsResult<T>
    where
        F: Future<Output = FsResult<T>>,
    {
        let spent = TimeSpent::new();
        let res = fut.await;
        let used_us = spent.used_us();

        self.op_metric(cmd, used_us);
        self.audit(cmd, src, dst, res, used_us)
    }

    pub fn conf(&self) -> &ClusterConf {
        self.cv.conf()
    }

    pub fn cv(&self) -> &CurvineFileSystem {
        &self.cv
    }

    pub fn fs_context(&self) -> &Arc<FsContext> {
        self.cv.fs_context_ref()
    }

    pub fn fs_client(&self) -> Arc<FsClient> {
        self.cv.fs_client()
    }

    // Check if the path is a mount point, if so, return the mount point information.
    pub async fn get_mount(
        &self,
        path: &Path,
        rpc_code: RpcCode,
    ) -> FsResult<Option<(Path, Arc<MountValue>)>> {
        if !path.is_cv() {
            return err_box!("path is not curvine path");
        }

        if !self.enable_unified {
            return Ok(None);
        }

        let state = self.mount_cache.get_mount(self, path).await?;
        if let Some(mnt) = state {
            if mnt.info.is_read_only_cache_mode() && Self::is_mount_write_rpc(rpc_code) {
                return err_ext!(FsError::unsupported(format!(
                    "{} on read_only cache_mode mount {}",
                    rpc_code, path
                )));
            }

            let ufs_path = mnt.get_ufs_path(path)?;
            Ok(Some((ufs_path, mnt)))
        } else {
            Ok(None)
        }
    }

    fn is_mount_write_rpc(rpc_code: RpcCode) -> bool {
        matches!(
            rpc_code,
            RpcCode::Mkdir
                | RpcCode::Delete
                | RpcCode::CreateFile
                | RpcCode::AppendFile
                | RpcCode::Rename
                | RpcCode::SetAttr
                | RpcCode::Symlink
                | RpcCode::Link
                | RpcCode::ResizeFile
                | RpcCode::SetLock
        )
    }

    pub async fn get_mount_checked(
        &self,
        path: &Path,
        rpc_code: RpcCode,
    ) -> FsResult<Option<(Path, Arc<MountValue>)>> {
        match self.get_mount(path, rpc_code).await? {
            Some(v) if v.1.info.is_cache_mode() => Ok(Some(v)),
            _ => Ok(None),
        }
    }

    pub async fn get_filesystem_info(&self) -> FsResult<FilesystemInfo> {
        let fut = async { self.cv.get_filesystem_info().await };
        self.track("GetFilesystemInfo", "", "", fut).await
    }

    /// Client-master version handshake: report this client's `component_info`
    /// and cache the master's advertised version / protocol / capabilities.
    pub async fn handshake(&self) -> FsResult<MasterHandshake> {
        let fut = async { self.cv.handshake().await };
        self.track("GetFilesystemInfo", "", "", fut).await
    }

    /// Cached master handshake (version / protocol / capabilities). Before the
    /// first handshake and against legacy masters this reports a legacy peer,
    /// which is never rejected.
    pub fn master_handshake(&self) -> MasterHandshake {
        self.cv.master_handshake()
    }

    pub async fn get_filesystem_info_bytes(&self) -> FsResult<BytesMut> {
        let fut = async { self.cv.get_filesystem_info_bytes().await };
        self.track("GetFilesystemInfo", "", "", fut).await
    }

    pub async fn mount(&self, ufs_path: &Path, cv_path: &Path, opts: MountOptions) -> FsResult<()> {
        let fut = async {
            self.cv.mount(ufs_path, cv_path, opts).await?;
            self.mount_cache.check_update(self, true).await?;
            Ok(())
        };
        self.track("Mount", cv_path.path(), ufs_path.full_path(), fut)
            .await
    }

    pub async fn umount(&self, cv_path: &Path) -> FsResult<()> {
        let fut = async {
            self.cv.umount(cv_path).await?;
            self.mount_cache.remove(cv_path);
            Ok(())
        };
        self.track("Umount", cv_path.path(), "", fut).await
    }

    pub async fn toggle_path(&self, path: &Path, check_cache: bool) -> FsResult<Option<Path>> {
        if check_cache {
            let state = self.mount_cache.get_mount(self, path).await?;
            if let Some(mnt) = state {
                let toggle_path = mnt.toggle_path(path)?;
                Ok(Some(toggle_path))
            } else {
                Ok(None)
            }
        } else {
            match self.get_mount_info(path).await? {
                Some(mnt) => {
                    let toggle_path = mnt.toggle_path(path)?;
                    Ok(Some(toggle_path))
                }
                None => Ok(None),
            }
        }
    }

    pub async fn get_mount_info(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        let fut = async { self.cv.get_mount_info(path).await };
        self.track("GetMountInfo", path.path(), "", fut).await
    }

    pub async fn get_mount_info_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        let fut = async { self.cv.get_mount_info_bytes(path).await };
        self.track("GetMountInfo", path.path(), "", fut).await
    }

    pub async fn get_mount_table(&self) -> FsResult<Vec<MountInfo>> {
        let fut = async { self.cv.get_mount_table().await };
        self.track("GetMountTable", "", "", fut).await
    }

    pub fn clone_runtime(&self) -> Arc<Runtime> {
        self.cv.clone_runtime()
    }

    pub async fn free(&self, path: &Path, recursive: bool) -> FsResult<FreeResult> {
        // Free bridge (task #6, gpt56 `961e17b5` P0 + `6e4a5599`): the
        // public Free NEVER branches on the local mount snapshot — the
        // client always sends Free and the MASTER is the sole
        // authoritative route: cache-mode paths go to the typed
        // Key/Prefix/Mount free bound to the current incarnation,
        // everything else falls to the legacy inode free, all decided
        // server-side. The Unified/SDK layer NEVER deletes CV inodes for
        // a free (the old list+`cv.delete` helper is removed).
        // `FsClient::free` drives the bounded continuation walk to
        // done=true.
        let fut = self.cv.free(path, recursive);
        self.track("Free", path.path(), "", fut).await
    }

    /// Resolve a CV path to its cache-mode target: the mount's CURRENT
    /// incarnation (from the response-only `MountSnapshot`) plus the
    /// derived cache key. Loud on every non-cache-mode shape — these APIs
    /// are cache-domain only and never fall back to CV inodes.
    async fn resolve_cache_target(&self, path: &Path) -> FsResult<(Arc<MountValue>, String, u64)> {
        // Unified-disabled contract (gpt56 `2e74f4ac` #2): the public
        // cache entries are part of the Unified surface and respect the
        // same `enable_unified` gate as `get_mount`; a disabled client
        // must fail loud instead of quietly serving cache queries from a
        // bypassing path. The raw `fs.cv()` bindings stay independent.
        if !self.enable_unified {
            return err_box!(
                "unified filesystem is disabled: public cache_status/invalidate_cache are unavailable (raw fs.cv() bindings are unaffected)"
            );
        }
        if !path.is_cv() {
            return err_box!("cache status path is not curvine path: {}", path);
        }
        let Some(mnt) = self.mount_cache.get_mount(self, path).await? else {
            return err_box!("no mount covers cache path {}", path);
        };
        if !mnt.info.is_cache_mode() {
            return err_box!(
                "cache APIs target cache-mode mounts only: {} is {:?}",
                path,
                mnt.info.write_type
            );
        }
        // Defensive: the snapshot decode already fails closed on a
        // CacheMode row without a nonzero incarnation.
        let Some(incarnation) = mnt.cache_incarnation.filter(|i| *i != 0) else {
            return err_box!(
                "cache-mode mount {} ({}) snapshot has no cache incarnation",
                mnt.info.cv_path,
                mnt.info.mount_id
            );
        };
        let ufs_path = mnt.get_ufs_path(path)?;
        let key = mnt.info.get_cache_key(&ufs_path)?;
        Ok((mnt, key, incarnation))
    }

    /// P4-3 bound scoped purge (gpt56 `2a089d5a`): the ONE purge
    /// primitive for cache-mode mounts. Routes through the Free bridge
    /// with the caller-observed mount/incarnation binding — the master
    /// validates the resolved route EXACTLY on the first page of a
    /// fresh walk, so a remount between observation and purge is the
    /// typed FENCED terminal and this purge can never delete a
    /// different incarnation than the caller observed. The derived
    /// scope (Key for a non-recursive file, Prefix for recursive)
    /// clears every live row: Valid, Reserved, expired, and
    /// locations-incomplete.
    ///
    /// Callers run this BEFORE the UFS mutation (ordering ruling #3):
    /// a fenced or failed purge leaves UFS untouched and the whole
    /// operation is safe to retry. The error is always loud.
    async fn bound_purge(&self, mount: &MountValue, path: &Path, recursive: bool) -> FsResult<()> {
        let Some(incarnation) = mount.cache_incarnation.filter(|i| *i != 0) else {
            return err_box!(
                "cache-mode mount {} ({}) snapshot has no cache incarnation; refusing unbound purge",
                mount.info.cv_path,
                mount.info.mount_id
            );
        };
        self.cv
            .free_with_binding(path, recursive, mount.info.mount_id, incarnation)
            .await?;
        Ok(())
    }

    /// ONE metadata Get observation with the fenced one-refresh policy
    /// (shared by `cache_status` and `invalidate_cache`).
    ///
    /// On a typed CacheIncarnationFenced the mount table is force-refreshed
    /// ONCE and the same path re-resolved; if the mount vanished, turned
    /// non-CacheMode, or the refreshed incarnation still fences, the error
    /// is loud — a dead namespace is never folded into a miss.
    ///
    /// The returned `(response, key, incarnation, mount)` always comes
    /// from the SAME successful call (gpt56 `694593c1` P0: a re-resolved
    /// Get must never be paired with the stale outer resolution; gpt56
    /// `e53671d1` P0: the refreshed MOUNT travels with the observation —
    /// every consumer of a hit or miss (D5 verify, UFS fallback,
    /// auto_cache, metrics) uses this mount and the ufs path derived
    /// from it, never the caller's pre-refresh resolution).
    async fn cache_get_observed(
        &self,
        path: &Path,
        need_locations: bool,
    ) -> FsResult<(
        curvine_proto::CacheGetResponse,
        String,
        u64,
        Arc<MountValue>,
    )> {
        let (mount, key, incarnation) = self.resolve_cache_target(path).await?;
        match self.cv.cache_get(incarnation, &key, need_locations).await {
            Ok(rep) => Ok((rep, key, incarnation, mount)),
            Err(e) => {
                if !is_incarnation_fenced(&e) {
                    return Err(e);
                }
                self.mount_cache.check_update(self, true).await?;
                let (mount, key, incarnation) = self.resolve_cache_target(path).await?;
                let rep = self.cv.cache_get(incarnation, &key, need_locations).await?;
                Ok((rep, key, incarnation, mount))
            }
        }
    }

    /// P4-2 D5 validity matrix (gpt56 `c1d51e75`): expiry is ALWAYS
    /// checked client-side (the server filtered at Get time; this is the
    /// defense-in-depth re-check); when the mount demands UFS
    /// verification, len and ufs_mtime are compared against ONE
    /// authoritative UFS stat observation fetched here — the comparison
    /// never mixes two different stat calls. An entry that fails any D5
    /// check is a whole-object miss, never a partial read.
    async fn d5_verify(
        &self,
        len: i64,
        ufs_mtime: i64,
        expire_at: i64,
        ufs_path: &Path,
        mount: &MountValue,
    ) -> FsResult<bool> {
        if expire_at != 0 && LocalTime::mills() as i64 >= expire_at {
            return Ok(false);
        }
        if mount.info.read_verify_ufs {
            let ufs_status = mount.ufs()?.get_status(ufs_path).await?;
            if len != ufs_status.len || ufs_mtime != ufs_status.mtime {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// P4-2 strict cache-block decode (gpt56 `c1d51e75` #2): the location
    /// set must be a COMPLETE, well-formed object layout — block ids
    /// unique and in stable (+1 monotone) order, per-block lengths
    /// exactly matching the geometry implied by `(len, block_size)`
    /// (non-last blocks == block_size, last block the remainder), the
    /// checked-overflow accumulation exactly equal to `len`, and every
    /// block carrying at least one worker. `len == 0` must carry an
    /// empty location set. ANY anomaly is a whole-object miss (None) —
    /// cache/UFS stitched reads are forbidden. The codec bits are NOT
    /// re-derived client-side; only ordering properties are checked.
    fn cache_blocks_from_get(
        rep: &curvine_proto::CacheGetResponse,
        len: i64,
        block_size: i64,
    ) -> Option<Vec<LocatedBlock>> {
        if block_size <= 0 || len < 0 {
            return None;
        }
        if len == 0 {
            return if rep.blocks.is_empty() {
                Some(Vec::new())
            } else {
                None
            };
        }
        if rep.blocks.is_empty() {
            return None;
        }

        let n = rep.blocks.len() as i64;
        let expected_count = len / block_size + i64::from(len % block_size != 0);
        if n != expected_count {
            return None;
        }
        let last_len = len.checked_sub((n - 1).checked_mul(block_size)?)?;
        if last_len <= 0 || last_len > block_size {
            return None;
        }

        let mut total: i64 = 0;
        let mut prev_id: Option<i64> = None;
        let mut block_locs = Vec::with_capacity(rep.blocks.len());
        for (index, block) in rep.blocks.iter().enumerate() {
            let expected_len = if index as i64 == n - 1 {
                last_len
            } else {
                block_size
            };
            if block.block_len != expected_len {
                return None;
            }
            total = total.checked_add(block.block_len)?;
            match prev_id {
                // checked: a hostile/corrupt id at i64::MAX must be a
                // whole-object miss, never an overflow panic (gpt56
                // `e53671d1`).
                Some(prev) if block.block_id != prev.checked_add(1)? => return None,
                _ => {}
            }
            prev_id = Some(block.block_id);
            if block.workers.is_empty() {
                return None;
            }
            let locs: Vec<_> = block
                .workers
                .iter()
                .map(ProtoUtils::worker_address_from_pb)
                .collect();
            block_locs.push(LocatedBlock {
                block: ExtendedBlock::new(
                    block.block_id,
                    block.block_len,
                    StorageType::Disk,
                    FileType::File,
                ),
                locs,
                has_spdk: false,
            });
        }
        if total != len {
            return None;
        }
        Some(block_locs)
    }

    /// Synthesizes the inode-free `FileStatus` for a cache-mode interior
    /// file from ONE strict hit observation (P4-2: strict-interior reads
    /// issue zero inode RPCs — no field is ever read from the inode
    /// tree). The status must be a usable UFS-like namespace stat
    /// (gpt56 `fd02f578` #2): the internal cache object id NEVER enters
    /// the namespace id (UFS_INODE_ID, the existing UFS-backed
    /// convention), and mode follows the UFS-layer regular-file default
    /// (`0o777`, as every synthesized UFS status already uses) so a
    /// FUSE permission check on a hit cannot see 000/EACCES.
    fn cache_entry_file_status(
        path: &Path,
        len: i64,
        block_size: i64,
        ufs_mtime: i64,
    ) -> FileStatus {
        let mut status = FileStatus::with_name(UFS_INODE_ID, path.name().to_string(), false);
        status.path = path.path().to_string();
        status.len = len;
        status.block_size = block_size;
        status.mtime = ufs_mtime;
        status.is_complete = true;
        status.mode = 0o777;
        status
    }

    /// P4-2 D5 read route for a cache-mode STRICT INTERIOR path: ONE
    /// locations-bearing CacheGet, strict metadata decode, D5 validity,
    /// and full geometry validation. A fully valid hit yields a
    /// CACHE-ONLY reader (gpt56 `c1d51e75` P0: the hit reader is never
    /// wrapped in `FallbackFsReader` — a mid-read worker failure is a
    /// loud error, not a per-read UFS stitch). Anything else is a miss
    /// (None). The SAME-OBSERVATION `(mount, ufs_path)` travels with the
    /// result (gpt56 `e53671d1` P0): the caller's D5/fallback/auto_cache
    /// path uses the mount the Get actually resolved against — if a
    /// fence refresh re-resolved onto a remounted target, that NEW mount
    /// is what the miss falls back to, never the stale outer one.
    async fn get_cache_d5_reader(
        &self,
        path: &Path,
    ) -> FsResult<(Option<FsReader>, Arc<MountValue>, Path)> {
        let (rep, _, _, mount) = self.cache_get_observed(path, true).await?;
        let ufs_path = mount.get_ufs_path(path)?;
        let (len, block_size, ufs_mtime, expire_at) = match Self::status_from_get(&rep)? {
            CacheEntryStatus::Hit {
                len,
                block_size,
                ufs_mtime,
                expire_at,
                ..
            } => (len, block_size, ufs_mtime, expire_at),
            CacheEntryStatus::Miss => return Ok((None, mount, ufs_path)),
        };
        if !self
            .d5_verify(len, ufs_mtime, expire_at, &ufs_path, &mount)
            .await?
        {
            return Ok((None, mount, ufs_path));
        }
        let Some(block_locs) = Self::cache_blocks_from_get(&rep, len, block_size) else {
            return Ok((None, mount, ufs_path));
        };
        let status = Self::cache_entry_file_status(path, len, block_size, ufs_mtime);
        let reader = FsReader::new(
            path.clone(),
            self.cv.fs_context(),
            FileBlocks::new(status, block_locs),
        )?;
        Ok((Some(reader), mount, ufs_path))
    }

    /// P4-2 D5 status route for a cache-mode STRICT INTERIOR path: ONE
    /// metadata-only CacheGet with the same strict decode and D5 matrix.
    /// A valid hit synthesizes the entry status; anything else is a miss
    /// (None). Returns the SAME-OBSERVATION `(mount, ufs_path)` for the
    /// caller's UFS status fallback (gpt56 `e53671d1` P0).
    async fn cache_d5_status(
        &self,
        path: &Path,
    ) -> FsResult<(Option<FileStatus>, Arc<MountValue>, Path)> {
        let (rep, _, _, mount) = self.cache_get_observed(path, false).await?;
        let ufs_path = mount.get_ufs_path(path)?;
        let (len, block_size, ufs_mtime, expire_at) = match Self::status_from_get(&rep)? {
            CacheEntryStatus::Hit {
                len,
                block_size,
                ufs_mtime,
                expire_at,
                ..
            } => (len, block_size, ufs_mtime, expire_at),
            CacheEntryStatus::Miss => return Ok((None, mount, ufs_path)),
        };
        if !self
            .d5_verify(len, ufs_mtime, expire_at, &ufs_path, &mount)
            .await?
        {
            return Ok((None, mount, ufs_path));
        }
        Ok((
            Some(Self::cache_entry_file_status(
                path, len, block_size, ufs_mtime,
            )),
            mount,
            ufs_path,
        ))
    }

    /// Task #6 P4-1 (gpt56 `88cda9cf`): public cache-entry status — a
    /// metadata-only CacheGet (`need_locations=false`).
    pub async fn cache_status(&self, path: &Path) -> FsResult<CacheEntryStatus> {
        let fut = async {
            let (rep, _, _, _) = self.cache_get_observed(path, false).await?;
            Self::status_from_get(&rep)
        };
        self.track("CacheGet", path.path(), "", fut).await
    }

    /// STRICT Get wire decode (gpt56 `2e74f4ac` #3), shared by the public
    /// status and the composite invalidate: a response that claims
    /// `hit=true` but omits ANY observation field is wire corruption —
    /// fabricating a Hit (or letting Invalidate mutate with a forged 0
    /// identity) would mask it, so the decode fails loud. Only a well
    /// formed `hit=false` (or absent hit) is a `Miss`.
    fn status_from_get(rep: &curvine_proto::CacheGetResponse) -> FsResult<CacheEntryStatus> {
        if !rep.hit.unwrap_or(false) {
            return Ok(CacheEntryStatus::Miss);
        }

        let (
            Some(object_id),
            Some(len),
            Some(block_size),
            Some(generation),
            Some(ufs_mtime),
            Some(expire_at),
        ) = (
            rep.object_id,
            rep.file_len,
            rep.block_size,
            rep.generation,
            rep.ufs_mtime,
            rep.expire_at,
        )
        else {
            return err_box!(
                "cache get response says hit but is missing observation fields: object_id={:?} file_len={:?} block_size={:?} generation={:?} ufs_mtime={:?} expire_at={:?}",
                rep.object_id,
                rep.file_len,
                rep.block_size,
                rep.generation,
                rep.ufs_mtime,
                rep.expire_at
            );
        };
        Ok(CacheEntryStatus::Hit {
            object_id,
            len,
            block_size,
            generation,
            ufs_mtime,
            expire_at,
        })
    }

    /// Task #6 P4-1 (gpt56 `88cda9cf` Q2): composite public Invalidate.
    ///
    /// Observes the entry with one metadata Get (same one-refresh FENCED
    /// policy as `cache_status`), then fences exactly the OBSERVED
    /// `(incarnation, key, generation, object_id)` — the identity CAS
    /// makes a forged or raced id a loud divergence instead of a silent
    /// cross-object fence. The mutation phase is TERMINAL on FENCED: it
    /// must never re-resolve onto a newer incarnation (only a transport
    /// response-loss may replay the identical request). A miss under an
    /// ACTIVE incarnation is simply `Miss` — nothing to invalidate.
    pub async fn invalidate_cache(&self, path: &Path) -> FsResult<CacheInvalidateResult> {
        let fut = async {
            // The observation and its resolution travel together: the
            // mutation below consumes EXACTLY this triple, whether the
            // Get succeeded first try or after the one forced refresh
            // (gpt56 `694593c1` P0 — never mix a refreshed Get with the
            // stale outer resolution).
            let (rep, key, incarnation, _) = self.cache_get_observed(path, false).await?;
            // Same STRICT decoder as the public status (gpt56 `2e74f4ac`
            // #3): the mutation identity (generation, object_id) is only
            // ever taken from a well-formed hit observation — never
            // defaulted from a malformed response.
            let (object_id, generation) = match Self::status_from_get(&rep)? {
                CacheEntryStatus::Hit {
                    object_id,
                    generation,
                    ..
                } => (object_id, generation),
                CacheEntryStatus::Miss => return Ok(CacheInvalidateResult::Miss),
            };
            let rep = self
                .cv
                .cache_invalidate(incarnation, &key, generation, object_id)
                .await?;
            use curvine_proto::CacheOpStatusProto as Status;
            let status = match rep.status {
                Some(s) if s == Status::Applied as i32 => Status::Applied,
                Some(s) if s == Status::AlreadyApplied as i32 => Status::AlreadyApplied,
                Some(s) if s == Status::Superseded as i32 => Status::Superseded,
                Some(s) if s == Status::ReplanNeeded as i32 => Status::ReplanNeeded,
                // Fail closed on a missing/unknown discriminator: inventing
                // an Applied would mask a wire-level corruption.
                other => return err_box!("cache invalidate returned unknown status {:?}", other),
            };
            Ok(match status {
                Status::Applied => CacheInvalidateResult::Applied,
                Status::AlreadyApplied => CacheInvalidateResult::AlreadyApplied,
                Status::Superseded => {
                    // A legal server current_generation of 0 is sent
                    // explicitly as Some(0); a MISSING field is wire
                    // corruption and must be loud (gpt56 `2e74f4ac` #3),
                    // never reported as Superseded { current_generation: 0 }.
                    let Some(current_generation) = rep.current_generation else {
                        return err_box!(
                            "cache invalidate Superseded response is missing current_generation"
                        );
                    };
                    CacheInvalidateResult::Superseded { current_generation }
                }
                // Commit-only re-planable state is impossible for an
                // invalidate; loud, never silently treated as applied.
                Status::ReplanNeeded => {
                    return err_box!("cache invalidate returned commit-only status ReplanNeeded")
                }
            })
        };
        self.track("CacheInvalidate", path.path(), "", fut).await
    }

    pub async fn symlink(&self, target: &str, link: &Path, force: bool) -> FsResult<()> {
        let fut = async {
            match self.get_mount_checked(link, RpcCode::Symlink).await? {
                None => self.cv.symlink(target, link, force).await,
                Some(_) => err_ext!(FsError::unsupported("symlink")),
            }
        };
        self.track("Symlink", target, link.path(), fut).await
    }

    pub async fn symlink_with_owner_group(
        &self,
        target: &str,
        link: &Path,
        force: bool,
        owner: Option<String>,
        group: Option<String>,
    ) -> FsResult<()> {
        let fut = async {
            match self.get_mount_checked(link, RpcCode::Symlink).await? {
                None => {
                    self.cv
                        .symlink_with_owner_group(target, link, force, owner, group)
                        .await
                }
                Some(_) => err_ext!(FsError::unsupported("symlink")),
            }
        };
        self.track("Symlink", target, link.path(), fut).await
    }

    pub async fn create_special_node(
        &self,
        path: &Path,
        opts: CreateFileOpts,
    ) -> FsResult<FileStatus> {
        let fut = async {
            match self.get_mount_checked(path, RpcCode::CreateFile).await? {
                None => self.cv.create_special_node(path, opts).await,
                Some(_) => err_ext!(FsError::unsupported("mknod")),
            }
        };
        self.track("CreateSpecialNode", "", path.path(), fut).await
    }

    pub async fn link(&self, src_path: &Path, dst_path: &Path) -> FsResult<()> {
        let fut = async {
            let _ = self.get_mount_checked(dst_path, RpcCode::Link).await?;
            match self.get_mount_checked(src_path, RpcCode::Link).await? {
                None => self.cv.link(src_path, dst_path).await,
                Some(_) => err_ext!(FsError::unsupported("link")),
            }
        };
        self.track("Link", src_path.path(), dst_path.path(), fut)
            .await
    }

    pub async fn resize(&self, path: &Path, opts: FileAllocOpts) -> FsResult<()> {
        let fut = async {
            match self.get_mount_checked(path, RpcCode::ResizeFile).await? {
                None => self.cv.resize(path, opts).await,
                Some(_) => err_ext!(FsError::unsupported("resize")),
            }
        };
        self.track("Resize", path.path(), "", fut).await
    }

    async fn check_cache_validity(
        &self,
        cv_status: &FileStatus,
        ufs_path: &Path,
        mount: &MountValue,
    ) -> FsResult<CacheValidity> {
        if mount.info.read_verify_ufs {
            let ufs_status = mount.ufs()?.get_status(ufs_path).await?;
            if cv_status.cv_valid(Some(&ufs_status)) {
                Ok(CacheValidity::Valid)
            } else {
                Ok(CacheValidity::Invalid(Some(ufs_status)))
            }
        } else if cv_status.cv_valid(None) {
            Ok(CacheValidity::Valid)
        } else {
            Ok(CacheValidity::Invalid(None))
        }
    }

    async fn get_cv_reader(
        &self,
        cv_path: &Path,
        ufs_path: &Path,
        mount: &MountValue,
    ) -> FsResult<Option<FallbackFsReader>> {
        let mut blocks = match self.cv.get_block_locations(cv_path).await {
            Ok(blocks) => blocks,
            Err(e) => {
                if !matches!(e, FsError::FileNotFound(_) | FsError::Expired(_)) {
                    error!("failed to get block locations for {}: {}", cv_path, e)
                }
                return Ok(None);
            }
        };

        if mount.info.is_fs_mode() {
            if blocks.cv_exists() {
                let cv_reader = FsReader::new(cv_path.clone(), self.cv.fs_context(), blocks)?;
                Ok(Some(FallbackFsReader::new(
                    cv_reader,
                    ufs_path.clone(),
                    mount.ufs()?,
                    mount.info.is_fs_mode(),
                )))
            } else if blocks.ufs_exists() {
                Ok(None)
            } else {
                err_box!("path {} data lost", cv_path)
            }
        } else {
            match self
                .check_cache_validity(&blocks.status, ufs_path, mount)
                .await?
            {
                CacheValidity::Valid => {
                    blocks.status.apply_ufs_fields();
                    let cv_reader = FsReader::new(cv_path.clone(), self.cv.fs_context(), blocks)?;
                    Ok(Some(FallbackFsReader::new(
                        cv_reader,
                        ufs_path.clone(),
                        mount.ufs()?,
                        mount.info.is_fs_mode(),
                    )))
                }
                CacheValidity::Invalid(_) => Ok(None),
            }
        }
    }

    pub fn async_cache(&self, source_path: &Path) -> FsResult<()> {
        let source_path = source_path.clone_uri();
        let pending_permit = match self.async_cache_pending.try_admit(source_path.clone()) {
            AsyncCacheAdmission::Accepted(pending) => pending,
            AsyncCacheAdmission::AlreadyPending => {
                self.metrics
                    .async_cache_admission_skipped
                    .with_label_values(&["already_pending"])
                    .inc();
                debug!("async cache request already pending for {}", source_path);
                return Ok(());
            }
            AsyncCacheAdmission::Overloaded => {
                self.metrics
                    .async_cache_admission_skipped
                    .with_label_values(&["overloaded"])
                    .inc();
                debug!(
                    "skip async cache request for {} because the client pending queue is full, capacity={}",
                    source_path, self.async_cache_pending.capacity
                );
                return Ok(());
            }
        };
        let fs = self.clone();
        let log = self.audit_logging_enabled;
        let metrics = self.metrics;

        self.fs_context().rt().spawn(async move {
            let _pending_permit = pending_permit;
            let _submit_permit = match fs
                .async_cache_pending
                .submit_slots
                .clone()
                .acquire_owned()
                .await
            {
                Ok(permit) => permit,
                Err(err) => {
                    warn!("async cache submit limiter closed unexpectedly: {}", err);
                    return;
                }
            };
            let time = TimeSpent::new();
            let res = fs.submit_async_cache(&source_path).await;

            let used_us = time.used_us();
            let metric_name = res
                .as_ref()
                .map(|(cmd, _, _)| cmd.as_str())
                .unwrap_or("SubmitCacheJob");
            metrics
                .metadata_operation_duration
                .with_label_values(&[metric_name])
                .observe(used_us as f64);

            match res {
                Err(e) => warn!("submit async cache error for {}: {}", source_path, e),
                Ok((cmd, job_id, target_path)) => {
                    if log {
                        info!(
                            target: "audit",
                            "cmd={} ok={} src={} dst={} usedUs={}",
                            cmd,
                            true,
                            source_path,
                            target_path,
                           used_us
                        );
                    }
                    debug!("submitted async cache job {} for {}", job_id, source_path);
                }
            }
        });

        Ok(())
    }

    async fn submit_async_cache(&self, source_path: &str) -> FsResult<(String, String, String)> {
        if self.cv.conf().transfer.enabled {
            let client = TransferClient::with_context(self.fs_context())?;
            let command = self
                .cache_transfer_command(&Path::from_str(source_path)?)
                .await?;
            let target_path = command.target_path.clone();
            let rep = submit_transfer_with_backoff(&client, command).await?;
            return Ok(("SubmitTransfer".to_string(), rep.job_id, target_path));
        }
        let client = JobMasterClient::new(self.fs_client());
        let result = client
            .submit_load_job(LoadJobCommand::builder(source_path).build())
            .await?;
        Ok(("SubmitJob".to_string(), result.job_id, result.target_path))
    }

    async fn cache_transfer_command(&self, requested_path: &Path) -> FsResult<TransferCommand> {
        let mount = self
            .mount_cache
            .get_mount(self, requested_path)
            .await?
            .ok_or_else(|| FsError::common(format!("{} is not mounted", requested_path)))?;
        let (source, target) = if requested_path.is_cv() {
            (mount.get_ufs_path(requested_path)?, requested_path.clone())
        } else {
            (requested_path.clone(), mount.get_cv_path(requested_path)?)
        };
        mount.ufs()?.get_status(&source).await?;

        Ok(TransferCommand {
            kind: TransferKind::Load,
            source_path: source.clone_uri(),
            target_path: target.clone_uri(),
            client_request_id: TransferCommand::default_client_request_id(
                TransferKind::Load,
                source.clone_uri(),
                target.clone_uri(),
            ),
            submitter: "curvine-client".to_string(),
            tenant: String::new(),
            options: Default::default(),
        })
    }

    pub async fn wait_job_complete(&self, path: &Path, fail_if_not_found: bool) -> FsResult<()> {
        if self.cv.conf().transfer.enabled {
            let command = self.cache_transfer_command(path).await?;
            let client = TransferClient::with_context(self.fs_context())?;
            let job = submit_transfer_with_backoff(&client, command).await?;
            return wait_transfer_complete(
                &client,
                &job.job_id,
                &self.cv.conf().client,
                fail_if_not_found,
            )
            .await;
        }
        if !path.is_cv() {
            return err_box!("the current file {} is not a cache file", path);
        }
        let (ufs_path, mnt) = match self.get_mount(path, RpcCode::GetJobStatus).await? {
            Some((ufs_path, mnt)) => (ufs_path, mnt),
            None => return err_box!("the current file {} is not mounted to ufs", path),
        };

        let job_id = if mnt.info.is_fs_mode() {
            UnifiedUtils::create_job_id(path.full_path())
        } else {
            UnifiedUtils::create_job_id(ufs_path.full_path())
        };
        let client = JobMasterClient::new(self.fs_client());
        client.wait_job_complete(job_id, fail_if_not_found).await
    }

    pub async fn get_job_status(&self, path: &Path) -> FsResult<JobStatus> {
        let client = JobMasterClient::new(self.fs_client());
        let job_id = UnifiedUtils::create_job_id(path.full_path());
        client.get_job_status(job_id).await
    }

    pub async fn cleanup(&self) {
        self.cv.cleanup().await
    }

    pub fn disable_unified(&mut self) {
        self.enable_unified = false
    }

    pub async fn copy_ufs_file(
        &self,
        path: &Path,
        mnt: &MountValue,
        opts: CreateFileOpts,
        cv_len: i64,
    ) -> FsResult<()> {
        let opts = mnt.info.merge_create_opts(opts);
        let ufs_path = mnt.get_ufs_path(path)?;
        let mut reader = mnt.ufs()?.open(&ufs_path).await?;
        if reader.len() != cv_len {
            return err_box!(
                "file length mismatch: cv_path={:?}, ufs_path={:?}, ufs_len={}, cv_len={}",
                path,
                ufs_path,
                reader.len(),
                cv_len
            );
        }

        let flags = OpenFlags::new_create().set_overwrite(true);
        let mut writer = self.cv.open_with_opts(path, opts, flags).await?;

        loop {
            let data = reader.async_read(None).await?;
            if data.is_empty() {
                break;
            }
            writer.async_write(data).await?;
        }
        reader.complete().await?;
        writer.complete().await?;

        Ok(())
    }

    pub async fn open_for_write(&self, path: &Path) -> FsResult<UnifiedWriter> {
        let opts = self.cv().create_opts_builder().create_parent(true).build();
        let flags = OpenFlags::new_write_only().set_create(true);
        self.open_with_opts(path, opts, flags).await
    }

    pub async fn open_with_opts(
        &self,
        path: &Path,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<UnifiedWriter> {
        let time = TimeSpent::new();
        let mut write_path = path.path().to_owned();

        let fut = async {
            let rpc_code = if flags.read_only() && !flags.create() {
                RpcCode::OpenFile
            } else {
                RpcCode::CreateFile
            };
            match self.get_mount(path, rpc_code).await? {
                None => {
                    let writer = self.cv.open_with_opts(path, opts, flags).await?;
                    Ok(UnifiedWriter::Cv(writer))
                }

                Some((_, mount)) if mount.info.is_fs_mode() => {
                    let opts = mount.info.merge_create_opts(opts);
                    let mut writer = self.cv.open_with_opts(path, opts.clone(), flags).await?;
                    if writer.file_blocks().data_exists() || flags.overwrite() {
                        Ok(UnifiedWriter::Cv(writer))
                    } else {
                        writer.complete().await?;

                        info!(
                            "copying data from UFS to CV, path={}, len={}",
                            path,
                            writer.status().len
                        );
                        self.copy_ufs_file(path, &mount, opts.clone(), writer.status().len)
                            .await?;

                        let writer = self.cv.open_with_opts(path, opts, flags).await?;
                        Ok(UnifiedWriter::Cv(writer))
                    }
                }

                Some((ufs_path, mount)) => {
                    // P4-3 purge fences (gpt56 `2a089d5a` #1+#3): purge
                    // BEFORE the UFS write. A Key-scoped bound Free clears
                    // every live row for this file — including expired and
                    // locations-incomplete rows that an exact Invalidate
                    // can never clear (Miss ≠ no-entry) and that would
                    // otherwise pin a fresh CacheAllocate forever. The
                    // binding is the caller-observed mount/incarnation; a
                    // remount in between is the typed FENCED terminal, the
                    // UFS target is untouched, and the open is safe to
                    // retry. Failure is loud — never write over an
                    // unconfirmed stale cache state.
                    self.bound_purge(&mount, path, false).await?;

                    write_path = ufs_path.full_path().to_owned();
                    let ufs = mount.ufs()?;
                    if flags.append() {
                        return ufs.append(&ufs_path).await;
                    }

                    let writer = ufs.create(&ufs_path, flags.overwrite()).await?;

                    if mount.info.write_cache_enabled() {
                        let mirror_opts = mount.info.merge_create_opts(opts);
                        match WriteCacheWriter::new(
                            writer,
                            self.cv.clone(),
                            ufs,
                            path.clone(),
                            ufs_path.clone(),
                            mirror_opts,
                        )
                        .await
                        {
                            Ok(writer) => Ok(UnifiedWriter::WriteCache(Box::new(writer))),
                            Err((writer, e)) => {
                                warn!(
                                    "failed to open write cache mirror for cv_path={}, ufs_path={}: {}",
                                    path, ufs_path, e
                                );
                                Ok(writer)
                            }
                        }
                    } else {
                        Ok(writer)
                    }
                }
            }
        };

        let res = fut.await;

        let used_us = time.used_us();
        self.op_metric("Open", used_us);

        let cmd = format!("Open:{}", flags.access_mark());
        self.audit(&cmd, &write_path, "", res, used_us)
    }

    pub async fn mkdir_with_opts(
        &self,
        path: &Path,
        opts: MkdirOpts,
    ) -> FsResult<Option<FileStatus>> {
        let fut = async {
            match self.get_mount_checked(path, RpcCode::Mkdir).await? {
                None => Ok(Some(self.cv.mkdir_with_opts(path, opts).await?)),

                Some((ufs_path, mount)) => {
                    let flag = mount.ufs()?.mkdir(&ufs_path, opts.create_parent).await?;
                    if !flag {
                        err_ext!(FsError::file_exists(ufs_path.path()))
                    } else {
                        Ok(None)
                    }
                }
            }
        };
        self.track("Mkdir", path.path(), "", fut).await
    }

    pub async fn fuse_set_attr(
        &self,
        path: &Path,
        opts: SetAttrOpts,
    ) -> FsResult<Option<FileStatus>> {
        let fut = async {
            match self.get_mount_checked(path, RpcCode::SetAttr).await? {
                None => {
                    let status = self.cv.set_attr(path, opts).await?;
                    Ok(Some(status))
                }

                Some(_) => Ok(None),
            }
        };
        self.track("SetAttr", path.path(), "", fut).await
    }

    pub async fn get_lock(&self, path: &Path, lock: FileLock) -> FsResult<Option<FileLock>> {
        let fut = async {
            match self.get_mount_checked(path, RpcCode::GetLock).await? {
                None => self.cv.get_lock(path, lock).await,
                Some(_) => err_ext!(FsError::unsupported("get_lock")),
            }
        };
        self.track("GetLock", path.path(), "", fut).await
    }

    pub async fn set_lock(&self, path: &Path, lock: FileLock) -> FsResult<Option<FileLock>> {
        let fut = async {
            match self.get_mount_checked(path, RpcCode::SetLock).await? {
                None => self.cv.set_lock(path, lock).await,
                Some(_) => err_ext!(FsError::unsupported("set_lock")),
            }
        };
        self.track("SetLock", path.path(), "", fut).await
    }

    pub async fn rename_with_flags(
        &self,
        src: &Path,
        dst: &Path,
        flags: RenameFlags,
    ) -> FsResult<bool> {
        let fut = async {
            let _ = self.get_mount_checked(dst, RpcCode::Rename).await?;
            match self.get_mount_checked(src, RpcCode::Rename).await? {
                None => self.cv.rename_with_flags(src, dst, flags).await,
                Some((src_ufs, mount)) => {
                    if !flags.is_empty() {
                        return err_ext!(FsError::unsupported(
                            "rename flags through unified mount"
                        ));
                    }

                    // P4-3 purge fences (gpt56 `2a089d5a` #2): prove dst
                    // resolves to the SAME mount as src before any purge
                    // — the purge bindings and the UFS rename must target
                    // one namespace. A dst under a different mount (or
                    // under none) is loud; never half-bind a rename.
                    let Some((_, dst_mount)) = self.get_mount(dst, RpcCode::Rename).await? else {
                        return err_box!(
                            "rename dst {} is not covered by a mount (src mount {})",
                            dst,
                            mount.info.cv_path
                        );
                    };
                    if dst_mount.info.mount_id != mount.info.mount_id {
                        return err_box!(
                            "rename crosses mounts: src mount_id={} ({}), dst mount_id={} ({})",
                            mount.info.mount_id,
                            mount.info.cv_path,
                            dst_mount.info.mount_id,
                            dst_mount.info.cv_path
                        );
                    }

                    let dst_ufs = mount.get_ufs_path(dst)?;

                    // P4-3 ordering ruling (#3): purge BEFORE the UFS
                    // rename. Component-safe Prefix scope on src covers
                    // the file itself plus descendants; then the same on
                    // dst. A failure at either purge leaves UFS untouched
                    // — src still readable via UFS fallback — and the
                    // whole rename is safe to retry. Purge-after-UFS has
                    // no recovery path (src is gone) and is forbidden.
                    self.bound_purge(&mount, src, true).await?;

                    // gpt56 `89ad4667` seam: test-only fault point
                    // BETWEEN the two purges — the armed closure can flip
                    // the master state (e.g. umount+remount) so the dst
                    // purge below hits the REAL typed FENCED terminal
                    // from the server, not a fabricated error. Production
                    // builds have no seam at all (cfg(test) only).
                    #[cfg(test)]
                    {
                        let fault = RENAME_PURGE_FAULT.lock().unwrap().take();
                        if let Some(fault) = fault {
                            fault().await?;
                        }
                    }

                    self.bound_purge(&mount, dst, true).await?;

                    #[cfg(test)]
                    UFS_RENAME_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let res = mount.ufs()?.rename(&src_ufs, &dst_ufs).await?;

                    // Legacy inode cleanup stays warn-only (P4-4 retires
                    // it); cache correctness is owned by the purge above.
                    if let Err(e) = self.cv.delete(src, true).await {
                        if !matches!(e, FsError::FileNotFound(_)) {
                            warn!("failed to delete cache for {}: {}", src, e);
                        }
                    }

                    Ok(res)
                }
            }
        };
        self.track("Rename", src.path(), dst.path(), fut).await
    }
}

struct UnifiedUtils;

impl UnifiedUtils {
    fn create_job_id(source: impl AsRef<str>) -> String {
        format!("job_{}", Utils::md5(source))
    }
}

async fn submit_transfer_with_backoff(
    client: &TransferClient,
    command: TransferCommand,
) -> FsResult<curvine_proto::SubmitTransferResponse> {
    let mut attempt = 0usize;
    loop {
        match client.submit(command.clone()).await {
            Ok(response) => return Ok(response),
            Err(err)
                if attempt + 1 < TRANSFER_SUBMIT_MAX_ATTEMPTS
                    && retryable_transfer_submit_error(&err) =>
            {
                attempt += 1;
                let delay_ms = 200_u64.saturating_mul(1_u64 << (attempt - 1));
                warn!(
                    "retry transfer submit for {} after retryable error (attempt {}/{}): {}",
                    command.source_path,
                    attempt + 1,
                    TRANSFER_SUBMIT_MAX_ATTEMPTS,
                    err
                );
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

fn retryable_transfer_submit_error(err: &FsError) -> bool {
    match err {
        FsError::IO(_)
        | FsError::Pipeline(_)
        | FsError::Timeout(_)
        | FsError::TransferOverloaded(_)
        | FsError::TransferStoreUnavailable(_) => true,
        FsError::Common(inner) => {
            let message = inner.to_string();
            message.contains("TransferQueueFull")
                || message.contains("TransferOverloaded")
                || message.contains("TransferStoreUnavailable")
                || message.contains("sqlite transfer store error:")
                || message.contains("mysql transfer store error:")
        }
        _ => false,
    }
}

async fn wait_transfer_complete(
    client: &TransferClient,
    job_id: &str,
    client_conf: &curvine_config::ClientConf,
    fail_if_not_found: bool,
) -> FsResult<()> {
    time::timeout(
        Duration::from_millis(client_conf.max_sync_wait_timeout_ms),
        wait_transfer_complete0(client, job_id, client_conf, fail_if_not_found),
    )
    .await?
}

async fn wait_transfer_complete0(
    client: &TransferClient,
    job_id: &str,
    client_conf: &curvine_config::ClientConf,
    fail_if_not_found: bool,
) -> FsResult<()> {
    let mut ticks = 0_u64;
    let elapsed = TimeSpent::new();

    loop {
        let status = match client.status(job_id).await {
            Ok(status) => status,
            Err(err @ FsError::JobNotFound(_)) if !fail_if_not_found => {
                time::sleep(Duration::from_millis(
                    client_conf.sync_check_interval_min_ms,
                ))
                .await;
                continue;
            }
            Err(err) => return Err(err),
        };
        let state = TransferState::from(status.state);
        match state {
            TransferState::Completed => return Ok(()),
            TransferState::Failed | TransferState::Canceled | TransferState::PartialSuccess => {
                return err_box!(
                    "transfer {} {:?}: {}",
                    status.job_id,
                    state,
                    status.progress.message
                )
            }
            TransferState::Pending
            | TransferState::Planning
            | TransferState::Dispatching
            | TransferState::Running
            | TransferState::Canceling => {
                ticks += 1;
                let sleep_ms = client_conf
                    .sync_check_interval_max_ms
                    .min(client_conf.sync_check_interval_min_ms.saturating_mul(ticks));
                time::sleep(Duration::from_millis(sleep_ms)).await;

                if ticks.is_multiple_of(u64::from(client_conf.sync_check_log_tick)) {
                    info!(
                        "waiting for transfer {} to complete, elapsed: {} ms, loaded_size={}, total_size={}",
                        status.job_id,
                        elapsed.used_ms(),
                        status.progress.loaded_size,
                        status.progress.total_size
                    );
                }
            }
        }
    }
}

impl FileSystem<UnifiedWriter, UnifiedReader> for UnifiedFileSystem {
    fn fs_kind(&self) -> FsKind {
        FsKind::Cv
    }

    async fn mkdir(&self, path: &Path, create_parent: bool) -> FsResult<bool> {
        let opts = MkdirOptsBuilder::with_conf(&self.cv.conf().client)
            .create_parent(create_parent)
            .build();
        match self.mkdir_with_opts(path, opts).await {
            Ok(_) => Ok(true),
            Err(FsError::FileAlreadyExists(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn create(&self, path: &Path, overwrite: bool) -> FsResult<UnifiedWriter> {
        let flags = OpenFlags::new_write_only()
            .set_create(true)
            .set_overwrite(overwrite);
        let opts = self.cv.create_opts_builder().create_parent(true).build();
        self.open_with_opts(path, opts, flags).await
    }

    async fn append(&self, path: &Path) -> FsResult<UnifiedWriter> {
        let flags = OpenFlags::new_append().set_create(true);
        let opts = self.cv.create_opts_builder().build();
        self.open_with_opts(path, opts, flags).await
    }

    async fn exists(&self, path: &Path) -> FsResult<bool> {
        let fut = async {
            match self.get_mount_checked(path, RpcCode::Exists).await? {
                None => self.cv.exists(path).await,
                Some((ufs_path, mount)) => mount.ufs()?.exists(&ufs_path).await,
            }
        };
        self.track("Exists", path.path(), "", fut).await
    }

    async fn open(&self, path: &Path) -> FsResult<UnifiedReader> {
        let time = TimeSpent::new();
        let mut read_path = path.path().to_owned();

        let fut = async {
            let (ufs_path, mount) = match self.get_mount(path, RpcCode::OpenFile).await? {
                None => {
                    let reader = UnifiedReader::Cv(self.cv.open(path).await?);
                    return if reader.status().is_expired() {
                        err_ext!(FsError::file_expired(path.path()))
                    } else {
                        Ok(reader)
                    };
                }
                Some(v) => v,
            };

            // The D5 route (cache-mode strict interior) carries its
            // SAME-OBSERVATION mount/ufs_path out of the Get: a fence
            // refresh may have re-resolved the path onto a remounted
            // target, and the hit metrics, auto_cache, and miss→UFS
            // fallback below must all use that observed mount, never
            // the stale outer resolution (gpt56 `e53671d1` P0).
            let (reader, mount, ufs_path) =
                if mount.info.is_cache_mode() && path.path() != mount.info.cv_path {
                    // P4-2 D5 strict interior (gpt56 `c1d51e75`): ONE
                    // locations-bearing CacheGet with strict decode, D5
                    // validity, and full geometry validation. The hit
                    // reader is CACHE-ONLY — never `FallbackFsReader`
                    // (no per-read UFS stitching); zero inode RPCs on
                    // this route.
                    let (reader, mount, ufs_path) = self.get_cache_d5_reader(path).await?;
                    (reader.map(UnifiedReader::Cv), mount, ufs_path)
                } else {
                    // FsMode interior or a cache mount ROOT: the legacy
                    // inode-mirror route (the root is the mount-point
                    // directory, outside the CacheGet file domain).
                    let reader = self
                        .get_cv_reader(path, &ufs_path, &mount)
                        .await?
                        .map(UnifiedReader::Fallback);
                    (reader, mount, ufs_path)
                };
            if let Some(reader) = reader {
                debug!(
                    "read from Curvine(cache), ufs path {}, cv path: {}",
                    ufs_path, path
                );

                self.metrics
                    .mount_cache_hits
                    .with_label_values(&[mount.mount_id()])
                    .inc();

                Ok(reader)
            } else {
                self.metrics
                    .mount_cache_misses
                    .with_label_values(&[mount.mount_id()])
                    .inc();

                if mount.info.auto_cache() {
                    // Auto-cache is advisory: scheduling failures must not block the
                    // foreground read from falling back to UFS.
                    if let Err(err) = self.async_cache(&ufs_path) {
                        warn!("skip async cache request for {}: {}", ufs_path, err);
                    }
                }

                read_path = ufs_path.full_path().to_owned();
                // Reading from ufs
                if self.enable_read_ufs {
                    debug!("read from ufs, ufs path {}, cv path: {}", ufs_path, path);
                    mount.ufs()?.open(&ufs_path).await
                } else {
                    err_ext!(FsError::unsupported_ufs_read(path.path()))
                }
            }
        };

        let res = fut.await;

        let used_us = time.used_us();
        self.op_metric("Open", used_us);

        self.audit("Open:R", &read_path, "", res, used_us)
    }

    async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        self.rename_with_flags(src, dst, RenameFlags::empty()).await
    }

    async fn delete(&self, path: &Path, recursive: bool) -> FsResult<DeleteResult> {
        let fut = async {
            match self.get_mount_checked(path, RpcCode::Delete).await? {
                None => self.cv.delete(path, recursive).await,
                Some((ufs_path, mount)) => {
                    if path.path() == mount.info.cv_path {
                        return err_box!(
                            "cannot delete mount point root: cv_path={}, ufs_path={}",
                            mount.info.cv_path,
                            mount.info.ufs_path
                        );
                    }

                    // P4-3 ordering ruling (gpt56 `2a089d5a` #3): purge
                    // BEFORE the UFS delete. Key scope for a file,
                    // Prefix for a recursive dir delete — bound to the
                    // caller-observed mount/incarnation so a remount is
                    // the typed FENCED terminal with UFS untouched and
                    // the delete safe to retry.
                    self.bound_purge(&mount, path, recursive).await?;

                    let mut delete_res = mount.ufs()?.delete(&ufs_path, recursive).await?;

                    // P4-3 ordering ruling (gpt56 `2a089d5a` #3): the
                    // cache purge runs BEFORE the UFS delete — see the
                    // bound_purge call above. This leg is the legacy
                    // inode cleanup, warn-only (P4-4 retires it); cache
                    // correctness is owned by the purge, never here.
                    match self.cv.delete(path, recursive).await {
                        Ok(res) => {
                            delete_res.inodes += res.inodes;
                            delete_res.bytes += res.bytes;
                        }
                        Err(FsError::FileNotFound(_)) => {}
                        Err(e) => {
                            warn!("failed to delete cache for {}: {}", path, e);
                        }
                    }

                    Ok(delete_res)
                }
            }
        };
        self.track("Delete", path.path(), "", fut).await
    }

    async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        let fut = async {
            match self.get_mount(path, RpcCode::FileStatus).await? {
                None => self.cv.get_status(path).await,

                Some((_, mnt)) if mnt.info.is_fs_mode() => self.cv.get_status(path).await,

                Some((_, mnt)) if mnt.info.is_cache_mode() && path.path() != mnt.info.cv_path => {
                    // P4-2 D5 strict interior: CacheGet + D5; a valid hit
                    // synthesizes the entry status with ZERO inode RPCs,
                    // anything else is a miss onto the UFS status — taken
                    // from the SAME-OBSERVATION mount (gpt56 `e53671d1`
                    // P0: a fence refresh may have re-resolved onto a
                    // remounted target; the fallback status reads the
                    // mount the Get actually resolved against).
                    let (status, mnt, ufs_path) = self.cache_d5_status(path).await?;
                    match status {
                        Some(status) => Ok(status),
                        None => mnt.ufs()?.get_status(&ufs_path).await,
                    }
                }

                Some((ufs_path, mnt)) => match self.cv.get_status(path).await {
                    Ok(mut v) => match self.check_cache_validity(&v, &ufs_path, &mnt).await? {
                        CacheValidity::Valid => {
                            v.apply_ufs_fields();
                            Ok(v)
                        }
                        CacheValidity::Invalid(Some(ufs_status)) => Ok(ufs_status),
                        CacheValidity::Invalid(None) => mnt.ufs()?.get_status(&ufs_path).await,
                    },

                    Err(e) => {
                        if !matches!(e, FsError::FileNotFound(_) | FsError::Expired(_)) {
                            warn!("failed to get status file {}: {}", path, e);
                        };
                        mnt.ufs()?.get_status(&ufs_path).await
                    }
                },
            }
        };
        self.track("GetStatus", path.path(), "", fut).await
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        let fut = async {
            match self.get_mount_checked(path, RpcCode::ListStatus).await? {
                None => self.cv.list_status(path).await,
                Some((ufs_path, mount)) => mount.ufs()?.list_status(&ufs_path).await,
            }
        };
        self.track("ListStatus", path.path(), "", fut).await
    }

    async fn list_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        let fut = async {
            match self.get_mount_checked(path, RpcCode::ListStatus).await? {
                None => self.cv.list_status_bytes(path).await,
                Some((ufs_path, mount)) => mount.ufs()?.list_status_bytes(&ufs_path).await,
            }
        };
        self.track("ListStatus", path.path(), "", fut).await
    }

    async fn list_options(&self, path: &Path, options: ListOptions) -> FsResult<Vec<FileStatus>> {
        let fut = async {
            match self.get_mount_checked(path, RpcCode::ListOptions).await? {
                None => self.cv.list_options(path, options).await,
                Some((ufs_path, mount)) => mount.ufs()?.list_options(&ufs_path, options).await,
            }
        };
        self.track("ListOptions", path.path(), "", fut).await
    }

    async fn list_options_bytes(&self, path: &Path, options: ListOptions) -> FsResult<BytesMut> {
        let fut = async {
            match self.get_mount_checked(path, RpcCode::ListOptions).await? {
                None => self.cv.list_options_bytes(path, options).await,
                Some((ufs_path, mount)) => {
                    mount.ufs()?.list_options_bytes(&ufs_path, options).await
                }
            }
        };
        self.track("ListOptions", path.path(), "", fut).await
    }

    async fn list_stream(&self, path: &Path, options: ListOptions) -> FsResult<ListStream> {
        let fut = async {
            match self.get_mount_checked(path, RpcCode::ListOptions).await? {
                None => self.cv.list_stream(path, options).await,
                Some((ufs_path, mount)) => mount.ufs()?.list_stream(&ufs_path, options).await,
            }
        };
        self.track("ListStream", path.path(), "", fut).await
    }

    async fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> FsResult<()> {
        let fut = async {
            if self
                .get_mount_checked(path, RpcCode::SetAttr)
                .await?
                .is_none()
            {
                self.cv.set_attr(path, opts).await?;
            }
            // ignore setting attr on ufs mount paths
            Ok(())
        };
        self.track("SetAttr", path.path(), "", fut).await
    }
}

#[cfg(test)]
mod tests {
    use super::{AsyncCacheAdmission, AsyncCachePending};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    /// P1 seams (gpt56 `2e74f4ac` #3): the public Get decoder is STRICT —
    /// a `hit=true` response missing any observation field fails loud
    /// instead of fabricating a Hit (or an invalidate identity of 0).
    #[test]
    fn cache_get_strict_decode_rejects_malformed_hit() {
        use super::{CacheEntryStatus, UnifiedFileSystem};
        use curvine_proto::CacheGetResponse;

        let full_hit = CacheGetResponse {
            hit: Some(true),
            object_id: Some(7),
            file_len: Some(1024),
            block_size: Some(64),
            generation: Some(3),
            ufs_mtime: Some(12345),
            expire_at: Some(67890),
            ..Default::default()
        };
        assert_eq!(
            UnifiedFileSystem::status_from_get(&full_hit).unwrap(),
            CacheEntryStatus::Hit {
                object_id: 7,
                len: 1024,
                block_size: 64,
                generation: 3,
                ufs_mtime: 12345,
                expire_at: 67890,
            }
        );

        // hit=false (or absent) is a well-formed Miss.
        let miss = CacheGetResponse {
            hit: Some(false),
            ..Default::default()
        };
        assert_eq!(
            UnifiedFileSystem::status_from_get(&miss).unwrap(),
            CacheEntryStatus::Miss
        );

        // Every observation field is individually required on a hit.
        for missing in [
            "object_id",
            "file_len",
            "block_size",
            "generation",
            "ufs_mtime",
            "expire_at",
        ] {
            let mut malformed = full_hit.clone();
            match missing {
                "object_id" => malformed.object_id = None,
                "file_len" => malformed.file_len = None,
                "block_size" => malformed.block_size = None,
                "generation" => malformed.generation = None,
                "ufs_mtime" => malformed.ufs_mtime = None,
                "expire_at" => malformed.expire_at = None,
                _ => unreachable!(),
            }
            let err = UnifiedFileSystem::status_from_get(&malformed)
                .expect_err("malformed hit must fail loud");
            assert!(
                format!("{:?}", err).contains("missing observation fields"),
                "field {} missing should fail strict decode, got {:?}",
                missing,
                err
            );
        }
    }

    /// P4-2 seams (gpt56 `c1d51e75` #2): the strict block decoder accepts
    /// only a COMPLETE, geometrically exact location set — any anomaly is
    /// a whole-object miss (None).
    #[test]
    fn cache_block_locations_strict_geometry() {
        use super::UnifiedFileSystem;
        use curvine_proto::{CacheBlockLocationProto, CacheGetResponse, WorkerAddressProto};

        fn block(id: i64, len: i64, workers: usize) -> CacheBlockLocationProto {
            CacheBlockLocationProto {
                block_id: id,
                block_len: len,
                workers: vec![WorkerAddressProto::default(); workers],
            }
        }
        fn rep(blocks: Vec<CacheBlockLocationProto>) -> CacheGetResponse {
            CacheGetResponse {
                hit: Some(true),
                blocks,
                ..Default::default()
            }
        }
        let bs = 100i64;

        // Exact single- and multi-block layouts decode.
        let one = rep(vec![block(1 << 24 | 1, 100, 1)]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&one, 100, bs).is_some());
        let two = rep(vec![block(1 << 24 | 1, 100, 2), block(1 << 24 | 2, 50, 1)]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&two, 150, bs).is_some());
        // len == 0 requires an empty location set.
        assert!(UnifiedFileSystem::cache_blocks_from_get(&rep(vec![]), 0, bs).is_some());

        // Wrong block count / geometry / total.
        let mismatched = rep(vec![block(1 << 24 | 1, 250, 1)]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&mismatched, 150, bs).is_none()); // count != ceil
        let short = rep(vec![block(1 << 24 | 1, 50, 1)]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&short, 150, bs).is_none()); // count != ceil
        let bad_last = rep(vec![block(1 << 24 | 1, 100, 1), block(1 << 24 | 2, 100, 1)]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&bad_last, 150, bs).is_none()); // last != remainder
        let bad_mid = rep(vec![block(1 << 24 | 1, 90, 1), block(1 << 24 | 2, 60, 1)]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&bad_mid, 150, bs).is_none()); // non-last != block_size

        // Block ids must be unique and monotone (+1): duplicates,
        // decreasing order, and gaps are all whole-object misses.
        let dup = rep(vec![block(5, 100, 1), block(5, 50, 1)]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&dup, 150, bs).is_none());
        let unordered = rep(vec![block(9, 100, 1), block(8, 50, 1)]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&unordered, 150, bs).is_none());
        let gap = rep(vec![block(4, 100, 1), block(6, 50, 1)]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&gap, 150, bs).is_none());

        // Every block needs at least one worker; empty blocks with len>0
        // or blocks with len==0 are misses; non-positive block_size too.
        let no_worker = rep(vec![block(1 << 24 | 1, 250, 0)]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&no_worker, 250, bs).is_none());
        let zero_len_block = rep(vec![block(1 << 24 | 1, 0, 1)]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&zero_len_block, 0, bs).is_none());
        let empty_nonzero = rep(vec![]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&empty_nonzero, 250, bs).is_none());
        assert!(UnifiedFileSystem::cache_blocks_from_get(&one, 250, 0).is_none());

        // Degenerate shapes are rejected without panicking (the decoder
        // uses checked arithmetic throughout).
        assert!(UnifiedFileSystem::cache_blocks_from_get(&one, -2, bs).is_none());
        let absurd = rep(vec![block(1, i64::MAX, 1), block(2, i64::MAX, 1)]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&absurd, -2, bs).is_none());

        // An id at i64::MAX must be a whole-object miss, never an
        // overflow panic in the +1 monotone check (gpt56 `e53671d1`).
        let max_first = rep(vec![block(i64::MAX, 100, 1), block(i64::MAX - 1, 50, 1)]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&max_first, 150, bs).is_none());
        let max_last = rep(vec![block(i64::MAX - 1, 100, 1), block(i64::MAX, 50, 1)]);
        assert!(UnifiedFileSystem::cache_blocks_from_get(&max_last, 150, bs).is_some());
    }

    #[test]
    fn async_cache_pending_enforces_capacity_and_releases_slots() {
        let pending = AsyncCachePending::new(1, 1);
        let first = match pending.try_admit("ufs://bucket/a".to_string()) {
            AsyncCacheAdmission::Accepted(permit) => permit,
            _ => panic!("first request should be accepted"),
        };

        assert!(matches!(
            pending.try_admit("ufs://bucket/a".to_string()),
            AsyncCacheAdmission::AlreadyPending
        ));
        assert!(matches!(
            pending.try_admit("ufs://bucket/b".to_string()),
            AsyncCacheAdmission::Overloaded
        ));

        drop(first);
        assert!(matches!(
            pending.try_admit("ufs://bucket/b".to_string()),
            AsyncCacheAdmission::Accepted(_)
        ));
    }

    #[test]
    fn async_cache_pending_deduplicates_concurrent_requests() {
        let pending = Arc::new(AsyncCachePending::new(32, 4));
        let accepted = Arc::new(Mutex::new(Vec::new()));
        let already_pending = Arc::new(AtomicUsize::new(0));
        let overloaded = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();

        for _ in 0..32 {
            let pending = pending.clone();
            let accepted = accepted.clone();
            let already_pending = already_pending.clone();
            let overloaded = overloaded.clone();
            threads.push(std::thread::spawn(move || {
                match pending.try_admit("ufs://bucket/same".to_string()) {
                    AsyncCacheAdmission::Accepted(permit) => {
                        accepted.lock().unwrap().push(permit);
                    }
                    AsyncCacheAdmission::AlreadyPending => {
                        already_pending.fetch_add(1, Ordering::Relaxed);
                    }
                    AsyncCacheAdmission::Overloaded => {
                        overloaded.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }

        assert_eq!(accepted.lock().unwrap().len(), 1);
        assert_eq!(already_pending.load(Ordering::Relaxed), 31);
        assert_eq!(overloaded.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn async_cache_pending_waiters_hold_admission_until_submission_finishes() {
        let pending = AsyncCachePending::new(2, 1);
        let first_pending = match pending.try_admit("ufs://bucket/a".to_string()) {
            AsyncCacheAdmission::Accepted(permit) => permit,
            _ => panic!("first path should be admitted"),
        };
        let second_pending = match pending.try_admit("ufs://bucket/b".to_string()) {
            AsyncCacheAdmission::Accepted(permit) => permit,
            _ => panic!("second path should wait within pending capacity"),
        };

        let first_submit = pending.submit_slots.clone().try_acquire_owned().unwrap();
        assert!(pending.submit_slots.clone().try_acquire_owned().is_err());
        assert_eq!(pending.paths.lock().len(), 2);

        drop(first_submit);
        let second_submit = pending.submit_slots.clone().try_acquire_owned().unwrap();
        drop(second_submit);
        assert_eq!(pending.paths.lock().len(), 2);

        drop(first_pending);
        assert!(!pending.paths.lock().contains("ufs://bucket/a"));
        assert!(pending.paths.lock().contains("ufs://bucket/b"));
        drop(second_pending);
        assert!(pending.paths.lock().is_empty());
    }

    /// In-crate cluster recipe (mirrors curvine-tests `Testing::build` /
    /// `start_cluster`): cache metadata capability ON, fresh tmp dirs,
    /// 1 master + 1 worker. `curvine-unified-fs` cannot dev-depend on
    /// `curvine-tests` (cycle), so the seam test builds `MiniCluster`
    /// directly via a dev-dep on `curvine-server`.
    fn rename_seam_cluster() -> (
        super::UnifiedFileSystem,
        Arc<curvine_server::test::MiniCluster>,
        String,
    ) {
        use super::UnifiedFileSystem;
        use curvine_config::ClusterConf;
        use curvine_runtime::common::Utils;
        use curvine_runtime::runtime::AsyncRuntime;

        let mut conf = ClusterConf::default();
        let base = format!(
            "testing/unified-fs-rename-seam/{}-{}",
            std::process::id(),
            Utils::rand_str(6)
        );
        conf.master.meta_dir = format!("{base}/meta");
        conf.journal.journal_dir = format!("{base}/journal");
        conf.worker.data_dir = vec![format!("{base}/data")];
        conf.master.min_block_size = 1;
        conf.journal.raft_tick_interval_ms = 100;
        conf.master.cache_metadata_enabled = true;
        let ufs_base = format!("file://{base}/ufs");
        std::fs::create_dir_all(format!("{base}/ufs")).unwrap();

        let cluster = Arc::new(curvine_server::test::MiniCluster::with_num(&conf, 1, 1));
        let cluster_conf = cluster.cluster_conf.clone();
        cluster.start_cluster();
        let rt = Arc::new(AsyncRuntime::single());
        // The client MUST bind to the cluster's resolved conf (real held
        // ports), not the base conf — same as curvine-tests'
        // `get_active_cluster_conf`.
        let fs = UnifiedFileSystem::with_rt(cluster_conf, rt).unwrap();
        (fs, cluster, ufs_base)
    }

    /// gpt56 `89ad4667` frozen two-stage rename seam: the src Prefix
    /// purge succeeds, then the armed fault performs a REAL umount +
    /// remount (same UFS dir) between the two purges, so the dst Prefix
    /// purge hits the REAL typed CacheIncarnationFenced terminal from
    /// the master. Asserts: UFS rename call count stays at its baseline
    /// (0 delta — purge-before-UFS means a fenced second stage never
    /// mutates the backend), UFS src/dst untouched, src old-inc row
    /// already cleared, new incarnation at zero rows, src still readable
    /// via the UFS fallback, and after a cache-domain refresh the RETRY
    /// rename succeeds. Also proves a pre-seeded dst cache entry is
    /// cleared by a SUCCESSFUL rename (nothing ever serves OLD).
    #[test]
    fn rename_two_stage_purge_fence_leaves_ufs_untouched_and_retry_safe() {
        use super::{RENAME_PURGE_FAULT, UFS_RENAME_CALLS};
        use crate::{CacheEntryStatus, UfsFileSystem};
        use curvine_fs_api::{FileSystem, Path, Reader, RpcCode, Writer};
        use curvine_model::{AccessMode, MountOptionsBuilder, WriteType};
        use curvine_runtime::common::Utils;
        use curvine_runtime::runtime::RpcRuntime;

        let (fs, cluster, ufs_base) = rename_seam_cluster();
        let master_fs = cluster.get_active_master_fs();
        let rt = fs.clone_runtime();

        rt.block_on(async move {
            let opts = MountOptionsBuilder::new()
                .write_type(WriteType::CacheMode)
                .access_mode(AccessMode::ReadWrite)
                .build();
            let ufs_root = Path::from_str(format!("{}/seam", ufs_base)).unwrap();
            let cv_root: Path = "/seam".into();
            let ufs = UfsFileSystem::new(&ufs_root, opts.add_properties.clone(), None).unwrap();
            ufs.mkdir(&ufs_root, true).await.unwrap();
            fs.mount(&ufs_root, &cv_root, opts.clone()).await.unwrap();

            let write = |name: &str, data: &str| {
                let fs = fs.clone();
                let path: Path = format!("/seam/{}", name).into();
                let data = data.to_string();
                async move {
                    let mut w = fs.create(&path, true).await.unwrap();
                    w.write_string(&data).await.unwrap();
                    w.complete().await.unwrap();
                    path
                }
            };
            let cache = |path: &Path| {
                let fs = fs.clone();
                let path = path.clone();
                async move {
                    let (ufs_path, _) = fs
                        .get_mount(&path, RpcCode::GetMountInfo)
                        .await
                        .unwrap()
                        .unwrap();
                    fs.async_cache(&ufs_path).unwrap();
                    fs.wait_job_complete(&path, false).await.unwrap();
                }
            };

            // ---- Part A: a SUCCESSFUL rename clears a pre-seeded dst
            // cache entry; the stale cached OLD never serves after.
            let old_dst = Utils::rand_str(2048);
            let dst_a = write("a_dst.log", &old_dst).await;
            cache(&dst_a).await;
            assert!(matches!(
                fs.cache_status(&dst_a).await.unwrap(),
                CacheEntryStatus::Hit { .. }
            ));

            let data_a = Utils::rand_str(3072);
            let src_a = write("a_src.log", &data_a).await;
            cache(&src_a).await;

            let (_, mnt) = fs
                .get_mount(&src_a, RpcCode::GetMountInfo)
                .await
                .unwrap()
                .unwrap();
            let inc1 = master_fs
                .cache_service
                .current_incarnation_for_mount(mnt.info.mount_id)
                .unwrap()
                .unwrap();
            let key = |p: &Path| -> String {
                let ufs_p = mnt.get_ufs_path(p).unwrap();
                mnt.info.get_cache_key(&ufs_p).unwrap()
            };

            assert!(fs.rename(&src_a, &dst_a).await.unwrap());
            assert_eq!(UFS_RENAME_CALLS.load(Ordering::SeqCst), 1);
            // Both the src and the PRE-SEEDED dst entries are cleared
            // under the observed incarnation.
            assert!(master_fs
                .cache_service
                .get(inc1, &key(&src_a), false)
                .unwrap()
                .is_none());
            assert!(
                master_fs
                    .cache_service
                    .get(inc1, &key(&dst_a), false)
                    .unwrap()
                    .is_none(),
                "a successful rename must clear the pre-seeded dst cache entry"
            );
            // The renamed dst serves the NEW content via UFS fallback —
            // never the stale cached OLD.
            let mut r = fs.open(&dst_a).await.unwrap();
            assert_eq!(r.read_as_string().await.unwrap(), data_a);

            // ---- Part B: two-stage fence. Src purge succeeds, the fault
            // flips the master (umount + remount), the dst purge FENCED.
            let old_dst_b = Utils::rand_str(1024);
            let dst_b = write("b_dst.log", &old_dst_b).await;
            cache(&dst_b).await;
            let data_b = Utils::rand_str(4096);
            let src_b = write("b_src.log", &data_b).await;
            cache(&src_b).await;

            let baseline = UFS_RENAME_CALLS.load(Ordering::SeqCst);
            let cv = fs.cv().clone();
            let fault_root = cv_root.clone();
            let fault_ufs = ufs_root.clone();
            let fault_opts = opts.clone();
            RENAME_PURGE_FAULT
                .lock()
                .unwrap()
                .replace(Box::new(move || {
                    Box::pin(async move {
                        cv.umount(&fault_root).await?;
                        cv.mount(&fault_ufs, &fault_root, fault_opts).await?;
                        Ok(())
                    })
                }));

            let err = match fs.rename(&src_b, &dst_b).await {
                Err(e) => e,
                Ok(_) => panic!("the fenced dst purge must abort the rename, not succeed"),
            };
            assert!(
                matches!(err.kind(), curvine_error::ErrorKind::CacheIncarnationFenced),
                "the dst purge must be the typed FENCED terminal, got {:?}",
                err
            );
            // The fenced second stage NEVER reached the backend.
            assert_eq!(
                UFS_RENAME_CALLS.load(Ordering::SeqCst),
                baseline,
                "a fenced dst purge must mean ZERO UFS rename calls"
            );

            // The remounted mount is a different (mount_id, incarnation).
            let table = fs.cv().get_mount_table().await.unwrap();
            let mnt2 = table
                .iter()
                .find(|m| m.cv_path == "/seam")
                .expect("remounted mount must be in the table");
            let inc2 = master_fs
                .cache_service
                .current_incarnation_for_mount(mnt2.mount_id)
                .unwrap()
                .unwrap();
            assert_ne!((mnt2.mount_id, inc2), (mnt.info.mount_id, inc1));

            // The src old-inc row IS cleared (the first purge succeeded
            // before the fault); the dst old-inc row survives untouched.
            // Dead-incarnation rows need the raw observability API (the
            // public `get` is correctly fenced).
            assert!(
                !master_fs
                    .cache_service
                    .raw_row_valid(inc1, &key(&src_b))
                    .unwrap(),
                "the src purge (before the fault) must have cleared the old-inc row"
            );
            assert!(
                master_fs
                    .cache_service
                    .raw_row_valid(inc1, &key(&dst_b))
                    .unwrap(),
                "the fenced purge must leave the old-inc dst row untouched (no victims)"
            );
            // The new incarnation is at ZERO rows for either key.
            let key2 = |p: &Path| -> String {
                let ufs_p = mnt2.get_ufs_path(p).unwrap();
                mnt2.get_cache_key(&ufs_p).unwrap()
            };
            for k in [key2(&src_b), key2(&dst_b)] {
                assert!(
                    master_fs
                        .cache_service
                        .get(inc2, &k, false)
                        .unwrap()
                        .is_none(),
                    "the new incarnation must stay at zero changes"
                );
            }

            // The UFS backend is untouched by the fenced rename: src
            // still has its content, dst still has the pre-seeded OLD.
            let src_b_ufs = mnt2.get_ufs_path(&src_b).unwrap();
            let dst_b_ufs = mnt2.get_ufs_path(&dst_b).unwrap();
            let mut r = ufs.open(&src_b_ufs).await.unwrap();
            assert_eq!(
                r.read_as_string().await.unwrap(),
                data_b,
                "the fenced rename must leave UFS src untouched"
            );
            let mut r = ufs.open(&dst_b_ufs).await.unwrap();
            assert_eq!(
                r.read_as_string().await.unwrap(),
                old_dst_b,
                "the fenced rename must leave UFS dst untouched"
            );

            // The unified view still serves src via the UFS fallback.
            let mut r = fs.open(&src_b).await.unwrap();
            assert_eq!(r.read_as_string().await.unwrap(), data_b);

            // Refresh through the cache-domain fence-refresh path, then
            // the RETRY rename succeeds against the new binding.
            assert_eq!(
                fs.cache_status(&src_b).await.unwrap(),
                CacheEntryStatus::Miss
            );
            assert!(fs.rename(&src_b, &dst_b).await.unwrap());
            assert_eq!(UFS_RENAME_CALLS.load(Ordering::SeqCst), baseline + 1);
            let mut r = fs.open(&dst_b).await.unwrap();
            assert_eq!(
                r.read_as_string().await.unwrap(),
                data_b,
                "the retried rename must serve the renamed content, never the stale OLD"
            );
        });
    }
}
