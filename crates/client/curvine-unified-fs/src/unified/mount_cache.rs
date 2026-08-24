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

/*!
# Mount Cache System

High-performance caching layer for filesystem mount information with bidirectional path mapping.

## Data Structure Architecture

```text
┌─────────────────────────────────────────────────────────────────┐
│                        MountCache                               │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │ update_interval │  │   last_update   │  │     mounts      │  │
│  │     (u64)       │  │(AtomicCounter)  │  │  RwLock<Map>    │  │
│  │ TTL in millis   │  │ lock-free time  │  │ thread-safe     │  │
│  └─────────────────┘  └─────────────────┘  └─────────┬───────┘  │
│                                                       │          │
└───────────────────────────────────────────────────────┼──────────┘
                                                        │
        ┌───────────────────────────────────────────────▼──────────┐
        │                    InnerMap                              │
        │           (Bidirectional Path Index)                    │
        │                                                         │
        │  ┌─────────────────────────┐  ┌─────────────────────────┐│
        │  │        cv_map           │  │       ufs_map           ││
        │  │FastHashMap<String, Arc> │  │FastHashMap<String, Arc> ││
        │  │                         │  │                         ││
        │  │Key: CV Path             │  │Key: UFS Path            ││
        │  │"/data/ml/model.bin"     │  │"s3://bucket/model.bin"  ││
        │  │"/data/ml/"              │  │"s3://bucket/"           ││
        │  │"/data/"                 │  │"hdfs://cluster/data/"   ││
        │  │                         │  │                         ││
        │  │Val: Arc<MountValue> ────┼──┼──▶ Same Instance       ││
        │  └─────────────────────────┘  └─────────────────────────┘│
        └─────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────▼───────────────┐
                    │          MountValue          │
                    │                              │
                    │  ┌─────────┐ ┌─────────────┐ │
                    │  │  info   │ │     ufs     │ │
                    │  │MountInfo│ │UfsFileSystem│ │
                    │  │metadata │ │ I/O handler │ │
                    │  └─────────┘ └─────────────┘ │
                    │           mount_id           │
                    │          (String)           │
                    └─────────────────────────────┘
```
*/

use crate::{UfsFileSystem, UnifiedFileSystem};
use curvine_core_error::{err_box, CommonResult};
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_model::{MountInfo, ProtoUtils};
use curvine_proto::MountInfoProto;
use curvine_runtime::common::{FastHashMap, LocalTime};
use curvine_runtime::runtime::RpcRuntime;
use curvine_runtime::sync::AtomicCounter;
use log::{debug, warn};
use once_cell::sync::OnceCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

/// Task #6 P4-1 (gpt56 `88cda9cf` Q1): client-only mount snapshot.
///
/// The current cache incarnation is a RESPONSE-ONLY, master-composed
/// field (`MountInfoProto.cache_incarnation`); it must never enter the
/// persisted `MountInfo` bincode model. `MountSnapshot` captures the
/// mount row and its incarnation in ONE decode of the raw proto, so the
/// pair is stored and replaced atomically inside `MountValue` — the
/// incarnation can never drift onto a different mount's row.
///
/// Fail-closed rule: a CacheMode mount whose response carries a missing
/// or zero incarnation is CORRUPTION (every visible cache-mode mount must
/// have an authoritative current incarnation), and the decode fails
/// loud. An FsMode mount's `None` is normal.
#[derive(Debug, Clone)]
pub struct MountSnapshot {
    pub info: MountInfo,
    pub cache_incarnation: Option<u64>,
}

impl MountSnapshot {
    /// Non-response construction (e.g. a worker building a data-plane
    /// `MountValue` from a job-spec `MountInfo`): no incarnation is
    /// present in that context and none is asserted. The fail-closed
    /// contract applies to mount-table RESPONSES only (`from_pb`).
    pub fn from_info(info: MountInfo) -> Self {
        Self {
            info,
            cache_incarnation: None,
        }
    }

    pub fn from_pb(pb: MountInfoProto) -> FsResult<Self> {
        let cache_incarnation = pb.cache_incarnation;
        let info = ProtoUtils::mount_info_from_pb(pb);
        if info.is_cache_mode() && cache_incarnation.unwrap_or(0) == 0 {
            return err_box!(
                "cache-mode mount {} ({}) has no authoritative cache incarnation in the mount-table response",
                info.cv_path,
                info.mount_id
            );
        }
        Ok(Self {
            info,
            cache_incarnation,
        })
    }
}

/// Represents a single mount point with its filesystem handler.
/// Contains mount metadata, UFS handler, and path conversion utilities.
pub struct MountValue {
    pub info: MountInfo,
    /// The incarnation observed in the SAME mount-table response as
    /// `info` (see `MountSnapshot`); `None` for FsMode mounts.
    pub cache_incarnation: Option<u64>,
    ufs: OnceCell<UfsFileSystem>,
    pub mount_id: String,
}

impl MountValue {
    pub fn new(snapshot: MountSnapshot) -> FsResult<Self> {
        let mount_id = format!("{}", snapshot.info.mount_id);

        Ok(Self {
            info: snapshot.info,
            cache_incarnation: snapshot.cache_incarnation,
            ufs: OnceCell::new(),
            mount_id,
        })
    }

    pub fn ufs(&self) -> FsResult<UfsFileSystem> {
        self.ufs
            .get_or_try_init(|| {
                let ufs_path = Path::from_str(&self.info.ufs_path)?;
                UfsFileSystem::new(&ufs_path, self.info.properties.clone(), self.info.provider)
            })
            .cloned()
    }

    /// Converts CV path to UFS path
    /// Example: cv://cluster/data/file.txt -> s3://bucket/data/file.txt
    pub fn get_ufs_path(&self, cv_path: &Path) -> CommonResult<Path> {
        self.info.get_ufs_path(cv_path)
    }

    /// Converts UFS path to CV path
    /// Example: s3://bucket/data/file.txt -> cv://cluster/data/file.txt
    pub fn get_cv_path(&self, ufs_path: &Path) -> CommonResult<Path> {
        self.info.get_cv_path(ufs_path)
    }

    pub fn toggle_path(&self, path: &Path) -> CommonResult<Path> {
        self.info.toggle_path(path)
    }

    pub fn mount_id(&self) -> &str {
        &self.mount_id
    }
}

#[derive(Default)]
struct InnerMap {
    ufs_map: FastHashMap<String, Arc<MountValue>>,
    cv_map: FastHashMap<String, Arc<MountValue>>,
}

impl InnerMap {
    pub fn insert(&mut self, snapshot: MountSnapshot) -> CommonResult<()> {
        let value = Arc::new(MountValue::new(snapshot)?);
        self.cv_map
            .insert(value.info.cv_path.clone(), value.clone());
        self.ufs_map.insert(value.info.ufs_path.clone(), value);
        Ok(())
    }

    pub fn remove(&mut self, path: &Path) {
        if path.is_cv() {
            if let Some(info) = self.cv_map.remove(path.path()) {
                let _ = self.ufs_map.remove(&info.info.ufs_path);
            }
        } else if let Some(info) = self.ufs_map.remove(path.full_path()) {
            let _ = self.cv_map.remove(&info.info.cv_path);
        }
    }

    pub fn get(&self, is_cv: bool, path: &str) -> Option<Arc<MountValue>> {
        if is_cv {
            self.cv_map.get(path).cloned()
        } else {
            self.ufs_map.get(path).cloned()
        }
    }

    pub fn len(&self) -> usize {
        self.cv_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// RAII guard that clears the `refreshing` flag on drop. This guarantees the
/// flag is released on *every* exit path of the background task — normal
/// completion, early return, and panic-unwind alike.
struct RefreshingGuard<'a> {
    flag: &'a AtomicBool,
}

impl Drop for RefreshingGuard<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

pub struct MountCache {
    mounts: RwLock<InnerMap>,
    update_interval: u64,
    last_update: AtomicCounter,
    /// Single-flight lock: only one task performs full refresh when TTL expires.
    refresh_lock: Mutex<()>,
    /// True while a background refresh has been scheduled but not yet finished.
    /// Used to avoid spawning more than one concurrent background refresh task.
    refreshing: AtomicBool,
}

impl MountCache {
    pub fn new(update_interval: u64) -> Self {
        Self {
            mounts: RwLock::new(InnerMap::default()),
            update_interval,
            last_update: AtomicCounter::new(0),
            refresh_lock: Mutex::new(()),
            refreshing: AtomicBool::new(false),
        }
    }

    fn need_update(&self) -> bool {
        LocalTime::mills() > self.update_interval + self.last_update.get()
    }

    /// Whether the cache has ever been successfully populated. `last_update` is
    /// 0 until the first successful refresh sets it to a real wall-clock millis.
    fn is_initialized(&self) -> bool {
        self.last_update.get() != 0
    }

    /// Performs the actual full refresh of the mount table from the master.
    /// Guarded by `refresh_lock` so only one refresh runs at a time.
    ///
    /// P4-1: refreshes from the RAW mount-table response so each row is
    /// decoded into a `MountSnapshot` (mount row + current incarnation,
    /// one decode, atomic replace). `get_mount_table`'s `MountInfo`
    /// conversion drops the response-only incarnation field and cannot be
    /// used here.
    async fn do_refresh(&self, fs: &UnifiedFileSystem) -> FsResult<()> {
        let raw = fs.cv().get_mount_table_raw().await?;
        self.apply_raw(raw)
    }

    /// Publishes a raw mount-table response ATOMICICALLY (gpt56
    /// `2e74f4ac` #1): the WHOLE response is decoded into a fresh table
    /// before the live cache is touched, and only a fully decoded table
    /// is swapped in under one write lock. A decode failure anywhere in
    /// the response (e.g. a cache-mode row missing its incarnation)
    /// leaves the previous complete snapshot untouched — the caller sees
    /// the error, never a half-published table.
    fn apply_raw(&self, raw: Vec<MountInfoProto>) -> FsResult<()> {
        let mut next = InnerMap::default();
        for pb in raw {
            next.insert(MountSnapshot::from_pb(pb)?)?;
        }

        let mut state = self.mounts.write().unwrap();
        *state = next;

        debug!("update mounts {:?}", state.len());
        self.last_update.set(LocalTime::mills());
        Ok(())
    }

    /// Synchronous refresh: blocks until the mount table is up to date.
    /// Used when the caller must observe the latest state immediately
    /// (e.g. right after a `mount` call). `force` bypasses the TTL check.
    pub async fn check_update(&self, fs: &UnifiedFileSystem, force: bool) -> FsResult<()> {
        if !self.need_update() && !force {
            return Ok(());
        }

        let _guard = self.refresh_lock.lock().await;
        if !self.need_update() && !force {
            return Ok(());
        }

        self.do_refresh(fs).await
    }

    /// Non-blocking refresh trigger: if the cache is stale, spawn a background
    /// task to refresh it and return immediately, letting the caller keep using
    /// the current (possibly stale) snapshot.
    ///
    /// At most one background refresh runs at a time: the `refreshing` flag is
    /// claimed via compare_exchange, and a duplicate trigger is a no-op until the
    /// in-flight task clears it. The task takes ownership of cloned handles
    /// (`Arc<MountCache>` and `UnifiedFileSystem`), so it can outlive the current
    /// request.
    fn trigger_async_update(self: &Arc<Self>, fs: &UnifiedFileSystem) {
        if !self.need_update() {
            return;
        }

        // Ensure at most one background refresh is in flight. compare_exchange
        // returning Err means another task already set the flag and will publish
        // a fresh snapshot soon, so this call becomes a no-op.
        if self
            .refreshing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let cache = self.clone();
        let fs = fs.clone();
        let rt = fs.clone_runtime();
        rt.spawn(async move {
            // Take ownership of the claimed `refreshing` flag via an RAII guard
            // so it is cleared on every exit path — including a panic in
            // `do_refresh` (e.g. a poisoned RwLock) — not just on the normal
            // tail. Without this, a panic here would leak the flag and wedge all
            // future refreshes. Created before the lock so it covers the whole
            // task body.
            let _refreshing = RefreshingGuard {
                flag: &cache.refreshing,
            };
            // Hold the single-flight lock for the whole refresh so the semantics
            // match the synchronous path and double-refresh is impossible.
            let _guard = cache.refresh_lock.lock().await;
            if cache.need_update() {
                if let Err(e) = cache.do_refresh(&fs).await {
                    warn!("background mount cache refresh failed: {:?}", e);
                }
            }
        });
    }

    /// Finds mount point for a path using hierarchical lookup.
    /// Returns the most specific mount that contains the given path.
    ///
    /// Refresh policy:
    /// - On cold start (cache never populated) the first call refreshes
    ///   synchronously so a freshly-created client observes the correct mount
    ///   table instead of an empty one.
    /// - Once populated, a stale cache triggers a background refresh that is NOT
    ///   awaited: the lookup proceeds against the current snapshot so the caller
    ///   is never blocked on a master round-trip. The refreshed table becomes
    ///   visible to subsequent calls.
    pub async fn get_mount(
        self: &Arc<Self>,
        fs: &UnifiedFileSystem,
        path: &Path,
    ) -> FsResult<Option<Arc<MountValue>>> {
        if self.is_initialized() {
            self.trigger_async_update(fs);
        } else {
            // First access: block once to populate the cache. check_update is
            // single-flight, so concurrent first callers share one refresh.
            self.check_update(fs, false).await?;
        }

        let state = self.mounts.read().unwrap();
        if state.is_empty() {
            return Ok(None);
        }

        for mount_path in path.get_possible_mounts() {
            if let Some(mount) = state.get(path.is_cv(), &mount_path) {
                return Ok(Some(mount));
            }
        }

        Ok(None)
    }

    pub fn remove(&self, path: &Path) {
        let mut state = self.mounts.write().unwrap();
        state.remove(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::{AccessMode, Provider, TtlAction, WriteType};
    use std::collections::HashMap;

    fn opendal_oss_mount() -> MountInfo {
        MountInfo {
            cv_path: "/oss-mount/data".to_string(),
            ufs_path: "oss://example-bucket/data".to_string(),
            mount_id: 1,
            properties: HashMap::new(),
            ttl_ms: 7 * 24 * 60 * 60 * 1000,
            ttl_action: TtlAction::Delete,
            read_verify_ufs: false,
            storage_type: None,
            block_size: None,
            replicas: None,
            write_type: WriteType::CacheMode,
            provider: Some(Provider::Opendal),
            auto_cache: true,
            access_mode: AccessMode::ReadOnly,
            write_cache: false,
        }
    }

    fn snapshot_for(info: MountInfo, incarnation: Option<u64>) -> FsResult<MountSnapshot> {
        let mut pb = ProtoUtils::mount_info_to_pb(info);
        pb.cache_incarnation = incarnation;
        MountSnapshot::from_pb(pb)
    }

    #[test]
    fn cache_insert_is_metadata_only_for_unsupported_provider() {
        let mut mounts = InnerMap::default();

        mounts
            .insert(snapshot_for(opendal_oss_mount(), Some(3)).unwrap())
            .expect("mount cache refresh should not initialize UFS providers");

        assert!(mounts.get(true, "/flink/checkpoints").is_none());
        let mount = mounts
            .get(true, "/oss-mount/data")
            .expect("mounted path should be cached");
        assert_eq!(mount.cache_incarnation, Some(3));
        assert!(mount.ufs.get().is_none());
    }

    #[test]
    fn unsupported_provider_error_is_lazy_until_mount_use() {
        let mut mounts = InnerMap::default();
        mounts
            .insert(snapshot_for(opendal_oss_mount(), Some(3)).unwrap())
            .expect("mount cache refresh should not initialize UFS providers");

        let mount = mounts
            .get(true, "/oss-mount/data")
            .expect("mounted path should be cached");

        assert!(mount.ufs().is_err());
    }

    /// P4-1 seam (gpt56 `88cda9cf`): a CacheMode row without a nonzero
    /// incarnation in the mount-table response is CORRUPTION and the
    /// snapshot decode fails closed — never a silent zero incarnation.
    #[test]
    fn cache_mode_snapshot_without_incarnation_fails_closed() {
        let err = snapshot_for(opendal_oss_mount(), None).unwrap_err();
        assert!(
            format!("{:?}", err).contains("no authoritative cache incarnation"),
            "missing incarnation must fail loud, got {:?}",
            err
        );

        let err = snapshot_for(opendal_oss_mount(), Some(0)).unwrap_err();
        assert!(
            format!("{:?}", err).contains("no authoritative cache incarnation"),
            "zero incarnation must fail loud, got {:?}",
            err
        );
    }

    /// FsMode rows never carry an incarnation; `None` is normal there.
    #[test]
    fn fs_mode_snapshot_without_incarnation_is_normal() {
        let mut info = opendal_oss_mount();
        info.write_type = WriteType::FsMode;
        let snapshot = snapshot_for(info, None).unwrap();
        assert_eq!(snapshot.cache_incarnation, None);
    }

    /// P1 seam (gpt56 `2e74f4ac` #1): a refresh whose response has a
    /// valid first row but a CORRUPT second row (cache-mode without
    /// incarnation) must fail WITHOUT touching the live table — the
    /// previously published snapshot stays complete, never a
    /// half-published mix of old and new rows.
    #[test]
    fn refresh_decode_failure_keeps_old_table_complete() {
        let mut second = opendal_oss_mount();
        second.cv_path = "/oss-mount/other".to_string();
        second.ufs_path = "oss://example-bucket/other".to_string();

        let cache = MountCache::new(u64::MAX);

        // Seed a complete old table: two rows, distinct incarnations.
        let old_first = ProtoUtils::mount_info_to_pb(opendal_oss_mount());
        let mut old_second = ProtoUtils::mount_info_to_pb(second.clone());
        cache
            .apply_raw(vec![
                {
                    let mut pb = old_first.clone();
                    pb.cache_incarnation = Some(11);
                    pb
                },
                {
                    old_second.cache_incarnation = Some(22);
                    old_second.clone()
                },
            ])
            .expect("seed refresh should succeed");

        // New response: first row valid, second row corrupt (cache-mode
        // row with no incarnation).
        let new_first = {
            let mut info = opendal_oss_mount();
            info.cv_path = "/oss-mount/newfirst".to_string();
            info.ufs_path = "oss://example-bucket/newfirst".to_string();
            let mut pb = ProtoUtils::mount_info_to_pb(info);
            pb.cache_incarnation = Some(33);
            pb
        };
        let corrupt_second = {
            let mut pb = ProtoUtils::mount_info_to_pb(second);
            pb.cache_incarnation = None;
            pb
        };

        let err = cache
            .apply_raw(vec![new_first, corrupt_second])
            .expect_err("corrupt second row must fail the whole refresh");
        assert!(
            format!("{:?}", err).contains("no authoritative cache incarnation"),
            "expected fail-closed incarnation error, got {:?}",
            err
        );

        // The old table is COMPLETE and unchanged: both original rows,
        // original incarnations, and none of the new response leaked in.
        let state = cache.mounts.read().unwrap();
        assert_eq!(state.len(), 2, "old table must keep both rows");
        let m1 = state.get(true, "/oss-mount/data").expect("old row 1 alive");
        assert_eq!(m1.cache_incarnation, Some(11));
        let m2 = state
            .get(true, "/oss-mount/other")
            .expect("old row 2 alive");
        assert_eq!(m2.cache_incarnation, Some(22));
        assert!(
            state.get(true, "/oss-mount/newfirst").is_none(),
            "no row from the failed response may leak into the live table"
        );
    }
}
