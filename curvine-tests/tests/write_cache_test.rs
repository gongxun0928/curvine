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

use bytes::BytesMut;
use curvine_client::unified::{
    CacheEntryStatus, CacheInvalidateResult, UfsFileSystem, UnifiedFileSystem, UnifiedReader,
};
use curvine_fs_api::{FileSystem, Path, Reader, RpcCode, Writer};
use curvine_io::DataSlice;
use curvine_model::{AccessMode, MountInfo, MountOptionsBuilder, WriteType, UFS_INODE_ID};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_tests::Testing;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;
fn get_fs() -> UnifiedFileSystem {
    let testing = Testing::builder().workers(1).build().unwrap();
    // Check if UFS configuration is available, if not, skip the test
    if env::var("UFS_TEST_PATH").is_err() {
        env::set_var("UFS_TEST_PATH", testing.ufs_path.clone());
    }

    testing.start_cluster().unwrap();
    let rt = Arc::new(AsyncRuntime::single());
    testing.get_unified_fs_with_rt(rt.clone()).unwrap()
}

/// Free bridge (task #6): the master-side typed free lives in the cache
/// metadata index, so the E2E seam runs with the capability ENABLED
/// (`master.cache_metadata_enabled` defaults to false). The in-process
/// master handle is returned so the seam can observe index entries
/// directly (the bridged free exposes no exact public stats — gpt56
/// `8336e8a8`).
fn get_fs_cache_meta() -> (UnifiedFileSystem, Arc<curvine_server::test::MiniCluster>) {
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(|conf| conf.master.cache_metadata_enabled = true)
        .build()
        .unwrap();
    if env::var("UFS_TEST_PATH").is_err() {
        env::set_var("UFS_TEST_PATH", testing.ufs_path.clone());
    }

    let cluster = testing.start_cluster().unwrap();
    let rt = Arc::new(AsyncRuntime::single());
    let fs = testing.get_unified_fs_with_rt(rt.clone()).unwrap();
    (fs, cluster)
}

#[test]
fn test_cache_mode() {
    let (fs, cluster) = get_fs_cache_meta();
    let master_fs = cluster.get_active_master_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::CacheMode).await;

        // Capability ruling (gpt56 1a641daf): cache-mode mounts REQUIRE
        // cache_metadata_enabled — their metadata lives in the master
        // cache INDEX (dual-mode split), the CV inode tree is never
        // populated, and reads fall back to UFS.

        let data = Utils::rand_str(4096);
        let path = format!("/write_cache_{:?}/test.log", WriteType::CacheMode).into();

        // Test 1: the unified write lands in UFS and reads back intact;
        // no CV inode exists for a cache-mode path.
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string(&data).await.unwrap();
        writer.complete().await.unwrap();
        assert!(!fs.cv().exists(&path).await.unwrap());

        let (ufs_path, mnt) = fs
            .get_mount(&path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        let ufs = mnt.ufs().unwrap();
        assert!(ufs.exists(&ufs_path).await.unwrap());
        let mount_id = mnt.info.mount_id;
        let key_ufs = mnt.info.get_ufs_path(&path).unwrap();
        let key = mnt.info.get_cache_key(&key_ufs).unwrap();
        let inc = master_fs
            .cache_service
            .current_incarnation_for_mount(mount_id)
            .unwrap()
            .unwrap();

        // Test 2: cache loads (resubmits included) never modify the UFS
        // source. Loads are import-style: the UFS source path is what
        // schedules the job (a CV-path submit is rejected FileNotFound —
        // cache-mode paths have no inode).
        let mtime_before = {
            let r = ufs.open(&ufs_path).await.unwrap();
            let m = r.status().mtime;
            drop(r);
            m
        };
        fs.async_cache(&ufs_path).unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();
        fs.async_cache(&ufs_path).unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();
        let mtime_after = {
            let r = ufs.open(&ufs_path).await.unwrap();
            let m = r.status().mtime;
            drop(r);
            m
        };
        assert_eq!(
            mtime_before, mtime_after,
            "cache loads (resubmits included) must not touch the UFS source"
        );

        // Test 3: the load committed an index entry; the typed free
        // expires it (the index-world replacement of the old delete-CV
        // expiry simulation); reads keep serving the content from UFS.
        assert!(
            master_fs
                .cache_service
                .get(inc, &key, false)
                .unwrap()
                .is_some(),
            "the load must commit an index entry"
        );
        fs.free(&path, false).await.unwrap();
        assert!(
            master_fs
                .cache_service
                .get(inc, &key, false)
                .unwrap()
                .is_none(),
            "free must expire the index entry"
        );
        let mut reader = fs.open(&path).await.unwrap();
        assert_eq!(reader.read_as_string().await.unwrap(), data);
    });
}

#[test]
fn test_cache_mode_recaches_after_lost_worker_cleanup() {
    let testing = Testing::builder()
        .workers(2)
        // The first worker remains alive in this in-process test, but the
        // master treats it as lost. The next heartbeat leaves enough time for
        // the cache reload while still allowing the mini-cluster to start.
        .mutate_conf(|conf| {
            // Capability ruling (gpt56 1a641daf): cache-mode mounts REQUIRE
            // the cache metadata capability.
            conf.master.cache_metadata_enabled = true;
            conf.master.heartbeat_interval = "10s".to_string();
            conf.master.worker_blacklist_interval = "20s".to_string();
            conf.master.worker_lost_interval = "30s".to_string();
        })
        .build()
        .unwrap();
    let ufs_base = testing.ufs_path.clone();
    let cluster = testing.start_cluster().unwrap();
    let master = cluster.get_active_master_fs();
    let rt = Arc::new(AsyncRuntime::single());
    let fs = testing.get_unified_fs_with_rt(rt.clone()).unwrap();

    rt.block_on(async move {
        let mount_dir = "cache_mode_lost_worker_recache";
        let cv_path = Path::from_str(format!("/{mount_dir}/data.log")).unwrap();
        let ufs_path = Path::from_str(format!("{ufs_base}/{mount_dir}")).unwrap();
        let opts = MountOptionsBuilder::new()
            .write_type(WriteType::CacheMode)
            .replicas(1)
            .build();
        let ufs = UfsFileSystem::new(&ufs_path, opts.add_properties.clone(), None).unwrap();
        ufs.mkdir(&ufs_path, true).await.unwrap();
        fs.mount(
            &ufs_path,
            &Path::from_str(format!("/{mount_dir}")).unwrap(),
            opts,
        )
        .await
        .unwrap();

        let (source_path, mount) = fs
            .get_mount(&cv_path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        let data = "cache data is re-admitted after the last replica is lost";
        let mut writer = mount
            .ufs()
            .unwrap()
            .create(&source_path, true)
            .await
            .unwrap();
        writer.write_string(data.to_string()).await.unwrap();
        writer.complete().await.unwrap();

        // Import-style load (the UFS source schedules the job — the
        // cache-mode CV path has no inode).
        fs.async_cache(&source_path).unwrap();
        fs.wait_job_complete(&cv_path, false).await.unwrap();

        let mount_id = mount.info.mount_id;
        let key_ufs = mount.info.get_ufs_path(&cv_path).unwrap();
        let key = mount.info.get_cache_key(&key_ufs).unwrap();
        let inc = master
            .cache_service
            .current_incarnation_for_mount(mount_id)
            .unwrap()
            .unwrap();
        let serving = master
            .cache_service
            .get(inc, &key, true)
            .unwrap()
            .expect("the load must commit a serving entry");
        let lost_worker_id = serving.blocks[0].workers[0].worker_id;

        // Match the lost-worker sequence: remove the worker from
        // placement, then purge its cache session — the index-world
        // equivalent of clearing its block locations at the master.
        assert!(master
            .worker_manager
            .write()
            .remove_expired_worker(lost_worker_id)
            .is_some());
        master
            .cache_service
            .purge_worker_cache_session(lost_worker_id);

        // 4d.2 R9-1: the purged worker's replicas are no longer served —
        // with the last replica gone the whole object is a miss — while
        // the row itself stays Valid (retirement is the reconcile path's
        // job, never a silent downgrade).
        assert!(
            master.cache_service.get(inc, &key, true).unwrap().is_none(),
            "a purged session must not serve replicas"
        );
        assert!(
            master
                .cache_service
                .get(inc, &key, false)
                .unwrap()
                .is_some(),
            "the row itself must stay Valid after the session purge"
        );

        // Re-admission: the typed free tombstones the dead-serving entry
        // and a fresh load re-serves the key. (Automatic reconcile-driven
        // retirement is covered by the 4d master-lib tests; the E2E seam
        // proves the user-visible recovery path.)
        fs.free(&cv_path, false).await.unwrap();
        assert!(
            master
                .cache_service
                .get(inc, &key, false)
                .unwrap()
                .is_none(),
            "free must tombstone the entry before the reload"
        );
        fs.async_cache(&source_path).unwrap();
        fs.wait_job_complete(&cv_path, false).await.unwrap();
        let recached = master
            .cache_service
            .get(inc, &key, true)
            .unwrap()
            .expect("the reload must re-serve the key");
        assert_ne!(
            recached.blocks[0].workers[0].worker_id, lost_worker_id,
            "the reload must place replicas on a live worker"
        );

        // The data is intact end-to-end.
        let mut reader = fs.open(&cv_path).await.unwrap();
        assert_eq!(reader.read_as_string().await.unwrap(), data);
    });
}

#[test]
fn test_cache_mode_mount_requires_capability_default_off() {
    // Capability ruling (gpt56 1a641daf): a visible cache-mode mount
    // MUST have an authoritative current incarnation, and the
    // incarnation lifecycle is a default-off capability — so on a
    // default cluster a CacheMode mount fails CLOSED (never a silent
    // incarnation-less legacy mount), while an FsMode mount on the very
    // same cluster is unaffected.
    let fs = get_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let ufs_base = env::var("UFS_TEST_PATH").unwrap();
        let base = Path::from_str(format!("{}/capability_gate", ufs_base)).unwrap();
        let ufs = UfsFileSystem::new(&base, HashMap::new(), None).unwrap();
        ufs.mkdir(&base, true).await.unwrap();

        // CacheMode on the default (capability-off) cluster: fail-closed.
        let cache_root = Path::from_str("/capability_gate_cache").unwrap();
        let cache_ufs = Path::from_str(format!("{ufs_base}/capability_gate/cache")).unwrap();
        let err = fs
            .mount(
                &cache_ufs,
                &cache_root,
                MountOptionsBuilder::new()
                    .write_type(WriteType::CacheMode)
                    .access_mode(AccessMode::ReadWrite)
                    .build(),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("cache metadata capability is disabled"),
            "cache-mode mount must fail closed on a default cluster, got: {}",
            err
        );

        // FsMode on the SAME cluster: fully unaffected.
        let fs_root = Path::from_str("/capability_gate_fs").unwrap();
        let fs_ufs = Path::from_str(format!("{ufs_base}/capability_gate/fs")).unwrap();
        fs.mount(
            &fs_ufs,
            &fs_root,
            MountOptionsBuilder::new()
                .write_type(WriteType::FsMode)
                .build(),
        )
        .await
        .unwrap();

        let path = Path::from_str("/capability_gate_fs/data.log").unwrap();
        let data = "fs-mode is unaffected by the cache capability gate";
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string(data.to_string()).await.unwrap();
        writer.complete().await.unwrap();
        let mut reader = fs.open(&path).await.unwrap();
        assert_eq!(reader.read_as_string().await.unwrap(), data);
    });
}

#[test]
fn test_fs_mode() {
    let fs = get_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::FsMode).await;
        let path = format!("/write_cache_{:?}/test.log", WriteType::FsMode).into();
        write(&fs, &path, false).await;

        let (_, mnt) = fs
            .get_mount(&path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();

        // Test rename
        let path = format!("/write_cache_{:?}/meta.log", WriteType::FsMode).into();
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string(Utils::rand_str(1024)).await.unwrap();
        writer.complete().await.unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();

        let dst_path = format!("/write_cache_{:?}/meta_rename.log", WriteType::FsMode).into();
        fs.rename(&path, &dst_path).await.unwrap();

        // FsMode rename updates CV first; UFS rename follows journal apply (may lag one tick).
        // Single-shot open fails with NotFound (see runtime: Rename ok then UFS stat NotFound).
        wait_for_cv_ufs_consistency(&fs, &dst_path).await;

        // Test delete
        let ufs_path = mnt.get_ufs_path(&dst_path).unwrap();
        fs.delete(&dst_path, false).await.unwrap();
        let mut ufs_gone = awaitility::at_most(Duration::from_secs(60));
        ufs_gone.poll_interval(Duration::from_millis(100));
        ufs_gone
            .until_async(|| async { !mnt.ufs().unwrap().exists(&ufs_path).await.unwrap_or(true) })
            .await;
        ufs_gone
            .result()
            .expect("UFS should not list deleted file after journal delete");
    });
}

#[test]
fn test_cache_mode_free() {
    let (fs, cluster) = get_fs_cache_meta();
    let master_fs = cluster.get_active_master_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::CacheMode).await;

        let data = Utils::rand_str(1024);

        let path = format!(
            "/write_cache_{:?}/test_cache_mode_free.log",
            WriteType::CacheMode
        )
        .into();
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string(&data).await.unwrap();
        writer.complete().await.unwrap();

        let (ufs_path, mnt) = fs
            .get_mount(&path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        assert!(mnt.ufs().unwrap().exists(&ufs_path).await.unwrap());
        assert!(
            !fs.cv().exists(&path).await.unwrap(),
            "cache-mode metadata lives in the cache index, not the CV inode tree"
        );

        // Import-style cache load (UFS source): with cache metadata
        // enabled the dual-mode split tracks cache-mode files in the
        // master cache INDEX only — a load submitted with the CV path is
        // rejected FileNotFound because no inode exists, so the load is
        // sourced from the mount-mapped UFS path exactly as a cache miss
        // read would.
        fs.async_cache(&ufs_path).unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();

        // The same key derivation the master uses for the typed free
        // (mount-relative cache key of the mount-mapped UFS path).
        let mount_id = mnt.info.mount_id;
        let key_ufs = mnt.info.get_ufs_path(&path).unwrap();
        let key = mnt.info.get_cache_key(&key_ufs).unwrap();
        let inc = master_fs
            .cache_service
            .current_incarnation_for_mount(mount_id)
            .unwrap()
            .unwrap();
        assert!(
            master_fs
                .cache_service
                .get(inc, &key, false)
                .unwrap()
                .is_some(),
            "the write-through mirror must have committed an index entry before the free"
        );

        // Free bridge (task #6, gpt56 `961e17b5` P0 + `6e4a5599`): the
        // PUBLIC free on an interior cache-mode path is unconditionally
        // `FsClient::free` — Free on the wire — and the MASTER routes it
        // to the typed Key free. The Unified/SDK layer NEVER deletes CV
        // inodes for a free. The bridge exposes no exact inode/byte
        // stats (response-loss replay would under-report, gpt56
        // `8336e8a8`), so the seam observes the INDEX directly.
        fs.free(&path, false).await.unwrap();

        // The typed free tombstoned the entry: the point read now misses.
        assert!(
            master_fs
                .cache_service
                .get(inc, &key, false)
                .unwrap()
                .is_none(),
            "free must remove the index entry"
        );
        // Never an inode delete, and UFS is untouched.
        assert!(
            !fs.cv().exists(&path).await.unwrap(),
            "free must not create or delete CV inodes"
        );
        assert!(
            mnt.ufs().unwrap().exists(&ufs_path).await.unwrap(),
            "cache mode free must not delete the UFS file"
        );

        // Idempotent re-free of the same exact key: the typed tombstones
        // make the second walk a no-op that still succeeds.
        fs.free(&path, false).await.unwrap();

        // The file is still readable end-to-end after the free (straight
        // from UFS — the content is the contract).
        let mut reader = fs.open(&path).await.unwrap();
        assert_eq!(reader.read_as_string().await.unwrap(), data);
    });
}

#[test]
fn test_cache_mode_free_child_with_unified_disabled() {
    let (mut fs, cluster) = get_fs_cache_meta();
    let master_fs = cluster.get_active_master_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::CacheMode).await;

        let parent = Path::from_str("/write_cache_CacheMode/cache_only_free").unwrap();
        fs.mkdir(&parent, true).await.unwrap();
        let path = Path::from_str("/write_cache_CacheMode/cache_only_free/child.log").unwrap();
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string("cache-only free").await.unwrap();
        writer.complete().await.unwrap();
        let (child_ufs, _) = fs
            .get_mount(&path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        fs.async_cache(&child_ufs).unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();

        // A second file directly under the mount root: the PREFIX free
        // below must NOT touch its entry; the ROOT free afterwards must.
        let sibling = Path::from_str("/write_cache_CacheMode/cache_only_root_free.log").unwrap();
        let mut writer = fs.create(&sibling, true).await.unwrap();
        writer.write_string("cache-only root free").await.unwrap();
        writer.complete().await.unwrap();
        let (sibling_ufs, _) = fs
            .get_mount(&sibling, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        fs.async_cache(&sibling_ufs).unwrap();
        fs.wait_job_complete(&sibling, false).await.unwrap();

        let (ufs_path, mount) = fs
            .get_mount(&path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        let mount_id = mount.info.mount_id;
        let child_key = {
            let ufs = mount.info.get_ufs_path(&path).unwrap();
            mount.info.get_cache_key(&ufs).unwrap()
        };
        let sibling_key = {
            let ufs = mount.info.get_ufs_path(&sibling).unwrap();
            mount.info.get_cache_key(&ufs).unwrap()
        };
        let inc = master_fs
            .cache_service
            .current_incarnation_for_mount(mount_id)
            .unwrap()
            .unwrap();
        assert!(
            master_fs
                .cache_service
                .get(inc, &child_key, false)
                .unwrap()
                .is_some()
                && master_fs
                    .cache_service
                    .get(inc, &sibling_key, false)
                    .unwrap()
                    .is_some(),
            "both write-through mirrors must have committed index entries before the frees"
        );

        // Unified DISABLED (the old free path branched on the local mount
        // snapshot here): the unconditional `cv.free` still sends Free
        // and the master still routes to the typed bridge.
        fs.disable_unified();

        // Interior PREFIX free (recursive over the parent directory):
        // the child entry dies, the sibling outside the prefix survives.
        fs.free(&parent, true).await.unwrap();
        assert!(
            master_fs
                .cache_service
                .get(inc, &child_key, false)
                .unwrap()
                .is_none(),
            "prefix free must remove the child index entry"
        );
        assert!(
            master_fs
                .cache_service
                .get(inc, &sibling_key, false)
                .unwrap()
                .is_some(),
            "prefix free must not touch an entry outside the prefix"
        );
        assert!(mount.ufs().unwrap().exists(&ufs_path).await.unwrap());
        assert!(
            !fs.cv().exists(&path).await.unwrap(),
            "free must not create or delete CV inodes"
        );

        // ROOT free (the mount root itself): the typed Mount scope of the
        // whole current incarnation — the sibling entry dies, its UFS
        // file stays.
        let root = Path::from_str("/write_cache_CacheMode").unwrap();
        fs.free(&root, true).await.unwrap();
        assert!(
            master_fs
                .cache_service
                .get(inc, &sibling_key, false)
                .unwrap()
                .is_none(),
            "root free must remove the sibling index entry"
        );
        assert!(mount.ufs().unwrap().exists(&sibling_ufs).await.unwrap());
    });
}

/// P4-1 (gpt56 `88cda9cf`): the public cache_status/invalidate_cache pair
/// against a live capability-on cluster. Proves the typed public results
/// (Hit/Miss, Applied/AlreadyApplied/Miss), the composite identity CAS
/// (invalidate fences exactly the OBSERVED object), and the raw-binding
/// response-loss contract (an identical re-send classifies AlreadyApplied
/// and changes nothing).
#[test]
fn test_cache_mode_status_and_invalidate() {
    let (fs, cluster) = get_fs_cache_meta();
    let master_fs = cluster.get_active_master_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::CacheMode).await;

        let data = Utils::rand_str(2048);
        let path: Path = format!("/write_cache_{:?}/p41_status.log", WriteType::CacheMode).into();
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string(&data).await.unwrap();
        writer.complete().await.unwrap();

        let (ufs_path, mnt) = fs
            .get_mount(&path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        fs.async_cache(&ufs_path).unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();

        // The status observation must agree with the authoritative index.
        let key = {
            let ufs = mnt.info.get_ufs_path(&path).unwrap();
            mnt.info.get_cache_key(&ufs).unwrap()
        };
        let inc = master_fs
            .cache_service
            .current_incarnation_for_mount(mnt.info.mount_id)
            .unwrap()
            .unwrap();
        let entry = master_fs
            .cache_service
            .get(inc, &key, false)
            .unwrap()
            .unwrap();

        let (object_id, observed_len, generation) = match fs.cache_status(&path).await.unwrap() {
            CacheEntryStatus::Hit {
                object_id,
                len,
                generation,
                ..
            } => (object_id, len, generation),
            other => panic!("expected Hit, got {:?}", other),
        };
        assert_eq!(object_id, entry.object_id);
        assert_eq!(observed_len, entry.len);
        assert_eq!(generation, entry.generation);

        // Composite invalidate: fences exactly the OBSERVED identity.
        assert_eq!(
            fs.invalidate_cache(&path).await.unwrap(),
            CacheInvalidateResult::Applied
        );
        assert!(
            master_fs
                .cache_service
                .get(inc, &key, false)
                .unwrap()
                .is_none(),
            "invalidate must tombstone the index entry"
        );
        assert!(
            mnt.ufs().unwrap().exists(&ufs_path).await.unwrap(),
            "invalidate is metadata-only; the UFS file survives"
        );

        // Response-loss contract at the binding level: an IDENTICAL raw
        // re-send of the applied invalidate classifies AlreadyApplied
        // (state changed once; the replay is a no-op).
        let replay = fs
            .cv()
            .cache_invalidate(inc, &key, generation, object_id)
            .await
            .unwrap();
        assert_eq!(replay.status, Some(2), "replay must be ALREADY_APPLIED");
        assert!(
            master_fs
                .cache_service
                .get(inc, &key, false)
                .unwrap()
                .is_none(),
            "replay must not resurrect or change anything"
        );

        // The raw CacheRemove binding is wire-complete but is the SAME
        // logical fence (alias of invalidate; physical vacuum is P4-3).
        let remove_replay = fs
            .cv()
            .cache_remove(inc, &key, generation, object_id)
            .await
            .unwrap();
        assert_eq!(remove_replay.status, Some(2));

        // Public second invalidate under the ACTIVE incarnation: the Get
        // observes a miss, so the composite reports Miss — nothing to do.
        assert_eq!(
            fs.invalidate_cache(&path).await.unwrap(),
            CacheInvalidateResult::Miss
        );
        assert_eq!(
            fs.cache_status(&path).await.unwrap(),
            CacheEntryStatus::Miss
        );

        // Content survives end-to-end.
        let mut reader = fs.open(&path).await.unwrap();
        assert_eq!(reader.read_as_string().await.unwrap(), data);
    });
}

/// P4-1 stale-incarnation seams (gpt56 `88cda9cf`):
/// - a stale snapshot fences the public Get, which force-refreshes ONCE
///   and re-resolves the same path (never folds the fence into a miss);
/// - a mutation carrying a pre-rebuild observation is TERMINAL (typed
///   CacheIncarnationFenced) and leaves the rebuilt incarnation at zero
///   changes;
/// - a vanished mount after the refresh is loud.
#[test]
fn test_cache_fence_refresh_and_terminal_invalidate() {
    let (fs, cluster) = get_fs_cache_meta();
    let master_fs = cluster.get_active_master_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::CacheMode).await;

        let data = Utils::rand_str(512);
        let cv_root = Path::from_str("/write_cache_CacheMode").unwrap();
        let path: Path = format!("/write_cache_{:?}/p41_fence.log", WriteType::CacheMode).into();
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string(&data).await.unwrap();
        writer.complete().await.unwrap();

        let (ufs_path, mnt) = fs
            .get_mount(&path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        fs.async_cache(&ufs_path).unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();

        // Observe under the pre-rebuild incarnation.
        let key = {
            let ufs = mnt.info.get_ufs_path(&path).unwrap();
            mnt.info.get_cache_key(&ufs).unwrap()
        };
        let inc1 = master_fs
            .cache_service
            .current_incarnation_for_mount(mnt.info.mount_id)
            .unwrap()
            .unwrap();
        let (object_id, generation) = match fs.cache_status(&path).await.unwrap() {
            CacheEntryStatus::Hit {
                object_id,
                generation,
                ..
            } => (object_id, generation),
            other => panic!("expected Hit under inc {}, got {:?}", inc1, other),
        };
        let ufs_base = env::var("UFS_TEST_PATH").unwrap();
        let ufs_mount_path = Path::from_str(format!("{}/write_cache_CacheMode", ufs_base)).unwrap();

        // Rebuild the mount THROUGH THE RAW CLIENT ONLY: the unified
        // mount cache keeps its stale (inc1) snapshot.
        fs.cv().umount(&cv_root).await.unwrap();
        let opts = MountOptionsBuilder::new()
            .write_type(WriteType::CacheMode)
            .access_mode(AccessMode::ReadWrite)
            .build();
        fs.cv()
            .mount(&ufs_mount_path, &cv_root, opts)
            .await
            .unwrap();
        let inc2_raw = master_fs
            .cache_service
            .current_incarnation_for_mount(mnt.info.mount_id)
            .unwrap();
        assert!(
            inc2_raw.is_none() || inc2_raw.unwrap() != inc1,
            "the rebuilt mount must not still be on the pre-rebuild incarnation"
        );

        // Mutation with the PRE-REBUILD observation: terminal typed FENCE,
        // never a silent success and never a re-resolve.
        let err = fs
            .cv()
            .cache_invalidate(inc1, &key, generation, object_id)
            .await
            .unwrap_err();
        assert!(
            matches!(err.kind(), curvine_error::ErrorKind::CacheIncarnationFenced),
            "stale-inc mutation must be the typed FENCED terminal, got {:?}",
            err
        );

        // Resolve the rebuilt mount's id/incarnation WITHOUT touching the
        // unified mount cache (a RAW CV table read — the snapshot must
        // stay stale for the seam below), then re-load under inc2.
        let table = fs.cv().get_mount_table().await.unwrap();
        let mnt2 = table
            .iter()
            .find(|m| m.cv_path == "/write_cache_CacheMode")
            .expect("rebuilt mount must be in the table");
        let inc2 = master_fs
            .cache_service
            .current_incarnation_for_mount(mnt2.mount_id)
            .unwrap()
            .unwrap();
        assert_ne!(inc2, inc1);
        fs.async_cache(&ufs_path).unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();
        assert!(
            master_fs
                .cache_service
                .get(inc2, &key, false)
                .unwrap()
                .is_some(),
            "the reload must commit into the rebuilt incarnation"
        );

        // The old-inc mutation stays terminal against the POPULATED new
        // incarnation and changes NOTHING there.
        let err = fs
            .cv()
            .cache_invalidate(inc1, &key, generation, object_id)
            .await
            .unwrap_err();
        assert!(
            matches!(err.kind(), curvine_error::ErrorKind::CacheIncarnationFenced),
            "old-inc mutation against a populated new incarnation must stay terminal"
        );
        assert!(
            master_fs
                .cache_service
                .get(inc2, &key, false)
                .unwrap()
                .is_some(),
            "the terminal old-inc mutation must leave the new incarnation at zero changes"
        );

        // REGRESSION SEAM (gpt56 694593c1 P0): with the snapshot STILL
        // stale (no cache_status called first), the DIRECT public
        // invalidate must fence its Get, refresh once, and then Apply the
        // mutation against the SAME refreshed observation — Applied, and
        // only the NEW incarnation changed. (The shadowed-binding bug
        // failed here with a terminal FENCED error instead.)
        assert_eq!(
            fs.invalidate_cache(&path).await.unwrap(),
            CacheInvalidateResult::Applied
        );
        assert!(
            master_fs
                .cache_service
                .get(inc2, &key, false)
                .unwrap()
                .is_none(),
            "the public invalidate must tombstone the entry under the new incarnation"
        );
        assert_eq!(
            fs.cache_status(&path).await.unwrap(),
            CacheEntryStatus::Miss
        );

        // Vanished mount after the refresh: loud, never a miss.
        fs.cv().umount(&cv_root).await.unwrap();
        let err = fs.cache_status(&path).await.unwrap_err();
        assert!(
            format!("{:?}", err).contains("no mount covers"),
            "vanished mount must fail loud, got {:?}",
            err
        );
    });
}

/// P4-2 D5 read path (gpt56 `c1d51e75`): a cache-mode STRICT INTERIOR read
/// routes CacheGet(need_locations=true) and, on a fully valid hit, serves
/// the object from cache blocks via a CACHE-ONLY reader — never a
/// per-read UFS fallback. RED seam: with the UFS source deleted after the
/// load, open/get_status must still serve the cached content and entry
/// metadata (today both fall back to UFS and fail FileNotFound).
#[test]
fn test_cache_read_d5_strict_interior_cache_only() {
    let (fs, cluster) = get_fs_cache_meta();
    let master_fs = cluster.get_active_master_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::CacheMode).await;

        let data = Utils::rand_str(8192);
        let path: Path = format!("/write_cache_{:?}/p42_read.log", WriteType::CacheMode).into();
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string(&data).await.unwrap();
        writer.complete().await.unwrap();

        let (ufs_path, mnt) = fs
            .get_mount(&path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        fs.async_cache(&ufs_path).unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();

        let key = {
            let ufs = mnt.info.get_ufs_path(&path).unwrap();
            mnt.info.get_cache_key(&ufs).unwrap()
        };
        let inc = master_fs
            .cache_service
            .current_incarnation_for_mount(mnt.info.mount_id)
            .unwrap()
            .unwrap();
        assert!(
            master_fs
                .cache_service
                .get(inc, &key, true)
                .unwrap()
                .is_some(),
            "the load must commit a complete location set"
        );

        // Remove the UFS source: any UFS fallback now fails. The read and
        // status must come from the cache index/blocks alone.
        mnt.ufs().unwrap().delete(&ufs_path, false).await.unwrap();
        assert!(!mnt.ufs().unwrap().exists(&ufs_path).await.unwrap());

        let mut reader = fs.open(&path).await.unwrap();
        assert_eq!(
            reader.read_as_string().await.unwrap(),
            data,
            "strict-interior read must serve the whole object from cache blocks"
        );

        let status = fs.get_status(&path).await.unwrap();
        assert_eq!(status.len, data.len() as i64);
        assert!(!status.is_dir);
    });
}

/// P4-2 D5 fence seams (gpt56 `c1d51e75`): the read-path Get obeys the
/// one-refresh FENCED policy —
/// - a STALE snapshot (raw umount+remount, snapshot still on the dead
///   incarnation) fences the Get, refreshes once, re-resolves onto the
///   new incarnation, and the miss falls back to the UFS read;
/// - a VANISHED mount (raw umount after the refresh) makes open loud
///   ("no mount covers"), never a silent UFS fallback under a dead
///   namespace; the same holds for get_status after re-arming the
///   snapshot via a unified remount.
#[test]
fn test_cache_read_d5_fence_refresh_and_vanished_loud() {
    let (fs, cluster) = get_fs_cache_meta();
    let _master_fs = cluster.get_active_master_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::CacheMode).await;

        let data = Utils::rand_str(2048);
        let cv_root = Path::from_str("/write_cache_CacheMode").unwrap();
        let path: Path = format!("/write_cache_{:?}/p42_fence.log", WriteType::CacheMode).into();
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string(&data).await.unwrap();
        writer.complete().await.unwrap();

        let (ufs_path, _mnt) = fs
            .get_mount(&path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        fs.async_cache(&ufs_path).unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();

        let ufs_base = env::var("UFS_TEST_PATH").unwrap();
        let ufs_mount_path = Path::from_str(format!("{}/write_cache_CacheMode", ufs_base)).unwrap();
        let opts = MountOptionsBuilder::new()
            .write_type(WriteType::CacheMode)
            .access_mode(AccessMode::ReadWrite)
            .build();

        // STALE snapshot: raw umount + remount leaves the client on the
        // dead incarnation (no unified call refreshes it in between).
        fs.cv().umount(&cv_root).await.unwrap();
        fs.cv()
            .mount(&ufs_mount_path, &cv_root, opts.clone())
            .await
            .unwrap();
        // The Get fences once, refreshes, re-resolves onto the NEW
        // incarnation, observes a miss, and the UFS read proceeds.
        let mut reader = fs.open(&path).await.unwrap();
        assert_eq!(reader.read_as_string().await.unwrap(), data);

        // VANISHED mount: the snapshot (refreshed above to the rebuilt
        // mount) goes stale again via a raw umount; open must refresh
        // once and then fail LOUD — never a silent UFS fallback.
        fs.cv().umount(&cv_root).await.unwrap();
        let err = match fs.open(&path).await {
            Err(e) => e,
            Ok(_) => panic!("vanished mount after the one refresh must fail loud"),
        };
        assert!(
            format!("{:?}", err).contains("no mount covers"),
            "vanished mount after the one refresh must fail loud, got {:?}",
            err
        );

        // Re-arm the snapshot (unified remount force-refreshes the client
        // table), then vanish it again: get_status must be loud too.
        fs.mount(&ufs_mount_path, &cv_root, opts).await.unwrap();
        fs.cv().umount(&cv_root).await.unwrap();
        let err = match fs.get_status(&path).await {
            Err(e) => e,
            Ok(_) => panic!("vanished mount must gate get_status loud"),
        };
        assert!(
            format!("{:?}", err).contains("no mount covers"),
            "vanished mount must gate get_status loud, got {:?}",
            err
        );
    });
}

/// P4-2 same-observation mount seam (gpt56 `e53671d1` P0): a raw remount
/// that swaps the UFS TARGET while the client snapshot still holds the
/// dead incarnation must fence, refresh, and — on the resulting miss —
/// fall back to the NEW mount's UFS. Pairing the refreshed Get with the
/// stale outer resolution (the old UFS) is exactly the bug class this
/// test forbids.
#[test]
fn test_cache_read_d5_remount_swap_miss_falls_to_new_ufs() {
    let (fs, _cluster) = get_fs_cache_meta();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::CacheMode).await;
        let opts = MountOptionsBuilder::new()
            .write_type(WriteType::CacheMode)
            .access_mode(AccessMode::ReadWrite)
            .build();

        let old_data = Utils::rand_str(2048);
        let cv_root = Path::from_str("/write_cache_CacheMode").unwrap();
        let path: Path = format!("/write_cache_{:?}/p42_swap.log", WriteType::CacheMode).into();
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string(&old_data).await.unwrap();
        writer.complete().await.unwrap();

        // Stage a SECOND UFS target via its own mount: same-named file,
        // different content (and length, so status also discriminates).
        let new_data = "swapped-target".to_string();
        let ufs_base = env::var("UFS_TEST_PATH").unwrap();
        let swap_dir = Path::from_str(format!("{}/write_cache_CacheMode_swap", ufs_base)).unwrap();
        let swap_cv = Path::from_str("/write_cache_CacheMode_swap").unwrap();
        let ufs = UfsFileSystem::new(&swap_dir, opts.add_properties.clone(), None).unwrap();
        ufs.mkdir(&swap_dir, true).await.unwrap();
        fs.mount(&swap_dir, &swap_cv, opts.clone()).await.unwrap();
        let swap_file: Path = "/write_cache_CacheMode_swap/p42_swap.log".into();
        let mut writer = fs.create(&swap_file, true).await.unwrap();
        writer.write_string(&new_data).await.unwrap();
        writer.complete().await.unwrap();

        // Raw umount + remount of the ORIGINAL cv root onto the SWAP
        // target (the staging mount gives the UFS up first — one UFS
        // cannot sit at two cv roots): the client snapshot stays on the
        // dead incarnation, the next Get fences.
        fs.cv().umount(&cv_root).await.unwrap();
        fs.cv().umount(&swap_cv).await.unwrap();
        fs.cv().mount(&swap_dir, &cv_root, opts).await.unwrap();

        // Fence → one refresh → re-resolve onto the swap mount → miss →
        // the fallback MUST read the swap target's file, never the stale
        // outer resolution's.
        let mut reader = fs.open(&path).await.unwrap();
        assert_eq!(
            reader.read_as_string().await.unwrap(),
            new_data,
            "the miss must fall back to the SAME-OBSERVATION (refreshed) mount's UFS"
        );

        let status = fs.get_status(&path).await.unwrap();
        assert_eq!(
            status.len,
            new_data.len() as i64,
            "the status miss must take the refreshed mount's UFS too"
        );
    });
}

/// P4-2 D5 core matrix (gpt56 `fd02f578`): with `read_verify_ufs=true`
/// the ONE UFS stat observation binds len + mtime — a length drift or an
/// mtime drift demotes the hit to a whole-object miss and the read
/// serves the CURRENT UFS content; the ttl expiry boundary demotes the
/// same way (and without verification only expiry is consulted); a
/// valid hit's synthesized public status is a usable UFS-like namespace
/// stat (id = UFS_INODE_ID, readable regular-file mode), never the
/// internal cache object id with mode 0.
#[test]
fn test_cache_read_d5_verify_matrix() {
    let (fs, _cluster) = get_fs_cache_meta();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let ufs_base = env::var("UFS_TEST_PATH").unwrap();

        // Main matrix mount: verification ON, long TTL.
        let d5_dir = Path::from_str(format!("{}/write_cache_CacheMode_d5", ufs_base)).unwrap();
        let d5_root = Path::from_str("/write_cache_CacheMode_d5").unwrap();
        let d5_opts = MountOptionsBuilder::new()
            .write_type(WriteType::CacheMode)
            .access_mode(AccessMode::ReadWrite)
            .read_verify_ufs(true)
            .ttl_ms(600_000)
            .build();
        let ufs = UfsFileSystem::new(&d5_dir, d5_opts.add_properties.clone(), None).unwrap();
        ufs.mkdir(&d5_dir, true).await.unwrap();
        fs.mount(&d5_dir, &d5_root, d5_opts).await.unwrap();

        // Short-TTL mount with verification OFF: only expiry is consulted.
        let ttl_dir = Path::from_str(format!("{}/write_cache_CacheMode_d5ttl", ufs_base)).unwrap();
        let ttl_root = Path::from_str("/write_cache_CacheMode_d5ttl").unwrap();
        let ttl_opts = MountOptionsBuilder::new()
            .write_type(WriteType::CacheMode)
            .access_mode(AccessMode::ReadWrite)
            .ttl_ms(2000)
            .build();
        let ufs = UfsFileSystem::new(&ttl_dir, ttl_opts.add_properties.clone(), None).unwrap();
        ufs.mkdir(&ttl_dir, true).await.unwrap();
        fs.mount(&ttl_dir, &ttl_root, ttl_opts).await.unwrap();

        let (_, d5_mount) = fs
            .get_mount(&d5_root, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        let (_, ttl_mount) = fs
            .get_mount(&ttl_root, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        // LEN drift: hit metadata says 4KB, the UFS file now holds 6KB —
        // whole-object miss, the read serves the current UFS content.
        let len_a = Utils::rand_str(4096);
        let len_b = Utils::rand_str(6144);
        let len_cv: Path = "/write_cache_CacheMode_d5/drift_len.log".into();
        let mut writer = fs.create(&len_cv, true).await.unwrap();
        writer.write_string(&len_a).await.unwrap();
        writer.complete().await.unwrap();
        fs.async_cache(&d5_mount.get_ufs_path(&len_cv).unwrap())
            .unwrap();
        fs.wait_job_complete(&len_cv, false).await.unwrap();
        rewrite_ufs(&d5_mount.info, &len_cv, &len_b).await;
        let mut reader = fs.open(&len_cv).await.unwrap();
        assert_eq!(
            reader.read_as_string().await.unwrap(),
            len_b,
            "len drift must demote the hit to a whole-object miss onto the current UFS"
        );

        // MTIME drift (same length, different bytes): the single stat
        // observation's mtime binds the entry — miss onto the UFS.
        let mt_a = Utils::rand_str(4096);
        let mt_b = Utils::rand_str(4096);
        assert_ne!(mt_a, mt_b);
        let mt_cv: Path = "/write_cache_CacheMode_d5/drift_mtime.log".into();
        let mut writer = fs.create(&mt_cv, true).await.unwrap();
        writer.write_string(&mt_a).await.unwrap();
        writer.complete().await.unwrap();
        fs.async_cache(&d5_mount.get_ufs_path(&mt_cv).unwrap())
            .unwrap();
        fs.wait_job_complete(&mt_cv, false).await.unwrap();
        // Guarantee an mtime delta (millisecond-resolution filesystems).
        tokio::time::sleep(Duration::from_millis(50)).await;
        rewrite_ufs(&d5_mount.info, &mt_cv, &mt_b).await;
        let mut reader = fs.open(&mt_cv).await.unwrap();
        assert_eq!(
            reader.read_as_string().await.unwrap(),
            mt_b,
            "mtime drift (same len) must demote the hit to a whole-object miss"
        );

        // EXPIRY boundary (verification OFF — only expiry is consulted):
        // before expiry the hit serves with the UFS source DELETED; after
        // the ttl the entry demotes and the (restored) UFS serves.
        let ttl_data = Utils::rand_str(2048);
        let ttl_cv: Path = "/write_cache_CacheMode_d5ttl/expiry.log".into();
        let mut writer = fs.create(&ttl_cv, true).await.unwrap();
        writer.write_string(&ttl_data).await.unwrap();
        writer.complete().await.unwrap();
        fs.async_cache(&ttl_mount.get_ufs_path(&ttl_cv).unwrap())
            .unwrap();
        fs.wait_job_complete(&ttl_cv, false).await.unwrap();
        let ttl_ufs = ttl_mount.get_ufs_path(&ttl_cv).unwrap();
        ttl_mount
            .ufs()
            .unwrap()
            .delete(&ttl_ufs, false)
            .await
            .unwrap();
        let mut reader = fs.open(&ttl_cv).await.unwrap();
        assert_eq!(
            reader.read_as_string().await.unwrap(),
            ttl_data,
            "unexpired hit must serve from cache with the UFS source gone"
        );
        // Restore the UFS source, cross the ttl boundary, demote to miss.
        rewrite_ufs(&ttl_mount.info, &ttl_cv, &ttl_data).await;
        tokio::time::sleep(Duration::from_millis(2500)).await;
        let mut reader = fs.open(&ttl_cv).await.unwrap();
        assert_eq!(
            reader.read_as_string().await.unwrap(),
            ttl_data,
            "expired entry must demote to the UFS read"
        );

        // Synthesized public status (valid hit): a UFS-like namespace
        // stat — UFS_INODE_ID (never the cache object id) and the
        // regular-file mode convention, so a FUSE permission check on a
        // hit cannot see 000/EACCES.
        let st_data = Utils::rand_str(4096);
        let st_cv: Path = "/write_cache_CacheMode_d5/status.log".into();
        let mut writer = fs.create(&st_cv, true).await.unwrap();
        writer.write_string(&st_data).await.unwrap();
        writer.complete().await.unwrap();
        fs.async_cache(&d5_mount.get_ufs_path(&st_cv).unwrap())
            .unwrap();
        fs.wait_job_complete(&st_cv, false).await.unwrap();
        let status = fs.get_status(&st_cv).await.unwrap();
        assert_eq!(
            status.id, UFS_INODE_ID,
            "cache object id must not leak as the namespace inode id"
        );
        assert_eq!(
            status.mode, 0o777,
            "hit status must carry the readable regular-file mode"
        );
        assert!(!status.is_dir);
        assert_eq!(status.len, st_data.len() as i64);
        assert!(status.is_complete);
        // The hit status' read-open stays usable end to end.
        let mut reader = fs.open(&st_cv).await.unwrap();
        assert_eq!(reader.read_as_string().await.unwrap(), st_data);
    });
}

/// P4-3 purge fences, seam 1 (gpt56 `2a089d5a` #1): an EXPIRED-but-live
/// row is not "no entry" — the server answers Miss, exact Invalidate can
/// never clear it, and a fresh CacheAllocate is rejected by the live row
/// forever. The overwrite's bound Key purge runs BEFORE the UFS write and
/// re-opens the key: the re-cache Allocate succeeds and the read serves
/// the NEW content (seam 5, overwrite leg).
#[test]
fn test_cache_p43_expired_row_overwrite_repurge() {
    let (fs, _cluster) = get_fs_cache_meta();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let ufs_base = env::var("UFS_TEST_PATH").unwrap();
        let dir = Path::from_str(format!("{}/write_cache_CacheMode_p43ttl", ufs_base)).unwrap();
        let root = Path::from_str("/write_cache_p43ttl").unwrap();
        let opts = MountOptionsBuilder::new()
            .write_type(WriteType::CacheMode)
            .access_mode(AccessMode::ReadWrite)
            .ttl_ms(2000)
            .build();
        let ufs = UfsFileSystem::new(&dir, opts.add_properties.clone(), None).unwrap();
        ufs.mkdir(&dir, true).await.unwrap();
        fs.mount(&dir, &root, opts).await.unwrap();

        let path: Path = "/write_cache_p43ttl/expired.log".into();
        let a = Utils::rand_str(2048);
        let mut w = fs.create(&path, true).await.unwrap();
        w.write_string(&a).await.unwrap();
        w.complete().await.unwrap();
        let (ufs_path, mnt) = fs
            .get_mount(&path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        fs.async_cache(&ufs_path).unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();
        assert!(matches!(
            fs.cache_status(&path).await.unwrap(),
            CacheEntryStatus::Hit { .. }
        ));

        // Cross the ttl: the public status answers Miss ...
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert_eq!(
            fs.cache_status(&path).await.unwrap(),
            CacheEntryStatus::Miss
        );

        // ... but the row is STILL LIVE (Miss != no entry): a re-cache
        // Allocate on the un-purged key is rejected by the live row.
        fs.async_cache(&ufs_path).unwrap();
        let err = fs.wait_job_complete(&path, false).await.unwrap_err();
        assert!(
            format!("{:?}", err).contains("live entry"),
            "the expired live row must reject the re-cache Allocate, got {:?}",
            err
        );

        // The overwrite's bound Key purge (BEFORE the UFS write) clears
        // the expired live row and re-opens the key ...
        let b = Utils::rand_str(3072);
        let mut w = fs.create(&path, true).await.unwrap();
        w.write_string(&b).await.unwrap();
        w.complete().await.unwrap();

        // ... so the re-cache Allocate now succeeds.
        fs.async_cache(&ufs_path).unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();
        match fs.cache_status(&path).await.unwrap() {
            CacheEntryStatus::Hit { len, .. } => assert_eq!(len, b.len() as i64),
            other => panic!("re-cache after the purge must hit, got {:?}", other),
        }
        // Seam 5 (overwrite): the read serves the NEW content.
        let mut r = fs.open(&path).await.unwrap();
        assert_eq!(r.read_as_string().await.unwrap(), b);
        let _ = mnt;
    });
}

/// P4-3 purge fences, seam 2 (gpt56 `2a089d5a` #2): a different-UFS-target
/// remount between the client's observation and the purge is the typed
/// FENCED terminal — the purge NEVER deletes a different incarnation than
/// the caller observed, and the UFS targets stay untouched. A cache->fs
/// switch is the same fence (a bound free never falls through to the
/// legacy inode free). After the client re-observes, the retry succeeds.
#[test]
fn test_cache_p43_remount_bound_purge_fenced() {
    let (fs, cluster) = get_fs_cache_meta();
    let master_fs = cluster.get_active_master_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let ufs_base = env::var("UFS_TEST_PATH").unwrap();
        let dir_a = Path::from_str(format!("{}/write_cache_CacheMode_p43a", ufs_base)).unwrap();
        let dir_b = Path::from_str(format!("{}/write_cache_CacheMode_p43b", ufs_base)).unwrap();
        let root = Path::from_str("/write_cache_p43").unwrap();
        let opts = MountOptionsBuilder::new()
            .write_type(WriteType::CacheMode)
            .access_mode(AccessMode::ReadWrite)
            .build();
        let ufs_a = UfsFileSystem::new(&dir_a, opts.add_properties.clone(), None).unwrap();
        ufs_a.mkdir(&dir_a, true).await.unwrap();
        fs.mount(&dir_a, &root, opts.clone()).await.unwrap();

        // Observe the mount (client snapshot binds to this incarnation).
        let path: Path = "/write_cache_p43/swap.log".into();
        let old_data = Utils::rand_str(2048);
        let mut w = fs.create(&path, true).await.unwrap();
        w.write_string(&old_data).await.unwrap();
        w.complete().await.unwrap();
        let (ufs_path, mnt) = fs
            .get_mount(&path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        fs.async_cache(&ufs_path).unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();
        let inc1 = master_fs
            .cache_service
            .current_incarnation_for_mount(mnt.info.mount_id)
            .unwrap()
            .unwrap();

        // Stage a SECOND UFS target directly (no second mount — one UFS
        // dir cannot sit at two cv roots): same-named file, different
        // content.
        let new_data = "swapped-target".to_string();
        let ufs_b = UfsFileSystem::new(&dir_b, opts.add_properties.clone(), None).unwrap();
        ufs_b.mkdir(&dir_b, true).await.unwrap();
        let b_file = Path::from_str(format!("{}/swap.log", dir_b.full_path())).unwrap();
        let mut w = ufs_b.create(&b_file, true).await.unwrap();
        w.write_string(&new_data).await.unwrap();
        w.complete().await.unwrap();

        // Raw umount + remount onto the DIFFERENT UFS target: the client
        // snapshot stays on the dead (mount_id, incarnation) binding.
        fs.cv().umount(&root).await.unwrap();
        fs.cv().mount(&dir_b, &root, opts.clone()).await.unwrap();

        // The stale-bound overwrite is the typed FENCED terminal — never
        // a purge of the new incarnation, never a UFS write.
        let err = match fs.create(&path, true).await {
            Err(e) => e,
            Ok(_) => panic!("the stale-bound overwrite must fence, not open"),
        };
        assert!(
            matches!(err.kind(), curvine_error::ErrorKind::CacheIncarnationFenced),
            "stale-bound purge must be the typed FENCED terminal, got {:?}",
            err
        );
        // The new target is untouched (the overwrite never happened) and
        // the new incarnation is at ZERO cache rows for this key.
        let mut r = ufs_b.open(&b_file).await.unwrap();
        assert_eq!(
            r.read_as_string().await.unwrap(),
            new_data,
            "the fenced purge must leave the new UFS target untouched"
        );
        let table = fs.cv().get_mount_table().await.unwrap();
        let mnt2 = table
            .iter()
            .find(|m| m.cv_path == "/write_cache_p43")
            .expect("remounted mount must be in the table");
        let inc2 = master_fs
            .cache_service
            .current_incarnation_for_mount(mnt2.mount_id)
            .unwrap()
            .unwrap();
        assert_ne!((mnt2.mount_id, inc2), (mnt.info.mount_id, inc1));
        let b_key = {
            let ufs_b_path = mnt2.get_ufs_path(&path).unwrap();
            mnt2.get_cache_key(&ufs_b_path).unwrap()
        };
        assert!(
            master_fs
                .cache_service
                .get(inc2, &b_key, false)
                .unwrap()
                .is_none(),
            "the fenced purge must leave the new incarnation at zero changes"
        );

        // Cache->fs switch while the snapshot is STILL stale (cache-mode):
        // the bound purge's path no longer routes cache-mode at the
        // master — the SAME typed fence, never a fall-through to the
        // legacy inode free.
        fs.cv().umount(&root).await.unwrap();
        let fs_opts = MountOptionsBuilder::new()
            .write_type(WriteType::FsMode)
            .access_mode(AccessMode::ReadWrite)
            .build();
        fs.cv().mount(&dir_b, &root, fs_opts).await.unwrap();
        let err = match fs.create(&path, true).await {
            Err(e) => e,
            Ok(_) => panic!("a bound purge on a cache->fs route must fence, not open"),
        };
        assert!(
            matches!(err.kind(), curvine_error::ErrorKind::CacheIncarnationFenced),
            "a bound purge on a cache->fs switched route must fence, got {:?}",
            err
        );

        // Back to cache-mode, re-observe through the cache-domain
        // fence-refresh path (the write path is loud by design — no
        // silent re-resolve), then the RETRY succeeds against the new
        // binding.
        fs.cv().umount(&root).await.unwrap();
        fs.cv().mount(&dir_b, &root, opts).await.unwrap();
        assert_eq!(
            fs.cache_status(&path).await.unwrap(),
            CacheEntryStatus::Miss
        );
        let retry_data = Utils::rand_str(1024);
        let mut w = fs.create(&path, true).await.unwrap();
        w.write_string(&retry_data).await.unwrap();
        w.complete().await.unwrap();
        let mut r = fs.open(&path).await.unwrap();
        assert_eq!(r.read_as_string().await.unwrap(), retry_data);
    });
}

/// P4-3 purge fences, seams 3+4+5 (gpt56 `2a089d5a` #3): the purge runs
/// BEFORE the UFS mutation. When the UFS rename/delete fails (read-only
/// UFS parent), the purge has already cleared the cache but the ORIGINAL
/// data stays readable via the UFS fallback, and the retry succeeds —
/// purge-after-UFS has no recovery path (src gone) and is forbidden.
/// After a SUCCESSFUL rename/delete nothing serves the old cache (seam 5).
#[test]
fn test_cache_p43_purge_before_ufs_rename_delete() {
    let (fs, _cluster) = get_fs_cache_meta();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        use std::os::unix::fs::PermissionsExt;

        let ufs_base = env::var("UFS_TEST_PATH").unwrap();
        let dir = Path::from_str(format!("{}/write_cache_CacheMode_p43ro", ufs_base)).unwrap();
        let root = Path::from_str("/write_cache_p43ro").unwrap();
        let opts = MountOptionsBuilder::new()
            .write_type(WriteType::CacheMode)
            .access_mode(AccessMode::ReadWrite)
            .build();
        let ufs = UfsFileSystem::new(&dir, opts.add_properties.clone(), None).unwrap();
        ufs.mkdir(&dir, true).await.unwrap();
        fs.mount(&dir, &root, opts).await.unwrap();

        let src: Path = "/write_cache_p43ro/f1.log".into();
        let dst: Path = "/write_cache_p43ro/f2.log".into();
        let data = Utils::rand_str(2048);
        let mut w = fs.create(&src, true).await.unwrap();
        w.write_string(&data).await.unwrap();
        w.complete().await.unwrap();
        let (ufs_path, mnt) = fs
            .get_mount(&src, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        fs.async_cache(&ufs_path).unwrap();
        fs.wait_job_complete(&src, false).await.unwrap();
        assert!(matches!(
            fs.cache_status(&src).await.unwrap(),
            CacheEntryStatus::Hit { .. }
        ));

        // Read-only UFS parent: the purges still succeed (master-side),
        // the UFS rename fails — src keeps serving via the UFS fallback.
        let ufs_dir = std::path::Path::new(dir.path());
        std::fs::set_permissions(ufs_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let err = fs.rename(&src, &dst).await.unwrap_err();
        assert!(
            format!("{:?}", err).to_lowercase().contains("permission")
                || format!("{:?}", err).to_lowercase().contains("denied")
                || format!("{:?}", err).to_lowercase().contains("read-only"),
            "the read-only UFS parent must fail the rename, got {:?}",
            err
        );
        // Purge-before is observable: the src cache entry is GONE even
        // though the UFS rename failed ...
        assert_eq!(fs.cache_status(&src).await.unwrap(), CacheEntryStatus::Miss);
        // ... and the original data still serves (UFS fallback).
        let mut r = fs.open(&src).await.unwrap();
        assert_eq!(r.read_as_string().await.unwrap(), data);

        // Retry after the permission fix: the rename succeeds and NOTHING
        // serves the old cache anywhere (seam 5, rename/delete legs).
        std::fs::set_permissions(ufs_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(fs.rename(&src, &dst).await.unwrap());
        let err = fs.get_status(&src).await.unwrap_err();
        assert!(
            format!("{:?}", err).contains("not found") || format!("{:?}", err).contains("NotFound"),
            "src must be gone after the rename, got {:?}",
            err
        );
        let mut r = fs.open(&dst).await.unwrap();
        assert_eq!(r.read_as_string().await.unwrap(), data);

        // Delete leg: the same purge-before ordering on a failing UFS
        // delete, then the successful delete leaves nothing behind.
        let f3: Path = "/write_cache_p43ro/f3.log".into();
        let mut w = fs.create(&f3, true).await.unwrap();
        w.write_string(&data).await.unwrap();
        w.complete().await.unwrap();
        let (f3_ufs, _) = fs
            .get_mount(&f3, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        fs.async_cache(&f3_ufs).unwrap();
        fs.wait_job_complete(&f3, false).await.unwrap();

        std::fs::set_permissions(ufs_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let err = fs.delete(&f3, false).await.unwrap_err();
        assert!(
            format!("{:?}", err).to_lowercase().contains("permission")
                || format!("{:?}", err).to_lowercase().contains("denied")
                || format!("{:?}", err).to_lowercase().contains("read-only"),
            "the read-only UFS parent must fail the delete, got {:?}",
            err
        );
        assert_eq!(fs.cache_status(&f3).await.unwrap(), CacheEntryStatus::Miss);
        let mut r = fs.open(&f3).await.unwrap();
        assert_eq!(r.read_as_string().await.unwrap(), data);

        std::fs::set_permissions(ufs_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        fs.delete(&f3, false).await.unwrap();
        let err = fs.get_status(&f3).await.unwrap_err();
        assert!(
            format!("{:?}", err).contains("not found") || format!("{:?}", err).contains("NotFound"),
            "the deleted file must be gone, got {:?}",
            err
        );
        let _ = mnt;
    });
}

/// P4-1 default-off + non-cache-mode seams (gpt56 `88cda9cf`): the public
/// entries are cache-domain only — an FsMode mount is loud, an unmounted
/// path is loud, and the RAW binding against a default (capability-off)
/// master fails closed with the capability error.
#[test]
fn test_cache_apis_default_off_and_fs_mode_loud() {
    let fs = get_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::FsMode).await;

        let path: Path = format!("/write_cache_{:?}/p41_fs_mode.log", WriteType::FsMode).into();
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string("fs mode").await.unwrap();
        writer.complete().await.unwrap();

        let err = fs.cache_status(&path).await.unwrap_err();
        assert!(
            format!("{:?}", err).contains("cache-mode mounts only"),
            "FsMode path must be loud, got {:?}",
            err
        );
        let err = fs.invalidate_cache(&path).await.unwrap_err();
        assert!(
            format!("{:?}", err).contains("cache-mode mounts only"),
            "FsMode path must be loud, got {:?}",
            err
        );

        let unmounted = Path::from_str("/no_such_mount/p41.log").unwrap();
        let err = fs.cache_status(&unmounted).await.unwrap_err();
        assert!(
            format!("{:?}", err).contains("no mount covers"),
            "unmounted path must be loud, got {:?}",
            err
        );

        // Raw binding against the default (capability-off) master: the
        // service gate fails closed before any lookup.
        let err = fs.cv().cache_get(1, "k", false).await.unwrap_err();
        assert!(
            format!("{:?}", err).contains("cache metadata capability is disabled"),
            "default-off master must fail closed, got {:?}",
            err
        );

        // P1-2 seam (gpt56 `2e74f4ac` #2): with Unified disabled the public
        // cache entries are gated LOUD — they must not bypass get_mount's
        // enable_unified gate via the mount cache. The raw cv() binding
        // above stays independent and unaffected.
        let mut disabled = fs.clone();
        disabled.disable_unified();
        let err = disabled.cache_status(&path).await.unwrap_err();
        assert!(
            format!("{:?}", err).contains("unified filesystem is disabled"),
            "disabled unified must gate cache_status loud, got {:?}",
            err
        );
        let err = disabled.invalidate_cache(&path).await.unwrap_err();
        assert!(
            format!("{:?}", err).contains("unified filesystem is disabled"),
            "disabled unified must gate invalidate_cache loud, got {:?}",
            err
        );
    });
}

/// P1-4 seam (gpt56 `2e74f4ac` #4): the raw CacheRemove binding's
/// response-loss contract on a LIVE entry — the FIRST remove changes the
/// state exactly once (APPLIED) and the IDENTICAL replay classifies
/// ALREADY_APPLIED with zero further state change. The alias must also
/// stay fenced across generations: a remove carrying the pre-rebuild
/// incarnation is terminal and leaves the rebuilt incarnation untouched.
#[test]
fn test_cache_remove_alias_replay_and_cross_generation_fence() {
    let (fs, cluster) = get_fs_cache_meta();
    let master_fs = cluster.get_active_master_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::CacheMode).await;

        let data = Utils::rand_str(512);
        let cv_root = Path::from_str("/write_cache_CacheMode").unwrap();
        let path: Path = format!("/write_cache_{:?}/p41_remove.log", WriteType::CacheMode).into();
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string(&data).await.unwrap();
        writer.complete().await.unwrap();

        let (ufs_path, mnt) = fs
            .get_mount(&path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        fs.async_cache(&ufs_path).unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();

        let key = {
            let ufs = mnt.info.get_ufs_path(&path).unwrap();
            mnt.info.get_cache_key(&ufs).unwrap()
        };
        let inc1 = master_fs
            .cache_service
            .current_incarnation_for_mount(mnt.info.mount_id)
            .unwrap()
            .unwrap();

        // Observe the LIVE entry's identity via the raw Get binding.
        let rep = fs.cv().cache_get(inc1, &key, false).await.unwrap();
        assert!(rep.hit.unwrap_or(false), "entry must be live before remove");
        let object_id = rep.object_id.unwrap();
        let generation = rep.generation.unwrap();

        // FIRST remove on the live entry: APPLIED, state changes once.
        let removed = fs
            .cv()
            .cache_remove(inc1, &key, generation, object_id)
            .await
            .unwrap();
        assert_eq!(removed.status, Some(1), "first remove must be APPLIED");
        assert!(
            master_fs
                .cache_service
                .get(inc1, &key, false)
                .unwrap()
                .is_none(),
            "first remove must tombstone the entry"
        );

        // Response-loss replay of the IDENTICAL payload: ALREADY_APPLIED,
        // zero further state change.
        let replay = fs
            .cv()
            .cache_remove(inc1, &key, generation, object_id)
            .await
            .unwrap();
        assert_eq!(replay.status, Some(2), "replay must be ALREADY_APPLIED");
        assert!(
            master_fs
                .cache_service
                .get(inc1, &key, false)
                .unwrap()
                .is_none(),
            "replay must change nothing"
        );

        // Cross-generation fence: rebuild the mount (raw client only) and
        // re-load the entry under the new incarnation; a remove carrying
        // the OLD incarnation is terminal and the new entry survives.
        let ufs_base = env::var("UFS_TEST_PATH").unwrap();
        let ufs_mount_path = Path::from_str(format!("{}/write_cache_CacheMode", ufs_base)).unwrap();
        fs.cv().umount(&cv_root).await.unwrap();
        let opts = MountOptionsBuilder::new()
            .write_type(WriteType::CacheMode)
            .access_mode(AccessMode::ReadWrite)
            .build();
        fs.cv()
            .mount(&ufs_mount_path, &cv_root, opts)
            .await
            .unwrap();
        let table = fs.cv().get_mount_table().await.unwrap();
        let mnt2 = table
            .iter()
            .find(|m| m.cv_path == "/write_cache_CacheMode")
            .expect("rebuilt mount must be in the table");
        let inc2 = master_fs
            .cache_service
            .current_incarnation_for_mount(mnt2.mount_id)
            .unwrap()
            .unwrap();
        assert_ne!(inc2, inc1);

        fs.async_cache(&ufs_path).unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();
        let rep2 = fs.cv().cache_get(inc2, &key, false).await.unwrap();
        assert!(rep2.hit.unwrap_or(false), "reload must commit under inc2");
        let object_id2 = rep2.object_id.unwrap();
        let generation2 = rep2.generation.unwrap();

        let err = fs
            .cv()
            .cache_remove(inc1, &key, generation2, object_id2)
            .await
            .unwrap_err();
        assert!(
            matches!(err.kind(), curvine_error::ErrorKind::CacheIncarnationFenced),
            "old-inc remove must be the typed FENCED terminal, got {:?}",
            err
        );
        assert!(
            master_fs
                .cache_service
                .get(inc2, &key, false)
                .unwrap()
                .is_some(),
            "the cross-generation remove must leave the new incarnation at zero changes"
        );
    });
}

#[test]
fn test_fs_mode_free() {
    let fs = get_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::FsMode).await;

        let data = Utils::rand_str(1024);

        let path = format!("/write_cache_{:?}/test_fs_mode_free.log", WriteType::FsMode).into();
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string(&data).await.unwrap();
        writer.complete().await.unwrap();

        let _ = fs.open(&path).await.unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();

        fs.free(&path, false).await.unwrap();

        let file_blocks = fs.cv().get_block_locations(&path).await.unwrap();
        println!("test_fs_mode_free status {:?}", file_blocks);
        assert_eq!(file_blocks.len, data.len() as i64);
        assert_eq!(file_blocks.block_locs.len(), 0);

        let reader = fs.open(&path).await.unwrap();
        assert!(!matches!(reader, UnifiedReader::Cv(_)));
    });
}

async fn prepare_fs_mode_file_then_free(fs: &UnifiedFileSystem, path: &Path, data: &str) {
    let mut writer = fs.create(path, true).await.unwrap();
    writer.write_string(data).await.unwrap();
    writer.complete().await.unwrap();
    let _ = fs.open(path).await.unwrap();
    fs.wait_job_complete(path, false).await.unwrap();
    fs.free(path, false).await.unwrap();
}

#[test]
fn test_fs_mode_ufs_write_overwrite() {
    let fs = get_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::FsMode).await;
        let base = format!("/write_cache_{:?}", WriteType::FsMode);
        let path =
            Path::from_str(format!("{}/test_fs_mode_ufs_write_overwrite.log", base)).unwrap();

        let data_initial = Utils::rand_str(1024);
        prepare_fs_mode_file_then_free(&fs, &path, &data_initial).await;

        let data_overwrite = Utils::rand_str(2048);
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string(&data_overwrite).await.unwrap();
        writer.complete().await.unwrap();

        let reader = fs.open(&path).await.unwrap();
        assert!(
            matches!(reader, UnifiedReader::Fallback(_)),
            "read should return Fallback reader after overwrite and sync"
        );

        verify_read_data(&fs, &path, data_overwrite.as_bytes()).await;

        // Wait until CV and UFS are fully consistent instead of a fixed sleep,
        // as UFS sync jobs may take longer under parallel test load.
        wait_for_cv_ufs_consistency(&fs, &path).await;
    });
}

#[test]
fn test_fs_mode_ufs_write_append() {
    let fs = get_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::FsMode).await;
        let base = format!("/write_cache_{:?}", WriteType::FsMode);
        let path = Path::from_str(format!("{}/test_fs_mode_ufs_write_append.log", base)).unwrap();

        let data_initial = Utils::rand_str(1024);
        prepare_fs_mode_file_then_free(&fs, &path, &data_initial).await;

        let data_append_extra = Utils::rand_str(512);
        let mut writer = fs.append(&path).await.unwrap();
        writer.write_string(&data_append_extra).await.unwrap();
        writer.complete().await.unwrap();

        let reader = fs.open(&path).await.unwrap();
        assert!(
            matches!(reader, UnifiedReader::Fallback(_)),
            "read should return Fallback reader after append and sync"
        );
        let expected_append = format!("{}{}", data_initial, data_append_extra);

        verify_read_data(&fs, &path, expected_append.as_bytes()).await;

        // Wait until CV and UFS are fully consistent instead of a fixed sleep,
        // as UFS sync jobs may take longer under parallel test load.
        wait_for_cv_ufs_consistency(&fs, &path).await;
    });
}

#[test]
fn test_fs_mode_ufs_write_random() {
    let fs = get_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::FsMode).await;
        let base = format!("/write_cache_{:?}", WriteType::FsMode);
        let path = Path::from_str(format!("{}/test_fs_mode_ufs_write_random.log", base)).unwrap();

        let data_initial = Utils::rand_str(1024);
        prepare_fs_mode_file_then_free(&fs, &path, &data_initial).await;

        let chunk_size = 64 * 1024;
        let total_size = 256 * 1024;
        let num_chunks = total_size / chunk_size;
        let mut writer = fs.open_for_write(&path).await.unwrap();
        let mut expected = vec![0u8; total_size];
        for _ in 0..num_chunks {
            let data_str = Utils::rand_str(chunk_size);
            let data = DataSlice::from_str(data_str.clone()).freeze();
            let write_pos = writer.pos() as usize;
            writer.async_write(data.clone()).await.unwrap();
            expected[write_pos..write_pos + chunk_size].copy_from_slice(data_str.as_bytes());
        }
        let random_pos = (num_chunks / 2 * chunk_size) as i64;
        writer.seek(random_pos).await.unwrap();
        let random_chunk = Utils::rand_str(chunk_size);
        let random_data = DataSlice::from_str(random_chunk.clone()).freeze();
        let write_pos = writer.pos() as usize;
        writer.async_write(random_data).await.unwrap();
        expected[write_pos..write_pos + chunk_size].copy_from_slice(random_chunk.as_bytes());
        writer.complete().await.unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();
        let reader = fs.open(&path).await.unwrap();
        assert!(
            matches!(reader, UnifiedReader::Fallback(_)),
            "read should return Fallback reader after random write and sync"
        );

        verify_read_data(&fs, &path, &expected).await;

        // Wait until CV and UFS are fully consistent instead of a fixed sleep,
        // as UFS sync jobs may take longer under parallel test load.
        wait_for_cv_ufs_consistency(&fs, &path).await;
    });
}

async fn write(fs: &UnifiedFileSystem, path: &Path, random_write: bool) {
    let chunk_size = 64 * 1024;
    let total_size = 1024 * 1024;
    let num_chunks = total_size / chunk_size;

    let mut writer = fs.create(path, true).await.unwrap();
    let mut written_data = vec![0u8; total_size];

    // Sequential write all chunks
    for _ in 0..num_chunks {
        let data_str = Utils::rand_str(chunk_size);
        let data = DataSlice::from_str(data_str.clone()).freeze();

        let write_pos = writer.pos() as usize;
        writer.async_write(data.clone()).await.unwrap();
        written_data[write_pos..write_pos + chunk_size].copy_from_slice(data_str.as_bytes());
    }

    if random_write {
        let random_chunk_data = Utils::rand_str(chunk_size);
        let random_data = DataSlice::from_str(random_chunk_data.clone()).freeze();

        let random_pos = (num_chunks / 2 * chunk_size) as i64;
        writer.seek(random_pos).await.unwrap();

        let write_pos = writer.pos() as usize;
        writer.async_write(random_data.clone()).await.unwrap();
        written_data[write_pos..write_pos + chunk_size]
            .copy_from_slice(random_chunk_data.as_bytes());
    }

    writer.complete().await.unwrap();

    verify_read_data(fs, path, &written_data).await;

    fs.wait_job_complete(path, false).await.unwrap();

    verify_cv_ufs_consistency(fs, path).await;
}

async fn verify_read_data(fs: &UnifiedFileSystem, path: &Path, expected_data: &[u8]) {
    let mut reader = fs.open(path).await.unwrap();

    let mut read_data = BytesMut::zeroed(reader.len() as usize);
    reader.read_full(&mut read_data).await.unwrap();
    reader.complete().await.unwrap();

    assert_eq!(
        Utils::crc32(&read_data),
        Utils::crc32(expected_data),
        "Read data does not match written data"
    );
}

/// Returns true when UFS object exists and matches CV (mtime + full content).
async fn try_verify_cv_ufs_consistency(fs: &UnifiedFileSystem, path: &Path) -> bool {
    let (ufs_path, mnt) = match fs.get_mount(path, RpcCode::GetMountInfo).await {
        Ok(Some(v)) => v,
        _ => return false,
    };
    let mut ufs_reader = match mnt.ufs().unwrap().open(&ufs_path).await {
        Ok(r) => r,
        Err(_) => return false,
    };
    let mut cv_reader = match fs.cv().open(path).await {
        Ok(r) => r,
        Err(_) => return false,
    };
    if !cv_reader.status().is_complete {
        return false;
    }
    if cv_reader.status().storage_policy.ufs_mtime != ufs_reader.status().mtime {
        return false;
    }
    let mut cv_data = BytesMut::zeroed(cv_reader.len() as usize);
    if cv_reader.read_full(&mut cv_data).await.is_err() {
        return false;
    }
    let mut ufs_data = BytesMut::zeroed(ufs_reader.len() as usize);
    if ufs_reader.read_full(&mut ufs_data).await.is_err() {
        return false;
    }
    Utils::crc32(&cv_data) == Utils::crc32(&ufs_data)
}

async fn verify_cv_ufs_consistency(fs: &UnifiedFileSystem, path: &Path) {
    assert!(
        try_verify_cv_ufs_consistency(fs, path).await,
        "CV/UFS consistency check failed for {}",
        path.path()
    );
}

async fn wait_for_cv_ufs_consistency(fs: &UnifiedFileSystem, path: &Path) {
    let mut w = awaitility::at_most(Duration::from_secs(60));
    w.poll_interval(Duration::from_millis(100));
    w.until_async(|| async { try_verify_cv_ufs_consistency(fs, path).await })
        .await;
    w.result()
        .expect("timed out waiting for CV and UFS to match after journal/UFS apply");
}

/// Rewrite a UFS file in place through the mount's own UFS handle —
/// used by the D5 drift seams to change the UFS state WITHOUT going
/// through the cache mounts.
async fn rewrite_ufs(mnt: &MountInfo, cv: &Path, content: &str) {
    let ufs_path = mnt.get_ufs_path(cv).unwrap();
    let ufs = UfsFileSystem::with_mount(mnt).unwrap();
    let mut w = ufs.create(&ufs_path, true).await.unwrap();
    w.write_string(content).await.unwrap();
    w.complete().await.unwrap();
}

async fn mount(fs: &UnifiedFileSystem, write_type: WriteType) {
    let ufs_base = env::var("UFS_TEST_PATH").unwrap();

    let dir = format!("write_cache_{:?}", write_type);
    let ufs_path = Path::from_str(format!("{}/{}", ufs_base, dir)).unwrap();
    let cv_path = Path::from_str(format!("/{}", dir)).unwrap();

    if fs
        .get_mount(&cv_path, RpcCode::GetMountInfo)
        .await
        .unwrap()
        .is_some()
    {
        return;
    }

    let mut opts_builder = MountOptionsBuilder::new().write_type(write_type);
    if write_type == WriteType::CacheMode {
        opts_builder = opts_builder.access_mode(AccessMode::ReadWrite);
    }

    // Add properties from environment variable if set
    if let Ok(props_str) = env::var("UFS_TEST_PROPERTIES") {
        for pair in props_str.split(',') {
            if let Some((key, value)) = pair.split_once('=') {
                opts_builder = opts_builder.add_property(key.trim(), value.trim());
            }
        }
    }

    let opts = opts_builder.build();
    let ufs = UfsFileSystem::new(&ufs_path, opts.add_properties.clone(), None).unwrap();
    if ufs.exists(&ufs_path).await.unwrap() {
        ufs.delete(&ufs_path, true).await.unwrap();
    }

    ufs.mkdir(&ufs_path, true).await.unwrap();

    fs.mount(&ufs_path, &cv_path, opts.clone()).await.unwrap();
}
