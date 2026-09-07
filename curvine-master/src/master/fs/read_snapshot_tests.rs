use super::*;
use crate::master::journal::JournalSystem;
use crate::master::meta::{BlockMeta, InodeId};
use crate::master::Master;
use curvine_config::{ClusterConf, JournalConf, MasterConf};
use curvine_model::{RenameFlags, SetAttrOptsBuilder, WorkerInfo};
use curvine_runtime::common::Utils;
use std::sync::{mpsc, Mutex};
use std::time::Duration;

static SERIAL: Mutex<()> = Mutex::new(());

fn filesystem() -> MasterFilesystem {
    Master::init_test_metrics();
    let c = ClusterConf {
        format_master: true,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir(format!("read-snapshot/{}", Utils::rand_str(10))),
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let fs = JournalSystem::fs_only_for_test(&c).unwrap();
    for id in [1, 2] {
        let mut worker = WorkerInfo::default();
        worker.address.worker_id = id;
        fs.add_test_worker(worker);
    }
    fs
}

fn snapshot_reader(fs: &MasterFilesystem) -> MasterFilesystem {
    let mut reader = fs.clone();
    Arc::make_mut(&mut reader.conf).metadata_read_snapshot = true;
    reader
}

fn modes(fs: &MasterFilesystem) -> Vec<MasterFilesystem> {
    vec![fs.clone(), snapshot_reader(fs)]
}

fn same<T: serde::Serialize>(a: &T, b: &T) {
    assert_eq!(
        SerdeUtils::serialize(a).unwrap(),
        SerdeUtils::serialize(b).unwrap()
    );
}

fn same_result<T: serde::Serialize>(a: FsResult<T>, b: FsResult<T>) {
    match (a, b) {
        (Ok(a), Ok(b)) => same(&a, &b),
        (Err(a), Err(b)) => assert_eq!(i32::from(a.kind()), i32::from(b.kind())),
        _ => panic!("snapshot and original paths disagreed on success/failure"),
    }
}

fn pause_next_read() -> (mpsc::Receiver<()>, mpsc::Sender<()>, impl FnOnce() + Send) {
    let (ready, ready_rx) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    (ready_rx, release, move || {
        AFTER_CAPTURE.with(|h| {
            *h.borrow_mut() = Some(Box::new(move || {
                ready.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            }))
        });
    })
}

#[test]
fn snapshot_read_equivalent_paths_aliases_and_pages() {
    let _serial = SERIAL.lock().unwrap();
    let fs = filesystem();
    fs.mkdir("/a/b", true).unwrap();
    fs.create("/a/b/f", false).unwrap();
    fs.link("/a/b/f", "/a/b/alias").unwrap();
    fs.symlink("/missing", "/a/b/sym", false, 0o777).unwrap();
    for path in ["/", "/a", "/a/b", "/a/b/f", "/a/b/alias", "/a/b/sym"] {
        for candidate in modes(&fs) {
            same(
                &fs.file_status(path).unwrap(),
                &candidate.file_status(path).unwrap(),
            );
            assert_eq!(fs.exists(path).unwrap(), candidate.exists(path).unwrap());
            for limit in [None, Some(0), Some(1), Some(2), Some(100), Some(4097)] {
                for after in [None, Some("alias".to_owned()), Some("z".to_owned())] {
                    let opts = ListOptions {
                        limit,
                        start_after: after,
                    };
                    same(
                        &fs.list_options(path, opts.clone()).unwrap(),
                        &candidate.list_options(path, opts).unwrap(),
                    );
                }
            }
        }
    }
    for path in [
        "",
        "relative",
        "/a/",
        "/a/nope",
        "/a/b/f/child",
        "/a/b/sym/child",
        "/a//b",
        "/a/b/*",
    ] {
        for candidate in modes(&fs) {
            same_result(fs.file_status(path), candidate.file_status(path));
            same_result(fs.exists(path), candidate.exists(path));
            same_result(
                fs.get_block_locations(path),
                candidate.get_block_locations(path),
            );
            same_result(
                fs.list_options(path, ListOptions::with_limit(64)),
                candidate.list_options(path, ListOptions::with_limit(64)),
            );
        }
    }
}

#[test]
fn snapshot_read_retains_old_identity_across_delete_recreate() {
    let _serial = SERIAL.lock().unwrap();
    let fs = filesystem();
    let old = fs.create("/f", false).unwrap();
    let reader = snapshot_reader(&fs);
    let (ready, release, install) = pause_next_read();
    let handle = std::thread::spawn(move || {
        install();
        reader.file_status("/f").unwrap()
    });
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    fs.delete("/f", false).unwrap();
    let new = fs.create("/f", false).unwrap();
    release.send(()).unwrap();
    let observed = handle.join().unwrap();
    assert_eq!(observed.id, old.id);
    assert_ne!(observed.id, new.id);
    assert_eq!(snapshot_reader(&fs).file_status("/f").unwrap().id, new.id);
}

#[test]
fn snapshot_read_page_is_one_version_during_rename() {
    let _serial = SERIAL.lock().unwrap();
    let fs = filesystem();
    fs.mkdir("/d", false).unwrap();
    fs.create("/d/a", false).unwrap();
    fs.create("/d/b", false).unwrap();
    let before = fs.list_options("/d", ListOptions::with_limit(10)).unwrap();
    let reader = snapshot_reader(&fs);
    let (ready, release, install) = pause_next_read();
    let handle = std::thread::spawn(move || {
        install();
        reader
            .list_options("/d", ListOptions::with_limit(10))
            .unwrap()
    });
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    fs.rename("/d/a", "/d/z", RenameFlags::empty()).unwrap();
    fs.delete("/d/b", false).unwrap();
    release.send(()).unwrap();
    same(&before, &handle.join().unwrap());
}

fn replace_block(fs: &MasterFilesystem, id: i64, block: i64, len: i64, worker: u32) {
    let dir = fs.fs_dir.write();
    let mut inode = dir.store.get_inode(id, Some("f")).unwrap().unwrap();
    let file = inode.as_file_mut().unwrap();
    let old = file.block_ids();
    file.blocks = vec![BlockMeta::new(block, len)];
    file.len = len;
    let mut batch = dir.store.new_batch();
    batch.write_inode(&inode).unwrap();
    for old in old {
        batch.delete_location(old, 1).unwrap();
        batch.delete_location(old, 2).unwrap();
    }
    batch
        .add_location(block, &BlockLocation::with_id(worker))
        .unwrap();
    batch.commit().unwrap();
}

#[test]
fn snapshot_read_block_and_location_use_same_snapshot() {
    let _serial = SERIAL.lock().unwrap();
    let fs = filesystem();
    let id = fs.create("/f", false).unwrap().id;
    let first_block = InodeId::create_block_id(id, 0).unwrap();
    let second_block = InodeId::create_block_id(id, 1).unwrap();
    replace_block(&fs, id, first_block, 4096, 1);
    let before = fs.get_block_locations("/f").unwrap();
    for candidate in modes(&fs) {
        same(&before, &candidate.get_block_locations("/f").unwrap());
    }
    let reader = snapshot_reader(&fs);
    let (ready, release, install) = pause_next_read();
    let handle = std::thread::spawn(move || {
        install();
        reader.get_block_locations("/f").unwrap()
    });
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    replace_block(&fs, id, second_block, 8192, 2);
    release.send(()).unwrap();
    same(&before, &handle.join().unwrap());
    let after = snapshot_reader(&fs).get_block_locations("/f").unwrap();
    assert_eq!(after.status.len, 8192);
    assert_eq!(after.block_locs[0].block.id, second_block);
    assert_eq!(after.block_locs[0].locs[0].worker_id, 2);
}

#[test]
fn snapshot_read_worker_generation_remains_pinned_after_fs_unlock() {
    let _serial = SERIAL.lock().unwrap();
    let fs = filesystem();
    let id = fs.create("/f", false).unwrap().id;
    replace_block(&fs, id, InodeId::create_block_id(id, 0).unwrap(), 4096, 1);
    let before = fs.get_block_locations("/f").unwrap();
    let reader = snapshot_reader(&fs);
    let (ready, release, install) = pause_next_read();
    let handle = std::thread::spawn(move || {
        install();
        reader.get_block_locations("/f").unwrap()
    });
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    // The namespace is already unlocked, but Worker membership is still pinned.
    assert!(fs.fs_dir.try_write().is_ok());
    assert!(fs.worker_manager.try_write().is_err());
    release.send(()).unwrap();
    same(&before, &handle.join().unwrap());
    let mut worker = WorkerInfo::default();
    worker.address.worker_id = 1;
    worker.address.hostname = "replacement-worker".into();
    fs.add_test_worker(worker);
    let after = snapshot_reader(&fs).get_block_locations("/f").unwrap();
    assert_eq!(after.block_locs[0].locs[0].hostname, "replacement-worker");
}

#[test]
fn snapshot_read_restore_drains_snapshot_before_replacing_db() {
    let _serial = SERIAL.lock().unwrap();
    let fs = filesystem();
    let old = fs.create("/f", false).unwrap();
    let checkpoint = fs.fs_dir.read().create_checkpoint(91).unwrap();
    let reader = snapshot_reader(&fs);
    let (ready, release, install) = pause_next_read();
    let handle = std::thread::spawn(move || {
        install();
        reader.file_status("/f").unwrap()
    });
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    let restore = fs.clone();
    let (started, started_rx) = mpsc::channel();
    let (done, done_rx) = mpsc::channel();
    let restore_handle = std::thread::spawn(move || {
        let mut dir = restore.fs_dir.write();
        started.send(()).unwrap();
        let result = dir.restore(checkpoint, 1);
        done.send(result.is_ok()).unwrap();
        result.unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let blocked = done_rx.recv_timeout(Duration::from_millis(100)).is_err();
    release.send(()).unwrap();
    assert_eq!(handle.join().unwrap().id, old.id);
    assert!(done_rx.recv_timeout(Duration::from_secs(10)).unwrap());
    restore_handle.join().unwrap();
    assert!(blocked);
    assert_eq!(fs.file_status("/f").unwrap().id, old.id);
}

#[test]
fn snapshot_read_preserves_namespace_on_lease_precondition_failure() {
    let _serial = SERIAL.lock().unwrap();
    let fs = filesystem();
    let old = fs.create("/f", false).unwrap();
    let mut dir = fs.fs_dir.write();
    let unrelated_owner = Arc::clone(&dir.store.store);
    assert!(dir.restore("/not-a-checkpoint", 0).is_err());
    drop(unrelated_owner);
    drop(dir);
    assert_eq!(fs.file_status("/f").unwrap().id, old.id);
}

#[test]
fn snapshot_read_concurrent_attributes_never_mix_generations() {
    let _serial = SERIAL.lock().unwrap();
    let fs = filesystem();
    fs.create("/f", false).unwrap();
    fs.set_attr(
        "/f",
        SetAttrOptsBuilder::new().owner("640").mode(640).build(),
    )
    .unwrap();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            for i in 0..2000 {
                let value = 640 + i % 2;
                fs.set_attr(
                    "/f",
                    SetAttrOptsBuilder::new()
                        .owner(value.to_string())
                        .mode(value)
                        .build(),
                )
                .unwrap();
            }
        });
        for _ in 0..4 {
            let reader = snapshot_reader(&fs);
            scope.spawn(move || {
                for _ in 0..2000 {
                    let status = reader.file_status("/f").unwrap();
                    assert_eq!(status.owner, status.mode.to_string());
                }
            });
        }
    });
}

#[test]
fn snapshot_read_parent_rename_preserves_captured_path_identity() {
    let _serial = SERIAL.lock().unwrap();
    let fs = filesystem();
    fs.mkdir("/a/b", true).unwrap();
    let original = fs.create("/a/b/f", false).unwrap();
    let reader = snapshot_reader(&fs);
    let (ready, release, install) = pause_next_read();
    let handle = std::thread::spawn(move || {
        install();
        reader.file_status("/a/b/f").unwrap()
    });
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    fs.rename("/a", "/moved", RenameFlags::empty()).unwrap();
    release.send(()).unwrap();
    let captured = handle.join().unwrap();
    assert_eq!(captured.id, original.id);
    assert_eq!(captured.path, "/a/b/f");
    assert_eq!(fs.file_status("/moved/b/f").unwrap().id, original.id);
    assert!(!fs.exists("/a/b/f").unwrap());
}

#[test]
fn snapshot_read_error_releases_snapshot_and_lifecycle_lease() {
    let _serial = SERIAL.lock().unwrap();
    let fs = filesystem();
    let id = fs.create("/f", false).unwrap().id;
    {
        let dir = fs.fs_dir.write();
        let mut batch = dir.store.new_batch();
        batch.delete_inode(id).unwrap();
        batch.commit().unwrap();
    }
    for candidate in modes(&fs) {
        assert!(candidate.file_status("/f").is_err());
        assert!(candidate.exists("/f").is_err());
        let dir = fs.fs_dir.read();
        assert_eq!(Arc::strong_count(&dir.store.store), 1);
        assert!(dir.read_lifecycle.try_write().is_some());
    }
}

#[test]
fn snapshot_read_corrupt_inode_preserves_errors_and_releases_readers() {
    let _serial = SERIAL.lock().unwrap();
    let fs = filesystem();
    fs.mkdir("/d", false).unwrap();
    let id = fs.create("/d/f", false).unwrap().id;
    {
        let dir = fs.fs_dir.write();
        dir.store
            .store
            .db
            .put_cf(
                RocksInodeStore::CF_INODES,
                RocksUtils::i64_to_bytes(id),
                [0xff; 3],
            )
            .unwrap();
    }
    let reader = snapshot_reader(&fs);
    assert!(reader.exists("/d/f").is_err());
    same_result(fs.file_status("/d/f"), reader.file_status("/d/f"));
    same_result(
        fs.get_block_locations("/d/f"),
        reader.get_block_locations("/d/f"),
    );
    same_result(
        fs.list_options("/d", ListOptions::with_limit(64)),
        reader.list_options("/d", ListOptions::with_limit(64)),
    );
    let dir = fs.fs_dir.read();
    assert_eq!(Arc::strong_count(&dir.store.store), 1);
    assert!(dir.read_lifecycle.try_write().is_some());
    assert!(fs.worker_manager.try_write().is_ok());
}

#[test]
fn snapshot_read_multiple_blocks_and_location_errors_match_original() {
    let _serial = SERIAL.lock().unwrap();
    let fs = filesystem();
    let id = fs.create("/f", false).unwrap().id;
    let first = InodeId::create_block_id(id, 0).unwrap();
    let second = InodeId::create_block_id(id, 1).unwrap();
    {
        let dir = fs.fs_dir.write();
        let mut inode = dir.store.get_inode(id, Some("f")).unwrap().unwrap();
        let file = inode.as_file_mut().unwrap();
        file.block_size = 4096;
        file.len = 4224;
        file.blocks = vec![BlockMeta::new(first, 4096), BlockMeta::new(second, 128)];
        let mut batch = dir.store.new_batch();
        batch.write_inode(&inode).unwrap();
        batch
            .add_location(first, &BlockLocation::with_id(1))
            .unwrap();
        batch
            .add_location(second, &BlockLocation::with_id(2))
            .unwrap();
        batch.commit().unwrap();
    }
    let reader = snapshot_reader(&fs);
    let observed = reader.get_block_locations("/f").unwrap();
    same(&fs.get_block_locations("/f").unwrap(), &observed);
    assert_eq!(observed.block_locs.len(), 2);
    assert_eq!(observed.block_locs[0].locs[0].worker_id, 1);
    assert_eq!(observed.block_locs[1].locs[0].worker_id, 2);
    {
        let dir = fs.fs_dir.write();
        let mut batch = dir.store.new_batch();
        batch.delete_location(second, 2).unwrap();
        batch.commit().unwrap();
    }
    // Empty locations are valid in the existing API. Non-empty locations with
    // no live workers must instead preserve the original error behavior.
    assert!(reader.get_block_locations("/f").unwrap().block_locs[1]
        .locs
        .is_empty());
    same_result(
        fs.get_block_locations("/f"),
        reader.get_block_locations("/f"),
    );
    assert!(fs.worker_manager.try_write().is_ok());
    {
        let dir = fs.fs_dir.write();
        let mut batch = dir.store.new_batch();
        batch
            .add_location(second, &BlockLocation::with_id(999))
            .unwrap();
        batch.commit().unwrap();
    }
    assert!(reader.get_block_locations("/f").is_err());
    same_result(
        fs.get_block_locations("/f"),
        reader.get_block_locations("/f"),
    );
    assert!(fs.worker_manager.try_write().is_ok());
    {
        let dir = fs.fs_dir.write();
        let mut inode = dir.store.get_inode(id, Some("f")).unwrap().unwrap();
        inode.as_file_mut().unwrap().blocks[0] = BlockMeta::new(first, 128);
        let mut batch = dir.store.new_batch();
        batch.write_inode(&inode).unwrap();
        batch.delete_location(second, 999).unwrap();
        batch
            .add_location(second, &BlockLocation::with_id(2))
            .unwrap();
        batch.commit().unwrap();
    }
    same_result(
        fs.get_block_locations("/f"),
        reader.get_block_locations("/f"),
    );
    same_result(fs.get_block_locations("/"), reader.get_block_locations("/"));
}

#[test]
fn snapshot_read_quota_fallback_preserves_access_bookkeeping() {
    use crate::master::quota::eviction::evictor::Evictor;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct AccessCounter(AtomicUsize);
    impl Evictor for AccessCounter {
        fn on_access(&self, _: i64) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn select_victims(&self, _: usize) -> Vec<i64> {
            vec![]
        }
        fn remove_victims(&self, _: &[i64]) {}
        fn cache_size(&self) -> usize {
            0
        }
    }

    let _serial = SERIAL.lock().unwrap();
    let fs = filesystem();
    fs.create("/f", false).unwrap();
    let access = Arc::new(AccessCounter(AtomicUsize::new(0)));
    fs.fs_dir.write().evictor = access.clone();
    let mut reader = snapshot_reader(&fs);
    Arc::make_mut(&mut reader.conf).enable_quota_eviction = true;
    same(
        &fs.get_block_locations("/f").unwrap(),
        &reader.get_block_locations("/f").unwrap(),
    );
    assert_eq!(access.0.load(Ordering::Relaxed), 2);
    let dir = fs.fs_dir.read();
    assert_eq!(Arc::strong_count(&dir.store.store), 1);
    assert!(dir.read_lifecycle.try_write().is_some());
}

#[test]
fn snapshot_read_page_boundary_and_mismatched_inode_are_validated() {
    let _serial = SERIAL.lock().unwrap();
    let fs = filesystem();
    fs.mkdir("/d", false).unwrap();
    for i in 0..70 {
        fs.create(format!("/d/{i:03}"), false).unwrap();
    }
    let reader = snapshot_reader(&fs);
    let first = reader
        .list_options("/d", ListOptions::with_limit(64))
        .unwrap();
    assert_eq!(first.len(), 64);
    let opts = ListOptions {
        limit: Some(64),
        start_after: Some(first.last().unwrap().name.clone()),
    };
    let second = reader.list_options("/d", opts.clone()).unwrap();
    assert_eq!(second.len(), 6);
    same(&fs.list_options("/d", opts).unwrap(), &second);
    for limit in [None, Some(0), Some(4096), Some(4097)] {
        let opts = ListOptions {
            limit,
            start_after: None,
        };
        same_result(
            fs.list_options("/d", opts.clone()),
            reader.list_options("/d", opts),
        );
    }
    let bad_key = fs.file_status("/d/000").unwrap().id;
    let other_id = fs.file_status("/d/001").unwrap().id;
    {
        let dir = fs.fs_dir.write();
        let wrong_inode = dir.store.get_inode(other_id, None).unwrap().unwrap();
        dir.store
            .store
            .db
            .put_cf(
                RocksInodeStore::CF_INODES,
                RocksUtils::i64_to_bytes(bad_key),
                SerdeUtils::serialize(&wrong_inode).unwrap(),
            )
            .unwrap();
    }
    assert!(reader
        .list_options("/d", ListOptions::with_limit(64))
        .is_err());
    same_result(
        fs.list_options("/d", ListOptions::with_limit(64)),
        reader.list_options("/d", ListOptions::with_limit(64)),
    );
}
