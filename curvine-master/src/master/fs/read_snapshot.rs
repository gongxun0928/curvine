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

//! Consistent metadata reads with a short namespace read lock.
//!
//! The namespace identity, owned in-memory attributes and RocksDB snapshot are
//! captured under FS read. Writers keep their existing FS write order. No tree
//! pointer escapes that lock; restore drains the FS-level lifecycle leases before
//! closing the database. Block reads additionally pin Worker state. The snapshot
//! fixes one request's view, not an entire cross-RPC listing or a Raft read index.

use super::MasterFilesystem;
use crate::master::meta::inode::InodeView;
use crate::master::meta::store::RocksInodeStore;
use curvine_core_error::{err_box, err_ext, try_err, CommonResult};
use curvine_error::{FsError, FsResult};
use curvine_model::{BlockLocation, ExtendedBlock, FileBlocks, FileStatus, ListOptions};
use curvine_rocksdb::{Direction, IteratorMode, ReadOptions, RocksUtils, Snapshot};
use curvine_runtime::common::SerdeUtils;
use std::sync::Arc;

pub(super) enum SnapshotReadRequest<'a> {
    Status,
    Exists,
    Blocks,
    List(&'a ListOptions),
}

pub(super) enum SnapshotReadReply {
    Status(FileStatus),
    Exists(bool),
    Blocks(FileBlocks),
    List(Vec<FileStatus>),
}

/// No tree pointers/references escape the namespace guard.
// Keep ready statuses inline to avoid an extra allocation for directory stat.
// Page capture is bounded; this does not change the stored inode representation.
#[allow(clippy::large_enum_variant)]
enum Captured {
    Status(FileStatus),
    File(InodeView),
    Stored { id: i64, name: String },
}

impl Captured {
    fn new(inode: &InodeView, path: &str, blocks: bool) -> CommonResult<Self> {
        Ok(match inode {
            InodeView::FileEntry(e) => Self::Stored {
                id: e.id,
                name: e.name.clone(),
            },
            InodeView::File(_) if blocks => Self::File(inode.clone()),
            _ => Self::Status(inode.to_file_status(path)?),
        })
    }

    fn materialize(self, store: &RocksInodeStore, snap: &Snapshot<'_>) -> CommonResult<Self> {
        match self {
            Self::Stored { id, name } => {
                let bytes = snap
                    .get_cf(
                        store.db.cf(RocksInodeStore::CF_INODES)?,
                        RocksUtils::i64_to_bytes(id),
                    )?
                    .ok_or_else(|| format!("Failed to load inode {} from store", id))?;
                let mut inode: InodeView = SerdeUtils::deserialize(&bytes)?;
                // Match existing name hydration, notably for hardlink aliases.
                inode.change_name(name);
                Ok(Self::File(inode))
            }
            other => Ok(other),
        }
    }

    fn status(self, path: &str) -> CommonResult<FileStatus> {
        match self {
            Self::Status(status) => Ok(status),
            Self::File(inode) => inode.to_file_status(path),
            Self::Stored { .. } => unreachable!("unmaterialized inode"),
        }
    }
}

/// Directory-only traversal. Non-directory ancestors retain the existing slow
/// path (including its validation/storage errors). No RawPtr::as_mut is used.
fn lookup<'a>(root: &'a InodeView, path: &str) -> CommonResult<Result<Option<&'a InodeView>, ()>> {
    let components = InodeView::path_components(path)?;
    if components.last().is_none_or(String::is_empty) {
        return err_box!("Path {} is invalid", path);
    }
    let mut node = root;
    for name in components.iter().skip(1) {
        if !node.is_dir() {
            return Ok(Err(()));
        }
        match node.get_child(name) {
            Some(child) => node = child,
            None => return Ok(Ok(None)),
        }
    }
    Ok(Ok(Some(node)))
}

impl MasterFilesystem {
    /// None means the caller must continue through the original path. All
    /// namespace references and guards have been dropped before returning None.
    pub(super) fn try_snapshot_read(
        &self,
        path: &str,
        request: SnapshotReadRequest<'_>,
    ) -> FsResult<Option<SnapshotReadReply>> {
        // Preserve unbounded/large-list semantics on the original path. The
        // read copies at most this many namespace entries per request.
        if matches!(&request, SnapshotReadRequest::List(opts) if opts.limit.is_none_or(|n| n > 4096))
        {
            return Ok(None);
        }
        // Keep access-policy bookkeeping on its original protected path. This
        // snapshot path makes no changes to eviction semantics.
        if matches!(request, SnapshotReadRequest::Blocks) && self.conf.enable_quota_eviction {
            return Ok(None);
        }
        let fs_guard = self.fs_dir.read();
        let fs = &*fs_guard;
        let node = match lookup(fs.root_dir(), path)? {
            Ok(node) => node,
            Err(()) => {
                drop(fs_guard);
                return Ok(None);
            }
        };
        let Some(node) = node else {
            return if matches!(request, SnapshotReadRequest::Exists) {
                Ok(Some(SnapshotReadReply::Exists(false)))
            } else {
                err_ext!(FsError::file_not_found(path))
            };
        };
        let blocks = matches!(request, SnapshotReadRequest::Blocks);
        let mut page = Vec::new();
        if let SnapshotReadRequest::List(opts) = &request {
            if let InodeView::Dir(dir) = node {
                for child in dir.list_options(opts) {
                    let child_path = if path == "/" {
                        format!("/{}", child.name())
                    } else {
                        format!("{}/{}", path, child.name())
                    };
                    let captured = Captured::new(child, &child_path, false)?;
                    page.push((child_path, captured));
                }
            }
        }
        let is_dir = node.is_dir();
        let captured = Captured::new(node, path, blocks)?;
        // Declare snapshot/store AFTER the lease, so they drop before it on
        // every exit. Restore holds FS write and drains these leases. A reader
        // must never reacquire FS while holding its lease.
        let lifecycle = Arc::clone(&fs.read_lifecycle);
        let _lease = lifecycle.read();
        let store = Arc::clone(&fs.store.store);
        // Preserve FS -> Worker lock order. Hold Worker read through completion
        // so worker membership and DB locations coexisted at snapshot creation.
        let worker_guard = blocks.then(|| self.worker_manager.read());
        let snap = store.db.get_db().snapshot();
        drop(fs_guard);
        #[cfg(test)]
        AFTER_CAPTURE.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        let captured = captured.materialize(&store, &snap)?;
        let reply = match request {
            SnapshotReadRequest::Status => Ok(SnapshotReadReply::Status(captured.status(path)?)),
            // Materialize before returning to preserve corrupt/missing-inode
            // behavior of the original exists resolver.
            SnapshotReadRequest::Exists => Ok(SnapshotReadReply::Exists(true)),
            SnapshotReadRequest::List(opts) => {
                if !is_dir {
                    let status = captured.status(path)?;
                    let include = opts.limit != Some(0)
                        && opts.start_after.as_ref().is_none_or(|s| status.name > *s);
                    return Ok(Some(SnapshotReadReply::List(if include {
                        vec![status]
                    } else {
                        vec![]
                    })));
                }
                // Retain batched MultiGet and PinnableSlice behavior, adding only
                // the snapshot ReadOptions. No per-entry serial Get regression.
                let keys: Vec<_> = page
                    .iter()
                    .filter_map(|(_, item)| match item {
                        Captured::Stored { id, .. } => Some(RocksUtils::i64_to_bytes(*id)),
                        _ => None,
                    })
                    .collect();
                let mut opts = ReadOptions::default();
                opts.set_snapshot(&snap);
                let cf = store.db.cf(RocksInodeStore::CF_INODES)?;
                let values =
                    store
                        .db
                        .get_db()
                        .batched_multi_get_cf_opt(cf, keys.iter(), false, &opts);
                let mut values = values.into_iter();
                let mut out = Vec::with_capacity(page.len());
                for (child_path, item) in page {
                    let item = match item {
                        Captured::Stored { id, name } => {
                            let bytes = try_err!(values.next().unwrap())
                                .ok_or_else(|| format!("inode missing: {id}"))?;
                            let mut inode: InodeView = SerdeUtils::deserialize(&bytes)?;
                            if inode.id() != id {
                                return err_box!("inode id mismatch: {} != {}", inode.id(), id);
                            }
                            inode.change_name(name);
                            Captured::File(inode)
                        }
                        other => other,
                    };
                    out.push(item.status(&child_path)?);
                }
                Ok(SnapshotReadReply::List(out))
            }
            SnapshotReadRequest::Blocks => {
                let Captured::File(inode) = captured else {
                    return err_box!("Not a file");
                };
                let file = inode.as_file_ref()?;
                let wm = worker_guard.as_ref().unwrap();
                let mut locations = Vec::with_capacity(file.blocks.len());
                let cf = store.db.cf(RocksInodeStore::CF_BLOCK)?;
                for (index, meta) in file.blocks.iter().enumerate() {
                    let start = RocksUtils::i64_to_bytes(meta.id);
                    let mut opts = ReadOptions::default();
                    opts.set_prefix_same_as_start(true);
                    opts.set_iterate_lower_bound(start);
                    opts.set_iterate_upper_bound(RocksUtils::calculate_end_bytes(&start));
                    let mut locs = Vec::with_capacity(8);
                    for item in snap.iterator_cf_opt(
                        cf,
                        opts,
                        IteratorMode::From(&start, Direction::Forward),
                    ) {
                        locs.push(SerdeUtils::deserialize::<BlockLocation>(&try_err!(item).1)?);
                    }
                    if index + 1 < file.blocks.len() && meta.len() != file.block_size as i64 {
                        return err_box!(
                            "block status abnormal, block id {}, block len {}, expected block size {}",
                            meta.id, meta.len(), file.block_size
                        );
                    }
                    let extended = ExtendedBlock {
                        id: meta.id,
                        len: meta.len(),
                        storage_type: file.storage_policy.storage_type,
                        file_type: file.file_type,
                        alloc_opts: meta.alloc_opts.clone(),
                    };
                    locations.push(wm.create_locate_block(path, extended, &locs)?);
                }
                Ok(SnapshotReadReply::Blocks(FileBlocks::new(
                    inode.to_file_status(path)?,
                    locations,
                )))
            }
        };
        reply.map(Some)
    }
}

#[cfg(test)]
thread_local! {
    static AFTER_CAPTURE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = Default::default();
}

#[cfg(test)]
#[path = "read_snapshot_tests.rs"]
mod tests;
