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

use crate::master::fs::WorkerManager;
use crate::master::journal::JournalLoader;
use crate::master::meta::FsDir;
use curvine_common::raft::storage::RocksLogStorage;
use curvine_common::raft::RaftJournal;
use orpc::sync::ArcRwLock;
use std::cell::UnsafeCell;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub mod meta;

mod job;
pub use self::job::*;

pub mod replication;

mod master_server;
pub use self::master_server::*;

mod master_handler;
pub use self::master_handler::*;

pub mod fs;

pub mod journal;

mod master_monitor;
pub use self::master_monitor::*;

mod master_metrics;
pub use self::master_metrics::*;

mod router_handler;
pub use self::router_handler::*;

mod rpc_context;
pub use rpc_context::RpcContext;

pub mod mount;

pub mod quota;
pub use self::quota::*;

pub type MetaRaftJournal = RaftJournal<RocksLogStorage, JournalLoader>;
struct SyncFsDirInner {
    fs_dir: UnsafeCell<FsDir>,
    global_lock: RwLock<()>,
}

// SAFETY: FsDir's in-memory tree mutations are protected by InodeLockPool
// path locks. The global_lock serializes write operations during migration.
// UnsafeCell allows read access without the global lock for path-locked ops.
unsafe impl Send for SyncFsDirInner {}
unsafe impl Sync for SyncFsDirInner {}

/// Thread-safe wrapper for FsDir with both global locking and path-level locking.
///
/// Provides three access modes:
/// - `read()`: global read lock (legacy, for compatibility)
/// - `write()`: global write lock (for restore/snapshot and journal replay)
/// - `as_ref()`: no global lock, caller must use path locks from `FsDir.lock_pool`
#[derive(Clone)]
pub struct SyncFsDir {
    inner: Arc<SyncFsDirInner>,
}

impl SyncFsDir {
    pub fn new(fs_dir: FsDir) -> Self {
        Self {
            inner: Arc::new(SyncFsDirInner {
                fs_dir: UnsafeCell::new(fs_dir),
                global_lock: RwLock::new(()),
            }),
        }
    }

    /// Access FsDir with global read lock (legacy compatibility).
    pub fn read(&self) -> GlobalReadGuard<'_> {
        let guard = self.inner.global_lock.read().unwrap();
        GlobalReadGuard {
            _guard: guard,
            fs_dir: unsafe { &*self.inner.fs_dir.get() },
        }
    }

    /// Access FsDir with global write lock (for restore/snapshot operations).
    pub fn write(&self) -> GlobalWriteGuard<'_> {
        let guard = self.inner.global_lock.write().unwrap();
        GlobalWriteGuard {
            _guard: guard,
            fs_dir: unsafe { &mut *self.inner.fs_dir.get() },
        }
    }

    /// Access FsDir WITHOUT global lock. Caller MUST use path locks
    /// from `FsDir.lock_pool` to protect concurrent access.
    pub fn get_ref(&self) -> &FsDir {
        unsafe { &*self.inner.fs_dir.get() }
    }
}

pub struct GlobalReadGuard<'a> {
    _guard: RwLockReadGuard<'a, ()>,
    fs_dir: &'a FsDir,
}

impl<'a> std::ops::Deref for GlobalReadGuard<'a> {
    type Target = FsDir;
    fn deref(&self) -> &FsDir {
        self.fs_dir
    }
}

pub struct GlobalWriteGuard<'a> {
    _guard: RwLockWriteGuard<'a, ()>,
    fs_dir: &'a mut FsDir,
}

impl<'a> std::ops::Deref for GlobalWriteGuard<'a> {
    type Target = FsDir;
    fn deref(&self) -> &FsDir {
        self.fs_dir
    }
}

impl<'a> std::ops::DerefMut for GlobalWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut FsDir {
        self.fs_dir
    }
}

pub type SyncWorkerManager = ArcRwLock<WorkerManager>;
pub use mount::MountManager;
