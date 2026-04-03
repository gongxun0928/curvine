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

//! Sharded Inode Table for fine-grained concurrent access to file metadata.
//!
//! # Design
//!
//! This table stores `InodeFile` data (file metadata, blocks, etc.) in a sharded
//! hash map. Each shard has its own `RwLock`, allowing concurrent access to
//! different inodes.
//!
//! # Lock Ordering Invariant
//!
//! When acquiring multiple shard locks (e.g., for rename), always acquire in
//! ascending shard index order to prevent deadlocks:
//!
//! ```text
//! tree_lock → shard_lock (never reverse)
//! shard_lock[i] → shard_lock[j] where i < j (ascending order)
//! ```

use crate::master::meta::inode::InodeFile;
use log::warn;
use std::collections::HashMap;
use std::sync::RwLock;

/// Default number of shards. Must be a power of 2.
pub const DEFAULT_SHARD_COUNT: usize = 64;

/// Sharded storage for file inode data.
///
/// Each shard is a `RwLock<HashMap<i64, InodeFile>>`, allowing concurrent
/// read/write access to different inodes. Shard assignment is by
/// `inode_id & (shard_count - 1)` (bitwise AND for O(1) hashing).
pub struct ShardedInodeTable {
    shards: Vec<RwLock<HashMap<i64, InodeFile>>>,
    shard_mask: usize,
}

impl ShardedInodeTable {
    /// Create a new sharded inode table with the given number of shards.
    /// `shard_count` must be a power of 2.
    pub fn new(shard_count: usize) -> Self {
        assert!(
            shard_count > 0 && shard_count.is_power_of_two(),
            "shard_count must be a power of 2, got {}",
            shard_count
        );
        let shards = (0..shard_count)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();
        Self {
            shards,
            shard_mask: shard_count - 1,
        }
    }

    /// Create with default shard count (64).
    pub fn with_default_shards() -> Self {
        Self::new(DEFAULT_SHARD_COUNT)
    }

    /// Compute shard index for a given inode ID.
    #[inline]
    fn shard_index(&self, id: i64) -> usize {
        (id as usize) & self.shard_mask
    }

    /// Insert a file inode into the table.
    pub fn insert(&self, id: i64, file: InodeFile) {
        let shard = &self.shards[self.shard_index(id)];
        match shard.write() {
            Ok(mut map) => {
                map.insert(id, file);
            }
            Err(e) => {
                warn!("ShardedInodeTable: lock poisoned for inode {}: {}", id, e);
            }
        }
    }

    /// Remove a file inode from the table. Returns the removed InodeFile if present.
    pub fn remove(&self, id: i64) -> Option<InodeFile> {
        let shard = &self.shards[self.shard_index(id)];
        shard.write().ok().and_then(|mut map| map.remove(&id))
    }

    /// Read access to a file inode via closure callback.
    ///
    /// The closure receives a reference to the `InodeFile`. The shard lock
    /// is held for the duration of the closure, ensuring safe access.
    ///
    /// Returns `None` if the inode is not found.
    pub fn with_file<R>(&self, id: i64, f: impl FnOnce(&InodeFile) -> R) -> Option<R> {
        let shard = &self.shards[self.shard_index(id)];
        let guard = shard.read().ok()?;
        let file = guard.get(&id)?;
        Some(f(file))
    }

    /// Write access to a file inode via closure callback.
    ///
    /// The closure receives a mutable reference to the `InodeFile`. The shard
    /// write lock is held for the duration of the closure.
    ///
    /// Returns `None` if the inode is not found.
    pub fn with_file_mut<R>(&self, id: i64, f: impl FnOnce(&mut InodeFile) -> R) -> Option<R> {
        let shard = &self.shards[self.shard_index(id)];
        let mut guard = shard.write().ok()?;
        let file = guard.get_mut(&id)?;
        Some(f(file))
    }

    /// Write access to two file inodes via closure callback.
    ///
    /// Acquires shard locks in ascending shard index order to prevent deadlocks.
    /// If both inodes map to the same shard, only one lock is acquired.
    ///
    /// Returns `None` if either inode is not found.
    pub fn with_two_files_mut<R>(
        &self,
        id1: i64,
        id2: i64,
        f: impl FnOnce(&mut InodeFile, &mut InodeFile),
    ) -> Option<R> {
        let idx1 = self.shard_index(id1);
        let idx2 = self.shard_index(id2);

        if idx1 == idx2 {
            // Same shard - acquire once
            let shard = &self.shards[idx1];
            let mut guard = shard.write().ok()?;
            let file1 = guard.get_mut(&id1)?;
            let file2 = guard.get_mut(&id2)?;
            Some(f(file1, file2))
        } else {
            // Different shards - acquire in order (ascending) to prevent deadlock
            let (first_idx, first_id, second_idx, second_id) = if idx1 < idx2 {
                (idx1, id1, idx2, id2)
            } else {
                (idx2, id2, idx1, id1)
            };

            let guard1 = self.shards[first_idx].write().ok()?;
            let mut guard2 = self.shards[second_idx].write().ok()?;

            // We need mutable refs from both guards
            // Safety: we confirmed different shards, so no aliasing
            let file1 = guard1.get_mut(&first_id)?;
            let file2 = guard2.get_mut(&second_id)?;

            if idx1 < idx2 {
                Some(f(file1, file2))
            } else {
                // Swap back: f expects (id1, id2) order
                Some(f(file2, file1))
            }
        }
    }

    /// Check if a file inode exists in the table.
    pub fn contains(&self, id: i64) -> bool {
        let shard = &self.shards[self.shard_index(id)];
        shard.read().map(|g| g.contains_key(&id)).unwrap_or(false)
    }

    /// Get the total number of file inodes across all shards.
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.read().map(|g| g.len()).unwrap_or(0))
            .sum()
    }

    /// Check if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the number of shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::inode::InodeFile;

    fn make_file(id: i64, len: i64) -> InodeFile {
        let mut f = InodeFile::new(id, 0);
        f.len = len;
        f
    }

    #[test]
    fn test_insert_and_get() {
        let table = ShardedInodeTable::new(4);
        table.insert(1, make_file(1, 100));
        table.insert(2, make_file(2, 200));

        let len = table.with_file(1, |f| f.len);
        assert_eq!(len, Some(100));

        let len = table.with_file(2, |f| f.len);
        assert_eq!(len, Some(200));

        assert_eq!(table.with_file(999, |_| ()), None);
    }

    #[test]
    fn test_mutate() {
        let table = ShardedInodeTable::new(4);
        table.insert(1, make_file(1, 100));

        table.with_file_mut(1, |f| {
            f.len = 500;
        });

        let len = table.with_file(1, |f| f.len);
        assert_eq!(len, Some(500));
    }

    #[test]
    fn test_remove() {
        let table = ShardedInodeTable::new(4);
        table.insert(1, make_file(1, 100));

        let removed = table.remove(1);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().len, 100);

        assert_eq!(table.with_file(1, |f| f.len), None);
    }

    #[test]
    fn test_two_files_same_shard() {
        let table = ShardedInodeTable::new(2);
        // Both id=0 and id=2 map to shard 0
        table.insert(0, make_file(0, 100));
        table.insert(2, make_file(2, 200));

        let result = table.with_two_files_mut(0, 2, |f1, f2| {
            f1.len = 111;
            f2.len = 222;
        });
        assert!(result.is_some());

        assert_eq!(table.with_file(0, |f| f.len), Some(111));
        assert_eq!(table.with_file(2, |f| f.len), Some(222));
    }

    #[test]
    fn test_two_files_different_shard() {
        let table = ShardedInodeTable::new(2);
        // id=0 maps to shard 0, id=1 maps to shard 1
        table.insert(0, make_file(0, 100));
        table.insert(1, make_file(1, 200));

        let result = table.with_two_files_mut(0, 1, |f1, f2| {
            f1.len = 111;
            f2.len = 222;
        });
        assert!(result.is_some());

        assert_eq!(table.with_file(0, |f| f.len), Some(111));
        assert_eq!(table.with_file(1, |f| f.len), Some(222));
    }

    #[test]
    fn test_len_and_empty() {
        let table = ShardedInodeTable::new(4);
        assert!(table.is_empty());

        table.insert(1, make_file(1, 100));
        table.insert(2, make_file(2, 200));
        assert_eq!(table.len(), 2);

        table.remove(1);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let table = Arc::new(ShardedInodeTable::new(64));
        let mut handles = vec![];

        // Insert from multiple threads
        for i in 0..100 {
            let t = Arc::clone(&table);
            handles.push(thread::spawn(move || {
                t.insert(i, make_file(i, i as i64 * 10));
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Verify all inserted
        assert_eq!(table.len(), 100);

        // Read from multiple threads
        let mut handles = vec![];
        for i in 0..100 {
            let t = Arc::clone(&table);
            handles.push(thread::spawn(move || t.with_file(i, |f| f.len).unwrap()));
        }
        for (i, h) in handles.into_iter().enumerate() {
            assert_eq!(h.join().unwrap(), i as i64 * 10);
        }
    }

    #[test]
    fn test_shard_mask() {
        let table = ShardedInodeTable::new(64);
        assert_eq!(table.shard_mask, 63);
        assert_eq!(table.shard_count(), 64);
    }
}
