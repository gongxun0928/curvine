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

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

const DEFAULT_LOCK_POOL_SIZE: usize = 1 << 16; // 65536

/// Fixed-size lock pool for path-level inode locking (lock striping).
///
/// Instead of one global RwLock for the entire directory tree, we use N
/// independent RwLock instances. Each inode maps to a lock slot via
/// `hash(inode_id) % N`. This allows concurrent operations on different
/// inodes while ensuring mutual exclusion on the same inode.
///
/// Memory overhead: N × sizeof(RwLock<()>) ≈ 65536 × 1 byte ≈ 64 KB
/// (parking_lot::RwLock<()> is 1 byte).
pub struct InodeLockPool {
    locks: Box<[RwLock<()>]>,
    size: usize,
}

impl InodeLockPool {
    pub fn new() -> Self {
        Self::with_size(DEFAULT_LOCK_POOL_SIZE)
    }

    pub fn with_size(size: usize) -> Self {
        let locks: Vec<RwLock<()>> = (0..size).map(|_| RwLock::new(())).collect();
        Self {
            locks: locks.into_boxed_slice(),
            size,
        }
    }

    #[inline]
    fn slot(&self, inode_id: i64) -> usize {
        // Multiplicative hash to distribute adjacent inode IDs across slots
        let h = (inode_id as u64).wrapping_mul(0x517cc1b727220a95);
        (h as usize) % self.size
    }

    /// Acquire a read lock for the given inode.
    pub fn read(&self, inode_id: i64) -> RwLockReadGuard<'_, ()> {
        self.locks[self.slot(inode_id)].read()
    }

    /// Acquire a write lock for the given inode.
    pub fn write(&self, inode_id: i64) -> RwLockWriteGuard<'_, ()> {
        self.locks[self.slot(inode_id)].write()
    }

    /// Acquire write locks on two inodes in a deadlock-free order.
    /// Locks are acquired in ascending slot order to prevent deadlocks.
    /// Returns guards for (id_a's lock, id_b's lock) regardless of acquisition order.
    pub fn write_two_ordered(
        &self,
        id_a: i64,
        id_b: i64,
    ) -> TwoWriteGuards<'_> {
        let slot_a = self.slot(id_a);
        let slot_b = self.slot(id_b);
        if slot_a == slot_b {
            let g = self.locks[slot_a].write();
            TwoWriteGuards::Same(g)
        } else if slot_a < slot_b {
            let g1 = self.locks[slot_a].write();
            let g2 = self.locks[slot_b].write();
            TwoWriteGuards::Two(g1, g2)
        } else {
            let g1 = self.locks[slot_b].write();
            let g2 = self.locks[slot_a].write();
            TwoWriteGuards::Two(g2, g1)
        }
    }
}

impl Default for InodeLockPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Guards returned by `write_two_ordered`. Holds one or two write locks.
pub enum TwoWriteGuards<'a> {
    /// Both inode IDs mapped to the same slot; single lock suffices.
    Same(RwLockWriteGuard<'a, ()>),
    /// Two distinct slots locked in order. First = id_a's lock, Second = id_b's lock.
    Two(RwLockWriteGuard<'a, ()>, RwLockWriteGuard<'a, ()>),
}

/// Holds all locks acquired along a path traversal (root → target).
/// Locks are released in reverse order (LIFO) when this guard is dropped.
pub struct PathLockGuard<'a> {
    _locks: Vec<LockGuard<'a>>,
}

enum LockGuard<'a> {
    Read(RwLockReadGuard<'a, ()>),
    Write(RwLockWriteGuard<'a, ()>),
}

impl<'a> PathLockGuard<'a> {
    pub fn new() -> Self {
        Self { _locks: Vec::with_capacity(8) }
    }

    pub fn push_read(&mut self, guard: RwLockReadGuard<'a, ()>) {
        self._locks.push(LockGuard::Read(guard));
    }

    pub fn push_write(&mut self, guard: RwLockWriteGuard<'a, ()>) {
        self._locks.push(LockGuard::Write(guard));
    }
}

impl Default for PathLockGuard<'_> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_pool_basic() {
        let pool = InodeLockPool::new();

        // Read locks on different inodes should not block each other
        let _r1 = pool.read(1);
        let _r2 = pool.read(2);
        let _r3 = pool.read(100000);
    }

    #[test]
    fn test_lock_pool_same_inode_read() {
        let pool = InodeLockPool::new();
        // Multiple read locks on same inode should coexist
        let _r1 = pool.read(42);
        let _r2 = pool.read(42);
    }

    #[test]
    fn test_write_two_ordered_different_slots() {
        let pool = InodeLockPool::new();
        let _guards = pool.write_two_ordered(1, 2);
    }

    #[test]
    fn test_write_two_ordered_same_slot() {
        let pool = InodeLockPool::with_size(1); // Force collision
        let _guards = pool.write_two_ordered(1, 2);
    }

    #[test]
    fn test_distribution() {
        let pool = InodeLockPool::new();
        // Adjacent IDs should map to different slots (multiplicative hash)
        let slot1 = pool.slot(1000);
        let slot2 = pool.slot(1001);
        assert_ne!(slot1, slot2, "Adjacent IDs should hash to different slots");
    }
}
