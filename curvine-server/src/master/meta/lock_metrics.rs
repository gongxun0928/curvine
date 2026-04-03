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

//! Lock metrics for measuring FsDir lock contention.
//!
//! Records lock wait time and hold time per operation type.
//! Used to quantify the benefit of lock refactoring.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

/// Operation types for lock metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockOp {
    Create,
    Delete,
    Rename,
    Mkdir,
    Open,
    Close,
    AddBlock,
    CompleteFile,
    GetBlockLocations,
    FileStatus,
    ListStatus,
    SetAttr,
    Symlink,
    Link,
    Free,
    BlockReport,
    Mount,
    Other,
}

impl LockOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            LockOp::Create => "create",
            LockOp::Delete => "delete",
            LockOp::Rename => "rename",
            LockOp::Mkdir => "mkdir",
            LockOp::Open => "open",
            LockOp::Close => "close",
            LockOp::AddBlock => "add_block",
            LockOp::CompleteFile => "complete_file",
            LockOp::GetBlockLocations => "get_block_locations",
            LockOp::FileStatus => "file_status",
            LockOp::ListStatus => "list_status",
            LockOp::SetAttr => "set_attr",
            LockOp::Symlink => "symlink",
            LockOp::Link => "link",
            LockOp::Free => "free",
            LockOp::BlockReport => "block_report",
            LockOp::Mount => "mount",
            LockOp::Other => "other",
        }
    }
}

/// Per-operation lock metrics.
#[derive(Debug, Default)]
struct OpMetrics {
    /// Total time spent waiting to acquire the lock (nanoseconds).
    wait_time_ns: AtomicU64,
    /// Total time the lock was held (nanoseconds).
    hold_time_ns: AtomicU64,
    /// Number of lock acquisitions.
    count: AtomicU64,
}

/// Global lock metrics collector.
pub struct LockMetrics {
    enabled: AtomicBool,
    metrics: [OpMetrics; 18], // One per LockOp variant
}

impl LockMetrics {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            metrics: Default::default(),
        }
    }

    /// Enable or disable metrics collection.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// Check if metrics collection is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    fn op_index(op: LockOp) -> usize {
        match op {
            LockOp::Create => 0,
            LockOp::Delete => 1,
            LockOp::Rename => 2,
            LockOp::Mkdir => 3,
            LockOp::Open => 4,
            LockOp::Close => 5,
            LockOp::AddBlock => 6,
            LockOp::CompleteFile => 7,
            LockOp::GetBlockLocations => 8,
            LockOp::FileStatus => 9,
            LockOp::ListStatus => 10,
            LockOp::SetAttr => 11,
            LockOp::Symlink => 12,
            LockOp::Link => 13,
            LockOp::Free => 14,
            LockOp::BlockReport => 15,
            LockOp::Mount => 16,
            LockOp::Other => 17,
        }
    }

    /// Record a lock acquisition event.
    pub fn record(&self, op: LockOp, wait_time_ns: u64, hold_time_ns: u64) {
        if !self.is_enabled() {
            return;
        }
        let idx = Self::op_index(op);
        let m = &self.metrics[idx];
        m.wait_time_ns.fetch_add(wait_time_ns, Ordering::Relaxed);
        m.hold_time_ns.fetch_add(hold_time_ns, Ordering::Relaxed);
        m.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a snapshot of all metrics and reset counters.
    pub fn snapshot(&self) -> LockMetricsSnapshot {
        let mut entries = Vec::new();
        for (i, m) in self.metrics.iter().enumerate() {
            let count = m.count.swap(0, Ordering::Relaxed);
            if count > 0 {
                let wait = m.wait_time_ns.swap(0, Ordering::Relaxed);
                let hold = m.hold_time_ns.swap(0, Ordering::Relaxed);
                let op_name = match i {
                    0 => "create",
                    1 => "delete",
                    2 => "rename",
                    3 => "mkdir",
                    4 => "open",
                    5 => "close",
                    6 => "add_block",
                    7 => "complete_file",
                    8 => "get_block_locations",
                    9 => "file_status",
                    10 => "list_status",
                    11 => "set_attr",
                    12 => "symlink",
                    13 => "link",
                    14 => "free",
                    15 => "block_report",
                    16 => "mount",
                    17 => "other",
                    _ => "unknown",
                };
                entries.push(OpSnapshot {
                    op: op_name.to_string(),
                    count,
                    total_wait_us: wait / 1000,
                    total_hold_us: hold / 1000,
                    avg_wait_us: wait / count / 1000,
                    avg_hold_us: hold / count / 1000,
                });
            }
        }
        LockMetricsSnapshot { entries }
    }

    /// Reset all counters.
    pub fn reset(&self) {
        for m in &self.metrics {
            m.wait_time_ns.store(0, Ordering::Relaxed);
            m.hold_time_ns.store(0, Ordering::Relaxed);
            m.count.store(0, Ordering::Relaxed);
        }
    }
}

/// RAII guard for measuring lock wait and hold time.
///
/// Usage:
/// ```ignore
/// let start = Instant::now();
/// let guard = fs_dir.write().unwrap();
/// let wait_time = start.elapsed();
/// let _lock_guard = LockWaitHoldGuard::new(&lock_metrics, LockOp::Create, wait_time);
/// // ... do work ...
/// // _lock_guard dropped here, records hold time
/// ```
pub struct LockWaitHoldGuard<'a> {
    metrics: &'a LockMetrics,
    op: LockOp,
    wait_time_ns: u64,
    acquire_instant: Instant,
}

impl<'a> LockWaitHoldGuard<'a> {
    pub fn new(metrics: &'a LockMetrics, op: LockOp, wait_time: std::time::Duration) -> Self {
        Self {
            metrics,
            op,
            wait_time_ns: wait_time.as_nanos() as u64,
            acquire_instant: Instant::now(),
        }
    }
}

impl<'a> Drop for LockWaitHoldGuard<'a> {
    fn drop(&mut self) {
        let hold_time_ns = self.acquire_instant.elapsed().as_nanos() as u64;
        self.metrics
            .record(self.op, self.wait_time_ns, hold_time_ns);
    }
}

/// Snapshot of lock metrics for reporting.
#[derive(Debug)]
pub struct LockMetricsSnapshot {
    pub entries: Vec<OpSnapshot>,
}

impl std::fmt::Display for LockMetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "{:<20} {:>8} {:>12} {:>12} {:>10} {:>10}",
            "op", "count", "total_wait_us", "total_hold_us", "avg_wait", "avg_hold"
        )?;
        writeln!(f, "{}", "-".repeat(75))?;
        for e in &self.entries {
            writeln!(
                f,
                "{:<20} {:>8} {:>12} {:>12} {:>10} {:>10}",
                e.op, e.count, e.total_wait_us, e.total_hold_us, e.avg_wait_us, e.avg_hold_us
            )?;
        }
        Ok(())
    }
}

/// Per-operation metrics snapshot.
#[derive(Debug)]
pub struct OpSnapshot {
    pub op: String,
    pub count: u64,
    pub total_wait_us: u64,
    pub total_hold_us: u64,
    pub avg_wait_us: u64,
    pub avg_hold_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_record() {
        let metrics = LockMetrics::new();
        metrics.set_enabled(true);

        metrics.record(LockOp::Create, 1000, 5000);
        metrics.record(LockOp::Create, 2000, 6000);

        let snap = metrics.snapshot();
        assert_eq!(snap.entries.len(), 1);
        let e = &snap.entries[0];
        assert_eq!(e.op, "create");
        assert_eq!(e.count, 2);
        assert_eq!(e.total_wait_us, 3); // 3000ns = 3us
        assert_eq!(e.total_hold_us, 11); // 11000ns = 11us
    }

    #[test]
    fn test_metrics_disabled() {
        let metrics = LockMetrics::new();
        // Disabled by default
        metrics.record(LockOp::Create, 1000, 5000);

        let snap = metrics.snapshot();
        assert!(snap.entries.is_empty());
    }

    #[test]
    fn test_raii_guard() {
        let metrics = LockMetrics::new();
        metrics.set_enabled(true);

        {
            let _guard = LockWaitHoldGuard::new(
                &metrics,
                LockOp::Open,
                std::time::Duration::from_micros(100),
            );
            // hold for a bit
        }

        let snap = metrics.snapshot();
        assert_eq!(snap.entries.len(), 1);
        assert_eq!(snap.entries[0].op, "open");
        assert_eq!(snap.entries[0].count, 1);
    }

    #[test]
    fn test_snapshot_reset() {
        let metrics = LockMetrics::new();
        metrics.set_enabled(true);
        metrics.record(LockOp::Create, 1000, 5000);

        let snap = metrics.snapshot();
        assert_eq!(snap.entries.len(), 1);

        // Snapshot should have reset counters
        let snap2 = metrics.snapshot();
        assert!(snap2.entries.is_empty());
    }
}
