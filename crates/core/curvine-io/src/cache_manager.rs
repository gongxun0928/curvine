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

use curvine_sys::{self, CInt, SysResult};
use std::fs::File;

const LONG_READ_THRESHOLD_LEN: i64 = 256 * 1024;

#[allow(unused)]
#[derive(Debug)]
pub struct ReadAheadTask {
    off: i64,
    len: i64,
    handle: SysResult<CInt>,
    /// End offset (exclusive) of the most recently served read frame:
    /// `frame_start + actual_returned_len`. The continuity test for the next
    /// read is `cur_pos == last_served_end`, which stays correct when transfer
    /// frames are variable-sized (demand-aware `read_len`). The worker/client
    /// must call `record_served` after each frame is actually read.
    last_served_end: i64,
    /// Whether the access that created this task was classified sequential
    /// (and therefore issued real read-ahead advice). Kept for tests and
    /// diagnostics.
    pub is_sequential: bool,
}

impl ReadAheadTask {
    /// Record the end of a served read frame (`start_off + actual_len`) so the
    /// next continuity test compares against what was actually returned, not a
    /// fixed frame size. Callers must invoke this after every completed read,
    /// including short/legacy frames.
    pub fn record_served(&mut self, start_off: i64, actual_len: i64) {
        self.last_served_end = start_off.saturating_add(actual_len);
    }
}

/// Operating system page cache manager, currently only supports linux。
#[allow(unused)]
#[derive(Debug, Clone)]
pub struct CacheManager {
    pub read_ahead_len: i64,
    pub drop_cache_len: i64,
    pub enable: bool,
    pub chunk_size: i64,
}

impl CacheManager {
    pub fn new(enable: bool, read_ahead_len: i64, drop_cache_len: i64, chunk_size: i64) -> Self {
        let enable = if cfg!(target_os = "linux") {
            if read_ahead_len <= 0 {
                false
            } else {
                enable
            }
        } else {
            false
        };

        CacheManager {
            read_ahead_len,
            drop_cache_len,
            enable,
            chunk_size,
        }
    }

    pub fn with_place() -> Self {
        Self::new(false, 4 * 1024 * 1024, 1024 * 1024, 128 * 1024 * 1024)
    }

    /// Performs read-ahead operation with simple sequential/random read detection.
    ///
    /// This method uses a simple strategy to detect random reads and disable read-ahead
    /// when random access patterns are detected. This helps reduce read amplification
    /// caused by unnecessary prefetching.
    ///
    /// # Random Read Detection Strategy
    ///
    /// The method detects random reads by comparing the current read position with
    /// the end of the previously served frame:
    /// - Sequential read: `cur_pos == last_served_end` (frame start + actual returned length)
    /// - Random read: `cur_pos != last_served_end` (position jump or re-read detected)
    ///
    /// Continuity is judged by the ACTUAL bytes served, not by the Open-time
    /// chunk size, so the classification is independent of transfer frame size
    /// (demand-aware `read_len` frames may be larger or smaller than chunk_size).
    ///
    /// When a random read is detected, read-ahead is disabled to avoid prefetching
    /// data that is unlikely to be used, reducing unnecessary I/O operations and
    /// read amplification.
    ///
    /// # Backward Seek Handling
    ///
    /// When a backward seek is detected (current position < last served end),
    /// read-ahead advice is suppressed for that access; it resumes once reads
    /// are contiguous with the served stream again.
    ///
    /// # Arguments
    ///
    /// * `file` - The file handle to perform read-ahead on
    /// * `cur_pos` - Current read position
    /// * `total_len` - Total file length
    /// * `last_task` - Previous read-ahead task (contains last served end)
    ///
    /// # Returns
    ///
    /// Returns `Some(ReadAheadTask)` if read-ahead should be performed, `None` otherwise.
    pub fn read_ahead(
        &self,
        file: &File,
        cur_pos: i64,
        total_len: i64,
        last_task: Option<ReadAheadTask>,
    ) -> Option<ReadAheadTask> {
        // The file is greater than 256kb, use the read-previous API.It is not necessary to use pre-reading of small files.
        if !self.enable || total_len < LONG_READ_THRESHOLD_LEN {
            // If read preview is not supported, no error will be returned.
            return None;
        };

        // Determine last offset and sequential status, handling backward seeks
        let (last_offset, is_sequential) = match last_task.as_ref() {
            Some(t) if cur_pos < t.last_served_end => (i64::MIN, false),

            Some(t) => (t.off, t.last_served_end == cur_pos),

            None => (i64::MIN, true),
        };

        // When cur_pos reaches halfway point, trigger read-ahead
        let next_offset = last_offset + self.read_ahead_len / 2;
        if cur_pos >= next_offset {
            let len = self.read_ahead_len.min(i64::MAX - cur_pos);
            if len <= 0 {
                None
            } else {
                let handle = if is_sequential {
                    curvine_sys::read_ahead(file, cur_pos, len)
                } else {
                    Ok(0)
                };

                Some(ReadAheadTask {
                    off: cur_pos,
                    len,
                    handle,
                    last_served_end: cur_pos,
                    is_sequential,
                })
            }
        } else {
            // Frame continuity state (last_served_end) is maintained by the
            // caller via `record_served` once the actual bytes are returned.
            last_task
        }
    }
}

impl Default for CacheManager {
    fn default() -> Self {
        Self::with_place()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Continuity must be judged by the ACTUAL bytes served, not by the
    // Open-time chunk size: a demand-aware 512 KiB frame followed by a read
    // at its end is sequential. With the old fixed-chunk arithmetic
    // (`last_read_off + chunk_size == cur_pos`) this read was misclassified
    // as random and read-ahead advice was silently dropped.
    #[test]
    fn test_read_ahead_sequential_after_large_frame() {
        if !cfg!(target_os = "linux") {
            return;
        }

        let dir = std::env::temp_dir().join("cv-cache-manager-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("large-frame-seq.bin");
        std::fs::write(&path, vec![0u8; 1024 * 1024]).unwrap();
        let file = std::fs::File::open(&path).unwrap();

        // chunk_size = 128 KiB (the default transfer frame), read_ahead 128 KiB
        // so each new frame position crosses the half-window re-check.
        let cm = CacheManager::new(true, 128 * 1024, 1024 * 1024, 128 * 1024);
        assert!(cm.enable);

        // First read at 0: no history, sequential by definition.
        let t1 = cm.read_ahead(&file, 0, 1024 * 1024, None).unwrap();
        assert!(t1.is_sequential);

        // A demand-aware frame serves 512 KiB starting at 0.
        let mut t1 = t1;
        t1.record_served(0, 512 * 1024);

        // Next read at 512 KiB: crosses off + read_ahead_len/2, so a new task
        // is created. It must still be classified sequential.
        let t2 = cm
            .read_ahead(&file, 512 * 1024, 1024 * 1024, Some(t1))
            .unwrap();
        assert!(t2.is_sequential, "large-frame continuation misclassified");
        assert_eq!(t2.last_served_end, 512 * 1024);

        // Fixed-frame parity: a default 128 KiB frame keeps working too.
        let mut t2 = t2;
        t2.record_served(512 * 1024, 128 * 1024);
        let t3 = cm
            .read_ahead(&file, 640 * 1024, 1024 * 1024, Some(t2))
            .unwrap();
        assert!(t3.is_sequential);

        std::fs::remove_file(&path).ok();
    }

    // A position jump forward past the served end is random: no read-ahead.
    #[test]
    fn test_read_ahead_random_jump() {
        if !cfg!(target_os = "linux") {
            return;
        }

        let dir = std::env::temp_dir().join("cv-cache-manager-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("random-jump.bin");
        std::fs::write(&path, vec![0u8; 1024 * 1024]).unwrap();
        let file = std::fs::File::open(&path).unwrap();

        let cm = CacheManager::new(true, 512 * 1024, 1024 * 1024, 128 * 1024);

        let t1 = cm.read_ahead(&file, 0, 1024 * 1024, None).unwrap();
        let mut t1 = t1;
        t1.record_served(0, 128 * 1024);

        // Jump to 512 KiB: not contiguous with the 128 KiB served end.
        let t2 = cm
            .read_ahead(&file, 512 * 1024, 1024 * 1024, Some(t1))
            .unwrap();
        assert!(!t2.is_sequential);

        std::fs::remove_file(&path).ok();
    }

    // record_served tracks the actual end including short/EOF frames.
    #[test]
    fn test_record_served() {
        let dir = std::env::temp_dir().join("cv-cache-manager-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("record-served.bin");
        std::fs::write(&path, vec![0u8; 512 * 1024]).unwrap();
        let file = std::fs::File::open(&path).unwrap();

        let cm = CacheManager::new(true, 256 * 1024, 1024 * 1024, 128 * 1024);
        let mut t = cm.read_ahead(&file, 0, 512 * 1024, None).unwrap();

        t.record_served(100 * 1024, 3);
        assert_eq!(t.last_served_end, 100 * 1024 + 3);

        // Empty frame (EOF): end stays at the frame start.
        t.record_served(200 * 1024, 0);
        assert_eq!(t.last_served_end, 200 * 1024);

        std::fs::remove_file(&path).ok();
    }
}
