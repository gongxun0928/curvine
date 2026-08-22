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

//! Typed ID domains for the dual-mode metadata split (task #3, phase 0
//! contract §2.1). This module sits below both the fs inode layer and the
//! cache layer: `inode_id` and `meta::cache` both depend on it, never on
//! each other.
//!
//! There is exactly one block-id codec in the codebase ([`BlockIdCodec`]).
//! Filesystem inodes and cache objects are two disjoint owner ranges inside
//! that codec:
//!
//! ```text
//! fs inode id:    [1, 2^38)      allocated by InodeId
//! cache object:   [2^38, 2^39)    allocated by CacheObjectId
//! block_id:       (owner << 24) | seq, owner < 2^39, seq < 2^24
//! ```
//!
//! Cache block layouts are fully derived from
//! `(object_id, len, block_size)`; no block-id list is ever stored.
use curvine_core_error::{err_box, CommonResult};
use curvine_runtime::sync::AtomicLong;

/// The single shared block-id codec. Both fs and cache owners encode and
/// decode through these functions; nothing may re-derive block ids with a
/// private mask (contract §2.1).
pub struct BlockIdCodec;

impl BlockIdCodec {
    pub const SEQ_BYTES: i64 = 24;
    /// Highest owner value any block id may carry (both domains).
    pub const OWNER_MAX: i64 = (1i64 << 39) - 1;
    /// Decode mask. Kept at 40 bits to preserve the pre-split
    /// `InodeId::get_id` behavior for existing journals; a 39-bit mask would
    /// be equally valid for codec-produced ids but narrowing the historical
    /// mask is a behavior change this phase must not make.
    pub const DECODE_OWNER_MASK: i64 = (1i64 << 40) - 1;
    pub const SEQ_MASK: i64 = (1i64 << Self::SEQ_BYTES) - 1;

    /// Filesystem inode upper bound (inclusive): `2^38 - 1`.
    pub const FS_INODE_MAX: i64 = (1i64 << 38) - 1;
    /// Cache object lower bound (inclusive): `2^38`.
    pub const CACHE_OBJECT_MIN: i64 = 1i64 << 38;
    /// Cache object upper bound (inclusive): `2^39 - 1`.
    pub const CACHE_OBJECT_MAX: i64 = Self::OWNER_MAX;
    /// Highest block sequence number (inclusive): `2^24 - 1`.
    pub const BLOCK_SEQ_MAX: i64 = Self::SEQ_MASK;

    /// Encode `(owner, seq)` into a block id. Rejects negative values and
    /// values outside the codec range. The sign bit is never set because
    /// `OWNER_MAX << 24 | SEQ_MASK < 2^63`.
    pub fn encode_block_id(owner: i64, seq: i64) -> CommonResult<i64> {
        if owner < 0 {
            return err_box!("block owner must be non-negative: {}", owner);
        }
        if owner > Self::OWNER_MAX {
            return err_box!(
                "block owner exceeds maximum value {}: {}",
                Self::OWNER_MAX,
                owner
            );
        }
        if seq < 0 {
            return err_box!("block seq must be non-negative: {}", seq);
        }
        if seq > Self::SEQ_MASK {
            return err_box!(
                "block seq exceeds maximum value {}: {}",
                Self::SEQ_MASK,
                seq
            );
        }

        Ok((owner << Self::SEQ_BYTES) | seq)
    }

    /// Extract the owner from a block id. Raw 40-bit extractor kept only for
    /// migrating legacy call sites; new domain logic, block reports, and GC
    /// must use [`BlockIdCodec::block_owner`] (which validates) instead.
    pub fn get_owner(block_id: i64) -> i64 {
        (block_id >> Self::SEQ_BYTES) & Self::DECODE_OWNER_MASK
    }

    /// Extract the sequence from a block id.
    pub fn get_seq(block_id: i64) -> i64 {
        block_id & Self::SEQ_MASK
    }

    /// Typed, validating owner decode: the physical owner field is 40 bits
    /// but only 39-bit owners (bit 39 clear) are legal, and negative block
    /// ids are always rejected. This is the decode path for cache-domain
    /// classification, block reports, and GC.
    pub fn block_owner(block_id: i64) -> CommonResult<i64> {
        if block_id < 0 {
            return err_box!("block id must be non-negative: {}", block_id);
        }
        let owner = Self::get_owner(block_id);
        if owner > Self::OWNER_MAX {
            return err_box!(
                "block id owner field exceeds legal 39-bit range: {} (block id {})",
                owner,
                block_id
            );
        }
        Ok(owner)
    }

    /// Typed, validating cache-domain check for a block id.
    pub fn is_cache_block_id(block_id: i64) -> CommonResult<bool> {
        Ok(Self::is_cache_owner(Self::block_owner(block_id)?))
    }

    /// Whether `owner` belongs to the cache object domain.
    pub fn is_cache_owner(owner: i64) -> bool {
        (Self::CACHE_OBJECT_MIN..=Self::CACHE_OBJECT_MAX).contains(&owner)
    }
}

/// Monotonic allocator for cache object ids. Ids start at
/// [`BlockIdCodec::CACHE_OBJECT_MIN`] and are never reused after recovery:
/// the watermark is journaled with every reservation and restored on replay,
/// and it only moves forward.
pub struct CacheObjectId(AtomicLong);

impl Default for CacheObjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheObjectId {
    pub fn new() -> Self {
        Self(AtomicLong::new(BlockIdCodec::CACHE_OBJECT_MIN - 1))
    }

    pub fn next(&self) -> CommonResult<i64> {
        let id = self.0.next();
        if id > BlockIdCodec::CACHE_OBJECT_MAX {
            err_box!(
                "cache object id exceeds maximum value {}",
                BlockIdCodec::CACHE_OBJECT_MAX
            )
        } else {
            Ok(id)
        }
    }

    /// Peek the last issued id (the watermark to persist).
    pub fn current(&self) -> i64 {
        self.0.get()
    }

    /// Restore the watermark on replay/snapshot restore. Only moves forward,
    /// and the restored value must stay inside the cache domain (a corrupt
    /// snapshot cannot push the allocator out of range).
    pub fn reset(&self, new_value: i64) -> CommonResult<()> {
        if new_value < BlockIdCodec::CACHE_OBJECT_MIN - 1 {
            return err_box!(
                "cache object id watermark below domain start: {}",
                new_value
            );
        }
        if new_value > BlockIdCodec::CACHE_OBJECT_MAX {
            return err_box!(
                "cache object id watermark above domain end {}: {}",
                BlockIdCodec::CACHE_OBJECT_MAX,
                new_value
            );
        }
        loop {
            let c = self.current();
            if new_value < c {
                return err_box!(
                    "cannot reset cache object id to less than the current value: {}, where newValue {}",
                    c,
                    new_value
                );
            }
            if self.0.compare_and_set(c, new_value) {
                return Ok(());
            }
        }
    }
}

/// Constant-size derived block layout for a cache object (contract §2.1):
///
/// ```text
/// n = 0                                  when len == 0
/// n = ceil(len / block_size)             when len > 0
/// block_id(i) = (object_id << 24) | i     i in 1..=n
/// last_len = len - (n - 1) * block_size   when n > 0
/// ```
///
/// `block_size` is immutable entry metadata. Sequence numbers start at 1;
/// 0 is the reserved "no block" value and never identifies a cache block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheBlockLayout {
    pub object_id: i64,
    pub len: i64,
    pub block_size: i64,
    /// Number of blocks, `0 <= n <= BLOCK_SEQ_MAX`.
    pub block_count: i64,
    /// Length of the last block; `0` when `block_count == 0`.
    pub last_len: i64,
}

impl CacheBlockLayout {
    pub fn derive(object_id: i64, len: i64, block_size: i64) -> CommonResult<Self> {
        if !BlockIdCodec::is_cache_owner(object_id) {
            return err_box!("object id outside cache domain: {}", object_id);
        }
        if len < 0 {
            return err_box!("object length must be non-negative: {}", len);
        }
        if block_size <= 0 {
            return err_box!("block size must be positive: {}", block_size);
        }

        // Overflow-free ceiling division: no addition or multiplication is
        // involved, so extreme (block_size, len) combinations cannot wrap.
        let n = len / block_size + i64::from(len % block_size != 0);
        if n > BlockIdCodec::BLOCK_SEQ_MAX {
            return err_box!(
                "block count {} exceeds maximum {} (len {}, block size {})",
                n,
                BlockIdCodec::BLOCK_SEQ_MAX,
                len,
                block_size
            );
        }

        let last_len = if n == 0 {
            0
        } else {
            len - (n - 1) * block_size
        };

        Ok(Self {
            object_id,
            len,
            block_size,
            block_count: n,
            last_len,
        })
    }

    /// Derived block id for the 1-based `index` in `1..=block_count`.
    pub fn block_id(&self, index: i64) -> CommonResult<i64> {
        if index < 1 || index > self.block_count {
            return err_box!(
                "block index {} out of range 1..={}",
                index,
                self.block_count
            );
        }
        BlockIdCodec::encode_block_id(self.object_id, index)
    }

    /// Iterate all derived block ids without materializing a `Vec`
    /// (contract §5: no `1..=n` block-id list in one call).
    pub fn block_ids(&self) -> impl Iterator<Item = CommonResult<i64>> + '_ {
        (1..=self.block_count).map(|i| self.block_id(i))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_domain_bounds() {
        assert_eq!(BlockIdCodec::FS_INODE_MAX, (1i64 << 38) - 1);
        assert_eq!(BlockIdCodec::CACHE_OBJECT_MIN, 1i64 << 38);
        assert_eq!(BlockIdCodec::CACHE_OBJECT_MAX, (1i64 << 39) - 1);
        assert_eq!(BlockIdCodec::BLOCK_SEQ_MAX, (1i64 << 24) - 1);

        // Domains are disjoint and contiguous across the full owner range.
        assert!(!BlockIdCodec::is_cache_owner(0));
        assert!(!BlockIdCodec::is_cache_owner(BlockIdCodec::FS_INODE_MAX));
        assert!(BlockIdCodec::is_cache_owner(BlockIdCodec::CACHE_OBJECT_MIN));
        assert!(BlockIdCodec::is_cache_owner(BlockIdCodec::CACHE_OBJECT_MAX));
        assert!(!BlockIdCodec::is_cache_owner(
            BlockIdCodec::CACHE_OBJECT_MAX + 1
        ));
    }

    #[test]
    fn test_codec_roundtrip_and_rejections() {
        for owner in [
            1i64,
            BlockIdCodec::FS_INODE_MAX,
            BlockIdCodec::CACHE_OBJECT_MIN,
            BlockIdCodec::CACHE_OBJECT_MAX,
        ] {
            for seq in [0i64, 1, 7, BlockIdCodec::SEQ_MASK] {
                let id = BlockIdCodec::encode_block_id(owner, seq).unwrap();
                assert!(id >= 0, "sign bit must never be set");
                assert_eq!(BlockIdCodec::get_owner(id), owner);
                assert_eq!(BlockIdCodec::get_seq(id), seq);
            }
        }

        // Negative and overflow rejection.
        assert!(BlockIdCodec::encode_block_id(-1, 0).is_err());
        assert!(BlockIdCodec::encode_block_id(0, -1).is_err());
        assert!(BlockIdCodec::encode_block_id(BlockIdCodec::OWNER_MAX + 1, 0).is_err());
        assert!(BlockIdCodec::encode_block_id(0, BlockIdCodec::SEQ_MASK + 1).is_err());

        // Highest encodable id stays non-negative.
        let max_id =
            BlockIdCodec::encode_block_id(BlockIdCodec::OWNER_MAX, BlockIdCodec::SEQ_MASK).unwrap();
        assert!(max_id > 0);
        assert!(BlockIdCodec::is_cache_block_id(max_id).unwrap());

        // Fs-owner block id is not a cache block.
        let fs_id = BlockIdCodec::encode_block_id(42, 3).unwrap();
        assert!(!BlockIdCodec::is_cache_block_id(fs_id).unwrap());

        // Typed decode rejects negative ids and illegal bit-39 owners while
        // accepting every codec-produced id.
        assert!(BlockIdCodec::block_owner(-1).is_err());
        let illegal = ((1i64 << 39) << 24) | 1; // owner field has bit 39 set
        assert!(BlockIdCodec::block_owner(illegal).is_err());
        let legal = BlockIdCodec::encode_block_id(BlockIdCodec::CACHE_OBJECT_MAX, 5).unwrap();
        assert_eq!(
            BlockIdCodec::block_owner(legal).unwrap(),
            BlockIdCodec::CACHE_OBJECT_MAX
        );
        assert!(BlockIdCodec::is_cache_block_id(legal).unwrap());
        assert!(
            !BlockIdCodec::is_cache_block_id(BlockIdCodec::encode_block_id(7, 1).unwrap()).unwrap()
        );
    }

    #[test]
    fn test_cache_object_allocator() {
        let alloc = CacheObjectId::new();
        assert_eq!(alloc.next().unwrap(), BlockIdCodec::CACHE_OBJECT_MIN);
        assert_eq!(alloc.next().unwrap(), BlockIdCodec::CACHE_OBJECT_MIN + 1);
        assert_eq!(alloc.current(), BlockIdCodec::CACHE_OBJECT_MIN + 1);

        // Watermark restore is monotonic and range-checked both ways.
        alloc.reset(BlockIdCodec::CACHE_OBJECT_MAX).unwrap();
        assert!(alloc.next().is_err(), "exhausted domain must fail");
        assert!(alloc.reset(BlockIdCodec::CACHE_OBJECT_MIN).is_err());
        assert!(alloc.reset(BlockIdCodec::CACHE_OBJECT_MAX + 1).is_err());
        assert!(alloc.reset(BlockIdCodec::CACHE_OBJECT_MIN - 2).is_err());

        // A fresh allocator accepts the exact domain-start watermark.
        CacheObjectId::new()
            .reset(BlockIdCodec::CACHE_OBJECT_MIN - 1)
            .unwrap();
    }

    #[test]
    fn test_layout_derivation() {
        let obj = BlockIdCodec::CACHE_OBJECT_MIN;

        // Zero-length object: no blocks.
        let l = CacheBlockLayout::derive(obj, 0, 128).unwrap();
        assert_eq!((l.block_count, l.last_len), (0, 0));
        assert!(l.block_id(1).is_err());

        // Exactly one block.
        let l = CacheBlockLayout::derive(obj, 128, 128).unwrap();
        assert_eq!((l.block_count, l.last_len), (1, 128));

        // Uneven tail: last block shorter.
        let l = CacheBlockLayout::derive(obj, 300, 128).unwrap();
        assert_eq!((l.block_count, l.last_len), (3, 44));
        assert_eq!(l.block_id(1).unwrap(), (obj << 24) | 1);
        assert_eq!(l.block_id(3).unwrap(), (obj << 24) | 3);
        assert!(l.block_id(0).is_err());
        assert!(l.block_id(4).is_err());

        // Block ids are sequential and lazy (no Vec materialized).
        assert_eq!(l.block_ids().count(), 3);

        // Overflow: n > BLOCK_SEQ_MAX rejected.
        let huge_len = (BlockIdCodec::BLOCK_SEQ_MAX + 1) * 128;
        assert!(CacheBlockLayout::derive(obj, huge_len, 128).is_err());
        // ...but the maximum legal count is accepted.
        let max_len = BlockIdCodec::BLOCK_SEQ_MAX * 128;
        let l = CacheBlockLayout::derive(obj, max_len, 128).unwrap();
        assert_eq!(l.block_count, BlockIdCodec::BLOCK_SEQ_MAX);

        // Extreme block sizes: a huge single-block object is legal and the
        // derivation must not overflow on (block_size, len) combinations
        // that a multiplication-based bound would wrongly reject.
        let l = CacheBlockLayout::derive(obj, 1, i64::MAX).unwrap();
        assert_eq!((l.block_count, l.last_len), (1, 1));
        let l = CacheBlockLayout::derive(obj, i64::MAX, i64::MAX).unwrap();
        assert_eq!((l.block_count, l.last_len), (1, i64::MAX));
        let l = CacheBlockLayout::derive(obj, i64::MAX, i64::MAX - 1).unwrap();
        assert_eq!((l.block_count, l.last_len), (2, 1));
        // Count bound still enforced independently of sizes: n = i64::MAX
        // with block_size 1 exceeds BLOCK_SEQ_MAX.
        assert!(CacheBlockLayout::derive(obj, i64::MAX, 1).is_err());

        // Validation: block_size > 0, len >= 0, cache-domain object id.
        assert!(CacheBlockLayout::derive(obj, 10, 0).is_err());
        assert!(CacheBlockLayout::derive(obj, -1, 128).is_err());
        assert!(CacheBlockLayout::derive(1, 10, 128).is_err());
    }
}
