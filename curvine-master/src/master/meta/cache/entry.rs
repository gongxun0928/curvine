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

//! CacheIndex value types and key encodings (phase 0 contract §2/§4, rev2).
//!
//! RocksDB schema (all keys big-endian, fixed-size where noted):
//!
//! ```text
//! CF_CACHE_ENTRY
//!   key   = mount_incarnation:u64 ++ encoded_key
//!           (encoded_key = raw UTF-8 bytes, order-preserving; component
//!            safety is enforced at scan time via key_in_scope, never by
//!            byte-prefix matching — see encode_key)
//!   value = CacheEntry {generation, state, object_id, len, ufs_mtime,
//!                        block_size, expire_at}
//!   life  = one row per (incarnation, key); superseded by generation
//!           overwrite in the same row; dropped by prefix/mount remove.
//!
//! CF_CACHE_OBJECT
//!   key   = object_id:i64
//!   value = ObjectRow {incarnation, key, generation}
//!   life  = written with the entry, deleted when its version is superseded
//!           or reclaimed by GC; only a reverse hint for GC.
//!
//! CF_CACHE_EXPIRY
//!   key   = expire_at:i64 ++ mount_incarnation:u64 ++ object_id:i64
//!           (frozen at 4a; deterministic (expire_at, incarnation,
//!            object_id) order gives stable same-timestamp paging, 4c.1)
//!   value = (key:String, generation:u64)
//!   life  = written with a Valid entry carrying expire_at; deleted on
//!           supersede/remove by generation-CAS on the frozen identity
//!           (incarnation, generation, object_id) — a stale row is a
//!           no-op, an identity mismatch is loud divergence.
//!
//! CF_CACHE_IDEMPOTENCY
//!   key   = 0x01 ++ client_id:u64 ++ op_seq:u64      (outcome rows)
//!         | 0x02 ++ client_id:u64                    (high-watermark rows)
//!   value = OpOutcome | op_seq:u64
//!   life  = outcome window is bounded; evicted outcomes below the client
//!           high-watermark answer Expired (never re-execute).
//!
//! CF_CACHE_MOUNT
//!   key   = 0x01 ++ mount_id:u32     (current incarnation pointer)
//!         | 0x02 ++ incarnation:u64  (incarnation row)
//!         | 0x03 ++ incarnation:u64  (4b policy row, option A)
//!   value = incarnation:u64 | IncarnationRow {mount_id, revoked}
//!         | IncarnationPolicyRow {ttl_ms}
//!   life  = incarnation rows are durable forever (cheap, monotonic);
//!           the pointer row is deleted on revoke. Policy rows are written
//!           with the allocation and never mutated.
//!
//! CF_CACHE_STATE
//!   key   = static tag (&str)
//!   value = i64 watermark (cache object id, mount incarnation)
//!   life  = monotonic; restored by checkpoint; advanced by journal replay.
//! ```

use crate::master::meta::cache::BlockIdCodec;
use curvine_core_error::{err_box, err_msg, CommonError, CommonResult};
use serde::{Deserialize, Serialize};

/// Lifecycle state of a cache entry version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheEntryState {
    /// An allocation token was issued; no Valid copy is readable yet.
    Reserved,
    /// A committed, readable cache copy.
    Valid,
    /// Removed/invalidated/fenced. The row is kept so a late Commit against
    /// the recorded generation is rejected; `generation` never goes back.
    Tombstoned,
}

/// Constant-size durable cache entry (apart from the key). `generation` is
/// the CAS version: every invalidate/remove/fence/UFS-write and every new
/// allocation increments it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEntry {
    pub generation: u64,
    pub state: CacheEntryState,
    pub object_id: i64,
    pub len: i64,
    pub ufs_mtime: i64,
    /// Immutable per-entry block size; the block layout is derived from it.
    pub block_size: i64,
    /// Expiry deadline in epoch millis; `0` means no TTL.
    pub expire_at: i64,
}

/// Reverse row for a cache object (GC/reclaim hint).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRow {
    pub incarnation: u64,
    pub key: String,
    pub generation: u64,
}

/// Expiry row. The ordered secondary index position is the frozen 4a
/// `(expire_at, incarnation, object_id)` key; the stored value carries
/// `(key, generation)` so a scanned row identifies the exact entry version
/// that produced it even after the reverse row is gone (contract §4 rev2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpiryRow {
    pub expire_at: i64,
    pub incarnation: u64,
    pub object_id: i64,
    pub key: String,
    pub generation: u64,
}

/// Exclusive continuation cursor for the ordered expiry index scan (4c.1):
/// the position of the last row a page returned. The next page starts
/// strictly after `(expire_at, incarnation, object_id)` in frozen index
/// order, so rows sharing an `expire_at` page deterministically by
/// `(incarnation, object_id)`, and a cursor whose row has since been
/// deleted still resumes with no skips and no duplicates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiryCursor {
    pub expire_at: i64,
    pub incarnation: u64,
    pub object_id: i64,
}

impl From<&ExpiryRow> for ExpiryCursor {
    fn from(row: &ExpiryRow) -> Self {
        ExpiryCursor {
            expire_at: row.expire_at,
            incarnation: row.incarnation,
            object_id: row.object_id,
        }
    }
}

/// Exact victim identity journaled by a bounded scope-remove batch (4c.2).
/// The leader discovers victims through a 4c.1 bounded page scan and
/// journals ONLY these exact identities; the committed apply never
/// re-scans — it runs an exact CAS per victim against the authoritative
/// entry. A victim whose row is missing, already advanced, or already
/// tombstoned at `new_generation` is a deterministic no-op; a same-
/// generation object mismatch is loud divergence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeRemoveVictim {
    pub key: String,
    /// The generation the leader observed for this key in its page.
    pub expected_generation: u64,
    /// `expected_generation + 1`: the tombstone generation this batch
    /// writes, identical to a single-key remove.
    pub new_generation: u64,
    /// Object identity CAS: the object id the leader observed.
    pub object_id: i64,
    /// Expiry identity CAS: the deadline the leader observed on the row
    /// (0 = none). The apply requires the committed row's `expire_at` to
    /// match exactly — the version is only fully pinned by
    /// (generation, object_id, expire_at).
    pub expire_at: i64,
}

/// Exact victim identity journaled by a bounded revoked-incarnation vacuum
/// batch (4c.2). Vacuum deletes whole rows (entry, expiry, reverse) — no
/// tombstone — under the gate-3 re-verification that the incarnation row
/// exists, belongs to the named mount, is revoked, and is not the mount's
/// current pointer. Stale/missing victims are deterministic no-ops.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VacuumVictim {
    pub key: String,
    pub generation: u64,
    pub object_id: i64,
    /// Expiry identity CAS: the deadline the leader observed on the row
    /// (0 = none); must match the committed row exactly.
    pub expire_at: i64,
}

/// One client's evictions in a bounded outcome-window GC batch (4c.2).
/// `evict_below` freezes the leader-observed client high-watermark into
/// the entry (the eligibility fence): replay judges against THIS value,
/// never against the apply-time watermark. Every listed op_seq satisfies
/// `op_seq < evict_below <= durable watermark` — the boundary outcome at
/// `op_seq == watermark` is never listed.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct OutcomeGcGroup {
    pub client_id: u64,
    pub evict_below: u64,
    /// Strictly ascending; every op_seq < evict_below.
    pub op_seqs: Vec<u64>,
}

/// Durable mount incarnation row. `mount_id` is a reusable routing id;
/// `incarnation` is never reused. The row layout is frozen at 4a (bincode
/// positional encoding cannot absorb appended fields — see journal/entry.rs);
/// the 4b policy snapshot lives in a separate [`IncarnationPolicyRow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncarnationRow {
    pub mount_id: u32,
    pub revoked: bool,
}

/// 4b policy snapshot for an incarnation, stored under a separate
/// CF_CACHE_MOUNT key (`0x03 ++ incarnation:u64`) so the legacy
/// [`IncarnationRow`] bytes stay decodable. `ttl_ms` freezes the VERIFIED
/// mount properties' TTL at allocation time: commits under this incarnation
/// derive `expire_at` from this durable value, never from the client and
/// never from a later mutable mount table entry. Rows written before 4b
/// have no policy row; readers treat a missing policy row as `ttl_ms == 0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncarnationPolicyRow {
    /// 0 = no TTL for entries committed under this incarnation.
    pub ttl_ms: i64,
}

/// Persistent operation token, decoupled from transport rpc ids: carried
/// unchanged across retries and leader failover (contract §3 rev2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OpToken {
    pub client_id: u64,
    pub op_seq: u64,
}

/// Persisted outcomes for identity-producing and load-commit operations.
/// Purely conditional mutations (remove/invalidate) never persist
/// outcomes; they are derived from entry state via their CAS fences.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpOutcome {
    /// Global cache-object-id segment reservation `[start, end)`.
    Reserved { start: i64, end: i64 },
    /// Mount incarnation allocation (4a legacy shape: incarnation only).
    /// Kept byte-compatible for journals/outcomes written before 4b; it can
    /// only be produced by replaying the legacy journal entry, and a new
    /// issuer never matches against it as an exact parameter binding.
    IncarnationAllocated { incarnation: u64 },
    /// Per-key load allocation: the object id identity must be recoverable,
    /// and the exact request geometry is recorded so a retry (including a
    /// re-plan after a master restart lost the volatile plan) can verify
    /// the recorded parameters before regenerating a plan.
    Allocated {
        incarnation: u64,
        key: String,
        generation: u64,
        object_id: i64,
        /// Target length as allocated (0 is legal: an empty object).
        file_len: i64,
        block_size: i64,
    },
    /// Per-key load commit (`Reserved@g -> Valid`): lets a lost-response
    /// commit retry resolve to its recorded result instead of re-judging
    /// the already-advanced entry row. Binds the FULL immutable request
    /// (load token + geometry + fence): a token replayed with any
    /// different parameter is divergence, never AlreadyApplied.
    Committed {
        incarnation: u64,
        key: String,
        generation: u64,
        object_id: i64,
        load_token: OpToken,
        len: i64,
        ufs_mtime: i64,
        expire_at: i64,
    },
    /// 4b incarnation allocation with the full frozen request bound: a
    /// token replayed with any different parameter (mount id, TTL,
    /// incarnation) is divergence, never AlreadyApplied. Appended at the
    /// enum tail so 4a `IncarnationAllocated` bytes keep decoding.
    IncarnationAllocatedV2 {
        incarnation: u64,
        mount_id: u32,
        ttl_ms: i64,
    },
}

/// Terminal result of a token-indexed idempotent operation. `Expired` is
/// terminal: the outcome window has evicted the record and the token is
/// below the client high-watermark, so the original identity cannot be
/// recovered and must never be re-allocated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenOutcome {
    Executed(OpOutcome),
    Expired,
}

/// Terminal states of a conditional (CAS) key mutation (contract §3 rev2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasOutcome {
    /// This command's expected generation matched and it applied now.
    Applied,
    /// The committed state already reflects this command (replay/retry).
    AlreadyApplied,
    /// A later generation has advanced; the command (and its load) is dead:
    /// no retry, no Commit, reclaim the object.
    Superseded { expected: u64, current: u64 },
}

/// Order-preserving, reversible cache key encoding: the raw UTF-8 bytes of
/// the key (contract §4 rev2). Encoded order equals string order, so
/// incarnation-scoped range scans, continuation paging, and future prefix
/// removes work directly on RocksDB key order.
///
/// This encoding is intentionally **not** component-safe by itself: the
/// encoding of `/a` is a byte-prefix of `/ab`, and RocksDB byte-prefix
/// matching must never be used for scoped removal. Component safety is
/// enforced at scan time by decoding each row's key and matching with
/// [`key_in_scope`] on component boundaries. Round-trips any UTF-8 key.
pub fn encode_key(key: &str) -> Vec<u8> {
    key.as_bytes().to_vec()
}

/// Decode an order-preserving key encoding back into the raw key string.
/// The only failure mode is invalid UTF-8 (corruption).
pub fn decode_key(bytes: &[u8]) -> CommonResult<String> {
    String::from_utf8(bytes.to_vec())
        .map_err(|e| CommonError::from(err_msg!("cache key is not valid UTF-8: {}", e)))
}

/// Validate entry invariants. The authoritative store write boundary
/// (`put_entry`) enforces this; callers cannot bypass it.
///
/// Invariants (contract §2/§4 rev2):
/// - `generation >= 1` (allocations start at 1 and only advance)
/// - `object_id` in the cache domain, `block_size > 0`, `len >= 0`
/// - `Valid` entries carry a real UFS fence timestamp (`ufs_mtime > 0`) and a
///   non-negative `expire_at` (0 = no TTL)
/// - only `Valid` entries carry an expiry deadline: `Reserved`/`Tombstoned`
///   must have `expire_at == 0` (no expiry row exists for them)
/// - a full block layout must be derivable for `Reserved`/`Valid`
pub fn validate_entry(entry: &CacheEntry) -> CommonResult<()> {
    if entry.generation < 1 {
        return err_box!("cache entry generation must be >= 1: {}", entry.generation);
    }
    if !BlockIdCodec::is_cache_owner(entry.object_id) {
        return err_box!(
            "cache entry object id outside cache domain: {}",
            entry.object_id
        );
    }
    if entry.block_size <= 0 {
        return err_box!(
            "cache entry block size must be positive: {}",
            entry.block_size
        );
    }
    if entry.len < 0 {
        return err_box!("cache entry length must be non-negative: {}", entry.len);
    }
    match entry.state {
        CacheEntryState::Valid => {
            if entry.ufs_mtime <= 0 {
                return err_box!(
                    "valid cache entry must carry a ufs_mtime fence > 0: {}",
                    entry.ufs_mtime
                );
            }
            if entry.expire_at < 0 {
                return err_box!(
                    "cache entry expire_at must be non-negative: {}",
                    entry.expire_at
                );
            }
        }
        CacheEntryState::Reserved | CacheEntryState::Tombstoned => {
            if entry.expire_at != 0 {
                return err_box!(
                    "only valid entries carry an expiry deadline, state {:?} has expire_at {}",
                    entry.state,
                    entry.expire_at
                );
            }
        }
    }
    // A full block layout must always be derivable for Reserved/Valid.
    crate::master::meta::cache::CacheBlockLayout::derive(
        entry.object_id,
        entry.len,
        entry.block_size,
    )?;
    Ok(())
}

/// Validate a reverse object row before it is persisted.
pub fn validate_object_row(object_id: i64, row: &ObjectRow) -> CommonResult<()> {
    if !BlockIdCodec::is_cache_owner(object_id) {
        return err_box!("object row id outside cache domain: {}", object_id);
    }
    if row.generation < 1 {
        return err_box!("object row generation must be >= 1: {}", row.generation);
    }
    Ok(())
}

/// Validate an expiry row before it is persisted. `expire_at` must be a real
/// future deadline: 0 means "no TTL" and must never produce a row, and
/// negative deadlines break the signed big-endian ordering the ordered scan
/// relies on.
pub fn validate_expiry_row(row: &ExpiryRow) -> CommonResult<()> {
    if row.expire_at <= 0 {
        return err_box!("expiry row deadline must be positive: {}", row.expire_at);
    }
    if !BlockIdCodec::is_cache_owner(row.object_id) {
        return err_box!(
            "expiry row object id outside cache domain: {}",
            row.object_id
        );
    }
    if row.generation < 1 {
        return err_box!("expiry row generation must be >= 1: {}", row.generation);
    }
    Ok(())
}

/// Whether `key` lies inside the prefix `scope` on component boundaries:
/// `/a` covers `/a` and `/a/b` but not `/ab`.
pub fn key_in_scope(key: &str, scope: &str) -> bool {
    if key == scope {
        return true;
    }
    let scope = scope.trim_end_matches('/');
    match key.strip_prefix(scope) {
        Some(rest) => rest.starts_with('/'),
        None => false,
    }
}

/// The highest incarnation any path may allocate or persist. `u64::MAX` is
/// reserved so a strict 8-byte big-endian upper bound always exists for
/// incarnation-scoped range scans. The allocator, watermark restore, and
/// every creating store write entry reject values outside `1..=` this.
pub const MAX_ALLOCATABLE_INCARNATION: u64 = u64::MAX - 1;

/// Hard cap on the UTF-8 byte size of a cache key (and a scope-remove
/// scope prefix) enforced at BOTH the service boundary and every 4c.2
/// bounded-mutation apply path: the per-page victim COUNT cap
/// (`MUTATION_PAGE_CAP`) alone is not a byte bound — unbounded key
/// strings would let one journal entry carry an unbounded payload
/// (review `303fb807`, bounded gate).
pub const MAX_CACHE_KEY_BYTES: usize = 4096;

/// Validate an incarnation before it is persisted by any creating write
/// (entry rows, reverse rows, expiry rows, incarnation rows, mount
/// pointers). Deletes of corrupt rows remain allowed.
pub fn validate_incarnation(incarnation: u64) -> CommonResult<()> {
    if incarnation == 0 || incarnation > MAX_ALLOCATABLE_INCARNATION {
        return err_box!(
            "incarnation outside allocatable range [1, {}]: {}",
            MAX_ALLOCATABLE_INCARNATION,
            incarnation
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_decode(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn test_frozen_7e3f8a02_row_and_outcome_bytes_decode_at_head() {
        // Frozen bytes of the 4a (7e3f8a02) IncarnationRow and legacy
        // IncarnationAllocated outcome. The legacy layouts are unchanged
        // since (4b stores the policy snapshot under a separate key and the
        // full binding in a V2 outcome variant), so RocksDB rows written by
        // 4a must keep decoding bit-identically.
        const ROW_HEX: &str = "0300000000";
        let row: IncarnationRow =
            curvine_runtime::common::SerdeUtils::deserialize(&hex_decode(ROW_HEX)).unwrap();
        assert_eq!(
            row,
            IncarnationRow {
                mount_id: 3,
                revoked: false
            }
        );

        const ROW_REVOKED_HEX: &str = "0400000001";
        let row: IncarnationRow =
            curvine_runtime::common::SerdeUtils::deserialize(&hex_decode(ROW_REVOKED_HEX)).unwrap();
        assert_eq!(
            row,
            IncarnationRow {
                mount_id: 4,
                revoked: true
            }
        );

        // Legacy outcome: incarnation only, discriminant 1. A V2 request
        // (mount + ttl bound) can never equal it, so a replay with the same
        // token is loud divergence rather than a false AlreadyApplied.
        const OUTCOME_LEGACY_HEX: &str = "010000000c00000000000000";
        let oc: OpOutcome =
            curvine_runtime::common::SerdeUtils::deserialize(&hex_decode(OUTCOME_LEGACY_HEX))
                .unwrap();
        assert_eq!(oc, OpOutcome::IncarnationAllocated { incarnation: 12 });
        assert_ne!(
            oc,
            OpOutcome::IncarnationAllocatedV2 {
                incarnation: 12,
                mount_id: 0,
                ttl_ms: 0
            }
        );

        // V2 outcome roundtrip: discriminant 4 (enum tail), full binding.
        const OUTCOME_V2_HEX: &str = "040000000c000000000000000300000080ee360000000000";
        let bytes =
            curvine_runtime::common::SerdeUtils::serialize(&OpOutcome::IncarnationAllocatedV2 {
                incarnation: 12,
                mount_id: 3,
                ttl_ms: 3_600_000,
            })
            .unwrap();
        assert_eq!(
            bytes
                .iter()
                .map(|x| format!("{:02x}", x))
                .collect::<String>(),
            OUTCOME_V2_HEX
        );
        let back: OpOutcome = curvine_runtime::common::SerdeUtils::deserialize(&bytes).unwrap();
        assert_eq!(
            back,
            OpOutcome::IncarnationAllocatedV2 {
                incarnation: 12,
                mount_id: 3,
                ttl_ms: 3_600_000
            }
        );
    }

    #[test]
    fn test_key_encoding_roundtrip_and_order() {
        for key in ["/", "/a", "/ab", "/a/b", "", "utf8-键/值", "x\0y"] {
            let enc = encode_key(key);
            assert_eq!(decode_key(&enc).unwrap(), key);
        }

        // Order preservation: encoded order equals string order across
        // different lengths and component boundaries.
        let mut keys = vec!["/a", "/z", "/aa", "/a/b", "/ab", "/a/b/c", "", "/"];
        let mut sorted_by_str = keys.clone();
        sorted_by_str.sort();
        keys.sort_by_key(|k| encode_key(k));
        assert_eq!(keys, sorted_by_str);
        // The exact expected order, spelled out.
        assert!(
            encode_key("/a") < encode_key("/a/b")
                && encode_key("/a/b") < encode_key("/aa")
                && encode_key("/aa") < encode_key("/ab")
                && encode_key("/ab") < encode_key("/z")
        );

        // The encoding is deliberately NOT component-safe by itself: raw
        // byte-prefix matching must not be used for scoped removal (scope
        // filtering happens via key_in_scope on decoded keys).
        assert!(encode_key("/ab").starts_with(&encode_key("/a")));

        // Malformed UTF-8 is rejected, not guessed.
        assert!(decode_key(&[0xFF, 0xFF, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn test_key_in_scope() {
        assert!(key_in_scope("/a", "/a"));
        assert!(key_in_scope("/a/b", "/a"));
        assert!(key_in_scope("/a/b/c", "/a"));
        assert!(!key_in_scope("/ab", "/a"));
        assert!(!key_in_scope("/", "/a"));
        assert!(!key_in_scope("/abc", "/ab"));
        assert!(key_in_scope("/ab", "/ab"));
        // Trailing slash on the scope is tolerated.
        assert!(key_in_scope("/a/b", "/a/"));
        assert!(!key_in_scope("/ab", "/a/"));
    }

    #[test]
    fn test_validate_entry() {
        let base = CacheEntry {
            generation: 1,
            state: CacheEntryState::Valid,
            object_id: BlockIdCodec::CACHE_OBJECT_MIN,
            len: 100,
            ufs_mtime: 42,
            block_size: 64,
            expire_at: 0,
        };
        validate_entry(&base).unwrap();

        let mut e = base.clone();
        e.object_id = 42; // fs domain
        assert!(validate_entry(&e).is_err());
        let mut e = base.clone();
        e.block_size = 0;
        assert!(validate_entry(&e).is_err());
        let mut e = base.clone();
        e.len = -1;
        assert!(validate_entry(&e).is_err());
        let mut e = base.clone();
        e.expire_at = -1;
        assert!(validate_entry(&e).is_err());
        let mut e = base.clone();
        e.generation = 0;
        assert!(validate_entry(&e).is_err());
        // Valid requires a real UFS fence timestamp.
        let mut e = base.clone();
        e.ufs_mtime = 0;
        assert!(validate_entry(&e).is_err());

        // Reserved: no ufs_mtime fence required yet, no expiry allowed.
        let mut e = base.clone();
        e.state = CacheEntryState::Reserved;
        e.ufs_mtime = 0;
        validate_entry(&e).unwrap();
        e.expire_at = 100;
        assert!(validate_entry(&e).is_err());

        // Tombstoned: same expiry rule.
        let mut e = base.clone();
        e.state = CacheEntryState::Tombstoned;
        e.expire_at = 100;
        assert!(validate_entry(&e).is_err());
        e.expire_at = 0;
        validate_entry(&e).unwrap();
    }

    #[test]
    fn test_validate_rows() {
        let obj = BlockIdCodec::CACHE_OBJECT_MIN;
        let o = ObjectRow {
            incarnation: 1,
            key: "/k".into(),
            generation: 1,
        };
        validate_object_row(obj, &o).unwrap();
        assert!(validate_object_row(42, &o).is_err());
        assert!(validate_object_row(obj - 1, &o).is_err());
        assert!(validate_object_row(
            obj,
            &ObjectRow {
                generation: 0,
                ..o.clone()
            }
        )
        .is_err());

        let x = ExpiryRow {
            expire_at: 100,
            incarnation: 1,
            object_id: obj,
            key: "/k".into(),
            generation: 1,
        };
        validate_expiry_row(&x).unwrap();
        assert!(validate_expiry_row(&ExpiryRow {
            expire_at: 0,
            ..x.clone()
        })
        .is_err());
        assert!(validate_expiry_row(&ExpiryRow {
            expire_at: -1,
            ..x.clone()
        })
        .is_err());
        assert!(validate_expiry_row(&ExpiryRow {
            object_id: 42,
            ..x.clone()
        })
        .is_err());
        assert!(validate_expiry_row(&ExpiryRow {
            generation: 0,
            ..x.clone()
        })
        .is_err());
    }
}
