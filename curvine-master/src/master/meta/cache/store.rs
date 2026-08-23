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

//! CacheIndex storage boundary (phase 0 contract §5).
//!
//! The master's RocksDB is the authoritative point/range store. This module
//! defines the **local synchronous** storage contract
//! ([`LocalCacheIndexStore`] and [`CacheWrite`]): all mutations are staged
//! in a [`CacheWrite`] and committed atomically, and all reads are point
//! reads or bounded range scans — no operation returns or materializes an
//! unbounded set (continuation-token paging via `after`).
//!
//! This trait is *not* an async-transaction seam. A future transactional
//! backend (e.g. FDB) needs retry/async semantics that this sync trait
//! cannot express; that backend will get its own trait, and the GAT batch
//! here must not be read as evidence of async readiness (contract §5
//! requires the boundary to *admit* such a backend, which the trait's
//! operation vocabulary — point/range reads, atomic staged writes — does).
//!
//! No in-memory entry table exists in Phase 1; every read hits the store.

use crate::master::meta::cache::entry::{
    CacheEntry, ExpiryCursor, ExpiryRow, IncarnationPolicyRow, IncarnationRow, ObjectRow,
    OpOutcome, OpToken,
};
use curvine_core_error::{err_msg, CommonError};
use curvine_error::{FsError, FsResult};

/// Hard upper bound for any single bounded scan page (4c.1). Every scan
/// validates `1..=SCAN_HARD_CAP` at the boundary: a caller can never ask
/// for an unbounded page, and every returned page holds at most `limit`
/// rows with no lock held across pages.
pub const SCAN_HARD_CAP: usize = 1024;

/// Hard upper bound for the victim/eviction list of any single bounded
/// mutation journal entry (4c.2). Every batch apply validates
/// `1..=MUTATION_PAGE_CAP` at the boundary: one committed apply mutates at
/// most this many identities in one transaction, and a journal entry can
/// never carry an unbounded mutation payload.
pub const MUTATION_PAGE_CAP: usize = 64;

/// Validate a scan page limit at the storage boundary (`1..=SCAN_HARD_CAP`).
pub fn validate_scan_limit(limit: usize) -> FsResult<()> {
    if limit == 0 || limit > SCAN_HARD_CAP {
        return Err(FsError::from(CommonError::from(err_msg!(
            "scan limit must be in 1..={}, got {}",
            SCAN_HARD_CAP,
            limit
        ))));
    }
    Ok(())
}

/// Validate a bounded mutation page length at the apply boundary
/// (`1..=MUTATION_PAGE_CAP`).
pub fn validate_page_len(len: usize) -> FsResult<()> {
    if len == 0 || len > MUTATION_PAGE_CAP {
        return Err(FsError::from(CommonError::from(err_msg!(
            "mutation page length must be in 1..={}, got {}",
            MUTATION_PAGE_CAP,
            len
        ))));
    }
    Ok(())
}

/// Durable mutations for the CacheIndex, part of the local synchronous seam.
/// All rows written through one batch commit atomically; a failed or
/// uncommitted batch leaves no partial state. Write methods validate their
/// rows (contract invariants) so no caller can persist a malformed row.
pub trait CacheWrite {
    /// Upsert the single current version row of `(incarnation, key)`.
    /// Validates the entry; the derived object/expiry rows are the caller's
    /// responsibility (the committed apply path writes them together).
    fn put_entry(&mut self, incarnation: u64, key: &str, entry: &CacheEntry) -> FsResult<()>;

    /// Remove the entry row (mount vacuum / prefix remove final step).
    fn delete_entry(&mut self, incarnation: u64, key: &str) -> FsResult<()>;

    fn put_object(&mut self, object_id: i64, row: &ObjectRow) -> FsResult<()>;

    /// Reverse rows are GC hints: deleted as soon as their version is
    /// superseded or reclaimed; stale rows are always safe to delete.
    fn delete_object(&mut self, object_id: i64) -> FsResult<()>;

    /// Exact-identity CAS upsert into the ordered expiry index (4c.1),
    /// keyed by the frozen `(expire_at, incarnation, object_id)` position.
    /// In legal history a position is uniquely bound to one
    /// `(key, generation)` — an object id never advances generations — so
    /// an existing committed row must be a bit-exact identity match
    /// (idempotent replay); any difference in key OR generation (in
    /// either direction) is replay divergence and fails loudly. Stale
    /// TTL/remove no-op semantics live above this seam (4c.2 CAS against
    /// the authoritative entry), never here.
    fn put_expiry(&mut self, row: &ExpiryRow) -> FsResult<()>;

    /// Exact-identity CAS delete (4c.1): removes the expiry index row at
    /// the frozen `(expire_at, incarnation, object_id)` position only when
    /// the committed row's `(key, generation)` matches the expected pair
    /// exactly; any mismatch is replay divergence and fails loudly; a
    /// missing row is an idempotent no-op.
    fn delete_expiry(
        &mut self,
        expire_at: i64,
        incarnation: u64,
        object_id: i64,
        key: &str,
        generation: u64,
    ) -> FsResult<()>;

    /// Create/update an incarnation row. Incarnation rows are durable forever.
    fn put_incarnation(&mut self, incarnation: u64, row: IncarnationRow) -> FsResult<()>;

    /// 4b option A: write the incarnation's frozen policy snapshot under a
    /// separate key. Written once at allocation; never mutated or deleted.
    fn put_incarnation_policy(
        &mut self,
        incarnation: u64,
        row: IncarnationPolicyRow,
    ) -> FsResult<()>;

    /// Set the mount's current-incarnation pointer.
    fn set_current_incarnation(&mut self, mount_id: u32, incarnation: u64) -> FsResult<()>;

    /// Revoke: drop the mount's pointer. The incarnation row stays and is
    /// marked revoked by `put_incarnation`.
    fn clear_current_incarnation(&mut self, mount_id: u32) -> FsResult<()>;

    /// Persist the outcome of an identity-producing operation.
    fn put_outcome(&mut self, token: OpToken, outcome: &OpOutcome) -> FsResult<()>;

    /// Evict a single outcome row (bounded-window reclamation).
    fn delete_outcome(&mut self, token: OpToken) -> FsResult<()>;

    /// Monotonic per-client op high-watermark: writing a value <= the
    /// persisted one is a no-op (a regressing journal replay cannot move the
    /// watermark backwards).
    fn set_client_watermark(&mut self, client_id: u64, op_seq: u64) -> FsResult<()>;

    /// Monotonic allocator watermark (`cache_object_id`, `cache_incarnation`):
    /// writing a value <= the persisted one is a no-op.
    fn set_state(&mut self, tag: &str, value: i64) -> FsResult<()>;

    fn commit(self) -> FsResult<()>;
}

/// Authoritative point/range reads over the CacheIndex — the **local
/// synchronous** contract (see module docs; an async transactional backend
/// is a future, separate trait).
pub trait LocalCacheIndexStore {
    type Write<'a>: CacheWrite + 'a
    where
        Self: 'a;

    /// Point-read the current version row of `(incarnation, key)`.
    /// `Ok(None)` means "no such entry" — distinct from backend failure.
    fn cache_get_entry(&self, incarnation: u64, key: &str) -> FsResult<Option<CacheEntry>>;

    /// Reverse lookup: which entry version owns this object id.
    fn cache_get_object(&self, object_id: i64) -> FsResult<Option<ObjectRow>>;

    /// Bounded, resumable scan of expiry rows with `expire_at <= now`, in
    /// frozen `(expire_at, incarnation, object_id)` index order, starting
    /// strictly after `after` (None = from the beginning; the cursor is
    /// validated: positive deadline, allocatable incarnation). Rows
    /// sharing an `expire_at` page stably by `(incarnation, object_id)`.
    /// `limit` is validated `1..=SCAN_HARD_CAP`.
    fn cache_scan_expiry(
        &self,
        now: i64,
        after: Option<&ExpiryCursor>,
        limit: usize,
    ) -> FsResult<Vec<ExpiryRow>>;

    /// Bounded, resumable scan of entry rows of one incarnation (the Mount
    /// scope), in key order, starting strictly after `after` (None = from
    /// the beginning). `limit` is validated `1..=SCAN_HARD_CAP`.
    fn cache_scan_entries(
        &self,
        incarnation: u64,
        after: Option<&str>,
        limit: usize,
    ) -> FsResult<Vec<(String, CacheEntry)>>;

    /// Bounded, resumable Prefix-scope scan (4c.1). `scope` is a
    /// mount-relative path: `/a` matches exactly `/a` and every descendant
    /// `/a/...`, never `/ab` — membership is judged only by
    /// [`key_in_scope`](crate::master::meta::cache::key_in_scope) on
    /// decoded keys, never by RocksDB byte-prefix bounds (`encode_key` is
    /// deliberately not component-safe). The Key scope is the point read
    /// `cache_get_entry`; the Mount scope is `cache_scan_entries`. Starts
    /// strictly after `after` (None = from the scope itself); a cursor
    /// outside the scope is rejected at the boundary (membership is a
    /// property of the strings, so a cursor whose row was deleted stays
    /// valid) — a foreign cursor can never turn the scan into an
    /// unbounded filtered walk. Iteration stops as soon as it passes the
    /// scope's whole key family.
    fn cache_scan_entries_in_scope(
        &self,
        incarnation: u64,
        scope: &str,
        after: Option<&str>,
        limit: usize,
    ) -> FsResult<Vec<(String, CacheEntry)>>;

    fn cache_get_incarnation(&self, incarnation: u64) -> FsResult<Option<IncarnationRow>>;

    /// The mount's current incarnation, if mounted and not revoked.
    fn cache_current_incarnation(&self, mount_id: u32) -> FsResult<Option<u64>>;

    /// The incarnation's frozen policy snapshot (4b option A). Pre-4b
    /// allocations have no policy row; implementations synthesize
    /// `ttl_ms == 0` so callers never need a None branch.
    fn cache_get_incarnation_policy(
        &self,
        incarnation: u64,
    ) -> FsResult<Option<IncarnationPolicyRow>>;

    fn cache_get_outcome(&self, token: OpToken) -> FsResult<Option<OpOutcome>>;

    /// Bounded, resumable scan of OUTCOME rows only (4c.2) — the `0x01`
    /// tag of the idempotency CF, never the `0x02` watermark rows — in
    /// `(client_id, op_seq)` order, starting strictly after `after` (None =
    /// from the beginning; `op_seq >= 1` is validated). `limit` is
    /// validated `1..=SCAN_HARD_CAP`. Returns the tokens whose outcome row
    /// exists; the outcome-GC driver filters them against the client
    /// high-watermark before proposing evictions.
    fn cache_scan_outcomes(&self, after: Option<&OpToken>, limit: usize) -> FsResult<Vec<OpToken>>;

    fn cache_client_watermark(&self, client_id: u64) -> FsResult<Option<u64>>;

    /// Read an allocator watermark tag.
    fn cache_get_state(&self, tag: &str) -> FsResult<Option<i64>>;

    /// Open an atomic write batch.
    fn cache_write(&self) -> Self::Write<'_>;
}

/// Watermark tags stored in the cache state column family.
pub mod state_tags {
    pub const CACHE_OBJECT_ID: &str = "cache_object_id";
    pub const CACHE_INCARNATION: &str = "cache_incarnation";
}
