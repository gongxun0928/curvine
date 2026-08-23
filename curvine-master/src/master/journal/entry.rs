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

use crate::master::meta::cache::{CacheEntry, OpToken};
use crate::master::meta::inode::{InodeDir, InodeFile, InodeView};
use crate::master::meta::BlockMeta;
use curvine_core_error::{err_box, CommonResult};
use curvine_model::{CommitBlock, FileLock, MountInfo, SetAttrOpts};
use curvine_runtime::common::SerdeUtils;
use log::debug;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

#[derive(Debug, Clone)]
pub(crate) struct CvMetadataChange {
    pub(crate) op_id: u64,
    pub(crate) path: String,
    pub(crate) include_subtree: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MkdirEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) dir: InodeDir,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateFileEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) file: InodeFile,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ReopenFileEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) file: InodeFile,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct OverWriteFileEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) file: InodeFile,
}

// Apply for a new block
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AddBlockEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) blocks: Vec<BlockMeta>,
    pub(crate) commit_block: Vec<CommitBlock>,
}

// File writing is completed.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CompleteFileEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) file: InodeFile,
    pub(crate) commit_blocks: Vec<CommitBlock>,
}

// Rename
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RenameEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) src: String,
    pub(crate) dst: String,
    pub(crate) mtime: i64,
    pub(crate) flags: u32,
    /// Pre-exchange inode ids for idempotent EXCHANGE replay (0 when absent / legacy).
    pub(crate) src_inode_id: i64,
    pub(crate) dst_inode_id: i64,
}

// delete
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DeleteEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) mtime: i64,
}

// mount
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MountEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) info: MountInfo,
}

// umount
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UnMountEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) id: u32,
}

// set attr
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SetAttrEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) opts: SetAttrOpts,
}

// symlink
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SymlinkEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) link: String,
    pub(crate) new_inode: InodeFile,
    pub(crate) force: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LinkEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    /// Link creation time, reused during replay for parent mtime and inode ctime.
    pub(crate) mtime: i64,
    pub(crate) src_path: String,
    pub(crate) dst_path: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SetLocksEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) ino: i64,
    pub(crate) locks: Vec<FileLock>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct FreeEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) mtime: i64,
    #[serde(default)]
    pub(crate) recursive: bool,
}

/// Clears an unusable Curvine cache copy while preserving the file's UFS
/// metadata. The full inode metadata is recorded so followers can apply the
/// same state transition without consulting their potentially stale locations.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CacheInvalidationEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) inodes: Vec<InodeView>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UfsAppliedEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) term: u64,
    pub(crate) index: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SnapshotEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) node_id: u64,
    pub(crate) dir: String,
}

// ---- Cache-mode metadata commands (task #3, phase 0 contract §2/§3 rev2).
// Keep these variants appended at the enum tail so existing journal
// discriminants remain stable. Every entry carries absolute values
// (incarnation / generation / object id) so committed replay is
// deterministic and identical on leader and follower. ----

/// Identity-producing: reserve the global cache object id segment
/// `[start, end)`. Replay advances the allocator watermark to `end - 1`
/// and persists the exact outcome for the op token (bounded window).
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CacheIdReserveEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) token: OpToken,
    pub(crate) start: i64,
    pub(crate) end: i64,
}

/// Identity-producing: allocate a never-reused mount incarnation and point
/// `mount_id` at it. Legacy 4a layout — frozen: appending fields would break
/// bincode decode of journals written before 4b (positional encoding, see
/// the compatibility note near `deserialize_compat`). 4b writers use
/// [`CacheIncarnationAllocateV2Entry`]; this variant only replays.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CacheIncarnationAllocateEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) token: OpToken,
    pub(crate) mount_id: u32,
    pub(crate) incarnation: u64,
}

/// Identity-producing (4b): allocate a never-reused mount incarnation with
/// the frozen policy snapshot. `ttl_ms` freezes the VERIFIED mount TTL at
/// allocation time: commits under this incarnation derive `expire_at` from
/// the durable policy row, never from the client and never from a later
/// mutable mount table entry. `cache_write` records the verified capability
/// (write_cache-enabled mount) at allocation; apply re-verifies it against
/// the persisted mount table and the issuer re-reads it post-barrier.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CacheIncarnationAllocateV2Entry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) token: OpToken,
    pub(crate) mount_id: u32,
    pub(crate) incarnation: u64,
    /// 0 = no TTL. Frozen from the verified mount properties.
    pub(crate) ttl_ms: i64,
    /// Capability snapshot: the mount was verified write-cache-enabled
    /// (cache mode, read-write access) when this allocation was issued.
    pub(crate) cache_write: bool,
}

/// Conditional: revoke a mount incarnation (unmount fence). The
/// incarnation row stays forever, marked revoked; the mount's current
/// pointer is cleared only if it still names this incarnation.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CacheIncarnationRevokeEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) mount_id: u32,
    pub(crate) incarnation: u64,
}

/// Identity-producing: per-key load allocation. Writes a `Reserved` entry
/// at `entry.generation`; the persisted outcome binds the object id for the
/// token so a retried allocate can never mint a second identity.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CacheAllocateEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) token: OpToken,
    pub(crate) incarnation: u64,
    pub(crate) key: String,
    /// Target object length as allocated (the Reserved row itself stays
    /// len=0 until commit): bound into the persisted Allocated outcome so
    /// an exact allocate retry (including a re-plan after master restart)
    /// can verify the recorded parameters.
    pub(crate) file_len: i64,
    pub(crate) entry: CacheEntry,
}

/// Conditional CAS: `Reserved@generation` -> `Valid` with the final
/// `(len, ufs_mtime, expire_at)`. The committed apply rejects the commit
/// when the recorded generation has advanced (Superseded is terminal: the
/// old load is dead and its object reclaimable). The load token binds the
/// commit to its allocate for durable idempotency: a retried commit with
/// the same token resolves to its recorded outcome instead of re-judging
/// the (already advanced) entry row.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CacheCommitEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    /// Durable idempotency token of this commit operation (never the load
    /// token: Allocate and Commit record different outcomes and must not
    /// alias one token to both).
    pub(crate) token: OpToken,
    /// The load identity token from the allocate; binds the commit to its
    /// recorded Allocated outcome and (volatile) placement plan.
    pub(crate) load_token: OpToken,
    pub(crate) incarnation: u64,
    pub(crate) key: String,
    pub(crate) generation: u64,
    /// Object identity CAS (contract §2.3): the commit must land on the
    /// object id its allocate reserved; any mismatch is divergence.
    pub(crate) expected_object_id: i64,
    pub(crate) len: i64,
    pub(crate) ufs_mtime: i64,
    pub(crate) expire_at: i64,
}

/// Conditional CAS: any state at `expected_generation` -> `Tombstoned` at
/// `expected_generation + 1`. Kept for late-commit rejection; the expiry
/// and reverse rows of the superseded version are dropped.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CacheRemoveEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) incarnation: u64,
    pub(crate) key: String,
    pub(crate) expected_generation: u64,
    pub(crate) new_generation: u64,
    /// Object identity CAS (contract §2.3): the remove must target the
    /// object id the caller observed; any mismatch is divergence.
    pub(crate) expected_object_id: i64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum JournalEntry {
    Mkdir(MkdirEntry),
    CreateFile(CreateFileEntry),
    ReopenFile(ReopenFileEntry),
    OverWriteFile(OverWriteFileEntry),
    AddBlock(AddBlockEntry),
    CompleteFile(CompleteFileEntry),
    Rename(RenameEntry),
    Delete(DeleteEntry),
    Mount(MountEntry),
    UnMount(UnMountEntry),
    SetAttr(SetAttrEntry),
    Symlink(SymlinkEntry),
    Link(LinkEntry),
    SetLocks(SetLocksEntry),
    Free(FreeEntry),
    UfsApplied(UfsAppliedEntry),
    Snapshot(SnapshotEntry),
    // Keep new variants appended so existing journal discriminants remain stable.
    CacheInvalidation(CacheInvalidationEntry),
    CacheIdReserve(CacheIdReserveEntry),
    CacheIncarnationAllocate(CacheIncarnationAllocateEntry),
    CacheIncarnationRevoke(CacheIncarnationRevokeEntry),
    CacheAllocate(CacheAllocateEntry),
    CacheCommit(CacheCommitEntry),
    CacheRemove(CacheRemoveEntry),
    CacheIncarnationAllocateV2(CacheIncarnationAllocateV2Entry),
}

impl JournalEntry {
    pub fn op_id(&self) -> u64 {
        match self {
            JournalEntry::Mkdir(e) => e.op_id,
            JournalEntry::CreateFile(e) => e.op_id,
            JournalEntry::ReopenFile(e) => e.op_id,
            JournalEntry::OverWriteFile(e) => e.op_id,
            JournalEntry::AddBlock(e) => e.op_id,
            JournalEntry::CompleteFile(e) => e.op_id,
            JournalEntry::Rename(e) => e.op_id,
            JournalEntry::Delete(e) => e.op_id,
            JournalEntry::Mount(e) => e.op_id,
            JournalEntry::UnMount(e) => e.op_id,
            JournalEntry::SetAttr(e) => e.op_id,
            JournalEntry::Symlink(e) => e.op_id,
            JournalEntry::Link(e) => e.op_id,
            JournalEntry::SetLocks(e) => e.op_id,
            JournalEntry::Free(e) => e.op_id,
            JournalEntry::CacheInvalidation(e) => e.op_id,
            JournalEntry::UfsApplied(e) => e.op_id,
            JournalEntry::Snapshot(e) => e.op_id,
            JournalEntry::CacheIdReserve(e) => e.op_id,
            JournalEntry::CacheIncarnationAllocate(e) => e.op_id,
            JournalEntry::CacheIncarnationRevoke(e) => e.op_id,
            JournalEntry::CacheAllocate(e) => e.op_id,
            JournalEntry::CacheCommit(e) => e.op_id,
            JournalEntry::CacheRemove(e) => e.op_id,
            JournalEntry::CacheIncarnationAllocateV2(e) => e.op_id,
        }
    }

    pub fn rpc_id(&self) -> i64 {
        match self {
            JournalEntry::Mkdir(e) => e.rpc_id,
            JournalEntry::CreateFile(e) => e.rpc_id,
            JournalEntry::ReopenFile(e) => e.rpc_id,
            JournalEntry::OverWriteFile(e) => e.rpc_id,
            JournalEntry::AddBlock(e) => e.rpc_id,
            JournalEntry::CompleteFile(e) => e.rpc_id,
            JournalEntry::Rename(e) => e.rpc_id,
            JournalEntry::Delete(e) => e.rpc_id,
            JournalEntry::Mount(e) => e.rpc_id,
            JournalEntry::UnMount(e) => e.rpc_id,
            JournalEntry::SetAttr(e) => e.rpc_id,
            JournalEntry::Symlink(e) => e.rpc_id,
            JournalEntry::Link(e) => e.rpc_id,
            JournalEntry::SetLocks(e) => e.rpc_id,
            JournalEntry::Free(e) => e.rpc_id,
            JournalEntry::CacheInvalidation(e) => e.rpc_id,
            JournalEntry::UfsApplied(e) => e.rpc_id,
            JournalEntry::Snapshot(e) => e.rpc_id,
            JournalEntry::CacheIdReserve(e) => e.rpc_id,
            JournalEntry::CacheIncarnationAllocate(e) => e.rpc_id,
            JournalEntry::CacheIncarnationRevoke(e) => e.rpc_id,
            JournalEntry::CacheAllocate(e) => e.rpc_id,
            JournalEntry::CacheCommit(e) => e.rpc_id,
            JournalEntry::CacheRemove(e) => e.rpc_id,
            JournalEntry::CacheIncarnationAllocateV2(e) => e.rpc_id,
        }
    }

    pub fn inode_id(&self) -> Option<i64> {
        match self {
            JournalEntry::Mkdir(e) => Some(e.dir.id),
            JournalEntry::CreateFile(e) => Some(e.file.id),
            JournalEntry::ReopenFile(e) => Some(e.file.id),
            JournalEntry::OverWriteFile(e) => Some(e.file.id),
            JournalEntry::CompleteFile(e) => Some(e.file.id),
            JournalEntry::Symlink(e) => Some(e.new_inode.id),
            JournalEntry::SetLocks(e) => Some(e.ino),
            _ => None,
        }
    }

    pub(crate) fn cv_metadata_changes(&self) -> Vec<CvMetadataChange> {
        match self {
            JournalEntry::Mkdir(e) => vec![CvMetadataChange::single(e.op_id, &e.path)],
            JournalEntry::CreateFile(e) => vec![CvMetadataChange::single(e.op_id, &e.path)],
            JournalEntry::ReopenFile(e) => vec![CvMetadataChange::single(e.op_id, &e.path)],
            JournalEntry::OverWriteFile(e) => vec![CvMetadataChange::single(e.op_id, &e.path)],
            JournalEntry::AddBlock(e) => vec![CvMetadataChange::single(e.op_id, &e.path)],
            JournalEntry::CompleteFile(e) => vec![CvMetadataChange::single(e.op_id, &e.path)],
            JournalEntry::Rename(e) => vec![
                CvMetadataChange::subtree(e.op_id, &e.src),
                CvMetadataChange::subtree(e.op_id, &e.dst),
            ],
            JournalEntry::Delete(e) => vec![CvMetadataChange::subtree(e.op_id, &e.path)],
            JournalEntry::SetAttr(e) => vec![CvMetadataChange::single(e.op_id, &e.path)],
            JournalEntry::Symlink(e) => vec![CvMetadataChange::single(e.op_id, &e.link)],
            JournalEntry::Link(e) => vec![
                CvMetadataChange::single(e.op_id, &e.src_path),
                CvMetadataChange::single(e.op_id, &e.dst_path),
            ],
            JournalEntry::Free(e) => vec![CvMetadataChange::subtree(e.op_id, &e.path)],
            JournalEntry::Mount(_)
            | JournalEntry::UnMount(_)
            | JournalEntry::SetLocks(_)
            | JournalEntry::CacheInvalidation(_)
            | JournalEntry::UfsApplied(_)
            | JournalEntry::Snapshot(_)
            | JournalEntry::CacheIdReserve(_)
            | JournalEntry::CacheIncarnationAllocate(_)
            | JournalEntry::CacheIncarnationRevoke(_)
            | JournalEntry::CacheAllocate(_)
            | JournalEntry::CacheCommit(_)
            | JournalEntry::CacheRemove(_)
            | JournalEntry::CacheIncarnationAllocateV2(_) => Vec::new(),
        }
    }

    pub fn allocated_inode_id(&self) -> Option<i64> {
        match self {
            JournalEntry::Mkdir(e) => Some(e.dir.id),
            JournalEntry::CreateFile(e) => Some(e.file.id),
            JournalEntry::Symlink(e) => Some(e.new_inode.id),
            _ => None,
        }
    }

    /// Whether this entry belongs to the cache metadata domain. Cache
    /// entries are applied on leader AND follower by the single committed
    /// `CacheManager` path — never by the leader pre-apply / UFS loader,
    /// and they have no UFS side effects (task #3, contract §3).
    pub fn is_cache_entry(&self) -> bool {
        matches!(
            self,
            JournalEntry::CacheIdReserve(_)
                | JournalEntry::CacheIncarnationAllocate(_)
                | JournalEntry::CacheIncarnationRevoke(_)
                | JournalEntry::CacheAllocate(_)
                | JournalEntry::CacheCommit(_)
                | JournalEntry::CacheRemove(_)
                | JournalEntry::CacheIncarnationAllocateV2(_)
        )
    }
}

impl CvMetadataChange {
    fn single(op_id: u64, path: &str) -> Self {
        Self {
            op_id,
            path: path.to_string(),
            include_subtree: false,
        }
    }

    fn subtree(op_id: u64, path: &str) -> Self {
        Self {
            op_id,
            path: path.to_string(),
            include_subtree: true,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct JournalBatch {
    pub(crate) seq_id: u64,
    pub(crate) batch: Vec<JournalEntry>,
}

impl JournalBatch {
    pub(crate) fn deserialize_compat(bytes: &[u8]) -> CommonResult<Self> {
        match SerdeUtils::deserialize(bytes) {
            Ok(batch) => Ok(batch),
            Err(current_err) => match deserialize_legacy_batch(bytes) {
                Ok(batch) => {
                    debug!(
                        "replaying legacy journal batch with pre-extension entry schemas, seq_id={}",
                        batch.seq_id
                    );
                    Ok(batch.into())
                }
                Err(legacy_err) => err_box!(
                    "failed to deserialize journal batch with current or legacy schemas: current={}, legacy={}",
                    current_err,
                    legacy_err
                ),
            },
        }
    }

    pub fn new(seq_id: u64) -> Self {
        Self {
            seq_id,
            batch: vec![],
        }
    }

    pub fn push(&mut self, entry: JournalEntry) {
        self.batch.push(entry)
    }

    pub fn len(&self) -> usize {
        self.batch.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn next(&mut self) {
        self.seq_id += 1;
        self.batch.clear();
    }
}

// bincode encodes struct fields positionally, so appending fields to RenameEntry
// cannot be made backward compatible with serde(default). Keep this schema only
// for replaying entries written before exchange inode ids were introduced.
#[derive(Deserialize)]
struct LegacyRenameEntry {
    op_id: u64,
    rpc_id: i64,
    src: String,
    dst: String,
    mtime: i64,
    flags: u32,
}

// FreeEntry::recursive was appended in #721. Old bincode entries do not have
// this field, so decoding them with the current type consumes the next entry's
// enum tag as a bool.
#[derive(Deserialize)]
struct LegacyFreeEntry {
    op_id: u64,
    rpc_id: i64,
    path: String,
    mtime: i64,
}

#[derive(Deserialize)]
enum LegacyJournalEntry {
    Mkdir(MkdirEntry),
    CreateFile(CreateFileEntry),
    ReopenFile(ReopenFileEntry),
    OverWriteFile(OverWriteFileEntry),
    AddBlock(AddBlockEntry),
    CompleteFile(CompleteFileEntry),
    Rename(LegacyRenameEntry),
    Delete(DeleteEntry),
    Mount(MountEntry),
    UnMount(UnMountEntry),
    SetAttr(SetAttrEntry),
    Symlink(SymlinkEntry),
    Link(LinkEntry),
    SetLocks(SetLocksEntry),
    Free(LegacyFreeEntry),
    UfsApplied(UfsAppliedEntry),
    Snapshot(SnapshotEntry),
}

#[derive(Deserialize)]
struct LegacyJournalBatch {
    seq_id: u64,
    batch: Vec<LegacyJournalEntry>,
}

fn deserialize_legacy_batch(bytes: &[u8]) -> CommonResult<LegacyJournalBatch> {
    let mut reader = Cursor::new(bytes);
    let batch = SerdeUtils::deserialize_from(&mut reader)?;
    if reader.position() != bytes.len() as u64 {
        return err_box!("legacy journal batch has trailing bytes");
    }
    Ok(batch)
}

impl From<LegacyJournalBatch> for JournalBatch {
    fn from(batch: LegacyJournalBatch) -> Self {
        Self {
            seq_id: batch.seq_id,
            batch: batch.batch.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<LegacyJournalEntry> for JournalEntry {
    fn from(entry: LegacyJournalEntry) -> Self {
        match entry {
            LegacyJournalEntry::Mkdir(entry) => Self::Mkdir(entry),
            LegacyJournalEntry::CreateFile(entry) => Self::CreateFile(entry),
            LegacyJournalEntry::ReopenFile(entry) => Self::ReopenFile(entry),
            LegacyJournalEntry::OverWriteFile(entry) => Self::OverWriteFile(entry),
            LegacyJournalEntry::AddBlock(entry) => Self::AddBlock(entry),
            LegacyJournalEntry::CompleteFile(entry) => Self::CompleteFile(entry),
            LegacyJournalEntry::Rename(entry) => Self::Rename(RenameEntry {
                op_id: entry.op_id,
                rpc_id: entry.rpc_id,
                src: entry.src,
                dst: entry.dst,
                mtime: entry.mtime,
                flags: entry.flags,
                src_inode_id: 0,
                dst_inode_id: 0,
            }),
            LegacyJournalEntry::Delete(entry) => Self::Delete(entry),
            LegacyJournalEntry::Mount(entry) => Self::Mount(entry),
            LegacyJournalEntry::UnMount(entry) => Self::UnMount(entry),
            LegacyJournalEntry::SetAttr(entry) => Self::SetAttr(entry),
            LegacyJournalEntry::Symlink(entry) => Self::Symlink(entry),
            LegacyJournalEntry::Link(entry) => Self::Link(entry),
            LegacyJournalEntry::SetLocks(entry) => Self::SetLocks(entry),
            LegacyJournalEntry::Free(entry) => Self::Free(FreeEntry {
                op_id: entry.op_id,
                rpc_id: entry.rpc_id,
                path: entry.path,
                mtime: entry.mtime,
                recursive: false,
            }),
            LegacyJournalEntry::UfsApplied(entry) => Self::UfsApplied(entry),
            LegacyJournalEntry::Snapshot(entry) => Self::Snapshot(entry),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // bincode 1.x encodes an enum variant as a u32 LE positional index
    // prefix. Journals persisted by older binaries replay on newer ones
    // purely by positional stability, so this test is the golden wire
    // format: 0..=17 are the pre-cache variants (identical to main at the
    // branch base), and the cache block appended by task #3 phase 1c must
    // occupy 18..=23. Reordering or inserting any variant silently
    // corrupts every persisted journal.
    fn discriminant(entry: &JournalEntry) -> u32 {
        let bytes = SerdeUtils::serialize(entry).expect("serialize journal entry");
        assert!(bytes.len() >= 4, "bincode output too short for variant tag");
        let mut tag = [0u8; 4];
        tag.copy_from_slice(&bytes[..4]);
        u32::from_le_bytes(tag)
    }

    fn file() -> InodeFile {
        InodeFile::new(1, 1)
    }

    fn dir() -> InodeDir {
        InodeDir::new(1, 1)
    }

    /// Frozen hex fixture helpers (4b compat gate, review msgs 2b88a96c /
    /// 5883932e): bytes written before the V2 variants existed must keep
    /// decoding at HEAD, V2 must roundtrip, and mixed batches must not
    /// crosswalk variants.
    fn hex_decode(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    fn hex_encode(bytes: &[u8]) -> String {
        bytes.iter().map(|x| format!("{:02x}", x)).collect()
    }

    fn legacy_allocate_fixture() -> CacheIncarnationAllocateEntry {
        CacheIncarnationAllocateEntry {
            op_id: 41,
            rpc_id: -7,
            token: OpToken {
                client_id: 99,
                op_seq: 7,
            },
            mount_id: 3,
            incarnation: 12,
        }
    }

    fn v2_allocate_fixture() -> CacheIncarnationAllocateV2Entry {
        CacheIncarnationAllocateV2Entry {
            op_id: 41,
            rpc_id: -7,
            token: OpToken {
                client_id: 99,
                op_seq: 7,
            },
            mount_id: 3,
            incarnation: 12,
            ttl_ms: 3_600_000,
            cache_write: true,
        }
    }

    #[test]
    fn test_frozen_7e3f8a02_journal_bytes_decode_at_head() {
        // Frozen bytes of the legacy CacheIncarnationAllocate journal entry.
        // Provenance: serialized from the legacy struct at 4a HEAD 7e3f8a02
        // layout; the diff since then is additive-only (V2 variant appended
        // at the enum tail), so any journal segment written by 4a decodes
        // bit-identically here.
        const LEGACY_HEX: &str = "130000002900000000000000f9ffffffffffffff63000000000000000700000000000000030000000c00000000000000";
        let entry: JournalEntry = SerdeUtils::deserialize(&hex_decode(LEGACY_HEX)).unwrap();
        match entry {
            JournalEntry::CacheIncarnationAllocate(e) => {
                assert_eq!(e.op_id, 41);
                assert_eq!(e.rpc_id, -7);
                assert_eq!(e.token.client_id, 99);
                assert_eq!(e.token.op_seq, 7);
                assert_eq!(e.mount_id, 3);
                assert_eq!(e.incarnation, 12);
            }
            other => panic!("legacy bytes crosswalked to {:?}", other),
        }
        // Re-encoding the decoded legacy entry reproduces the frozen bytes
        // (no silent layout drift).
        assert_eq!(
            hex_encode(
                &SerdeUtils::serialize(&JournalEntry::CacheIncarnationAllocate(
                    legacy_allocate_fixture()
                ))
                .unwrap()
            ),
            LEGACY_HEX
        );
    }

    #[test]
    fn test_incarnation_allocate_v2_roundtrip() {
        const V2_HEX: &str = "180000002900000000000000f9ffffffffffffff63000000000000000700000000000000030000000c0000000000000080ee36000000000001";
        let bytes = SerdeUtils::serialize(&JournalEntry::CacheIncarnationAllocateV2(
            v2_allocate_fixture(),
        ))
        .unwrap();
        assert_eq!(hex_encode(&bytes), V2_HEX);
        let entry: JournalEntry = SerdeUtils::deserialize(&bytes).unwrap();
        match entry {
            JournalEntry::CacheIncarnationAllocateV2(e) => {
                assert_eq!(e.op_id, 41);
                assert_eq!(e.ttl_ms, 3_600_000);
                assert!(e.cache_write);
            }
            other => panic!("v2 bytes crosswalked to {:?}", other),
        }
        // The V2 discriminant (0x18 = 24) sits strictly above every legacy
        // variant: old binaries fail loudly on V2 instead of misdecoding.
        assert!(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) > 23);
    }

    #[test]
    fn test_mixed_legacy_v2_journal_batch_no_crosswalk() {
        // A journal segment mixing pre-4b and 4b entries must decode each
        // entry to its own variant.
        let mixed = vec![
            JournalEntry::CacheIdReserve(CacheIdReserveEntry {
                op_id: 1,
                rpc_id: 0,
                token: token(),
                start: 1,
                end: 2,
            }),
            JournalEntry::CacheIncarnationAllocate(legacy_allocate_fixture()),
            JournalEntry::CacheIncarnationAllocateV2(v2_allocate_fixture()),
            JournalEntry::CacheRemove(CacheRemoveEntry {
                op_id: 2,
                rpc_id: 0,
                incarnation: 12,
                key: "/k".into(),
                expected_generation: 1,
                new_generation: 2,
                expected_object_id: crate::master::meta::cache::BlockIdCodec::CACHE_OBJECT_MIN,
            }),
        ];
        let bytes = SerdeUtils::serialize(&mixed).unwrap();
        let back: Vec<JournalEntry> = SerdeUtils::deserialize(&bytes).unwrap();
        assert_eq!(back.len(), 4);
        assert!(matches!(back[0], JournalEntry::CacheIdReserve(_)));
        assert!(matches!(back[1], JournalEntry::CacheIncarnationAllocate(_)));
        assert!(matches!(
            back[2],
            JournalEntry::CacheIncarnationAllocateV2(_)
        ));
        assert!(matches!(back[3], JournalEntry::CacheRemove(_)));
    }

    fn token() -> OpToken {
        OpToken {
            client_id: 7,
            op_seq: 3,
        }
    }

    #[test]
    fn test_bincode_variant_discriminants_are_stable() {
        let cases: Vec<(JournalEntry, u32)> = vec![
            (
                JournalEntry::Mkdir(MkdirEntry {
                    op_id: 1,
                    rpc_id: 0,
                    path: "/d".into(),
                    dir: dir(),
                }),
                0,
            ),
            (
                JournalEntry::CreateFile(CreateFileEntry {
                    op_id: 1,
                    rpc_id: 0,
                    path: "/f".into(),
                    file: file(),
                }),
                1,
            ),
            (
                JournalEntry::ReopenFile(ReopenFileEntry {
                    op_id: 1,
                    rpc_id: 0,
                    path: "/f".into(),
                    file: file(),
                }),
                2,
            ),
            (
                JournalEntry::OverWriteFile(OverWriteFileEntry {
                    op_id: 1,
                    rpc_id: 0,
                    path: "/f".into(),
                    file: file(),
                }),
                3,
            ),
            (
                JournalEntry::AddBlock(AddBlockEntry {
                    op_id: 1,
                    rpc_id: 0,
                    path: "/f".into(),
                    blocks: vec![],
                    commit_block: vec![],
                }),
                4,
            ),
            (
                JournalEntry::CompleteFile(CompleteFileEntry {
                    op_id: 1,
                    rpc_id: 0,
                    path: "/f".into(),
                    file: file(),
                    commit_blocks: vec![],
                }),
                5,
            ),
            (
                JournalEntry::Rename(RenameEntry {
                    op_id: 1,
                    rpc_id: 0,
                    src: "/a".into(),
                    dst: "/b".into(),
                    mtime: 1,
                    flags: 0,
                    src_inode_id: 0,
                    dst_inode_id: 0,
                }),
                6,
            ),
            (
                JournalEntry::Delete(DeleteEntry {
                    op_id: 1,
                    rpc_id: 0,
                    path: "/f".into(),
                    mtime: 1,
                }),
                7,
            ),
            (
                JournalEntry::Mount(MountEntry {
                    op_id: 1,
                    rpc_id: 0,
                    info: MountInfo::default(),
                }),
                8,
            ),
            (
                JournalEntry::UnMount(UnMountEntry {
                    op_id: 1,
                    rpc_id: 0,
                    id: 1,
                }),
                9,
            ),
            (
                JournalEntry::SetAttr(SetAttrEntry {
                    op_id: 1,
                    rpc_id: 0,
                    path: "/f".into(),
                    opts: SetAttrOpts::default(),
                }),
                10,
            ),
            (
                JournalEntry::Symlink(SymlinkEntry {
                    op_id: 1,
                    rpc_id: 0,
                    link: "/l".into(),
                    new_inode: file(),
                    force: false,
                }),
                11,
            ),
            (
                JournalEntry::Link(LinkEntry {
                    op_id: 1,
                    rpc_id: 0,
                    mtime: 1,
                    src_path: "/a".into(),
                    dst_path: "/b".into(),
                }),
                12,
            ),
            (
                JournalEntry::SetLocks(SetLocksEntry {
                    op_id: 1,
                    rpc_id: 0,
                    ino: 1,
                    locks: vec![],
                }),
                13,
            ),
            (
                JournalEntry::Free(FreeEntry {
                    op_id: 1,
                    rpc_id: 0,
                    path: "/f".into(),
                    mtime: 1,
                    recursive: false,
                }),
                14,
            ),
            (
                JournalEntry::UfsApplied(UfsAppliedEntry {
                    op_id: 1,
                    rpc_id: 0,
                    term: 1,
                    index: 1,
                }),
                15,
            ),
            (
                JournalEntry::Snapshot(SnapshotEntry {
                    op_id: 1,
                    rpc_id: 0,
                    node_id: 1,
                    dir: "/s".into(),
                }),
                16,
            ),
            (
                JournalEntry::CacheInvalidation(CacheInvalidationEntry {
                    op_id: 1,
                    rpc_id: 0,
                    inodes: vec![],
                }),
                17,
            ),
            // Cache-mode block appended at the tail (task #3 phase 1c).
            (
                JournalEntry::CacheIdReserve(CacheIdReserveEntry {
                    op_id: 1,
                    rpc_id: 0,
                    token: token(),
                    start: 1,
                    end: 2,
                }),
                18,
            ),
            (
                JournalEntry::CacheIncarnationAllocate(CacheIncarnationAllocateEntry {
                    op_id: 1,
                    rpc_id: 0,
                    token: token(),
                    mount_id: 1,
                    incarnation: 1,
                }),
                19,
            ),
            (
                JournalEntry::CacheIncarnationRevoke(CacheIncarnationRevokeEntry {
                    op_id: 1,
                    rpc_id: 0,
                    mount_id: 1,
                    incarnation: 1,
                }),
                20,
            ),
            (
                JournalEntry::CacheAllocate(CacheAllocateEntry {
                    op_id: 1,
                    rpc_id: 0,
                    token: token(),
                    incarnation: 1,
                    key: "/k".into(),
                    file_len: 1,
                    entry: CacheEntry {
                        generation: 1,
                        state: crate::master::meta::cache::CacheEntryState::Reserved,
                        object_id: crate::master::meta::cache::BlockIdCodec::CACHE_OBJECT_MIN,
                        len: 0,
                        ufs_mtime: 0,
                        block_size: 64,
                        expire_at: 0,
                    },
                }),
                21,
            ),
            (
                JournalEntry::CacheCommit(CacheCommitEntry {
                    op_id: 1,
                    rpc_id: 0,
                    token: token(),
                    load_token: token(),
                    incarnation: 1,
                    key: "/k".into(),
                    generation: 1,
                    expected_object_id: crate::master::meta::cache::BlockIdCodec::CACHE_OBJECT_MIN,
                    len: 1,
                    ufs_mtime: 1,
                    expire_at: 0,
                }),
                22,
            ),
            (
                JournalEntry::CacheRemove(CacheRemoveEntry {
                    op_id: 1,
                    rpc_id: 0,
                    incarnation: 1,
                    key: "/k".into(),
                    expected_generation: 1,
                    new_generation: 2,
                    expected_object_id: crate::master::meta::cache::BlockIdCodec::CACHE_OBJECT_MIN,
                }),
                23,
            ),
            // 4b append: V2 incarnation allocation at the tail. Everything
            // above keeps its discriminant byte (frozen-bytes compat gate).
            (
                JournalEntry::CacheIncarnationAllocateV2(CacheIncarnationAllocateV2Entry {
                    op_id: 1,
                    rpc_id: 0,
                    token: token(),
                    mount_id: 1,
                    incarnation: 1,
                    ttl_ms: 0,
                    cache_write: true,
                }),
                24,
            ),
        ];

        for (entry, expected) in cases {
            assert_eq!(
                discriminant(&entry),
                expected,
                "bincode variant discriminant moved for {:?}",
                entry
            );
        }
    }
}
