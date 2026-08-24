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

#![allow(unused)]
use crate::master::fs::MasterFilesystem;
use crate::master::meta::cache::{
    LocalCacheIndexStore, MountLifecycleKind, MountLifecycleStatus, OpOutcome, OpToken,
};
use crate::master::mount::MountTable;
use crate::master::{self, SyncFsDir};
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{self, CurvineURI, Path};
use curvine_model::{MkdirOpts, MountInfo, MountOptions};
use curvine_ufs_api::S3Conf;
use log::info;

pub struct MountManager {
    master_fs: MasterFilesystem,
    mount_table: MountTable,
}

impl MountManager {
    pub fn new(master_fs: MasterFilesystem) -> Self {
        let fs_dir = master_fs.fs_dir.clone();
        MountManager {
            master_fs,
            mount_table: MountTable::new(fs_dir),
        }
    }

    /// recovery mount points from store
    pub fn restore(&self) -> FsResult<()> {
        self.mount_table.restore()
    }

    pub fn restore_best_effort(&self) {
        self.mount_table.restore_best_effort()
    }

    fn create_mount_point(&self, mount_path: &str) -> FsResult<bool> {
        let exist = self.master_fs.exists(mount_path)?;
        if exist {
            return Ok(true);
        }

        let opts = MkdirOpts::with_create(true);
        self.master_fs.mkdir_with_opts(mount_path, opts)?;
        Ok(true)
    }

    fn normalize_mount_config(mount: &mut MountInfo) -> FsResult<()> {
        // D7 fail-closed (task #6 / gpt56 92883fff option C): write-through
        // client cache mirroring is retired; `write_cache=true` is rejected
        // for ALL mounts, including cache-mode read-write.
        if mount.write_cache {
            return err_box!(
                "write_cache is not supported (fail-closed): mount {}",
                mount.cv_path
            );
        }

        let path = Path::from_str(&mount.ufs_path)?;
        if !matches!(path.scheme(), Some("s3" | "s3a")) {
            return Ok(());
        }

        let properties = std::mem::take(&mut mount.properties);
        mount.properties = S3Conf::canonicalize_properties(properties).map_err(|err| {
            FsError::common(format!(
                "Invalid mount configuration for {}: {}",
                mount.ufs_path, err
            ))
        })?;
        S3Conf::validate(&mount.properties).map_err(|err| {
            FsError::common(format!(
                "Invalid mount configuration for {}: {}",
                mount.ufs_path, err
            ))
        })
    }

    /// P4-0 fail-closed token gate (task #6, gpt56 a929ae03 point 3):
    /// every composite mount lifecycle op on the wire MUST carry a typed
    /// non-zero op token; an absent/zero token never falls back to the
    /// legacy non-atomic path.
    fn require_lifecycle_token(token: Option<OpToken>) -> FsResult<OpToken> {
        match token {
            Some(t) if t.client_id != 0 && t.op_seq != 0 => Ok(t),
            _ => err_box!(
                "cache-mode mount lifecycle requires a non-zero op token (op_client_id/op_seq on the wire); retry the mount RPC with a fresh token"
            ),
        }
    }

    /// A Superseded lifecycle (the CAS lost to a racing writer) is a loud
    /// caller-visible failure: the durable state did NOT move to this
    /// request's target and only a fresh re-read + retry can resolve it.
    fn lifecycle_result(status: MountLifecycleStatus, what: &str) -> FsResult<()> {
        match status {
            MountLifecycleStatus::Executed | MountLifecycleStatus::AlreadyApplied => Ok(()),
            MountLifecycleStatus::Superseded => err_box!(
                "mount {} raced a concurrent mount change (Superseded): re-read the mount table and retry with a fresh token",
                what
            ),
        }
    }

    /// P4-0 outcome-first retry resolution (gpt56 a4e3804f blocker 1):
    /// resolve an exact recorded retry from the durable lifecycle outcome
    /// BEFORE mount-id assignment, conflict prechecks, delta detection, or
    /// not-found checks — a response-loss retry of a committed Add/Update/
    /// Unmount would otherwise die on "already exists" / fall to the legacy
    /// path / die on "not found". The comparison re-derives the request's
    /// effect from the ORIGINAL request parameters and the recorded
    /// outcome's frozen rows; a mismatch is loud divergence, never a
    /// silent rebinding.
    ///
    /// `mnt_opt` is the original options for Add/Update and `None` for
    /// Unmount; `ufs_path` is only meaningful for Add.
    fn lifecycle_retry_resolved(
        &self,
        token: Option<OpToken>,
        kind: MountLifecycleKind,
        cv_path: &str,
        ufs_path: &str,
        mnt_opt: Option<&MountOptions>,
    ) -> FsResult<bool> {
        let Some(token) = token else {
            return Ok(false);
        };
        if token.client_id == 0 || token.op_seq == 0 {
            // Malformed tokens fail the require gate downstream with the
            // full context; nothing to resolve here.
            return Ok(false);
        }

        let outcome = {
            let fs = self.master_fs.fs_dir.read();
            fs.get_rocks_store().cache_get_outcome(token)?
        };
        match outcome {
            // Blocker 4 (gpt56 6ba07ee2): a missing recorded outcome with
            // the token at/below the client watermark is a terminal Expired
            // — the outcome was GC'd or never committed and never will.
            // Fail loudly here, BEFORE id assignment / conflict prechecks /
            // delta detection / not-found checks can misfire; zero
            // proposals on all three kinds.
            None => {
                let watermark = {
                    let fs = self.master_fs.fs_dir.read();
                    fs.get_rocks_store()
                        .cache_client_watermark(token.client_id)?
                };
                match watermark {
                    Some(hw) if token.op_seq <= hw => err_box!(
                        "mount lifecycle token {:?} is expired (client watermark {}): terminal, re-issue with a fresh token",
                        token,
                        hw
                    ),
                    _ => Ok(false),
                }
            }
            Some(OpOutcome::MountLifecycle {
                kind: out_kind,
                mount_id,
                expected_mount,
                next_mount,
                ..
            }) => {
                if out_kind != kind {
                    return err_box!(
                        "mount lifecycle retry token {:?} recorded kind {:?}, request kind {:?}",
                        token,
                        out_kind,
                        kind
                    );
                }
                let matches = match kind {
                    MountLifecycleKind::Add => {
                        let Some(next) = next_mount.as_ref() else {
                            return err_box!(
                                "mount lifecycle retry token {:?} recorded add without a next row",
                                token
                            );
                        };
                        if next.cv_path != cv_path || next.ufs_path != ufs_path {
                            false
                        } else {
                            let Some(opt) = mnt_opt else {
                                return err_box!(
                                    "mount lifecycle retry token {:?} add without request options",
                                    token
                                );
                            };
                            let mut rebuilt = opt.clone().to_info(mount_id, cv_path, ufs_path);
                            Self::normalize_mount_config(&mut rebuilt).is_ok()
                                && rebuilt == *next
                        }
                    }
                    MountLifecycleKind::Update => {
                        let (Some(expected), Some(next)) =
                            (expected_mount.as_ref(), next_mount.as_ref())
                        else {
                            return err_box!(
                                "mount lifecycle retry token {:?} recorded update without frozen rows",
                                token
                            );
                        };
                        if expected.cv_path != cv_path {
                            false
                        } else {
                            let Some(opt) = mnt_opt else {
                                return err_box!(
                                    "mount lifecycle retry token {:?} update without request options",
                                    token
                                );
                            };
                            let mut merged = expected.clone().merge_with(opt.clone());
                            Self::normalize_mount_config(&mut merged).is_ok() && merged == *next
                        }
                    }
                    MountLifecycleKind::Unmount => match expected_mount.as_ref() {
                        Some(expected) => expected.cv_path == cv_path,
                        None => {
                            return err_box!(
                                "mount lifecycle retry token {:?} recorded unmount without the frozen row",
                                token
                            )
                        }
                    },
                };
                if matches {
                    Ok(true)
                } else {
                    err_box!(
                        "mount lifecycle retry token {:?} replayed with different parameters (kind {:?}, cv {})",
                        token,
                        kind,
                        cv_path
                    )
                }
            }
            // Blocker 1 routing half: a recorded loser verdict reproduces
            // loudly on the same-token retry. The durable state did NOT
            // move to this request's target — never AlreadyApplied, never
            // a silent rebinding onto a different request.
            Some(OpOutcome::MountLifecycleRejected {
                kind: r_kind,
                mount_id,
                expected_mount,
                next_mount,
                reason,
                ..
            }) => {
                if r_kind != kind {
                    return err_box!(
                        "mount lifecycle retry token {:?} recorded rejected kind {:?}, request kind {:?}",
                        token,
                        r_kind,
                        kind
                    );
                }
                let matches = match kind {
                    MountLifecycleKind::Add => {
                        let Some(next) = next_mount.as_ref() else {
                            return err_box!(
                                "mount lifecycle retry token {:?} recorded rejected add without a next row",
                                token
                            );
                        };
                        if next.cv_path != cv_path || next.ufs_path != ufs_path {
                            false
                        } else {
                            let Some(opt) = mnt_opt else {
                                return err_box!(
                                    "mount lifecycle retry token {:?} add without request options",
                                    token
                                );
                            };
                            let mut rebuilt = opt.clone().to_info(mount_id, cv_path, ufs_path);
                            Self::normalize_mount_config(&mut rebuilt).is_ok() && rebuilt == *next
                        }
                    }
                    MountLifecycleKind::Update => {
                        let (Some(expected), Some(next)) =
                            (expected_mount.as_ref(), next_mount.as_ref())
                        else {
                            return err_box!(
                                "mount lifecycle retry token {:?} recorded rejected update without frozen rows",
                                token
                            );
                        };
                        if expected.cv_path != cv_path {
                            false
                        } else {
                            let Some(opt) = mnt_opt else {
                                return err_box!(
                                    "mount lifecycle retry token {:?} update without request options",
                                    token
                                );
                            };
                            let mut merged = expected.clone().merge_with(opt.clone());
                            Self::normalize_mount_config(&mut merged).is_ok() && merged == *next
                        }
                    }
                    MountLifecycleKind::Unmount => match expected_mount.as_ref() {
                        Some(expected) => expected.cv_path == cv_path,
                        None => false,
                    },
                };
                if !matches {
                    return err_box!(
                        "mount lifecycle retry token {:?} replayed with different parameters (kind {:?}, cv {})",
                        token,
                        kind,
                        cv_path
                    );
                }
                err_box!(
                    "mount {} lost the composite lifecycle CAS ({:?}): durable state was not changed, re-read the mount table and retry with a fresh token",
                    cv_path,
                    reason
                )
            }
            Some(other) => err_box!(
                "mount lifecycle retry token {:?} has a non-lifecycle committed outcome: {:?}",
                token,
                other
            ),
        }
    }

    /// same baseuri of ufs can only mount once
    ///
    /// ufs_uri maybe scheme://authority/xxxx/yyy,
    /// base_uri is scheme://authority/
    fn add_mount(
        &self,
        mnt_id: Option<u32>,
        mount_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
        token: Option<OpToken>,
        rpc_id: i64,
    ) -> FsResult<()> {
        let assign_id = match mnt_id {
            Some(id) => id,
            None => self.mount_table.assign_mount_id()?,
        };
        let mut mount = mnt_opt.clone().to_info(assign_id, mount_path, ufs_path);
        Self::normalize_mount_config(&mut mount)?;
        let _ = self.create_mount_point(mount_path)?;

        // P4-0: a cache-mode mount row and its namespace identity move in
        // ONE committed lifecycle entry (mount row + incarnation + policy
        // + pointer + HW); the live table converges at journal apply.
        if mount.is_cache_mode() {
            let token = Self::require_lifecycle_token(token)?;
            self.mount_table.check_new_mount(mount_path, ufs_path)?;
            let status = self.master_fs.cache_service.mount_lifecycle(
                token,
                rpc_id,
                MountLifecycleKind::Add,
                assign_id,
                Some(mount),
            )?;
            return Self::lifecycle_result(status, "add");
        }

        let mut normalized_options = mnt_opt.clone();
        normalized_options.add_properties = mount.properties;
        self.mount_table
            .add_mount(assign_id, mount_path, ufs_path, &normalized_options)
    }

    fn update_mount(
        &self,
        cv_path: &str,
        mnt_opt: &MountOptions,
        token: Option<OpToken>,
        rpc_id: i64,
    ) -> FsResult<()> {
        let path = Path::from_str(cv_path)?;
        let Some(existing) = self.get_mount_info(&path)? else {
            return err_box!("mount point {} not found for update", cv_path);
        };
        let mut merged = existing.clone().merge_with(mnt_opt.clone());
        Self::normalize_mount_config(&mut merged)?;

        // q3 delta fence (gpt56 ruling): the composite switch runs ONLY on
        // a cache-mode enter/leave or a TTL change — the transitions that
        // invalidate the frozen namespace policy. Property / capability
        // updates stay on the legacy path with the incarnation untouched.
        let mode_crosses = existing.is_cache_mode() != merged.is_cache_mode();
        let ttl_changes =
            existing.is_cache_mode() && merged.is_cache_mode() && existing.ttl_ms != merged.ttl_ms;
        if mode_crosses || ttl_changes {
            let token = Self::require_lifecycle_token(token)?;
            if merged.cv_path != existing.cv_path || merged.ufs_path != existing.ufs_path {
                return err_box!("cannot change mount path");
            }
            let status = self.master_fs.cache_service.mount_lifecycle(
                token,
                rpc_id,
                MountLifecycleKind::Update,
                merged.mount_id,
                Some(merged),
            )?;
            return Self::lifecycle_result(status, "update");
        }

        self.mount_table.update_mount(merged)
    }

    /// same baseuri of ufs can only mount once
    ///
    /// ufs_uri maybe scheme://authority/xxxx/yyy,
    /// base_uri is scheme://authority/
    pub fn mount(
        &self,
        mnt_id: Option<u32>,
        cv_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
    ) -> FsResult<()> {
        self.mount_with_token(mnt_id, cv_path, ufs_path, mnt_opt, None, 0)
    }

    /// Tokened mount entry (task #6 P4-0): cache-mode add/update
    /// transitions MUST carry a non-zero op token and route through the
    /// composite lifecycle; fs-mode mounts keep the legacy path.
    pub fn mount_with_token(
        &self,
        mnt_id: Option<u32>,
        cv_path: &str,
        ufs_path: &str,
        mnt_opt: &MountOptions,
        token: Option<OpToken>,
        rpc_id: i64,
    ) -> FsResult<()> {
        // Outcome-first (a4e3804f blocker 1): an exact recorded retry
        // resolves here, before id assignment / conflict prechecks / delta
        // detection can misfire on the already-committed state.
        let kind = if mnt_opt.update {
            MountLifecycleKind::Update
        } else {
            MountLifecycleKind::Add
        };
        if self.lifecycle_retry_resolved(token, kind, cv_path, ufs_path, Some(mnt_opt))? {
            return Ok(());
        }

        if mnt_opt.update {
            return self.update_mount(cv_path, mnt_opt, token, rpc_id);
        }

        self.add_mount(mnt_id, cv_path, ufs_path, mnt_opt, token, rpc_id)
    }

    pub fn unprotected_add_mount(&self, info: MountInfo) -> FsResult<()> {
        self.mount_table.unprotected_add_mount(info)
    }

    pub fn umount(&self, cv_path: &str) -> FsResult<()> {
        self.umount_with_token(cv_path, None, 0)
    }

    /// Tokened unmount (task #6 P4-0): unmounting a cache-mode mount
    /// revokes its current incarnation and removes the mount row in ONE
    /// committed lifecycle entry; fs-mode unmounts keep the legacy path.
    pub fn umount_with_token(
        &self,
        cv_path: &str,
        token: Option<OpToken>,
        rpc_id: i64,
    ) -> FsResult<()> {
        // Outcome-first (a4e3804f blocker 1): resolve a committed unmount
        // retry before the not-found check can misfire.
        if self.lifecycle_retry_resolved(token, MountLifecycleKind::Unmount, cv_path, "", None)? {
            return Ok(());
        }

        let path = Path::from_str(cv_path)?;
        let Some(info) = self.get_mount_info(&path)? else {
            return err_box!("failed found {} to umount", cv_path);
        };

        if info.is_cache_mode() {
            let token = Self::require_lifecycle_token(token)?;
            let status = self.master_fs.cache_service.mount_lifecycle(
                token,
                rpc_id,
                MountLifecycleKind::Unmount,
                info.mount_id,
                None,
            )?;
            return Self::lifecycle_result(status, "unmount");
        }

        self.mount_table.umount(cv_path)
    }

    pub fn unmount_by_id(&self, id: u32) -> FsResult<()> {
        let info = self.mount_table.get_mount_info_by_id(id)?;
        self.umount(&info.cv_path)
    }

    pub fn unprotected_umount_by_id(&self, id: u32) -> FsResult<()> {
        self.mount_table.unprotected_umount_by_id(id)
    }

    pub fn has_mounted(&self, id: u32) -> FsResult<bool> {
        self.mount_table.has_mounted(id)
    }

    /**
     * use ufs_uri to find mount entry
     */
    pub fn get_mount_info(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        self.mount_table.get_mount_info(path)
    }

    pub fn get_mount_table(&self) -> FsResult<Vec<MountInfo>> {
        let table = self.mount_table.get_mount_table()?;

        let mut entries = Vec::new();
        table.iter().for_each(|entry| {
            entries.push(entry.clone());
        });
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::MountManager;
    use curvine_model::{AccessMode, MountInfo, MountOptions, WriteType};

    fn mount_info(write_type: WriteType, access_mode: AccessMode, write_cache: bool) -> MountInfo {
        MountOptions::builder()
            .write_type(write_type)
            .access_mode(access_mode)
            .write_cache(write_cache)
            .build()
            .to_info(1, "/mnt", "file:///tmp/curvine-mount")
    }

    #[test]
    fn normalize_mount_config_rejects_write_cache_for_cache_mode_read_write() {
        let mut info = mount_info(WriteType::CacheMode, AccessMode::ReadWrite, true);

        let err = MountManager::normalize_mount_config(&mut info).unwrap_err();
        assert!(err.to_string().contains("write_cache is not supported"));
    }

    #[test]
    fn normalize_mount_config_rejects_write_cache_for_read_only_cache_mode() {
        let mut info = mount_info(WriteType::CacheMode, AccessMode::ReadOnly, true);

        let err = MountManager::normalize_mount_config(&mut info).unwrap_err();
        assert!(err.to_string().contains("write_cache is not supported"));
    }

    #[test]
    fn normalize_mount_config_rejects_write_cache_for_fs_mode() {
        let mut info = mount_info(WriteType::FsMode, AccessMode::ReadWrite, true);

        let err = MountManager::normalize_mount_config(&mut info).unwrap_err();
        assert!(err.to_string().contains("write_cache is not supported"));
    }

    #[test]
    fn normalize_mount_config_accepts_disabled_write_cache() {
        let mut info = mount_info(WriteType::CacheMode, AccessMode::ReadOnly, false);

        MountManager::normalize_mount_config(&mut info).unwrap();
    }
}
