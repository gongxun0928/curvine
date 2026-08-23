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

use crate::state::{MountInfo, StorageType, TtlAction, WorkerAddress};
use curvine_runtime::common::ByteUnit;
use num_enum::{FromPrimitive, IntoPrimitive};
use serde::{Deserialize, Serialize};

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    IntoPrimitive,
    FromPrimitive,
    Serialize,
    Deserialize,
    Default,
)]
#[repr(i8)]
pub enum JobTaskState {
    #[default]
    UNKNOWN = 0,
    Pending = 1,
    Loading = 2,
    Completed = 3,
    Failed = 4,
    Canceled = 5,
    /// Some sub-tasks succeeded and some failed (transfer load jobs).
    PartialSuccess = 6,
}

impl JobTaskState {
    pub fn is_finish(&self) -> bool {
        matches!(
            self,
            JobTaskState::Completed
                | JobTaskState::Failed
                | JobTaskState::Canceled
                | JobTaskState::PartialSuccess
        )
    }

    pub fn is_running(&self) -> bool {
        matches!(self, JobTaskState::Pending | JobTaskState::Loading)
    }
}

/// Outcome of a load submit or status query: `job_id` / `target_path` identify the
/// job; `state` is the current phase.
///
/// **Callers must not assume this struct always reflects the latest
/// [`LoadJobCommand`]:** concurrent submits for the same path are defined by the
/// server as **first submitter wins**; a later `Ok` may describe the in-flight job
/// only (paths and state), not the superseded request’s options.
#[derive(Debug, Clone)]
pub struct LoadJobResult {
    pub job_id: String,
    pub target_path: String,
    pub state: JobTaskState,
}

impl LoadJobResult {
    pub fn with_job(job: &LoadJobInfo) -> Self {
        Self {
            job_id: job.job_id.to_owned(),
            target_path: job.target_path.to_owned(),
            state: JobTaskState::Pending,
        }
    }

    pub fn with_state(job: &LoadJobInfo, state: JobTaskState) -> Self {
        Self {
            job_id: job.job_id.to_owned(),
            target_path: job.target_path.to_owned(),
            state,
        }
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    IntoPrimitive,
    FromPrimitive,
    Serialize,
    Deserialize,
)]
#[repr(i32)]
pub enum JobTaskType {
    #[num_enum(default)]
    Load = 1,
    Export = 2,
    /// Phase 3: cache-mode load — CacheAllocate → write planned workers →
    /// CacheCommit against the object index (no inode involvement).
    CacheLoad = 3,
}

impl JobTaskType {
    /// Wire discriminator decode for handlers routing `SubmitTaskRequest`
    /// (callers outside this crate cannot reach `num_enum`'s trait).
    /// Unknown discriminators decode to the `#[num_enum(default)]` Load.
    pub fn from_i32(v: i32) -> Self {
        num_enum::FromPrimitive::from_primitive(v)
    }
}

#[derive(Default)]
pub struct JobStatus {
    pub job_id: String,
    pub state: JobTaskState,
    pub source_path: String,
    pub target_path: String,
    pub progress: JobTaskProgress,
}

impl JobStatus {
    /// Returns a formatted progress string with percentage and byte counts
    pub fn progress_string(&self, show_bar: bool) -> String {
        self.progress.progress_string(show_bar)
    }
}

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct LoadJobCommand {
    pub source_path: String,
    pub target_path: Option<String>,
    pub replicas: Option<i32>,
    pub block_size: Option<i64>,
    pub storage_type: Option<StorageType>,
    pub ttl_ms: Option<i64>,
    pub ttl_action: Option<TtlAction>,
    pub overwrite: Option<bool>,
}

impl LoadJobCommand {
    pub fn builder(source_path: impl Into<String>) -> LoadJobCommandBuilder {
        LoadJobCommandBuilder::new(source_path).overwrite(true)
    }
}

#[derive(Default)]
pub struct LoadJobCommandBuilder {
    source_path: String,
    target_path: Option<String>,
    replicas: Option<i32>,
    block_size: Option<i64>,
    storage_type: Option<StorageType>,
    ttl_ms: Option<i64>,
    ttl_action: Option<TtlAction>,
    overwrite: Option<bool>,
}

impl LoadJobCommandBuilder {
    pub fn new(source_path: impl Into<String>) -> Self {
        Self {
            source_path: source_path.into(),
            ..Default::default()
        }
    }

    pub fn target_path(mut self, target_path: impl Into<String>) -> Self {
        let _ = self.target_path.insert(target_path.into());
        self
    }

    pub fn replicas(mut self, replicas: i32) -> Self {
        let _ = self.replicas.insert(replicas);
        self
    }

    pub fn block_size(mut self, block_size: i64) -> Self {
        let _ = self.block_size.insert(block_size);
        self
    }

    pub fn storage_type(mut self, storage_type: StorageType) -> Self {
        let _ = self.storage_type.insert(storage_type);
        self
    }

    pub fn ttl_ms(mut self, ttl_ms: i64) -> Self {
        let _ = self.ttl_ms.insert(ttl_ms);
        self
    }

    pub fn ttl_action(mut self, ttl_action: TtlAction) -> Self {
        let _ = self.ttl_action.insert(ttl_action);
        self
    }

    pub fn overwrite(mut self, overwrite: bool) -> Self {
        let _ = self.overwrite.insert(overwrite);
        self
    }

    pub fn build(self) -> LoadJobCommand {
        LoadJobCommand {
            source_path: self.source_path,
            target_path: self.target_path,
            replicas: self.replicas,
            block_size: self.block_size,
            storage_type: self.storage_type,
            ttl_ms: self.ttl_ms,
            ttl_action: self.ttl_action,
            overwrite: self.overwrite,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadJobInfo {
    pub job_id: String,
    pub source_path: String,
    pub target_path: String,
    pub block_size: i64,
    pub replicas: i32,
    pub storage_type: StorageType,
    pub ttl_ms: i64,
    pub ttl_action: TtlAction,
    pub mount_info: MountInfo,
    pub create_time: i64,
    pub overwrite: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadTaskInfo {
    pub job: LoadJobInfo,
    pub task_id: String,
    pub worker: WorkerAddress,
    pub source_path: String,
    pub target_path: String,
    pub create_time: i64,
    #[serde(default)]
    pub source_read_plan_json: String,
    #[serde(default)]
    pub transfer_report: Option<TransferTaskReportInfo>,
    /// Phase 3 (dual-mode metadata split): when present, this task is a
    /// CACHE-mode load — the worker executes the CacheAllocate → write
    /// planned workers → CacheCommit chain against the object index and
    /// must NOT touch the inode tree (no create/rename/set_attr). The
    /// incumbent fields above keep their meaning for job bookkeeping and
    /// progress reporting (`source_path` = UFS path, `target_path` =
    /// display-only cache identity). `#[serde(default)]` keeps the wire
    /// compatible with in-flight fs-mode tasks across a rolling upgrade.
    #[serde(default)]
    pub cache: Option<CacheLoadSpec>,
}

/// Retry-stable op identity for the cache RPCs issued by one cache-mode
/// load task. The master mints BOTH tokens once at task creation and
/// serializes them into the task; a worker retry or RPC response loss
/// replays the exact same tokens, never re-deriving them from a
/// transient rpc req_id (gpt56 `f7788b98` point 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheOpTokenId {
    pub client_id: u64,
    pub op_seq: u64,
}

/// Master-injected cache-domain identity for one cache-mode load task.
///
/// `incarnation` is the AUTHORITATIVE current incarnation of the mount at
/// task-creation time — the worker never derives it from the mount id and
/// never self-issues one (gpt56 `f7788b98` point 4: provenance is the
/// master's alone; a mount without an installed incarnation fails closed
/// at routing time).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheLoadSpec {
    pub incarnation: u64,
    /// Object key in the cache index (mount-relative UFS key).
    pub key: String,
    /// The load identity token handed to CacheAllocate (and echoed by
    /// CacheCommit as `load_token`).
    pub load_token: CacheOpTokenId,
    /// The independent commit op token for CacheCommit idempotency.
    pub commit_token: CacheOpTokenId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferTaskReportInfo {
    pub run_id: u64,
    pub attempt_id: u64,
    pub worker_id: u32,
    pub worker_session_id: String,
    #[serde(default)]
    pub report_target: String,
    #[serde(default)]
    pub report_endpoints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobTaskProgress {
    pub state: JobTaskState,
    pub loaded_size: i64,
    pub total_size: i64,
    pub update_time: i64,
    pub message: String,
}

impl Default for JobTaskProgress {
    fn default() -> Self {
        Self {
            state: JobTaskState::Pending,
            total_size: 0,
            loaded_size: 0,
            update_time: 0,
            message: String::new(),
        }
    }
}

impl JobTaskProgress {
    /// Returns a formatted progress string with percentage and byte counts
    /// Format: "[████████░░░░░░░░░░] 45.2% (123.4 MB / 273.0 MB)"
    /// If show_bar is false, format: "45.2% (123.4 MB / 273.0 MB)"
    pub fn progress_string(&self, show_bar: bool) -> String {
        let loaded = self.loaded_size.max(0) as u64;
        let total = self.total_size.max(0) as u64;
        let display_loaded = if total == 0 {
            loaded
        } else {
            loaded.min(total)
        };

        let percentage = if total == 0 {
            0.0
        } else {
            display_loaded as f64 / total as f64 * 100.0
        };

        if show_bar {
            if total == 0 {
                return format!(
                    "[{}] 0.0% ({} / {})",
                    "░".repeat(20),
                    ByteUnit::byte_to_string(display_loaded),
                    ByteUnit::byte_to_string(total)
                );
            }

            let filled = (percentage / 100.0 * 20.0) as usize;
            let empty = 20 - filled.min(20);

            let progress_bar = format!("{}{}", "█".repeat(filled.min(20)), "░".repeat(empty));

            format!(
                "[{}] {:.1}% ({} / {})",
                progress_bar,
                percentage,
                ByteUnit::byte_to_string(display_loaded),
                ByteUnit::byte_to_string(total)
            )
        } else {
            format!(
                "{:.1}% ({} / {})",
                percentage,
                ByteUnit::byte_to_string(display_loaded),
                ByteUnit::byte_to_string(total)
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JobTaskProgress;
    use curvine_runtime::common::ByteUnit;
    #[test]
    fn progress_string_caps_loaded_bytes_at_total() {
        let progress = JobTaskProgress {
            loaded_size: 66,
            total_size: 33,
            ..Default::default()
        };
        let expected_counts = format!(
            "{} / {}",
            ByteUnit::byte_to_string(33),
            ByteUnit::byte_to_string(33)
        );

        assert!(progress.progress_string(false).contains(&expected_counts));
        assert!(progress.progress_string(true).contains(&expected_counts));
    }

    // Phase 3: `LoadTaskInfo.cache` must stay wire compatible with
    // in-flight fs-mode tasks across a rolling upgrade — an old payload
    // (no `cache` field) decodes with `cache: None`, and a cache-mode
    // payload round-trips its spec (incarnation + dual op tokens)
    // losslessly.
    #[test]
    fn load_task_info_cache_field_is_wire_compatible() {
        let task = super::LoadTaskInfo {
            job: super::LoadJobInfo {
                job_id: "j1".to_string(),
                source_path: "s3://b/k".to_string(),
                target_path: "cv:/m/k".to_string(),
                block_size: 1,
                replicas: 1,
                storage_type: Default::default(),
                ttl_ms: 0,
                ttl_action: Default::default(),
                mount_info: Default::default(),
                create_time: 0,
                overwrite: None,
            },
            task_id: "t1".to_string(),
            worker: Default::default(),
            source_path: "s3://b/k".to_string(),
            target_path: "cv:/m/k".to_string(),
            create_time: 1,
            source_read_plan_json: String::new(),
            transfer_report: None,
            cache: None,
        };

        // Old (pre-Phase-3) payload: the `cache` key is absent entirely.
        let mut value = serde_json::to_value(&task).unwrap();
        value.as_object_mut().unwrap().remove("cache");
        let decoded: super::LoadTaskInfo = serde_json::from_value(value).unwrap();
        assert!(decoded.cache.is_none());

        let spec = super::CacheLoadSpec {
            incarnation: 42,
            key: "dir/file".to_string(),
            load_token: super::CacheOpTokenId {
                client_id: 7,
                op_seq: 8,
            },
            commit_token: super::CacheOpTokenId {
                client_id: 7,
                op_seq: 9,
            },
        };
        let with_cache = super::LoadTaskInfo {
            cache: Some(spec.clone()),
            ..decoded
        };
        let round: super::LoadTaskInfo =
            serde_json::from_value(serde_json::to_value(&with_cache).unwrap()).unwrap();
        assert_eq!(round.cache.as_ref().unwrap(), &spec);
    }
}
