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
use num_enum::{FromPrimitive, IntoPrimitive};
use orpc::common::ByteUnit;
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
}

impl JobTaskState {
    pub fn is_finish(&self) -> bool {
        matches!(
            self,
            JobTaskState::Completed | JobTaskState::Failed | JobTaskState::Canceled
        )
    }
}

/// Job-level state machine for the new Scheduler architecture.
///
/// Allowed transitions:
///   Pending    -> Dispatching | Failed | Canceled
///   Dispatching -> Dispatching | Running | Failed | Canceled | Canceling
///   Running    -> Completed | Failed | Canceling
///   Canceling  -> Canceled | CancelFailed | Completed
///
/// Terminal states: Completed, Failed, Canceled, CancelFailed
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    IntoPrimitive,
    FromPrimitive,
    Serialize,
    Deserialize,
    Default,
)]
#[repr(i8)]
pub enum JobState {
    #[default]
    Pending = 0,
    Dispatching = 1,
    Running = 2,
    Completed = 3,
    Failed = 4,
    Canceling = 5,
    Canceled = 6,
    CancelFailed = 7,
}

impl JobState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobState::Completed | JobState::Failed | JobState::Canceled | JobState::CancelFailed
        )
    }

    pub fn can_transition_to(&self, target: JobState) -> bool {
        match self {
            JobState::Pending => matches!(
                target,
                JobState::Dispatching | JobState::Failed | JobState::Canceled
            ),
            JobState::Dispatching => matches!(
                target,
                JobState::Dispatching
                    | JobState::Running
                    | JobState::Failed
                    | JobState::Canceled
                    | JobState::Canceling
            ),
            JobState::Running => matches!(
                target,
                JobState::Completed | JobState::Failed | JobState::Canceling
            ),
            JobState::Canceling => matches!(
                target,
                JobState::Canceled | JobState::CancelFailed | JobState::Completed
            ),
            // Terminal states: no further transitions
            JobState::Completed | JobState::Failed | JobState::Canceled | JobState::CancelFailed => {
                false
            }
        }
    }
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

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

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
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

/// Persisted job metadata for the Scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMeta {
    pub job_id: String,
    pub job_type: JobTaskType,
    pub client_request_id: String,
    pub state: JobState,
    pub epoch: u64,
    pub attempt: u32,
    pub source_path: String,
    pub target_path: String,
    pub assigned_worker: Option<WorkerAddress>,
    pub create_time: i64,
    pub update_time: i64,
    pub message: String,
    pub load_job_command: Option<LoadJobCommand>,
    pub progress: JobTaskProgress,
}

impl JobMeta {
    pub fn new(
        job_id: String,
        job_type: JobTaskType,
        client_request_id: String,
        source_path: String,
        target_path: String,
        command: LoadJobCommand,
    ) -> Self {
        let now = orpc::common::LocalTime::mills() as i64;
        Self {
            job_id,
            job_type,
            client_request_id,
            state: JobState::Pending,
            epoch: 1,
            attempt: 0,
            source_path,
            target_path,
            assigned_worker: None,
            create_time: now,
            update_time: now,
            message: String::new(),
            load_job_command: Some(command),
            progress: JobTaskProgress::default(),
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    pub fn try_transition(&mut self, target: JobState, message: impl Into<String>) -> bool {
        if self.state.can_transition_to(target) {
            self.state = target;
            self.update_time = orpc::common::LocalTime::mills() as i64;
            self.message = message.into();
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadTaskInfo {
    pub job: LoadJobInfo,
    pub task_id: String,
    pub worker: WorkerAddress,
    pub source_path: String,
    pub target_path: String,
    pub create_time: i64,
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

        let percentage = if total == 0 {
            0.0
        } else {
            (loaded as f64 / total as f64 * 100.0).min(100.0)
        };

        if show_bar {
            if total == 0 {
                return format!(
                    "[{}] 0.0% ({} / {})",
                    "░".repeat(20),
                    ByteUnit::byte_to_string(loaded),
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
                ByteUnit::byte_to_string(loaded),
                ByteUnit::byte_to_string(total)
            )
        } else {
            format!(
                "{:.1}% ({} / {})",
                percentage,
                ByteUnit::byte_to_string(loaded),
                ByteUnit::byte_to_string(total)
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_state_terminal() {
        assert!(!JobState::Pending.is_terminal());
        assert!(!JobState::Dispatching.is_terminal());
        assert!(!JobState::Running.is_terminal());
        assert!(!JobState::Canceling.is_terminal());

        assert!(JobState::Completed.is_terminal());
        assert!(JobState::Failed.is_terminal());
        assert!(JobState::Canceled.is_terminal());
        assert!(JobState::CancelFailed.is_terminal());
    }

    #[test]
    fn test_pending_transitions() {
        let s = JobState::Pending;
        assert!(s.can_transition_to(JobState::Dispatching));
        assert!(s.can_transition_to(JobState::Failed));
        assert!(s.can_transition_to(JobState::Canceled));
        assert!(!s.can_transition_to(JobState::Running));
        assert!(!s.can_transition_to(JobState::Completed));
        assert!(!s.can_transition_to(JobState::Canceling));
        assert!(!s.can_transition_to(JobState::CancelFailed));
    }

    #[test]
    fn test_dispatching_transitions() {
        let s = JobState::Dispatching;
        assert!(s.can_transition_to(JobState::Dispatching));
        assert!(s.can_transition_to(JobState::Running));
        assert!(s.can_transition_to(JobState::Failed));
        assert!(s.can_transition_to(JobState::Canceled));
        assert!(s.can_transition_to(JobState::Canceling));
        assert!(!s.can_transition_to(JobState::Completed));
        assert!(!s.can_transition_to(JobState::CancelFailed));
    }

    #[test]
    fn test_running_transitions() {
        let s = JobState::Running;
        assert!(s.can_transition_to(JobState::Completed));
        assert!(s.can_transition_to(JobState::Failed));
        assert!(s.can_transition_to(JobState::Canceling));
        assert!(!s.can_transition_to(JobState::Pending));
        assert!(!s.can_transition_to(JobState::Dispatching));
        assert!(!s.can_transition_to(JobState::Canceled));
        assert!(!s.can_transition_to(JobState::CancelFailed));
    }

    #[test]
    fn test_canceling_transitions() {
        let s = JobState::Canceling;
        assert!(s.can_transition_to(JobState::Canceled));
        assert!(s.can_transition_to(JobState::CancelFailed));
        assert!(s.can_transition_to(JobState::Completed));
        assert!(!s.can_transition_to(JobState::Pending));
        assert!(!s.can_transition_to(JobState::Dispatching));
        assert!(!s.can_transition_to(JobState::Running));
        assert!(!s.can_transition_to(JobState::Failed));
    }

    #[test]
    fn test_terminal_states_no_transitions() {
        for state in [
            JobState::Completed,
            JobState::Failed,
            JobState::Canceled,
            JobState::CancelFailed,
        ] {
            for target in [
                JobState::Pending,
                JobState::Dispatching,
                JobState::Running,
                JobState::Completed,
                JobState::Failed,
                JobState::Canceling,
                JobState::Canceled,
                JobState::CancelFailed,
            ] {
                assert!(
                    !state.can_transition_to(target),
                    "{:?} should not transition to {:?}",
                    state,
                    target
                );
            }
        }
    }

    #[test]
    fn test_job_meta_try_transition() {
        let cmd = LoadJobCommand::default();
        let mut meta = JobMeta::new(
            "test-job-1".to_string(),
            JobTaskType::Load,
            "req-1".to_string(),
            "s3://bucket/path".to_string(),
            "/cv/path".to_string(),
            cmd,
        );

        assert_eq!(meta.state, JobState::Pending);

        // Valid transition
        assert!(meta.try_transition(JobState::Dispatching, "dispatching"));
        assert_eq!(meta.state, JobState::Dispatching);

        // Valid: Dispatching -> Running
        assert!(meta.try_transition(JobState::Running, "running"));
        assert_eq!(meta.state, JobState::Running);

        // Valid: Running -> Completed
        assert!(meta.try_transition(JobState::Completed, "done"));
        assert_eq!(meta.state, JobState::Completed);

        // Invalid: terminal state -> anything
        assert!(!meta.try_transition(JobState::Pending, "reset"));
        assert_eq!(meta.state, JobState::Completed);
    }

    #[test]
    fn test_cancel_conflict_completed_wins() {
        let cmd = LoadJobCommand::default();
        let mut meta = JobMeta::new(
            "test-job-2".to_string(),
            JobTaskType::Load,
            "req-2".to_string(),
            "s3://bucket/path".to_string(),
            "/cv/path".to_string(),
            cmd,
        );

        meta.try_transition(JobState::Dispatching, "");
        meta.try_transition(JobState::Running, "");
        meta.try_transition(JobState::Canceling, "user cancel");

        // Completed arrives while Canceling -> Completed wins
        assert!(meta.try_transition(JobState::Completed, "completed"));
        assert_eq!(meta.state, JobState::Completed);
    }
}
