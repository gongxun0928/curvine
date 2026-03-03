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

use std::sync::Mutex;

use curvine_common::state::{JobTaskProgress, JobTaskState, LoadTaskInfo};
use orpc::common::LocalTime;
use orpc::sync::StateCtl;

pub struct TaskContext {
    pub info: LoadTaskInfo,
    state: StateCtl,
    progress: Mutex<JobTaskProgress>,
}

impl TaskContext {
    pub fn new(info: LoadTaskInfo) -> Self {
        Self {
            info,
            state: StateCtl::new(JobTaskState::Pending.into()),
            progress: Mutex::new(JobTaskProgress::default()),
        }
    }

    pub fn get_state(&self) -> JobTaskState {
        self.state.state()
    }

    pub fn set_failed(&self, message: impl Into<String>) -> JobTaskProgress {
        let mut lock = self.progress.lock().unwrap();
        self.state.set_state(JobTaskState::Failed);
        lock.message = message.into();
        lock.update_time = LocalTime::mills() as i64;

        JobTaskProgress {
            state: self.get_state(),
            total_size: lock.total_size,
            loaded_size: lock.loaded_size,
            update_time: lock.update_time,
            message: lock.message.clone(),
        }
    }

    pub fn set_canceled(&self, message: impl Into<String>) -> JobTaskProgress {
        let mut lock = self.progress.lock().unwrap();
        self.state.set_state(JobTaskState::Canceled);
        lock.message = message.into();
        lock.update_time = LocalTime::mills() as i64;

        JobTaskProgress {
            state: self.get_state(),
            total_size: lock.total_size,
            loaded_size: lock.loaded_size,
            update_time: lock.update_time,
            message: lock.message.clone(),
        }
    }

    pub fn is_submit(&self) -> bool {
        self.get_state() <= JobTaskState::Loading
    }

    pub fn is_cancel(&self) -> bool {
        self.get_state() == JobTaskState::Canceled
    }

    pub fn update_state(&self, state: JobTaskState, message: impl Into<String>) {
        let mut lock = self.progress.lock().unwrap();
        self.state.set_state(state);
        lock.message = message.into();
        lock.update_time = LocalTime::mills() as i64;
    }

    pub fn update_progress(&self, loaded_size: i64, total_size: i64) -> JobTaskProgress {
        let mut lock = self.progress.lock().unwrap();
        let state = self.get_state();

        lock.loaded_size = loaded_size;
        lock.total_size = total_size;
        lock.update_time = LocalTime::mills() as i64;

        if !state.is_terminal() && loaded_size >= total_size {
            lock.message = "task completed successfully".into();
            self.state.set_state(JobTaskState::Completed);
        }

        JobTaskProgress {
            state: self.get_state(),
            total_size: lock.total_size,
            loaded_size: lock.loaded_size,
            update_time: lock.update_time,
            message: lock.message.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::state::{LoadJobInfo, MountInfo, StorageType, TtlAction, WorkerAddress};
    use std::sync::Arc;

    fn mock_task_context() -> Arc<TaskContext> {
        let task = LoadTaskInfo {
            job: LoadJobInfo {
                job_id: "job-1".to_string(),
                source_path: "cv:///src".to_string(),
                target_path: "s3://bucket/src".to_string(),
                block_size: 1024,
                replicas: 1,
                storage_type: StorageType::Disk,
                ttl_ms: 0,
                ttl_action: TtlAction::None,
                mount_info: MountInfo::default(),
                create_time: 1,
                overwrite: Some(true),
            },
            task_id: "task-1".to_string(),
            worker: WorkerAddress {
                worker_id: 1,
                hostname: "w1".to_string(),
                ip_addr: "127.0.0.1".to_string(),
                rpc_port: 10000,
                web_port: 10001,
            },
            source_path: "cv:///src/file1".to_string(),
            target_path: "s3://bucket/src/file1".to_string(),
            create_time: 1,
        };
        Arc::new(TaskContext::new(task))
    }

    #[test]
    fn canceled_task_should_not_turn_completed_after_progress_update() {
        let ctx = mock_task_context();
        let _ = ctx.set_canceled("cancel by test");
        let progress = ctx.update_progress(100, 100);

        assert_eq!(progress.state, JobTaskState::Canceled);
        assert_eq!(ctx.get_state(), JobTaskState::Canceled);
    }
}
