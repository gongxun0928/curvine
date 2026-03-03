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

use curvine_common::conf::ClientConf;
use curvine_common::state::{
    JobTaskProgress, JobTaskState, LoadJobCommand, LoadJobInfo, LoadTaskInfo, MountInfo,
    WorkerAddress,
};
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::{ByteUnit, FastHashMap, FastHashSet, LocalTime};
use orpc::err_box;
use orpc::sync::StateCtl;

#[derive(Debug, Clone)]
pub struct TaskDetail {
    pub task: LoadTaskInfo,
    pub progress: JobTaskProgress,
}

impl TaskDetail {
    pub fn new(task: LoadTaskInfo) -> Self {
        Self {
            task,
            progress: JobTaskProgress::default(),
        }
    }
}

#[derive(Clone)]
pub struct JobContext {
    pub info: LoadJobInfo,
    pub state: StateCtl,
    pub progress: JobTaskProgress,
    pub assigned_workers: FastHashSet<WorkerAddress>,
    pub tasks: FastHashMap<String, TaskDetail>,
}

impl JobContext {
    pub fn with_conf(
        job_conf: &LoadJobCommand,
        job_id: String,
        source_path: String,
        target_path: String,
        mnt: &MountInfo,
        client_conf: &ClientConf,
    ) -> Self {
        let replicas = job_conf
            .replicas
            .unwrap_or(mnt.replicas.unwrap_or(client_conf.replicas));

        let block_size = job_conf
            .block_size
            .unwrap_or(mnt.block_size.unwrap_or(client_conf.block_size));

        let storage_type = job_conf
            .storage_type
            .unwrap_or(mnt.storage_type.unwrap_or(client_conf.storage_type));

        let ttl_ms = job_conf.ttl_ms.unwrap_or(mnt.ttl_ms);

        let ttl_action = job_conf.ttl_action.unwrap_or(mnt.ttl_action);

        let job = LoadJobInfo {
            job_id,
            source_path,
            target_path,
            replicas,
            block_size,
            storage_type,
            ttl_ms,
            ttl_action,
            mount_info: mnt.clone(),
            create_time: LocalTime::mills() as i64,
            overwrite: job_conf.overwrite,
        };

        JobContext {
            info: job,
            state: StateCtl::new(JobTaskState::Pending.into()),
            progress: Default::default(),
            assigned_workers: Default::default(),
            tasks: Default::default(),
        }
    }

    pub fn add_task(&mut self, task: LoadTaskInfo) {
        self.update_state(
            JobTaskState::Dispatching,
            format!("Assigned to worker {}", task.worker),
        );
        self.assigned_workers.insert(task.worker.clone());
        self.tasks
            .insert(task.task_id.clone(), TaskDetail::new(task));
    }

    pub fn update_state(&mut self, state: JobTaskState, message: impl Into<String>) {
        self.state.set_state(state);
        self.progress.update_time = LocalTime::mills() as i64;
        self.progress.message = message.into();
    }

    pub fn update_progress(
        &mut self,
        task_id: impl AsRef<str>,
        progress: JobTaskProgress,
    ) -> FsResult<()> {
        let task_id = task_id.as_ref();
        let detail = if let Some(v) = self.tasks.get_mut(task_id) {
            v
        } else {
            return err_box!("Not fond task {}", task_id);
        };
        // set task progress
        detail.progress = progress;

        // check job status
        let mut total_size: i64 = 0;
        let mut loaded_size: i64 = 0;
        let mut complete: usize = 0;
        let mut canceled: usize = 0;
        let mut loading: usize = 0;
        let mut job_state: JobTaskState = self.state.state();
        let mut message = String::new();

        for detail in self.tasks.values() {
            total_size += detail.progress.total_size;
            loaded_size += detail.progress.loaded_size;
            match detail.progress.state {
                JobTaskState::Completed => complete += 1,
                JobTaskState::Canceled => canceled += 1,
                JobTaskState::Failed => {
                    job_state = JobTaskState::Failed;
                    message = format!(
                        "task {} failed: {}",
                        detail.task.task_id, detail.progress.message
                    )
                }
                JobTaskState::Pending | JobTaskState::Dispatching | JobTaskState::Loading => {
                    loading += 1
                }
                _ => {}
            }
        }

        if complete == self.tasks.len() {
            job_state = JobTaskState::Completed;
            message = "All subtasks completed".into();
            info!(
                "job {} all subtasks completed, tasks {}, len = {}, cost {} ms",
                self.info.job_id,
                self.tasks.len(),
                ByteUnit::byte_to_string(loaded_size as u64),
                LocalTime::mills() as i64 - self.info.create_time
            )
        } else if job_state == JobTaskState::Failed {
            warn!(
                "job {} execute failed, tasks {}, len = {}, cost {} ms, error {}",
                self.info.job_id,
                self.tasks.len(),
                ByteUnit::byte_to_string(loaded_size as u64),
                LocalTime::mills() as i64 - self.info.create_time,
                message
            )
        } else if canceled + complete == self.tasks.len() && !self.tasks.is_empty() {
            job_state = JobTaskState::Canceled;
            message = "All subtasks are canceled or completed under cancellation".into();
        } else if job_state == JobTaskState::Canceling {
            message = format!(
                "Canceling job, finished {}/{} tasks",
                complete + canceled,
                self.tasks.len()
            );
        } else if loading > 0 {
            job_state = JobTaskState::Loading;
            message = format!("Running {}/{} subtasks", loading, self.tasks.len());
        }

        self.update_state(job_state, message);
        self.progress.loaded_size = loaded_size;
        self.progress.total_size = total_size;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_job_context(task_count: usize) -> JobContext {
        let command = LoadJobCommand::builder("cv:///src")
            .target_path("s3://bucket/src")
            .build();

        let mut ctx = JobContext::with_conf(
            &command,
            "job-1".to_string(),
            "cv:///src".to_string(),
            "s3://bucket/src".to_string(),
            &MountInfo::default(),
            &ClientConf::default(),
        );

        for idx in 0..task_count {
            let task = LoadTaskInfo {
                job: ctx.info.clone(),
                task_id: format!("task-{}", idx),
                worker: WorkerAddress {
                    worker_id: idx as u32 + 1,
                    hostname: format!("worker-{}", idx),
                    ip_addr: "127.0.0.1".to_string(),
                    rpc_port: 10000 + idx as u32,
                    web_port: 11000 + idx as u32,
                },
                source_path: format!("cv:///src/file-{}", idx),
                target_path: format!("s3://bucket/src/file-{}", idx),
                create_time: 1,
            };
            ctx.add_task(task);
        }

        ctx
    }

    fn progress(state: JobTaskState, loaded_size: i64, total_size: i64) -> JobTaskProgress {
        JobTaskProgress {
            state,
            loaded_size,
            total_size,
            update_time: 1,
            message: String::new(),
        }
    }

    #[test]
    fn canceling_job_should_end_canceled_when_all_tasks_canceled() {
        let mut ctx = new_job_context(2);
        ctx.update_state(JobTaskState::Canceling, "user cancel");

        ctx.update_progress("task-0", progress(JobTaskState::Canceled, 1, 10))
            .unwrap();
        ctx.update_progress("task-1", progress(JobTaskState::Canceled, 1, 10))
            .unwrap();

        assert_eq!(ctx.state.state::<JobTaskState>(), JobTaskState::Canceled);
    }

    #[test]
    fn canceling_job_should_end_completed_when_all_tasks_completed() {
        let mut ctx = new_job_context(2);
        ctx.update_state(JobTaskState::Canceling, "user cancel");

        ctx.update_progress("task-0", progress(JobTaskState::Completed, 10, 10))
            .unwrap();
        ctx.update_progress("task-1", progress(JobTaskState::Completed, 10, 10))
            .unwrap();

        assert_eq!(ctx.state.state::<JobTaskState>(), JobTaskState::Completed);
    }

    #[test]
    fn failed_task_should_win_over_canceling_state() {
        let mut ctx = new_job_context(2);
        ctx.update_state(JobTaskState::Canceling, "user cancel");

        ctx.update_progress(
            "task-0",
            JobTaskProgress {
                state: JobTaskState::Failed,
                loaded_size: 0,
                total_size: 10,
                update_time: 1,
                message: "boom".to_string(),
            },
        )
        .unwrap();

        assert_eq!(ctx.state.state::<JobTaskState>(), JobTaskState::Failed);
    }

    #[test]
    fn still_running_tasks_should_keep_loading_state() {
        let mut ctx = new_job_context(2);

        ctx.update_progress("task-0", progress(JobTaskState::Loading, 3, 10))
            .unwrap();
        ctx.update_progress("task-1", progress(JobTaskState::Pending, 0, 10))
            .unwrap();

        assert_eq!(ctx.state.state::<JobTaskState>(), JobTaskState::Loading);
    }
}
