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

use crate::master::{JobContext, TaskDetail};
use curvine_common::error::FsError;
use curvine_common::state::{
    JobTaskProgress, JobTaskState, LoadJobInfo, LoadTaskInfo, WorkerAddress,
};
use curvine_common::FsResult;
use orpc::common::{FastHashMap, FastHashSet};
use orpc::sync::StateCtl;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskSnapshot {
    task: LoadTaskInfo,
    progress: JobTaskProgress,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobSnapshot {
    info: LoadJobInfo,
    state: JobTaskState,
    progress: JobTaskProgress,
    assigned_workers: Vec<WorkerAddress>,
    tasks: Vec<TaskSnapshot>,
}

impl JobSnapshot {
    fn from_context(ctx: &JobContext) -> Self {
        let assigned_workers = ctx.assigned_workers.iter().map(|v| v.clone()).collect();
        let tasks = ctx
            .tasks
            .iter()
            .map(|(_, detail)| TaskSnapshot {
                task: detail.task.clone(),
                progress: detail.progress.clone(),
            })
            .collect();

        Self {
            info: ctx.info.clone(),
            state: ctx.state.state(),
            progress: ctx.progress.clone(),
            assigned_workers,
            tasks,
        }
    }

    fn into_context(self) -> JobContext {
        let mut assigned_workers = FastHashSet::default();
        for worker in self.assigned_workers {
            assigned_workers.insert(worker);
        }

        let mut tasks = FastHashMap::default();
        for task in self.tasks {
            tasks.insert(
                task.task.task_id.clone(),
                TaskDetail {
                    task: task.task,
                    progress: task.progress,
                },
            );
        }

        let mut progress = self.progress;
        progress.state = self.state;

        JobContext {
            info: self.info,
            state: StateCtl::new(self.state.into()),
            progress,
            assigned_workers,
            tasks,
        }
    }
}

#[derive(Clone)]
pub struct JobPersistence {
    path: PathBuf,
}

impl JobPersistence {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> FsResult<Vec<JobContext>> {
        if !self.path.exists() {
            return Ok(vec![]);
        }

        let content = fs::read_to_string(&self.path).map_err(|e| {
            FsError::common(format!(
                "Read job snapshot {} failed: {}",
                self.path.display(),
                e
            ))
        })?;

        if content.trim().is_empty() {
            return Ok(vec![]);
        }

        let snapshots = serde_json::from_str::<Vec<JobSnapshot>>(&content).map_err(|e| {
            FsError::common(format!(
                "Parse job snapshot {} failed: {}",
                self.path.display(),
                e
            ))
        })?;

        Ok(snapshots
            .into_iter()
            .map(JobSnapshot::into_context)
            .collect())
    }

    pub fn save(&self, jobs: &[JobContext]) -> FsResult<()> {
        self.ensure_parent_dir()?;

        let snapshots: Vec<JobSnapshot> = jobs.iter().map(JobSnapshot::from_context).collect();
        let data = serde_json::to_vec_pretty(&snapshots).map_err(|e| {
            FsError::common(format!(
                "Encode job snapshot {} failed: {}",
                self.path.display(),
                e
            ))
        })?;

        let tmp_path = self.path.with_extension("tmp");
        fs::write(&tmp_path, &data).map_err(|e| {
            FsError::common(format!(
                "Write temp job snapshot {} failed: {}",
                tmp_path.display(),
                e
            ))
        })?;
        fs::rename(&tmp_path, &self.path).map_err(|e| {
            FsError::common(format!(
                "Rename job snapshot {} -> {} failed: {}",
                tmp_path.display(),
                self.path.display(),
                e
            ))
        })?;

        Ok(())
    }

    fn ensure_parent_dir(&self) -> FsResult<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                FsError::common(format!(
                    "Create job snapshot dir {} failed: {}",
                    parent.display(),
                    e
                ))
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::conf::ClientConf;
    use curvine_common::state::{LoadJobCommand, LoadTaskInfo, MountInfo, WorkerAddress};
    use orpc::common::LocalTime;

    fn sample_job() -> JobContext {
        let command = LoadJobCommand::builder("cv:///src")
            .target_path("s3://bucket/src")
            .build();
        let mut job = JobContext::with_conf(
            &command,
            "job-persist-1".to_string(),
            "cv:///src".to_string(),
            "s3://bucket/src".to_string(),
            &MountInfo::default(),
            &ClientConf::default(),
        );

        let task = LoadTaskInfo {
            job: job.info.clone(),
            task_id: "task-persist-1".to_string(),
            worker: WorkerAddress {
                worker_id: 1,
                hostname: "worker-1".to_string(),
                ip_addr: "127.0.0.1".to_string(),
                rpc_port: 9000,
                web_port: 9001,
            },
            source_path: "cv:///src/file1".to_string(),
            target_path: "s3://bucket/src/file1".to_string(),
            create_time: 1,
        };
        job.add_task(task);
        job.update_state(JobTaskState::Loading, "running");
        job
    }

    #[test]
    fn should_roundtrip_job_snapshot() {
        let filename = format!("curvine-job-persistence-{}.json", LocalTime::mills());
        let path = std::env::temp_dir().join(filename);
        let persistence = JobPersistence::new(path.clone());
        let job = sample_job();

        persistence.save(std::slice::from_ref(&job)).unwrap();
        let loaded = persistence.load().unwrap();
        fs::remove_file(path).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].info.job_id, job.info.job_id);
        assert_eq!(loaded[0].tasks.len(), 1);
        assert_eq!(
            loaded[0].state.state::<JobTaskState>(),
            JobTaskState::Loading
        );
    }
}
