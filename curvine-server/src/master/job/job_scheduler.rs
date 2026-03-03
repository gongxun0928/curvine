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

use crate::master::JobManager;
use curvine_common::state::{JobStatus, JobTaskProgress, LoadJobCommand, LoadJobResult};
use curvine_common::FsResult;
use std::sync::Arc;

pub type SyncJobScheduler = Arc<dyn JobScheduler>;

/// Job control-plane boundary.
///
/// Master RPC handlers should depend on this trait instead of `JobManager`,
/// so that in-process and out-of-process scheduler implementations can be
/// swapped without touching external RPC compatibility.
pub trait JobScheduler: Send + Sync {
    fn submit_job(&self, command: LoadJobCommand) -> FsResult<LoadJobResult>;

    fn get_job_status(&self, job_id: &str) -> FsResult<JobStatus>;

    fn cancel_job(&self, job_id: &str) -> FsResult<()>;

    fn report_task(&self, job_id: &str, task_id: &str, progress: JobTaskProgress) -> FsResult<()>;
}

#[derive(Clone)]
pub struct InProcessJobScheduler {
    job_manager: Arc<JobManager>,
}

impl InProcessJobScheduler {
    pub fn new(job_manager: Arc<JobManager>) -> Self {
        Self { job_manager }
    }
}

impl JobScheduler for InProcessJobScheduler {
    fn submit_job(&self, command: LoadJobCommand) -> FsResult<LoadJobResult> {
        self.job_manager.submit_load_job(command)
    }

    fn get_job_status(&self, job_id: &str) -> FsResult<JobStatus> {
        self.job_manager.get_job_status(job_id)
    }

    fn cancel_job(&self, job_id: &str) -> FsResult<()> {
        self.job_manager.cancel_job(job_id)
    }

    fn report_task(&self, job_id: &str, task_id: &str, progress: JobTaskProgress) -> FsResult<()> {
        self.job_manager.update_progress(job_id, task_id, progress)
    }
}
