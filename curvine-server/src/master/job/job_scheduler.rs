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
use curvine_common::conf::ClusterConf;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    CancelJobRequest, CancelJobResponse, GetJobStatusRequest, GetJobStatusResponse,
    SubmitJobRequest, SubmitJobResponse, TaskReportRequest, TaskReportResponse,
};
use curvine_common::state::{JobStatus, JobTaskProgress, LoadJobCommand, LoadJobResult};
use curvine_common::utils::{ProtoUtils, SerdeUtils};
use curvine_common::FsResult;
use orpc::client::{ClientFactory, SyncClient};
use orpc::message::Builder;
use orpc::runtime::Runtime;
use prost::Message as PMessage;
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

pub struct RemoteJobScheduler {
    client: SyncClient,
}

impl RemoteJobScheduler {
    pub fn with_conf(rt: Arc<Runtime>, conf: &ClusterConf) -> FsResult<Self> {
        let factory = ClientFactory::with_rt(conf.client_rpc_conf(), rt);
        let client = factory.create_sync(&conf.scheduler_addr())?;
        Ok(Self { client })
    }

    fn proto_rpc<T, R>(&self, code: RpcCode, header: T) -> FsResult<R>
    where
        T: PMessage + Default,
        R: PMessage + Default,
    {
        let msg = Builder::new_rpc(code).proto_header(header).build();
        let rep = self.client.rpc_check(msg)?;
        Ok(rep.parse_header()?)
    }
}

impl JobScheduler for RemoteJobScheduler {
    fn submit_job(&self, command: LoadJobCommand) -> FsResult<LoadJobResult> {
        let req = SubmitJobRequest {
            job_type: curvine_common::state::JobTaskType::Load.into(),
            job_command: SerdeUtils::serialize(&command)?,
        };
        let rep: SubmitJobResponse = self.proto_rpc(RpcCode::SubmitJob, req)?;
        Ok(LoadJobResult {
            job_id: rep.job_id,
            target_path: rep.target_path,
        })
    }

    fn get_job_status(&self, job_id: &str) -> FsResult<JobStatus> {
        let req = GetJobStatusRequest {
            job_id: job_id.to_string(),
            verbose: false,
        };
        let rep: GetJobStatusResponse = self.proto_rpc(RpcCode::GetJobStatus, req)?;
        Ok(JobStatus {
            job_id: rep.job_id,
            state: curvine_common::state::JobTaskState::from(rep.state as i8),
            source_path: rep.source_path,
            target_path: rep.target_path,
            progress: ProtoUtils::work_progress_from_pb(rep.progress),
        })
    }

    fn cancel_job(&self, job_id: &str) -> FsResult<()> {
        let req = CancelJobRequest {
            job_id: job_id.to_string(),
        };
        let _: CancelJobResponse = self.proto_rpc(RpcCode::CancelJob, req)?;
        Ok(())
    }

    fn report_task(&self, job_id: &str, task_id: &str, progress: JobTaskProgress) -> FsResult<()> {
        let req = TaskReportRequest {
            job_id: job_id.to_string(),
            task_id: task_id.to_string(),
            report: ProtoUtils::work_progress_to_pb(progress),
        };
        let _: TaskReportResponse = self.proto_rpc(RpcCode::ReportTask, req)?;
        Ok(())
    }
}
