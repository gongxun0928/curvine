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

use crate::scheduler::scheduler_state::SchedulerState;
use crate::scheduler::scheduler_worker_client::SchedulerClientFactory;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::proto::*;
use curvine_common::state::{JobMeta, JobState, JobTaskState, JobTaskType, LoadJobCommand};
use curvine_common::utils::{ProtoUtils, SerdeUtils};
use curvine_common::FsResult;
use log::info;
use orpc::err_box;
use orpc::handler::{FrameBuf, MessageHandler};
use orpc::message::{Builder, Message};
use std::sync::Arc;
use uuid::Uuid;

/// RPC handler for the Scheduler process.
///
/// Handles both external client requests (SubmitJob, GetJobStatus, CancelJob)
/// and internal worker reports (ReportJobEvent).
pub struct SchedulerHandler {
    state: Arc<SchedulerState>,
    _client_factory: Arc<SchedulerClientFactory>,
    _buf: FrameBuf,
}

impl SchedulerHandler {
    pub fn new(
        state: Arc<SchedulerState>,
        client_factory: Arc<SchedulerClientFactory>,
        buf_size: usize,
    ) -> Self {
        Self {
            state,
            _client_factory: client_factory,
            _buf: FrameBuf::new(buf_size),
        }
    }

    fn submit_job(&mut self, msg: &Message) -> FsResult<Message> {
        let req: SubmitJobRequest = msg.parse_header()?;
        let command: LoadJobCommand = SerdeUtils::deserialize(&req.job_command)?;

        if command.source_path.is_empty() {
            return err_box!("source path cannot be empty");
        }

        let job_id = Uuid::new_v4().to_string();
        let target_path = command
            .target_path
            .clone()
            .unwrap_or_else(|| command.source_path.clone());

        let meta = JobMeta::new(
            job_id.clone(),
            JobTaskType::Load,
            String::new(),
            command.source_path.clone(),
            target_path.clone(),
            command,
        );

        self.state.create_job(meta)?;

        info!("job {} created: source_path={}", job_id, req.job_command.len());

        let response = SubmitJobResponse {
            job_id,
            target_path,
            state: JobTaskState::Pending as i32,
        };

        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn get_job_status(&mut self, msg: &Message) -> FsResult<Message> {
        let req: GetJobStatusRequest = msg.parse_header()?;

        let meta = self.state.get_job(&req.job_id);

        match meta {
            Some(meta) => {
                let state = job_state_to_task_state(meta.state);
                let response = GetJobStatusResponse {
                    job_id: meta.job_id,
                    state: state as i32,
                    source_path: meta.source_path,
                    target_path: meta.target_path,
                    progress: ProtoUtils::work_progress_to_pb(meta.progress),
                };
                Ok(Builder::success(msg).proto_header(response).build())
            }
            None => {
                // Try loading from persistent store for terminal jobs
                err_box!("job {} not found", req.job_id)
            }
        }
    }

    fn cancel_job(&mut self, msg: &Message) -> FsResult<Message> {
        let req: CancelJobRequest = msg.parse_header()?;
        let job_id = &req.job_id;

        let meta = self.state.get_job(job_id);
        match meta {
            Some(meta) => {
                if meta.state.is_terminal() {
                    info!("job {} already in terminal state {:?}", job_id, meta.state);
                    return Ok(Builder::success(msg)
                        .proto_header(CancelJobResponse {})
                        .build());
                }

                // Transition to Canceling (or directly Canceled if still Pending)
                if meta.state == JobState::Pending {
                    self.state.transition(job_id, JobState::Canceled, "Canceled by user before dispatch")?;
                } else {
                    self.state.transition(job_id, JobState::Canceling, "Cancel requested by user")?;
                }

                info!("job {} cancel initiated", job_id);

                Ok(Builder::success(msg)
                    .proto_header(CancelJobResponse {})
                    .build())
            }
            None => err_box!("job {} not found", job_id),
        }
    }

    fn report_job_event(&mut self, msg: &Message) -> FsResult<Message> {
        let req: ReportJobEventRequest = msg.parse_header()?;

        let reported_state = proto_job_state_to_job_state(req.state);
        let progress = ProtoUtils::work_progress_from_pb(req.progress);

        self.state.process_worker_event(
            &req.job_id,
            req.epoch,
            req.attempt,
            reported_state,
            progress,
            req.message,
        )?;

        Ok(Builder::success(msg)
            .proto_header(ReportJobEventResponse {})
            .build())
    }
}

impl MessageHandler for SchedulerHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let code = RpcCode::from(msg.code());
        let res = match code {
            RpcCode::SubmitJob => self.submit_job(msg),
            RpcCode::GetJobStatus => self.get_job_status(msg),
            RpcCode::CancelJob => self.cancel_job(msg),
            RpcCode::ReportJobEvent => self.report_job_event(msg),
            _ => err_box!("unsupported operation {:?}", code),
        };

        match res {
            Ok(v) => Ok(v),
            Err(e) => Ok(msg.error_ext(&e)),
        }
    }
}

/// Maps the new JobState to the legacy JobTaskState for backward-compatible client responses.
fn job_state_to_task_state(state: JobState) -> JobTaskState {
    match state {
        JobState::Pending | JobState::Dispatching => JobTaskState::Pending,
        JobState::Running | JobState::Canceling => JobTaskState::Loading,
        JobState::Completed => JobTaskState::Completed,
        JobState::Failed | JobState::CancelFailed => JobTaskState::Failed,
        JobState::Canceled => JobTaskState::Canceled,
    }
}

fn proto_job_state_to_job_state(v: i32) -> JobState {
    match v {
        0 => JobState::Pending,
        1 => JobState::Dispatching,
        2 => JobState::Running,
        3 => JobState::Completed,
        4 => JobState::Failed,
        5 => JobState::Canceling,
        6 => JobState::Canceled,
        7 => JobState::CancelFailed,
        _ => JobState::Pending,
    }
}
