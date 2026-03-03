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

use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    CancelJobRequest, CancelJobResponse, GetJobStatusRequest, GetJobStatusResponse,
    SubmitJobRequest, SubmitJobResponse, TaskReportRequest, TaskReportResponse,
};
use curvine_common::state::{JobTaskProgress, JobTaskState, LoadJobCommand};
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use curvine_server::master::{JobScheduler, RemoteJobScheduler};
use orpc::handler::{HandlerService, MessageHandler};
use orpc::io::net::ConnState;
use orpc::message::{Builder, Message};
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::server::RpcServer;
use orpc::{err_box, CommonResult};
use std::net::TcpListener;
use std::sync::Arc;

#[derive(Clone)]
struct MockSchedulerService;

struct MockSchedulerHandler;

impl MessageHandler for MockSchedulerHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        match RpcCode::from(msg.code()) {
            RpcCode::SubmitJob => {
                let _: SubmitJobRequest = msg.parse_header()?;
                let rep = SubmitJobResponse {
                    job_id: "mock-job-1".to_string(),
                    target_path: "cv:///tmp/target".to_string(),
                };
                Ok(Builder::success_with_header(msg, rep).build())
            }

            RpcCode::GetJobStatus => {
                let req: GetJobStatusRequest = msg.parse_header()?;
                let rep = GetJobStatusResponse {
                    job_id: req.job_id,
                    state: JobTaskState::Loading as i32,
                    source_path: "ufs:///tmp/source".to_string(),
                    target_path: "cv:///tmp/target".to_string(),
                    progress: ProtoUtils::work_progress_to_pb(JobTaskProgress::default()),
                };
                Ok(Builder::success_with_header(msg, rep).build())
            }

            RpcCode::CancelJob => {
                let _: CancelJobRequest = msg.parse_header()?;
                Ok(Builder::success_with_header(msg, CancelJobResponse {}).build())
            }

            RpcCode::ReportTask => {
                let _: TaskReportRequest = msg.parse_header()?;
                Ok(Builder::success_with_header(msg, TaskReportResponse {}).build())
            }

            v => err_box!("Unsupported rpc code: {:?}", v),
        }
    }
}

impl HandlerService for MockSchedulerService {
    type Item = MockSchedulerHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        MockSchedulerHandler
    }
}

#[test]
fn test_remote_scheduler_rpc_roundtrip() -> CommonResult<()> {
    let port = allocate_port();
    let mut conf = ClusterConf::default();
    conf.job.scheduler_hostname = "127.0.0.1".to_string();
    conf.job.scheduler_rpc_port = port;

    let rt = Arc::new(AsyncRuntime::single());
    let server = RpcServer::with_rt(
        rt.clone(),
        conf.scheduler_server_conf(),
        MockSchedulerService,
    );
    let mut state = server.start();
    rt.block_on(state.wait_running())?;

    let scheduler = RemoteJobScheduler::with_conf(rt.clone(), &conf)?;
    let submit_result = scheduler.submit_job(LoadJobCommand {
        source_path: "ufs:///tmp/source".to_string(),
        ..Default::default()
    })?;
    assert_eq!(submit_result.job_id, "mock-job-1");
    assert_eq!(submit_result.target_path, "cv:///tmp/target");

    let status = scheduler.get_job_status("mock-job-1")?;
    assert_eq!(status.job_id, "mock-job-1");
    assert_eq!(status.state, JobTaskState::Loading);
    assert_eq!(status.source_path, "ufs:///tmp/source");
    assert_eq!(status.target_path, "cv:///tmp/target");

    scheduler.cancel_job("mock-job-1")?;
    scheduler.report_task("mock-job-1", "mock-task-1", JobTaskProgress::default())?;
    Ok(())
}

fn allocate_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind random port")
        .local_addr()
        .expect("load local address")
        .port()
}
