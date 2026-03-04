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
use curvine_common::FsResult;
use curvine_server::master::{
    InProcessJobScheduler, JobHandler, JobManager, JobScheduler, RemoteJobScheduler, RpcContext,
    SyncJobScheduler,
};
use orpc::common::Utils;
use orpc::err_box;
use orpc::handler::{HandlerService, MessageHandler};
use orpc::io::net::{ConnState, NetUtils};
use orpc::message::Message;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::server::RpcServer;
use std::sync::Arc;

#[derive(Clone)]
struct SplitSchedulerService {
    scheduler: SyncJobScheduler,
}

struct SplitSchedulerHandler {
    job_handler: JobHandler,
}

impl MessageHandler for SplitSchedulerHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let mut rpc_context = RpcContext::new(msg);
        let ctx = &mut rpc_context;

        let response = match RpcCode::from(msg.code()) {
            RpcCode::SubmitJob
            | RpcCode::GetJobStatus
            | RpcCode::CancelJob
            | RpcCode::ReportTask => self.job_handler.handle(ctx),
            v => err_box!("Unsupported operation for scheduler split test: {:?}", v),
        };

        match response {
            Ok(v) => Ok(v),
            Err(e) => Ok(msg.error_ext(&e)),
        }
    }
}

impl HandlerService for SplitSchedulerService {
    type Item = SplitSchedulerHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        SplitSchedulerHandler {
            job_handler: JobHandler::new(self.scheduler.clone()),
        }
    }
}

#[test]
fn test_real_scheduler_service_can_be_called_remotely() -> FsResult<()> {
    let rt = Arc::new(AsyncRuntime::single());
    let mut conf = ClusterConf::default();
    let suffix = NetUtils::get_available_port();
    conf.master.meta_dir = Utils::test_sub_dir(format!("scheduler-split-test/meta-{}", suffix));
    conf.journal.journal_dir =
        Utils::test_sub_dir(format!("scheduler-split-test/journal-{}", suffix));
    conf.master.hostname = "127.0.0.1".to_string();
    conf.job.scheduler_hostname = "127.0.0.1".to_string();
    conf.job.scheduler_rpc_port = NetUtils::get_available_port();
    conf.job.init()?;

    let job_manager = Arc::new(JobManager::from_cluster_conf(rt.clone(), &conf)?);
    let scheduler: SyncJobScheduler = Arc::new(InProcessJobScheduler::new(job_manager));
    let service = SplitSchedulerService { scheduler };

    let server = RpcServer::with_rt(rt.clone(), conf.scheduler_server_conf(), service);
    let mut state = server.start();
    rt.block_on(state.wait_running())?;

    let client = RemoteJobScheduler::with_conf(rt.clone(), &conf)?;
    let missing = match client.get_job_status("missing-job") {
        Ok(_) => {
            return Err(FsError::common(
                "Expected JobNotFound when querying missing job status",
            ))
        }
        Err(e) => e,
    };
    assert!(
        matches!(missing, FsError::JobNotFound(_)),
        "unexpected error kind: {:?}",
        missing
    );

    let cancel_missing = client.cancel_job("missing-job").unwrap_err();
    assert!(
        matches!(cancel_missing, FsError::JobNotFound(_)),
        "unexpected error kind: {:?}",
        cancel_missing
    );
    Ok(())
}
