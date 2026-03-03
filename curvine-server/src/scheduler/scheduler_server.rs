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

use crate::master::{InProcessJobScheduler, JobHandler, JobManager, RpcContext, SyncJobScheduler};
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::FsResult;
use orpc::common::Logger;
use orpc::handler::{HandlerService, MessageHandler};
use orpc::message::Message;
use orpc::runtime::RpcRuntime;
use orpc::server::{RpcServer, ServerStateListener};
use orpc::{err_box, CommonResult};
use std::sync::Arc;

#[derive(Clone)]
pub struct SchedulerService {
    conf: ClusterConf,
    scheduler: SyncJobScheduler,
}

impl SchedulerService {
    pub fn new(conf: ClusterConf, scheduler: SyncJobScheduler) -> Self {
        Self { conf, scheduler }
    }
}

pub struct SchedulerHandler {
    audit_logging_enabled: bool,
    job_handler: JobHandler,
}

impl MessageHandler for SchedulerHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let mut rpc_context = RpcContext::new(msg);
        let ctx = &mut rpc_context;

        let response = match RpcCode::from(msg.code()) {
            RpcCode::SubmitJob
            | RpcCode::GetJobStatus
            | RpcCode::CancelJob
            | RpcCode::ReportTask => self.job_handler.handle(ctx),
            v => err_box!("Unsupported operation for scheduler: {:?}", v),
        };

        let used_us = ctx.spent.used_us();
        if self.audit_logging_enabled {
            ctx.audit_log(response.is_ok(), used_us, None);
        }

        match response {
            Ok(v) => Ok(v),
            Err(e) => Ok(msg.error_ext(&e)),
        }
    }
}

impl HandlerService for SchedulerService {
    type Item = SchedulerHandler;

    fn get_message_handler(&self, _: Option<orpc::io::net::ConnState>) -> Self::Item {
        SchedulerHandler {
            audit_logging_enabled: self.conf.master.audit_logging_enabled,
            job_handler: JobHandler::new(self.scheduler.clone()),
        }
    }
}

pub struct Scheduler {
    rpc_server: RpcServer<SchedulerService>,
    job_manager: Arc<JobManager>,
}

impl Scheduler {
    pub fn with_conf(conf: ClusterConf) -> CommonResult<Self> {
        Logger::init(conf.master.log.clone());

        let rt = Arc::new(conf.scheduler_server_conf().create_runtime());
        let job_manager = Arc::new(JobManager::from_cluster_conf(rt.clone(), &conf)?);
        let scheduler: SyncJobScheduler = Arc::new(InProcessJobScheduler::new(job_manager.clone()));

        let service = SchedulerService::new(conf.clone(), scheduler);
        let rpc_server = RpcServer::with_rt(rt, conf.scheduler_server_conf(), service);

        Ok(Self {
            rpc_server,
            job_manager,
        })
    }

    pub async fn start(self) -> ServerStateListener {
        self.job_manager.start();

        let mut status = self.rpc_server.start();
        status.wait_running().await.unwrap();
        status
    }

    pub fn block_on_start(self) {
        let rt = self.rpc_server.clone_rt();
        rt.block_on(async move {
            let mut status = self.start().await;
            status.wait_stop().await.unwrap();
        });
    }
}
