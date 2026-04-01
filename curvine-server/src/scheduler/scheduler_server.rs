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

use crate::scheduler::dispatcher::{Dispatcher, WorkerRegistry};
use crate::scheduler::job_meta_store::JobMetaStore;
use crate::scheduler::scheduler_handler::SchedulerHandler;
use crate::scheduler::scheduler_state::SchedulerState;
use crate::scheduler::scheduler_worker_client::SchedulerClientFactory;
use curvine_common::conf::{ClusterConf, SchedulerConf};
use log::info;
use orpc::client::ClientFactory;
use orpc::common::Logger;
use orpc::handler::HandlerService;
use orpc::io::net::ConnState;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::server::{RpcServer, ServerConf, ServerStateListener};
use orpc::CommonResult;
use std::sync::Arc;

/// Service that creates per-connection handlers for the Scheduler RPC server.
#[derive(Clone)]
pub struct SchedulerService {
    state: Arc<SchedulerState>,
    client_factory: Arc<SchedulerClientFactory>,
    buf_size: usize,
}

impl HandlerService for SchedulerService {
    type Item = SchedulerHandler;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        SchedulerHandler::new(
            self.state.clone(),
            self.client_factory.clone(),
            self.buf_size,
        )
    }
}

/// The Scheduler process: Job control plane, decoupled from Master.
///
/// Responsibilities:
/// - Accept SubmitJob / GetJobStatus / CancelJob from clients
/// - Persist JobMeta to RocksDB
/// - Dispatch jobs to TaskNode (Worker) processes
/// - Process worker event reports
/// - Recover active jobs on restart
pub struct Scheduler {
    rpc_server: RpcServer<SchedulerService>,
    dispatcher: Arc<Dispatcher>,
    worker_registry: Arc<WorkerRegistry>,
    rt: Arc<Runtime>,
}

impl Scheduler {
    pub fn with_conf(conf: ClusterConf) -> CommonResult<Self> {
        Logger::init(conf.log.clone());

        let scheduler_conf = &conf.scheduler;

        // Open persistent store
        let store = Arc::new(JobMetaStore::open(&scheduler_conf.meta_dir)?);

        // Create in-memory state and recover
        let state = Arc::new(SchedulerState::new(store));
        let recovered = state.recover()?;
        info!("Scheduler recovered {} active jobs", recovered);

        // Create runtime
        let server_conf = Self::server_conf(scheduler_conf);
        let rt = Arc::new(server_conf.create_runtime());

        // Create worker client factory
        let rpc_conf = conf.client.client_rpc_conf();
        let client_factory = ClientFactory::with_rt(rpc_conf, rt.clone());
        let scheduler_client_factory = Arc::new(SchedulerClientFactory::new(
            client_factory,
            scheduler_conf.rpc_timeout,
        ));

        // Worker registry
        let worker_registry = Arc::new(WorkerRegistry::new());

        // Dispatcher
        let dispatcher = Arc::new(Dispatcher::new(
            state.clone(),
            scheduler_client_factory.clone(),
            worker_registry.clone(),
            scheduler_conf.dispatch_interval,
        ));

        // RPC service
        let service = SchedulerService {
            state,
            client_factory: scheduler_client_factory,
            buf_size: scheduler_conf.buffer_size,
        };

        let rpc_server = RpcServer::with_rt(rt.clone(), server_conf, service);

        Ok(Self {
            rpc_server,
            dispatcher,
            worker_registry,
            rt,
        })
    }

    fn server_conf(conf: &SchedulerConf) -> ServerConf {
        let mut server_conf = ServerConf::with_hostname(&conf.hostname, conf.rpc_port);
        server_conf.name = "curvine-scheduler".to_string();
        server_conf.io_threads = conf.io_threads;
        server_conf.worker_threads = conf.worker_threads;
        server_conf
    }

    pub async fn start(self) -> ServerStateListener {
        // Start RPC server
        let mut rpc_status = self.rpc_server.start();
        rpc_status.wait_running().await.unwrap();

        // Start dispatcher loop
        let dispatcher = self.dispatcher.clone();
        self.rt.spawn(async move {
            dispatcher.run().await;
        });

        info!("Scheduler started");
        rpc_status
    }

    pub fn block_on_start(self) {
        let rt = self.rpc_server.clone_rt();
        rt.block_on(async move {
            let mut status = self.start().await;
            status.wait_stop().await.unwrap();
        });
    }

    pub fn worker_registry(&self) -> &Arc<WorkerRegistry> {
        &self.worker_registry
    }
}
