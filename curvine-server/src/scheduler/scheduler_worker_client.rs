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

use curvine_common::fs::RpcCode;
use curvine_common::proto::*;
use curvine_common::state::{JobTaskType, LoadJobCommand, WorkerAddress};
use curvine_common::utils::{RpcUtils, SerdeUtils};
use curvine_common::FsResult;
use orpc::client::{ClientFactory, RpcClient};
use orpc::io::net::InetAddr;
use prost::Message as PMessage;
use std::time::Duration;

/// RPC client used by the Scheduler to communicate with TaskNode (Worker).
#[derive(Clone)]
pub struct SchedulerWorkerClient {
    client: RpcClient,
    timeout: Duration,
}

impl SchedulerWorkerClient {
    pub fn new(client: RpcClient, timeout: Duration) -> Self {
        Self { client, timeout }
    }

    pub async fn rpc<T, R>(&self, code: RpcCode, header: T) -> FsResult<R>
    where
        T: PMessage + Default,
        R: PMessage + Default,
    {
        RpcUtils::proto_rpc(&self.client, self.timeout, code, header).await
    }

    /// Dispatch a whole job to a worker node.
    pub async fn accept_job(
        &self,
        job_id: &str,
        job_type: JobTaskType,
        epoch: u64,
        attempt: u32,
        command: &LoadJobCommand,
    ) -> FsResult<AcceptJobResponse> {
        let request = AcceptJobRequest {
            job_id: job_id.to_string(),
            job_type: Into::<i32>::into(job_type),
            epoch,
            attempt,
            job_command: SerdeUtils::serialize(command)?,
        };

        self.rpc(RpcCode::AcceptJob, request).await
    }

    /// Send cancel to a specific worker node.
    pub async fn cancel_job_to_node(
        &self,
        job_id: &str,
        epoch: u64,
    ) -> FsResult<CancelJobToNodeResponse> {
        let request = CancelJobToNodeRequest {
            job_id: job_id.to_string(),
            epoch,
        };

        self.rpc(RpcCode::CancelJobToNode, request).await
    }
}

/// Factory for creating worker clients from the Scheduler.
pub struct SchedulerClientFactory {
    factory: ClientFactory,
    timeout: Duration,
}

impl SchedulerClientFactory {
    pub fn new(factory: ClientFactory, timeout: Duration) -> Self {
        Self { factory, timeout }
    }

    pub async fn get_client(&self, worker: &WorkerAddress) -> FsResult<SchedulerWorkerClient> {
        let addr = InetAddr::new(worker.ip_addr.clone(), worker.rpc_port as u16);
        let client = self.factory.get(&addr).await?;
        Ok(SchedulerWorkerClient::new(client, self.timeout))
    }
}
