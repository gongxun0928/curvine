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

use crate::master::fs::MasterFilesystem;
use crate::master::{Master, MasterMetrics, SyncWorkerManager};
use curvine_common::conf::ClusterConf;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    ReportBlockReplicationRequest, SubmitBlockReplicationRequest, SubmitBlockReplicationResponse,
};
use curvine_common::state::{BlockLocation, WorkerAddress};
use curvine_common::utils::ProtoUtils;
use log::{error, info, warn};
use orpc::client::ClientFactory;
use orpc::io::net::InetAddr;
use orpc::message::{Builder, RequestStatus};
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::sync::FastDashMap;
use orpc::{err_box, try_option, CommonResult};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub type BlockId = i64;
type WorkerId = u32;

#[derive(Clone)]
pub struct MasterReplicationManager {
    fs: MasterFilesystem,
    worker_manager: SyncWorkerManager,

    replication_semaphore: Arc<Semaphore>,

    staging_queue_sender: Arc<Sender<BlockId>>,
    inflight_blocks: Arc<FastDashMap<BlockId, InflightReplicationJob>>,
    next_job_id: Arc<AtomicU64>,

    worker_client_factory: Arc<ClientFactory>,

    replication_enabled: bool,

    metrics: &'static MasterMetrics,
}

struct InflightReplicationJob {
    job_id: u64,
    _permit: OwnedSemaphorePermit,
    target_worker: WorkerAddress,
}

fn insert_inflight_job(
    inflight_blocks: &FastDashMap<BlockId, InflightReplicationJob>,
    block_id: BlockId,
    job: InflightReplicationJob,
) -> bool {
    let job_id = job.job_id;
    let current = inflight_blocks.entry(block_id).or_insert(job);
    current.job_id == job_id
}

fn remove_inflight_job(
    inflight_blocks: &FastDashMap<BlockId, InflightReplicationJob>,
    block_id: BlockId,
    job_id: u64,
) -> Option<InflightReplicationJob> {
    inflight_blocks
        .remove_if(&block_id, |_, job| job.job_id == job_id)
        .map(|(_, job)| job)
}

impl MasterReplicationManager {
    pub fn new(
        fs: &MasterFilesystem,
        conf: &ClusterConf,
        rt: &Arc<AsyncRuntime>,
        worker_manager: &SyncWorkerManager,
    ) -> CommonResult<Arc<Self>> {
        let async_runtime = rt.clone();
        let semaphore = Semaphore::new(conf.master.block_replication_concurrency_limit);
        let (send, recv) = tokio::sync::mpsc::channel(Semaphore::MAX_PERMITS);

        let manager = Self {
            fs: fs.clone(),
            worker_manager: worker_manager.clone(),
            replication_semaphore: Arc::new(semaphore),
            staging_queue_sender: Arc::new(send),
            inflight_blocks: Default::default(),
            next_job_id: Arc::new(AtomicU64::new(0)),
            worker_client_factory: Arc::new(Default::default()),
            replication_enabled: conf.master.block_replication_enabled,
            metrics: Master::get_metrics()?,
        };
        let manager = Arc::new(manager);
        Self::handle(async_runtime, manager.clone(), recv);

        info!("Master replication manager is initialized");
        Ok(manager)
    }

    fn handle(async_runtime: Arc<AsyncRuntime>, me: Arc<Self>, mut recv: Receiver<BlockId>) {
        let fork = me.clone();
        async_runtime.spawn(async move {
            let manager = fork;
            while let Some(block_id) = recv.recv().await {
                manager.metrics.replication_staging_number.dec();
                let permit = match manager.replication_semaphore.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(e) => {
                        error!("Block replication loop stopped: semaphore closed: {}", e);
                        break;
                    }
                };
                if let Err(e) = manager.replicate_block(block_id, permit).await {
                    error!("Failed to replicate block: {}. err: {}", block_id, e);
                }
            }
        });
    }

    fn get_next_worker(&self, worker_id: WorkerId) -> CommonResult<WorkerAddress> {
        let worker_manager = self.worker_manager.read();
        match worker_manager.get_worker(worker_id) {
            None => {
                err_box!("Worker not found: {}", worker_id)
            }
            Some(worker) => Ok(worker.address.clone()),
        }
    }

    fn assign(&self, exclusive_worker_ids: Vec<WorkerId>) -> CommonResult<WorkerAddress> {
        let worker_manager = self.worker_manager.read();
        let mut assignment = worker_manager.choose_workers(1, exclusive_worker_ids)?;
        let Some(worker_id) = assignment.pop().map(|worker| worker.worker_id) else {
            return err_box!("no target worker selected for block replication");
        };
        let Some(worker) = worker_manager.get_worker(worker_id) else {
            return err_box!(
                "selected replication target worker {} no longer exists",
                worker_id
            );
        };
        let worker_addr = worker.address.clone();
        Ok(worker_addr)
    }

    async fn replicate_block(
        &self,
        block_id: BlockId,
        permit: OwnedSemaphorePermit,
    ) -> CommonResult<()> {
        // todo: check whether the block_id replicas legal

        let locations = {
            let fs_dir = self.fs.fs_dir.read();
            fs_dir.get_block_locations(block_id)?
        };

        // step1: find out the available worker to replicate blocks
        // todo: use pluggable policy to find out the best worker to do replication
        let source_worker_id =
            try_option!(locations.first(), "missing block: {}", block_id).worker_id;
        let source_worker_addr = self.get_next_worker(source_worker_id)?;

        // step2: choose the target worker
        let target_worker_addr = self.assign(locations.iter().map(|x| x.worker_id).collect())?;
        info!(
            "block_id: {}. locations: {:?}, target: {}",
            block_id, &locations, &target_worker_addr
        );

        // step3: call the corresponding worker to do replication
        let source_worker_addr = InetAddr::new(
            &source_worker_addr.ip_addr,
            source_worker_addr.rpc_port as u16,
        );
        let source_worker_client = self
            .worker_client_factory
            .create_raw(&source_worker_addr)
            .await?;

        let request = SubmitBlockReplicationRequest {
            block_id,
            target_worker_info: ProtoUtils::worker_address_to_pb(&target_worker_addr),
        };
        let msg = Builder::new_rpc(RpcCode::SubmitBlockReplicationJob)
            .request(RequestStatus::Rpc)
            .proto_header(request)
            .build();
        // Register before the submit RPC. The worker acknowledges as soon as the
        // job is queued and reports on another connection, so the report can
        // otherwise reach the master before this await resumes.
        let job_id = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        if !insert_inflight_job(
            &self.inflight_blocks,
            block_id,
            InflightReplicationJob {
                job_id,
                _permit: permit,
                target_worker: target_worker_addr,
            },
        ) {
            return err_box!("Block {} already has an inflight replication", block_id);
        }
        self.metrics.replication_inflight_number.inc();

        let submit_result = match source_worker_client.rpc(msg).await {
            Ok(response) => match response.parse_header::<SubmitBlockReplicationResponse>() {
                Ok(response) if response.success => Ok(()),
                Ok(response) => err_box!(
                    "Errors on submit replication job to {}. err: {:?}",
                    &source_worker_addr,
                    response.message
                ),
                Err(e) => Err(e),
            },
            Err(e) => err_box!(
                "Errors on sending replication job to {}, err: {:?}",
                &source_worker_addr,
                e
            ),
        };

        if let Err(e) = submit_result {
            if remove_inflight_job(&self.inflight_blocks, block_id, job_id).is_some() {
                self.metrics.replication_inflight_number.dec();
            }
            return Err(e);
        }

        Ok(())
    }

    pub fn report_under_replicated_blocks(
        &self,
        _worker_id: WorkerId,
        block_ids: Vec<i64>,
    ) -> CommonResult<()> {
        if !self.replication_enabled {
            return Ok(());
        }

        let sender = self.staging_queue_sender.clone();
        let metrics = self.metrics;

        for block_id in &block_ids {
            info!("Accepting block {} replication job", block_id);

            match sender.try_send(*block_id) {
                Ok(_) => {
                    metrics.replication_staging_number.inc();
                }
                Err(e) => {
                    error!(
                        "Failed to queue replication job for block {}: {}. Queue may be full. Will retry on next heartbeat check.",
                        block_id, e
                    );
                }
            }
        }
        Ok(())
    }

    pub fn finish_replicated_block(&self, req: ReportBlockReplicationRequest) -> CommonResult<()> {
        // todo: retry on failure of block replication

        let block_id = req.block_id;
        let success = req.success;
        let message = req.message;
        let storage_type = req.storage_type;
        match self.inflight_blocks.remove(&block_id) {
            None => {
                warn!(
                    "Ignoring stale or duplicate replication result for block {}",
                    block_id
                );
            }
            Some(entry) => {
                self.metrics.replication_inflight_number.dec();
                if success {
                    info!("Successfully replicated {}", block_id);
                    let dir = self.fs.fs_dir.write();
                    let location =
                        BlockLocation::new(entry.1.target_worker.worker_id, storage_type.into());
                    dir.add_block_location(block_id, location)?;
                } else {
                    error!(
                        "Errors on block replication for block_id: {} to worker: {}. error: {:?}",
                        block_id, &entry.1.target_worker, message
                    );
                    self.metrics.replication_failure_count.inc();
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{insert_inflight_job, remove_inflight_job, BlockId, InflightReplicationJob};
    use curvine_common::state::WorkerAddress;
    use orpc::sync::FastDashMap;
    use tokio::sync::Semaphore;

    async fn job(semaphore: &std::sync::Arc<Semaphore>, job_id: u64) -> InflightReplicationJob {
        InflightReplicationJob {
            job_id,
            _permit: semaphore.clone().acquire_owned().await.unwrap(),
            target_worker: WorkerAddress::default(),
        }
    }

    #[tokio::test]
    async fn rollback_only_removes_its_own_inflight_generation() {
        let semaphore = std::sync::Arc::new(Semaphore::new(2));
        let inflight = FastDashMap::<BlockId, InflightReplicationJob>::default();

        assert!(insert_inflight_job(&inflight, 7, job(&semaphore, 1).await));
        assert!(!insert_inflight_job(&inflight, 7, job(&semaphore, 2).await));
        assert_eq!(semaphore.available_permits(), 1);

        assert!(remove_inflight_job(&inflight, 7, 2).is_none());
        assert!(inflight.contains_key(&7));
        assert_eq!(semaphore.available_permits(), 1);

        drop(remove_inflight_job(&inflight, 7, 1));
        assert!(!inflight.contains_key(&7));
        assert_eq!(semaphore.available_permits(), 2);
    }

    #[tokio::test]
    async fn early_report_removal_makes_submit_rollback_a_noop() {
        let semaphore = std::sync::Arc::new(Semaphore::new(1));
        let inflight = FastDashMap::<BlockId, InflightReplicationJob>::default();

        assert!(insert_inflight_job(&inflight, 8, job(&semaphore, 1).await));
        assert_eq!(semaphore.available_permits(), 0);

        // Simulate a worker report racing ahead of the submit RPC response.
        drop(inflight.remove(&8));
        assert_eq!(semaphore.available_permits(), 1);

        assert!(remove_inflight_job(&inflight, 8, 1).is_none());
        assert_eq!(semaphore.available_permits(), 1);
    }
}
