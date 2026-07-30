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
use dashmap::mapref::entry::Entry;
use log::{error, info, warn};
use orpc::client::ClientFactory;
use orpc::io::net::InetAddr;
use orpc::message::{Builder, RequestStatus};
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::sync::FastDashMap;
use orpc::{err_box, try_option, CommonResult};
use std::future::Future;
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

    worker_client_factory: Arc<ClientFactory>,

    replication_enabled: bool,

    metrics: &'static MasterMetrics,
}

struct InflightReplicationJob {
    _block_id: BlockId,
    _permit: OwnedSemaphorePermit,
    target_worker: WorkerAddress,
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
                // The item is no longer queued once recv() returns, even if it
                // later waits for a concurrency permit or submission fails.
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

    fn try_insert_inflight(
        &self,
        block_id: BlockId,
        permit: OwnedSemaphorePermit,
        target_worker: WorkerAddress,
    ) -> bool {
        match self.inflight_blocks.entry(block_id) {
            Entry::Occupied(_) => {
                warn!(
                    "Block {} already has an inflight replication job; skipping duplicate",
                    block_id
                );
                false
            }
            Entry::Vacant(entry) => {
                // Increment while the shard is locked. A report cannot remove
                // the entry and decrement the gauge before this increment.
                self.metrics.replication_inflight_number.inc();
                entry.insert(InflightReplicationJob {
                    _block_id: block_id,
                    _permit: permit,
                    target_worker,
                });
                true
            }
        }
    }

    fn take_inflight(&self, block_id: BlockId) -> Option<InflightReplicationJob> {
        self.inflight_blocks.remove(&block_id).map(|(_, job)| {
            self.metrics.replication_inflight_number.dec();
            job
        })
    }

    fn rollback_inflight(&self, block_id: BlockId) {
        drop(self.take_inflight(block_id));
    }

    async fn submit_replication_job<F>(
        &self,
        block_id: BlockId,
        permit: OwnedSemaphorePermit,
        target_worker: WorkerAddress,
        submit: F,
    ) -> CommonResult<()>
    where
        F: Future<Output = CommonResult<SubmitBlockReplicationResponse>>,
    {
        if !self.try_insert_inflight(block_id, permit, target_worker) {
            return Ok(());
        }

        let response = match submit.await {
            Ok(response) => response,
            Err(e) => {
                // An RPC error is ambiguous: the worker may have queued the
                // job before the response was lost. Keep the entry so a later
                // report can still be matched. A lease-based reaper will be
                // needed to reclaim jobs that were never queued.
                return err_box!(
                    "Replication submit result for block {} is unknown; keeping inflight state: {}",
                    block_id,
                    e
                );
            }
        };

        if response.success {
            return Ok(());
        }

        self.rollback_inflight(block_id);
        err_box!(
            "Replication job for block {} was rejected: {:?}",
            block_id,
            response.message
        )
    }

    async fn replicate_block(
        &self,
        block_id: BlockId,
        permit: OwnedSemaphorePermit,
    ) -> CommonResult<()> {
        // todo: check whether the block_id replicas legal
        if self.inflight_blocks.contains_key(&block_id) {
            warn!(
                "Block {} already has an inflight replication job; skipping duplicate",
                block_id
            );
            return Ok(());
        }

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
        let submit = async move {
            match source_worker_client.rpc(msg).await {
                Ok(response) => response.parse_header(),
                Err(e) => err_box!(
                    "Errors on sending replication job to {}, err: {:?}",
                    &source_worker_addr,
                    e
                ),
            }
        };
        self.submit_replication_job(block_id, permit, target_worker_addr, submit)
            .await
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

            // Increment before publishing the item so the consumer cannot
            // dequeue it and decrement the gauge first.
            metrics.replication_staging_number.inc();
            match sender.try_send(*block_id) {
                Ok(_) => {}
                Err(e) => {
                    metrics.replication_staging_number.dec();
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
        match self.take_inflight(block_id) {
            None => {
                warn!("Should not happen that Block {} not found", block_id);
            }
            Some(entry) => {
                if success {
                    info!("Successfully replicated {}", block_id);
                    let dir = self.fs.fs_dir.write();
                    let location =
                        BlockLocation::new(entry.target_worker.worker_id, storage_type.into());
                    dir.add_block_location(block_id, location)?;
                } else {
                    error!(
                        "Errors on block replication for block_id: {} to worker: {}. error: {:?}",
                        block_id, &entry.target_worker, message
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
    use super::*;
    use crate::master::journal::JournalSystem;
    use curvine_common::state::{ClientAddress, StorageType, WorkerInfo};
    use orpc::common::Utils;
    use std::sync::{Mutex, OnceLock};

    static REPLICATION_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

    fn test_serial() -> std::sync::MutexGuard<'static, ()> {
        REPLICATION_TEST_SERIAL
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn test_manager(name: &str) -> Arc<MasterReplicationManager> {
        Master::init_test_metrics();

        let mut conf = ClusterConf::format();
        let test_id = Utils::rand_str(8);
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.block_replication_enabled = true;
        conf.master.block_replication_concurrency_limit = 1;
        conf.master.meta_dir =
            Utils::test_sub_dir(format!("replication-manager/{name}-{test_id}/meta"));
        conf.journal.journal_dir =
            Utils::test_sub_dir(format!("replication-manager/{name}-{test_id}/journal"));

        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        let worker_manager = fs.worker_manager.clone();
        let (sender, _receiver) = tokio::sync::mpsc::channel(Semaphore::MAX_PERMITS);
        let metrics = Master::get_metrics().unwrap();
        metrics.replication_staging_number.set(0);
        metrics.replication_inflight_number.set(0);

        Arc::new(MasterReplicationManager {
            fs,
            worker_manager,
            replication_semaphore: Arc::new(Semaphore::new(1)),
            staging_queue_sender: Arc::new(sender),
            inflight_blocks: Default::default(),
            worker_client_factory: Arc::new(Default::default()),
            replication_enabled: true,
            metrics,
        })
    }

    fn target_worker(worker_id: u32) -> WorkerAddress {
        WorkerAddress {
            worker_id,
            ip_addr: "127.0.0.1".to_string(),
            rpc_port: 10000 + worker_id,
            ..Default::default()
        }
    }

    #[test]
    fn early_report_is_processed_before_submit_returns() -> CommonResult<()> {
        let _serial = test_serial();
        let manager = test_manager("early-report");
        manager.fs.add_test_worker(WorkerInfo::default());

        let path = "/early-report";
        manager.fs.create(path, true)?;
        let block = manager.fs.add_block(
            path,
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )?;
        let block_id = block.block.id;
        let target = target_worker(200);
        let expected_target = target.worker_id;
        let runtime = AsyncRuntime::single();

        runtime.block_on(async {
            let permit = manager
                .replication_semaphore
                .clone()
                .acquire_owned()
                .await
                .unwrap();
            assert_eq!(manager.replication_semaphore.available_permits(), 0);

            let reporting_manager = manager.clone();
            let submit = async move {
                // The mocked submit does not return until the report has been
                // handled, establishing report-before-RPC-return deterministically.
                assert!(reporting_manager.inflight_blocks.contains_key(&block_id));
                reporting_manager.finish_replicated_block(ReportBlockReplicationRequest {
                    block_id,
                    storage_type: StorageType::Disk.into(),
                    success: true,
                    message: None,
                })?;
                Ok(SubmitBlockReplicationResponse {
                    success: true,
                    message: None,
                })
            };

            manager
                .submit_replication_job(block_id, permit, target, submit)
                .await
        })?;

        assert!(!manager.inflight_blocks.contains_key(&block_id));
        assert_eq!(manager.replication_semaphore.available_permits(), 1);
        assert_eq!(manager.metrics.replication_inflight_number.get(), 0);
        let locations = manager.fs.fs_dir.read().get_block_locations(block_id)?;
        assert!(locations
            .iter()
            .any(|location| location.worker_id == expected_target));
        Ok(())
    }

    #[test]
    fn rejected_submit_rolls_back_inflight_state() -> CommonResult<()> {
        let _serial = test_serial();
        let manager = test_manager("submit-rejected");
        let block_id = 11;
        let runtime = AsyncRuntime::single();

        let result = runtime.block_on(async {
            let permit = manager
                .replication_semaphore
                .clone()
                .acquire_owned()
                .await
                .unwrap();
            manager
                .submit_replication_job(block_id, permit, target_worker(201), async {
                    Ok(SubmitBlockReplicationResponse {
                        success: false,
                        message: Some("queue full".to_string()),
                    })
                })
                .await
        });

        assert!(result.is_err());
        assert!(!manager.inflight_blocks.contains_key(&block_id));
        assert_eq!(manager.replication_semaphore.available_permits(), 1);
        assert_eq!(manager.metrics.replication_inflight_number.get(), 0);
        Ok(())
    }

    #[test]
    fn unknown_submit_result_keeps_inflight_state() -> CommonResult<()> {
        let _serial = test_serial();
        let manager = test_manager("submit-unknown");
        let block_id = 12;
        let runtime = AsyncRuntime::single();

        let result = runtime.block_on(async {
            let permit = manager
                .replication_semaphore
                .clone()
                .acquire_owned()
                .await
                .unwrap();
            manager
                .submit_replication_job(block_id, permit, target_worker(202), async {
                    err_box!("simulated transport error")
                })
                .await
        });

        assert!(result.is_err());
        assert!(manager.inflight_blocks.contains_key(&block_id));
        assert_eq!(manager.replication_semaphore.available_permits(), 0);
        assert_eq!(manager.metrics.replication_inflight_number.get(), 1);

        manager.rollback_inflight(block_id);
        assert_eq!(manager.replication_semaphore.available_permits(), 1);
        assert_eq!(manager.metrics.replication_inflight_number.get(), 0);
        Ok(())
    }
}
