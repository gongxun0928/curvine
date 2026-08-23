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
use crate::master::quota::QuotaManager;
use crate::master::replication::master_replication_manager::MasterReplicationManager;
use crate::master::MasterMonitor;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::runtime::{GroupExecutor, LoopTask};
use log::{error, info, warn};
use std::sync::Arc;

pub struct HeartbeatChecker {
    fs: MasterFilesystem,
    monitor: MasterMonitor,
    executor: Arc<GroupExecutor>,
    worker_blacklist_ms: u64,
    worker_lost_ms: u64,
    replication_manager: Arc<MasterReplicationManager>,
    quota_manager: Arc<QuotaManager>,
}

impl HeartbeatChecker {
    pub fn new(
        fs: MasterFilesystem,
        monitor: MasterMonitor,
        executor: Arc<GroupExecutor>,
        replication_manager: Arc<MasterReplicationManager>,
        quota_manager: Arc<QuotaManager>,
    ) -> Self {
        let worker_blacklist_ms = fs.conf.worker_blacklist_interval_ms();
        let worker_lost_ms = fs.conf.worker_lost_interval_ms();
        Self {
            fs,
            monitor,
            executor,
            worker_blacklist_ms,
            worker_lost_ms,
            replication_manager,
            quota_manager,
        }
    }
}

impl LoopTask for HeartbeatChecker {
    type Error = FsError;

    fn run(&self) -> FsResult<()> {
        if !self.monitor.is_active() {
            return Ok(());
        }

        let mut blacklisted_workers = Vec::new();
        let mut removed_workers = Vec::new();
        {
            // 4d.2 round-3 (gpt56 `f5980e03` P0-1): the lost-worker
            // transition — WM expired removal below and the exact cache
            // retire in the spawned task — shares the outcome apply's
            // transition gate (`start_gate`), so an incremental-report
            // outcome's tag recheck and its WM/ack side effects are
            // linearized against the retire: either the retire wins first
            // (the outcome's recheck sees the tag gone and drops it) or
            // the apply wins first (the retire blocks until the side
            // effects land). Lock order matches the heartbeat path
            // (start_gate → WM write).
            let _gate = self.fs.start_gate.lock();
            let mut wm = self.fs.worker_manager.write();
            let workers = wm.get_last_heartbeat();
            let now = LocalTime::mills();

            for (id, last_update) in workers {
                if now > last_update + self.worker_blacklist_ms {
                    // Worker blacklist timeout
                    if let Some(worker) = wm.add_blacklist_worker(id) {
                        blacklisted_workers.push((id, worker.address, worker.last_update));
                    }
                }

                if now > last_update + self.worker_lost_ms {
                    // Heartbeat timeout
                    if let Some(worker) = wm.remove_expired_worker(id) {
                        // 4d (R9-2): snapshot the worker's wire session id
                        // BEFORE the WorkerInfo is discarded, so the async
                        // cleanup can retire the CACHE session exactly — a
                        // worker that restarted in between keeps its new
                        // session untouched.
                        removed_workers.push((
                            id,
                            worker.address,
                            worker.last_update,
                            worker.worker_session_id,
                        ));
                    }
                }
            }
        }

        for (id, address, last_update) in blacklisted_workers {
            warn!(
                "Worker {} ({}) last heartbeat {} has exceeded blacklist timeout {} ms",
                id, address, last_update, self.worker_blacklist_ms
            );
        }

        for (id, address, last_update, worker_session_id) in removed_workers {
            warn!(
                "Worker {} ({}) last heartbeat {} has exceeded lost timeout {} ms and will be removed",
                id, address, last_update, self.worker_lost_ms
            );
            // Asynchronously delete all block location data.
            let fs = self.fs.clone();
            let rm = self.replication_manager.clone();
            let res = self.executor.spawn(move || {
                let spend = TimeSpent::new();
                // 4d (R9-2): retire the lost worker's CACHE session
                // first and exactly — the volatile registry callback
                // verifies the recorded session still matches before
                // moving the live reverse set to the retired drain; a
                // Start that landed in between makes this a no-op.
                // Round-3 P0-1: the retire takes the same transition
                // gate as the fenced outcome apply, closing the
                // recheck→side-effect window against a concurrent
                // incremental-report outcome. The gate is dropped before
                // the FS location cleanup so the (possibly long) delete
                // never stretches the transition critical section.
                {
                    let _gate = fs.start_gate.lock();
                    fs.end_worker_session(id, &worker_session_id);
                }
                let cleanup = match fs.delete_locations(id) {
                    Err(e) => {
                        warn!("{}", curvine_core_error::err_msg!(e));
                        Default::default()
                    }
                    Ok(res) => res,
                };
                let replication_block_num = cleanup.replication_block_ids.len();
                if let Err(e) = rm.report_under_replicated_blocks(id, cleanup.replication_block_ids)
                {
                    error!(
                        "Errors on reporting under-replicated {} blocks. err: {:?}",
                        replication_block_num, e
                    );
                }
                info!(
                    "Delete worker {} all locations used {} ms",
                    id,
                    spend.used_ms()
                );
            });
            if let Err(e) = &res {
                warn!("{}", e);
            }
        }

        if let Ok(info) = self.fs.filesystem_info() {
            self.quota_manager.detector(Some(info));
        };

        Ok(())
    }

    fn terminate(&self) -> bool {
        self.monitor.is_stop()
    }
}
