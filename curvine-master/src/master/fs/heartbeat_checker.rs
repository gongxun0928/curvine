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
        // 4d.2 round-4 (gpt56 `36f4e28b` P0-1): the scan only FLAGS
        // candidates; the actual removal + cache retire run inside the
        // contiguous `lost_worker_transition` primitive (ONE start_gate
        // hold), so the session snapshot below is only the exactness
        // token the primitive re-verifies under the gate.
        let mut lost_candidates = Vec::new();
        {
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
                    // Heartbeat timeout — flag for the contiguous
                    // transition below. The session is snapshotted under
                    // this gate hold; the primitive re-verifies it.
                    if let Some(worker) = wm.get_worker(id) {
                        lost_candidates.push((
                            id,
                            worker.address.clone(),
                            worker.last_update,
                            worker.worker_session_id.clone(),
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

        for (id, _scan_address, _scan_last_update, worker_session_id) in lost_candidates {
            // 4d.2 round-4 (gpt56 `36f4e28b` P0-1): the CONTIGUOUS
            // lost-worker transition — WM expired removal + exact cache
            // accumulator/volatile retire inside ONE gate hold. The
            // primitive re-verifies the row (exact session + still
            // expired), so a re-registration between this scan and the
            // transition survives untouched. Returns None = superseded,
            // nothing removed.
            let worker =
                match self
                    .fs
                    .lost_worker_transition(id, &worker_session_id, self.worker_lost_ms)
                {
                    Some(worker) => worker,
                    None => continue,
                };
            warn!(
                "Worker {} ({}) last heartbeat {} has exceeded lost timeout {} ms and will be removed",
                id, worker.address, worker.last_update, self.worker_lost_ms
            );
            // Only the long FS location cleanup + replication report are
            // async — the transition critical section is already complete,
            // so this (possibly long) delete never stretches it.
            let fs = self.fs.clone();
            let rm = self.replication_manager.clone();
            let res = self.executor.spawn(move || {
                let spend = TimeSpent::new();
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
