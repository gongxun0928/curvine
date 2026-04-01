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
use curvine_common::state::{JobMeta, JobState, WorkerAddress};
use log::{error, info, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

/// Background dispatcher loop that picks Pending jobs and dispatches them to workers.
///
/// It also handles:
/// - Re-dispatch for jobs stuck in Dispatching (e.g., after Scheduler restart)
/// - Sending cancel commands to workers for Canceling jobs
pub struct Dispatcher {
    state: Arc<SchedulerState>,
    client_factory: Arc<SchedulerClientFactory>,
    available_workers: Arc<WorkerRegistry>,
    dispatch_interval: Duration,
}

/// Simple worker registry. In the full implementation this would be backed by
/// heartbeat information. For now it accepts a static list that can be updated.
pub struct WorkerRegistry {
    workers: parking_lot::RwLock<Vec<WorkerAddress>>,
    next_index: std::sync::atomic::AtomicUsize,
}

impl WorkerRegistry {
    pub fn new() -> Self {
        Self {
            workers: parking_lot::RwLock::new(Vec::new()),
            next_index: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn set_workers(&self, workers: Vec<WorkerAddress>) {
        *self.workers.write() = workers;
    }

    pub fn add_worker(&self, worker: WorkerAddress) {
        let mut list = self.workers.write();
        if !list.iter().any(|w| w.worker_id == worker.worker_id) {
            list.push(worker);
        }
    }

    pub fn remove_worker(&self, worker_id: u32) {
        let mut list = self.workers.write();
        list.retain(|w| w.worker_id != worker_id);
    }

    /// Round-robin worker selection.
    pub fn choose_worker(&self) -> Option<WorkerAddress> {
        let list = self.workers.read();
        if list.is_empty() {
            return None;
        }
        let idx = self
            .next_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % list.len();
        Some(list[idx].clone())
    }

    pub fn worker_count(&self) -> usize {
        self.workers.read().len()
    }
}

impl Dispatcher {
    pub fn new(
        state: Arc<SchedulerState>,
        client_factory: Arc<SchedulerClientFactory>,
        workers: Arc<WorkerRegistry>,
        dispatch_interval: Duration,
    ) -> Self {
        Self {
            state,
            client_factory,
            available_workers: workers,
            dispatch_interval,
        }
    }

    /// Start the dispatch loop. Runs until the tokio runtime shuts down.
    pub async fn run(&self) {
        info!("Dispatcher started, interval={:?}", self.dispatch_interval);
        let mut interval = time::interval(self.dispatch_interval);

        loop {
            interval.tick().await;
            self.dispatch_cycle().await;
        }
    }

    async fn dispatch_cycle(&self) {
        let jobs = self.state.iter_active_jobs();

        for meta in jobs {
            match meta.state {
                JobState::Pending => {
                    self.try_dispatch(&meta).await;
                }
                JobState::Dispatching => {
                    // Already dispatching but might need re-dispatch after restart
                    if meta.assigned_worker.is_some() {
                        // Worker was assigned previously, check if we need to re-dispatch
                        // For Phase-1, we treat this as an already-dispatched job
                    } else {
                        self.try_dispatch(&meta).await;
                    }
                }
                JobState::Canceling => {
                    self.try_cancel(&meta).await;
                }
                _ => {}
            }
        }
    }

    async fn try_dispatch(&self, meta: &JobMeta) {
        let worker = match self.available_workers.choose_worker() {
            Some(w) => w,
            None => {
                warn!("no available workers for job {}", meta.job_id);
                return;
            }
        };

        // Transition to Dispatching
        if meta.state == JobState::Pending {
            if let Err(e) = self.state.transition(
                &meta.job_id,
                JobState::Dispatching,
                format!("dispatching to worker {}", worker),
            ) {
                warn!("failed to transition job {} to Dispatching: {}", meta.job_id, e);
                return;
            }
        }

        // Assign worker and get epoch/attempt
        let (epoch, attempt) = match self.state.assign_worker(&meta.job_id, worker.clone()) {
            Ok(v) => v,
            Err(e) => {
                error!("failed to assign worker for job {}: {}", meta.job_id, e);
                return;
            }
        };

        // Send AcceptJob to worker
        let command = match &meta.load_job_command {
            Some(cmd) => cmd,
            None => {
                error!("job {} has no load_job_command", meta.job_id);
                let _ = self.state.transition(
                    &meta.job_id,
                    JobState::Failed,
                    "missing job command",
                );
                return;
            }
        };

        match self.client_factory.get_client(&worker).await {
            Ok(client) => {
                match client
                    .accept_job(&meta.job_id, meta.job_type, epoch, attempt, command)
                    .await
                {
                    Ok(resp) => {
                        if resp.accepted {
                            info!(
                                "job {} dispatched to worker {} (epoch={}, attempt={})",
                                meta.job_id, worker, epoch, attempt
                            );
                        } else {
                            warn!(
                                "worker {} rejected job {}: {}",
                                worker,
                                meta.job_id,
                                resp.reason.unwrap_or_default()
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            "failed to dispatch job {} to worker {}: {}",
                            meta.job_id, worker, e
                        );
                    }
                }
            }
            Err(e) => {
                error!(
                    "failed to connect to worker {} for job {}: {}",
                    worker, meta.job_id, e
                );
            }
        }
    }

    async fn try_cancel(&self, meta: &JobMeta) {
        let worker = match &meta.assigned_worker {
            Some(w) => w.clone(),
            None => {
                // No worker assigned, go directly to Canceled
                let _ = self.state.transition(
                    &meta.job_id,
                    JobState::Canceled,
                    "canceled before worker assignment",
                );
                return;
            }
        };

        match self.client_factory.get_client(&worker).await {
            Ok(client) => {
                match client
                    .cancel_job_to_node(&meta.job_id, meta.epoch)
                    .await
                {
                    Ok(resp) => {
                        if resp.success {
                            let _ = self.state.transition(
                                &meta.job_id,
                                JobState::Canceled,
                                "cancel confirmed by worker",
                            );
                            info!("job {} canceled on worker {}", meta.job_id, worker);
                        } else {
                            let reason = resp.reason.unwrap_or_default();
                            warn!(
                                "worker {} failed to cancel job {}: {}",
                                worker, meta.job_id, reason
                            );
                            let _ = self.state.transition(
                                &meta.job_id,
                                JobState::CancelFailed,
                                format!("worker cancel failed: {}", reason),
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            "failed to send cancel to worker {} for job {}: {}",
                            worker, meta.job_id, e
                        );
                    }
                }
            }
            Err(e) => {
                error!(
                    "failed to connect to worker {} for cancel of job {}: {}",
                    worker, meta.job_id, e
                );
            }
        }
    }
}
