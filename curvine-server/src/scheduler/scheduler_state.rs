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

use crate::scheduler::job_meta_store::JobMetaStore;
use curvine_common::state::{
    JobMeta, JobState, JobTaskProgress, WorkerAddress,
};
use curvine_common::FsResult;
use log::warn;
use orpc::common::LocalTime;
use orpc::err_box;
use orpc::sync::FastDashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

/// In-memory job state combined with persistent store.
///
/// Every state transition is first validated against the state machine,
/// then persisted, then applied to in-memory state, and finally broadcast.
pub struct SchedulerState {
    jobs: FastDashMap<String, JobMeta>,
    store: Arc<JobMetaStore>,
    event_tx: broadcast::Sender<JobEvent>,
}

#[derive(Clone, Debug)]
pub struct JobEvent {
    pub job_id: String,
    pub old_state: JobState,
    pub new_state: JobState,
}

impl SchedulerState {
    pub fn new(store: Arc<JobMetaStore>) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            jobs: FastDashMap::default(),
            store,
            event_tx,
        }
    }

    /// Recover active jobs from persistent store into memory.
    pub fn recover(&self) -> FsResult<usize> {
        let active_jobs = self.store.load_active_jobs()?;
        let count = active_jobs.len();
        for meta in active_jobs {
            self.jobs.insert(meta.job_id.clone(), meta);
        }
        Ok(count)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<JobEvent> {
        self.event_tx.subscribe()
    }

    /// Create a new job in Pending state and persist it.
    pub fn create_job(&self, meta: JobMeta) -> FsResult<()> {
        if self.jobs.contains_key(&meta.job_id) {
            return err_box!("job {} already exists", meta.job_id);
        }
        self.store.put(&meta)?;
        self.jobs.insert(meta.job_id.clone(), meta);
        Ok(())
    }

    /// Transition a job's state. Returns the old state on success.
    pub fn transition(
        &self,
        job_id: &str,
        target: JobState,
        message: impl Into<String>,
    ) -> FsResult<JobState> {
        let msg = message.into();
        let mut entry = self
            .jobs
            .get_mut(job_id)
            .ok_or_else(|| orpc::CommonError::from(format!("job {} not found", job_id)))?;

        let old_state = entry.state;
        if !old_state.can_transition_to(target) {
            return err_box!(
                "invalid transition for job {}: {:?} -> {:?}",
                job_id,
                old_state,
                target
            );
        }

        entry.state = target;
        entry.update_time = LocalTime::mills() as i64;
        entry.message = msg;

        self.store.put(&entry)?;

        let event = JobEvent {
            job_id: job_id.to_string(),
            old_state,
            new_state: target,
        };

        // Best-effort broadcast; no receivers is OK
        let _ = self.event_tx.send(event);

        Ok(old_state)
    }

    /// Assign a worker to a job and increment the attempt counter.
    pub fn assign_worker(
        &self,
        job_id: &str,
        worker: WorkerAddress,
    ) -> FsResult<(u64, u32)> {
        let mut entry = self
            .jobs
            .get_mut(job_id)
            .ok_or_else(|| orpc::CommonError::from(format!("job {} not found", job_id)))?;

        entry.assigned_worker = Some(worker);
        entry.attempt += 1;
        entry.update_time = LocalTime::mills() as i64;
        let epoch = entry.epoch;
        let attempt = entry.attempt;

        self.store.put(&entry)?;
        Ok((epoch, attempt))
    }

    /// Update job progress from worker report (non-critical, allowed to regress slightly on restart).
    pub fn update_progress(
        &self,
        job_id: &str,
        epoch: u64,
        attempt: u32,
        progress: JobTaskProgress,
    ) -> FsResult<()> {
        let mut entry = self
            .jobs
            .get_mut(job_id)
            .ok_or_else(|| orpc::CommonError::from(format!("job {} not found", job_id)))?;

        // Fencing: reject stale epoch/attempt
        if epoch < entry.epoch || (epoch == entry.epoch && attempt < entry.attempt) {
            warn!(
                "dropping stale progress for job {}: event epoch/attempt={}/{}, current={}/{}",
                job_id, epoch, attempt, entry.epoch, entry.attempt
            );
            return Ok(());
        }

        entry.progress = progress;
        entry.update_time = LocalTime::mills() as i64;

        // Progress persistence is best-effort (non-critical field)
        let _ = self.store.put(&entry);
        Ok(())
    }

    /// Handle a state event reported by a worker. Validates fencing.
    pub fn process_worker_event(
        &self,
        job_id: &str,
        epoch: u64,
        attempt: u32,
        reported_state: JobState,
        progress: JobTaskProgress,
        message: Option<String>,
    ) -> FsResult<()> {
        let mut entry = self
            .jobs
            .get_mut(job_id)
            .ok_or_else(|| orpc::CommonError::from(format!("job {} not found", job_id)))?;

        // Fencing: reject stale epoch/attempt
        if epoch < entry.epoch || (epoch == entry.epoch && attempt < entry.attempt) {
            warn!(
                "dropping stale event for job {}: event epoch/attempt={}/{}, current={}/{}",
                job_id, epoch, attempt, entry.epoch, entry.attempt
            );
            return Ok(());
        }

        // Terminal state guard: never regress
        if entry.state.is_terminal() {
            warn!(
                "dropping event for terminal job {}: current={:?}, reported={:?}",
                job_id, entry.state, reported_state
            );
            return Ok(());
        }

        let old_state = entry.state;
        if !old_state.can_transition_to(reported_state) {
            warn!(
                "invalid worker event transition for job {}: {:?} -> {:?}, ignoring",
                job_id, old_state, reported_state
            );
            return Ok(());
        }

        entry.state = reported_state;
        entry.progress = progress;
        entry.update_time = LocalTime::mills() as i64;
        if let Some(msg) = message {
            entry.message = msg;
        }

        self.store.put(&entry)?;

        let event = JobEvent {
            job_id: job_id.to_string(),
            old_state,
            new_state: reported_state,
        };
        let _ = self.event_tx.send(event);

        Ok(())
    }

    pub fn get_job(&self, job_id: &str) -> Option<JobMeta> {
        self.jobs.get(job_id).map(|e| e.clone())
    }

    /// Bump epoch for re-dispatch scenarios.
    pub fn bump_epoch(&self, job_id: &str) -> FsResult<u64> {
        let mut entry = self
            .jobs
            .get_mut(job_id)
            .ok_or_else(|| orpc::CommonError::from(format!("job {} not found", job_id)))?;

        entry.epoch += 1;
        entry.attempt = 0;
        entry.update_time = LocalTime::mills() as i64;

        self.store.put(&entry)?;
        Ok(entry.epoch)
    }

    /// Remove a terminal job from memory (persistent store retains for query).
    pub fn cleanup_terminal(&self, job_id: &str) {
        if let Some(entry) = self.jobs.get(job_id) {
            if entry.state.is_terminal() {
                drop(entry);
                self.jobs.remove(job_id);
            }
        }
    }

    pub fn active_job_count(&self) -> usize {
        self.jobs.len()
    }

    pub fn iter_active_jobs(&self) -> Vec<JobMeta> {
        self.jobs.iter().map(|e| e.value().clone()).collect()
    }
}
