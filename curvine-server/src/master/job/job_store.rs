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

use crate::master::{JobContext, JobPersistence};
use curvine_common::state::{JobTaskProgress, JobTaskState};
use curvine_common::FsResult;
use log::{info, warn};
use orpc::err_box;
use orpc::sync::FastDashMap;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, RwLock};

pub type JobStateCallback =
    Arc<dyn Fn(&str, JobTaskState, JobTaskState, &JobContext) + Send + Sync>;

pub struct JobCallback {
    callback: JobStateCallback,
    filter_states: Option<Vec<JobTaskState>>,
}

impl JobCallback {
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn(&str, JobTaskState, JobTaskState, &JobContext) + Send + Sync + 'static,
    {
        Self {
            callback: Arc::new(callback),
            filter_states: None,
        }
    }

    pub fn with_filter(mut self, states: Vec<JobTaskState>) -> Self {
        self.filter_states = Some(states);
        self
    }

    pub fn should_trigger(&self, new_state: JobTaskState) -> bool {
        match &self.filter_states {
            None => true,
            Some(states) => states.contains(&new_state),
        }
    }
}

#[derive(Clone)]
pub struct JobStore {
    jobs: Arc<FastDashMap<String, JobContext>>,
    callbacks: Arc<RwLock<HashMap<String, Vec<JobCallback>>>>,
    persistence: Option<Arc<JobPersistence>>,
}

impl Default for JobStore {
    fn default() -> Self {
        Self::new()
    }
}

impl JobStore {
    pub fn new() -> Self {
        JobStore {
            jobs: Arc::new(FastDashMap::default()),
            callbacks: Arc::new(RwLock::new(HashMap::new())),
            persistence: None,
        }
    }

    pub fn with_persistence(persistence: Arc<JobPersistence>) -> Self {
        let store = JobStore {
            jobs: Arc::new(FastDashMap::default()),
            callbacks: Arc::new(RwLock::new(HashMap::new())),
            persistence: Some(persistence.clone()),
        };

        match persistence.load() {
            Ok(jobs) => {
                if !jobs.is_empty() {
                    info!(
                        "Restore {} jobs from snapshot {}",
                        jobs.len(),
                        persistence.path().display()
                    );
                    store.restore_jobs(jobs);
                }
            }
            Err(e) => {
                warn!(
                    "Load jobs snapshot {} failed: {}",
                    persistence.path().display(),
                    e
                );
            }
        }

        store
    }

    fn restore_jobs(&self, jobs: Vec<JobContext>) {
        for job in jobs {
            self.jobs.insert(job.info.job_id.clone(), job);
        }
    }

    fn persist_all_jobs(&self) {
        let Some(persistence) = &self.persistence else {
            return;
        };

        let jobs: Vec<JobContext> = self
            .jobs
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        if let Err(e) = persistence.save(&jobs) {
            warn!(
                "Persist jobs snapshot {} failed: {}",
                persistence.path().display(),
                e
            );
        }
    }

    pub fn insert_job(&self, job_id: String, job: JobContext) {
        self.jobs.insert(job_id, job);
        self.persist_all_jobs();
    }

    pub fn remove_job(&self, job_id: impl AsRef<str>) -> Option<(String, JobContext)> {
        let job_id = job_id.as_ref();
        let removed = self.jobs.remove(job_id);
        self.remove_callbacks(job_id);
        if removed.is_some() {
            self.persist_all_jobs();
        }
        removed
    }

    pub fn register_callback(&self, job_id: String, callback: JobCallback) {
        let mut callbacks = self.callbacks.write().unwrap();
        callbacks.entry(job_id).or_default().push(callback);
    }

    pub fn register_completion_callback<F>(&self, job_id: String, callback: F)
    where
        F: Fn(&str, JobTaskState, JobTaskState, &JobContext) + Send + Sync + 'static,
    {
        let cb = JobCallback::new(callback)
            .with_filter(vec![JobTaskState::Completed, JobTaskState::Failed]);
        self.register_callback(job_id, cb);
    }

    fn trigger_callbacks(
        &self,
        job_id: &str,
        old_state: JobTaskState,
        new_state: JobTaskState,
        job: &JobContext,
    ) {
        let callbacks_guard = self.callbacks.read().unwrap();
        if let Some(callbacks) = callbacks_guard.get(job_id) {
            for cb in callbacks {
                if cb.should_trigger(new_state) {
                    (cb.callback)(job_id, old_state, new_state, job);
                }
            }
        }
    }

    pub fn update_progress(
        &self,
        job_id: impl AsRef<str>,
        task_id: impl AsRef<str>,
        progress: JobTaskProgress,
    ) -> FsResult<()> {
        let job_id = job_id.as_ref();
        let task_id = task_id.as_ref();

        let mut job = if let Some(job) = self.jobs.get_mut(job_id) {
            job
        } else {
            return err_box!("Not fond job {}", job_id);
        };

        let old_state: JobTaskState = job.state.state();
        if old_state.is_terminal() {
            return Ok(());
        }

        job.update_progress(task_id, progress)?;

        let new_state: JobTaskState = job.state.state();

        if old_state != new_state {
            let job_id_owned = job_id.to_string();
            let job_clone = (*job).clone();
            drop(job);

            self.trigger_callbacks(&job_id_owned, old_state, new_state, &job_clone);
            self.persist_all_jobs();
        } else {
            drop(job);
            self.persist_all_jobs();
        }

        Ok(())
    }

    pub fn update_state(&self, job_id: &str, state: JobTaskState, message: impl Into<String>) {
        if let Some(mut job) = self.jobs.get_mut(job_id) {
            let old_state: JobTaskState = job.state.state();
            if old_state.is_terminal() && old_state != state {
                return;
            }
            job.update_state(state, message);
            let new_state = state;

            if old_state != new_state {
                let job_clone = (*job).clone();
                drop(job);

                self.trigger_callbacks(job_id, old_state, new_state, &job_clone);
                self.persist_all_jobs();
            } else {
                drop(job);
                self.persist_all_jobs();
            }
        }
    }

    pub fn remove_callbacks(&self, job_id: &str) {
        let mut callbacks = self.callbacks.write().unwrap();
        callbacks.remove(job_id);
    }
}

impl Deref for JobStore {
    type Target = FastDashMap<String, JobContext>;

    fn deref(&self) -> &Self::Target {
        &self.jobs
    }
}
