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

use curvine_common::state::JobMeta;
use curvine_common::utils::SerdeUtils;
use curvine_common::FsResult;
use log::{debug, info, warn};
use rocksdb::{Options, DB};
use std::path::Path;
use std::sync::Arc;

const CF_JOBS: &str = "jobs";

fn rocks_err(e: rocksdb::Error) -> curvine_common::error::FsError {
    curvine_common::error::FsError::from(e.to_string())
}

/// Persistent job metadata store backed by RocksDB.
///
/// Stores `JobMeta` keyed by `job_id`. On Scheduler restart, all non-terminal
/// jobs are loaded back into memory for continued orchestration.
pub struct JobMetaStore {
    db: Arc<DB>,
}

impl JobMetaStore {
    pub fn open(data_dir: impl AsRef<Path>) -> FsResult<Self> {
        let path = data_dir.as_ref().join("scheduler_meta");
        std::fs::create_dir_all(&path)?;

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = vec![CF_JOBS];
        let db = DB::open_cf(&opts, &path, cfs).map_err(rocks_err)?;

        info!("JobMetaStore opened at {:?}", path);
        Ok(Self { db: Arc::new(db) })
    }

    fn cf_jobs(&self) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(CF_JOBS).expect("CF_JOBS must exist")
    }

    pub fn put(&self, meta: &JobMeta) -> FsResult<()> {
        let key = meta.job_id.as_bytes();
        let value = SerdeUtils::serialize(meta)?;
        self.db
            .put_cf(self.cf_jobs(), key, value)
            .map_err(rocks_err)?;
        debug!(
            "persisted job meta: job_id={}, state={}",
            meta.job_id, meta.state
        );
        Ok(())
    }

    pub fn get(&self, job_id: &str) -> FsResult<Option<JobMeta>> {
        match self
            .db
            .get_cf(self.cf_jobs(), job_id.as_bytes())
            .map_err(rocks_err)?
        {
            Some(bytes) => {
                let meta: JobMeta = SerdeUtils::deserialize(&bytes)?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    pub fn delete(&self, job_id: &str) -> FsResult<()> {
        self.db
            .delete_cf(self.cf_jobs(), job_id.as_bytes())
            .map_err(rocks_err)?;
        Ok(())
    }

    /// Load all non-terminal jobs for recovery after restart.
    pub fn load_active_jobs(&self) -> FsResult<Vec<JobMeta>> {
        let mut jobs = Vec::new();
        let iter = self
            .db
            .iterator_cf(self.cf_jobs(), rocksdb::IteratorMode::Start);

        for item in iter {
            let (_, value) = item.map_err(rocks_err)?;
            match SerdeUtils::deserialize::<JobMeta>(&value) {
                Ok(meta) => {
                    if !meta.is_terminal() {
                        info!(
                            "recovered active job: job_id={}, state={}, epoch={}",
                            meta.job_id, meta.state, meta.epoch
                        );
                        jobs.push(meta);
                    }
                }
                Err(e) => {
                    warn!("skipping corrupt job meta entry: {}", e);
                }
            }
        }

        info!("recovered {} active jobs from persistent store", jobs.len());
        Ok(jobs)
    }

    /// Load all jobs (including terminal) for querying.
    pub fn load_all_jobs(&self) -> FsResult<Vec<JobMeta>> {
        let mut jobs = Vec::new();
        let iter = self
            .db
            .iterator_cf(self.cf_jobs(), rocksdb::IteratorMode::Start);

        for item in iter {
            let (_, value) = item.map_err(rocks_err)?;
            match SerdeUtils::deserialize::<JobMeta>(&value) {
                Ok(meta) => jobs.push(meta),
                Err(e) => {
                    warn!("skipping corrupt job meta entry: {}", e);
                }
            }
        }

        Ok(jobs)
    }
}
