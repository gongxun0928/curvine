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

use crate::FsResult;
use orpc::common::DurationUnit;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for the Scheduler process (Job control plane).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerConf {
    pub hostname: String,
    pub rpc_port: u16,
    pub web_port: u16,

    pub meta_dir: String,

    pub io_threads: usize,
    pub worker_threads: usize,
    pub buffer_size: usize,

    #[serde(skip)]
    pub dispatch_interval: Duration,
    #[serde(alias = "dispatch_interval")]
    pub dispatch_interval_str: String,

    #[serde(skip)]
    pub rpc_timeout: Duration,
    #[serde(alias = "rpc_timeout")]
    pub rpc_timeout_str: String,

    #[serde(skip)]
    pub job_cleanup_ttl: Duration,
    #[serde(alias = "job_cleanup_ttl")]
    pub job_cleanup_ttl_str: String,
}

impl SchedulerConf {
    pub const DEFAULT_HOSTNAME: &'static str = "localhost";
    pub const DEFAULT_RPC_PORT: u16 = 8998;
    pub const DEFAULT_WEB_PORT: u16 = 9004;
    pub const DEFAULT_META_DIR: &'static str = "/tmp/curvine/scheduler";
    pub const DEFAULT_IO_THREADS: usize = 4;
    pub const DEFAULT_WORKER_THREADS: usize = 4;
    pub const DEFAULT_BUFFER_SIZE: usize = 65536;
    pub const DEFAULT_DISPATCH_INTERVAL: &'static str = "1s";
    pub const DEFAULT_RPC_TIMEOUT: &'static str = "30s";
    pub const DEFAULT_JOB_CLEANUP_TTL: &'static str = "24h";

    pub fn init(&mut self) -> FsResult<()> {
        self.dispatch_interval =
            DurationUnit::from_str(&self.dispatch_interval_str)?.as_duration();
        self.rpc_timeout = DurationUnit::from_str(&self.rpc_timeout_str)?.as_duration();
        self.job_cleanup_ttl =
            DurationUnit::from_str(&self.job_cleanup_ttl_str)?.as_duration();
        Ok(())
    }
}

impl Default for SchedulerConf {
    fn default() -> Self {
        Self {
            hostname: Self::DEFAULT_HOSTNAME.to_string(),
            rpc_port: Self::DEFAULT_RPC_PORT,
            web_port: Self::DEFAULT_WEB_PORT,
            meta_dir: Self::DEFAULT_META_DIR.to_string(),
            io_threads: Self::DEFAULT_IO_THREADS,
            worker_threads: Self::DEFAULT_WORKER_THREADS,
            buffer_size: Self::DEFAULT_BUFFER_SIZE,
            dispatch_interval: Duration::from_secs(1),
            dispatch_interval_str: Self::DEFAULT_DISPATCH_INTERVAL.to_string(),
            rpc_timeout: Duration::from_secs(30),
            rpc_timeout_str: Self::DEFAULT_RPC_TIMEOUT.to_string(),
            job_cleanup_ttl: Duration::from_secs(86400),
            job_cleanup_ttl_str: Self::DEFAULT_JOB_CLEANUP_TTL.to_string(),
        }
    }
}
