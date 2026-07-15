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

use crate::block::{BatchBlockWriterLocal, BatchBlockWriterRemote};
use crate::file::FsContext;
use curvine_common::fs::Path;
use curvine_common::state::{ExtendedBlock, StorageType, WorkerAddress};
use curvine_common::FsResult;
use orpc::err_box;
use std::sync::Arc;

enum BatchWriterAdapter {
    Local(BatchBlockWriterLocal),
    Remote(BatchBlockWriterRemote),
}

impl BatchWriterAdapter {
    async fn write(&mut self, files: &[(&Path, &str)], active: &[bool]) -> FsResult<Vec<bool>> {
        match self {
            Self::Local(writer) => writer.write(files, active).await,
            Self::Remote(writer) => writer.write(files, active).await,
        }
    }

    async fn complete(&mut self, cancels: &[bool]) -> FsResult<Vec<bool>> {
        match self {
            Self::Local(writer) => writer.complete(cancels).await,
            Self::Remote(writer) => writer.complete(cancels).await,
        }
    }
}

pub(crate) fn check_batch_item_count(
    operation: &str,
    expected: usize,
    actual: usize,
) -> FsResult<()> {
    if expected != actual {
        return err_box!(
            "batch block {} item count mismatch, expected {}, actual {}",
            operation,
            expected,
            actual
        );
    }
    Ok(())
}

pub(crate) fn combine_complete_results(
    requested_cancels: &[bool],
    effective_cancels: &[bool],
    worker_results: Vec<bool>,
) -> Vec<bool> {
    requested_cancels
        .iter()
        .zip(effective_cancels)
        .zip(worker_results)
        .map(|((requested_cancel, effective_cancel), worker_success)| {
            worker_success && (*requested_cancel || !*effective_cancel)
        })
        .collect()
}

/// One batch write session to one Worker.
///
/// File-level scheduling, replica aggregation and Worker grouping belong to
/// the caller. This type only preserves item order within a single Worker RPC.
pub struct BatchBlockWriter {
    inner: BatchWriterAdapter,
}

impl BatchBlockWriter {
    pub async fn new(
        fs_context: Arc<FsContext>,
        blocks: Vec<ExtendedBlock>,
        worker_addr: WorkerAddress,
    ) -> FsResult<(Self, Vec<bool>)> {
        if blocks.is_empty() {
            return err_box!("No blocks provided");
        }

        let has_spdk = blocks
            .iter()
            .any(|block| block.storage_type == StorageType::SpdkDisk);
        let short_circuit = fs_context.conf.client.short_circuit
            && fs_context.is_local_worker(&worker_addr)
            && !has_spdk;

        let (inner, results) = if short_circuit {
            let (writer, results) =
                BatchBlockWriterLocal::new(fs_context, blocks, worker_addr).await?;
            (BatchWriterAdapter::Local(writer), results)
        } else {
            let (writer, results) =
                BatchBlockWriterRemote::new(&fs_context, blocks, worker_addr).await?;
            (BatchWriterAdapter::Remote(writer), results)
        };

        Ok((Self { inner }, results))
    }

    pub async fn write(&mut self, files: &[(&Path, &str)], active: &[bool]) -> FsResult<Vec<bool>> {
        self.inner.write(files, active).await
    }

    pub async fn complete(&mut self, cancels: &[bool]) -> FsResult<Vec<bool>> {
        self.inner.complete(cancels).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forced_cancel_does_not_turn_failed_commit_into_success() {
        let results = combine_complete_results(
            &[false, true, false],
            &[false, true, true],
            vec![true, true, true],
        );
        assert_eq!(results, vec![true, true, false]);
    }

    #[test]
    fn validates_item_count() {
        assert!(check_batch_item_count("write", 2, 2).is_ok());
        assert!(check_batch_item_count("write", 2, 1).is_err());
    }
}
