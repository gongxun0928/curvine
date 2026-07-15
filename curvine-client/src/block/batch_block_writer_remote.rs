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

use crate::block::batch_block_writer::{check_batch_item_count, combine_complete_results};
use crate::block::BlockClient;
use crate::file::FsContext;
use curvine_common::fs::Path;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use orpc::common::Utils;

pub struct BatchBlockWriterRemote {
    blocks: Vec<ExtendedBlock>,
    client: BlockClient,
    item_active: Vec<bool>,
    seq_id: i32,
    req_id: i64,
    block_size: i64,
}

impl BatchBlockWriterRemote {
    pub async fn new(
        fs_context: &FsContext,
        blocks: Vec<ExtendedBlock>,
        worker_address: WorkerAddress,
    ) -> FsResult<(Self, Vec<bool>)> {
        let req_id = Utils::req_id();
        let seq_id = 0;
        let block_size = fs_context.block_size();
        let client = fs_context.block_client(&worker_address).await?;
        let open_results = match client
            .write_blocks_batch(
                &blocks,
                0,
                block_size,
                req_id,
                seq_id,
                fs_context.write_chunk_size() as i32,
                false,
            )
            .await
        {
            Ok(results) => results,
            Err(error) => {
                let cancels = vec![true; blocks.len()];
                if let Err(cancel_error) = client
                    .write_commit_batch(&blocks, 0, block_size, req_id, seq_id + 1, &cancels)
                    .await
                {
                    log::warn!(
                        "failed to cancel batch blocks on worker {} after open RPC error: {}",
                        worker_address,
                        cancel_error
                    );
                }
                return Err(error);
            }
        };

        let item_active = open_results
            .into_iter()
            .enumerate()
            .map(|(index, result)| match result {
                Ok(context)
                    if context.id == blocks[index].id && context.block_size == block_size =>
                {
                    true
                }
                Ok(context) => {
                    log::warn!(
                        "invalid remote batch open response at index {}: expected block {} size {}, actual block {} size {}",
                        index,
                        blocks[index].id,
                        block_size,
                        context.id,
                        context.block_size
                    );
                    false
                }
                Err(error) => {
                    log::warn!(
                        "failed to open remote batch block {} at index {}: {}",
                        blocks[index].id,
                        index,
                        error
                    );
                    false
                }
            })
            .collect::<Vec<_>>();
        let open_success = item_active.clone();

        Ok((
            Self {
                blocks,
                client,
                item_active,
                seq_id,
                req_id,
                block_size,
            },
            open_success,
        ))
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    pub async fn write(&mut self, files: &[(&Path, &str)], active: &[bool]) -> FsResult<Vec<bool>> {
        check_batch_item_count("remote write files", self.blocks.len(), files.len())?;
        check_batch_item_count("remote write active", self.blocks.len(), active.len())?;

        let effective_active = active
            .iter()
            .zip(&self.item_active)
            .map(|(requested, opened)| *requested && *opened)
            .collect::<Vec<_>>();
        // Inactive slots keep their open state for a later cancel. Sending an
        // empty write would still touch Worker I/O, so skip the write RPC when
        // nothing remains active.
        if effective_active.iter().all(|active| !active) {
            return Ok(vec![false; files.len()]);
        }

        let wire_files = files
            .iter()
            .zip(&effective_active)
            .map(|((path, content), active)| {
                if *active {
                    (*path, *content)
                } else {
                    (*path, "")
                }
            })
            .collect::<Vec<_>>();
        let next_seq_id = self.next_seq_id();
        let worker_results = self
            .client
            .write_files_batch(&wire_files, self.req_id, next_seq_id)
            .await?;

        let mut results = Vec::with_capacity(files.len());
        for (index, worker_success) in worker_results.into_iter().enumerate() {
            let success = effective_active[index] && worker_success;
            if success {
                self.blocks[index].len = files[index].1.len() as i64;
            } else if effective_active[index] {
                self.item_active[index] = false;
            }
            results.push(success);
        }
        Ok(results)
    }

    pub async fn complete(&mut self, cancels: &[bool]) -> FsResult<Vec<bool>> {
        check_batch_item_count("remote complete", self.blocks.len(), cancels.len())?;
        let requested_cancels = cancels.to_vec();
        let effective_cancels = cancels
            .iter()
            .zip(&self.item_active)
            .map(|(requested, active)| *requested || !*active)
            .collect::<Vec<_>>();
        let next_seq_id = self.next_seq_id();
        let worker_results = match self
            .client
            .write_commit_batch(
                &self.blocks,
                0,
                self.block_size,
                self.req_id,
                next_seq_id,
                &effective_cancels,
            )
            .await
        {
            Ok(results) => results,
            Err(error) => {
                let cancels = vec![true; self.blocks.len()];
                if let Err(cancel_error) = self
                    .client
                    .write_commit_batch(
                        &self.blocks,
                        0,
                        self.block_size,
                        self.req_id,
                        next_seq_id + 1,
                        &cancels,
                    )
                    .await
                {
                    log::warn!(
                        "failed to cancel remote batch blocks after complete RPC error: {}",
                        cancel_error
                    );
                }
                self.item_active.fill(false);
                return Err(error);
            }
        };
        self.item_active.fill(false);
        Ok(combine_complete_results(
            &requested_cancels,
            &effective_cancels,
            worker_results,
        ))
    }
}
