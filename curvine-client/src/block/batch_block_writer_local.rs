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
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use orpc::common::Utils;
use orpc::io::LocalFile;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::RawPtr;
use std::sync::Arc;

pub struct BatchBlockWriterLocal {
    rt: Arc<Runtime>,
    blocks: Vec<ExtendedBlock>,
    client: BlockClient,
    files: Vec<Option<RawPtr<LocalFile>>>,
    item_active: Vec<bool>,
    block_size: i64,
    req_id: i64,
}

impl BatchBlockWriterLocal {
    pub async fn new(
        fs_context: Arc<FsContext>,
        blocks: Vec<ExtendedBlock>,
        worker_address: WorkerAddress,
    ) -> FsResult<(Self, Vec<bool>)> {
        let req_id = Utils::req_id();
        let block_size = fs_context.block_size();
        let client = fs_context.block_client(&worker_address).await?;
        let open_results = match client
            .write_blocks_batch(
                &blocks,
                0,
                block_size,
                req_id,
                0,
                fs_context.write_chunk_size() as i32,
                true,
            )
            .await
        {
            Ok(results) => results,
            Err(error) => {
                let cancels = vec![true; blocks.len()];
                if let Err(cancel_error) = client
                    .write_commit_batch(&blocks, 0, block_size, req_id, 1, &cancels)
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

        let mut files = Vec::with_capacity(blocks.len());
        let mut item_active = Vec::with_capacity(blocks.len());
        for (index, result) in open_results.into_iter().enumerate() {
            let file = match result {
                Ok(context)
                    if context.id == blocks[index].id && context.block_size == block_size =>
                {
                    match context.path {
                        Some(path) => match LocalFile::with_write_offset(path, false, 0) {
                            Ok(file) => Some(RawPtr::from_owned(file)),
                            Err(error) => {
                                log::warn!(
                                    "failed to open local batch block {} at index {}: {}",
                                    blocks[index].id,
                                    index,
                                    error
                                );
                                None
                            }
                        },
                        None => {
                            log::warn!(
                                "local batch open returned no path for block {} at index {}",
                                blocks[index].id,
                                index
                            );
                            None
                        }
                    }
                }
                Ok(context) => {
                    log::warn!(
                        "invalid local batch open response at index {}: expected block {} size {}, actual block {} size {}",
                        index,
                        blocks[index].id,
                        block_size,
                        context.id,
                        context.block_size
                    );
                    None
                }
                Err(error) => {
                    log::warn!(
                        "failed to open local batch block {} at index {}: {}",
                        blocks[index].id,
                        index,
                        error
                    );
                    None
                }
            };
            item_active.push(file.is_some());
            files.push(file);
        }

        let open_success = item_active.clone();
        Ok((
            Self {
                rt: fs_context.clone_runtime(),
                blocks,
                client,
                files,
                item_active,
                block_size,
                req_id,
            },
            open_success,
        ))
    }

    pub async fn write(&mut self, files: &[(&Path, &str)], active: &[bool]) -> FsResult<Vec<bool>> {
        check_batch_item_count("local write files", self.blocks.len(), files.len())?;
        check_batch_item_count("local write active", self.blocks.len(), active.len())?;

        let mut results = Vec::with_capacity(files.len());
        for (index, (_, content)) in files.iter().enumerate() {
            if !active[index] || !self.item_active[index] {
                results.push(false);
                continue;
            }

            let Some(local_file) = self.files[index].as_ref().cloned() else {
                self.item_active[index] = false;
                results.push(false);
                continue;
            };
            let content = bytes::Bytes::copy_from_slice(content.as_bytes());
            let content_len = content.len();
            let write_result = self
                .rt
                .spawn_blocking(move || {
                    local_file.as_mut().write_all(&content)?;
                    Ok::<(), FsError>(())
                })
                .await;

            match write_result {
                Ok(Ok(())) => {
                    self.blocks[index].len = content_len as i64;
                    results.push(true);
                }
                Ok(Err(error)) => {
                    log::warn!(
                        "failed to write local batch block {} at index {}: {}",
                        self.blocks[index].id,
                        index,
                        error
                    );
                    self.item_active[index] = false;
                    drop(self.files[index].take());
                    results.push(false);
                }
                Err(error) => {
                    log::warn!(
                        "failed to run local batch write for block {} at index {}: {}",
                        self.blocks[index].id,
                        index,
                        error
                    );
                    self.item_active[index] = false;
                    drop(self.files[index].take());
                    results.push(false);
                }
            }
        }
        Ok(results)
    }

    pub async fn complete(&mut self, cancels: &[bool]) -> FsResult<Vec<bool>> {
        check_batch_item_count("local complete", self.blocks.len(), cancels.len())?;
        let requested_cancels = cancels.to_vec();
        let mut effective_cancels = cancels.to_vec();

        for (index, effective_cancel) in effective_cancels.iter_mut().enumerate() {
            if *effective_cancel || !self.item_active[index] {
                *effective_cancel = true;
                continue;
            }
            let Some(local_file) = self.files[index].as_ref().cloned() else {
                self.item_active[index] = false;
                *effective_cancel = true;
                continue;
            };
            let flush_result = self
                .rt
                .spawn_blocking(move || {
                    local_file.as_mut().flush()?;
                    Ok::<(), FsError>(())
                })
                .await;
            match flush_result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    log::warn!(
                        "failed to flush local batch block {} at index {}: {}",
                        self.blocks[index].id,
                        index,
                        error
                    );
                    self.item_active[index] = false;
                    *effective_cancel = true;
                }
                Err(error) => {
                    log::warn!(
                        "failed to run local batch flush for block {} at index {}: {}",
                        self.blocks[index].id,
                        index,
                        error
                    );
                    self.item_active[index] = false;
                    *effective_cancel = true;
                }
            }
        }

        // Close every short-circuit file before the Worker finalizes or aborts
        // its block state.
        drop(std::mem::take(&mut self.files));
        let worker_results = match self
            .client
            .write_commit_batch(
                &self.blocks,
                0,
                self.block_size,
                self.req_id,
                1,
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
                        1,
                        &cancels,
                    )
                    .await
                {
                    log::warn!(
                        "failed to cancel local batch blocks after complete RPC error: {}",
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
