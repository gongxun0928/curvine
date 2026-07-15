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

use crate::worker::block::BlockStore;
use crate::worker::handler::WriteContext;
use crate::worker::handler::WriteHandler;
use crate::worker::storage::BlockWriteContext;
use curvine_common::error::FsError;
use curvine_common::proto::{
    BlockBatchOpenResult, BlockWriteRequest, BlockWriteResponse, BlocksBatchCommitRequest,
    BlocksBatchCommitResponse, BlocksBatchWriteRequest, BlocksBatchWriteResponse,
    FilesBatchWriteRequest, FilesBatchWriteResponse,
};
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use orpc::err_box;
use orpc::handler::MessageHandler;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::sys::DataSlice;
use orpc::CommonResult;

pub struct BatchWriteHandler {
    pub(crate) store: BlockStore,
    pub(crate) context: Option<Vec<Option<WriteContext>>>,
    pub(crate) file: Option<Vec<Option<BlockWriteContext>>>,
    pub(crate) write_handler: WriteHandler,
}

impl BatchWriteHandler {
    pub fn new(store: BlockStore) -> CommonResult<Self> {
        let store_clone = store.clone();
        Ok(Self {
            store,
            context: None,
            file: None,
            write_handler: WriteHandler::new(store_clone)?,
        })
    }

    fn abort_open_contexts(store: &BlockStore, contexts: &[Option<WriteContext>]) {
        for context in contexts.iter().flatten() {
            if let Err(e) = store.abort_block(&context.block) {
                log::warn!(
                    "failed to abort batch-opened block {} after open error: {}",
                    context.block.id,
                    e
                );
            }
        }
    }

    fn abort_batch_state(&mut self) {
        // Close all open devices before releasing their block allocations.
        drop(self.write_handler.file.take());
        let active_context = self.write_handler.context.take();
        drop(self.file.take());
        let mut contexts = self.context.take().unwrap_or_default();
        if let Some(context) = active_context {
            contexts.push(Some(context));
        }
        Self::abort_open_contexts(&self.store, &contexts);
    }

    pub fn open_batch(&mut self, msg: &Message) -> FsResult<Message> {
        self.abort_batch_state();
        let header: BlocksBatchWriteRequest = msg.parse_header()?;
        let mut results = Vec::with_capacity(header.blocks.len());
        let mut files = Vec::with_capacity(header.blocks.len());
        let mut contexts = Vec::with_capacity(header.blocks.len());

        for (i, block_proto) in header.blocks.iter().cloned().enumerate() {
            let unique_req_id = msg.req_id() + i as i64;
            // Create a single BlockWriteRequest from the block
            let header = BlockWriteRequest {
                block: block_proto,
                off: header.off,
                block_size: header.block_size,
                short_circuit: header.short_circuit,
                client_name: header.client_name.clone(),
                chunk_size: header.chunk_size,
                pipeline_stream: Vec::new(),
            };

            // Create single request message for each block
            let single_msg_req = Builder::new()
                .code(msg.code())
                .request(RequestStatus::Open)
                .req_id(unique_req_id)
                .seq_id(msg.seq_id())
                .proto_header(header)
                .build();

            let response = match self.write_handler.open(&single_msg_req) {
                Ok(response) => response,
                Err(e) => {
                    drop(self.write_handler.file.take());
                    if let Some(context) = self.write_handler.context.take() {
                        if let Err(abort_error) = self.store.abort_block(&context.block) {
                            log::warn!(
                                "failed to abort batch block {} after open error: {}",
                                context.block.id,
                                abort_error
                            );
                        }
                    }
                    results.push(BlockBatchOpenResult {
                        response: None,
                        error: Some(e.to_string()),
                    });
                    files.push(None);
                    contexts.push(None);
                    continue;
                }
            };

            // Extract file and context from handler and store in batch vectors
            let Some(context) = self.write_handler.context.take() else {
                drop(self.write_handler.file.take());
                results.push(BlockBatchOpenResult {
                    response: None,
                    error: Some(format!(
                        "batch open did not create write context for block index {}",
                        i
                    )),
                });
                files.push(None);
                contexts.push(None);
                continue;
            };
            let block_response: BlockWriteResponse = match response.parse_header() {
                Ok(response) => response,
                Err(e) => {
                    drop(self.write_handler.file.take());
                    if let Err(abort_err) = self.store.abort_block(&context.block) {
                        log::warn!(
                            "failed to abort batch-opened block {} after response parse error: {}",
                            context.block.id,
                            abort_err
                        );
                    }
                    results.push(BlockBatchOpenResult {
                        response: None,
                        error: Some(e.to_string()),
                    });
                    files.push(None);
                    contexts.push(None);
                    continue;
                }
            };
            results.push(BlockBatchOpenResult {
                response: Some(block_response),
                error: None,
            });
            files.push(self.write_handler.file.take());
            contexts.push(Some(context));
        }
        self.file = Some(files);
        self.context = Some(contexts);
        // Legacy `responses` is only safe when every item succeeds. Partial
        // failures would otherwise return a dense success subset that no longer
        // aligns with request indices. New clients must use `results`.
        let responses = if results.iter().all(|result| result.response.is_some()) {
            results
                .iter()
                .filter_map(|result| result.response.clone())
                .collect()
        } else {
            Vec::new()
        };
        let batch_response = BlocksBatchWriteResponse { responses, results };

        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    pub fn complete_batch(&mut self, msg: &Message, cancel_request: bool) -> FsResult<Message> {
        let header: BlocksBatchCommitRequest = match msg.parse_header() {
            Ok(header) => header,
            Err(error) => {
                self.abort_batch_state();
                return Err(error.into());
            }
        };
        let cancels = if header.cancel_flags.is_empty() {
            vec![header.cancel; header.blocks.len()]
        } else if header.cancel_flags.len() == header.blocks.len() {
            header.cancel_flags.clone()
        } else {
            let block_count = header.blocks.len();
            let action_count = header.cancel_flags.len();
            self.abort_batch_state();
            return err_box!(
                "batch commit action count mismatch: blocks={}, actions={}",
                block_count,
                action_count
            );
        };
        let Some(contexts) = self.context.take() else {
            drop(self.file.take());
            return err_box!("batch commit without open context");
        };
        let Some(files) = self.file.take() else {
            Self::abort_open_contexts(&self.store, &contexts);
            return err_box!("batch commit without open files");
        };
        if contexts.len() != header.blocks.len() || files.len() != header.blocks.len() {
            let context_count = contexts.len();
            let file_count = files.len();
            drop(files);
            Self::abort_open_contexts(&self.store, &contexts);
            return err_box!(
                "batch commit request count mismatch: blocks={}, contexts={}, files={}",
                header.blocks.len(),
                context_count,
                file_count
            );
        }
        if cancel_request && cancels.iter().any(|cancel| !cancel) {
            drop(files);
            Self::abort_open_contexts(&self.store, &contexts);
            return err_box!("batch cancel request contains commit items");
        }

        let mut results = Vec::with_capacity(header.blocks.len());
        for (index, (((block, cancel), opened), file)) in header
            .blocks
            .into_iter()
            .zip(cancels.into_iter())
            .zip(contexts.into_iter())
            .zip(files.into_iter())
            .enumerate()
        {
            let Some(opened) = opened else {
                drop(file);
                // Canceling a never-opened item is an idempotent success.
                results.push(cancel);
                continue;
            };
            if header.off != opened.off || header.block_size != opened.block_size {
                drop(file);
                if let Err(abort_error) = self.store.abort_block(&opened.block) {
                    log::warn!(
                        "failed to abort batch block {} after commit header mismatch: {}",
                        opened.block.id,
                        abort_error
                    );
                }
                results.push(false);
                continue;
            }
            let requested = WriteContext {
                block: ProtoUtils::extend_block_from_pb(block),
                req_id: msg.req_id() + index as i64,
                chunk_size: opened.chunk_size,
                short_circuit: opened.short_circuit,
                off: opened.off,
                block_size: opened.block_size,
            };
            let result =
                WriteHandler::complete_block(&self.store, file, Some(&opened), &requested, !cancel);
            if let Err(error) = result.as_ref() {
                log::warn!(
                    "failed to complete batch block {} at index {}: {}",
                    requested.block.id,
                    index,
                    error
                );
            }
            results.push(result.is_ok());
        }
        let batch_response = BlocksBatchCommitResponse { results };

        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    pub fn write_batch(&mut self, msg: &Message) -> FsResult<Message> {
        let header: FilesBatchWriteRequest = match msg.parse_header() {
            Ok(header) => header,
            Err(error) => {
                self.abort_batch_state();
                return Err(error.into());
            }
        };
        let (Some(files), Some(contexts)) = (self.file.as_mut(), self.context.as_ref()) else {
            self.abort_batch_state();
            return err_box!("batch write without complete open state");
        };
        let file_count = files.len();
        let context_count = contexts.len();
        if header.files.len() != file_count || header.files.len() != context_count {
            self.abort_batch_state();
            return err_box!(
                "batch write file count mismatch: request={}, files={}, contexts={}",
                header.files.len(),
                file_count,
                context_count
            );
        }
        if files.iter().zip(contexts).any(|(file, context)| {
            !matches!(
                (file.as_ref(), context.as_ref()),
                (Some(_), Some(_)) | (None, None)
            )
        }) {
            self.abort_batch_state();
            return err_box!("batch write contains inconsistent item state");
        }

        let mut results = Vec::with_capacity(header.files.len());

        for (index, ((file, context), file_data)) in files
            .iter_mut()
            .zip(contexts)
            .zip(header.files.into_iter())
            .enumerate()
        {
            let (Some(file), Some(context)) = (file.as_mut(), context.as_ref()) else {
                results.push(false);
                continue;
            };
            let result = WriteHandler::check_req_id(context, header.req_id + index as i64)
                .and_then(|_| {
                    let data = DataSlice::Bytes(bytes::Bytes::from(file_data.content));
                    WriteHandler::write_data(
                        file,
                        &data,
                        self.write_handler.io_slow_us,
                        self.write_handler.metrics,
                    )
                });
            if let Err(error) = result.as_ref() {
                log::warn!(
                    "failed to write batch block {} at index {}: {}",
                    context.block.id,
                    index,
                    error
                );
            }
            results.push(result.is_ok());
        }

        let batch_response = FilesBatchWriteResponse { results };
        Ok(Builder::success(msg).proto_header(batch_response).build())
    }
}

impl MessageHandler for BatchWriteHandler {
    type Error = FsError;
    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let request_status = msg.request_status();
        match request_status {
            // batch operations
            RequestStatus::Open => self.open_batch(msg),
            RequestStatus::Running => self.write_batch(msg),
            RequestStatus::Complete => self.complete_batch(msg, false),
            RequestStatus::Cancel => self.complete_batch(msg, true),
            _ => err_box!("Unsupported request type"),
        }
    }
}

impl Drop for BatchWriteHandler {
    fn drop(&mut self) {
        self.abort_batch_state();
    }
}
