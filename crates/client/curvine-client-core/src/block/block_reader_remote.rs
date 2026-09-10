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

use crate::block::BlockClient;
use crate::file::FsContext;
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, WorkerAddress};
use curvine_proto::{BlockReadRequest, DataHeaderProto};
use curvine_runtime::common::Utils;

pub struct BlockReaderRemote {
    client: BlockClient,
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    pos: i64,
    len: i64,
    req_id: i64,
    seq_id: i32,
    header: Option<DataHeaderProto>,
    read_once: Option<BlockReadRequest>,
    initial_data: Option<DataSlice>,
    read_once_completed: bool,
}

impl BlockReaderRemote {
    pub async fn new(
        fs_context: &FsContext,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        off: i64,
        len: i64,
    ) -> FsResult<Self> {
        let req_id = Utils::req_id();
        let seq_id = 0;

        let client = fs_context.acquire_read(&worker_address).await?;
        if len >= 0 && len <= fs_context.read_chunk_size() as i64 && off >= 0 && off <= len {
            let request = client.read_request(&fs_context.conf.client, &block, off, len, false);
            let data = client.read_small_block(request.clone(), req_id).await?;
            let mut reader =
                Self::from_opened(client, block, worker_address, off, len, req_id, seq_id);
            reader.read_once = Some(request);
            reader.initial_data = Some(data);
            return Ok(reader);
        }
        let _ = client
            .open_block(
                &fs_context.conf.client,
                &block,
                off,
                len,
                req_id,
                seq_id,
                false,
            )
            .await?;

        Ok(Self::from_opened(
            client,
            block,
            worker_address,
            off,
            len,
            req_id,
            seq_id,
        ))
    }

    pub(crate) fn from_opened(
        client: BlockClient,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        off: i64,
        len: i64,
        req_id: i64,
        seq_id: i32,
    ) -> Self {
        Self {
            client,
            block,
            worker_address,
            pos: off,
            len,
            req_id,
            seq_id,
            header: None,
            read_once: None,
            initial_data: None,
            read_once_completed: false,
        }
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn remaining(&self) -> i64 {
        self.len - self.pos
    }

    pub fn seek(&mut self, pos: i64) -> FsResult<i64> {
        self.pos = pos;
        // A seek must observe a fresh read, rather than indefinitely reusing
        // the bytes prefetched by the constructor.
        self.initial_data = None;
        if let Some(request) = &mut self.read_once {
            request.off = pos;
        }
        self.header = Some(DataHeaderProto {
            offset: pos,
            flush: false,
            is_last: false,
        });
        Ok(self.pos)
    }

    pub async fn read(&mut self) -> FsResult<DataSlice> {
        if self.read_once_completed {
            return err_box!("Read session has completed");
        }
        if self.remaining() <= 0 {
            return err_box!("No readable data");
        }

        if let Some(request) = &self.read_once {
            let data = match self.initial_data.take() {
                Some(data) => data,
                None => {
                    self.client
                        .read_small_block(request.clone(), Utils::req_id())
                        .await?
                }
            };
            self.pos += data.len() as i64;
            return Ok(data);
        }

        let seq_id = self.next_seq_id();
        let header = self.header.take();
        let chunk = self.client.read_data(self.req_id, seq_id, header).await?;

        self.pos += chunk.len() as i64;
        Ok(chunk)
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        if self.read_once.is_some() {
            self.initial_data = None;
            self.read_once_completed = true;
            return Ok(());
        }
        let next_seq_id = self.next_seq_id();
        self.client
            .read_commit(&self.block, self.req_id, next_seq_id)
            .await
    }

    pub fn block_id(&self) -> i64 {
        self.block.id
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }
}
