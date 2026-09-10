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

use crate::block::block_client::is_read_once_eligible;
use crate::block::{BlockClient, ReadSession};
use crate::file::FsContext;
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, WorkerAddress};
use curvine_proto::{BlockReadRequest, DataHeaderProto};
use curvine_runtime::common::Utils;

enum ReadMode {
    Streaming {
        session: ReadSession,
        header: Option<DataHeaderProto>,
    },
    Once {
        request: BlockReadRequest,
    },
    Closed,
}

pub struct BlockReaderRemote {
    client: BlockClient,
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    pos: i64,
    len: i64,
    mode: ReadMode,
}

impl BlockReaderRemote {
    pub async fn new(
        fs_context: &FsContext,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        off: i64,
        len: i64,
    ) -> FsResult<Self> {
        let client = fs_context.acquire_read(&worker_address).await?;
        let request = client.read_request(&fs_context.conf.client, &block, off, len, false);
        let mode = if is_read_once_eligible(off, len, request.chunk_size) {
            // Defer the block RPC until read(), after any initial seek.
            ReadMode::Once { request }
        } else {
            let session = ReadSession::new(Utils::req_id(), 0);
            client
                .open_block(
                    &fs_context.conf.client,
                    &block,
                    off,
                    len,
                    session.req_id,
                    session.seq_id,
                    false,
                )
                .await?;
            ReadMode::Streaming {
                session,
                header: None,
            }
        };

        Ok(Self {
            client,
            block,
            worker_address,
            pos: off,
            len,
            mode,
        })
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
            mode: ReadMode::Streaming {
                session: ReadSession::new(req_id, seq_id),
                header: None,
            },
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        matches!(self.mode, ReadMode::Closed)
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
        if self.is_closed() {
            return err_box!("Read session has completed");
        }
        if pos < 0 || pos > self.len {
            return err_box!("Invalid read offset: {}, block length: {}", pos, self.len);
        }
        match &mut self.mode {
            ReadMode::Streaming { header, .. } => {
                *header = Some(DataHeaderProto {
                    offset: pos,
                    flush: false,
                    is_last: false,
                });
            }
            ReadMode::Once { request } => request.off = pos,
            ReadMode::Closed => unreachable!(),
        }
        self.pos = pos;
        Ok(self.pos)
    }

    pub async fn read(&mut self) -> FsResult<DataSlice> {
        if self.is_closed() {
            return err_box!("Read session has completed");
        }
        if self.remaining() <= 0 {
            return err_box!("No readable data");
        }

        let chunk = match &mut self.mode {
            ReadMode::Streaming { session, header } => {
                let seq_id = session.next_seq_id();
                self.client
                    .read_data(session.req_id, seq_id, header.take())
                    .await?
            }
            ReadMode::Once { request } => {
                self.client
                    .read_small_block(request.clone(), Utils::req_id())
                    .await?
            }
            ReadMode::Closed => unreachable!(),
        };
        self.pos += chunk.len() as i64;
        Ok(chunk)
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        if let ReadMode::Streaming { session, .. } = &mut self.mode {
            let seq_id = session.next_seq_id();
            self.client
                .read_commit(&self.block, session.req_id, seq_id)
                .await?;
        }
        self.mode = ReadMode::Closed;
        Ok(())
    }

    pub fn block_id(&self) -> i64 {
        self.block.id
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }
}
