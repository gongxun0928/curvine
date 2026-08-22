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
use curvine_proto::DataHeaderProto;
use curvine_runtime::common::Utils;

pub struct BlockReaderRemote {
    client: BlockClient,
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    pos: i64,
    len: i64,
    req_id: i64,
    seq_id: i32,
    /// Default (Open-time) frame length, used by `read()` so every Running
    /// frame from a new client carries an explicit offset + read_len.
    chunk_size: i64,
}

impl BlockReaderRemote {
    /// Mirror of the worker's `ReadHandler::MAX_READ_AHEAD`: the largest frame
    /// length a worker accepts on a Running read.
    pub const MAX_READ_LEN: i64 = 16 * 1024 * 1024;

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
            fs_context.read_chunk_size() as i64,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_opened(
        client: BlockClient,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        off: i64,
        len: i64,
        req_id: i64,
        seq_id: i32,
        chunk_size: i64,
    ) -> Self {
        Self {
            client,
            block,
            worker_address,
            pos: off,
            len,
            req_id,
            seq_id,
            chunk_size,
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
        Ok(self.pos)
    }

    /// Default-frame read. Every Running frame from a new client carries an
    /// explicit offset + read_len (frozen protocol): route through
    /// `read_with_len` with the Open-time chunk size so the frame size and
    /// worker behavior are unchanged, only the header is now always present.
    /// Legacy workers ignore `read_len` and answer with their fixed frame.
    pub async fn read(&mut self) -> FsResult<DataSlice> {
        let fetch_len = self.chunk_size.min(self.remaining()).max(1);
        self.read_with_len(fetch_len).await
    }

    /// Demand-aware read: ask the worker to return up to `fetch_len` bytes in
    /// this single Running response, starting at the current position. The
    /// header carries an explicit offset. A legacy worker ignores `read_len`
    /// and returns its fixed Open-time frame; the position then advances by
    /// the actually received bytes and the caller loops, so mixed versions
    /// stay byte-correct.
    pub async fn read_with_len(&mut self, fetch_len: i64) -> FsResult<DataSlice> {
        if self.remaining() <= 0 {
            return err_box!("No readable data");
        }

        // Bound the request by what is left in this block and by the worker's
        // maximum accepted frame length.
        let fetch_len = fetch_len.clamp(1, Self::MAX_READ_LEN).min(self.remaining());
        let header = DataHeaderProto {
            offset: self.pos,
            flush: false,
            is_last: false,
            read_len: Some(fetch_len),
        };

        let seq_id = self.next_seq_id();
        let chunk = self
            .client
            .read_data(self.req_id, seq_id, Some(header))
            .await?;

        // Advance only by the actual returned length: a short frame (legacy
        // worker, block/file tail) is valid progress, not an error.
        self.pos += chunk.len() as i64;
        Ok(chunk)
    }

    pub async fn complete(&mut self) -> FsResult<()> {
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
