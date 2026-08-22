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

use crate::file::{FsContext, FsReaderBase, FsReaderParallel, ReadDetector};
use crate::{FileChunk, FileSlice};
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_io::DataSlice;
use curvine_model::FileBlocks;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::channel::{
    AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender,
};
use curvine_runtime::sync::ErrorMonitor;
use log::error;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Permit;
use tokio::sync::Notify;

// Control task type
enum ReadTask {
    Seek(i64, CallSender<i8>),
    Stop(CallSender<i8>),
    Pause((i64, bool)),
}

/// Byte-budgeted prefetch window shared between one lane's producer task and
/// its consumer. The producer reserves bytes before fetching a frame; the
/// consumer releases them once the frame is fully consumed (the caller only
/// pulls the next frame after its buffer is empty), discarded on seek, or at
/// completion. One lane therefore holds at most `window` bytes across queued,
/// in-flight, and currently buffered data, independent of the frame size —
/// message-count capacity alone would let 1 MiB frames inflate the lane to
/// 8 MiB.
struct ByteBudget {
    window: i64,
    used: AtomicI64,
    notify: Notify,
}

impl ByteBudget {
    fn new(window: i64) -> Self {
        Self {
            window,
            used: AtomicI64::new(0),
            notify: Notify::new(),
        }
    }

    /// Non-blocking reserve of up to `want` bytes. Returns `None` when fewer
    /// than `floor` bytes are free. `want` must be >= floor >= 1.
    fn try_acquire(&self, want: i64, floor: i64) -> Option<i64> {
        let mut cur = self.used.load(Ordering::Acquire);
        loop {
            let take = want.min(self.window - cur);
            if take < floor {
                return None;
            }
            match self.used.compare_exchange_weak(
                cur,
                cur + take,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(take),
                Err(actual) => cur = actual,
            }
        }
    }

    fn release(&self, n: i64) {
        if n > 0 {
            self.used.fetch_sub(n, Ordering::AcqRel);
            self.notify.notify_waiters();
        }
    }
}

struct PrefetchArgs {
    rt: Arc<Runtime>,
    reader: FsReaderParallel,
    chunk_sender: AsyncSender<FileChunk>,
    task_receiver: AsyncReceiver<ReadTask>,
    budget: Arc<ByteBudget>,
    /// First-frame length: the real demand that started this lane (when it
    /// exceeds chunk_size) or chunk_size. Keeps a small foreground read from
    /// waiting on a large first response.
    initial_len: i64,
    /// Client local delivery unit / legacy fallback frame size.
    chunk_size: i64,
    /// Coalesced sequential transfer target for subsequent frames.
    target: i64,
}

// A parallel task description structure
// chunk_receiver: accept data
// task_sender: Send control command
struct BufferChannel {
    prefetch: Option<PrefetchArgs>,
    chunk_receiver: AsyncReceiver<FileChunk>,
    task_sender: AsyncSender<ReadTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
    budget: Arc<ByteBudget>,
    /// Bytes of the most recently received frame still counted against the
    /// budget. Released when the frame is fully consumed, on seek, or at
    /// completion — never while the bytes are still buffered for the caller.
    held: i64,
}

impl BufferChannel {
    fn check_error(&self, e: impl Into<FsError>) -> FsError {
        self.err_monitor.take_error().unwrap_or(e.into())
    }

    /// Start the prefetch task on the first read. `demand` is the caller's
    /// real current demand in bytes (<= 0 means unknown, keep chunk_size).
    fn start_prefetch(&mut self, demand: i64) {
        let Some(mut args) = self.prefetch.take() else {
            return;
        };

        // A real demand larger than chunk_size lifts the first frame so the
        // foreground caller does not wait on a small first response.
        if demand > args.chunk_size {
            args.initial_len = demand.min(args.target);
        }

        let monitor = self.err_monitor.clone();
        let parallel_id = args.reader.parallel_id();

        args.rt.spawn(async move {
            let res = FsReaderBuffer::read_future(
                args.chunk_sender,
                args.task_receiver,
                args.budget,
                args.initial_len,
                args.chunk_size,
                args.target,
                args.reader,
            )
            .await;
            match res {
                Ok(_) => {}
                Err(e) => {
                    error!("buffer read(parallel id {}) error: {:?}", parallel_id, e);
                    monitor.set_error(e);
                }
            }
        });
    }

    /// Release the budget held for the frame currently buffered for the
    /// caller. Called when that frame is fully consumed, on seek, and at
    /// completion — never while the bytes are still buffered.
    fn release_held(&mut self) {
        if self.held > 0 {
            self.budget.release(self.held);
            self.held = 0;
        }
    }

    async fn read(&mut self, demand: i64) -> FsResult<FileChunk> {
        self.start_prefetch(demand);

        // The previous frame has been fully consumed by this new read.
        self.release_held();

        let chunk = self
            .chunk_receiver
            .recv_check()
            .await
            .map_err(|e| self.check_error(e))?;

        self.held = chunk.len() as i64;
        Ok(chunk)
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        self.start_prefetch(0);

        // Bytes still buffered for the caller are discarded by the seek.
        self.release_held();

        let budget = self.budget.clone();
        let fun = async {
            // Notify seek and seek will pause data reading.
            let (tx, rx) = CallChannel::channel();
            self.task_sender.send(ReadTask::Seek(pos, tx)).await?;
            rx.receive().await?;

            // Clear the buffer data to remove old prefetched data before seek.
            // Both random and sequential reads need to clear buffer after seek,
            // because the prefetched data may be from the old position.
            // Every discarded frame also releases its byte budget.
            while let Some(chunk) = self.chunk_receiver.try_recv()? {
                budget.release(chunk.len() as i64);
            }

            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.release_held();

        if self.prefetch.is_some() {
            // Prefetch task was never started, nothing to stop.
            return Ok(());
        }

        let fun = async {
            // Send a stop command and wait for the command to complete
            let (tx, rx) = CallChannel::channel();
            self.task_sender.send(ReadTask::Stop(tx)).await?;
            rx.receive().await?;
            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    async fn pause(&self, pos: i64, pause: bool) -> FsResult<()> {
        if self.prefetch.is_some() {
            return Ok(());
        }
        let fun = async { self.task_sender.send(ReadTask::Pause((pos, pause))).await };
        fun.await.map_err(|e| self.check_error(e))
    }
}

#[allow(clippy::large_enum_variant)]
enum ReaderAdapter {
    Buffer(BufferChannel),
    Base(FsReaderParallel),
}

impl ReaderAdapter {
    /// Demand-aware read: `demand > 0` is the caller's real current demand in
    /// bytes; `demand <= 0` keeps the default frame behavior.
    async fn read_demand(&mut self, demand: i64) -> FsResult<FileChunk> {
        match self {
            ReaderAdapter::Buffer(r) => r.read(demand).await,
            ReaderAdapter::Base(r) => r.read_with_len(demand).await,
        }
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        match self {
            ReaderAdapter::Buffer(r) => r.seek(pos).await,
            ReaderAdapter::Base(r) => r.seek(pos).await,
        }
    }

    async fn complete(&mut self) -> FsResult<()> {
        match self {
            ReaderAdapter::Buffer(r) => r.complete().await,
            ReaderAdapter::Base(r) => r.complete().await,
        }
    }

    async fn pause(&mut self, pos: i64, pause: bool) -> FsResult<()> {
        match self {
            ReaderAdapter::Buffer(r) => r.pause(pos, pause).await,
            ReaderAdapter::Base(r) => r.seek(pos).await,
        }
    }
}

// Reader with buffer.
pub struct FsReaderBuffer {
    readers: Vec<ReaderAdapter>,
    base_reader_index: usize,
    path: Path,
    pos: i64,
    len: i64,

    slice_size: i64,

    read_detector: ReadDetector,
}

impl FsReaderBuffer {
    /// Coalesced sequential transfer target for background prefetch frames.
    const SEQ_FETCH_TARGET: i64 = 1024 * 1024;

    pub fn new(
        path: Path,
        fs_context: Arc<FsContext>,
        file_blocks: FileBlocks,
        read_detector: ReadDetector,
    ) -> FsResult<Self> {
        let rt = fs_context.clone_runtime();
        let err_monitor = Arc::new(ErrorMonitor::new());

        let conf = &fs_context.conf.client;
        let chunk_num = conf.read_chunk_num;
        let chunk_size = conf.read_chunk_size;
        let slice_size = conf.read_slice_size;

        // Byte-budgeted prefetch window: with large frames the message-count
        // channel capacity alone would let one lane buffer
        // read_chunk_num * target bytes. Keep the window at the historical
        // read_chunk_size * read_chunk_num bytes, and cap the coalesced frame
        // target at half the window so the producer can fetch the next frame
        // while the consumer still holds the current one (pipeline depth 2
        // frames per lane; without it every frame boundary stalls the
        // foreground reader until the next fetch completes).
        let chunk_len = chunk_size as i64;
        let window_bytes = chunk_len.saturating_mul(chunk_num as i64).max(chunk_len);
        let seq_fetch_len = Self::SEQ_FETCH_TARGET.min(window_bytes / 2).max(chunk_len);

        let pos = 0;
        let len = file_blocks.status.len;

        let base = FsReaderParallel::from_base(
            FsReaderBase::new(path.clone(), fs_context.clone(), file_blocks.clone()),
            read_detector.read_parallel() as usize,
            slice_size,
            vec![FileSlice::new(0, len)],
            file_blocks.status.id,
        );

        let all = FsReaderParallel::create_all(
            path.clone(),
            fs_context,
            file_blocks,
            read_detector.read_parallel(),
            slice_size,
            chunk_size,
        )?;

        let mut readers = Vec::with_capacity(all.len() + 1);
        for reader in all {
            let reader = if chunk_num == 1 {
                ReaderAdapter::Base(reader)
            } else {
                let (chunk_sender, chunk_receiver) = AsyncChannel::new(chunk_num).split();
                let (task_sender, task_receiver) = AsyncChannel::new(2).split();
                let budget = Arc::new(ByteBudget::new(window_bytes));
                let channel = BufferChannel {
                    prefetch: Some(PrefetchArgs {
                        rt: rt.clone(),
                        reader,
                        chunk_sender,
                        task_receiver,
                        budget: budget.clone(),
                        initial_len: chunk_len,
                        chunk_size: chunk_len,
                        target: seq_fetch_len,
                    }),
                    chunk_receiver,
                    task_sender,
                    err_monitor: err_monitor.clone(),
                    budget,
                    held: 0,
                };
                ReaderAdapter::Buffer(channel)
            };
            readers.push(reader);
        }

        let base_reader_index = readers.len();
        readers.push(ReaderAdapter::Base(base));

        let reader = Self {
            readers,
            base_reader_index,
            path,
            pos,
            len,
            slice_size,
            read_detector,
        };
        Ok(reader)
    }

    pub fn remaining(&self) -> i64 {
        self.len - self.pos
    }

    pub fn has_remaining(&self) -> bool {
        self.remaining() > 0
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

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn select_reader_index(
        is_random: bool,
        pos: i64,
        slice_size: i64,
        read_parallel: i64,
        base_reader_index: usize,
    ) -> Option<usize> {
        if is_random {
            return Some(base_reader_index);
        }

        if slice_size <= 0 || read_parallel <= 0 {
            return None;
        }

        Some((pos / slice_size % read_parallel) as usize)
    }

    fn get_reader(&mut self) -> FsResult<&mut ReaderAdapter> {
        let Some(id) = Self::select_reader_index(
            self.read_detector.is_random(),
            self.pos,
            self.slice_size,
            self.read_detector.read_parallel(),
            self.base_reader_index,
        ) else {
            return err_box!(
                "reader is not initialized: pos={}, slice_size={}, read_parallel={}, base_reader_index={}",
                self.pos,
                self.slice_size,
                self.read_detector.read_parallel(),
                self.base_reader_index
            );
        };

        match self.readers.get_mut(id) {
            Some(v) => Ok(v),
            None => err_box!("reader {} is not initialized", id),
        }
    }

    pub async fn read(&mut self) -> FsResult<DataSlice> {
        self.read_demand(0).await
    }

    /// Demand-aware read: `demand > 0` is the caller's real current demand in
    /// bytes and may be served by one large frame instead of many chunk_size
    /// frames. `demand <= 0` keeps the default frame behavior.
    pub async fn read_demand(&mut self, demand: i64) -> FsResult<DataSlice> {
        if !self.has_remaining() {
            return Ok(DataSlice::Empty);
        }

        let reader = self.get_reader()?;
        let mut chunk = reader.read_demand(demand).await?;

        // Handle data alignment issues.
        // The chunk read by the underlying reader may be aligned according to chunk_size,
        // so when returning data, you need to discard the excess data
        let diff = self.pos - chunk.off;
        let bytes = if diff == 0 {
            chunk.data
        } else if diff > 0 && diff <= chunk.len() as i64 {
            chunk.data.split_off(diff as usize)
        } else {
            return err_box!(
                "read data error: chunk offset {}, pos {}, diff {}",
                chunk.off,
                self.pos,
                diff
            );
        };

        let start_pos = self.pos;
        self.pos += bytes.len() as i64;

        let is_changed = self
            .read_detector
            .record_read(start_pos, self.pos, &self.path);

        if is_changed && self.read_detector.is_sequential() {
            for reader in &mut self.readers {
                reader.pause(self.pos, false).await?;
            }
        }

        Ok(bytes)
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos == self.pos() {
            return Ok(());
        }

        self.read_detector.record_seek(&self.path);
        for reader in &mut self.readers {
            reader.seek(pos).await?;

            if !self.read_detector.enabled {
                reader.pause(pos, false).await?;
            }
        }

        self.pos = pos;
        Ok(())
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        for reader in &mut self.readers {
            reader.complete().await?;
        }
        Ok(())
    }

    /// Producer body of one prefetch lane. `next_len` is the frame size the
    /// lane starts with (chunk_size, or the real demand that started the
    /// lane); after the first frame, background frames are coalesced to
    /// `target` bytes, each bounded by the byte budget window.
    async fn read_future(
        chunk_sender: AsyncSender<FileChunk>,
        mut task_receiver: AsyncReceiver<ReadTask>,
        budget: Arc<ByteBudget>,
        mut next_len: i64,
        chunk_size: i64,
        target: i64,
        mut reader: FsReaderParallel,
    ) -> FsResult<()> {
        let mut paused = false;
        loop {
            // Acquire a send permit AND window bytes before fetching the next
            // frame. Control tasks keep being serviced while waiting, so
            // seek/stop can never deadlock behind a full window.
            let mut permit: Option<Permit<'_, FileChunk>> = None;
            let reserved;
            {
                // Register the budget waiter BEFORE each check so a release
                // between check and wait cannot be missed.
                let notified = budget.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();

                loop {
                    if paused {
                        // Fetching was paused by a control task: release any
                        // held send permit and wait for the next control task.
                        if let Some(p) = permit.take() {
                            drop(p);
                        }
                        match task_receiver.recv().await {
                            Some(task) => {
                                if Self::handle_control(
                                    task,
                                    &mut reader,
                                    &mut paused,
                                    &mut next_len,
                                    chunk_size,
                                )
                                .await?
                                {
                                    return Ok(());
                                }
                                continue;
                            }
                            None => return Ok(()), // control channel closed while paused
                        }
                    }

                    if permit.is_none() {
                        tokio::select! {
                            biased;

                            task_opt = task_receiver.recv() => {
                                match task_opt {
                                    Some(task) => {
                                        if Self::handle_control(
                                            task,
                                            &mut reader,
                                            &mut paused,
                                            &mut next_len,
                                            chunk_size,
                                        )
                                        .await?
                                        {
                                            return Ok(());
                                        }
                                        continue;
                                    }
                                    None => return Ok(()), // control channel closed: normal shutdown
                                }
                            }

                            permit_res = chunk_sender.reserve() => {
                                match permit_res {
                                    Ok(p) => permit = Some(p),
                                    Err(_e) => return Ok(()), // data channel closed: normal shutdown
                                }
                            }
                        }
                    }

                    // Permit held: reserve window bytes. The floor is
                    // chunk_size so a fragmented window still admits a
                    // default-sized frame.
                    if let Some(take) = budget.try_acquire(next_len, next_len.min(chunk_size)) {
                        reserved = take;
                        break;
                    }

                    // Window full: wait for a consumer release or a control
                    // task. Holding the permit here is safe — the consumer
                    // frees budget exactly by draining the queue.
                    tokio::select! {
                        biased;

                        task_opt = task_receiver.recv() => {
                            match task_opt {
                                Some(task) => {
                                    if Self::handle_control(
                                        task,
                                        &mut reader,
                                        &mut paused,
                                        &mut next_len,
                                        chunk_size,
                                    )
                                    .await?
                                    {
                                        return Ok(());
                                    }
                                    continue;
                                }
                                None => return Ok(()), // control channel closed: normal shutdown
                            }
                        }

                        _ = &mut notified => {
                            notified.set(budget.notify.notified());
                            notified.as_mut().enable();
                        }
                    }
                }
            }

            let permit = match permit {
                Some(p) => p,
                // paused is guaranteed false here (paused branch never yields a Permit)
                None => unreachable!("acquired budget without a permit"),
            };

            let chunk = reader.read_with_len(reserved).await?;
            if chunk.is_empty() {
                paused = true;
            }
            // Return over-reservation (short frame, EOF, or a legacy worker
            // falling back to its fixed frame) so the window does not leak.
            let slack = reserved - chunk.len() as i64;
            if slack > 0 {
                budget.release(slack);
            }
            // Send the chunk (possibly empty) so the reader side does not block.
            permit.send(chunk);

            // After the first frame, coalesce background prefetch to target.
            if !paused {
                next_len = target;
            }
        }
    }

    /// Handle one control task. Returns `true` when the lane must stop.
    async fn handle_control(
        task: ReadTask,
        reader: &mut FsReaderParallel,
        paused: &mut bool,
        next_len: &mut i64,
        chunk_size: i64,
    ) -> FsResult<bool> {
        match task {
            ReadTask::Seek(pos, tx) => {
                // 1. reader executes seek
                // 2. Set paused = true
                // 3. The notification pause was successful
                *paused = true;
                *next_len = chunk_size;
                reader.seek(pos).await?;
                tx.send(1)?;
            }

            ReadTask::Pause((pos, v)) => {
                *paused = v;
                *next_len = chunk_size;
                reader.seek(pos).await?;
            }

            ReadTask::Stop(tx) => {
                reader.complete().await?;
                tx.send(1)?;
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_config::ClusterConf;
    use curvine_model::FileStatus;

    #[test]
    fn test_byte_budget_try_acquire_release() {
        let budget = ByteBudget::new(1000);

        assert_eq!(budget.try_acquire(600, 600), Some(600));
        // Partial grant: only 400 bytes free, floor 400 is satisfied.
        assert_eq!(budget.try_acquire(500, 400), Some(400));
        // Window exhausted: even a default-sized frame cannot be admitted.
        assert_eq!(budget.try_acquire(500, 400), None);
        assert_eq!(budget.try_acquire(100, 100), None);

        // Releasing consumed bytes unblocks a floor-sized frame.
        budget.release(400);
        assert_eq!(budget.try_acquire(500, 400), Some(400));

        // Zero release is a no-op.
        budget.release(0);
    }

    fn sparse_file_blocks(len: i64) -> FileBlocks {
        FileBlocks::new(
            FileStatus {
                id: 1,
                len,
                is_complete: true,
                ..Default::default()
            },
            vec![],
        )
    }

    #[test]
    fn test_random_read_uses_base_reader_for_sparse_parallel_slices() {
        let mut conf = ClusterConf::default();
        conf.client.read_parallel = 4;
        conf.client.read_chunk_num = 1;
        conf.client.read_slice_size_str = "16MB".to_string();
        conf.client.large_file_size_str = "1GB".to_string();
        conf.client.init().unwrap();

        let file_len = conf.client.read_slice_size / 2;
        let file_blocks = sparse_file_blocks(file_len);
        let path = Path::from_str("/small-file").unwrap();
        let read_detector = ReadDetector::with_conf(&conf.client, file_len);
        let fs_context = Arc::new(FsContext::new(conf).unwrap());
        let rt = fs_context.clone_runtime();
        let mut reader = FsReaderBuffer::new(path, fs_context, file_blocks, read_detector).unwrap();

        assert_eq!(reader.base_reader_index, 1);

        rt.block_on(reader.seek(file_len)).unwrap();
        assert!(reader.read_detector.is_random());
        assert!(reader.get_reader().is_ok());
    }
}
