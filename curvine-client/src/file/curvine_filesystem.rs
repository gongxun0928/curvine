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

use crate::block::BatchBlockWriter;
use crate::file::{
    BatchAddBlockRequest, BatchCompleteFileRequest, FsClient, FsContext, FsReader, FsWriter,
    FsWriterBase,
};
use crate::ClientMetrics;
use async_stream::stream;
use bytes::BytesMut;
use curvine_common::alloc::allocator_type_name;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, FsKind, ListStream, Path, Reader, Writer};
use curvine_common::state::{CommitBlock, FreeResult, ListOptions, LocatedBlock, WorkerAddress};
use curvine_common::state::{
    CreateFileOpts, CreateFileOptsBuilder, FileAllocOpts, FileBlocks, FileLock, FileStatus,
    MasterInfo, MkdirOpts, MkdirOptsBuilder, MountInfo, MountOptions, OpenFlags, SetAttrOpts,
};
use curvine_common::utils::ProtoUtils;
use curvine_common::version::GIT_VERSION;
use curvine_common::FsResult;
use futures::future::join_all;
use log::info;
use log::warn;
use orpc::client::ClientConf;
use orpc::err_box;
use orpc::runtime::{RpcRuntime, Runtime};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

#[derive(Clone)]
pub struct CurvineFileSystem {
    pub(crate) fs_context: Arc<FsContext>,
    pub(crate) fs_client: Arc<FsClient>,
}

const MAX_BATCH_FILES: usize = 1024;

struct BatchFileItem<'a> {
    input_index: usize,
    path: &'a Path,
    content: &'a str,
    inode_id: Option<i64>,
    block: Option<LocatedBlock>,
    error: Option<FsError>,
}

impl BatchFileItem<'_> {
    fn is_active(&self) -> bool {
        self.error.is_none()
    }

    fn fail(&mut self, stage: &str, error: impl std::fmt::Display) {
        if self.error.is_none() {
            self.error = Some(FsError::common(format!(
                "batch {} failed for {}: {}",
                stage, self.path, error
            )));
        }
    }
}

struct WorkerBatchPlan {
    worker: WorkerAddress,
    item_indices: Vec<usize>,
    payload_len: usize,
}

struct WorkerBatchWriter {
    worker: WorkerAddress,
    item_indices: Vec<usize>,
    writer: BatchBlockWriter,
}

impl CurvineFileSystem {
    pub fn with_rt(conf: ClusterConf, rt: Arc<Runtime>) -> FsResult<Self> {
        let fs_context = Arc::new(FsContext::with_rt(conf, rt.clone())?);
        let fs_client = FsClient::new(fs_context.clone());
        let fs = Self {
            fs_context,
            fs_client: Arc::new(fs_client),
        };

        FsContext::start_clean_task(fs.clone(), fs.fs_context.block_pool.clone());

        let c = &fs.conf().client;
        info!(
            "Create new filesystem, git version: {}, allocator: {}, masters: {}, threads: {}-{}, \
            buffer(rw): {}-{}, conn timeout(ms): {}-{}, rpc timeout(ms): {}-{}, data timeout(ms): {}",
            GIT_VERSION,
            allocator_type_name(),
            fs.conf().masters_string(),
            rt.io_threads(),
            rt.worker_threads(),
            c.read_chunk_size,
            c.write_chunk_size,
            c.conn_timeout_ms,
            c.conn_retry_max_duration_ms,
            c.rpc_timeout_ms,
            c.rpc_retry_max_duration_ms,
            c.data_timeout_ms
        );

        Ok(fs)
    }

    pub fn conf(&self) -> &ClusterConf {
        &self.fs_context.conf
    }

    pub fn rpc_conf(&self) -> &ClientConf {
        self.fs_context.rpc_conf()
    }

    pub async fn mkdir_with_opts(&self, path: &Path, opts: MkdirOpts) -> FsResult<FileStatus> {
        self.fs_client.mkdir(path, opts).await
    }

    pub async fn mkdir(&self, path: &Path, create_parent: bool) -> FsResult<bool> {
        let opts = MkdirOptsBuilder::with_conf(&self.fs_context.conf.client)
            .create_parent(create_parent)
            .build();
        match self.mkdir_with_opts(path, opts).await {
            Ok(_) => Ok(true),
            Err(e) => {
                if matches!(e, FsError::FileAlreadyExists(_)) {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    pub async fn create_with_opts(
        &self,
        path: &Path,
        opts: CreateFileOpts,
        overwrite: bool,
    ) -> FsResult<FsWriter> {
        let status = self
            .fs_client
            .create_with_opts(path, opts, overwrite)
            .await?;
        let file_blocks = FileBlocks::new(status, vec![]);
        let writer = FsWriter::create(self.fs_context.clone(), path.clone(), file_blocks);
        Ok(writer)
    }

    pub fn create_opts_builder(&self) -> CreateFileOptsBuilder {
        CreateFileOptsBuilder::with_conf(&self.fs_context.conf.client)
            .client_name(self.fs_context.clone_client_name())
    }

    pub async fn create(&self, path: &Path, overwrite: bool) -> FsResult<FsWriter> {
        let opts = self.create_opts_builder().create_parent(true).build();
        self.create_with_opts(path, opts, overwrite).await
    }

    pub async fn append(&self, path: &Path) -> FsResult<FsWriter> {
        let opts = self.create_opts_builder().create_parent(false).build();
        let flags = OpenFlags::new_append().set_create(true);
        self.open_with_opts(path, opts, flags).await
    }

    pub async fn exists(&self, path: &Path) -> FsResult<bool> {
        self.fs_client.exists(path).await
    }

    pub async fn open(&self, path: &Path) -> FsResult<FsReader> {
        let file_blocks = self.fs_client.get_block_locations(path).await?;

        let reader = FsReader::new(path.clone(), self.fs_context.clone(), file_blocks)?;
        Ok(reader)
    }

    pub async fn open_for_write(&self, path: &Path, overwrite: bool) -> FsResult<FsWriter> {
        let create_opts = self.create_opts_builder().create_parent(true).build();
        let flags = OpenFlags::new_write_only()
            .set_create(true)
            .set_overwrite(overwrite);
        self.open_with_opts(path, create_opts, flags).await
    }

    pub async fn open_with_opts(
        &self,
        path: &Path,
        opts: CreateFileOpts,
        flags: OpenFlags,
    ) -> FsResult<FsWriter> {
        let file_block = self.fs_client.open_with_opts(path, opts, flags).await?;
        let writer = FsWriter::new(
            self.fs_context.clone(),
            path.clone(),
            file_block,
            flags.append(),
        );
        Ok(writer)
    }

    pub async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        self.fs_client.rename(src, dst).await
    }

    pub async fn delete(&self, path: &Path, recursive: bool) -> FsResult<()> {
        self.fs_client.delete(path, recursive).await
    }

    pub async fn free(&self, path: &Path, recursive: bool) -> FsResult<FreeResult> {
        self.fs_client.free(path, recursive).await
    }

    pub async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        self.fs_client.file_status(path).await
    }

    pub async fn get_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        self.fs_client.file_status_bytes(path).await
    }

    pub async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        self.fs_client.list_status(path).await
    }

    pub async fn list_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        self.fs_client.list_status_bytes(path).await
    }

    pub async fn list_options(&self, path: &Path, opts: ListOptions) -> FsResult<Vec<FileStatus>> {
        self.fs_client.list_options(path, opts).await
    }

    pub async fn list_stream(&self, path: &Path, options: ListOptions) -> FsResult<ListStream> {
        let fs = self.clone();
        let path = path.clone();
        let (limit, mut start_after) = (options.limit, options.start_after);

        let stream = stream! {
            loop {
                let options = ListOptions {
                    limit,
                    start_after: start_after.clone(),
                };
                let list = match fs.list_options(&path, options).await {
                    Ok(p) => p,
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                };

                if list.is_empty() {
                    break;
                }

                let n = list.len();
                let last_name = list.last().map(|s| s.name.clone());
                for status in list {
                    yield Ok(status);
                }

                if let Some(l) = limit {
                    if n < l {
                        break;
                    }
                }
                start_after = last_name;
            }
        };

        Ok(ListStream::new(stream))
    }

    pub async fn list_options_bytes(&self, path: &Path, opts: ListOptions) -> FsResult<BytesMut> {
        self.fs_client.list_options_bytes(path, opts).await
    }

    pub async fn list_files(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        self.fs_client.list_files(path).await
    }

    pub async fn get_block_locations(&self, path: &Path) -> FsResult<FileBlocks> {
        self.fs_client.get_block_locations(path).await
    }

    pub async fn get_master_info(&self) -> FsResult<MasterInfo> {
        self.fs_client.get_master_info().await
    }

    pub async fn get_master_info_bytes(&self) -> FsResult<BytesMut> {
        self.fs_client.get_master_info_bytes().await
    }

    pub async fn get_mount_table(&self) -> FsResult<Vec<MountInfo>> {
        let res = self.fs_client.get_mount_table().await?;
        let table = res
            .mount_table
            .into_iter()
            .map(ProtoUtils::mount_info_from_pb)
            .collect();

        Ok(table)
    }

    pub async fn mount(&self, ufs_path: &Path, cv_path: &Path, opts: MountOptions) -> FsResult<()> {
        if !opts.update && ufs_path.scheme().is_none() {
            return err_box!("ufs path {} invalid must be start with schema://", ufs_path);
        }
        if cv_path.is_root() {
            return err_box!("mount path can not be root");
        }

        self.fs_client.mount(ufs_path, cv_path, opts).await?;
        Ok(())
    }

    pub async fn umount(&self, cv_path: &Path) -> FsResult<()> {
        self.fs_client.umount(cv_path).await?;
        Ok(())
    }

    pub async fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> FsResult<FileStatus> {
        self.fs_client.set_attr(path, opts).await
    }

    pub async fn symlink(&self, target: &str, link: &Path, force: bool) -> FsResult<()> {
        self.fs_client.symlink(target, link, force).await
    }

    pub async fn link(&self, src_path: &Path, dst_path: &Path) -> FsResult<()> {
        self.fs_client.link(src_path, dst_path).await
    }

    pub async fn get_mount_info(&self, path: &Path) -> FsResult<Option<MountInfo>> {
        self.fs_client.get_mount_info(path).await
    }

    pub async fn get_mount_info_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        self.fs_client.get_mount_info_bytes(path).await
    }

    pub async fn resize(&self, path: &Path, opts: FileAllocOpts) -> FsResult<()> {
        let create_opts = self.create_opts_builder().create_parent(true).build();
        let flags = OpenFlags::new_write_only()
            .set_create(true)
            .set_overwrite(false);
        let file_blocks = self
            .fs_client
            .open_with_opts(path, create_opts, flags)
            .await?;

        let mut writer = FsWriterBase::new(self.fs_context.clone(), path.clone(), file_blocks, 0);
        writer.resize(opts).await?;
        writer.complete().await?;

        Ok(())
    }

    pub async fn get_lock(&self, path: &Path, lock: FileLock) -> FsResult<Option<FileLock>> {
        self.fs_client.get_lock(path, lock).await
    }

    pub async fn set_lock(&self, path: &Path, lock: FileLock) -> FsResult<Option<FileLock>> {
        self.fs_client.set_lock(path, lock).await
    }

    pub fn clone_runtime(&self) -> Arc<Runtime> {
        self.fs_context.clone_runtime()
    }

    pub fn fs_client(&self) -> Arc<FsClient> {
        self.fs_client.clone()
    }

    pub fn fs_context(&self) -> Arc<FsContext> {
        self.fs_context.clone()
    }

    pub async fn read_string(&self, path: &Path) -> FsResult<String> {
        let mut reader = self.open(path).await?;

        let len = reader.len() as usize;
        let mut buf = BytesMut::zeroed(len);

        reader.read_full(&mut buf).await?;
        reader.complete().await?;
        Ok(String::from_utf8_lossy(&buf).to_string())
    }

    pub async fn metrics_report(&self) -> FsResult<()> {
        let metrics = ClientMetrics::encode()?;
        self.fs_client.metrics_report(metrics).await
    }

    pub async fn write_string(&self, path: &Path, str: impl AsRef<str>) -> FsResult<()> {
        let mut writer = self.create(path, true).await?;
        writer.write(str.as_ref().as_bytes()).await?;
        writer.complete().await?;
        Ok(())
    }

    pub async fn append_string(&self, path: &Path, str: impl AsRef<str>) -> FsResult<()> {
        let mut writer = self.append(path).await?;
        writer.write(str.as_ref().as_bytes()).await?;
        writer.complete().await?;
        Ok(())
    }

    // close fs, report metrics
    pub async fn cleanup(&self) {
        let res = timeout(
            Duration::from_secs(self.conf().client.close_timeout_secs),
            self.metrics_report(),
        )
        .await;
        if let Err(e) = res {
            warn!("close {}", e);
        }
    }

    /// Writes files independently while batching the one-block small-file path.
    ///
    /// The outer result is reserved for structurally invalid input. Duplicate
    /// paths are rejected because a batch does not define ordering between
    /// multiple writes to the same inode. Each inner result maps to the file at
    /// the same input index. A stage-wide failure is copied only to the items
    /// affected by that stage, so outcomes already determined for unrelated
    /// files remain visible to the caller.
    pub async fn write_batch_string(&self, files: &[(Path, &str)]) -> FsResult<Vec<FsResult<()>>> {
        let mut paths = HashSet::with_capacity(files.len());
        for (path, _) in files {
            if !paths.insert(path.encode()) {
                return err_box!("duplicate path in batch write: {}", path);
            }
        }

        let batch_file_size = self
            .conf()
            .client
            .small_file_size
            .max(0)
            .min(self.fs_context.block_size().max(0)) as usize;
        let batch_file_size = batch_file_size.min(self.fs_context.write_chunk_size());
        let mut outcomes: Vec<Option<FsResult<()>>> =
            std::iter::repeat_with(|| None).take(files.len()).collect();
        let mut batch = Vec::with_capacity(files.len().min(MAX_BATCH_FILES));

        for (input_index, (path, content)) in files.iter().enumerate() {
            if content.is_empty() || content.len() <= batch_file_size {
                batch.push((input_index, path, *content));
                if batch.len() == MAX_BATCH_FILES {
                    self.finish_batch_items(&mut outcomes, &batch).await;
                    batch.clear();
                }
            } else {
                // Preserve input order across the batch/standalone boundary.
                // Earlier small files may create paths that affect this write.
                self.finish_batch_items(&mut outcomes, &batch).await;
                batch.clear();
                outcomes[input_index] = Some(self.write_string(path, *content).await);
            }
        }
        self.finish_batch_items(&mut outcomes, &batch).await;

        outcomes
            .into_iter()
            .enumerate()
            .map(|(index, outcome)| {
                outcome.ok_or_else(|| {
                    FsError::common(format!("missing batch write outcome at index {}", index))
                })
            })
            .collect()
    }

    async fn finish_batch_items(
        &self,
        outcomes: &mut [Option<FsResult<()>>],
        files: &[(usize, &Path, &str)],
    ) {
        for (input_index, result) in self.handle_batch_files(files).await {
            outcomes[input_index] = Some(result);
        }
    }

    fn group_batch_files_by_worker(
        items: &[BatchFileItem<'_>],
        payload_limit: usize,
    ) -> Vec<WorkerBatchPlan> {
        let mut plans: Vec<WorkerBatchPlan> = Vec::new();
        let mut latest_plan_indices: HashMap<WorkerAddress, usize> = HashMap::new();
        for (item_index, item) in items.iter().enumerate() {
            let Some(block) = item.block.as_ref().filter(|_| item.is_active()) else {
                continue;
            };
            for worker in &block.locs {
                let next_payload = item.content.len();
                let existing_index = latest_plan_indices.get(worker).copied().filter(|index| {
                    let plan = &plans[*index];
                    plan.item_indices.len() < MAX_BATCH_FILES
                        && plan.payload_len.saturating_add(next_payload) <= payload_limit
                });
                if let Some(index) = existing_index {
                    let plan = &mut plans[index];
                    plan.item_indices.push(item_index);
                    plan.payload_len += next_payload;
                } else {
                    let index = plans.len();
                    plans.push(WorkerBatchPlan {
                        worker: worker.clone(),
                        item_indices: vec![item_index],
                        payload_len: next_payload,
                    });
                    latest_plan_indices.insert(worker.clone(), index);
                }
            }
        }
        plans
    }

    fn fail_worker_items(
        items: &mut [BatchFileItem<'_>],
        indices: &[usize],
        stage: &str,
        worker: &WorkerAddress,
        error: impl std::fmt::Display,
    ) {
        for index in indices {
            items[*index].fail(stage, format_args!("worker {}: {}", worker, error));
        }
    }

    async fn handle_batch_files(
        &self,
        files: &[(usize, &Path, &str)],
    ) -> Vec<(usize, FsResult<()>)> {
        if files.is_empty() {
            return Vec::new();
        }

        let mut items = files
            .iter()
            .map(|(input_index, path, content)| BatchFileItem {
                input_index: *input_index,
                path,
                content,
                inode_id: None,
                block: None,
                error: None,
            })
            .collect::<Vec<_>>();
        let create_requests = items
            .iter()
            .map(|item| {
                let opts = self.create_opts_builder().create_parent(true).build();
                let flags = OpenFlags::new_write_only()
                    .set_create(true)
                    .set_overwrite(true);
                (item.path.encode(), opts, flags)
            })
            .collect();

        match self.fs_client.create_files_batch(create_requests).await {
            Ok(results) => {
                for (item, result) in items.iter_mut().zip(results) {
                    match result {
                        Ok(status) => item.inode_id = Some(status.id),
                        Err(error) => item.fail("create", error),
                    }
                }
            }
            Err(error) => {
                for item in &mut items {
                    item.fail("create", &error);
                }
                return items
                    .into_iter()
                    .map(|item| (item.input_index, Err(item.error.unwrap())))
                    .collect();
            }
        }

        let add_indices = items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| {
                (item.is_active() && !item.content.is_empty()).then_some(index)
            })
            .collect::<Vec<_>>();
        let add_requests = add_indices
            .iter()
            .map(|index| {
                let item = &items[*index];
                BatchAddBlockRequest {
                    path: (*item.path).clone(),
                    inode_id: item
                        .inode_id
                        .expect("active batch file must preserve its inode id"),
                    commit_blocks: Vec::new(),
                    file_len: 0,
                    last_block: None,
                }
            })
            .collect::<Vec<_>>();
        if !add_requests.is_empty() {
            match self.fs_client.add_blocks_batch(add_requests).await {
                Ok(results) => {
                    for (index, result) in add_indices.iter().zip(results) {
                        match result {
                            Ok(block) if block.locs.is_empty() => {
                                items[*index].fail("allocate block", "no available worker")
                            }
                            Ok(block) => items[*index].block = Some(block),
                            Err(error) => items[*index].fail("allocate block", error),
                        }
                    }
                }
                Err(error) => {
                    for index in &add_indices {
                        items[*index].fail("allocate block", &error);
                    }
                }
            }
        }

        let plans =
            Self::group_batch_files_by_worker(&items, self.fs_context.write_chunk_size().max(1));
        let mut writers = Vec::with_capacity(plans.len());
        for plan in plans {
            let blocks = plan
                .item_indices
                .iter()
                .filter_map(|index| {
                    items[*index]
                        .block
                        .as_ref()
                        .map(|block| block.block.clone())
                })
                .collect::<Vec<_>>();
            match BatchBlockWriter::new(self.fs_context.clone(), blocks, plan.worker.clone()).await
            {
                Ok((writer, results)) => {
                    for (index, success) in plan.item_indices.iter().zip(results) {
                        if !success {
                            items[*index].fail(
                                "open block",
                                format_args!("worker {} rejected the block", plan.worker),
                            );
                        }
                    }
                    writers.push(WorkerBatchWriter {
                        worker: plan.worker,
                        item_indices: plan.item_indices,
                        writer,
                    });
                }
                Err(error) => {
                    self.fs_context.add_failed_worker(&plan.worker);
                    Self::fail_worker_items(
                        &mut items,
                        &plan.item_indices,
                        "open block",
                        &plan.worker,
                        error,
                    );
                }
            }
        }

        let write_results = join_all(writers.iter_mut().map(|group| {
            let active = group
                .item_indices
                .iter()
                .map(|index| items[*index].is_active())
                .collect::<Vec<_>>();
            let group_files = group
                .item_indices
                .iter()
                .map(|index| (items[*index].path, items[*index].content))
                .collect::<Vec<_>>();
            async move {
                let result = group.writer.write(&group_files, &active).await;
                (active, result)
            }
        }))
        .await;
        for (group, (active, result)) in writers.iter().zip(write_results) {
            match result {
                Ok(results) => {
                    for ((index, was_active), success) in
                        group.item_indices.iter().zip(active).zip(results)
                    {
                        if was_active && !success {
                            items[*index].fail(
                                "write block",
                                format_args!("worker {} rejected the data", group.worker),
                            );
                        }
                    }
                }
                Err(error) => {
                    self.fs_context.add_failed_worker(&group.worker);
                    Self::fail_worker_items(
                        &mut items,
                        &group.item_indices,
                        "write block",
                        &group.worker,
                        error,
                    );
                }
            }
        }

        let complete_results = join_all(writers.iter_mut().map(|group| {
            let cancels = group
                .item_indices
                .iter()
                .map(|index| !items[*index].is_active())
                .collect::<Vec<_>>();
            async move {
                let result = group.writer.complete(&cancels).await;
                (cancels, result)
            }
        }))
        .await;
        for (group, (cancels, result)) in writers.iter().zip(complete_results) {
            match result {
                Ok(results) => {
                    for ((index, cancel), success) in
                        group.item_indices.iter().zip(cancels).zip(results)
                    {
                        if !cancel && !success {
                            items[*index].fail(
                                "complete block",
                                format_args!("worker {} rejected the block", group.worker),
                            );
                        } else if cancel && !success {
                            log::warn!(
                                "failed to cancel batch block for {} on worker {}",
                                items[*index].path,
                                group.worker
                            );
                        }
                    }
                }
                Err(error) => {
                    self.fs_context.add_failed_worker(&group.worker);
                    for (index, cancel) in group.item_indices.iter().zip(cancels) {
                        if cancel {
                            log::warn!(
                                "failed to cancel batch block for {} on worker {}: {}",
                                items[*index].path,
                                group.worker,
                                error
                            );
                        } else {
                            items[*index].fail(
                                "complete block",
                                format_args!("worker {}: {}", group.worker, error),
                            );
                        }
                    }
                }
            }
        }

        let complete_indices = items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| item.is_active().then_some(index))
            .collect::<Vec<_>>();
        let complete_requests = complete_indices
            .iter()
            .map(|index| {
                let item = &items[*index];
                let commit_blocks = item
                    .block
                    .as_ref()
                    .map(|block| {
                        let mut commit = CommitBlock::from(block);
                        commit.block_len = item.content.len() as i64;
                        vec![commit]
                    })
                    .unwrap_or_default();
                BatchCompleteFileRequest {
                    path: (*item.path).clone(),
                    inode_id: item
                        .inode_id
                        .expect("active batch file must preserve its inode id"),
                    len: item.content.len() as i64,
                    commit_blocks,
                    only_flush: false,
                }
            })
            .collect::<Vec<_>>();
        if !complete_requests.is_empty() {
            match self.fs_client.complete_files_batch(complete_requests).await {
                Ok(results) => {
                    for (index, result) in complete_indices.iter().zip(results) {
                        if let Err(error) = result {
                            items[*index].fail("complete file", error);
                        }
                    }
                }
                Err(error) => {
                    for index in &complete_indices {
                        items[*index].fail("complete file", &error);
                    }
                }
            }
        }

        // Best-effort cleanup for files that were created but never completed.
        // Keep the original per-file error as the caller-visible outcome.
        for item in &items {
            if item.inode_id.is_none() || item.is_active() {
                continue;
            }
            if let Err(error) = self.fs_client.delete(item.path, false).await {
                log::warn!(
                    "failed to cleanup incomplete batch file {}: {}",
                    item.path,
                    error
                );
            }
        }

        items
            .into_iter()
            .map(|item| {
                let result = item.error.map_or(Ok(()), Err);
                (item.input_index, result)
            })
            .collect()
    }
}

impl FileSystem<FsWriter, FsReader> for CurvineFileSystem {
    fn fs_kind(&self) -> FsKind {
        FsKind::Cv
    }

    async fn mkdir(&self, path: &Path, create_parent: bool) -> FsResult<bool> {
        self.mkdir(path, create_parent).await
    }

    async fn create(&self, path: &Path, overwrite: bool) -> FsResult<FsWriter> {
        self.create(path, overwrite).await
    }

    async fn append(&self, path: &Path) -> FsResult<FsWriter> {
        self.append(path).await
    }

    async fn exists(&self, path: &Path) -> FsResult<bool> {
        self.exists(path).await
    }

    async fn open(&self, path: &Path) -> FsResult<FsReader> {
        self.open(path).await
    }

    async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        self.rename(src, dst).await
    }

    async fn delete(&self, path: &Path, recursive: bool) -> FsResult<()> {
        self.delete(path, recursive).await
    }

    async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        self.get_status(path).await
    }

    async fn get_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        self.get_status_bytes(path).await
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        self.list_status(path).await
    }

    async fn list_status_bytes(&self, path: &Path) -> FsResult<BytesMut> {
        self.list_status_bytes(path).await
    }

    async fn set_attr(&self, path: &Path, opts: SetAttrOpts) -> FsResult<()> {
        self.set_attr(path, opts).await?;
        Ok(())
    }

    async fn list_options(&self, path: &Path, opts: ListOptions) -> FsResult<Vec<FileStatus>> {
        self.list_options(path, opts).await
    }

    async fn list_options_bytes(&self, path: &Path, opts: ListOptions) -> FsResult<BytesMut> {
        self.list_options_bytes(path, opts).await
    }

    async fn list_stream(&self, path: &Path, opts: ListOptions) -> FsResult<ListStream> {
        self.list_stream(path, opts).await
    }
}

#[cfg(test)]
mod batch_tests {
    use super::*;
    use curvine_common::state::{ExtendedBlock, FileType, StorageType};

    fn worker(worker_id: u32) -> WorkerAddress {
        WorkerAddress {
            worker_id,
            ..Default::default()
        }
    }

    fn item<'a>(
        input_index: usize,
        path: &'a Path,
        content: &'a str,
        block_id: i64,
        locs: Vec<WorkerAddress>,
    ) -> BatchFileItem<'a> {
        BatchFileItem {
            input_index,
            path,
            content,
            inode_id: Some(input_index as i64 + 1),
            block: Some(LocatedBlock {
                block: ExtendedBlock::new(block_id, 0, StorageType::Disk, FileType::File),
                locs,
            }),
            error: None,
        }
    }

    #[test]
    fn groups_by_worker_and_splits_at_payload_limit() {
        let worker_1 = worker(1);
        let worker_2 = worker(2);
        let paths = [
            Path::from_str("/batch/0").unwrap(),
            Path::from_str("/batch/1").unwrap(),
            Path::from_str("/batch/2").unwrap(),
        ];
        let items = vec![
            item(
                0,
                &paths[0],
                "aaaa",
                10,
                vec![worker_1.clone(), worker_2.clone()],
            ),
            item(1, &paths[1], "bbbb", 11, vec![worker_1.clone()]),
            item(2, &paths[2], "ccccc", 12, vec![worker_1.clone()]),
        ];

        let plans = CurvineFileSystem::group_batch_files_by_worker(&items, 8);

        assert_eq!(plans.len(), 3);
        assert_eq!(plans[0].worker, worker_1);
        assert_eq!(plans[0].item_indices, vec![0, 1]);
        assert_eq!(plans[0].payload_len, 8);
        assert_eq!(plans[1].worker, worker_2);
        assert_eq!(plans[1].item_indices, vec![0]);
        assert_eq!(plans[2].worker, worker_1);
        assert_eq!(plans[2].item_indices, vec![2]);
    }
}
