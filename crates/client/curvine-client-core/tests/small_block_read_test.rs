// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use bytes::Bytes;
use curvine_client_core::block::{BlockReader, BlockReaderRemote};
use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_error::FsError;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, LocatedBlock, WorkerAddress};
use curvine_proto::{BlockReadRequest, BlockReadResponse, DataHeaderProto};
use curvine_rpc::handler::{Frame, RpcFrame};
use curvine_rpc::message::{Builder, Message, RequestStatus};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};

#[derive(Default)]
struct Observed {
    contents: Vec<u8>,
    calls: Vec<(i64, i32, RequestStatus)>,
    offsets: Vec<i64>,
    bytes_sent: usize,
    connections: usize,
    fail_open: Arc<AtomicBool>,
    fail_read: bool,
    fail_complete: bool,
}

struct Session {
    req_id: i64,
    seq_id: i32,
    offset: usize,
    end: usize,
    chunk: usize,
}

struct TestWorker {
    address: WorkerAddress,
    observed: Arc<Mutex<Observed>>,
    task: JoinHandle<()>,
}

impl Drop for TestWorker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TestWorker {
    async fn start(legacy: bool, size: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port() as u32;
        let address = WorkerAddress {
            worker_id: port,
            hostname: "127.0.0.1".into(),
            ip_addr: "127.0.0.1".into(),
            rpc_port: port,
            web_port: 0,
        };
        let observed = Arc::new(Mutex::new(Observed {
            contents: vec![7; size],
            ..Observed::default()
        }));
        let state = observed.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        stream.set_nodelay(true).unwrap();
                        state.lock().unwrap().connections += 1;
                        let state = state.clone();
                        connections.spawn(async move {
                            let mut frame = RpcFrame::with_client(stream, 4096);
                            let mut session = None;
                            while let Ok(request) = frame.receive().await {
                                if request.is_empty() {
                                    break;
                                }
                                let response = Self::respond(
                                    legacy, &request, &mut session, &mut state.lock().unwrap(),
                                );
                                if frame.send(response).await.is_err() {
                                    break;
                                }
                            }
                        });
                    }
                    result = connections.join_next(), if !connections.is_empty() => {
                        result.unwrap().unwrap();
                    }
                }
            }
        });
        Self {
            address,
            observed,
            task,
        }
    }

    fn respond(
        legacy: bool,
        request: &Message,
        session: &mut Option<Session>,
        state: &mut Observed,
    ) -> Message {
        state
            .calls
            .push((request.req_id(), request.seq_id(), request.request_status()));
        match request.request_status() {
            RequestStatus::Open => {
                assert!(session.is_none(), "previous legacy session must be closed");
                assert_eq!(request.seq_id(), 0);
                let header: BlockReadRequest = request.parse_header().unwrap();
                state.offsets.push(header.off);
                if state.fail_open.swap(false, Ordering::SeqCst) {
                    return request.error_ext(&FsError::block_not_found(header.id));
                }
                let builder = Builder::success(request).proto_header(BlockReadResponse {
                    id: header.id,
                    len: header.len,
                    path: None,
                    storage_type: 0,
                });
                let opened = Session {
                    req_id: request.req_id(),
                    seq_id: request.seq_id(),
                    offset: header.off as usize,
                    end: header.len as usize,
                    chunk: header.chunk_size as usize,
                };
                if !legacy && header.read_once.unwrap_or(false) {
                    let data = Self::data(state, &opened);
                    builder.request(RequestStatus::Complete).data(data).build()
                } else {
                    *session = Some(opened);
                    builder.build()
                }
            }
            RequestStatus::Running | RequestStatus::Complete => {
                let opened = session.as_mut().unwrap();
                assert_eq!(request.req_id(), opened.req_id);
                assert_eq!(request.seq_id(), opened.seq_id + 1);
                opened.seq_id = request.seq_id();
                if request.request_status() == RequestStatus::Complete {
                    if std::mem::take(&mut state.fail_complete) {
                        return request.error_ext(&FsError::common("injected close failure"));
                    }
                    *session = None;
                    return request.success();
                }
                if std::mem::take(&mut state.fail_read) {
                    return request.error_ext(&FsError::common("injected read failure"));
                }
                if request.header_len() > 0 {
                    opened.offset =
                        request.parse_header::<DataHeaderProto>().unwrap().offset as usize;
                }
                let data = Self::data(state, opened);
                opened.offset += data.len();
                request.success_with_data(None, data)
            }
            other => panic!("unexpected status {other:?}"),
        }
    }

    fn data(state: &mut Observed, session: &Session) -> DataSlice {
        let end = session
            .end
            .min(session.offset + session.chunk)
            .min(state.contents.len());
        let bytes = Bytes::copy_from_slice(&state.contents[session.offset..end]);
        state.bytes_sent += bytes.len();
        DataSlice::Bytes(bytes)
    }

    fn calls(&self) -> usize {
        self.observed.lock().unwrap().calls.len()
    }

    async fn reader(&self, context: &FsContext, len: i64) -> BlockReaderRemote {
        BlockReaderRemote::new(
            context,
            ExtendedBlock::with_id(1),
            self.address.clone(),
            0,
            len,
        )
        .await
        .unwrap()
    }
}

fn context() -> (Arc<Runtime>, Arc<FsContext>) {
    let rt = Arc::new(Runtime::new("small-read-test", 2, 2));
    let mut conf = ClusterConf::default();
    conf.client.short_circuit = false;
    let context = Arc::new(FsContext::with_rt(conf, rt.clone()).unwrap());
    (rt, context)
}

#[test]
fn lazy_reads_seek_without_prefetch_and_support_legacy_workers() {
    let (rt, context) = context();
    rt.block_on(async {
        for legacy in [false, true] {
            let worker = TestWorker::start(legacy, 4096).await;
            let mut reader = worker.reader(&context, 4096).await;
            assert_eq!(worker.calls(), 0);
            reader.seek(17).unwrap();
            assert_eq!(worker.calls(), 0);
            assert_eq!(reader.read().await.unwrap().as_slice(), &[7; 4096 - 17]);
            let per_read = if legacy { 3 } else { 1 };
            assert_eq!(worker.calls(), per_read);
            assert_eq!(worker.observed.lock().unwrap().bytes_sent, 4096 - 17);
            worker.observed.lock().unwrap().contents[17] = 99;
            reader.seek(17).unwrap();
            assert_eq!(reader.read().await.unwrap().as_slice()[0], 99);
            assert_eq!(worker.calls(), 2 * per_read);
            assert!(reader.seek(-1).is_err());
            assert!(reader.seek(4097).is_err());
            reader.complete().await.unwrap();
            reader.complete().await.unwrap();
            assert!(reader.seek(0).is_err());
            assert!(reader.read().await.is_err());
            assert_eq!(worker.calls(), 2 * per_read);
            let observed = worker.observed.lock().unwrap();
            assert_ne!(observed.calls[0].0, observed.calls[per_read].0);
            assert_eq!(observed.offsets, [17, 17]);
        }
    });
}

#[test]
fn closing_an_unread_or_empty_small_block_sends_no_block_rpc() {
    let (rt, context) = context();
    rt.block_on(async {
        let worker = TestWorker::start(false, 4096).await;
        for offset in [0, 4096] {
            let mut reader = worker.reader(&context, 4096).await;
            reader.seek(offset).unwrap();
            reader.complete().await.unwrap();
            reader.complete().await.unwrap();
            assert!(reader.read().await.is_err());
            assert!(reader.seek(0).is_err());
        }
        assert_eq!(worker.calls(), 0);
    });
}

#[test]
fn short_responses_fail_on_read_and_leave_the_connection_reusable() {
    let (rt, context) = context();
    rt.block_on(async {
        for legacy in [false, true] {
            let worker = TestWorker::start(legacy, 4096).await;
            let mut reader = worker.reader(&context, 8192).await;
            assert_eq!(worker.calls(), 0);
            let error = reader.read().await.err().unwrap();
            assert!(error.to_string().contains("Incomplete small block read"));
            assert_eq!(reader.pos(), 0);
            drop(reader);
            let mut reader = worker.reader(&context, 4096).await;
            assert_eq!(reader.read().await.unwrap().len(), 4096);
            reader.complete().await.unwrap();
            assert_eq!(worker.observed.lock().unwrap().connections, 1);
        }
    });
}

#[test]
fn legacy_errors_always_attempt_cleanup_and_discard_connections_if_cleanup_fails() {
    let (rt, context) = context();
    rt.block_on(async {
        for (fail_read, fail_complete) in [(true, false), (false, true), (true, true)] {
            let worker = TestWorker::start(true, 4096).await;
            {
                let mut state = worker.observed.lock().unwrap();
                state.fail_read = fail_read;
                state.fail_complete = fail_complete;
            }
            let mut reader = worker.reader(&context, 4096).await;
            let error = reader.read().await.err().unwrap().to_string();
            assert!(error.contains(if fail_read {
                "injected read failure"
            } else {
                "injected close failure"
            }));
            assert_eq!(reader.pos(), 0);
            assert_eq!(worker.calls(), 3);
            assert_eq!(
                worker.observed.lock().unwrap().calls[2].2,
                RequestStatus::Complete
            );
            drop(reader);
            let mut reader = worker.reader(&context, 4096).await;
            assert_eq!(reader.read().await.unwrap().len(), 4096);
            reader.complete().await.unwrap();
            assert_eq!(worker.calls(), 6);
            assert_eq!(
                worker.observed.lock().unwrap().connections,
                if fail_complete { 2 } else { 1 }
            );
        }
    });
}

#[test]
fn streaming_and_once_readers_stay_closed_without_replica_retry() {
    let (rt, context) = context();
    rt.block_on(async {
        for size in [4096, 256 * 1024] {
            for read_first in [false, true] {
                let worker = TestWorker::start(false, size).await;
                let mut block = ExtendedBlock::with_id(1);
                block.len = size as i64;
                let mut reader = BlockReader::new(
                    context.clone(),
                    LocatedBlock::new(block, vec![worker.address.clone()]),
                    0,
                )
                .await
                .unwrap();
                if read_first {
                    assert_eq!(reader.read().await.unwrap().len(), size.min(128 * 1024));
                }
                reader.complete().await.unwrap();
                let calls = worker.calls();
                reader.complete().await.unwrap();
                assert!(reader
                    .read()
                    .await
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("Read session has completed"));
                assert!(reader.seek(0).is_err());
                assert_eq!(worker.calls(), calls);
                assert_eq!(
                    calls,
                    if size == 4096 {
                        usize::from(read_first)
                    } else {
                        2 + usize::from(read_first)
                    }
                );
            }
        }
    });
}

#[test]
fn lazy_open_failure_retries_another_replica_at_the_requested_offset() {
    let (rt, context) = context();
    rt.block_on(async {
        let first = TestWorker::start(false, 4096).await;
        let second = TestWorker::start(false, 4096).await;
        // Replica order is randomized. Fail the first actual Open whichever
        // worker receives it, then let the other replica serve the same range.
        let fail_first = Arc::new(AtomicBool::new(true));
        first.observed.lock().unwrap().fail_open = fail_first.clone();
        second.observed.lock().unwrap().fail_open = fail_first;
        let mut block = ExtendedBlock::with_id(1);
        block.len = 4096;
        let located = LocatedBlock::new(block, vec![first.address.clone(), second.address.clone()]);
        let mut reader = BlockReader::new(context, located, 0).await.unwrap();
        reader.seek(17).unwrap();
        assert_eq!(first.calls() + second.calls(), 0);
        assert_eq!(reader.read().await.unwrap().as_slice(), &[7; 4096 - 17]);
        assert_eq!(first.calls(), 1);
        assert_eq!(second.calls(), 1);
        assert_eq!(first.observed.lock().unwrap().offsets, [17]);
        assert_eq!(second.observed.lock().unwrap().offsets, [17]);
        reader.complete().await.unwrap();
        assert!(reader.read().await.is_err());
        assert_eq!(first.calls() + second.calls(), 2);
    });
}
