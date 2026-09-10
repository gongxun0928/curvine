// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use bytes::Bytes;
use curvine_client_core::block::BlockReaderRemote;
use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, WorkerAddress};
use curvine_proto::{BlockReadRequest, BlockReadResponse};
use curvine_rpc::handler::{Frame, RpcFrame};
use curvine_rpc::message::{Builder, RequestStatus};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

#[test]
fn small_reads_support_new_and_legacy_workers_and_refresh_after_seek() {
    let rt = Arc::new(Runtime::new("small-read-test", 2, 2));
    let context = FsContext::with_rt(ClusterConf::default(), rt.clone()).unwrap();
    rt.block_on(async {
        for legacy in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = WorkerAddress {
                worker_id: if legacy { 2 } else { 1 },
                hostname: "127.0.0.1".into(),
                ip_addr: "127.0.0.1".into(),
                rpc_port: listener.local_addr().unwrap().port() as u32,
                web_port: 0,
            };
            let contents = Arc::new(Mutex::new(vec![7_u8; 4096]));
            let calls = Arc::new(Mutex::new(Vec::new()));
            let data = contents.clone();
            let observed = calls.clone();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                stream.set_nodelay(true).unwrap();
                let mut frame = RpcFrame::with_client(stream, 4096);
                let mut offset = 0;
                loop {
                    let request = frame.receive().await.unwrap();
                    if request.is_empty() {
                        break;
                    }
                    observed.lock().unwrap().push(request.request_status());
                    let response = match request.request_status() {
                        RequestStatus::Open => {
                            let header: BlockReadRequest = request.parse_header().unwrap();
                            offset = header.off as usize;
                            let builder =
                                Builder::success(&request).proto_header(BlockReadResponse {
                                    id: header.id,
                                    len: header.len,
                                    path: None,
                                    storage_type: 0,
                                });
                            if !legacy && header.read_once.unwrap_or(false) {
                                builder
                                    .request(RequestStatus::Complete)
                                    .data(DataSlice::Bytes(Bytes::copy_from_slice(
                                        &data.lock().unwrap()[offset..],
                                    )))
                                    .build()
                            } else {
                                builder.build()
                            }
                        }
                        RequestStatus::Running => request.success_with_data(
                            None,
                            DataSlice::Bytes(Bytes::copy_from_slice(
                                &data.lock().unwrap()[offset..],
                            )),
                        ),
                        RequestStatus::Complete => request.success(),
                        other => panic!("unexpected status {other:?}"),
                    };
                    frame.send(response).await.unwrap();
                }
            });
            let block = ExtendedBlock::with_id(1);
            let mut reader =
                BlockReaderRemote::new(&context, block.clone(), address.clone(), 0, 4096)
                    .await
                    .unwrap();
            assert_eq!(reader.read().await.unwrap().as_slice(), &[7_u8; 4096]);
            let per_read = if legacy { 3 } else { 1 };
            assert_eq!(calls.lock().unwrap().len(), per_read);
            contents.lock().unwrap()[17] = 99;
            reader.seek(17).unwrap();
            let data = reader.read().await.unwrap();
            assert_eq!(data.len(), 4096 - 17);
            assert_eq!(
                data.as_slice()[0],
                99,
                "seek must not reuse stale prefetched bytes"
            );
            assert_eq!(calls.lock().unwrap().len(), 2 * per_read);
            reader.seek(-1).unwrap();
            assert!(reader.read().await.is_err());
            assert_eq!(calls.lock().unwrap().len(), 2 * per_read);
            reader.complete().await.unwrap();
            reader.seek(0).unwrap();
            assert!(reader.read().await.is_err());
            assert_eq!(
                calls.lock().unwrap().len(),
                2 * per_read,
                "complete must not send a redundant RPC"
            );
            if legacy {
                assert_eq!(
                    &calls.lock().unwrap()[..3],
                    &[
                        RequestStatus::Open,
                        RequestStatus::Running,
                        RequestStatus::Complete
                    ]
                );
            }
            drop(reader);
            let before = calls.lock().unwrap().len();
            assert!(
                BlockReaderRemote::new(&context, block.clone(), address.clone(), 0, 8192)
                    .await
                    .is_err(),
                "a short response must not become a successful read"
            );
            assert_eq!(calls.lock().unwrap().len(), before + per_read);
            // Reuse the connection after that error, and handle an empty tail
            // without sending a legacy Running request past EOF.
            let mut empty = BlockReaderRemote::new(&context, block, address, 4096, 4096)
                .await
                .unwrap();
            empty.complete().await.unwrap();
            assert_eq!(
                calls.lock().unwrap().len(),
                before + per_read + if legacy { 2 } else { 1 }
            );
            drop(empty);
            server.abort();
        }
    });
}
