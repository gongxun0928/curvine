// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use bytes::Bytes;
use curvine_config::ClusterConf;
use curvine_fs_api::RpcCode;
use curvine_io::DataSlice;
use curvine_model::{ExtendedBlock, ProtoUtils};
use curvine_proto::{BlockReadRequest, BlockWriteRequest};
use curvine_rpc::handler::{HandlerService, MessageHandler};
use curvine_rpc::message::{Builder, RequestStatus};
use curvine_worker::Worker;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn small_read_releases_session_preserves_sparse_data_and_keeps_legacy_flow() {
    let directory = std::env::temp_dir().join(format!(
        "curvine-read-once-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let mut conf = ClusterConf {
        cluster_id: "read-once-test".into(),
        format_worker: true,
        ..ClusterConf::default()
    };
    conf.worker.data_dir = vec![format!("[DISK:16MB]{}", directory.display())];
    conf.worker.dir_reserved = "0".into();
    conf.worker.io_threads = 2;
    conf.worker.worker_threads = 2;
    conf.worker.log.level = "ERROR".into();
    conf.worker.log.log_dir = "stderr".into();
    let worker = Worker::with_conf(conf).unwrap();
    let handler = worker.service().get_message_handler(None);
    let mut block = ExtendedBlock::with_id(42);
    let write_header = |block: &ExtendedBlock| BlockWriteRequest {
        block: ProtoUtils::extend_block_to_pb(block.clone()),
        off: 0,
        block_size: 8192,
        chunk_size: 4096,
        ..Default::default()
    };
    let message = |status, header: BlockWriteRequest, data: DataSlice| {
        Builder::new()
            .code(RpcCode::WriteBlock)
            .request(status)
            .req_id(1)
            .proto_header(header)
            .data(data)
            .build()
    };
    assert!(handler
        .handle(&message(
            RequestStatus::Open,
            write_header(&block),
            DataSlice::Empty
        ))
        .unwrap()
        .is_success());
    let bytes = Bytes::from(vec![23_u8; 4096]);
    assert!(handler
        .handle(
            &Builder::new()
                .code(RpcCode::WriteBlock)
                .request(RequestStatus::Running)
                .req_id(1)
                .data(DataSlice::Bytes(bytes.clone()))
                .build()
        )
        .unwrap()
        .is_success());
    block.len = 4096;
    assert!(handler
        .handle(&message(
            RequestStatus::Complete,
            write_header(&block),
            DataSlice::Empty
        ))
        .unwrap()
        .is_success());

    let read = |off, len, chunk_size, once, short_circuit| {
        Builder::new()
            .code(RpcCode::ReadBlock)
            .request(RequestStatus::Open)
            .req_id(2)
            .proto_header(BlockReadRequest {
                id: 42,
                off,
                len,
                chunk_size,
                short_circuit,
                read_once: Some(once),
                ..Default::default()
            })
            .build()
    };

    for (off, len) in [(0, 4096), (17, 4096), (4096, 4096), (0, 8192)] {
        let response = handler.handle(&read(off, len, 8192, true, false)).unwrap();
        response
            .check_error_ext::<curvine_error::FsError>()
            .unwrap();
        assert_eq!(response.request_status(), RequestStatus::Complete);
        assert_eq!(response.data.len(), (len - off) as usize);
        let mut expected = bytes.to_vec();
        expected.resize(len as usize, 0);
        assert_eq!(response.data.as_slice(), &expected[off as usize..]);
        assert!(
            handler.handler.lock().is_none(),
            "one-shot response must release the connection handler"
        );
    }

    // No opt-in, an oversized range, and a short-circuit read all retain the
    // original open/stream/complete contract.
    for (len, chunk, once, local) in [
        (4096, 4096, false, false),
        (4096, 1024, true, false),
        (4096, 4096, true, true),
    ] {
        let response = handler.handle(&read(0, len, chunk, once, local)).unwrap();
        assert!(response.is_success());
        assert_eq!(response.request_status(), RequestStatus::Open);
        assert!(response.data.is_empty());
        assert!(handler.handler.lock().is_some());
        if !local {
            let response = handler
                .handle(
                    &Builder::new()
                        .code(RpcCode::ReadBlock)
                        .request(RequestStatus::Running)
                        .req_id(2)
                        .build(),
                )
                .unwrap();
            assert_eq!(response.data.len(), chunk as usize);
        }
        assert!(handler
            .handle(
                &Builder::new()
                    .code(RpcCode::ReadBlock)
                    .request(RequestStatus::Complete)
                    .req_id(2)
                    .build()
            )
            .unwrap()
            .is_success());
        assert!(handler.handler.lock().is_none());
    }
    assert!(!handler
        .handle(&read(-1, 4096, 4096, true, false))
        .unwrap()
        .is_success());
    handler.store.remove_block(42).unwrap();
    assert!(!handler
        .handle(&read(0, 4096, 4096, true, false))
        .unwrap()
        .is_success());
    drop(handler);
    drop(worker);
    std::fs::remove_dir_all(directory).unwrap();
}
