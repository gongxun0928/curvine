// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use bytes::{Bytes, BytesMut};
use curvine_io::DataSlice;
use curvine_rpc::handler::{Frame, RpcFrame};
use curvine_rpc::message::{Builder, Message};
use socket2::SockRef;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

async fn pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    client.set_nodelay(true).unwrap();
    SockRef::from(&client).set_send_buffer_size(4096).unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (client, server)
}

// Check the bytes on the wire independently of the receive implementation,
// including frame boundaries after a large, backpressured write.
async fn check_memory_frames(split: bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (client, mut server) = pair().await;
        let mut messages = Vec::new();
        let mut backing = Vec::new();
        let mut expected = BytesMut::new();
        for (i, size) in [0, 1, 4096, 131072, 16 * 1024 * 1024, 17]
            .into_iter()
            .enumerate()
        {
            let bytes = Bytes::from((0..size).map(|n| (n % 251) as u8).collect::<Vec<_>>());
            // MemSlice borrows raw memory; retain its owner through send().
            backing.push(bytes.clone());
            let data = match i % 4 {
                0 => DataSlice::Bytes(bytes),
                1 => DataSlice::Buffer(BytesMut::from(bytes.as_ref())),
                2 => DataSlice::mem_slice(&bytes),
                _ => DataSlice::Bytes(bytes),
            };
            let mut builder = Builder::new_rpc(7_i8).seq_id(i as i32).data(data);
            if i % 2 == 0 {
                builder = builder.header(BytesMut::from(&b"metadata"[..]));
            }
            let message = builder.build();
            message.encode(&mut expected).unwrap();
            messages.push(message);
        }
        let reader = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let mut actual = Vec::new();
            server.read_to_end(&mut actual).await.unwrap();
            assert_eq!(actual, expected);
        });
        let mut frame = RpcFrame::with_client(client, 4096);
        if split {
            let (read, mut write) = frame.split();
            for message in messages {
                write.send(&message).await.unwrap();
            }
            drop(write);
            drop(read);
        } else {
            for message in messages {
                frame.send(message).await.unwrap();
            }
            drop(frame);
        }
        reader.await.unwrap();
        drop(backing);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn raw_frames_preserve_wire_bytes_under_backpressure() {
    check_memory_frames(false).await;
}

#[tokio::test]
async fn split_frames_preserve_wire_bytes_under_backpressure() {
    check_memory_frames(true).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn sendfile_preserves_header_offset_length_and_following_frame() {
    use std::os::fd::AsRawFd;
    use std::time::{SystemTime, UNIX_EPOCH};
    let path = std::env::temp_dir().join(format!(
        "rpc-sendfile-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let bytes: Vec<u8> = (0..1024 * 1024 + 37).map(|n| (n % 251) as u8).collect();
    std::fs::write(&path, &bytes).unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let (client, mut peer) = pair().await;
    let mut frame = RpcFrame::with_client(client, 4096);
    let message = Builder::new_rpc(8_i8)
        .header(BytesMut::from(&b"file metadata"[..]))
        .data(DataSlice::io_slice(file.as_raw_fd(), Some(17), 1024 * 1024))
        .build();
    let mut expected = BytesMut::new();
    let memory_message = Message {
        protocol: message.protocol,
        header: message.header.clone(),
        data: DataSlice::Buffer(BytesMut::from(&bytes[17..17 + 1024 * 1024])),
    };
    memory_message.encode(&mut expected).unwrap();
    let following = Builder::new_rpc(9_i8).build();
    following.encode(&mut expected).unwrap();
    let reader = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut actual = Vec::new();
        peer.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
    });
    tokio::time::timeout(Duration::from_secs(20), async {
        frame.send(message).await.unwrap();
        frame.send(following).await.unwrap();
        drop(frame);
        reader.await.unwrap();
    })
    .await
    .unwrap();
    drop(file);
    std::fs::remove_file(path).unwrap();
}
