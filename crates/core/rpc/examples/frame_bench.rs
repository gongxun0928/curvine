// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//! Loopback RPC transport benchmark. Run identical release binaries alternately
//! before/after a change; this excludes storage and metadata costs.
use bytes::{Bytes, BytesMut};
use curvine_io::{DataSlice, IOResult};
use curvine_rpc::handler::{Frame, RpcFrame};
use curvine_rpc::message::Builder;
use std::time::Instant;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main(worker_threads = 2)]
async fn main() -> IOResult<()> {
    println!("direction,bytes,iterations,mean_us,p50_us,p99_us,mib_per_s");
    for size in [4096, 128 * 1024, 1024 * 1024] {
        for read in [false, true] {
            run(size, read).await?;
        }
    }
    Ok(())
}

async fn run(size: usize, read: bool) -> IOResult<()> {
    let iterations = if size <= 4096 { 10000 } else { 2000 };
    let warmup = 200;
    let payload = Bytes::from((0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let stream = TcpStream::connect(listener.local_addr()?).await?;
    stream.set_nodelay(true)?;
    let (peer, _) = listener.accept().await?;
    peer.set_nodelay(true)?;
    let expected = payload.clone();
    let server = tokio::spawn(async move {
        let mut frame = RpcFrame::with_client(peer, size);
        for _ in 0..iterations + warmup {
            let msg = frame.receive().await?;
            let response = if read {
                msg.success_with_data(None, DataSlice::Bytes(expected.clone()))
            } else {
                assert_eq!(msg.data.as_slice(), expected.as_ref());
                msg.success()
            };
            frame.send(response).await?;
        }
        Ok::<_, curvine_io::IOError>(())
    });
    let mut client = RpcFrame::with_client(stream, size);
    let mut times = Vec::with_capacity(iterations);
    for i in 0..iterations + warmup {
        let request = Builder::new_rpc(1_i8)
            .seq_id(i as i32)
            .header(BytesMut::from(&b"block request metadata"[..]))
            .data(if read {
                DataSlice::Empty
            } else {
                DataSlice::Bytes(payload.clone())
            })
            .build();
        let started = Instant::now();
        client.send(request).await?;
        let response = client.receive().await?;
        let elapsed = started.elapsed().as_nanos() as f64 / 1000.0;
        assert_eq!(response.seq_id(), i as i32);
        if read {
            assert_eq!(response.data.as_slice(), payload.as_ref());
        }
        if i >= warmup {
            times.push(elapsed);
        }
    }
    server.await.unwrap()?;
    times.sort_by(f64::total_cmp);
    let mean = times.iter().sum::<f64>() / times.len() as f64;
    println!(
        "{},{size},{iterations},{mean:.3},{:.3},{:.3},{:.3}",
        if read { "read" } else { "write" },
        times[times.len() / 2],
        times[times.len() * 99 / 100],
        size as f64 / 1048576.0 / (mean / 1e6)
    );
    Ok(())
}
