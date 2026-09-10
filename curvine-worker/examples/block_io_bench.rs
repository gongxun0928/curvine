// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

//! Real remote block I/O benchmark, including open and finalize, without a
//! Master. Uses a unique directory beneath the optional first argument (default
//! /tmp). Every written byte is read back and verified. The directory is removed
//! only after a successful run. Run release binaries alternately for comparison.
use bytes::Bytes;
use curvine_client_core::block::{BlockReaderRemote, BlockWriterRemote};
use curvine_client_core::file::FsContext;
use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_io::DataSlice;
use curvine_model::ExtendedBlock;
use curvine_net::net::NetUtils;
use curvine_rpc::handler::HandlerService;
use curvine_rpc::server::RpcServer;
use curvine_runtime::runtime::RpcRuntime;
use curvine_worker::Worker;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn main() -> CommonResult<()> {
    let base = std::env::args().nth(1).unwrap_or_else(|| "/tmp".into());
    let directory = std::path::Path::new(&base).join(format!(
        "curvine-block-bench-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    std::fs::create_dir(&directory)?;
    let mut conf = ClusterConf {
        cluster_id: "block-io-bench".into(),
        format_worker: true,
        ..ClusterConf::default()
    };
    conf.worker.hostname = "127.0.0.1".into();
    conf.worker.rpc_port = NetUtils::get_available_port();
    conf.worker.data_dir = vec![format!("[DISK:1GB]{}", directory.display())];
    conf.worker.dir_reserved = "0".into();
    conf.worker.io_threads = 2;
    conf.worker.worker_threads = 4;
    conf.worker.log.level = "ERROR".into();
    conf.worker.log.log_dir = "stderr".into();
    // Exercise sendfile by default; pass 0 as the second argument to test the
    // buffered response path. Both runs still execute storage flush/finalize.
    conf.worker.enable_send_file = std::env::args().nth(2).as_deref() != Some("0");
    conf.client.short_circuit = false;
    conf.client.io_threads = 2;
    conf.client.worker_threads = 4;
    if let Some(chunk) = std::env::args().nth(3) {
        conf.client.write_chunk_size = chunk.parse()?;
        conf.client.read_chunk_size = chunk.parse()?;
        assert!(
            conf.client.write_chunk_size > 0 && conf.client.write_chunk_size <= 16 * 1024 * 1024
        );
    }
    // Pass 1 as the fourth argument to group writes before reads. This helps
    // distinguish write latency from scheduling effects of alternating I/O.
    let separate = std::env::args().nth(4).as_deref() == Some("1");
    // Pass 1 as the fifth argument to seek before the first read. Even a
    // same-position seek discarded the eager implementation's prefetched block.
    let initial_seek = std::env::args().nth(5).as_deref() == Some("1");
    let worker = Worker::with_conf(conf.clone())?;
    let service = worker.service().clone();
    let store = service.get_message_handler(None).store;
    let runtime = service.clone_rt();
    let server = RpcServer::with_rt(runtime.clone(), conf.worker_server_conf(), service);
    let mut state = server.new_state_listener();
    let server_task = runtime.spawn(async move { server.run().await });
    let context = Arc::new(FsContext::with_rt(conf.clone(), runtime.clone())?);
    let address = worker.addr.clone();
    runtime.block_on(async {
        state.wait_running().await?;
        println!("direction,block_bytes,chunk_bytes,iterations,mean_us,p50_us,p99_us,mib_per_s");
        let mut block_id = 1;
        for size in [4096, 128 * 1024, 64 * 1024 * 1024] {
            let chunk_size = conf.client.write_chunk_size;
            let bytes = Bytes::from((0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>());
            let iterations = if size < 1024 * 1024 { 400 } else { 16 };
            let warmup = if size < 1024 * 1024 { 20 } else { 2 };
            let mut writes = Vec::new();
            let mut reads = Vec::new();
            let batch_size = if separate {
                if size < 1024 * 1024 {
                    iterations + warmup
                } else {
                    4
                }
            } else {
                1
            };
            for batch_start in (0..iterations + warmup).step_by(batch_size) {
                let mut pending = Vec::new();
                for iteration in batch_start..(batch_start + batch_size).min(iterations + warmup) {
                    let mut block = ExtendedBlock::with_id(block_id);
                    block_id += 1;
                    let started = Instant::now();
                    let mut writer = BlockWriterRemote::new(
                        &context,
                        block.clone(),
                        address.clone(),
                        0,
                        size as i64,
                    )
                    .await?;
                    for off in (0..size).step_by(chunk_size) {
                        writer
                            .write(DataSlice::Bytes(
                                bytes.slice(off..(off + chunk_size).min(size)),
                            ))
                            .await?;
                    }
                    writer.complete().await?;
                    drop(writer);
                    let write_time = started.elapsed();
                    block.len = size as i64;
                    pending.push((iteration, block, write_time));
                }
                for (iteration, block, write_time) in pending {
                    let started = Instant::now();
                    let mut reader = BlockReaderRemote::new(
                        &context,
                        block.clone(),
                        address.clone(),
                        0,
                        size as i64,
                    )
                    .await?;
                    if initial_seek {
                        reader.seek(0)?;
                    }
                    let mut read_time = started.elapsed();
                    let mut offset = 0;
                    while reader.remaining() > 0 {
                        let started = Instant::now();
                        let data = reader.read().await?;
                        read_time += started.elapsed();
                        assert!(!data.is_empty());
                        assert_eq!(data.as_slice(), &bytes[offset..offset + data.len()]);
                        offset += data.len();
                    }
                    assert_eq!(offset, size);
                    let started = Instant::now();
                    reader.complete().await?;
                    drop(reader);
                    read_time += started.elapsed();
                    if iteration >= warmup {
                        writes.push(write_time);
                        reads.push(read_time);
                    }
                    store.remove_block(block.id)?;
                }
            }
            report("write", size, chunk_size, writes);
            report("read", size, chunk_size, reads);
        }
        Ok::<_, curvine_core_error::CommonError>(())
    })?;
    drop(context);
    server_task.abort();
    drop(worker);
    std::fs::remove_dir_all(directory)?;
    Ok(())
}

fn report(direction: &str, size: usize, chunk: usize, mut times: Vec<Duration>) {
    times.sort();
    let mean = times.iter().map(Duration::as_secs_f64).sum::<f64>() / times.len() as f64;
    println!(
        "{direction},{size},{chunk},{},{:.3},{:.3},{:.3},{:.3}",
        times.len(),
        mean * 1e6,
        times[times.len() / 2].as_secs_f64() * 1e6,
        times[times.len() * 99 / 100].as_secs_f64() * 1e6,
        size as f64 / 1048576.0 / mean
    );
}
