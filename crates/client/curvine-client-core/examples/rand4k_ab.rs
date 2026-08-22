// Random 4KiB-demand SDK harness for the exact-demand experiment (task #7).
// Not part of any released binary; used only for paired A/B evidence.
//
// Usage:
//   rand4k_ab <conf.toml> <cv-path> <class> <ops> <seed> [chunk_ops]
//     class = uniform   : every op picks a uniformly random 4KiB-ALIGNED
//                         offset in the whole file (no locality)
//             uniformu  : same but byte-unaligned offsets (supplementary)
//             chunklocal: pick a random 128KiB-aligned chunk, then do
//                         `chunk_ops` random 4KiB-aligned reads inside
//                         that chunk before moving on (in-chunk locality)
//
// Prints one line of TSV: class, ops, iops, p50_us, p95_us, p99_us, max_us,
// bytes_read, errors.

use curvine_client_core::file::CurvineFileSystem;
use curvine_config::ClusterConf;
use curvine_fs_api::{Path, Reader};
use std::time::{Duration, Instant};

fn pct(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 6 {
        eprintln!(
            "usage: {} <conf.toml> <cv-path> <uniform|chunklocal> <ops> <seed> [chunk_ops]",
            args[0]
        );
        std::process::exit(2);
    }
    let conf_path = &args[1];
    let path = Path::from(args[2].as_str());
    let class = args[3].clone();
    let ops: u64 = args[4].parse()?;
    let seed: u64 = args[5].parse()?;
    let chunk_ops: u64 = if args.len() > 6 { args[6].parse()? } else { 8 };

    let conf = ClusterConf::from(conf_path.as_str()).map_err(|e| e.to_string())?;
    let rpc_rt = std::sync::Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = CurvineFileSystem::with_rt(conf, rpc_rt.clone()).map_err(|e| e.to_string())?;

    // Drive the async workload on a local tokio runtime, then drop the fs and
    // the RPC runtime in sync context (they must not be dropped inside async).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = rt.block_on(run(&fs, &path, &class, ops, seed, chunk_ops));
    drop(rt);
    drop(fs);
    drop(rpc_rt);
    let (class, ops, iops, lats, bytes, errors) = result?;
    println!(
        "{}\t{}\t{:.1}\t{}\t{}\t{}\t{}\t{}\t{}",
        class,
        ops,
        iops,
        pct(&lats, 0.50),
        pct(&lats, 0.95),
        pct(&lats, 0.99),
        lats.last().copied().unwrap_or(0),
        bytes,
        errors
    );
    Ok(())
}

async fn run(
    fs: &CurvineFileSystem,
    path: &Path,
    class: &str,
    ops: u64,
    seed: u64,
    chunk_ops: u64,
) -> Result<(String, u64, f64, Vec<u128>, u64, u64), String> {
    let mut reader = fs.open(path).await.map_err(|e| e.to_string())?;
    let len = reader.len();
    const BS: i64 = 4096;
    const CHUNK: i64 = 131072;

    // xorshift64* deterministic RNG
    let mut rng = seed | 1;
    let mut next = move || {
        rng ^= rng >> 12;
        rng ^= rng << 25;
        rng ^= rng >> 27;
        rng.wrapping_mul(0x2545F4914F6CDD1D)
    };

    let mut buf = vec![0u8; BS as usize];
    let mut lats: Vec<u128> = Vec::with_capacity(ops as usize);
    let mut bytes: u64 = 0;
    let mut errors: u64 = 0;

    let mut cur_chunk: i64 = -1;
    let mut ops_in_chunk: u64 = 0;

    let t0 = Instant::now();
    for _ in 0..ops {
        let off = match class {
            // 4KiB-aligned uniform randread (standard fio randread semantics).
            // A unit index is drawn, then scaled: identical op sequence across
            // sides for the same seed, and no reads straddle 4KiB boundaries.
            "uniform" => {
                let units = ((len - BS) / BS + 1) as u64;
                (next() % units) as i64 * BS
            }
            // Unaligned variant (byte-granularity offset) kept as a separate
            // supplementary class so alignment effects stay measurable.
            "uniformu" => (next() % ((len - BS) as u64)) as i64,
            "chunklocal" => {
                if ops_in_chunk == 0 {
                    let nchunks = (len / CHUNK) as u64;
                    cur_chunk = (next() % nchunks) as i64;
                    ops_in_chunk = chunk_ops;
                }
                ops_in_chunk -= 1;
                // In-chunk offset also 4KiB-aligned (32 possible unit offsets
                // within a 128KiB chunk).
                cur_chunk * CHUNK + (next() % (((CHUNK - BS) / BS + 1) as u64)) as i64 * BS
            }
            // Sequential 4KiB reads within one chunk: the regime where the
            // 128KiB frame should clearly win (1 fetch + local hits vs 1
            // fetch per op).
            "seqchunk" => {
                if ops_in_chunk == 0 {
                    let nchunks = (len / CHUNK) as u64;
                    cur_chunk = (next() % nchunks) as i64;
                    ops_in_chunk = (CHUNK / BS) as u64; // walk the whole chunk
                }
                let off = cur_chunk * CHUNK + CHUNK - (ops_in_chunk * BS as u64) as i64;
                ops_in_chunk -= 1;
                off
            }
            other => return Err(format!("unknown class {other}")),
        };

        let t = Instant::now();
        let res = async {
            reader.seek(off).await?;
            let mut got = 0usize;
            while got < BS as usize {
                let n = reader.read(&mut buf[got..]).await?;
                if n == 0 {
                    break;
                }
                got += n;
            }
            Ok::<usize, curvine_error::FsError>(got)
        }
        .await;
        lats.push(t.elapsed().as_micros());
        match res {
            Ok(got) => {
                if got != BS as usize {
                    errors += 1;
                }
                bytes += got as u64;
            }
            Err(_) => errors += 1,
        }
    }
    let elapsed: Duration = t0.elapsed();

    lats.sort_unstable();
    let iops = ops as f64 / elapsed.as_secs_f64();
    Ok((class.to_string(), ops, iops, lats, bytes, errors))
}
