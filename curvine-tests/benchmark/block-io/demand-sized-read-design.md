# 按调用需求调整远程读取大小

状态：设计草案。小 block 单 RPC 读取已包含在 `codex/small-block-read-once` 分支；本文的大请求尺寸传递、动态 chunk 和单 RPC 写入尚未实现。已有全局 `read_chunk_size`/`write_chunk_size` 默认值保持 128KiB。

## 两项改动的边界

- 小 block 单 RPC 读取：当前实现只在逻辑 block 长度不超过默认 read chunk 时启用。新 Worker 返回数据和 Complete；旧 Worker 返回 Open，新 client 自动补 read/complete。减少的是该次远程 block 会话的控制往返，Master 元数据 RPC 仍独立存在。
- 按需扩大读取：保留 128KiB 常规读取粒度；将调用方已明确需要的大范围拆成更少、更大的数据请求。RPC 返回数据量、后台预取总量和 buffer 容量必须分开控制。

## 当前调用链的具体障碍

1. Java `CurvineInputStream.read(byte[], offset, length)` 知道 length，但调用 `libFs.read(nativeHandle, tmp)` 时没有向 native 传长度。`LibFsReader::read()` 接着调用 `blocking_read()`，仍没有长度参数。扩大 Java buffer 本身不会扩大 Worker 每次返回的数据量。
2. Rust `Reader::read_chunk(Some(len))` 只用 len 切割已经取回的 chunk；触发后端取数的 `read_chunk0()` 没有参数。`FsReader::read_chunk0()` → `FsReaderBuffer` → `FsReaderParallel` → `FsReaderBase` → `BlockReaderRemote` 都需要传播读取需求。
3. 当前 Worker 在 Open 时保存 `BlockReadRequest.chunk_size`，后续 Running 调用 `file.read_region(..., context.chunk_size)`。不能只修改 client 的内存分配；已打开的 session 也需要支持每次请求长度。
4. `BlockReadRequest.len` 是逻辑 block 边界，用于 EOF、稀疏尾部等语义，不能将它重解释成一次 SDK 请求长度。当前 `DataHeaderProto.offset` 表示 seek，增加长度字段时也不能用默认 offset=0 意外改变游标。
5. `FsReaderParallel` 按 `read_slice_size` 分配范围。直接让 chunk 跨过分配的 slice，会侵入其他 reader 的范围。大 demand 需要由调度层分配连续 range，或者只在自身 slice 边界以内发请求。

## 建议策略

拟新增配置，名称尚未进入代码：

```toml
[client]
read_chunk_size = "128KB"       # 现有默认值不变
max_read_chunk_size = "2MB"    # 拟新增，可配置为 4MB
```

只按用户指定的 length / buffer 的可写范围判定需求，不能按内存池分配容量判断；一个容量 4MiB、实际只读 4KiB 的 buffer 仍属于小请求。

设 `n` 为本次调用尚未满足、且未被现有有效缓存覆盖的字节数：

- 未提供 n 或 n ≤ 128KiB：保持现有 128KiB chunk 上限，实际还受 block/EOF 等边界限制。这保留现有小请求行为，不等于消除了现有预读。
- n > 128KiB：`request_bytes = min(n, max_read_chunk_size, block_remaining, assigned_range_remaining)`。不要为了使用 2MiB buffer 而将 129KiB 请求取整成 2MiB。
- 如果 demand 跨多个 block，分别发往对应 block/Worker；如果超过上限，在同一调用中继续处理剩余范围。
- 后续遇到小请求时恢复默认粒度；大 chunk 不能变成该句柄永久的最小读取量。

假设同一 block、无已有缓存、已分配足够连续范围，上限 2MiB：

| 调用实际需求 | 每次数据请求策略 |
|---|---|
| 4KiB | 保持原有 ≤128KiB chunk 行为 |
| 128KiB | 128KiB |
| 129KiB | 129KiB，不补到 2MiB |
| 1MiB | 1MiB |
| 8MiB | 四个 2MiB 数据请求 |

表中只计算数据请求，open/complete、跨 block 和旧 Worker 回退另计。预取已命中时先消费缓存，不能为了取得更大的 chunk 把已经读取的数据丢掉重读。

实现接口建议：

- 为 Reader 增加带大小提示的取数方法，默认实现转发旧方法，其他后端可保持行为。`read`、`read_full`、`async_read` 和 `fuse_read` 将实际剩余需求传入；无提示 API 保持默认。
- Java/native 和 Python/native 增加带长度的新入口，保留原入口以兼容旧调用方。Java 当前允许返回一个 chunk 的短读，优化不需要强行改变 `InputStream.read` 的语义。
- 为 Running 的 read header 增加可选长度字段，Worker 校验正数、可配置上限、协议上限和剩余边界。Open 响应可携带可选能力标志；旧 Worker 回退到 128KiB 多次读取，不为提高 chunk 而反复 open/close。
- 调度层将 demand 范围与推测性预读分开。大于现有 slice 的需求须重新分配连续 range，不能只放大 `FsReaderParallel` 的一次 read。seek/cancel 要清理旧位置的任务和数据，防止错位、重复读取和遗漏。

## 读放大与内存预算

存在至少三类不同的额外读取：RPC 超出调用需求的数据、client 后台预取、Worker/内核文件预读。扩大实际需求已达 2MiB 的请求到 2MiB，并不必然增加前一种放大；让每个 4KiB 随机请求都读 2MiB 则会明显放大。

当前配置初始化会把未显式指定的 `read_slice_size` 和 `read_ahead_len` 设为 `read_chunk_num × read_chunk_size`。全局把 chunk 从 128KiB 改为 2MiB，默认 8 个 chunk 就从 1MiB 变成 16MiB，后台预取窗口和内存需求也可能一起增长。按需方案应：

- 保持默认 chunk、现有预读范围独立于大 chunk 上限。
- 用字节预算限制预取，而不只限制队列中的 chunk 数；一条前台 2MiB 数据响应不意味着允许再预读 8 × 2MiB。
- 同时限制总在途字节数及并行 reader 数，单请求 cap 不是单文件/单进程内存上限。
- 随机读及 seek 后抑制推测性预读，并独立测量 Worker 的 cache advice 和实际设备 I/O。即使网络不多读，OS 仍可能读取额外页面。

FUSE 的实际需求应取 `op.arg.size`。当前代码通过 init 协商 `max_readahead` 和 `max_pages`，普通 4KiB 页下 daemon buffer 可以容纳约 1MiB payload；因此不能把 128KiB 当成所有机器上不变的 FUSE 协议上限。保留 128KiB 默认值与按实际请求大小调整并不冲突。

写入放大要另看：Writer flush 发送有效数据，不因 buffer 容量变大而补零写满。大 write chunk 主要改变聚合等待、内存和请求数。当前文件后端覆盖已提交 block 时会将旧文件复制到 staging；这类读写放大来自覆盖写语义，与把 chunk 调大是不同问题。

## 跨 AZ 的单 RPC 写入推理

对已知目标 Worker、完整 payload 可放入一次请求的小 block，设 R 为往返时延，T 为其余不能省掉的传输/服务/写入/提交成本。假设当前三个阶段串行、单 RPC 保持相同成功条件：

```text
open + write + complete ≈ 3R + T
PutBlock                ≈  R + T
节省                    ≈ 2R
```

当 R=1ms 时，下表仅为推导，不是跨 AZ 实测：

| T | 三阶段 | 单 RPC | 延迟降低 |
|---:|---:|---:|---:|
| 0.2ms | 3.2ms | 1.2ms | 62.5% |
| 1ms | 4ms | 2ms | 50% |
| 5ms | 8ms | 6ms | 25% |

如果“1ms 延迟”指单程，R 约为 2ms，省下约 4ms。T 越大，相对提升越小，但这不抹去串行控制往返的绝对成本。Master 请求和副本内部通信须另外计入，不能将这组推导直接当作整个文件写入的提升。

当前普通文件后端 `LocalFile` 包装 `std::fs::File`；`flush()` 转发 File.flush，finalize 发布做 rename。该路径没有生产代码的 sync_all/sync_data，测试辅助函数中的 sync_all 不代表 Worker 提交调用它。Rust 在 Unix 上的 File.flush 是 no-op，不能将已有微秒级写入数据解释成 fsync 完成延迟。[Rust File 实现](https://doc.rust-lang.org/src/std/fs.rs.html#1435-1450)、[sync_all 文档](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all)。上表也适用于未来明确要求 fsync 的实现，只需将实际同步成本加入 T。

单 RPC 写入仍须由服务端在响应前完成既定的写入/发布/副本要求。客户端需要完整 block payload 或有明确容量上限的延迟打开策略；提交应答丢失后的幂等重试、未完成数据清理、覆盖写 generation 和旧 Worker 能力回退需要一并设计。不能通过提前 ACK 取得这里声称的收益。

## 动态读取的验收标准

- 4KiB/128KiB 随机读：读取返回字节数和预取字节数不高于相同默认策略基线；分别统计 cache 命中和不命中。
- 129KiB/1MiB/2MiB/4MiB/8MiB SDK 请求：逐字节校验，记录真实 Worker 请求数；超 cap 自动分块，非整块尾部不为了填 buffer 多取。
- 大小请求交替、seek、EOF、稀疏文件、跨 block、跨 slice、并行读取、取消和错误重试都不出现重复/缺失/错位。
- 新旧 SDK/native、client/Worker 的组合测试；无新长度能力时明确回退固定 chunk。
- 测量 RPC 返回字节/应用返回字节、后台预读字节、设备实际读取字节、p50/p99、吞吐和峰值在途内存，不能只凭大文件吞吐验收。
- 分开测 loopback 和受控 RTT，记录热缓存/冷缓存及具体持久化策略。动态读取尚无性能提升结论。
