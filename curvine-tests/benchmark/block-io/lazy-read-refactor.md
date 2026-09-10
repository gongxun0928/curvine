# Lazy small-block reads

本次在 `864dc895` 的单 RPC 实现上重构客户端生命周期。调用方仍使用 `new / seek / read / complete`；默认 chunk 保持 128KiB。

## 接口与状态

| 操作 | Once：整个逻辑 block 不超过一个 chunk | Streaming：原有分块读取 |
|---|---|---|
| `new()` | 获取连接，保存请求，不发送 block RPC | 获取连接并 Open |
| `seek(pos)` | 校验范围，只更新请求 offset | 校验范围，保存下一次 Read 的 offset |
| `read()` | 按当前 offset 发请求，新 Worker 一次返回数据并结束服务端会话 | 在已有会话上发送 Read |
| `complete()` | 切换为 Closed，不发 RPC | Complete 成功后切换为 Closed |
| Closed 后调用 | 重复 complete 是空操作；read/seek 报错 | 同左 |

`new()` 仍可能建立 TCP 连接。“不发 RPC”指不发送 block 请求。小块不存在、权限不足等 Worker 校验错误延后到实际 `read()`；外层 `BlockReader` 仍按该次读取的 offset 尝试其他副本。Closed 状态在进入副本重试前检查，避免关闭后重新打开会话。

EOF 和 Closed 分开：读到 EOF 后仍能 seek 并重新读取；只有显式 complete 才关闭客户端 reader。Streaming Complete 失败时保留会话状态，允许重试。外层 `BlockReader::complete()` 原有记录并吞掉清理错误的行为保持原样。

`ReadMode::{Streaming, Once, Closed}` 取代独立的 once 标志、预取 buffer 和完成标志。Once 不再持有无意义的 streaming header/seq_id。两条客户端路径共用 `is_read_once_eligible`；仍要求整个逻辑 block 能放入一个 chunk，以保证后续任意合法 seek 都可继续使用 Once。Worker 独立校验实际剩余范围。

旧 Worker 忽略可选字段并返回 Open 后，独立的 legacy 函数完成 Read/Complete。Streaming 与 legacy 共用 `ReadSession::next_seq_id()`。Read 失败也尝试清理；两者都失败时保留原读取错误，Complete 失败的连接不可重新入池使用。

Worker 通过 `release_session()` 清理文件、context、预读记录和缓存策略。一次读取仍返回拥有所有权的 bytes，保持关闭文件后的响应有效性。重置预读记录不会取消已经发给内核的 read-ahead。

指标分开记录：`OpenBlock` 保留会话建立含义，`ReadOnceBlock` 记录完整的小块操作（包含可能的 legacy 回退）；legacy 的 Read/Complete 原有指标仍保留。比较跨版本监控时需采用相应的操作标签。

## 正确性验证

本轮 client、worker 和 server 相关测试共 97 个通过、1 个已有 benchmark ignored，Clippy（两个改动 crate 的 all-targets）、格式和 diff 检查通过。详见 [验证记录](block-io-2026-09-10-results/lazy-validation.txt)。

新增/扩展的 6 个客户端回归测试核对协议请求、序号、返回字节和连接复用：

- `new → seek(17) → read`：4096 字节 block 只传输 4079 字节；新 Worker 为 1 次 RPC，旧 Worker 为 3 次。new/seek 均不发送 block RPC。
- 未读取即关闭，以及 seek 到 EOF 后关闭，均为 0 次 block RPC。
- 读到 EOF 后 seek 会获取新数据；非法 offset 在本地拒绝；重复 complete 不发送 RPC。
- 短响应在 read 时失败，位置不前移，清理后的连接可复用。
- legacy Read 错误仍发送 Complete；清理失败后换连接；同时失败时保留读取错误。
- 两种远程模式关闭后不触发副本重试；首次实际 Open 失败后在另一副本的相同 offset 重试成功。

真实 Worker 的原有集成测试继续覆盖稀疏尾部、旧协议、short-circuit 和 handler 释放。此前 RPC/protobuf 的验证记录保留在原始报告中，本次没有改动其生产代码。

## 性能结果与复现

以重构前的 eager 实现 `864dc895` 为基线，同一磁盘文件系统、CPU 2–5、128KiB chunk、sendfile 开启，两个场景各交替执行 3 轮。下面取各轮均值的中位数，口径与原报告一致：

| 场景 | block | eager | lazy | 读取延迟变化 |
|---|---|---:|---:|---:|
| new → seek(0) → read | 4KiB | 105.734µs | 60.225µs | −43.0% |
| new → seek(0) → read | 128KiB | 215.525µs | 106.850µs | −50.4% |
| 普通顺序读取 | 4KiB | 59.631µs | 60.704µs | +1.8% |
| 普通顺序读取 | 128KiB | 108.923µs | 106.129µs | −2.6% |
| 普通顺序读取 | 64MiB | 35844.306µs | 35548.023µs | −0.8% |

提前 seek 的小块读取确实减少了白读和一次往返；普通顺序读取没有稳定的额外加速结论。普通场景 4KiB 读 p99 为 69.282→71.239µs。数据是本机热缓存结果，不代表跨 AZ 或冷盘性能。

保留写入测量作为对照，不能把读取收益推广到写入：普通顺序场景 4KiB 写均值为 179.087→182.113µs（+1.7%），p99 为 195.864→215.126µs；提前 seek 的交替读写场景中，4KiB 写均值为 165.766→176.370µs（+6.4%）。本次没有改动写入实现，但读写交替调度与运行噪声仍会影响这些结果。每轮数据和二进制 SHA256 均已保留，不据此声称写入性能提升或尾延迟不变。

原始统计与摘要：[提前 seek](block-io-2026-09-10-results/lazy-seek-disk/summary.csv)、[普通顺序读取](block-io-2026-09-10-results/lazy-sequential-disk/summary.csv)。各目录还包含 6 份逐轮 CSV 和已去除机器目录的 metadata。

在 `864dc895` 和当前代码中使用相同的 `block_io_bench.rs`，分别构建 release 二进制。新增第五个位置参数控制首次 read 前是否 `seek(0)`；默认不 seek，原有测试口径保持不变。同位置 seek 在 eager 实现中也会丢弃已读数据，因而能直接复现本次修复。

```sh
python3 build/tests/benchmark_block_io.py \
  --before /tmp/curvine-rpc-perf-results/block-eager-seek \
  --after /tmp/curvine-rpc-perf-results/block-lazy \
  --output /tmp/block-io-lazy-seek --data-dir /tmp \
  --cpus 2-5 --rounds 3 --chunks 131072 --initial-seek
```

去掉 `--initial-seek` 可比较普通顺序读取。两边均计入 new、seek（若启用）、read 和 complete 的耗时，逐字节校验完整 block。沿用原报告的 loopback、热缓存、单连接限制。
