# Block I/O review materials

此分支实现了小 block 远程读取的单 RPC 路径，并兼容旧 Worker。默认 chunk 仍为 128KiB。

- [Lazy 重构说明](lazy-read-refactor.md)：当前接口、状态机、legacy 错误清理和 97 个相关回归测试；真实 Worker 提前 seek 的小块读取延迟降低约 43–50%。
- [性能报告与复现方法](block-io-2026-09-10.md)：真实 Worker 的 loopback A/B 测量、适用范围和撤回的实验。
- [动态读取设计](demand-sized-read-design.md)：按请求长度选择 chunk、读放大控制和跨 AZ 单 RPC 写入分析；这些内容尚未实现。
- [重构前磁盘目录结果](block-io-2026-09-10-results/selected-disk-summary.csv)和[重构前 tmpfs 结果](block-io-2026-09-10-results/selected-tmpfs-summary.csv)。
- [重构前验证记录](block-io-2026-09-10-results/validation.txt)：142 个测试通过、1 个已有测试忽略，Clippy 和格式检查通过。

`selected-*` 对应 lazy 重构之前的单 RPC 实现（`27367378`）；当前重构的验证见上面的 Lazy 说明。`gather-*` 和实验 patch 用于记录已经撤回的聚合发送方案，不属于当前生产实现。记录中的机器目录已替换为存储类型说明，二进制文件名、SHA256 和测量值保持不变。

实验 patch 采用零行上下文格式，如需复现实验，需在独立 checkout 中使用 `git apply --unidiff-zero`。
