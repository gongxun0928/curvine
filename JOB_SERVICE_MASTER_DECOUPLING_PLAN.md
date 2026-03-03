# Job Service 解耦 Master：收敛方案与分阶段计划

## 结论：应该做，但必须先收敛一致性与恢复语义

基于当前代码实现（`master/job/*`、`worker/task/*`），解耦方向是正确的，且应尽快推进。原因：

- `SubmitJob/GetJobStatus/CancelJob/ReportTask` 直接挂在 Master RPC 路由上，离线作业与元数据主路径同进程竞争资源。
- Job 状态存于内存 `JobStore`，历史上无稳定落盘恢复闭环，Master 重启后可见性会丢失。
- 取消语义存在收敛风险：取消时旧实现直接改 `Canceled`，Worker 端任务取消信号也不完整，终态可解释性不足。

因此“做解耦”是必要项，不是可选优化项。

## 对原设计文稿的采纳与调整

## 采纳点（合理）

- 保持客户端外部 API 不变（`SubmitJob/GetJobStatus/CancelJob`）。
- 将 Job 生命周期控制面独立为 Scheduler。
- 明确终态与冲突裁决，优先保证一致性和可恢复性。
- 增加幂等/过期事件过滤与基础可观测项。

## 调整点（需要收敛）

- **不直接一步到位“全量迁移扫描/拆分到 Worker”**：应先完成状态机+持久化，再做进程边界拆分，否则故障定位与回滚成本过高。
- **取消冲突裁决需协议先行**：当前系统没有独立 `CancelResponse(success=true/false)` 语义，必须先补内部事件模型再谈“完成 vs 取消”竞态裁决。
- **避免把 Scheduler 重新绑定 Master 内部对象**：Scheduler 只能依赖稳定 RPC/存储契约，不能继续依赖 Master 内部数据结构。
- **`job_id` 与幂等键分离**：`job_id` 应唯一随机；`client_request_id` 用于幂等提交判重。不能继续把路径哈希当唯一主键。

## 已落地阶段

- **Phase-1（已完成）**：状态机与取消语义加固  
  commit: `0aed46c`
  - 新增状态：`Dispatching/Canceling/CancelFailed`
  - 终态不回退保护
  - 修复取消路径，Worker 取消上报闭环
  - 补充状态机/取消单测

- **Phase-2（已完成）**：Job 快照持久化与恢复  
  commit: `f1a2758`
  - JobStore 增加快照持久化
  - Master 启动自动恢复历史 Job
  - 任务状态更新触发快照刷新
  - 补充持久化 round-trip 单测

## 后续阶段计划（每阶段一个 commit）

## Phase-3：引入 Scheduler 协议层（同进程，先不拆进程）

目标：
- 在代码层抽出 Scheduler 协议接口：
  - Scheduler <- `SubmitJob/GetJobStatus/CancelJob`
  - Scheduler -> Worker: `AcceptJob/CancelJobToNode`
  - Worker -> Scheduler: `ReportJobEvent/CancelResponse`
- Master 的 Job RPC 仅做转发，不再直接编排任务细节。

验收：
- 现有 CLI 与 SDK 无感知变更。
- 回归测试通过，状态推进与现行为兼容。

## Phase-4：独立 Scheduler 进程化（边界真正落地）

目标：
- 新增 `scheduler` 服务形态（独立 RPC 端口）。
- Master 仅保留元数据路径，不再承载 Job 编排状态机。

验收：
- 关闭 Scheduler 时，`SubmitJob` 失败可解释；恢复后可继续查询/控制已有作业。
- Master 资源曲线中，Job 编排 CPU/内存占比明显下降。

## Phase-5：Worker 事件模型升级（attempt/epoch fencing）

目标：
- 事件携带 `job_id + task_id + epoch + attempt + event_time`。
- Scheduler 丢弃过期/重复/乱序事件，计数到指标。
- 明确 `CancelResponse` 与 `Completed` 冲突裁决日志。

验收：
- 乱序注入测试下终态不回退。
- `stale_event_drop_total`、`attempt_fence_reject_total` 指标可观测。

## Phase-6：扫描/拆分迁移到 TaskNode（执行面彻底下沉）

目标：
- Scheduler 只派发 Job，不做 UFS 深度扫描。
- TaskNode 分批扫描、分片执行并上报聚合事件。
- 引入扫描背压，避免单节点过载。

验收：
- 大目录压测下 Master 元数据尾延迟下降。
- Job 吞吐/失败恢复符合预期。

## Phase-7：旧路径下线与灰度切换

目标：
- 删除 Master 内旧编排代码与兼容分支。
- 灰度开关默认切到独立 Scheduler。

验收：
- 双周稳定运行，无高优先级回退。
- 关键指标满足 SLO：提交成功率、取消收敛率、状态查询一致性。

## 风险清单与控制

- **风险：协议升级导致旧 Worker 不兼容**  
  控制：版本握手 + 双协议窗口期。

- **风险：事件风暴导致 Scheduler 堵塞**  
  控制：事件限流、批量上报、状态压缩。

- **风险：持久化写放大**  
  控制：快照节流、增量 WAL、终态压缩。

## 里程碑退出条件（Definition of Done）

- Scheduler 崩溃恢复后，非终态 Job 可继续推进到终态。
- 取消语义可解释：`Canceled` 或 `CancelFailed`，且冲突有审计日志。
- Master 不再承担 Job 编排主循环，元数据路径性能受 Job 波动影响显著降低。
