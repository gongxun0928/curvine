# RocksDB 使用方式与优化分析

## 1. 目的与范围

本文聚焦当前仓库中 **元数据层** 对 RocksDB 的使用方式，重点分析：

1. 当前到底用了 RocksDB 的哪些能力。
2. 当前没有用、但对 metadata workload 可能非常关键的能力有哪些。
3. 当前配置会带来哪些内存、CPU、读放大、写放大、尾延迟问题。
4. 结合本仓库的 key 设计，`prefix_scan` 到底算不算真正吃到了 RocksDB 的前缀优化。
5. 如果要优化，应该按什么阶段推进。

本文主要覆盖以下模块：

- `curvine-common/src/rocksdb/db_conf.rs`
- `curvine-common/src/rocksdb/db_engine.rs`
- `curvine-common/src/rocksdb/rocks_utils.rs`
- `curvine-server/src/master/meta/store/rocks_inode_store.rs`
- `curvine-server/src/master/meta/store/inode_store.rs`
- `curvine-common/src/conf/master_conf.rs`
- `curvine-common/src/conf/cluster_conf.rs`

本文不展开 Raft 日志 Rocks 存储的单独优化，只分析元数据主路径。

---

## 2. 当前 RocksDB 的角色定位

在当前实现中，RocksDB 不是一个“直接承载所有在线查询”的唯一索引层，而更像是：

- **元数据持久化后端**
- **完整 inode 与块位置信息的补全来源**
- **master 重启时重建目录树的恢复来源**

结合前一份 Inode 报告，可以把它理解成：

- 内存树负责快速路径级访问
- RocksDB 负责完整 inode、目录边、块位置的持久化与按需读取

因此当前 RocksDB workload 的特点很明显：

1. **大量小 value**
2. **大量点查**
3. **大量按前缀的有界扫描**
4. **少量全表扫描（恢复、hash、checkpoint 相关）**
5. **写操作通常由上层 `WriteBatch` 打包后提交**

---

## 3. 当前实现里实际用了哪些 RocksDB 能力

## 3.1 多列族（CF）

元数据 RocksDB 使用的列族在 `RocksInodeStore::new()` 中定义：

- `inodes`
- `edges`
- `block`
- `location`
- `common`

来源：`curvine-server/src/master/meta/store/rocks_inode_store.rs:28-46`

这几个 CF 的职责大致如下：

### `inodes`

保存完整 inode 记录，key 是 inode id 的大端字节序。

### `edges`

保存目录父子边，key 是：

- `parent_id(8 bytes, big-endian) + child_name(bytes)`

### `block`

保存：

- `(block_id, worker_id) -> BlockLocation`

### `location`

保存反向索引：

- `(worker_id, block_id) -> block_id`

### `common`

保存：

- mount table
- file lock

---

## 3.2 单键读写

`DBEngine` 封装了标准的：

- `put` / `put_cf`
- `get` / `get_cf`
- `delete` / `delete_cf`

来源：`curvine-common/src/rocksdb/db_engine.rs:89-140`

在元数据主路径里，最常见的是：

- `get_cf(CF_INODES, inode_id)`
- `put_cf(CF_COMMON, lock/mount_key, value)`
- `delete_cf(CF_EDGES, parent+name)`

---

## 3.3 前缀扫描 / 区间扫描 / 全表扫描

`DBEngine` 提供：

- `scan()`
- `range_scan()`
- `prefix_scan()`

来源：`db_engine.rs:143-199`

其中 `prefix_scan()` 的实现是：

1. `set_prefix_same_as_start(true)`
2. 计算 lower bound = prefix
3. 计算 upper bound = `calculate_end_bytes(prefix)`
4. 用 iterator 做有界扫描

关键代码：

```rust
pub fn prefix_scan<K>(&self, cf: &str, key: K) -> CommonResult<RocksIterator<'_>>
where
    K: AsRef<[u8]>,
{
    let mut opt = self.conf.create_read_opt();
    opt.set_prefix_same_as_start(true);

    let start = key.as_ref();
    let end = RocksUtils::calculate_end_bytes(start);
    opt.set_iterate_lower_bound(start);
    opt.set_iterate_upper_bound(end);

    let cf = self.cf(cf)?;
    let mode = IteratorMode::From(start, Direction::Forward);
    let iter = self.db.iterator_cf_opt(cf, opt, mode);
    Ok(RocksIterator { inner: iter })
}
```

来源：`curvine-common/src/rocksdb/db_engine.rs:173-188`

当前主要使用场景：

- 列目录：`edges` 按 parent id 做 prefix scan
- 查 block locations：`block` 按 block_id 做 prefix scan
- 查 worker blocks：`location` 按 worker_id 做 prefix scan
- mount table：`common` 按 prefix 做 prefix scan

---

## 3.4 WriteBatch

`RocksInodeStore` 使用：

- `WriteBatchWithTransaction<false>`

来源：`curvine-server/src/master/meta/store/rocks_inode_store.rs:236-321`

它的用途是：

- 把一组 inode / edge / block / location 的更新打成一个 batch
- 统一提交给 RocksDB

这意味着当前已经在利用 RocksDB 的“批量原子写入”能力，但：

- 不是事务 DB
- 没有看到更复杂的快照隔离或事务读写逻辑

---

## 3.5 checkpoint / flush

`DBEngine::create_checkpoint()` 会先：

1. `flush(true)`
2. `Checkpoint::new`
3. `create_checkpoint(path)`

来源：`db_engine.rs:209-234`

同时：

- `flush()` 会在 WAL 未禁用时先 `flush_wal`
- 再 `flush_mem`

来源：`db_engine.rs:267-272`

这说明当前已经把 RocksDB checkpoint 用作上层恢复/快照流程的一部分。

---

## 3.6 metrics

当前实现有较完整的 RocksDB 指标封装：

- block cache
- memtable
- table readers
- snapshots
- running flush/compaction
- pending compaction bytes
- live versions

来源：`db_engine.rs:318-453`

这个实现里有一个很重要的注释：

> `db_*` 下某些 property 仅代表 default CF，不代表整个多 CF DB

这说明作者已经意识到 **多列族 RocksDB 的指标容易被误判**。

---

## 3.7 `multi_get_cf` 已实现但没真正用起来

`DBEngine` 已有：

- `multi_get_cf`

来源：`db_engine.rs:293-311`

但从当前仓库检索结果看，元数据主路径没有明显使用它。

这意味着像这些路径：

- 一个目录下对多个 `FileEntry` 补全 inode
- 批量查询多个 inode id

目前仍然是：

- 多次 `get_cf`
- 而不是一次 `multi_get_cf`

---

## 4. 当前没有用、但很关键的 RocksDB 能力

这一部分是最值得关注的。

## 4.1 BlockBasedTableOptions / block cache 没有显式配置

`DBConf::create_db_opt()` 里只配置了：

- `allow_concurrent_memtable_write(false)`
- `create_if_missing`
- `create_missing_column_families`
- `max_open_files(-1)`
- compression
- `db_write_buffer_size`
- `write_buffer_size`

来源：`curvine-common/src/rocksdb/db_conf.rs:93-111`

没有看到：

- `BlockBasedTableOptions`
- `set_block_cache`
- `cache_index_and_filter_blocks`
- `pin_l0_filter_and_index_blocks_in_cache`
- block size
- metadata block size

这意味着：

- RocksDB 表格式和缓存基本使用默认值
- 没有针对 metadata workload 做专门调优

---

## 4.2 prefix extractor / prefix bloom 没有配置

当前 `prefix_scan()` 虽然设置了：

- `set_prefix_same_as_start(true)`

但 **没有看到任何 CF 设置 prefix extractor**。

这点很关键，因为 RocksDB 真正的前缀优化通常需要：

- 合适的 `SliceTransform`
- 配套 Bloom/filter

否则 `prefix_scan` 更像是：

- “有上下界的迭代器扫描”

而不是：

- “引擎层基于 prefix bloom/filter 的强剪枝”

---

## 4.3 没有显式使用 snapshot 做一致性读

当前元数据层没有看到：

- `db.snapshot()`
- 基于 snapshot 的一致性遍历

这意味着：

- 一致性主要靠上层锁和 Raft/journal 语义
- 不是靠 RocksDB snapshot 做多版本只读视图

这在当前架构下未必是错，但如果未来要做：

- 长时间遍历
- 异步只读副本
- 低锁读

就会受限。

---

## 4.4 没有针对 metadata 批量补全使用 `multi_get`

例如：

- `FsDir::list_status`
- `FsDir::list_options`

对每个 `FileEntry` 都单独 `get_inode`

而不是先收集 inode id 再 `multi_get_cf`

这在 RocksDB 层面会增加：

- API 往返
- per-key 处理成本
- cache miss 时的尾延迟

---

## 4.5 没有对 iterator read option 分场景调优

当前 `create_iterator_opt()` 一刀切：

```rust
opt.set_readahead_size(64 * 1024 * 1024);
```

来源：`db_conf.rs:118-121`

这对于：

- 恢复全表扫描
- hash 校验

可能有帮助，但对于：

- 普通目录扫描
- 较小范围的迭代

则可能太大。

---

## 5. 当前配置带来的主要问题

## 5.1 明确问题一：并发 memtable write 被禁用

`db_conf.rs:96`：

```rust
opt.set_allow_concurrent_memtable_write(false);
```

### 影响

对于并发写场景：

- memtable 写入更倾向串行
- 写线程更容易等待
- 上层元数据大锁持有时间可能被进一步放大

在这个仓库里，这个问题不会单独出现，而是和：

- `FsDir` 全局写锁
- journal / checkpoint

一起形成串行化链路。

### 判断

这是一个**中高风险性能问题**。

---

## 5.2 明确问题二：多 CF 共享同一套 Options，memtable 叠加明显

`create_cf_opt()`：

```rust
let opt = self.create_db_opt();
cfs.push((DEFAULT_FAMILY.to_string(), opt.clone()));
for family_name in &self.family_list {
    cfs.push((family_name.to_string(), opt.clone()));
}
```

来源：`db_conf.rs:132-141`

默认：

- `meta_write_buffer_size = 64MB`
- `meta_db_write_buffer_size = 0`

来源：

- `master_conf.rs:237-241`
- `cluster_conf.rs:251-257`

### 影响

每个 CF 都可能吃一份较大的 memtable。

在当前元数据 DB 中，至少有：

- default
- inodes
- edges
- block
- location
- common

即使不同时活跃，整体内存上界也会抬高。

### 判断

这是**中风险内存问题**。

---

## 5.3 明确问题三：`prefix_scan` 是“应用层前缀语义”，不是“完整 Rocks 优化”

当前 key 设计是合理的：

- `i64_to_bytes(parent_id)` 作为目录前缀
- `i64_str_to_bytes(parent_id, name)` 作为目录边 key
- `i64_u32_to_bytes(block_id, worker_id)` 作为块位置 key
- `u32_i64_to_bytes(worker_id, block_id)` 作为反向索引 key

来源：

- `rocks_utils.rs:25-149`

而 `calculate_end_bytes()` 用来生成 prefix 上界：

- `rocks_utils.rs:162-184`

### 说明

这让 `prefix_scan` 在逻辑上是对的：

- 目录下子项确实会被限制在某个 key 范围内

但如果没有：

- prefix extractor
- prefix bloom

那么 RocksDB 不能把它当成一个“前缀优化查询”去做强过滤。

### 结论

当前实现可以说：

- **prefix 语义成立**

但不能说：

- **prefix scan 已被 RocksDB 最佳化**

这两者是有本质差别的。

---

## 5.4 明确问题四：默认无压缩会放大 IO / SST 体积

默认：

- `meta_compression_type = "none"`

来源：`master_conf.rs:238-239`

### 影响

对于 metadata workload：

- value 通常不算大，但数量多
- 无压缩会增大 SST 体积
- compaction / page cache / 磁盘读写压力会提高

是否值得改成 LZ4 需要看：

- CPU 预算
- IO 瓶颈程度

但从经验上，metadata DB 用轻压缩通常是可以认真评估的。

---

## 5.5 明确问题五：iterator 64MB readahead 可能拉高尾延迟

来源：`db_conf.rs:118-121`

### 影响

对这些路径尤其明显：

- `iter_cf_opt`
- `cf_hash`
- 大范围恢复 / 扫描

如果磁盘较慢或数据集大：

- 预读会拉高 IO 峰值
- 也可能带来页缓存压力
- 某些场景下形成无意义的超量读取

### 判断

这是**中风险、偏扫描场景**的问题。

---

## 5.6 明确问题六：checkpoint 前强制 flush，对写尾延迟敏感

`create_checkpoint()` 会先 `flush(true)`：

- `db_engine.rs:209-226`

而 `JournalWriter::maybe_emit_snapshot()` 可能在元数据变更流中触发 checkpoint。

虽然 checkpoint 本身不一定高频，但一旦频繁触发，就会和：

- flush
- compaction
- 上层写入路径

一起放大尾延迟。

---

## 6. 结合元数据 workload 看当前 RocksDB 症状

## 6.1 目录遍历

路径：

- `edges` prefix scan
- 对每个 `FileEntry` 单独 `get_inode`

RocksDB 视角的症状：

- 小范围前缀扫描
- 多次点查
- cache miss 时尾延迟明显

如果没有 prefix bloom / block cache 调优，大目录遍历会偏吃亏。

---

## 6.2 热点 inode 状态查询

路径：

- `get_cf(CF_INODES, inode_id)`

如果热点 inode 多、cache 又没有针对性配置，那么：

- table reader
- block cache
- OS page cache

之间的边界不可控，表现会依赖默认值。

---

## 6.3 create / unlink / rename

这些路径通常是：

- 多个 CF 一起 batch 写
- 上层大锁持有
- journal / checkpoint 干预

RocksDB 层面表现为：

- 写批量不算小
- 但并发度低
- 更容易出现尾延迟，而不是纯吞吐瓶颈

---

## 6.4 块位置查询

路径：

- `block` prefix scan

这类查询是非常适合固定前缀优化的，但当前没有 prefix extractor。

所以这块也存在“逻辑支持前缀，物理没真正吃到引擎优化”的问题。

---

## 7. 当前实现里值得保留的部分

虽然问题不少，但也有几处实现是值得肯定的：

## 7.1 key 设计本身是清晰的

当前 key 编码是标准的大端有序设计：

- inode id
- parent + name
- block + worker
- worker + block

这让：

- range/prefix 查询天然可行
- key 排序符合访问模式

这是一个好的基础。

## 7.2 指标注释写得很清楚

`get_rocksdb_metrics()` 的文档把：

- `db_*`
- `cf_*`
- memtable
- table readers
- block cache

之间的关系讲得很明确。

这为后续调优提供了非常好的观测基础。

## 7.3 batch 写已经统一封装

`InodeWriteBatch` 把 inode / edge / location 的更新组合起来，后续做更深入优化时不需要重构整个写路径。

---

## 8. 分阶段优化建议

下面按“先低风险、后结构性优化”的顺序给建议。

## 8.1 第一阶段：先把观测和可配项做起来

### 建议

1. 把以下指标纳入常规观测：
   - `db_rocksdb_block-cache-usage`
   - 各 CF 的 `cf_*_rocksdb_size-all-mem-tables`
   - 各 CF 的 `cf_*_rocksdb_estimate-table-readers-mem`
   - `compaction-pending`
   - `estimate-pending-compaction-bytes`
   - `num-running-compactions`
2. 把 iterator readahead 改成可配置，而不是硬编码 64MB。
3. 对 metadata DB 评估 `LZ4` 压缩。
4. 评估 `allow_concurrent_memtable_write(true)` 的收益。

### 原因

这一步侵入性低，但能快速避免“根本看不清 RocksDB 在干什么”的状态。

---

## 8.2 第二阶段：针对前缀 workload 做真正的 Rocks 调优

### 建议

1. 对 `edges` CF 配置固定长度 prefix extractor（8 字节 parent id）。
2. 对 `block` / `location` 这类定长组合 key 也配置对应前缀提取。
3. 配置 Bloom / prefix bloom。
4. 引入显式 block cache，并根据 metadata 负载设容量。

### 预期收益

- 列目录更快
- block/location 查询更稳
- 读放大下降
- 冷读时尾延迟下降

---

## 8.3 第三阶段：减少上层对 RocksDB 的细碎访问

### 建议

1. 在 `list_status` / `list_options` 中把 `FileEntry` 批量补全改成 `multi_get_cf`。
2. 对热点目录的 inode 补全做批量或局部缓存。
3. 避免在一条调用链里多次重复反序列化同一个 inode。

### 预期收益

- 降低 API 往返成本
- 降低 RocksDB per-key overhead
- 改善大目录场景的尾延迟

---

## 8.4 第四阶段：重新评估元数据与 RocksDB 的边界

如果继续往前走，可以考虑：

1. 哪些 inode 字段必须跟着 `FileEntry` 补全，哪些可以做更轻量的 side-cache。
2. 是否要把热点目录做二级缓存而不是频繁 `get_inode`。
3. 是否要在更低锁粒度的 master 元数据模型下，配合 RocksDB snapshot 做更便宜的一致性读。

这一步已经是架构级优化，不是简单调参。

---

## 9. 结论

从当前代码看，RocksDB 的使用方式可以概括为：

1. **基础能力用得比较完整**：多 CF、batch、prefix scan、checkpoint、metrics 都有。
2. **针对 metadata workload 的“引擎级优化”明显不足**：
   - 没有 block-based table tuning
   - 没有 block cache 显式策略
   - 没有 prefix extractor / prefix bloom
   - 没有批量 multi_get 落地使用
3. **当前 `prefix_scan` 更准确地说是“应用层有界扫描”，而不是“RocksDB 已完成前缀优化”**
4. **当前最值得优先处理的风险点**是：
   - `allow_concurrent_memtable_write(false)`
   - 多 CF memtable 叠加
   - 默认无压缩
   - iterator 64MB readahead
   - `FileEntry` 补全时缺乏批量查询

所以这部分的关键结论不是“RocksDB 用错了”，而是：

> 现有 RocksDB 用法已经能支撑功能，但还停留在“可用层面”，离“针对元数据负载做过专门优化”的状态还有明显距离。
