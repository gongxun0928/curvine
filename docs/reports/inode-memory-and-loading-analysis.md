# InodeView / InodeStore 内存使用与加载时机分析

## 1. 目的与范围

本文聚焦 `curvine-server` 中元数据目录树的内存模型，重点解释以下问题：

1. `InodeView`、`InodeDir`、`InodeFile`、`FileEntry` 在内存中分别承担什么角色。
2. `InodeStore` / `RocksInodeStore` 与内存目录树之间如何分工。
3. `FileEntry` 何时创建、何时只作为轻量目录项存在、何时会被补全成完整 `InodeFile`。
4. `BlockMeta` / `blocks` / `BlockLocation` 在什么时机进入内存，什么时机只存在于 RocksDB。
5. 当前实现中有哪些内存放大点，以及这些放大点在什么操作路径上最明显。

本文仅基于仓库静态代码阅读，未运行系统或做实际压测。

---

## 2. 元数据分层模型

当前实现不是“纯 RocksDB 按需查询”的模式，而是典型的：

- **内存层**：`FsDir.root_dir` 持有完整命名空间树的根节点
- **持久化层**：`InodeStore` / `RocksInodeStore` 负责 inode、目录边、块位置等的落盘

关键结构：

- `FsDir`：`curvine-server/src/master/meta/fs_dir.rs:39-45`
- `InodeView`：`curvine-server/src/master/meta/inode/inode_view.rs:104-110`
- `InodeDir`：`curvine-server/src/master/meta/inode/inode_dir.rs:26-38`
- `InodeFile`：`curvine-server/src/master/meta/inode/inode_file.rs:31-55`
- `RocksInodeStore`：`curvine-server/src/master/meta/store/rocks_inode_store.rs:24-46`

`FsDir` 的关键字段如下：

```rust
pub struct FsDir {
    pub(crate) root_dir: InodeView,
    pub(crate) inode_id: InodeId,
    pub(crate) store: InodeStore,
    pub(crate) journal_writer: Arc<JournalWriter>,
    pub(crate) evictor: Arc<dyn Evictor>,
    pub(crate) op_id: AtomicCounter,
}
```

这意味着 master 会同时持有：

1. 一棵内存中的目录树
2. 一个 RocksDB 元数据存储句柄

二者不是互斥关系，而是“**内存树负责快速路径名访问，RocksDB 负责持久化与补全完整 inode**”。

---

## 3. `InodeView` 三种变体的职责

`InodeView` 定义如下：

```rust
pub enum InodeView {
    File(Box<NamedFile>) = 1,
    Dir(Box<NamedDir>) = 2,
    FileEntry(Box<NamedEntry>) = 3,
}
```

来源：`curvine-server/src/master/meta/inode/inode_view.rs:104-123`

### 3.1 `Dir`

`Dir` 代表完整目录 inode，内部持有 `InodeDir`，而 `InodeDir` 又持有目录孩子表：

```rust
pub struct InodeDir {
    pub(crate) id: i64,
    pub(crate) parent_id: i64,
    pub(crate) mtime: i64,
    pub(crate) atime: i64,
    pub(crate) nlink: u32,
    pub(crate) storage_policy: StoragePolicy,
    pub(crate) features: DirFeature,
    #[serde(skip)]
    children: InodeChildren,
}
```

来源：`curvine-server/src/master/meta/inode/inode_dir.rs:26-38`

注意两点：

1. `children` 在序列化时被 `#[serde(skip)]` 跳过，不直接写进 RocksDB。
2. 目录层级关系单独存放在 `edges` 列族，恢复时再重建内存树。

### 3.2 `File`

`File` 代表完整文件 inode，对应 `InodeFile`。这是真正包含文件元数据的结构：

- 文件长度 `len`
- 块大小 `block_size`
- 副本数 `replicas`
- 存储策略 `storage_policy`
- 扩展特性 `features`
- **块列表 `blocks: Vec<BlockMeta>`**
- 硬链接计数 `nlink`
- 可选 link target

来源：`curvine-server/src/master/meta/inode/inode_file.rs:31-55`

### 3.3 `FileEntry`

`FileEntry` 是轻量目录项，只保存：

- `name`
- `id`

定义见：`curvine-server/src/master/meta/inode/inode_view.rs:84-102`

它**不包含完整文件元数据**，也不包含块列表。它的主要目标是：

- 在目录树中降低文件子项的常驻内存占用
- 让目录映射里只保留“名字 + inode id”这种轻量信息

---

## 4. 为什么目录树里要把文件存成 `FileEntry`

核心逻辑在 `InodeChildren::add_child`：

```rust
if inode.is_file() {
    v.insert(Box::new(InodeView::new_entry(
        inode.name().to_string(),
        inode.id(),
    )));
    Ok(InodePtr::from_owned(*inode))
} else {
    let inserted = v.insert(inode);
    Ok(InodePtr::from_ref(inserted.as_ref()))
}
```

来源：`curvine-server/src/master/meta/inode/inodes_children.rs:174-188`

这里的语义很关键：

- **目录树内部真正长期保存的是 `FileEntry`**
- 但当前调用方立即拿到的返回值仍然是**完整 `File` inode**

换句话说，这是一种“**树内轻量存储 + 当前调用链继续使用完整对象**”的折中方案。

### 4.1 好处

如果一个目录下有很多普通文件，那么目录树内不会为每个文件都长期挂一份完整 `InodeFile`，从而减少：

- 文件级元数据常驻堆内存
- `Vec<BlockMeta>` 对目录树体积的放大

### 4.2 代价

凡是后续需要“完整文件元数据”的地方，都要再根据 `id` 去 `InodeStore` 拉一次完整 inode。

因此这个设计把问题从：

- “内存常驻很大”

变成了：

- “目录树更轻，但读目录/路径解析/状态查询时会有补全成本”

---

## 5. `FileEntry` 和 `InodeFile` 什么时候分别出现

这一部分是理解内存行为的关键。

## 5.1 创建文件时：当前调用链拿到 `InodeFile`，目录树里留下 `FileEntry`

创建文件走 `FsDir::create_file`：

- `fs_dir.rs:379-394`

流程：

1. 构造完整 `InodeFile`
2. 包装成 `InodeView::new_file`
3. 调用 `add_last_inode`

`add_last_inode` 里：

- `parent.add_child(child)?`
- `self.store.apply_add(parent.as_ref(), added.as_ref())?`
- `inp.append(added)?`

来源：`fs_dir.rs:396-430`

这里的 `added` 对于文件来说，是 `InodeChildren::add_child` 返回的**完整文件指针**；但父目录的 `children` 里，保存的是 `FileEntry`。

因此：

- **本次创建调用链中的 `InodePath` 最后一个 inode 是完整 `File`**
- **目录树长期保存的是 `FileEntry`**

## 5.2 master 重启恢复时：目录边先进树为 entry，再按需决定保留什么

恢复逻辑在 `InodeStore::create_tree()`：

- `curvine-server/src/master/meta/store/inode_store.rs:376-454`

步骤大致是：

1. 从 root inode 开始
2. 遍历 `edges`
3. 每个 child 先构造成 `InodeView::new_entry(child_name, child_id)`
4. 再 `get_inode(child_id)` 取完整 inode
5. 如果 child 是目录，则把完整目录挂进树
6. 如果 child 不是目录，则最终仍以 `file_entry` 形式进入目录树

关键片段：

```rust
let inode = if matches!(store_inode, InodeView::Dir(_)) {
    store_inode
} else {
    file_entry
};
parent.add_child(inode)?
```

来源：`inode_store.rs:413-431`

也就是说，恢复后：

- **目录节点**以完整 `Dir` 常驻
- **文件节点**默认以 `FileEntry` 常驻

## 5.3 路径解析时：遇到 `FileEntry` 才补全成完整 inode

逻辑在 `InodePath::resolve`：

- `curvine-server/src/master/meta/inode/inode_path.rs:33-92`

关键片段：

```rust
let resolved_inode = match cur_inode.as_ref() {
    FileEntry(f) => {
        match store.get_inode(f.id(), Some(f.name()))? {
            Some(full_inode) => InodePtr::from_owned(full_inode),
            None => return err_box!(...)
        }
    }
    _ => cur_inode.clone(),
};
inodes.push(resolved_inode);
```

这说明：

- `InodePath` 的 `inodes` 向量里，可能会装入**从 RocksDB 新反序列化出来的完整 inode**
- 这些完整 inode 只是当前解析路径的临时结果，并不会自动替换目录树里的 `FileEntry`

因此 `resolve` 会制造短生命周期对象分配。

## 5.4 目录列举时：对每个 `FileEntry` 做一次完整 inode 补全

逻辑在：

- `FsDir::list_status`：`fs_dir.rs:449-484`
- `FsDir::list_options`：`fs_dir.rs:498-542`

二者都会在遇到 `FileEntry` 时执行：

```rust
let inode_opt = self.store.get_inode(e.id, Some(&e.name))?;
```

这表示：

- 目录树内虽然只存轻量 entry
- 但 list/status 类接口又会逐项把完整 inode 反序列化回来

这就是典型的：

- **内存节省与访问成本互换**

对于大目录，这是一个很显著的 N+1 模式。

## 5.5 某些操作不接受 `FileEntry` 作为最终节点

例如：

- `FsDir::file_status` 遇到 `FileEntry` 会直接报错：`fs_dir.rs:433-446`
- `FsDir::reopen_file` 遇到 `FileEntry` 也直接报错：`fs_dir.rs:621-650`

这说明这类操作要求调用前已经通过路径解析拿到了完整 `File`。

---

## 6. `InodeStore` / `RocksInodeStore` 在元数据生命周期里的职责

`RocksInodeStore` 管理以下列族：

- `inodes`
- `edges`
- `block`
- `location`
- `common`

来源：`curvine-server/src/master/meta/store/rocks_inode_store.rs:28-44`

它的职责大体如下：

### 6.1 `inodes`

保存完整 inode 记录。

`write_inode` 直接序列化整个 `InodeView`：

```rust
pub fn write_inode(&mut self, inode: &InodeView) -> CommonResult<()> {
    let key = RocksUtils::i64_to_bytes(inode.id());
    let value = Serde::serialize(inode)?;
    self.put_cf(RocksInodeStore::CF_INODES, key, value)
}
```

来源：`rocks_inode_store.rs:280-285`

注意：

- 这里写的是完整 `File` / `Dir`
- **不是目录树里的 `FileEntry`**

### 6.2 `edges`

保存父子关系，key 是 `parent_id + child_name`：

- 编码：`RocksUtils::i64_str_to_bytes`，`rocks_utils.rs:89-95`
- 扫描：`prefix_scan(Self::CF_EDGES, RocksUtils::i64_to_bytes(id))`

也就是说目录层级不是嵌在 inode 记录中，而是拆成边关系单独存。

### 6.3 `block` / `location`

这两个列族保存块与 worker 的关系：

- `CF_BLOCK`：`(block_id, worker_id) -> BlockLocation`
- `CF_LOCATION`：`(worker_id, block_id) -> block_id`

相关实现：`rocks_inode_store.rs:268-317`

### 6.4 `common`

用于 mount table 和 lock 信息：

- mount：`PREFIX_MOUNT`
- file lock：`PREFIX_LOCK`

来源：`rocks_inode_store.rs:35-37, 148-205`

---

## 7. `BlockMeta`、`blocks`、`BlockLocation` 什么时候进入内存

这是第二个高频混淆点。

## 7.1 `InodeFile.blocks` 是文件 inode 的一部分

`InodeFile` 持有：

```rust
pub(crate) blocks: Vec<BlockMeta>,
```

来源：`inode_file.rs:46`

这意味着只要一个完整 `InodeFile` 被反序列化，**块列表就会一起进入内存**。

## 7.2 `BlockMeta` 自身可能带临时 `locs`

定义：

```rust
pub struct BlockMeta {
    pub(crate) id: i64,
    pub(crate) len: u32,
    pub(crate) replicas: u8,
    pub(crate) locs: Option<Vec<BlockLocation>>,
    pub(crate) alloc_opts: Option<FileAllocOpts>,
}
```

来源：`curvine-server/src/master/meta/block_meta.rs:28-36`

### 含义

- `locs` 并不是永远都有
- 它更像是创建/分配流程中的临时内存信息

## 7.3 预分配块时：`BlockMeta` 带 `locs`

新块分配走：

- `FsDir::acquire_new_block`：`fs_dir.rs:544-575`

其中：

```rust
file.add_block(BlockMeta::with_pre(new_block_id, choose_workers));
```

而 `BlockMeta::with_pre` 会把 worker id 先写进 `locs`：

```rust
pub fn with_pre(id: i64, workers: &[WorkerAddress]) -> Self {
    let locs = workers.iter().map(...).collect();
    Self { ..., locs: Some(locs), ... }
}
```

来源：`block_meta.rs:49-61`

也就是说：

- 新块在刚被分配时，内存中的 `BlockMeta` 可能带一份 location 列表

## 7.4 扩容/预分配文件长度时：`BlockMeta` 可能只带 `alloc_opts`

`InodeFile::extend()` 中，新块通过：

```rust
self.add_block(BlockMeta::with_alloc(new_block_id, block_opts));
```

来源：`inode_file.rs:416-439`

这类块：

- `locs = None`
- `alloc_opts = Some(...)`

代表先记录逻辑分配信息，尚未落成真实位置信息。

## 7.5 块提交后：`locs` / `alloc_opts` 会被清掉

`BlockMeta::commit()`：

```rust
pub fn commit(&mut self, commit: &CommitBlock) {
    self.len = ...
    let _ = self.locs.take();
    let _ = self.alloc_opts.take();
}
```

来源：`block_meta.rs:82-86`

也就是说：

- 提交完成后，`BlockMeta` 不再长期保留内存中的临时位置列表
- 块位置的权威来源转移到 RocksDB 的 `block` / `location` 列族

## 7.6 需要位置信息时，再从 RocksDB 展开

`InodeFile::get_locs()`：

```rust
if let Some(locs) = &meta.locs {
    res.insert(meta.id, locs.clone());
} else {
    let locs = store.get_locations(meta.id)?;
    ...
}
```

来源：`inode_file.rs:497-510`

`InodeStore::get_file_locations()` 也会对每个 block 去 `get_locations()`：

- `inode_store.rs:456-466`

因此位置数据的策略是：

- **块列表 `blocks` 常随完整 `InodeFile` 进入内存**
- **块位置 `BlockLocation` 不总是常驻在 inode 中**
- **提交后通常通过 RocksDB 按需加载**

---

## 8. 一条完整的“文件元数据生命周期”示例

下面用一条典型路径把上述机制串起来。

### 8.1 创建文件

1. `FsDir::create_file` 构造完整 `InodeFile`
2. `add_last_inode` 把完整文件加入父目录
3. 父目录 `children` 实际保存的是 `FileEntry`
4. RocksDB `inodes` 列族保存的是完整 `File`
5. RocksDB `edges` 列族保存的是目录边

### 8.2 目录树中长期驻留

1. 父目录里看到的是 `FileEntry(name, id)`
2. 完整 `InodeFile` 不作为子节点长期挂在目录树 map 中

### 8.3 后续路径解析

1. `InodePath::resolve` 遇到 `FileEntry`
2. `store.get_inode(id, Some(name))`
3. 反序列化出完整 `InodeFile`
4. 把这个完整 inode 放入当前 `InodePath.inodes`

### 8.4 列目录

1. 遍历目录孩子
2. 普通目录节点直接转 `FileStatus`
3. 文件孩子若是 `FileEntry`，则逐项 `get_inode`
4. 临时反序列化完整文件 inode，再转 `FileStatus`

### 8.5 获取 block location

1. 先拿完整文件 inode
2. 遍历 `file.blocks`
3. 若 `BlockMeta.locs` 为空，则对每个 block 去 RocksDB 查 `get_locations`

因此，“文件元数据”在系统里不是单次加载一次后永久复用，而是：

- 目录树中常驻轻量 entry
- 需要完整语义时反序列化完整 inode
- 需要物理位置信息时再继续展开到 `BlockLocation`

---

## 9. 额外内存放大点

除了 `FileEntry` / `InodeFile` 的主线外，还有若干次级放大项。

## 9.1 目录名字和路径字符串重复

`InodeChildren::Map` 的 key 是 `String`，而 `NamedFile` / `NamedDir` / `NamedEntry` 内部也持有 `name`。

此外 `InodePath` 还保存：

- `path: String`
- `name: String`
- `components: Vec<String>`
- `inodes: Vec<InodePtr>`

来源：

- `inodes_children.rs:26-29, 174-181`
- `inode_path.rs:25-30, 85-90`

这会在大目录、深路径下形成明显字符串冗余。

## 9.2 路径解析中的短生命周期 inode 对象

`InodePath::resolve` 遇到 `FileEntry` 就会 `get_inode` 并构造一个新的完整 `InodeView`。

这些对象：

- 不会自动替换树中的 `FileEntry`
- 常只是当前调用链临时使用

因此会增加分配/反序列化开销。

## 9.3 `children()` / `children_vec()` 这类 API 会额外拷贝

例如：

- `InodeView::children()` 注释明确写了会发生 memory copying：`inode_view.rs:195-205`
- `InodeDir::children_vec()` 会克隆孩子：`inode_dir.rs:122-124`

虽然不一定在主热路径，但用于调试、测试或某些高级操作时会有额外成本。

## 9.4 TTL bucket 是线性增长的附加索引

TTL 相关索引不是整 inode 缓存，而是：

- `BTreeMap<bucket_start, bucket>`
- 每个 bucket 里是 inode id 集合

这部分内存通常小于整树，但会随带 TTL 文件数线性增长。

## 9.5 重试缓存不是大头

`FsRetryCache` 只缓存：

- `req_id -> OperationStatus`

状态只有：

- `Init`
- `Success`
- `Failed`

来源：`curvine-server/src/master/fs/fs_retry_cache.rs:26-93`

因此它不是 inode 内存的主要来源。

---

## 10. 当前实现的核心取舍

如果把当前实现简化成一句话：

> 目录树用 `FileEntry` 降低文件子项常驻内存，完整文件 inode 与块位置转移为“按需从 RocksDB 补全”。

这套设计的优点是：

- 树本身更轻
- 大量普通文件不会把完整 `InodeFile.blocks` 全都挂在父目录 map 里

缺点是：

- `resolve` / `list_status` / `list_options` 会反复做 `get_inode`
- 大目录场景容易变成 N+1 RocksDB 读取
- 一次目录遍历会制造很多短生命周期反序列化对象

所以它不是简单的“节省内存”，而是更准确地说：

- **把内存压力转移成了部分读路径的 CPU / 反序列化 / RocksDB 压力**

---

## 11. 结论

从内存模型角度看，当前系统的关键事实可以概括为：

1. **目录树整体常驻内存**，master 不是纯按需元数据模式。
2. **目录节点长期是完整对象，文件节点长期多为 `FileEntry`**。
3. **完整 `InodeFile` 在创建、路径解析、列目录、重打开等路径上按需出现**。
4. **`BlockMeta` 常跟随完整 `InodeFile` 进入内存，但 `BlockLocation` 多数在提交后改为 RocksDB 按需加载**。
5. 当前内存放大的主要来源不是单一结构，而是：
   - 整棵目录树
   - 目录孩子表中的名字冗余
   - 路径对象中的字符串复制
   - `list_status` / `list_options` 对 `FileEntry` 的重复补全
   - 大文件 inode 自带的 `blocks: Vec<BlockMeta>`

如果后续要进一步优化内存，这一层最值得优先关注的就是：

1. `FileEntry -> get_inode` 的补全频率
2. 目录树中 name/path 的重复存储
3. block 元数据是否继续跟随完整 `InodeFile` 一起频繁反序列化
