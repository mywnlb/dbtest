# Spec: T1.1+T1.2 — 事务核心 + 聚簇记录隐藏列

- 日期：2026-06-17
- 关联设计：`docs/design/innodb-transaction-mvcc-design.md` §5（核心领域模型）、§7.1-§7.3（生命周期）、§17（并发与锁顺序）、§20（实现顺序）；`docs/design/innodb-undo-log-purge-design.md` §5.1（RollPointer 值对象）、§6.4（undo page）；`docs/design/innodb-record-design.md` §6（隐藏列区域）、§7.1、§12.4。
- 前置：B3（`SplitCapableBTreeIndexService`，height-1 split）、record R1-R5、redo/recovery R1/R2。
- 状态：T1 事务/MVCC/undo epic 的第一片。建立事务生命周期词汇（纯内存），并让聚簇 conventional 记录携带 `DB_TRX_ID`/`DB_ROLL_PTR` 隐藏列，由聚簇 insert 用 `TransactionId` 盖戳。

## 1. 范围

**做：**

- 新增 `cn.zhangyis.db.domain` 值对象：`TransactionId`、`TransactionNo`、`RollPointer`（含 7 字节定长 codec 与 `NULL` 哨兵）。
- 新增 `cn.zhangyis.db.storage.trx`：`IsolationLevel`（枚举，本片仅记录不驱动行为）、`TransactionState`、`TransactionOptions`、`Transaction`、`TransactionSystem`（含其私有 `ActiveTransactionTable`）、`TransactionManager`、`TransactionStateException`。
- 事务生命周期（纯内存）：begin、commit、rollback；读写事务惰性分配 `TransactionId`；commit 给读写事务分配 `TransactionNo`；commit/rollback 把读写事务移出活跃表。
- record 层隐藏列：`TableSchema.clustered` 标志（兼容副构造器默认 false）、`LogicalRecord.hiddenColumns`、`HiddenColumnLayout`，并让 `RecordEncoder`/`RecordDecoder`/`RecordFieldResolver`/`RecordCursor` 读写聚簇记录尾部 15 字节隐藏区。
- B+Tree 聚簇盖戳：`BTreeIndex.clustered()`（从 `schema.clustered()` 派生）、`SplitCapableBTreeIndexService` 新增接受 `TransactionId` 的聚簇 insert 入口，写入时盖 `DB_TRX_ID=事务 id`、`DB_ROLL_PTR=RollPointer.NULL`。

**不做（本片非目标）：**

- **不做 ReadView / 可见性。** ReadView 没有 undo 就无法构造旧版本、无人消费，移到「可见性读路径」片（在 undo 之后），届时与 `lookup`/scan 过滤、旧版本构造一起落地。
- **不做 undo（紧接的下一片 T1.3）。** 因此本片 `DB_ROLL_PTR` 恒为 `NULL`；**commit/rollback 只翻转内存事务状态、移出活跃表，不撤销任何已写记录**——被 rollback 的事务插入的行仍物理留在页上、仍带它的 `DB_TRX_ID`。
- 不做事务化 storage API 包装层（T1.5）。聚簇 insert 直接收一个原始 `TransactionId`，不引入 `TransactionalSession`/`TransactionContext`、不引入 `current()`。
- 不做 `LockManager`、行锁、gap/next-key lock、死锁检测；唯一检查保持现有**物理重复 key 检查**，不做 MVCC-aware 唯一语义。
- 不做二级索引隐藏列、`DB_ROW_ID`、`PREPARED`/事务恢复、后台 purge、B+Tree height>1。
- 不实现 `READ_UNCOMMITTED`/`SERIALIZABLE`/RR/RC 的运行语义（`IsolationLevel` 仅作记录字段，无行为）。

## 2. 关键决策

1. **合并 T1.1+T1.2 为一份 spec，但剔除 ReadView**：先立事务生命周期词汇，同时让聚簇记录具备隐藏列并被盖戳，形成「begin → 聚簇 insert → 记录携带该事务 `DB_TRX_ID`」的可端到端测试的子回路。承重的 record 格式改动被单独隔离、全量回归保护。
2. **ReadView 推迟到 undo 之后的可见性片**：没有 undo 无法沿 `DB_ROLL_PTR` 构造旧版本，ReadView 这片无人消费、属于空转件，放到真正消费它的读路径片。
3. **undo 紧接本片（T1.3）且不可前置**：undo 物理上依赖隐藏列（INSERT undo 需记录有 `DB_ROLL_PTR` 可指；UPDATE/DELETE undo 需保存旧 `DB_TRX_ID`/`DB_ROLL_PTR`），故顺序为「隐藏列 → undo → 可见性」。
4. **隐藏列用独立隐藏区，不混进用户列 schema**（record-design §6 对齐）：物理布局 `[header][nullbitmap][varlen dir][用户字段][隐藏区 15B]`。`LogicalRecord` 另设 `hiddenColumns`，`materialize()` 绝不把隐藏列混进 `columnValues`，不泄漏给上层。B+Tree 取 key/用户列逻辑零改动。
5. **现有非聚簇 index 默认 `clustered=false`，字节布局完全不变**：通过 `TableSchema` 兼容副构造器保旧调用点不破；B3/record 既有测试与编码字节逐位不变；隐藏列由新增 clustered 测试覆盖。`schemaVersion` 仅对显式 clustered 的 schema bump。
6. **读写事务惰性分配 `TransactionId`**（§7.1）：只读事务保持 `TransactionId.NONE`（值 0），首次写入时由 manager 分配单调 id；只读事务调 `assignWriteId` 直接拒绝。
7. **本片 rollback ≠ 数据回滚**：无 undo，rollback 只做内存生命周期收尾。这是合并 T1.1+T1.2 但不含 undo 的必然中间态，必须在 spec/类注释醒目标注。
8. **clustered 单一权威态在 `TableSchema`**：`BTreeIndex.clustered()` 从 `schema.clustered()` 派生，不在 `BTreeIndex` 另立字段、不改其 canonical constructor。

## 3. domain 值对象

### `TransactionId`
- 不可变 `record TransactionId(long value)`，`value >= 0`。
- `0` 为 `NONE` 哨兵（只读/未分配写者）；提供 `TransactionId.NONE`、`isNone()`、`of(long)`。
- 对应聚簇记录隐藏列 `DB_TRX_ID`，8 字节无符号编码。

### `TransactionNo`
- 不可变 `record TransactionNo(long value)`，`value >= 0`，提交序号，commit 时给读写事务分配，单调。
- `0` 为 `NONE`：提供 `TransactionNo.NONE`、`isNone()`、`of(long)`。`Transaction` 在 commit 前 `transactionNo == NONE`，只读事务 commit 后仍 `NONE`。

### `RollPointer`
- 不可变值对象：`{boolean insert, PageNo pageNo, int offset}`。**不存 spaceId**：本片单 undo 表空间假设，space 由 undo 子系统隐含（注释标注；多 undo 表空间需改为 InnoDB 风格 rseg-id 编码，留 T1.3）。
- `RollPointer.NULL`：全零哨兵（`insert=false, pageNo=0, offset=0`）；`isNull()`。undo page 0 保留作头页，故全零与任何真实 undo 记录位置无歧义。本片所有聚簇记录 `DB_ROLL_PTR` 均为 `NULL`（无 undo）。
- **7 字节（56 bit）定长 codec，big-endian，与 InnoDB 二进制不兼容（注释标注）：**
  - byte0：bit7 = insert flag；bit6..0 = reserved，必须为 0（解码非 0 抛 `DatabaseValidationException`）。
  - byte1..4：`pageNo` 作 u32（范围 `[0, 2^32-1]`，越界抛 `DatabaseValidationException`）。
  - byte5..6：`offset` 作 u16（范围 `[0, 2^16-1]`，越界抛 `DatabaseValidationException`）。
  - `encode(RollPointer)->byte[7]` / `decode(byte[7],off)->RollPointer`；缓冲不足 7 字节抛 `DatabaseValidationException`。

## 4. storage.trx 核心（T1.1）

### `IsolationLevel`
枚举四级 `READ_UNCOMMITTED / READ_COMMITTED / REPEATABLE_READ / SERIALIZABLE`。本片仅作记录字段（begin 时落到 `Transaction`），**不驱动任何行为**（无 ReadView）。注释标注：隔离语义随可见性片接入。

### `TransactionState`
枚举仅 5 个流转态：`ACTIVE / COMMITTING / COMMITTED / ROLLING_BACK / ROLLED_BACK`（**本片不引入 `PREPARED`/`RECOVERED_ACTIVE`**，避免暗示未实现的恢复语义；待恢复片再加）。合法转换：`ACTIVE→COMMITTING→COMMITTED`、`ACTIVE→ROLLING_BACK→ROLLED_BACK`。非法转换抛 `TransactionStateException`，由状态机集中校验。

### `TransactionOptions`
不可变 `record TransactionOptions(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)`。
- `TransactionOptions.defaults()` = `(REPEATABLE_READ, readOnly=false, autoCommit=true)`。
- null `isolationLevel` 抛 `DatabaseValidationException`。

### `Transaction`
事务聚合：`transactionId`（惰性，初始 `NONE`）、`transactionNo`（commit 前 `NONE`）、`state`、`isolationLevel`、`readOnly`、`autoCommit`、`startTimeMillis`。

- 单 owner 线程使用（一个 session 一个事务）；状态由 `TransactionManager` 经状态机转换，不暴露公共 setter。
- 不持有 `BufferFrame`/`PageGuard`、不含 `readView` 字段（ReadView 推迟）。

### `TransactionSystem`
全局协调器，`ReentrantLock` 短锁保护：`nextTransactionId`、`nextTransactionNo`、以及其**私有** `ActiveTransactionTable`。

- `allocateTransactionId()`、`allocateTransactionNo()`：锁内自增。
- `registerActive(txnId)` / `removeActive(txnId)` / `snapshotActiveReadWriteIds()`：均锁内完成；snapshot 返回不可变拷贝后立即释放锁。
- 持锁期间不访问 Buffer Pool、不持 page latch、不等待（§17）。

#### `ActiveTransactionTable`（`TransactionSystem` 私有/包内可见）
活跃读写事务 id 的有序集合。**仅由 `TransactionSystem` 在其锁内调用**，不对外暴露为独立可变 owner（避免双重 owner）。`snapshot()` 返回不可变拷贝，供后续 undo/可见性片消费；本片无 ReadView 消费，仅测试断言。

### `TransactionManager`（Facade）
- `begin(TransactionOptions)`：建 `ACTIVE` 事务，落隔离级别/readOnly/autoCommit；读写事务暂不分配 id。
- `assignWriteId(txn)`：首次写入调用，幂等（已分配返回原 id）；分配单调 id 并 `registerActive`。**对 `readOnly` 事务调用直接抛 `TransactionStateException`**。
- `commit(txn)`：`ACTIVE→COMMITTING`→（读写事务）分配 `transactionNo` 并 `removeActive`；（只读事务，id 仍 `NONE`）不分配 `transactionNo`、无需 removeActive→`COMMITTED`。**不刷数据页、不撤销记录**。
- `rollback(txn)`：`ACTIVE→ROLLING_BACK`→（读写事务）`removeActive`→`ROLLED_BACK`。**不撤销已写记录**（无 undo）。
- **本片不提供 `current()`**（不引入 ThreadLocal 绑定/嵌套 begin/线程切换语义）；调用方显式持有并传递 `Transaction`。

## 5. record 隐藏列（T1.2）

### 物理布局
`[FIL 信封][record header][null bitmap][varlen dir][用户字段区][隐藏区 15B]`
隐藏区 = `DB_TRX_ID`(8B 无符号) + `DB_ROLL_PTR`(7B)。**仅 clustered leaf CONVENTIONAL 记录写隐藏区。**

### `HiddenColumnLayout`（record.format 新增）
- `HIDDEN_BYTES = 15`、`DB_TRX_ID_OFFSET=0`、`DB_ROLL_PTR_OFFSET=8`。
- `encode(buf, off, TransactionId, RollPointer)` / `decodeTrxId(buf,off)` / `decodeRollPtr(buf,off)`。

### `TableSchema`
新增 canonical 组件 `clustered`：`TableSchema(long schemaVersion, List<ColumnDef> columns, boolean clustered)`。
- **兼容副构造器** `TableSchema(long, List<ColumnDef>)` 委托 `clustered=false`，保 B1/B2/B3 等既有调用点源码不破。
- clustered schema 的 `schemaVersion` 显式 bump。

### `LogicalRecord`
新增 `HiddenColumns hiddenColumns`（值对象 `{TransactionId dbTrxId, RollPointer dbRollPtr}`，非聚簇为 `null`）。
- **`LogicalRecord` 只校验自身形状**：`hiddenColumns` 在场时 `dbTrxId`/`dbRollPtr` 均非 null。
- **clustered ⇔ hiddenColumns 在场的一致性不在 `LogicalRecord` 校验**（它不持有 schema），改由持有 schema 的 `RecordEncoder`（编码时）和 `RecordDecoder`（解码时）校验，不一致抛 `DatabaseValidationException`。

### 编解码改动
- `RecordEncoder`：`schema.clustered()` 为真时在用户字段区之后追加 15B 隐藏区，`recordLength` +15；同时校验 `LogicalRecord.hiddenColumns` 与 `clustered` 一致。
- `RecordDecoder`/`RecordFieldResolver`：clustered 时解析尾部 15B；隐藏列**不进入** `columnValues`，单列在 `hiddenColumns`。**校验隐藏区正好位于记录尾部**：clustered 记录长度须恰为「用户字段末尾 + 15」，非聚簇记录不得有尾随隐藏字节，违反抛 `RecordFormatException`。
- `RecordCursor`：新增 `dbTrxId()`、`dbRollPtr()`；`materialize()` 产出的 `LogicalRecord` 把隐藏列放入 `hiddenColumns`，`columnValues` 只含用户列。
- 非聚簇路径（clustered=false）：编解码字节与现状逐位一致。

### 隐藏列保留不变量（关键）
clustered 记录的隐藏列在**所有重编码/搬运路径**必须保留：
- **本片在用且必测**：B+Tree split 的 `materialize→reinsert`（`SplitCapableBTreeIndexService.materializeLeafRecords` 走 `RecordCursor.materialize()`，再 `RecordPageInserter.insert` 重编码）。故 `materialize()` 必须带 `hiddenColumns`、encoder 必须按 clustered 写出，否则 split 静默丢 `DB_TRX_ID`。
- **本片未触发但需保持的约束（写进注释，供 T1.3+）**：`RecordPageUpdater`（原地/搬迁）、`RecordPageReorganizer`（按链快照重排）、`RecordPagePurger` 均按整条记录字节搬运（含尾部 +15），不得从丢失隐藏列的 `LogicalRecord` 重编码。

## 6. B+Tree 聚簇盖戳（T1.2 接入）

- `BTreeIndex` 新增**派生**方法 `clustered()` 返回 `schema.clustered()`——**不加构造器参数、不改 canonical constructor**，clustered 单一权威态在 `TableSchema`。
- node-pointer 派生 schema（`pointerSchema`）**恒 `clustered=false`**：根/非叶页的 node pointer 记录永远不追加 15B 隐藏区。系统记录（infimum/supremum）绕过 encoder，不受影响。隐藏列只用于 clustered leaf CONVENTIONAL 记录。
- `SplitCapableBTreeIndexService` 新增聚簇 insert 入口（命名候选 `insertClustered(mtr, index, record, transactionId)`，计划阶段定稿）：
  1. 校验 `index.clustered()` 为真、`transactionId` 非 `NONE`。
  2. 用 `hiddenColumns(dbTrxId=transactionId, dbRollPtr=RollPointer.NULL)` 组装 `LogicalRecord`。
  3. 走现有 `RecordPageInserter`/split 路径（盖戳只改记录内容，split/redo 行为不变；split 经 §5 保留不变量带住隐藏列）。
- **事务 id 来源写死**：调用方先 `TransactionManager.assignWriteId(txn)`，再把 `txn.transactionId()` 传入；**B+Tree 只验非 NONE，不依赖 `TransactionManager`**（保持 btree 与 trx 管理解耦）。
- 非聚簇 insert 路径保持原签名不变。`lookup`/`scan` 本片不变（不消费可见性）。唯一检查保持现有物理重复 key 检查。

## 7. 错误模型

- `TransactionStateException`（extends `DatabaseRuntimeException`）：非法状态转换；对 `readOnly` 事务调 `assignWriteId`；对已 `COMMITTING`/`COMMITTED`/`ROLLED_BACK` 事务再操作。
- `DatabaseValidationException`：null 入参；`schema.clustered` 与 `hiddenColumns` 不一致（encoder/decoder 处）；聚簇 insert 收到 `NONE` 事务 id；`RollPointer` reserved 位非 0 / pageNo/offset 越界 / 缓冲不足。
- `RecordFormatException`：解码时隐藏区不在记录尾部（clustered 长度不符 / 非聚簇有尾随字节）。
- 复用既有 B+Tree 异常；不新增其它 record 物理结构异常。

## 8. 并发与边界

- `TransactionSystem` 短锁（`ReentrantLock` + `try/finally`，无 `synchronized`）：id/no 分配与活跃表读写均锁内完成，`snapshot`/`snapshotActiveReadWriteIds` 拷贝后立即释放，**期间不持 page latch、不做 IO、不等待**（§17）。`ActiveTransactionTable` 只由 `TransactionSystem` 在锁内驱动，无第二 owner。
- `Transaction` 单 owner 线程；状态转换经 manager 串行，不跨线程共享可变状态。
- record 编解码在调用方持有的单页 latch 内（沿用 R3-R5 约束），本片不改并发模型。
- 无可阻塞等待引入。

## 9. 恢复边界

本片不新增任何 redo/恢复语义：事务系统纯内存，聚簇记录隐藏区作为页内字节由现有 `PAGE_INIT/PAGE_BYTES` + pageLSN 幂等覆盖。crash 后事务状态全部丢失（无 PREPARED/recovery），与 trx-mvcc §12 的事务恢复留 T1 后续片一致。

## 10. 测试

- 值对象：`TransactionId.NONE`/边界；`TransactionNo.NONE`/单调；`RollPointer` 7B 往返、`NULL` 全零、reserved 位非 0 拒绝、pageNo/offset 越界拒绝、缓冲不足拒绝。
- `TransactionState` 状态机：合法转换通过、非法转换抛 `TransactionStateException`。
- `TransactionOptions`：`defaults()` 值正确、null isolation 拒绝。
- `TransactionManager`：begin→commit（读写事务分配 `transactionNo`、移出活跃表）、begin→rollback、惰性 `assignWriteId` 幂等、**readOnly 事务 `assignWriteId` 拒绝**、**只读事务 commit 不分配 `transactionNo`**。
- `TransactionSystem`/`ActiveTransactionTable`：注册/移除、`snapshot` 拷贝隔离（拷贝后外部增删不影响快照）、id/no 单调。
- record 隐藏列：clustered 记录 encode→decode→cursor 读回 `DB_TRX_ID`/`DB_ROLL_PTR`；`materialize()` 的 `columnValues` 不含隐藏列、`hiddenColumns` 正确；encoder/decoder 对 clustered/`hiddenColumns` 不一致拒绝；**decoder 拒绝隐藏区不在尾部**（clustered 长度不符 / 非聚簇有尾随）；**非聚簇记录编码字节与现状逐位一致**（回归保护）。
- B+Tree 聚簇 insert 端到端：begin 事务 →（assignWriteId）→聚簇 insert → 读回 `DB_TRX_ID == 事务 id`、`DB_ROLL_PTR.isNull()`；**聚簇 insert 触发 split 后两个子 leaf 上的记录都仍带正确 `DB_TRX_ID`**（隐藏列保留不变量）；**root/node-pointer 记录不含隐藏区**；非聚簇 insert 路径不变。
- 回归：全量 Gradle `test` 通过，测试数不倒退（具体数由实际输出记录，不在 spec 写死）。

## 11. 简化点与后续

- ReadView/可见性移到 undo 之后的可见性读路径片（lookup→可见性→沿 `DB_ROLL_PTR` 构造旧版本）。
- rollback 不撤销数据（T1.3 undo 接入后才有真正回滚）。
- 聚簇 insert 收原始 `TransactionId`（T1.5：事务化 storage API 包住，SQL/executor 不碰 MTR/page）。
- `IsolationLevel`/RU/SERIALIZABLE/RR/RC 仅记录字段；语义随可见性片与 LockManager 片接入。
- `RollPointer` 仅 `NULL`、单 undo 表空间假设；真实 undo 位置编码与多表空间 rseg-id 编码在 T1.3 启用。
- 二级索引隐藏列、`DB_ROW_ID`、PREPARED/事务恢复、purge、`current()` 留后续。

## 12. 十三点自检

1. 范围限定 trx 生命周期 + 隐藏列 + 盖戳；ReadView/undo/可见性/事务 API 明确为非目标。
2. ReadView 推迟到其消费方（可见性读路径片）。
3. undo 顺序在隐藏列之后、可见性之前，依赖不可颠倒。
4. 隐藏列用独立区域，不污染用户列、不泄漏给 `materialize()`。
5. `LogicalRecord` 只校验自身形状，clustered 一致性由持 schema 的 encoder/decoder 校验（落地正确）。
6. `TableSchema` 兼容副构造器默认 false，非聚簇字节零变化，B3/record 回归不破。
7. `BTreeIndex.clustered()` 从 schema 派生，单一权威态，不改 canonical constructor。
8. node-pointer schema 恒非 clustered，根/非叶页不会被误加 15B；系统记录绕过 encoder。
9. 隐藏列保留不变量覆盖 split materialize→reinsert（本片必测）及 updater/reorganizer/purger 字节搬运约束。
10. 读写事务惰性分配 id、readOnly 拒绝写 id、只读 commit 不分配 no；`TransactionNo.NONE` 已定义。
11. `RollPointer` 7B 布局具体可落地（1+7bit+u32+u16）、range 校验、NULL 全零、空间隐含已注明。
12. `TransactionSystem` 短锁独占活跃表、拷后即释放、不持 latch/不 IO；无 `synchronized`、无可阻塞等待；删 `current()`。
13. 本片不新增 redo/恢复语义，隐藏区靠现有物理 redo 幂等覆盖；文档引用已更正为 §17 等实际章节。
