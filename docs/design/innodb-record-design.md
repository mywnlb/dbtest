# MiniMySQL InnoDB 风格 Record 模块设计

版本：2026-06-05  
实现语言：Java  
参考基线：MySQL 8.0.46 InnoDB 官方手册与源码文档  
关联设计：[innodb-disk-manager-design.md](innodb-disk-manager-design.md)、[innodb-buffer-pool-design.md](innodb-buffer-pool-design.md)、[innodb-transaction-mvcc-design.md](innodb-transaction-mvcc-design.md)、[innodb-undo-log-purge-design.md](innodb-undo-log-purge-design.md)

## 1. 目标与边界

本设计面向 MiniMySQL 存储引擎的 `storage.record` 模块。Disk Manager 负责把 page、extent、segment 映射到表空间文件；Buffer Pool 负责把 page 缓存在内存 frame 中；Record 模块负责解释索引页内的记录字节、维护页内逻辑顺序、编码和比较字段值，并为 B+Tree、MVCC、undo、redo 提供稳定的记录访问边界。

设计目标：

- 高内聚：记录头、字段布局、NULL bitmap、变长字段目录、隐藏列、页内目录、类型编码、比较器、insert/update/delete-mark/purge 都收敛在 `storage.record` 内部。
- 低耦合：B+Tree 只通过 `RecordPageAccessor`、`RecordCursor`、`RecordComparator` 操作页内记录，不直接解析 byte array；事务模块只依赖隐藏列访问接口，不关心物理编码细节。
- 物理/逻辑分离：物理记录是页内 byte layout 和 offset；逻辑记录是 schema 下的列值、隐藏列和可见性元数据。
- InnoDB 风格：保留 `heap_no`、`deleted_flag`、`next_record`、`n_owned`、`PageDirectory`、`infimum/supremum`、聚簇索引隐藏列、delete-mark 与 purge 的核心思想。
- Java 可落地：用值对象、策略、模板方法、工厂、访问者、命令、组合和适配器表达类型系统与记录操作。

非目标：

- 不实现 SQL parser、执行器、完整 B+Tree 分裂算法和完整 MVCC 决策。
- 不追求与 InnoDB COMPACT/DYNAMIC 行格式二进制兼容。
- 不在 Record 模块直接读写文件、BufferFrame、extent bitmap 或 segment inode。
- 第一阶段不实现完整 off-page LOB 链；只定义可扩展接口和 inline/overflow 边界。
- 不生成 Java 源码。本文件只定义设计、关系和数据流。

## 2. MySQL 8.0 参考依据

本设计参考以下 MySQL 8.0 行为：

- InnoDB 索引页中记录按 heap 存储，`heap_no` 表示页内物理序号，不等同于 key 顺序。
- 页内逻辑顺序由记录头中的 `next_record` 串成单向链表，`infimum` 和 `supremum` 作为边界记录。
- Page Directory 保存稀疏槽，每个槽指向一组记录的最后一条记录；页内查找先二分 directory，再组内顺序扫描。
- 记录头包含 delete-mark、min-rec、record type、heap no、n-owned、next record offset 等信息。
- 聚簇索引记录保存隐藏列 `DB_TRX_ID`、`DB_ROLL_PTR`，必要时保存 `DB_ROW_ID`。
- 删除先设置 delete-mark；purge 在没有活跃 ReadView 需要旧版本后再物理移除记录。
- 变长列、NULL 列和字符集/排序规则会影响记录编码、长度计算和索引比较。
- Redo 记录物理页修改，undo 保存回滚和旧版本构造所需的逻辑前镜像。

## 3. 总体架构

架构图见 [record-architecture.mmd](diagrams/record-architecture.mmd)。

`storage.record` 分为九组：

1. `storage.record.api`：对 B+Tree、事务、执行器暴露稳定入口。
2. `storage.record.schema`：表结构、列定义、索引 key part、schema version。
3. `storage.record.type`：数据类型描述符、编码器、解码器、比较器、排序规则。
4. `storage.record.format`：物理记录头、NULL bitmap、变长字段目录、字段 offset。
5. `storage.record.page`：页内 heap、free list、PageDirectory、infimum/supremum。
6. `storage.record.cursor`：记录定位、字段读取、隐藏列读取、受控写入。
7. `storage.record.op`：insert、update、delete-mark、purge、page reorganize。
8. `storage.record.mvcc`：隐藏列适配、RecordVersion 快照、undo payload 构造边界。
9. `storage.record.metric`：页内碎片、重组、编码、比较统计。

核心原则：

- `storage.record` 通过 `PageCursor` 读写 page image，不持有裸 `BufferFrame`。
- `storage.record` 不分配 page、extent 或 segment；需要新页时由 B+Tree 调用 Disk Manager。
- `storage.record` 不决定事务可见性，只提供 `DB_TRX_ID`、`DB_ROLL_PTR`、delete-mark 和当前记录镜像。
- `storage.record` 不直接生成 redo log block；它把页内修改表达为 `RecordPageOperation`，由 MTR/redo 模块编码成可恢复的物理修改。
- `storage.record.type` 是可插拔类型系统，字段编码、比较和长度计算不能散落在 B+Tree 或事务模块。

## 4. 包与职责

| 包 | 职责 | 主要依赖 | 设计模式 |
| --- | --- | --- | --- |
| `storage.record.api` | 记录层门面、页访问入口、错误模型 | `buf`, `mtr`, `schema` | Facade |
| `storage.record.schema` | `TableSchema`、`ColumnDef`、`IndexKeyDef`、schema version | `domain` | Value Object, Builder |
| `storage.record.type` | 类型描述符、编码、解码、比较、排序规则 | `schema`, `config` | Strategy, Factory, Flyweight |
| `storage.record.format` | 记录头、NULL bitmap、变长目录、字段定位 | `type` | Template Method, Composite |
| `storage.record.page` | heap 管理、PageDirectory、组内链表、空间重组 | `format`, `buf` | Repository, Policy |
| `storage.record.cursor` | 字段级读写、隐藏列访问、record 生命周期保护 | `page`, `type` | Cursor, Adapter |
| `storage.record.op` | insert/update/delete/purge 操作模板 | `cursor`, `mtr`, `trx` | Template Method, Command |
| `storage.record.mvcc` | `RecordVersion`、隐藏列适配、undo payload | `trx`, `type` | Adapter, Snapshot |
| `storage.record.metric` | 类型编码、比较、页内碎片指标 | 无 | Observer |

推荐依赖方向：

`btree -> record.api -> record.page -> record.format -> record.type`  
`trx.mvcc/undo -> record.mvcc + record.cursor`  
`record.op -> mtr + buf.api + redo command boundary`  
`record.page -> PageCursor`  

禁止方向：

- `record` 不能 import `storage.fsp`、`storage.fil`、`storage.sql.parser`。
- `record.type` 不能依赖 page、B+Tree、事务或 redo。
- `record.page` 不能决定字段语义，只能调用 type codec 和 comparator。
- `record.mvcc` 不能判断隔离级别；可见性由事务模块决定。
- `buf` 不能 import `record`。

## 5. 核心领域模型

类关系图见 [record-class-relation.mmd](diagrams/record-class-relation.mmd)。

### 5.1 RecordRef

`RecordRef` 是页内记录的稳定定位值：

- `pageId`
- `heapNo`
- `pageOffset`
- `schemaVersion`
- `indexId`

规则：

- `pageOffset` 是物理定位，可能在页重组后变化。
- `heapNo` 是页内物理序号，不代表 key 顺序。
- B+Tree 长期持有记录位置时优先保存 key 或主键，不应长期依赖 `pageOffset`。
- purge、page reorganize 或 page split 后，旧 `RecordRef` 必须重新验证。

### 5.2 PhysicalRecord

`PhysicalRecord` 表示页内物理记录：

- `recordHeader`
- `nullBitmap`
- `varLengthDirectory`
- `fixedFieldArea`
- `variableFieldArea`
- `hiddenColumnArea`

`RecordHeader`：

- `deletedFlag`
- `minRecFlag`
- `recordType`: `CONVENTIONAL`, `NODE_POINTER`, `INFIMUM`, `SUPREMUM`
- `heapNo`
- `nOwned`
- `nextRecordOffset`
- `recordLength`

约束：

- 物理记录只描述 byte layout，不暴露业务列名。
- 修改 `nextRecordOffset`、`nOwned`、`deletedFlag` 必须通过 `RecordPageAccessor`。
- `recordLength` 由 schema、NULL bitmap、变长字段目录和字段 payload 共同计算。
- `INFIMUM` 和 `SUPREMUM` 是特殊记录，不参与 MVCC，也不保存用户列。

### 5.3 LogicalRecord

`LogicalRecord` 表示 schema 下的逻辑行：

- `schemaVersion`
- `columnValues`
- `hiddenColumns`
- `deleted`
- `recordType`

`HiddenColumns`：

- `dbRowId`：无显式主键时由聚簇索引使用。
- `dbTrxId`：最后插入或更新该记录的事务 ID。
- `dbRollPtr`：指向 undo record。

规则：

- 聚簇索引记录保存完整用户列和隐藏列。
- 二级索引记录保存二级 key part 和聚簇主键；第一阶段不要求二级索引项独立完成 MVCC 判断。
- `LogicalRecord` 可以从当前页解码，也可以由 undo record 链构造旧版本。
- `LogicalRecord` 不携带 latch 或 buffer fix 生命周期。

### 5.4 RecordImage

`RecordImage` 是不可变快照，用于 undo、redo payload、比较测试和旧版本构造：

- `beforeImage`
- `afterImage`
- `changedColumns`
- `physicalBytes`
- `logicalValues`

使用规则：

- undo 优先保存逻辑前镜像或变更列值。
- redo 优先保存物理页修改或 record operation command。
- 旧版本构造使用 `RecordImage + UndoRecord`，不把历史版本常驻记录页。

## 6. 物理记录与逻辑记录的转换

转换入口：

- `RecordDecoder.decode(PhysicalRecord, TableSchema)`
- `RecordEncoder.encode(LogicalRecord, TableSchema)`
- `RecordCursor.readColumn(ColumnId)`
- `RecordCursor.writeColumn(ColumnId, ColumnValue)`
- `HiddenColumnAccessor.readTrxId/readRollPtr/writeHiddenColumns`

转换流程：

1. `RecordPageAccessor` 根据 `RecordRef` 或 page directory 定位物理 offset。
2. `RecordFormatReader` 读取 record header。
3. `NullBitmapReader` 判断 nullable 字段是否为 NULL。
4. `FieldOffsetResolver` 根据列定义、固定长度、变长目录计算字段 slice。
5. `TypeCodecRegistry` 根据 `ColumnType` 找到 `TypeCodec`。
6. `TypeCodec` 把字段 slice 解码为 `ColumnValue`。
7. 隐藏列通过 `HiddenColumnLayout` 单独解释，提供给 MVCC/undo。

反向编码流程：

1. 校验 schema version 与列数量。
2. `ColumnValueValidator` 校验 NULL、长度、精度、scale、charset。
3. 每列使用对应 `TypeCodec` 编码。
4. `RecordLayoutPlanner` 计算 NULL bitmap、变长目录和 payload 长度。
5. `RecordEncoder` 生成 `PhysicalRecordBytes`。
6. `RecordPageAccessor` 写入页内 heap，并维护 `next_record`、`n_owned`、PageDirectory。

## 7. 页内记录结构

索引页内记录区域包含：

| 区域 | 职责 |
| --- | --- |
| `Infimum` | 页内最小边界记录 |
| `Supremum` | 页内最大边界记录 |
| `UserRecordHeap` | 物理记录 heap，按插入和重用空间写入 |
| `GarbageList` | delete/purge 后可复用的碎片空间 |
| `FreeSpace` | heap top 与 page directory 之间的连续可用空间 |
| `PageDirectory` | 页尾向前增长的稀疏槽数组 |

页内逻辑顺序：

- `Infimum -> record1 -> record2 -> ... -> Supremum`
- `next_record` 按 key 顺序连接。
- `heap_no` 按物理分配顺序增长。
- `PageDirectory` 槽指向每个 group 的最后一条记录。
- `n_owned` 只在 group 最后一条记录上记录成员数。

页内查找：

1. 使用 `RecordComparator` 对 page directory slot 做二分。
2. 找到覆盖目标 key 的 group。
3. 从上一 slot 的记录开始沿 `next_record` 扫描。
4. 比较目标 key，返回命中、插入位置或范围边界。

## 8. 数据类型系统

### 8.1 ColumnType

`ColumnType` 是不可变值对象：

- `typeId`
- `nullable`
- `length`
- `precision`
- `scale`
- `unsigned`
- `charset`
- `collation`
- `storageKind`: `FIXED`, `VARIABLE`, `OVERFLOW_CAPABLE`
- `sortBehavior`

`ColumnType` 不负责读写 page。它只描述类型属性，并通过 `TypeCodecRegistry` 找到可执行策略。

### 8.2 支持的数据类型

第一阶段支持以下类型：

| 类型组 | 类型 | 物理编码策略 | 比较策略 |
| --- | --- | --- | --- |
| 整数 | `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | 固定长度二进制，支持 signed/unsigned | 数值比较 |
| 布尔 | `BOOLEAN` | `TINYINT(1)` 语义别名 | 数值比较 |
| 定点 | `DECIMAL(p,s)` | 压缩十进制或定长 decimal byte array | 精度/scale 归一后比较 |
| 浮点 | `FLOAT`, `DOUBLE` | IEEE 754 二进制 | 数值比较，明确 NaN 排序策略 |
| 字符 | `CHAR(n)`, `VARCHAR(n)` | charset 编码，`VARCHAR` 进入变长目录 | collation 比较 |
| 二进制 | `BINARY(n)`, `VARBINARY(n)` | 原始 byte，`VARBINARY` 进入变长目录 | unsigned byte lexicographic |
| 时间 | `DATE`, `TIME`, `DATETIME`, `TIMESTAMP`, `YEAR` | 归一化整数或定长 byte | 时间线性比较 |

第二阶段扩展：

| 类型组 | 类型 | 设计边界 |
| --- | --- | --- |
| 大对象 | `TINYTEXT`, `TEXT`, `MEDIUMTEXT`, `LONGTEXT`, `BLOB` 系列 | inline prefix + overflow pointer，完整 off-page 链由 LOB 模块实现 |
| 枚举 | `ENUM`, `SET` | schema 中保存字典，物理记录保存 ordinal/bitmap |
| JSON | `JSON` | 第一阶段按 `LONGTEXT` 或 binary JSON 适配器处理，不进入核心索引比较 |

### 8.3 TypeCodec

`TypeCodec<T>` 是编码策略接口：

- `encodedLength(ColumnValue value, ColumnType type)`
- `encode(ColumnValue value, FieldWriter writer, EncodeContext ctx)`
- `decode(FieldSlice slice, ColumnType type, DecodeContext ctx)`
- `compare(FieldSlice left, FieldSlice right, ColumnType type, CompareContext ctx)`
- `validate(ColumnValue value, ColumnType type)`

设计规则：

- 固定长度类型必须能在不构造 Java 对象的情况下比较 encoded bytes。
- 变长类型必须显式返回长度，不能扫描到 sentinel。
- 字符类型比较必须通过 `CollationStrategy`，不能直接用 Java `String.compareTo`。
- NULL 不由具体 codec 编码；NULL 由 `NullBitmapCodec` 统一处理。

### 8.4 ColumnValue

`ColumnValue` 是逻辑值对象：

- `NullValue`
- `IntValue`
- `DecimalValue`
- `StringValue`
- `BinaryValue`
- `TemporalValue`

原则：

- `ColumnValue` 不知道自己位于哪个 page。
- 类型转换、默认值、截断策略由 SQL 层或 schema validator 决定；record 层只做物理编码前的边界校验。
- 对索引比较，优先比较 encoded key slice，避免频繁创建 `ColumnValue`。

## 9. 类型系统使用的设计模式

- Strategy：每种 `ColumnType` 对应独立 `TypeCodec`、`TypeComparator`、`CollationStrategy`。
- Factory：`TypeCodecRegistry` 根据 `typeId + charset + collation` 创建或返回 codec。
- Flyweight：不可变 `ColumnType`、`Collation`、`CharsetDescriptor` 可被 schema 共享。
- Template Method：`RecordEncoder` 固定 `validate -> plan layout -> encode fields -> write header` 流程。
- Visitor：`ColumnTypeVisitor` 用于统计固定长度、变长列数量、nullable 列数量、最大 inline 长度。
- Composite：`IndexKeyComparator` 由多个 `KeyPartComparator` 组合，支持复合索引。
- Decorator：`NullableCodec`、`LengthPrefixCodec`、`OverflowPointerCodec` 包装基础类型编码。
- Adapter：`PageCursorFieldReader` 把 Buffer Pool 的 `PageCursor` 适配成字段级 reader/writer。
- Command：`RecordPageOperation` 表达 insert、update、delete-mark、purge、reorganize，可转成 redo/undo 所需 payload。
- Snapshot：`RecordImage` 保存操作前后镜像。

## 10. 记录操作

记录读写数据流程见 [record-data-flow.mmd](diagrams/record-data-flow.mmd)。

### 10.1 Insert

插入流程：

1. B+Tree 定位 leaf page，并以 X latch 获取 `PageHandle`。
2. `RecordInsertTemplate` 校验 schema、NULL、长度、key 顺序。
3. `RecordEncoder` 编码逻辑记录和隐藏列。
4. `HeapSpaceManager` 优先复用 `GarbageList`，否则使用连续 `FreeSpace`。
5. 写入物理记录。
6. 调整前驱和新记录的 `next_record`。
7. 更新 group 的 `n_owned`；必要时拆分 group 并新增 PageDirectory slot。
8. 生成 `INSERT_RECORD` 或等价 `WRITE_PAGE_BYTES` redo payload。
9. MTR commit 后页进入 dirty tracking。

### 10.2 Update

更新分三类：

- 原地更新：新旧 encoded length 相同或变短，且不改变索引 key 顺序。
- 页内搬迁：新记录变长但当前页仍有空间，移动记录并修正 `next_record` 和 directory。
- delete-mark + insert：主键或索引 key 变化，或当前页空间不足，需要 B+Tree 重新定位。

规则：

- 普通聚簇记录 update 必须先写 update undo，再更新 payload、`DB_TRX_ID`、`DB_ROLL_PTR`。
- 二级索引 key 变化由 B+Tree/索引层转换为旧索引项 delete-mark 和新索引项 insert。
- update 不直接触发新 page 分配；需要 split 时返回 `RecordPageOverflow` 给 B+Tree。

### 10.3 Delete-Mark 与 Purge

delete-mark：

1. 写 update undo。
2. 设置 `deletedFlag = true`。
3. 更新隐藏列。
4. 保留记录在 `next_record` 链表中，供旧 ReadView 构造历史版本。

purge：

1. 事务模块确认没有 ReadView 需要该记录版本。
2. `RecordPurgeOperation` 从 `next_record` 链表摘除记录。
3. 将物理空间挂入 `GarbageList`。
4. 更新 `n_owned` 和 PageDirectory；group 过小则合并。
5. 必要时触发 page reorganize，整理碎片。

### 10.4 Page Reorganize

页内重组目标：

- 合并 garbage。
- 压缩 heap。
- 重建 `next_record` offset。
- 重建 PageDirectory。

重组规则：

- 必须持有 page X latch。
- 对记录按逻辑 key 顺序重写。
- `heapNo` 可保留或重分配；若重分配，所有依赖 `heapNo` 的短期引用必须失效。
- 重组产生 redo；崩溃恢复必须能重放到一致页。

## 11. 比较、排序与索引 key

`RecordComparator` 输入：

- `IndexKeyDef`
- `left RecordCursor/FieldSlice`
- `right SearchKey/RecordCursor`
- `CompareContext`

比较规则：

- 复合索引按 key part 顺序比较。
- 每个 key part 使用对应 `TypeComparator`。
- ASC/DESC 由 `KeyPartOrder` 处理。
- NULL 在升序中排在非 NULL 前；降序反转。
- 字符串使用 `CollationStrategy`，binary 类型使用 byte 顺序。
- prefix index 只比较定义的前缀长度，必要时回表确认完整行。

优化：

- 数值、时间、binary 类型优先直接比较 encoded bytes。
- VARCHAR/CHAR 使用 collation weight cache，但 cache 不能进入 page body。
- group scan 中可复用 `FieldOffsetResolver`，避免重复解析整条记录。

## 12. 与其它模块的协作

### 12.1 与 Buffer Pool

- Record 模块只通过 `PageHandle` 和 `PageCursor` 访问 page。
- Buffer Pool 不解析记录头、PageDirectory 或字段编码。
- Record 写页必须持有 X latch；读页至少持有 S latch。
- `PageCursor` 释放后，`RecordCursor` 必须失效。

### 12.2 与 Disk Manager

- Disk Manager 分配和释放 page，不解析 record。
- Record 页空间不足时返回明确结果，由 B+Tree 决定是否 split 并向 Disk Manager 申请新页。
- Record 不修改 XDES、segment inode、space header。

### 12.3 与 B+Tree

- B+Tree 负责跨页导航、split、merge、root 调整。
- Record 负责页内 key 比较、插入位置、slot 维护、record 编码。
- B+Tree 不直接修改 `next_record`、`n_owned`、PageDirectory。

### 12.4 与 Transaction/MVCC

- Transaction 模块决定可见性、锁和 undo 生命周期。
- Record 模块提供隐藏列读写、delete-mark、当前版本镜像。
- 构造旧版本时，事务模块沿 undo 链应用 `RecordImage` 或 changed columns。
- 普通 update/delete 必须在同一 MTR 内完成 undo 写入和记录隐藏列更新。

### 12.5 与 Redo/Recovery

- Record 操作通过 `RecordPageOperation` 或 `WRITE_PAGE_BYTES` 进入 redo。
- Recovery 重放时只要求物理页一致，不重新执行 SQL 语义。
- redo handler 可以调用 record format helper 校验页内结构，但不能依赖事务可见性。

## 13. 并发与锁顺序

锁状态图见 [record-lock-state.mmd](diagrams/record-lock-state.mmd)。

Record 模块同时面对两类并发控制：page latch 保护页内物理结构，事务锁保护数据库逻辑冲突。Record 模块本身不拥有事务锁管理器，也不拥有 page latch 生命周期；它只声明操作前置条件，并在可能等待事务锁的场景使用重新定位协议。

### 13.1 锁与 latch 边界

| 资源 | 所属模块 | 保护内容 | Record 模块职责 |
| --- | --- | --- | --- |
| `pageLatch(S/X)` | Buffer Pool | page body、record heap、PageDirectory | 调用前校验 latch mode |
| `BUFFER_FIX` | Buffer Pool/MTR | frame 生命周期 | 不缓存超过 `PageHandle` 生命周期的 cursor |
| record/gap/next-key lock | Transaction LockManager | 逻辑行、索引范围、插入冲突 | 只提供 `IndexRecordRef`、`IndexGapRef` |
| undo page latch | Transaction/Undo | undo record page | 不在持有 undo page latch 时进入 record page 修改 |
| B+Tree latch | B+Tree | 跨页结构、split/merge/root | 页内操作不反向获取父页 latch |

规则：

- 页内读至少要求 `pageLatch(S)`。
- 页内 insert/update/delete-mark/purge/reorganize 要求 `pageLatch(X)`。
- `RecordCursor` 不拥有锁；`PageHandle` release 后必须失效。
- Record 不在内部等待行锁；行锁等待由 B+Tree/Transaction 在调用 record 修改前完成。
- Record 不持有 Buffer Pool list/hash/frame 锁。

### 13.2 锁状态与持有变化

Record 模块不创建全局锁对象，但必须描述调用过程中 page latch、cursor 引用和事务锁的状态变化。

| 状态 | 持有者 | 持有资源 | 进入条件 | 退出条件 |
| --- | --- | --- | --- | --- |
| `NO_HANDLE` | 无 | 无 page latch、无 cursor | 操作尚未进入页 | B+Tree 获取 page handle |
| `PAGE_LATCHED_S` | 当前线程/MTR | page S latch、buffer fix | 页内读或查找 | 创建读 cursor 或释放 handle |
| `PAGE_LATCHED_X` | 当前线程/MTR | page X latch、buffer fix | 页内写、purge、reorganize | 修改完成、释放 handle 或等待行锁前释放 |
| `CURSOR_ACTIVE` | 当前线程 | `RecordCursor` 借用 `PageHandle` 生命周期 | 解析记录或字段 | cursor release 或 PageHandle release |
| `REF_CAPTURED` | 当前线程 | `IndexRecordRef` / `IndexGapRef` 值对象 | 需要进入 LockManager 前 | 释放 page latch 后进入事务锁申请 |
| `PAGE_RELEASED` | 无 page latch 持有者 | 只保留逻辑定位值 | 等待行锁前释放 page latch | 进入 LockManager wait/grant |
| `WAITING_ROW_LOCK` | LockManager wait queue | 事务等待边，不持有 page latch | record/gap/next-key lock 冲突 | grant、timeout 或 deadlock victim |
| `ROW_LOCK_GRANTED` | Transaction | record/gap/next-key lock | LockManager 授予锁 | 重新定位并进入页内修改 |
| `RELOCATING` | 当前线程 | 事务锁，暂不持有 page latch | 等待后重新进入 B+Tree | 重新获取目标 page latch |
| `MODIFYING_RECORD` | 当前线程/MTR | page X latch、buffer fix、事务锁 | 重新校验成功 | MTR commit 或异常 rollback |
| `MTR_COMMITTING` | MTR | page latch、buffer fix、redo memo | 页内修改完成 | dirty 发布并释放 memo |
| `RELEASED` | 无 | 无 cursor、无 page latch | 正常结束、超时、victim 或异常清理 | 操作结束 |

锁持有变化规则：

- `acquire page latch`：只通过 Buffer Pool `PageHandle` 获得，进入 MTR memo。
- `create cursor`：只借用 page latch，不增加独立锁所有权。
- `capture ref`：把物理位置转换为可重新定位的逻辑引用。
- `release before wait`：进入事务锁等待前必须释放 `RecordCursor`、page latch 和 buffer fix。
- `grant row lock`：事务锁归 `Transaction` 所有，不归 `RecordCursor` 所有。
- `relocate`：等待结束后重新定位，不能复用旧 cursor。
- `commit release`：MTR 释放 page latch/buffer fix；事务锁由事务结束或语句级策略释放。
- `rollback cleanup`：deadlock victim 或异常路径必须释放已授予事务锁，并丢弃所有旧 `RecordRef`。

### 13.3 当前读与重新定位协议

当前读或写操作可能需要等待行锁。为避免 page latch 与事务锁形成跨层死锁，调用方必须遵守：

1. B+Tree 使用 page latch 定位候选记录。
2. Record 生成 `IndexRecordRef` 或 `IndexGapRef`。
3. 调用方释放 page latch 和 `RecordCursor`。
4. 通过 `LockManager` 获取 record/gap/next-key lock；如果等待，死锁检测只发生在事务等待图中。
5. 锁授予后重新进入 B+Tree，从 root 或安全 latch coupling 点重新定位记录。
6. Record 重新校验 key、heapNo、delete flag、pageLSN 或 schemaVersion。
7. 校验通过后执行页内修改；校验失败则按新位置重试。

这个协议让长期事务锁等待不占用 Buffer Pool page latch，也避免 page split、purge 或 reorganize 后继续使用过期 `RecordRef`。

### 13.4 页内操作锁顺序

页内修改的推荐顺序：

1. 事务层完成必要 record/gap/next-key lock。
2. B+Tree 获取必要的结构 latch。
3. Buffer Pool 获取目标 page X latch 和 buffer fix。
4. Record 读取和校验物理记录。
5. Undo 模块写 undo record。
6. Record 修改 page body、hidden columns、PageDirectory。
7. MTR 收集 redo payload。
8. MTR commit 后释放 page latch 和 buffer fix。

约束：

- 写 undo 和写数据记录处于同一个 MTR，但不能在等待行锁时持有 undo page latch。
- Record reorganize 必须持有 page X latch，但不能持有父页 latch 后批量等待其它 leaf page。
- purge 物理删除前必须确认事务模块给出的 purge view 安全。
- 同一批多个 record page 修改按 `PageId` 排序获取 page latch。
- update 发现记录变长且页内空间不足时返回 `RecordPageOverflow`，由 B+Tree 释放当前 latch 后处理 split。

### 13.5 并发异常

Record 层需要把并发冲突表达为可重试错误，而不是静默修复：

- `RecordRelocationRequiredException`：等待锁后记录位置失效，需要重新定位。
- `RecordChangedException`：目标记录 key、delete flag 或隐藏列已变化。
- `RecordPageReorganizedException`：page reorganize 后 `heapNo/pageOffset` 失效。
- `RecordLatchRequiredException`：调用方未持有足够 page latch。
- `RecordPurgeConflictException`：purge 发现记录仍被可见版本或锁保护。

## 14. API 设计

### 14.1 RecordPageAccessor

对 B+Tree 暴露页内能力：

- `findInsertPosition(PageHandle, SearchKey, IndexKeyDef)`
- `findEqual(PageHandle, SearchKey, IndexKeyDef)`
- `insert(PageHandle, LogicalRecord, InsertContext, MTR)`
- `update(PageHandle, RecordRef, UpdatePatch, UpdateContext, MTR)`
- `deleteMark(PageHandle, RecordRef, DeleteContext, MTR)`
- `purge(PageHandle, RecordRef, PurgeContext, MTR)`
- `reorganize(PageHandle, MTR)`

### 14.2 RecordCursor

字段级访问入口：

- `recordRef()`
- `recordHeader()`
- `readColumn(ColumnId)`
- `readKeyPart(KeyPartDef)`
- `readHiddenColumns()`
- `isDeleted()`
- `materialize(TableSchema)`
- `release()`

`RecordCursor` 不拥有 latch 和 buffer fix 生命周期。调用方必须保证对应 `PageHandle` 仍有效。

### 14.3 TypeCodecRegistry

类型编码入口：

- `codecFor(ColumnType)`
- `comparatorFor(ColumnType)`
- `collationFor(CharsetId, CollationId)`
- `validate(ColumnValue, ColumnType)`

registry 初始化后应只读，避免运行中 schema 行为变化。

## 15. 异常处理

异常类型：

- `RecordFormatException`
- `RecordTooLargeException`
- `ColumnValueOutOfRangeException`
- `InvalidColumnTypeException`
- `SchemaVersionMismatchException`
- `RecordNotFoundException`
- `RecordDeletedException`
- `PageDirectoryCorruptedException`
- `UnsupportedRecordTypeException`
- `UnsupportedColumnTypeException`
- `RecordRelocationRequiredException`
- `RecordChangedException`
- `RecordPageReorganizedException`
- `RecordLatchRequiredException`
- `RecordPurgeConflictException`

错误策略：

- 页内结构损坏时抛出明确异常，并由上层标记 tablespace/page 需要 recovery 检查。
- 编码长度超过 page inline 上限时，若类型支持 overflow，返回 overflow 需求；否则抛出 `RecordTooLargeException`。
- schema version 不匹配时不猜测字段布局，必须由上层做 online DDL 版本桥接。
- 字符集或 collation 缺失时禁止比较，避免索引顺序不稳定。
- purge 发现记录仍可能被旧 ReadView 需要时必须拒绝物理删除。

## 16. 测试设计

后续实现应覆盖：

- 记录头测试：`heapNo`、`nOwned`、`deletedFlag`、`nextRecordOffset` 编解码。
- NULL bitmap 测试：nullable/non-nullable、全 NULL、无 NULL 列。
- 变长字段测试：`VARCHAR/VARBINARY` 长度目录、空字符串、最大长度。
- 数值类型测试：signed/unsigned 边界、decimal scale、float NaN 排序策略。
- 字符类型测试：charset 编码、collation 比较、prefix key。
- 时间类型测试：日期范围、timestamp 归一化、排序一致性。
- 页内查找测试：PageDirectory 二分、group scan、infimum/supremum 边界。
- insert 测试：顺序插入、随机插入、slot split、空间不足。
- update 测试：原地更新、变长搬迁、key 变化返回 split/重插信号。
- delete/purge 测试：delete-mark 保留逻辑链、purge 物理摘除、group 合并。
- MVCC 协作测试：隐藏列读写、undo payload、旧版本构造输入。
- 并发测试：等待行锁前释放 page latch、等待后重新定位、page reorganize 后 cursor 失效、purge 与当前读冲突。
- redo/recovery 测试：record operation 重放后 page directory 和 next_record 一致。
- property-based 测试：随机 insert/update/delete/purge/reorganize 后 key 顺序、heap、directory 不变量成立。

## 17. 后续实现顺序

推荐分阶段实现：

1. `schema`：`TableSchema`、`ColumnDef`、`IndexKeyDef`、`ColumnType`。
2. `type`：整数、字符串、二进制、时间的 codec 和 comparator。
3. `format`：record header、NULL bitmap、变长字段目录。
4. `page`：infimum/supremum、`next_record` 链、PageDirectory。
5. `cursor`：`RecordCursor` 和字段级读取。
6. `op`：insert 和页内查找。
7. update、delete-mark、purge。
8. 隐藏列与事务/MVCC 适配。
9. redo/undo payload 边界。
10. page reorganize 和故障注入测试。

## 18. 十五轮自检记录

| 轮次 | 检查主题 | 检查结果 |
| --- | --- | --- |
| 1 | 不写代码边界 | 只新增 Markdown 和 Mermaid 设计内容，没有生成 Java 源码 |
| 2 | 目标与非目标 | 已明确 Record 模块负责页内记录格式、类型编码、逻辑/物理转换，不负责 SQL、B+Tree 分裂和文件 IO |
| 3 | MySQL 8.0 贴合 | 已覆盖 InnoDB record header、heap_no、next_record、Page Directory、infimum/supremum、隐藏列和 delete-mark/purge |
| 4 | 高内聚 | 记录头、字段布局、类型编码、页内目录、RecordCursor、record operation 都收敛在 `storage.record` |
| 5 | 低耦合 | 已禁止 record 访问 `BufferFrame`、extent bitmap、segment inode、SQL parser 和具体文件层 |
| 6 | 面向对象 | 已定义 `RecordRef`、`PhysicalRecord`、`LogicalRecord`、`RecordImage`、`ColumnType`、`TypeCodec` 等领域对象 |
| 7 | 设计模式 | 已覆盖 Strategy、Factory、Flyweight、Template Method、Visitor、Composite、Decorator、Adapter、Command、Snapshot |
| 8 | 核心领域模型 | 已区分物理记录、逻辑记录、记录镜像、隐藏列、PageDirectory 和 TypeCodec |
| 9 | 依赖方向 | 已明确 `btree -> record.api -> record.page -> record.format -> record.type`，并列出禁止依赖 |
| 10 | 物理/逻辑双视角 | 已区分页内 byte layout、heap offset、`heapNo` 与 schema 下的列值、key 顺序和 MVCC 元数据 |
| 11 | 关键数据流 | 已给出页内查找、读解码、写入等待行锁、重新定位、undo、redo、MTR commit 数据流 |
| 12 | 图示 | 已新增并引用架构图、类关系图、数据流程图和锁状态图 |
| 13 | 并发锁状态 | 已给出 page latch、RecordCursor、事务锁等待、重新定位、MTR commit 的状态和持有变化 |
| 14 | 异常与恢复 | 已定义格式损坏、schema mismatch、并发重定位、purge 冲突等异常，并说明 redo/recovery 只重放物理页修改 |
| 15 | 测试与占位检查 | 已覆盖类型、页内结构、MVCC 协作、并发、redo/recovery 和 property-based 测试；文档未留下未完成占位文本 |

## 19. 参考链接

- MySQL 8.0 Reference Manual - InnoDB Row Formats: https://dev.mysql.com/doc/refman/8.0/en/innodb-row-format.html
- MySQL 8.0 Reference Manual - InnoDB Record Structure: https://dev.mysql.com/doc/refman/8.0/en/innodb-physical-record.html
- MySQL 8.0 Reference Manual - InnoDB Index Types: https://dev.mysql.com/doc/refman/8.0/en/innodb-index-types.html
- MySQL 8.0 Reference Manual - The InnoDB Transaction Model and Locking: https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-model.html
- MySQL 8.0.46 Source Documentation - `rem0rec.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/rem0rec_8h.html
- MySQL 8.0.46 Source Documentation - `page0page.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/page0page_8h.html
- MySQL 8.0.46 Source Documentation - `dict0types.h`: https://dev.mysql.com/doc/dev/mysql-server/8.0.46/dict0types_8h.html
