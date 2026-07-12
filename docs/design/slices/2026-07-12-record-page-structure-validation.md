# Record Page Structure Validation Slice

## 目标

- 为已格式化 INDEX 页增加一次性物理结构校验，损坏页在 B+Tree 解析 key、修改记录或产生 redo 前 fail-closed。
- 校验 header 几何、系统记录、next-record 链、用户记录头和 PageDirectory group 账本的一致性。
- 保持 Record 只解释页内字节，不读取文件、不拥有 latch，也不依赖 B+Tree schema/key 语义。

## 生产接线

- 新增无状态 `RecordPageStructureValidator.validate(RecordPage)`，只在调用方已持 S/X page latch 时读取。
- `IndexPageAccess.openIndexPageHandle` 在 `mtr.getPage` 后构造 `RecordPage` 并校验一次，再返回 handle。
- `openIndexPage` 继续委托 handle 入口；LeafOnly 与 SplitCapable 两条生产链因此同时受保护。
- `IndexPageHandle.recordPage()` 不重复校验，避免 40 余处调用重复 O(n) 扫描和对写操作中间态误判。
- `createIndexPage` 继续先 `newPage`、盖信封并 `format`；未格式化新页不进入 validator。
- 校验异常发生在既有 latch/fix 内但早于页内容修改；MTR rollback 负责统一释放资源，不新增等待或锁序。

## Header 与系统记录不变量

- `nDirSlots>=2`，目录区域不得越过 trailer，`USER_RECORDS_START<=heapTop<=dirStart`。
- `nHeap>=2`、`0<=nRecs<=nHeap-2`；本片不要求 `nHeap==nRecs+2`，因为 purge 后 heap number 不回退。
- infimum/supremum 固定 offset、type、heapNo、recordLength 和标签必须匹配格式常量。
- infimum 只能作为 slot 0 owner 且 `nOwned=1`；supremum 必须 `nextRecordOffset=0`。
- header/free/garbage 链的完整一致性留给后续 garbage-list 切片，本片只验证其 u16 已有编码边界。

## next-record 与用户记录不变量

- 从 infimum 单向遍历必须唯一到达 supremum；0、回环、重复 offset 或越界均视为损坏。
- 用户记录起点必须位于 `[USER_RECORDS_START, heapTop)`，完整 header 与 `recordLength` 不得越过 heapTop。
- 用户记录只允许 CONVENTIONAL/NODE_POINTER；recordLength 至少覆盖固定 header。
- 所有链内用户记录的物理 `[offset,offset+recordLength)` 区间不得互相重叠。
- 用户 heapNo 必须位于 `[2,nHeap)` 且页内唯一；链中用户数必须等于 header `nRecs`。
- validator 只验证物理链，不验证 key 排序；后者需要 schema、keyDef 与 collation，不下沉到 RecordPage。

## PageDirectory / nOwned 不变量

- slot 0 必须指向 infimum，末 slot 必须指向 supremum；内部 slot 必须指向链中用户记录。
- directory owner 在链上必须严格前进且不得重复，不能跳回前一 group。
- 每个 slot owner 的 `nOwned` 必须等于上一 owner 之后到当前 owner（含当前）的实际记录数。
- 非 owner 用户记录的 `nOwned` 必须为 0；所有 slot `nOwned` 总和必须等于 `nRecs+2`。
- 不强制 InnoDB 4–8 group 大小：当前 insert 在目录空间不足时允许 owner 暂时超过 MAX，属于已记录教学简化。

## 异常与非目标

- 所有结构不一致统一抛既有 `PageDirectoryCorruptedException`；低层 header/record/bounds 读取异常保留为 cause。
- 不改变页格式、record 编码、split/merge、redo、checksum、MVCC、行锁或 tablespace corrupted 状态机。
- 本片不做 free/garbage 链全量校验、key 有序性、跨页 sibling/parent 校验、property-based 随机生成框架。

## 验收

- 空页及 insert/update/delete-mark/purge/reorganize 后的合法页均通过完整校验。
- header 几何、系统记录、链 0/环/越界、长度越界、heapNo 重复、nRecs 不符均有失败测试。
- sentinel slot、内部 owner 顺序/重复、owner `nOwned`、interior 非零 `nOwned` 均有失败测试。
- `IndexPageAccess` 集成测试证明损坏页在 S/X open 时拒绝，create/format 路径不受影响。
- 更新 current map/backlog；实现后按源码真实调用链复核五遍并跑固定 JDK/Gradle 全量测试。
