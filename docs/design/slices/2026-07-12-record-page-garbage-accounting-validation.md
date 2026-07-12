# Record Page Garbage Accounting Validation Slice

## 目标

- 在 0.21a 已格式化 INDEX 页校验中补齐 GarbageList、FREE fragment 与 GARBAGE 字节账本。
- 损坏的空闲链、重复 heap identity、live/free 物理重叠和虚高可回收空间在 B+Tree 决策前 fail-closed。
- 保持现有页格式与 first-fit/整块消费语义，不把 validator 变成修复器或空间分配器。

## 当前语义与关键取舍

- `FREE` 是单向 fragment 链头；每个 fragment 沿用旧用户记录 header，以 `next_record` 串接。
- 链上 fragment 容量取旧 `recordLength`，heapNo 保留，复用时整块从 FREE/GARBAGE 账本摘除。
- `GARBAGE = free 累加整条 - reuse 扣整条 + in-place shrink 差额`，不是 FREE 链容量精确和。
- 原地缩短产生的碎片只计字节、不可定位；oversized fragment 复用后的余量既不可定位也不计 GARBAGE。
- 因页格式没有保存两类余量位置，本片采用可证明上下界，禁止伪造无法验证的精确相等。

## 生产接线与数据流

- 扩展既有 `RecordPageStructureValidator`，不新增第二个 validator 或新的生产调用点。
- 0.21a 用户链扫描结果增加 live heapNo、物理区间与 `liveRecordBytes` 只读快照。
- 校验顺序为 header/system → live chain → garbage chain/accounting → PageDirectory。
- `IndexPageAccess.openIndexPageHandle` 仍在既有 S/X fix 后只调用一次 validator；create/format 路径不变。
- 校验只读当前 PageGuard，不触发 reorganize、redo、IO、锁等待或 tablespace 状态转换。

## FREE fragment 不变量

- `FREE=0` 表示无可定位 fragment；`GARBAGE>0` 仍可由原地缩短产生，因此不要求二者同时为零。
- 每个 fragment 起点必须在 `[USER_RECORDS_START, heapTop)`，完整 header/recordLength 不得越过 heapTop。
- fragment 类型只允许 CONVENTIONAL/NODE_POINTER，recordLength 至少覆盖固定 record header。
- FREE 链必须唯一到 0；重复 offset、环、越界、重复 heapNo 均视为损坏。
- fragment heapNo 必须位于 `[2,nHeap)`，且不得与 live record heapNo 重复。
- fragment 物理区间彼此不得重叠，也不得与任何 live record 区间重叠。
- fragment 的 deleted/nOwned/minRec 残值不参与校验；记录离开 live 链后这些字段不再是权威状态。

## GARBAGE 账本不变量

- `linkedFreeBytes = Σ FREE fragment recordLength`，使用 long checked/有界累加。
- `heapSpan = heapTop - USER_RECORDS_START`；`physicalDeadUpper = heapSpan - liveRecordBytes`。
- 必须满足 `linkedFreeBytes <= GARBAGE <= physicalDeadUpper`。
- 下界保证每个可定位 fragment 都已计入；上界阻止虚高 GARBAGE 欺骗 underflow/merge-fit 决策。
- 不要求 `GARBAGE == linkedFreeBytes`；两者差值可表示已计数但不可定位的 in-place shrink 碎片。

## 异常与非目标

- 损坏继续统一抛 `PageDirectoryCorruptedException`，低层 header/bounds 异常保留 cause，不静默截链。
- 不修改 `HeapSpaceManager` 分配/回收算法，不自动 reorganize，不新增 fragment footer/size class/checksum。
- 不验证 schema-aware key 顺序、FIL sibling、free-space 清零字节，也不定位 oversized reuse 的未计余量。

## 验收

- purge/moved-update 多 fragment、first-fit 跳过、exact/oversized reuse、in-place shrink、reorganize 均通过。
- FREE 越界/环/重复、长度越界、heapNo 与 live/free 冲突、live/free 区间重叠均有失败测试。
- GARBAGE 小于 linked sum、超过 physical dead upper 均失败；FREE=0 且仅有 shrink garbage 合法。
- `IndexPageAccess` 集成测试证明损坏账本在 S/X open 时拒绝且不新增 redo。
- 更新 current map/backlog；实现后复核五遍并跑固定 JDK/Gradle 全量测试。
