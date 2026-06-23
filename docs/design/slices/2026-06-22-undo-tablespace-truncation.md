# Slice: Undo Tablespace Truncation — 可恢复原地重建

- 日期：2026-06-22
- 关联设计：`innodb-disk-manager-design.md` §5.2、§8.1、§18；`innodb-undo-log-purge-design.md` §6.2、§7.8、§8、§12、§16。
- 前置：`DiskSpaceManager.dropSegment/freePage` 已能回收 segment、extent、fragment page 和 inode slot；tablespace registry 准入切片已接入空间管理 API。
- 定位：完成可由未来 purge 调用的 undo tablespace 物理收缩生命周期；purge 仍负责判定 undo 死亡并先执行 `dropSegment`。

## 1. 范围

做：
- 新增 `TablespaceAccessController`：每个 `SpaceId` 使用公平显式读写锁；先取得可超时 S lease 再复核 registry 状态，truncate 持可超时 X lease，消除“先检查状态、后加锁”的竞态。
- `MiniTransaction` 每个 tablespace 只登记一个 lease，释放顺序固定为 page latch/fix 在前、tablespace lease 在后。
- `IndexPageAccess`、`UndoPageAccess`、`DiskSpaceManager`、`FlushCoordinator` 使用同一 controller；registry loader 在 controller S 内读 page-0；`PageStore` 保持 registry/state-free。
- page-0 预留区 198–255 固定布局为 magic(4)、format(4)、stateCode(4)、initialSize(8)、epoch(8)、targetSize(8)、finishStateCode(4)、reserved；state 使用稳定显式 code，禁止 ordinal 落盘。
- UNDO create 持久化初始大小和 `ACTIVE`；loader 从 page-0 恢复状态，普通准入拒绝 `TRUNCATING`。
- `BufferPool.invalidateTablespace` 等待 fixed frame 释放、拒绝 dirty frame，成功后移除该空间全部 resident frame。
- `PageStore/DataFileHandle.truncate` 在物理 lifecycle X、file-size X 下缩短文件、force 并发布新的页数边界。
- `UndoTablespaceTruncationService` 验证 UNDO 类型和 inode 全空，编排 WAL、checkpoint、drain、invalidate、truncate、FSP 重建和状态发布。
- recovery 接收显式配置的 undo SpaceId 集合并要求均已打开；先修 page-0 并读取 lifecycle，`TRUNCATING` space 的 doublewrite tail page（pageNo>=target）不得恢复，随后 redo replay、幂等续做，再开放流量。

不做：
- 不计算 purge boundary、history list 或最老 ReadView；不自动选择 truncate 候选和调度频率。
- `TRUNCATE_CANDIDATE` 属于未来 purge 调度状态，本片从已验证的 `ACTIVE/INACTIVE` 直接进入持久 `TRUNCATING`。
- 不实现 data/general tablespace truncate、普通 tablespace 状态持久化、discard 文件删除、多文件 tablespace、discovery/loadAll 或 page-0 envelope。
- 不迁移缺少 lifecycle header 的旧 UNDO 文件；其 truncate 明确失败，避免把当前扩展后大小误认成初始大小。

## 2. 状态与数据流

1. 获取目标 space X lease；验证状态为 `ACTIVE/INACTIVE`、类型为 UNDO、inode 无 used slot。
2. 持久化 `TRUNCATING(epoch+1,target=initialSize,finishState)`，commit 并确保 redo durable；marker LSN 取 page-0 pageLSN，随后立即发布同状态 runtime snapshot。
3. 通过安全 flush 路径刷出不晚于 marker LSN 的全部 dirty page，并持久化 checkpoint `>= marker LSN`；recovery 未满足该 barrier 时必须先补做。
4. drain 目标 space；确认无 dirty/fixed frame后 invalidate；此时不得持 page latch 进入物理 truncate。
5. 截短文件；以同一 epoch 重建 page-0 FSP/XDES、page-2 inode 和系统 extent0，再 WAL+flush+force。
6. 持久化 `finishState`，发布 registry 的 size/freeLimit/version 快照，最后释放 X lease。
7. marker durable 前失败不改物理文件并恢复原运行时状态；marker durable 后即为不可回退点，任何失败均保持 `TRUNCATING`，只允许 retry/recovery。
8. recovery 看到 durable marker 后信任截断前的 inode 校验，总是重做 invalidate/truncate/rebuild/finalize；相同 epoch 和目标大小必须幂等。

## 3. 错误与并发约束

- 非 UNDO、活动 inode、缺失 lifecycle header、target 小于一 extent/未按 extent 对齐、finishState 非 `ACTIVE/INACTIVE`、redo 不 durable 均在物理截断前抛领域异常。
- lease、checkpoint、drain、fixed-frame 等待全部有 timeout；中断恢复线程中断标志，不进入事务 Wait-For Graph。
- 普通路径锁序为 tablespace S lease → MTR/page latch → physical IO S；truncate 为 tablespace X lease → buffer drain/invalidate → physical lifecycle X → file-size X；X owner 仅允许同线程为 maintenance MTR/flush 降级式重入 S。
- 获取 X lease 前不得持 page latch、buffer fix、history mutex、purge queue mutex或事务行锁。

## 4. 验收测试与 current map

- lifecycle codec/state 测试：字段往返、epoch 单调、非法流转、旧 UNDO header 拒绝。
- lease/Buffer Pool 测试：S/X 互斥、超时、释放顺序、fixed/dirty 拒绝、成功失效后重新读盘。
- fil/fsp 测试：真实临时文件缩短、尾页越界、FSP/XDES/inode 重建、完成后可重新创建 segment 并分配页。
- 集成失败测试：活动 inode、GENERAL space、redo/checkpoint 不安全、drain timeout 均不缩短文件。
- 恢复测试：marker、checkpoint、truncate、重建、finalize 各故障点重启续做；补 checkpoint barrier、page-0 先修、tail doublewrite 跳过；配置 space 未打开时 fail closed；重复恢复不推进 epoch。
- 故障注入使用显式阶段 hook，生产实现为无操作策略，不用时间竞争模拟 crash 点。
- 并发测试：既有 guard 阻塞 X、新普通 lease 在 X 期间被阻断，truncate 不与 flush/page IO 交叉。
- 固定 JDK/Gradle 全量 `test`；实现后更新 current map 的 Disk/Buffer/Flush/Recovery/Undo 流和剩余缺口。
