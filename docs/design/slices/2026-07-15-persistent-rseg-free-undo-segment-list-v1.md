# Persistent Rollback-Segment Free Undo Segment List v1

## 目标

- 在 rollback-segment header 中持久化与 undo kind 无关的单页 free segment FIFO。
- 让 INSERT / UPDATE undo 优先复用同 kind cache，其次复用 free segment，最后分配新页。
- 让事务终结优先进入 cache，cache 不可接纳时进入 free list，无法复用时才释放物理页。
- 保证 crash recovery、truncate 与普通运行期观察到同一套 owner 语义。

## 关键决策

- page3 升级为 format v4，在 `lastTransactionNo` 后追加 free head、tail、length 三个 u64 字段。
- v1-v3 rollback-segment header fail-closed，不提供在线迁移。
- 普通 undo 页格式保持 v3；FREE 使用物理状态码 3。
- FREE 页复用 history prev/next 字段保存 free prev/next，字段含义由页状态区分。
- free list 只接纳单 fragment、单 ordinary page、无 extent、无 external payload 的 segment。
- FREE 页清空事务 owner、提交号、记录计数和逻辑头；first/last 均指向自身。
- cache 仍是按 kind 独立、有界、LIFO；free list 是跨 kind、无配置上限、FIFO。
- 运行期由统一 `UndoSegmentReuseDirectory` 维护两类 cache 与一个 free deque。
- 目录锁内不执行页 IO；transition fence 与 drain gate 防止相同 owner 并发迁移。
- committed MTR 边界上，保留 segment 只能由 occupied slot、kind cache 或 free list 唯一拥有。
- history 只是 occupied UPDATE slot 之间的提交顺序关系，不构成额外 owner。
- 普通终结 MTR 触及的全部 undo 首页一次收集，按 `(spaceId,pageNo)` 全局升序加 X latch。
- free 复用以 page3 head/base CAS 为线性化点，同一 MTR 完成 slot 获取、摘头和页激活。
- free 入队以 tail append；摘头后将新 head 的 prev 清空，空队列同时清空 tail。
- redo 沿用 logical page delta wire format，仅追加稳定 metadata kind code。
- recovery 按持久 length 精确遍历，校验 prev/next、tail、cycle、页状态、资格与 owner 去重。
- recovery 在回滚 recovered ACTIVE transaction 前恢复统一目录，使恢复回滚也可产生 free owner。
- truncate 以统一非阻塞 drain 批量清理 cache/free，每批最多八个 segment。
- truncate 批次顺序固定为 INSERT cache top、UPDATE cache top、free head。

## 非目标

- 不回收或压缩多页 undo segment，不拆 extent，不迁移旧 page3 格式。
- 不改变普通 undo record/page 的 v3 编码与 external payload 编码。
- 不实现 free list 配置容量、后台 shrink 或跨 rollback segment 负载均衡。
- 不改变 `TransactionUndoRecoveryResult` 对外结果形状。

## 验收

- page3 v4 编解码、容量边界、CAS、tail append/head pop 与 redo replay 幂等均有测试。
- FREE 页 reset、跨 kind activate、非法状态、非单页拒绝均有测试。
- 统一目录覆盖 cache-first、free FIFO、重复 owner、transition 与 drain 并发边界。
- commit、rollback、purge 的混合首页场景验证全局页序，不依赖逻辑操作顺序。
- 跨重启 recovery 能恢复 free FIFO，并对链损坏、FSP 不一致和重复 owner fail-closed。
- truncate 覆盖混合多批、busy、marker、page3 v4 rebuild 与剩余 free head relink。
- 预算测试断言 actual redo 不超过 domain-scaled budget，固定 profile 同步升级。
- 固定 Java 25 / Gradle 9.5.1 全量测试通过，测试数量不倒退。

## 文档收尾

- 按生产源码调用链更新 `current-implementation-map.md` 的 undo/recovery/truncate 小节。
- 更新 `storage-backlog.md`，把本切片标为完成并记录仍不支持多页 free reuse。
- 仅在本切片改变长期模块职责时才修改全局架构图。
