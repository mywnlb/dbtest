# Buffer Pool 状态与批量自适应 Flush v1

- 目标：补齐 `DIRTY_PENDING/EVICTING/STALE` frame 生命周期，接入 FlushList/LRU 批量刷脏，并用 redo/flush 速率与 IO capacity 调节批量大小。
- 依据：`innodb-buffer-pool-design.md` §5.7、§9.3、§10、§13；`innodb-flush-checkpoint-doublewrite-design.md` §7.4、§8、§9。
- 依赖：现有 `FrameStateMachine`、真实 `DirtyPageList`、WAL gate、bounded `DoublewriteBatch`、PageCleanerWorker。
- 非目标：不改变 SQL/DD/事务语义；不实现 doublewrite 物理双文件、DISCARD/IMPORT 或 online DDL。
- 状态不变量：只有 `DIRTY/FLUSHING` 计入 dirty；pending 不进 flush list；EVICTING/STALE 不可被普通读路径返回。
- pending 发布：PageGuard 首次写入通知 pending；MTR 仍按 append、pageLSN、release、markClosed 顺序发布 dirty。
- generation：frame 重新绑定时递增，flush completion 校验 pageId、generation、dirtyVersion 和 pageLSN。
- 淘汰：clean victim 先转 EVICTING 再摘 hash/LRU；失效 tablespace 先转 STALE 再复位 FREE。
- 批量来源：`FLUSH_LIST` 按 oldest LSN，`LRU` 按可淘汰 dirty 尾部。
- 批量 WAL：snapshot 后释放 Buffer Pool 内部锁；按 SpaceId 升序持 shared lease；redo durable 后才进入 doublewrite/data-file IO。
- 批量物理 IO：doublewrite batch 一次 append/force；data page 按 space 分组写入并 force；部分失败保留未成功页恢复副本。
- 兼容：FrameReleaser、BufferPool、DoublewriteStrategy、FlushService 保留默认实现/旧构造器；FlushAdvice/FlushCycleResult 旧语义不变。
- Adaptive 输入：LSN 字节增量、成功刷页数、采样秒数、dirty/free ratio、IO capacity、idle percent。
- Adaptive 输出：总目标页数以及 FlushList/LRU 分配；无压力仍不主动刷脏，ASYNC 受 idle percent 限制。
- 验收：状态转换、旧 snapshot 防污染、WAL skip、doublewrite 部分失败、批量 force、速率/IO cap、LRU 水位和并发锁边界测试。
- 回归：固定 JDK/Gradle 全量 `test`；完成后更新 current map 的生产实线与 reserved 类型表。
