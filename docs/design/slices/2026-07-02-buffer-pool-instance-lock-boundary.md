# Slice: Buffer Pool instance 锁边界显式化（13.1a）

依据：`innodb-buffer-pool-design.md` §13.1、§13.2、§13.4、§13.5；
`current-implementation-map.md` Buffer Pool + MTR 小节；`storage-backlog.md` BP §13.1 缺口。
前置：0.10d 多 instance + `PageHashTable` 已落；0.22 stale-frame 版本语义已接；
当前 `BufferPoolInstance` 仍用一把 `instanceLock` 保护 hash/free/LRU/frame metadata。

## 目标

- 把当前单把 `instanceLock` 从裸 `ReentrantLock` 提升为包内显式锁边界对象，命名为 metadata lock。
- 统一所有 `lock/unlock/condition/signal/await` 入口，让后续 `pageHashLock`、`freeListLock`、`lruListLock` 拆分有明确替换点。
- 在生产代码中表达不变量：进入 `PageStore` 读写、`DirtyVictimFlusher.flushVictim`、`PageLoadFuture.await` 前不得持有 Buffer Pool metadata lock。
- 保持现有并发语义和行为不变：本片只显式化锁边界，不拆出真正子锁，不改变 frame/hash/LRU 原子性。
- 为 current map 增加 13.1a 状态，避免后续会话把“准备片”误读成“§13.1 已完成”。

## 关键决策

- 新增包内 `BufferPoolInstanceLatchSet`，内部仍持有一个公平性不变的 `ReentrantLock` 和一个 `Condition frameReleased`。
- `BufferPoolInstance` 不再直接持有 `ReentrantLock instanceLock` 和 `Condition frameReleased`；全部改走 latch set。
- latch set 暴露固定语义方法：`lockMetadata()`、`unlockMetadata()`、`awaitFrameReleased(long nanos)`、`signalFrameReleased()`，不把裸 `Condition` 暴露回 instance。
- latch set 增加 `assertMetadataUnlocked(String operation)`，在物理 IO、victim flush 和 loading wait 前调用；违反时抛新增 `BufferPoolLatchViolationException`。
- 不引入 public API，不让 SQL/session/storage.api 感知该内部锁对象；测试放在同包 `storage.buf` 内，通过真实行为和异常路径验证。
- 不改变 `BufferFrame` 的 `pageLatch/pageIntentLatch`；page latch 继续只保护 page body。
- 不改变 `ReplacementPolicy` 接口；LRU 仍由 metadata lock 统一保护，后续真正拆分时再引入 list lock。
- 不改变 `SpaceLifecycleClock`：clock lock 仍独立短持有，且不跨 metadata lock 调外部 IO。
- 注释必须明确：13.1a 是单锁语义的命名和守卫，不是 `pageHashLock/freeListLock/lruListLock` 的完成实现。

## 非目标

- 不拆 `pageHashLock`、`freeListLock`、`lruListLock`、`flushListLock` 或 per-frame `frameMutex`。
- 不改变 `obtainVictim`、`readAndPublish`、`prefetch`、`invalidateTablespace` 的算法和可见行为。
- 不收敛 legacy `flush/flushAll` 持锁直写；本片只把该路径标为仍待收敛，并用注释说明它是 legacy 例外。
- 不新增性能指标、JMX、Performance Schema 或 SQL 可观测接口。
- 不处理 B+Tree/MTR latch 排序、redo collector 命令分类或 DML facade。

## 验收测试

- `metadataLockReleasedBeforePhysicalRead`：阻塞 `PageStore.readPage` 后，另一个线程可进入同分片不同页 miss 并注册 LOADING，证明读盘前未持 metadata lock。
- `metadataLockReleasedBeforeLoadingWait`：同页第二线程等待 LOADING future 时，第三线程可访问同分片其它驻留页，证明 future 等待前已释放 metadata lock。
- `metadataLockReleasedBeforeDirtyVictimFlush`：脏 victim 触发 `DirtyVictimFlusher.flushVictim` 阻塞时，另一个线程可访问同分片驻留 clean 页，证明 victim flush 前已释放 metadata lock。
- `latchSetRejectsIoWhileMetadataLocked`：包内单测直接锁住 `BufferPoolInstanceLatchSet` 后调用 `assertMetadataUnlocked`，断言抛 `BufferPoolLatchViolationException`，锁守卫能捕捉未来回归。
- 既有 `BufferPoolTablespaceInvalidationTest`、`SharedExclusiveLatchTest`、read-ahead/warmup 测试不倒退。
- 全量 Gradle `test` 通过。

## current map 更新要求

- Buffer Pool package status 补充 13.1a：`BufferPoolInstanceLatchSet` 已把单锁 metadata boundary 显式化并守卫 IO 前释放。
- Known gaps 保留 §13.1 真正子锁拆分：`pageHashLock/freeListLock/lruListLock/flushListLock/frameMutex` 仍未落地。
- storage-backlog 中 BP 粗估可小幅更新，但不能把 “per-instance 拆分锁” 标成完成；只能写 “13.1a boundary prep 已落”。
