# Bounded Cached INSERT / UPDATE Undo Segment Reuse

## Goal

- 在单 rollback segment / 单 undo tablespace 约束内，为 INSERT、UPDATE 各维护固定容量的 cached segment LIFO。
- 只复用仍由 FSP inode 持有的空单 fragment segment，减少下一事务首写的 segment create/drop 抖动。
- page3 是 crash 后 owner 权威；内存 cache directory 只是在全部持久证据验证后的运行期投影。

## Key Decisions

- `EngineConfig.undoCachedSegmentsPerKind` 默认 8；0 显式禁用；容量与 slot array 必须共同装入 page3。
- rollback segment header 升为 v2，持久化每类 capacity、count 和 pageNo LIFO entries；v1/未知版本 fail-closed。
- owner 只允许位于 active slot 或一个 cached stack；active→cache、cache→active 均在业务 MTR 中原子改 page3。
- cache entry 只保存 first pageNo；恢复再从 undo 首页重建 segmentId/inodeSlot，并与 FSP inode 交叉校验。
- 可缓存条件固定为 `usedPageCount=1`、`fragmentPageCount=1`、`extentCount=0` 且 first=last。
- commit/rollback/purge 遇到容量满、同 kind transition 忙或 drain 中时不等待，直接 drop 新终结 segment。
- 首写计划冻结 cached top/count；执行时 owner 漂移按 stale plan 失败，不隐式改为新建 segment。
- cached 首页保留旧 record bytes 但重置权威 header、链端点、计数、逻辑头和事务身份；旧字节不可寻址。

## Concurrency and Crash Boundary

- `UndoSegmentCacheDirectory` 仅用短 `ReentrantLock` 保护两个栈、per-kind transition 和全局 drain gate。
- 短锁内不访问 Buffer Pool、FSP、redo、文件或 lifecycle lease；push/pop 用 RAII lease 跨物理 MTR。
- 物理修改前失败释放 transition；物理修改后的异常保留 fail-stop fence，禁止同进程猜测持久结果。
- 恢复先校验 page3、cached undo 首页和 FSP inode，再一次性发布两个内存栈；cache 不参与事务 reconciliation。
- cached segment 复用后可正常 grow/externalize；一旦成为多页段，下一次终结直接 drop，不再缩回 cache。

## Truncate

- 文件已处于 initial size 的稳定重复 truncate 仍是幂等 no-op，不因 cache 推进 epoch。
- 真正 truncate 持 lifecycle X 后只非阻塞取得 cache drain gate；忙则释放 X 并由调用方重试。
- 首个 cache drop 前必须确认 active slots 为空；随后每个 MTR 最多 drop 八段并原子移除 page3 tops。
- TRUNCATING marker 只能写在 active/cache owner 与 FSP inode 均为空之后。
- 恢复续作已有 marker 时再次断言 page3/FSP 为空；物理 rebuild 同一 MTR 重建 page0、page2 和 page3 v2。

## Non-goals

- 不实现多 rollback segment、多 undo tablespace 选择，也不扩展 `RollPointer` 编码。
- 不实现持久 history linked-list、purge→truncate 自动调度或后台 cache resize。
- 不缓存 TEMPORARY undo，不缓存 extent/multi-page/external payload segment，不做 segment shrink。
- 不迁移 page3 v1 教学文件；需要保留旧数据时另立离线迁移切片。

## Acceptance

- page3 v2 format/read、owner 唯一性、LIFO push/pop/drain、容量/版本损坏均有断言测试。
- INSERT commit 与 UPDATE purge 可缓存；下一同 kind 首写复用原 inode/page，跨重启仍可复用。
- 容量满、容量 0、多页段均走 drop；两类 cache 相互独立。
- truncate 可排空 cache 并重建空 page3；存在 active owner 时 cache 保持未动并拒绝 truncate。
- 固定 JDK/Gradle 全量测试通过；`current-implementation-map.md` 与 `storage-backlog.md` 更新为源码事实。
