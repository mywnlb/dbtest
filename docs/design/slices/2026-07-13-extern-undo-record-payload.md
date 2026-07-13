# 1.6 Extern undo record payload slice

## 目标

- 解除单条 `UndoRecord` 必须完整放入一个普通 UNDO record 槽的限制。
- 保持既有 inline undo 编码与 `RollPointer` 编码不变。
- 让 MVCC、rollback、purge、recovery 共用同一外置记录解析入口。
- 在 DML 写 MTR admission 前冻结编码、页数、reservation 与 redo workload。

## 关键决策

- 新增稳定 `PageType.UNDO_PAYLOAD=9`，不复用业务 LOB 的 `BLOB=8`。
- 普通 UNDO record 槽保存 35B 根描述符，tag=`0x7F`、version=`1`。
- 描述符保存 record type、事务、undoNo、首页、总长、页数和整值 CRC32。
- 既有 inline record 首字节 `1/2/3` 不变，读取时按首字节分流。
- payload 页与 root 属于同一 FSP undo segment，但不加入主 UNDO FIL 页链。
- 每个 payload 页保存 chain index、segment/inode identity、owner、总长与 CRC。
- 读取严格校验页类型、链接、页序、owner、长度、页数和整值 CRC。
- 完整 payload 重组后再由 `UndoRecordCodec` 解码，并拒绝尾随字节。
- `UndoStoredRecordResolver` 是 segment read、direct roll-pointer read 和遍历的统一入口。
- 写计划按 fresh UNDO record 槽容量决定 inline/external，不按当前尾页剩余空间决定。
- external 页数受 `EngineConfig.maxExternalUndoPayloadPages` 限制，默认 16。
- 配置上限不得超过最小 buffer-pool instance 的 frame 数。
- `UndoWritePlan` 在业务 MTR 前冻结事务/持久头快照、编码和精确预留页数。
- 首写预留 root 页加全部 payload 页；续写只预留 grow 页和 payload 页。
- redo workload 每个 external 页增加保守页 image 预算。
- 进入物理写阶段后的异常 fail-stop；claim lease 不把已写 owner 退回 FREE。
- DML 在 undo 物理写成功后若 B+Tree/commit 失败，也转为 fatal 防止同进程重试。
- 教学兼容入口 `beforeInsert/beforeUpdate/beforeDelete` 保留给既有低层测试；生产 DML 使用 plan API。

## 非目标

- 不改变 7B `RollPointer`，不增加多 undo tablespace 定位信息。
- 不拆独立 insert/update undo log，不实现 cached segment reuse。
- 不实现修改聚簇主键的 delete+insert 语义。
- 不把 undo payload 链与 Record LOB 页链合并或共享回收 ownership。
- 不实现跨 MTR staged payload 发布；单条链仍在一个 MTR 内完成。
- 不提供旧格式在线迁移；既有 inline undo 文件继续可读。

## 验收

- descriptor 稳定字节、未知版本、错误长度与 codec 尾随字节有单测。
- 超配置页数在 reservation/页写前抛领域异常。
- oversized INSERT/UPDATE 可写、按 pointer 读、遍历并保持主 UNDO 链不增长。
- planned DML 精确报告 external 页数、预留页数与 redo workload。
- external UPDATE flush、关闭、reopen 后仍可完整读取旧行。
- 既有 MVCC、rollback、purge、recovery 与 DML 全量测试不回退。
- 更新 `current-implementation-map.md` 与 `storage-backlog.md`。
- 固定 JDK 25.0.2 / Gradle 9.5.1 全量回归通过。
