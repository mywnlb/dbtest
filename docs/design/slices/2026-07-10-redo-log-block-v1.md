# Redo LogBlock v1 Slice

## 目标

- 在既有 `RLG1` batch frame 外增加固定 512B LogBlock，检测 block torn write 与中段损坏。
- 单文件和默认 redo ring 共用相同 block codec/scanner，恢复输出仍为完整 `RedoLogBatch`。
- redo control 升级为物理隔离双槽 v2，并把 redo format version 写入 checkpoint label。
- 保持逻辑 LSN、pageLSN、MTR batch end 与 WAL/checkpoint 语义不变。

## 关键决策

- LogBlock header 32B、payload 472B、trailer 8B；CRC32 覆盖 header、payload、padding 与 trailer blockNo。
- 每个 MTR batch 独占一个 block chain；record/frame 可跨 block，但 batch 不跨 ring 文件。
- `blockNo` 全局单调；同一 chain 重复 batch start LSN，内层 `RLG1` frame CRC 继续保留。
- 末文件短块、最后物理块 checksum 失败或 EOF 未闭合 chain 视为 torn tail，并丢弃整个未完成 chain。
- CRC 正确但 flags/长度/padding/blockNo/LSN/内层 frame 非法始终 fail-closed。
- 旧裸 `RLG1`、ring header v1、redo-control v1 不迁移、不混读，统一抛致命格式异常。
- READ_ONLY_VALIDATE 只读 data/control，不创建、截断、修复或 force 文件。

## 实现边界

- 新增纯 LogBlock codec、顺序 scanner 与扫描结果值对象。
- 单文件首次 append 才覆盖 recovery 已判定的 torn tail；普通/read-only 扫描不改文件。
- ring 先验证完整文件集合和 v2 header，再按 start LSN 扫描；只有逻辑末文件允许 torn tail。
- ring `fileBytes` 表示 block 区容量，至少 512B 且按 512B 对齐；超大 batch 写前拒绝。
- `RedoRecoveryScan` 保留 torn-only ring 的非零 header 起点；reader 校验 checkpoint 覆盖与 batch range 连续。
- recovery 在任何修复写前校验 control/data format 一致。
- redo-control 两槽位于偏移 0/4096，固定 8192B；slot 记录 format version 与 generation。

## 非目标

- 不做旧格式在线迁移或长期双格式兼容。
- 不做 batch 跨 ring 文件、跨 batch 尾块复用、checkpoint 物理 fileId/offset 或 marker 回写。
- 不切换 CRC32C，不改变逻辑 LSN，也不解决 per-operation 精确物理 redo 预算。

## 验收

- codec 覆盖单块、多块、header/trailer、padding、checksum、flags、offset、blockNo 溢出。
- 单文件覆盖重开、torn tail、中段损坏、未闭合 chain、尾部覆盖续写、旧格式和只读。
- ring 覆盖轮转/回收、跨文件连续性、末文件 torn、早期文件损坏、文件集不完整和容量拒绝。
- checkpoint 覆盖双槽整页损坏回退、format mismatch、checkpoint gap/ahead 和只读缺失。
- 默认 ring、单文件 opt-out、durability、transaction recovery 与 force recovery 全量回归通过。
- 更新 `current-implementation-map.md` 与 `storage-backlog.md` 的受影响小节。
