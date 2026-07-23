# Change Buffer v1 slice

## 目标

- 实现 MySQL 8.0 风格、可恢复的全局 Change Buffer，而不是每表内存队列。
- 新实例创建 SpaceId 0 `system.ibd`、page3 header、page4 稳定 B+Tree root 和用户空间 4-bit/page bitmap。
- 非唯一升序二级索引的冷 leaf 允许延迟 INSERT、DELETE_MARK、DELETE；其它情况安全回退直写。
- 目标页对普通 fixer 发布前完成 merge，恢复、DDL 和后台线程均复用同一持久证据。

## 关键决策

- 全局 key 为 `(targetSpaceId,targetPageNo,sequence)`，payload 保存 exact table/schema/index identity、operation、entry 与 CRC。
- header 的 sequence、pending、root level 与树 mutation 在同一 MTR 更新；bitmap buffered 位也在该 append/consume MTR 更新。
- `BTreeIndex.pageType` 区分普通 `INDEX` 与内部 `IBUF_INDEX`，既有页类型 code 不重排。
- `locateLeafWithoutLoading` 只读取 root/internal；root leaf、unique、DESC、驻留/LOADING、首张 bitmap 范围外、Buffer Pool 百分比容量或 gate 失败均直写。
- Buffer Pool 使用 set-once `PageLoadInterceptor`；LOADING frame 由 detached MTR adopt，commit 后才完成 future，未关闭 guard/后半程失败则传播 fatal。
- 同目标页 append/merge/DDL 由有界 striped gate 串行；不持页 latch 等 gate，不使用 Java monitor。
- DML、rollback、purge 通过一个 `SecondaryIndexMutationCoordinator` 决定 buffer 或 direct，不复制 eligibility。
- 新聚簇主键已锁定的 INSERT 对 non-unique secondary 跳过 leaf physical-state read；unique 与 UPDATE/revive 保持重定位检查。
- DDL DROP/DISCARD/REBUILD 先丢弃旧 identity；IMPORT 在重新开放 NORMAL 前清零可信 bitmap 页。
- redo 后新增 `CHANGE_BUFFER_RECOVER` 校验 header/tree/count/sequence/bitmap；用户页仍按首次加载惰性 merge。
- legacy 无 `system.ibd` 或没有持久 exact-version resolver 时 effective mode 为 `NONE`，不静默升级。
- FORCE 不能隔离 SpaceId 0；存在 pending 而 metadata 不可解析时启动 fail-closed。

## 非目标与明确简化

- 不兼容 MySQL ibuf 磁盘字节格式，复用本项目 Record/B+Tree 编码。
- 单页 merge 不触发 leaf merge；低填充页留给后续普通结构维护。
- worker 单线程、固定周期/批量，不实现 MySQL master-thread 自适应 IO 或并行 partition。
- 当前 FSP 只保证 page1 bitmap；普通 DML 在第二覆盖区间安全直写，自动重复固定页预留尚未实现。
- IMPORT 遇到公式指定但非 `IBUF_BITMAP` 的重复页会 fail-closed，不猜测或覆盖用户数据。
- snapshot 的 bitmap page 数是本进程已观察页数，不是磁盘全量统计；无 Performance Schema 表。

## 验收测试

- mode/config、64 条单页/64 页 worker 边界、codec、bitmap 公式/nibble/free class、gate timeout/owner 有测试。
- system.ibd page0–4 类型、extent 保留、segment/root bootstrap、legacy NONE 与 FORCE 拒绝有集成测试。
- append 同 MTR 更新 tree/header/bitmap，发布前 merge 同 MTR 修改 target/consume/clear bitmap，失败不发布。
- eligibility 覆盖 unique、DESC、root-leaf、resident/LOADING、容量、pending 上限和 direct fallback。
- SQL INSERT 在真实冷 non-unique secondary 上产生 pending，二级查询首次加载后观察 merge 结果。
- rollback、purge 和 DDL index/space discard 经统一 coordinator/barrier；IMPORT bitmap reset 保持 DISCARDED 到 WAL+force。
- recovery 哨兵/count/identity/bitmap、READ_ONLY_VALIDATE、metadata 缺失、worker 启停与实例 fail-stop 均有断言。
- 固定 JDK/Gradle 全量回归测试数不得低于实现前 315 suites / 1814 tests。

## Current map 更新

- 更新 Disk、Buffer Pool/MTR、B+Tree、DML、Recovery、DDL 的真实生产链。
- 新增 Change Buffer 小节并列出 bootstrap、append、prepublish merge、worker、DDL、recovery 与 snapshot。
- 跨 bitmap 区间 FSP 预留和高级调度写入 Known Gaps，不画成已实现实线。
- 新增类型均由生产组合根接线；无新增 `Reserved / Unwired Production Types` 项。
- 按 current map 10 项清单和本切片五遍成品检查重新核对源码与测试。
