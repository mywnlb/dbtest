# MiniMySQL XA、范围事务与阻塞式 ALTER 统一设计

## 1. 目标与当前基线

本设计统一收敛 `storage-backlog.md` 中四条跨层主线：Server XID Registry / SQL XA、比较与复合范围、
RU/SERIALIZABLE 与命名 SAVEPOINT、以及 DISCARD/IMPORT 和剩余阻塞式 ALTER。它是长期设计资产，
不是一次提交必须同时上线的 implementation plan；每个实施切片仍须独立测试、更新 current map，并保持恢复 gate
在任何证据不完整时 fail-closed。

当前生产基线已经具备 storage PREPARED participant、聚簇/二级点查、单列 non-unique equality range、
current-read/LockManager、statement rollback、atomic CREATE/DROP TABLE/INDEX、DDL log v3 和
DISCARD/IMPORT control-plane。新增代码必须复用这些真链路，不建立平行事务、锁或 DDL 实现。

## 2. 跨模块边界

- Session 只持 opaque transaction/XA/savepoint handle；SQL 不接触 `Transaction`、B+Tree 或页对象。
- Parser 产生通用 comparison/ALTER/XA AST；Binder 负责名称、类型、范围交集和确定性访问路径。
- Executor 求值 residual predicate；storage API 只接收 typed index range、current-read mode 和 row mutation。
- Server XA registry 拥有 XID→TransactionId 与最终决议；storage 继续只认识 TransactionId。
- DD 拥有 table options/default/目标 schema；storage rebuild 只消费不可变 source/target definition。
- 所有 page latch/fix 必须在事务锁等待、文件移动、redo durable wait 和 DD cache wait 前释放。

## 3. 比较范围与执行

WHERE v1 支持 AND 连接的 `=,<,<=,>,>=,BETWEEN`。Binder 对同列约束求交，矛盾范围直接形成 empty
plan；普通比较遇 NULL 返回 UNKNOWN。访问路径依次为聚簇完整等值、唯一二级完整等值、最长连续索引前缀
（若干 equality 加一个 range part）、最后聚簇 full scan；同分数按 stable index id 选择。

`IndexRange` 使用 `UNBOUNDED/OPEN/CLOSED` 边界。logical prefix 到 physical secondary key 的 clustered
suffix 由 storage adapter 用不落盘 boundary sentinel 扩展；ASC/DESC、prefix 和 collation 始终复用 Record
比较器。B+Tree 以 256-row batch 和“最后一个完整 physical key” continuation 分页；SQL 单语句最多返回
4096 个聚簇 identity、检查 16384 个 physical candidate，超限不返回 partial result。

非锁定读用同一 ReadView 覆盖分页、回表、undo、LOB hydration 和 residual evaluation。锁定读采用短 MTR
定位、释放页资源、等待 record/gap/next-key lock、重新定位复核。范围 UPDATE/DELETE 先锁定并物化聚簇
identity，再通过 point DML 执行，防止修改扫描 key 产生 Halloween；超过上限必须早于第一笔 row mutation。

## 4. 隔离级别与保存点

RU 不创建 ReadView，直接返回最新非 delete-marked 版本；DML/锁规则沿用 RC。SERIALIZABLE 的显式事务或
autocommit=0 普通 SELECT 转为 FOR SHARE；单语句 autocommit SELECT 仍可走一致性读。

Session 用规范化名称映射 opaque `SqlSavepointHandle`。同名 SAVEPOINT 替换旧边界；ROLLBACK TO 撤销更晚
undo、删除更晚名称但保留目标；RELEASE 只删除目标名称。internal statement savepoint 使用独立 namespace。
LockManager 为授予锁记录 owner-global acquisition sequence 和 retention kind：普通 record/gap/next-key
始终保留到事务终态，只有已被 INSERT undo 删除的实体锁与 statement-duration lock 可按保存点释放。

## 5. Server XA

`XaId` 保存 signed formatId、1..64B gtrid、0..64B bqual，总长不超过 128B。SQL 支持
`XA START|BEGIN ... [JOIN|RESUME]`、`END ... [SUSPEND [FOR MIGRATE]]`、PREPARE、
`COMMIT [ONE PHASE]`、ROLLBACK 和 `RECOVER [CONVERT XID]`；JOIN/RESUME/SUSPEND 只兼容语法，
不提供跨 Session branch migration。

`<instance-root>/mysql.xa` 是带 frame CRC、sequence 和 fsync 的 append-only registry。写分支状态固定为：

`PREPARING -> PREPARED -> COMMIT_DECIDED|ROLLBACK_DECIDED -> COMPLETED`

prepare 必须先 durable PREPARING，再调用 storage prepare/fsync，最后 durable PREPARED 才应答。phase two
必须先 durable decision，再调用 storage，最后写 COMPLETED。恢复 provider 对 PREPARING 返回 ROLLBACK，
对 DECIDED 返回对应结果，对无决议 PREPARED 返回 UNRESOLVED 并阻止 OPEN。成功启动后才能收敛已消费记录。

离线 `XaRecoveryMaintenance` 独占 instance lock，只打开 registry，支持 inspect 与写入最终决议；它不打开
storage 或普通 Session。无写分支的 PREPARE 返回 READ_ONLY 完成，不建立 storage PREPARED。

## 6. DD、表空间迁移与 ALTER

`TableOptions` 保存 comment/default charset/default collation；`ColumnDefaultDefinition` 保存
REQUIRED、IMPLICIT_NULL 或已按列类型验证的 constant。catalog/SDI 新格式显式保存它们；旧 catalog 从 schema
继承 table defaults，旧 nullable 列推导 IMPLICIT_NULL，旧 NOT NULL 列保持 REQUIRED，未知格式拒绝。

ALTER action 按语句顺序在 staged table definition 上绑定，支持 ADD COLUMN 的 FIRST/AFTER、DROP COLUMN、
ADD/DROP secondary index、单表跨 schema RENAME、COMMENT、DEFAULT CHARACTER SET/COLLATE 和
CONVERT TO CHARACTER SET。DISCARD/IMPORT 必须是单独 action。禁止删除聚簇 key；删除 secondary key part
会收缩索引，空索引删除。任一目标 unique 冲突、编码失败、长度溢出都在 DD 提交前终止。

涉及列布局、CONVERT 或多个结构 action 时使用一次 shadow rebuild：table X 下等待 purge barrier，持久化
old/new binding 与 target digest，创建新 SpaceId/segments/root，分页复制 live row/LOB 并重建索引，force
新空间与 SDI，原子发布新 DD binding，最后 drain/drop 旧空间。恢复只看 committed DD：旧 binding 删除
shadow；新 binding 校验 shadow 并删除旧空间；任何 identity/digest/path 冲突都阻止 OPEN。

当前 v1 的持久 marker 先保存 table/version 与精确 old/new space/path，尚未单列 schema digest；恢复阶段以
committed DD binding 裁决方向，并由 committed DD 对 target SDI 做 exact reconcile。显式 target digest
作为后续 marker 格式升级，不能在 current map 中写成已经落地。

SQL DISCARD/IMPORT 使用实例内受控 `tablespace-transfer/discarded` 与 `incoming`，文件名固定包含
tableId/spaceId。IMPORT 先校验 page0/SDI/file identity；DDL 内只做同 FileStore atomic move，跨设备来源必须
先由管理员复制并 force 到 incoming。

## 7. 明确非目标

本轮不实现 OR/IN/IS NULL/LIKE、代价优化器、JOIN、SQL LIMIT、流式网络结果、online DDL row log、binlog
participant、主键/type 修改、foreign key、generated column、多对 RENAME、temporary undo 或 XA 权限系统。
全局图只在职责/依赖方向改变时更新；每个生产接线切片必须以源码和测试复核 current map。
