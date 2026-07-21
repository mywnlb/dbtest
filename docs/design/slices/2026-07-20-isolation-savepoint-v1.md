# Isolation 与命名 SAVEPOINT v1

## 目标

- SQL transaction port 开放 READ UNCOMMITTED 与 SERIALIZABLE。
- RU 一致性读不创建 ReadView，返回读取时最新的未 delete-marked 聚簇版本。
- SERIALIZABLE 显式事务和 autocommit=0 的普通 SELECT 在绑定前提升为 FOR SHARE。
- SERIALIZABLE autocommit 单语句普通 SELECT 保持 RC 风格语句级一致性读。
- Parser/Session 接入 SAVEPOINT、ROLLBACK TO SAVEPOINT、RELEASE SAVEPOINT。
- LockManager 固定保存点获取序号与 retention kind，明确 partial rollback 的释放白名单。

## 关键决策

- RU 的二级 entry 只作候选；必须回聚簇读取当前版本并重算 logical key/residual。
- RU 的 DML/current-read gap 规则与 RC 相同，不把 RU 误当 RR 取得 next-key/gap 锁。
- Session 在事务准备和 Binder 之前改写 SERIALIZABLE 普通 SELECT，避免 SQL 层绕过 MDL/plan。
- storage adapter 只对 SERIALIZABLE autocommit handle 映射为 RC；显式/隐式事务保留 SERIALIZABLE。
- Session 以 `Locale.ROOT` 小写名称维护 insertion-ordered opaque savepoint map。
- 同名 SAVEPOINT 创建新边界后替换旧边界；失败不得丢失原边界。
- ROLLBACK TO 删除更晚名称并保留目标；RELEASE 只删除目标名称。
- 首写前保存点使用 EmptyUndoBoundary；回滚消费后立即建立等价新边界。
- UndoContext release 只移除指定 runtime savepoint，不再隐式移除更晚边界。
- 锁获取使用全局单调 request sequence，再以 owner 过滤形成事务保存点边界。
- 普通 record/gap/next-key/secondary logical locks retention 为 TRANSACTION。
- 只有显式标成 SAVEPOINT_RELEASEABLE 或 STATEMENT_DURATION 的锁可按边界释放。
- 本片不把现有 DML 普通锁错误标成可提前释放；完整事务终态仍统一 releaseAll。

## 非目标

- 不实现 SET TRANSACTION ISOLATION LEVEL；隔离级别仍由 SessionOptions 创建时固定。
- 不实现跨 Session 保存点、持久保存点或 XA branch 内保存点迁移。
- 不实现 RC residual-miss 提前解锁，也不改变死锁 victim 选择。
- 不把 RU 变成无 latch 的脏字节读取；每次物化仍通过短只读 MTR。

## 验收

- Parser 覆盖三种命名保存点语法、可选 SAVEPOINT 关键字和非法尾部。
- Session 测试覆盖同名替换、ROLLBACK TO 保留目标、RELEASE 只删目标和终态清理。
- Session 测试覆盖 SERIALIZABLE autocommit consistent 与 explicit/implicit FOR SHARE。
- MvccReader/gateway 测试覆盖 RU 看见未提交最新值、忽略当前 delete-marked 行。
- BTree current-read 测试覆盖 SERIALIZABLE 使用 RR 的 next-key/terminal-gap 规则。
- LockManager 测试覆盖保存点只释放白名单锁，普通 record/gap 锁继续阻塞到终态。
- 相关测试和全量 Gradle test 通过。

## Current map 更新

- 更新 SQL parser/session、ReadView/MVCC、current-read/LockManager 的生产实线。
- 从 transaction known gaps 删除 RU、SERIALIZABLE、命名 SAVEPOINT 与保存点锁白名单。
- 保留 SET TRANSACTION、RC residual-miss 提前释放和插入实体锁精细分类为后续缺口。
