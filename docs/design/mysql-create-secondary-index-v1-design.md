# CREATE SECONDARY INDEX v1 设计

## 1. 目标

本设计把 `CREATE [UNIQUE] INDEX` 与 `ALTER TABLE ... ADD [UNIQUE] INDEX` 接入同一个
DDL operation。实现覆盖 SQL 解析、名称绑定、MDL、聚簇扫描、二级 B+Tree 构建、DD/SDI 发布、
DDL log 与崩溃恢复，而不是只向字典追加 `IndexDefinition`。

本切片遵守 `mysql-data-dictionary-ddl-design.md` 的 `InplaceIndexBuildStrategy` 边界。v1
全程持有 table `MDL_EXCLUSIVE`，不实现 online DDL row log；因此构建期间没有并发 DML，
也不需要合并增量变更。

## 2. 语法与非目标

支持：

```sql
CREATE [UNIQUE] INDEX index_name ON [schema.]table_name (column [ASC|DESC], ...)
ALTER TABLE [schema.]table_name ADD [UNIQUE] INDEX index_name (column [ASC|DESC], ...)
```

两种语法绑定为同一个不可变 `CreateSecondaryIndexCommand`。v1 不支持 `IF NOT EXISTS`、表达式索引、
前缀长度、不可见索引、`ALGORITHM`、`LOCK`、并行构建、drop/rebuild index，也不静默降级。

## 3. 字典版本与物理行格式版本

新增索引会发布新的 `TableDefinition.version`，但不会改写既有聚簇记录。物理 record codec
使用的 schema version 因而不能继续直接等于字典版本。

`TableStorageBinding` 增加 `rowFormatVersion`：

- CREATE TABLE 把初始字典版本写为 `rowFormatVersion`。
- CREATE INDEX 保持该值不变，只推进 `TableDefinition.version`。
- DD→storage mapper 使用 `rowFormatVersion` 构造 `StorageTableDefinition` 和全部 index layout。
- 旧 catalog/SDI 没有该字段时，以当时的 `TableDefinition.version` 派生，保持向后可读。
- 将来 rebuild table/add column 必须显式生成新的物理行格式版本。

catalog binding 采用可判别尾部扩展；SDI payload 升级为 v2，同时保留 v1 解码。任何未知格式、
非法版本或尾随字节都 fail-closed。

## 4. 持久状态

DDL log 追加 `CREATE_INDEX` 稳定码。marker 的 `affectedObjectId` 仍是 table id，新增稳定
`secondaryObjectId` 保存预留的 index id；旧 v1 marker 解码为 0，CREATE/DROP TABLE 仍要求其为 0。
DDL log v2 同时在 key/payload 交叉校验 identity，历史 v1 可继续读取。

既有表空间 page3 增加固定大小的 index-build footer，位于 SDI payload 区与 FIL trailer 之间：

- magic / format / state；
- ddl id、目标 dictionary version、table id、index id；
- root page、root level、leaf segment、non-leaf segment；
- descriptor CRC32C。

footer 的 `EMPTY`/`BUILDING` 是物理资源归属证据，不是字典提交证据。SDI 新写入容量扣除 footer，
写 SDI 时必须保留有效 footer；旧 SDI 若占用 footer 预留区，CREATE INDEX 在产生任何 segment
副作用前明确拒绝，普通读取仍兼容旧容量。

## 5. 正常流程

1. 取得 schema `MDL_INTENTION_EXCLUSIVE` 和 table `MDL_EXCLUSIVE`，读取唯一 ACTIVE table。
2. 在未持有 page latch/MTR 时等待 `TablePurgeBarrier` 清零，并等待旧 metadata pin 排空。
   这保证没有历史 purge 或旧 descriptor 在构建期间访问该表。
3. 校验索引名、列名、重复 key part 和 v1 能力，预留 index/DDL/dictionary version，写
   `CREATE_INDEX/PREPARED`。
4. 一个 MTR 创建 leaf/non-leaf segment 与 level-0 root，并在同一 MTR 写 page3 `BUILDING`
   descriptor。descriptor 与 segment 分配要么共同进入 redo，要么共同不可见。
5. 从聚簇索引最左叶开始扫描 live record，按新 index layout 投影 entry。每行使用短 MTR
   插入，root grow 后刷新 descriptor 的 root/level；UNIQUE 索引在插入前按 logical key
   检查重复，包含 NULL 的 logical key 允许多行。
6. 构建完成并持久化最终 root 后，DDL log 从 `PREPARED` 进入 `ENGINE_DONE`。
7. 以“旧列、旧 rowFormatVersion、旧 indexes + 新 index、旧 binding + 新 binding”构造
   新 ACTIVE table。先写完整 SDI v2，再原子提交 DD，随后写 `DICTIONARY_COMMITTED`。
8. 发布 cache，清空 page3 build descriptor 并持久化，最后写 `COMMITTED`。

page3 descriptor 初始化发生在已经持有新 root 高页号 latch 的同一 MTR。此处只允许使用窄作用域
out-of-order guard：table MDL X、history barrier 和 pin drain 已证明没有其它线程能在该表上形成
page3/FSP/index 反向等待。作用域结束立即恢复全序。

## 6. 失败补偿

在 DD 提交前的确定性失败（重复数据、编码失败、redo admission、扫描或插入失败）执行物理回滚：

1. 读取并交叉校验 page3 descriptor 与 marker。
2. 先按最新 root/segment 计划申请 redo budget，再分别 drop leaf/non-leaf segment。
3. 同一收敛流程清空 descriptor并满足 WAL/force。
4. 只有物理清理确认完成后才把 marker 写为 `ROLLED_BACK`。

如果 catalog append/force 返回导致 DD 发布结果不确定，当前进程不得猜测回滚。它保留 descriptor
和 marker，由重启恢复读取 committed DD 裁决。

## 7. 崩溃恢复裁决

恢复顺序仍是 storage crash recovery 完成后、开放 Session 前执行 DDL recovery。

| durable DD | marker / descriptor | 恢复动作 |
| --- | --- | --- |
| 旧 ACTIVE table，不含 index | PREPARED + 无 descriptor | marker → ROLLED_BACK |
| 旧 ACTIVE table，不含 index | PREPARED/ENGINE_DONE + BUILDING | drop 两个 staged segment，清 descriptor，marker → ROLLED_BACK |
| 新 ACTIVE table，包含 exact index/binding | ENGINE_DONE/DICTIONARY_COMMITTED + BUILDING | 校验 binding，清 descriptor，发布 cache，逐阶段补到 COMMITTED |
| 新 ACTIVE table，包含 exact index/binding | descriptor 已空 | 发布 cache并补到 COMMITTED |
| 新 DD 与 marker index/table/version/binding 不一致 | 任意 | fail-closed，阻止引擎 OPEN |
| marker 声称 committed 但 DD 仍旧 | DICTIONARY_COMMITTED | fail-closed，不删除可能已提交资源 |

committed DD 始终是逻辑提交真相；SDI 和 build descriptor 只用于校验、修复和回收，不能反向创建
catalog object。

## 8. SQL Session 事务语义

DDL 不能复用 Session 的 transaction-duration MDL owner，否则会与自身 table X 冲突。
Session 执行 DDL 前先完成 MySQL 风格 implicit commit，释放事务 MDL 与 dictionary lease；随后为本条
DDL 分配普通低半区 owner，通过 engine adapter 调用 DD coordinator。DDL 成功或失败后，
`autocommit=0` 的 Session 再建立新的隐式事务。

SQL parser/binder 不访问 page、B+Tree 或 DDL log。binder 只完成 schema 默认值、名称规范化和语法能力
校验；列存在性、重复索引名和 exact table version 由持有 MDL X 的 DD coordinator 重新验证。

## 9. 验收

- 两种 SQL 语法生成同一绑定命令，非法/不支持语法有明确异常。
- 空表、已有多行表、非唯一重复键和 UNIQUE 重复键均有自动化测试。
- 构建后 point/range secondary lookup、后续 INSERT/UPDATE/DELETE 与 rollback/purge 维护新索引。
- 重启后 DD、SDI、root/segments、row format version 一致。
- 覆盖 allocation、backfill、ENGINE_DONE、DD publish、descriptor clear 各崩溃边界。
- 旧 DDL log/catalog/SDI fixtures 继续可读；损坏 identity/CRC/未知版本阻止启动。
- `current-implementation-map.md` 只把实际接线画为生产实线，并记录仍未实现的 online DDL/drop index。
