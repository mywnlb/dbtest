# MiniMySQL SDI v1 持久化与恢复设计

版本：2026-07-17  
关联设计：[mysql-data-dictionary-ddl-design.md](mysql-data-dictionary-ddl-design.md)、
[innodb-disk-manager-design.md](innodb-disk-manager-design.md)、
[innodb-crash-recovery-design.md](innodb-crash-recovery-design.md)、
[current-implementation-map.md](current-implementation-map.md)

## 1. 目标

本设计把 MySQL 8.0 风格 Serialized Dictionary Information 的第一版冗余快照接入现有
CREATE TABLE、GENERAL file-per-table tablespace 和启动 DDL recovery。SDI 不是新的字典提交点：
committed DD catalog 始终是逻辑元数据真相，SDI 是随表空间保存、可校验和可重写的物理冗余。

本切片必须满足：

- GENERAL 表空间 page 0 的 `SDI_ROOT` 指向固定 page 3，page 3 使用既有稳定 `PageType.SDI=4`。
- SDI 保存完整 `TableDefinition` 聚合，包括列、索引和最终 storage binding，而不是只保存摘要。
- storage 层只保存带 identity/version 的 opaque payload，不 import DD 类型。
- CREATE 在提交 ACTIVE DD 前先使 SDI durable；DD 提交后的启动恢复仍会逐张 ACTIVE 表校验并修复。
- 旧 GENERAL 表空间允许 `SDI_ROOT=0`；只要 committed DD 与 binding 完整，启动可原地补写 SDI。
- SDI 逻辑损坏或版本/内容错配时，以 committed DD 重写；物理页 checksum、space/page identity
  或未知 root 指针损坏仍 fail-closed，不猜测其它数据页。

## 2. 边界与简化

SDI v1 是固定单页快照，不实现 InnoDB 真实 SDI B+Tree、多页对象分片或压缩。最大 payload
由实例 page size、FIL header/trailer 和 SDI header 共同限定；超限 CREATE 在 DD publish 前失败，
保留 DDL marker/物理文件供既有 recovery 精确回滚。

本切片不自动从“完全丢失的 `mysql.ibd`”重建 schema/catalog。单张 SDI 只含 table 的
`schemaId`，不含 schema 默认 charset/collation，也不能证明整库 catalog 的完整提交边界。
未来 catalog rebuild 必须先设计 schema 级冗余、目录级 manifest 和冲突隔离，不能把扫描到的任意
`.ibd` 直接发布为 ACTIVE DD。

UNDO 表空间 page 3 继续属于 rollback segment header；只有 GENERAL 表空间启用 SDI。
temporary undo 与 SDI 没有共享 owner 或生命周期，不在本切片接入。

## 3. 分层与对象

依赖保持为：

`dd.ddl / dd.recovery -> dd.sdi -> storage.api.ddl -> storage.sdi -> buf/mtr/fsp/fil/redo/flush`

主要对象：

| 对象 | 层 | 职责 |
| --- | --- | --- |
| `DictionarySdiCodec` | `dd.sdi` | `TableDefinition` 与稳定二进制 payload 双向转换 |
| `SerializedDictionaryInfoService` | `dd.sdi` | 生成期望快照、调用 storage、比较并按 committed DD 修复 |
| `SerializedDictionaryInfo` | `storage.api.ddl` | storage-neutral 的 tableId/version/opaque payload |
| `SdiPageRepository` | `storage.sdi` | page0 root 与 page3 envelope/body 的 MTR 读写和格式校验 |
| `TableDdlStorageService` | `storage.api.ddl` | 提供 durable write/read facade，不暴露 `PageGuard` |

禁止事项：

- `storage.sdi` 不解释列类型、对象名、索引 key part 或 DD lifecycle。
- `dd.sdi` 不直接获取 page latch、BufferFrame、FileChannel 或 redo buffer。
- redo replay 只恢复 page0/page3 物理字节，不在 redo handler 解码 DD 或执行 DDL。

## 4. page0 与 page3 布局

GENERAL 表空间创建时 extent 0 已把 page 0..3 标为系统占用。SDI v1 复用该保留位：

- page0 `SpaceHeaderLayout.SDI_ROOT`：`0` 表示 legacy/尚未初始化，`3` 表示 SDI v1；
  其它值在 v1 中不受支持，读取和修复均 fail-closed。
- page3 FIL envelope：`spaceId` 必须等于所属表空间，`pageNo=3`，`pageType=SDI`，
  sibling 均为 `FIL_NULL`。
- page3 body 从 `FIL_PAGE_HEADER_BYTES=38` 开始，页尾保留统一 8B trailer。

page3 body 使用大端编码：

| 相对页首偏移 | 长度 | 字段 | 语义 |
| --- | ---: | --- | --- |
| 38 | 4 | magic | `0x53444931`，ASCII `SDI1` |
| 42 | 4 | formatVersion | 固定 `1` |
| 46 | 8 | tableId | `0` 表示已格式化但尚无已发布快照 |
| 54 | 8 | dictionaryVersion | 空页为 `0`，有效快照必须为正 |
| 62 | 4 | payloadLength | 空页为 `0`；有效值不得超过单页容量 |
| 66 | 4 | payloadCrc32c | payload 的 CRC32C 低 32 位；空页为 `0` |
| 70 | N | payload | DD codec 产生的完整 table 聚合快照 |

写入在一个 MTR 内先写 payload，再写长度、CRC 和 identity；MTR commit 为 page0/page3
收集 redo、盖 page LSN 并发布脏页。flush 前沿用现有 WAL gate 和 doublewrite，不新增旁路 IO。

## 5. DD payload v1

payload 自带独立 magic/version，避免未来离线导出时必须依赖页外上下文。按顺序编码：

1. payload magic、format version。
2. tableId、schemaId、display/canonical table name、dictionaryVersion、稳定 table state code。
3. storage binding：spaceId、规范路径、每个 index 的 root page/level/leaf segment/non-leaf segment，
   以及可选 LOB segment。
4. 按 ordinal 排列的 columns：identity/name/type stable code、unsigned/nullable、length/scale、
   charset/collation、ENUM/SET symbols。
5. 按定义顺序排列的 indexes：identity/name/unique/clustered 与有序 key parts。

所有 count、字符串长度和 payload 总长度都有显式上界。decoder 必须消费到 EOF，拒绝未知 code、
负数/过大 count、截断、尾随字节、canonical name 不一致和聚合不变量不成立。编码结果必须确定：
相同 `TableDefinition` 产生相同字节，供 recovery 做精确比较。

## 6. CREATE 数据流

1. 物理 CREATE 在既有 MTR 中完成 page0/FSP、segment 和 index root 后，因该 space 尚未向 DD 发布，
   在有明确无并发访问证明的 latch-order 例外作用域内格式化固定 page3，并把 page0 root 设为 3。
2. 物理 MTR commit、redo/dirty flushThrough 和 tablespace force 成功后返回 binding，DDL log 写
   `ENGINE_DONE`。
3. DD 组装最终 ACTIVE `TableDefinition`，`DictionarySdiCodec` 编码完整快照；
   storage facade 以独立 MTR 覆盖 page3，并等待 redo/dirty durable 后 force tablespace。
4. SDI durable 后才提交 ACTIVE catalog，再推进 `DICTIONARY_COMMITTED`、cache 和 `COMMITTED`。

阶段 1 或 2 失败沿用 physical CREATE 的补偿/保留规则。阶段 3 失败时 DD 尚未提交；
marker 最多为 `ENGINE_DONE`，启动 recovery 会按 exact path 删除无 committed DD 的文件。
阶段 4 响应不确定时文件、SDI 和可能已提交 DD 都保留，由启动时双方真相裁决。

## 7. 启动校验与修复

storage crash recovery 先按正常顺序完成 doublewrite repair 和 redo replay，再由 DDL recovery：

1. 收敛非终态 CREATE/DROP marker 和 legacy DROP_PENDING。
2. 重新读取最新 committed snapshot，只选择 `ACTIVE + binding + existing/opened GENERAL space`。
3. 编码 committed table 的期望 SDI，并读取实际 page0/page3。
4. identity、version 和 payload 全部一致时不写页。
5. root 为 0、空 SDI、逻辑 header/CRC 损坏或内容错配时，以 committed DD 覆盖 page3并将 root 设为 3。
6. 未知 root、binding/opened path 不一致、space/page envelope 不一致或底层 page checksum 损坏时阻止 OPEN。
7. 全部 ACTIVE 表校验/修复成功后，才执行最终 orphan cleanup 并发布数据库 OPEN。

此策略不会把 SDI 中较高 version 误当成未提交 DDL。即使 SDI 比 catalog 新，也必须回写 catalog
当前 committed 版本；未提交物理尝试只能由 DDL log 和 DD 提交状态决定 finish/rollback。

## 8. 并发、资源与失败边界

- 普通 CREATE 持 table MDL X；SDI 写入不引入新的 MDL 等待。
- storage facade 进入 MTR 前校验 binding path 与已打开 tablespace path 完全一致。
- MTR 通过 tablespace shared operation lease 阻止与 DROP/TRUNCATE exclusive lease 交叉。
- page latch 顺序固定为 page0→page3。CREATE 内唯一例外是已经持有更高 index root 时初始化 page3；
  该 space 尚未发布且没有其它业务 MTR，例外作用域必须写明这一无环证明。
- durable write 顺序为 MTR commit→`flushThrough(commitLsn, timeout)`→`PageStore.force(spaceId)`。
- timeout 必须为正；失败不得清除或伪造 DD/DDL marker，只把原始 cause 包装为领域异常。
- read 只使用 read-only MTR；无论成功或异常都由 manager commit/rollback 释放 latch/fix/lease。

## 9. 测试要求

- codec round-trip：28 类字段属性中的代表性 scalar/LOB/ENUM，多个 index、DESC/prefix、LOB binding。
- codec 损坏：未知版本/code、count 越界、截断、尾随、canonical name 不一致。
- page 格式：page0 root=3、page3 type/identity/magic/version、payload CRC、单页容量上界。
- storage 协作：write/read、重启后读取、legacy root=0 补写、binding path 错配拒绝。
- CREATE 集成：正常返回时 SDI 与 ACTIVE DD 一致；SDI 失败不得发布 DD。
- recovery：空 SDI、旧 version、不同 payload、逻辑 CRC 损坏均按 committed DD 重写；
  未知 root、物理 envelope/checksum 损坏 fail-closed。
- DROP 回归：DROP_PENDING/物理删除/重启续作不要求更新将被删除的 SDI。
- 全量回归：测试总数不得倒退，禁止 `synchronized`、裸 `RuntimeException` 和 storage→DD 反向 import。

## 10. 后续扩展

catalog-loss rebuild 需要另行设计 schema 级 SDI、目录 manifest、重复 identity/path 冲突隔离和
“完整扫描结束”提交点。多页 SDI 应通过独立 segment/B+Tree 或有界 page chain 扩展，不能改变 v1
page3 header 的既有字段语义；v1 decoder 保持只接受 format 1，升级由显式迁移器完成。
