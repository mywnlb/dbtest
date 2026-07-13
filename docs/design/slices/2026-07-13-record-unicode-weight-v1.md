# 0.21g Record Unicode Weight V1

## 目标

- 增加稳定、版本化的 UTF-8 Unicode 教学排序规则。
- 提供 case/accent/combining-mark 等价，同时保持索引跨运行环境可重复。
- 让 leaf、node-pointer、页内校验和 prefix index 共用相同权重语义。

## 设计依据

- `innodb-record-design.md` §8/§11：字符比较必须走 CollationStrategy，leaf/node 同序。
- `current-implementation-map.md`：collation registry 初始化后只读，stable id 不可改写。
- 现有 byte-prefix 可能截断 UTF-8 code point，引入 Unicode weight 前必须闭合字符边界。

## 关键决策

- 新 stable id `UTF8_UNICODE_CI_V1=4`，只与 UTF8 组成合法 pair。
- V1 权重代码随仓库固定，不使用 JDK Collator、默认 locale 或可变外部表。
- ASCII/Latin-1、Greek、Cyrillic 提供确定性 case fold。
- Latin-1 accent 映射到基础字母，U+0300..U+036F combining marks 不产生主权重。
- 未显式折叠的 Unicode code point 使用自身码点权重，保持确定性与全字符可比较。
- UTF-8 必须严格解码；malformed bytes 作为字段损坏 fail-closed。
- `prefixBytes` 仍是 byte budget，但 UTF8_UNICODE_CI_V1 会退到预算内最后一个完整 code point。

## 与 MySQL/InnoDB 的差异

- V1 不是 MySQL utf8mb4_0900_ai_ci，也不复刻 UCA 全量 weight table。
- 不实现 locale tailoring、contraction、expansion（如 ß=ss）或 secondary/tertiary weight。
- 未列入教学 fold 表的大小写/重音字符不会自动等价。
- stable id 绑定当前 V1；未来扩表必须新增 V2 id，不能静默改变已建索引顺序。

## 非目标

- 不修改既有 BINARY、UTF8_ASCII_CI、LATIN1_ASCII_CI 的 stable id 或行为。
- 不把 prefixBytes 改名或改为字符数，避免既有 schema 语义漂移。
- 不增加运行时 collation 注册/替换能力或 weight cache 进 page body。
- 不实现 SQL COLLATE 解析、DD 持久化迁移或索引重建编排。

## 验收测试

- stable id/fromStableId 钉死 4，UTF8 pair 可解析、LATIN1 错配拒绝。
- 预组合/分解 accent、ASCII/Greek/Cyrillic case 等价。
- 未映射 code point 保持稳定码点序，非法 UTF-8 保留 cause 并拒绝。
- SearchKeyComparator 覆盖 ASC/DESC、Unicode 等价和完整字符 prefix。
- RecordComparator/页内校验既有矩阵不得倒退；全量 Gradle test 通过。

## Current Map / Backlog 更新

- `record.schema/type` 记录 stable Unicode V1 pair 与限制。
- prefix gap 更新为 byte budget + UTF-8 完整字符边界。
- 0.21 剩余项不再包含基础 Unicode weight，后续升级必须新增版本 ID。
