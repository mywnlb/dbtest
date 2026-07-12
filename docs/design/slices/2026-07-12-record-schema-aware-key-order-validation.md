# 0.21d Record Schema-aware Key Order Validation Slice

## 目标

- 在物理页结构校验之后，用真实 schema、keyDef 与 0.21c collation 语义验证 INDEX 页用户链顺序。
- leaf 与 internal 页在参与查找、scan、split/merge 或写入前发现逆序、错误 record type 和字段损坏。
- 保持 `IndexPageAccess` / `RecordPageStructureValidator` schema-free，不把 B+Tree 元数据下沉到 Buffer/Record 物理入口。

## 当前事实与 blast radius

- `IndexPageAccess.openIndexPageHandle` 已在每次既有页 fix 后完成物理 header/live/FREE/directory 校验，但没有 schema/keyDef。
- `BTreeIndex` 同时持 indexId、leaf schema 与 keyDef；internal 页 schema 可由 `BTreeNodePointerSchema.from(index)` 确定派生。
- SplitCapable 的统一私有 open helper 有 12 个既有页调用点；另 3 个直开 handle 均是刚 create/format 的空新页。
- LeafOnly 只有 `openRootLeaf` 一个集中入口；高扇出 `recordPage()` 视图和 IndexPageAccess API 不需要修改。

## Record 层校验语义

- `RecordComparator` 增加 record-to-record 比较入口，逐 part 复用 `EncodedKeyPartComparator`，不物化 `ColumnValue`。
- 新增 `RecordPageKeyOrderValidator`，输入 PageId、RecordPage、TableSchema、IndexKeyDef、期望 RecordType 与共享 registry。
- 按 `recordOffsetsInOrder()` 比较相邻用户记录；要求 previous <= current（索引序），首条/空页直接通过。
- 重复 key、prefix 等价 key 与相同 collation weight 合法；unique/MVCC duplicate 仍由 B+Tree/事务语义处理。
- delete-mark 记录仍在逻辑链中且 key 未变，必须参与顺序校验；不能跳过后造成隐藏逆序。
- leaf 只允许 CONVENTIONAL，level>0 internal 只允许 NODE_POINTER；混入另一类型即按损坏拒绝。
- 每条非 NULL CHAR/VARCHAR key slice 先经 registry 严格 charset 校验；坏 UTF8/LATIN1 不得仅按原始字节继续排序。
- DESC、NULL、byte-prefix、BINARY/ASCII-CI 全部沿 0.21c 统一比较器，不建立 validator 专用排序规则。
- 新增 `RecordKeyOrderCorruptedException`；诊断包含 pageId、相邻 offset/type，字段解码或 collation 错误保留 cause。

## B+Tree 生产接线

- LeafOnly 在 root indexId/level 校验后，用 leaf schema/keyDef/CONVENTIONAL 校验一次，再进入 search/scan/insert。
- SplitCapable 的既有页 open helper 增加 BTreeIndex 参数：fix+物理校验 → indexId 核对 → 按实际 level 选 leaf/internal schema → 顺序校验。
- 12 个 helper 调用点统一传 index；现有重复 header 防御可保留，本片不重构 descent/SMO 控制流。
- fresh create/format 的空页直开不做顺序校验；内容由有序 inserter 灌入，并在下一次既有页打开时进入统一校验。
- 校验在既有 S/X latch 内只读执行，不新增 latch、事务锁、等待、redo、dirty 标记或修复动作。

## 非目标与简化

- 不验证跨 sibling 页的 max(left)<=min(right)、parent lowKey 与 child 最小 key、child PageId 存在性或树可达性。
- 不在每个写算子结束时重复校验；本片保证下次打开前 fail-closed，写算法正确性继续由现有回归测试保护。
- 不缓存“已校验 pageLSN/schemaVersion”；当前与物理 validator 一样每次 fix 做 O(n) 扫描，优先教学正确性。
- 不实现页修复、重排、recovery 重建、DD schema discovery 或在线 DDL schema-version bridge。

## 验收测试与文档

- validator 单测覆盖 ASC/DESC、NULL、ASCII-CI、byte-prefix、重复/等价 key、delete-mark、空/单记录。
- 损坏测试用等长字段原地改写制造“物理结构合法但 key 逆序”，并覆盖错误 record type、坏 UTF8 与 cause。
- LeafOnly 与 SplitCapable 协作测试证明既有页在 search/写入前拒绝逆序；合法 leaf/internal、split/merge/root shrink 不倒退。
- 全量测试通过；current map 标出 schema-aware B+Tree open 链，backlog 完成 0.21d 并推荐 0.21e inline scalar types。

## 5 遍复核清单

- 第 1 遍：对照 Record §7/§11/§15 与 B+Tree 页/导航设计，确认非降序、类型和 fail-closed 语义。
- 第 2 遍：逐一核对 12 个 existing-page helper 调用与 LeafOnly root，fresh page 例外没有误漏既有页。
- 第 3 遍：核对 leaf/internal schema 选择、prefix/重复/NULL/DESC/collation 矩阵不改义。
- 第 4 遍：核对 latch/依赖/异常边界，无 schema 下沉、反向依赖、写页、redo 或资源释放变化。
- 第 5 遍：核对测试能构造物理合法的逆序页，并覆盖 current map/backlog 与下一切片边界。
