# Spec：D4b — record.page 的 MTR 生产入口（IndexPageAccess）

- 日期：2026-06-16
- 关联设计：`innodb-disk-manager-design.md`（redo 集成）、D3 spec（`docs/superpowers/specs/2026-06-16-mtr-redo-append-skeleton-design.md` §9 D4b 衔接点）、`innodb-record-design.md`（§7 INDEX 页、§14 访问入口）。
- 上游依赖：storage.api、storage.buf（BufferPool/PageGuard/PageLatchMode）、storage.mtr（MiniTransaction）、storage.page（PageEnvelope/FilePageHeader/PageType）、storage.record.page（RecordPage 及 R3–R5 算子）、domain。
- 前置：D3、D4a 全绿。
- 状态：D4b 让 record 的页格式化/写入经 MTR-owned guard 自动产 redo——补上「record redo integration」，R3–R5 算子不变。

## 1. 背景与范围

R3–R5 的 record 算子（RecordPage/RecordPageInserter/Search/Updater/Purger/Deleter/Reorganizer）都在一个外部传入的 `PageGuard` 上工作；现有测试经 `pool.getPage(...)` 直接取 guard（**绕过 MTR**），故 record 写**不产 redo**。D4b 提供一个薄 facade，把「建 RecordPage 的页」绑定到 MTR（`mtr.getPage`/`mtr.newPage`），使 record 写经 D3 的 collector 自动捕获为 PAGE_BYTES、commit 盖 pageLSN；INDEX 页创建产 `PAGE_INIT(INDEX)`。

**做**：新建 `IndexPageAccess`（storage.api）：`createIndexPage`（建/格式化 INDEX 页）、`openIndexPage`（取页做 CRUD/只读）。

**不做**（注释标注，后续片）：
- 不封装 insert/find/update 等行级 CRUD（facade 只返回 RecordPage，算子由调用方用；行级编排是未来 btree/SQL 职责）。
- 不改 R3–R5 算子签名/实现；不让 record.page 依赖 mtr（保持 R3 解耦）。
- 不做跨页 split/merge/root 维护（btree）。
- 不与 `DiskSpaceManager.allocatePage` 合并（分配与格式化分属两个 facade，调用方串联）。

## 2. 关键决策（写进代码注释）

1. **facade 落点 = storage.api `IndexPageAccess`**（持 `pool`+`pageSize`），不把 MTR 绑定塞进 record.page（RecordPage 继续吃裸 guard，保持可独立测试）。storage.api 是顶层 facade，依赖 buf/mtr/storage.page/record.page 均为允许方向。
2. **createIndexPage 用 `mtr.newPage(pageId, X, INDEX)`**（页创建语义：D4a 已让 newPage 对驻留页重初始化）。优点：自包含、可独立测试（不依赖先 allocatePage）；与真实 btree 串联（allocatePage→createIndexPage）时，同页会出现两条 `PAGE_INIT`（ALLOCATED→INDEX），恢复时后者 LSN 胜，**冗余但正确**（简化点）。
   - **破坏性前置（写进方法 Javadoc + §1 不做）**：因走 newPage（驻留页会被清零重初始化），`createIndexPage` **只能用于新分配/有意重初始化的页**；对**已有 INDEX 页**的读写必须走 `openIndexPage`，否则会清空在用页。本片不做「页已是 INDEX 则拒绝重建」的运行时校验（无 caller/btree 守护，YAGNI），仅以契约+Javadoc 约束。
3. **信封 + format 分两步写**：`PageEnvelope.writeHeader`（写 `[0,38)` 信封，type=INDEX、pageLsn=0 由 commit 盖）+ `RecordPage.format(indexId, level)`（写 `[38,..)` 的 INDEX 页头/infimum/supremum/目录）。两者都经 PageGuard → 被 collector 捕获为 PAGE_BYTES。RecordPage.format 不碰信封（沿用 R3 约定）。
4. **facade 返回 RecordPage，不 close guard**：guard 由 mtr memo 持有，commit 时释放并盖 pageLSN。调用方在**同一 MTR 内**用返回的 RecordPage 跑算子；不要跨 MTR 持有。

## 3. 组件：IndexPageAccess（storage.api）

```
public final class IndexPageAccess {
    private final BufferPool pool;
    private final PageSize pageSize;
    public IndexPageAccess(BufferPool pool, PageSize pageSize) { ...非空校验... }

    /** 建并格式化一个 INDEX 页（要求在 mtr 内）：newPage(X,INDEX) → 写信封(INDEX) → format(indexId,level)。
     *  产 PAGE_INIT(INDEX) + 信封/格式 PAGE_BYTES；commit 盖 pageLSN。返回的 RecordPage 由 mtr 持 guard，勿 close。
     *  <b>破坏性入口</b>：因走 newPage（D4a 对驻留页会重初始化清零），**只能用于新分配/有意重初始化的页**；
     *  对已有 INDEX 页做读写**必须走 {@link #openIndexPage}**，否则会清空在用页。 */
    public RecordPage createIndexPage(MiniTransaction mtr, PageId pageId, long indexId, int level);

    /** 取已存在页做 CRUD（X）或只读扫描（S）：getPage(mode) → new RecordPage。只读不产 redo。 */
    public RecordPage openIndexPage(MiniTransaction mtr, PageId pageId, PageLatchMode mode);
}
```

`createIndexPage` 体（**校验全部前置——任何写页/newPage 之前**；否则 level<0/indexId<0 在 format/IndexPageHeader 才失败时，页已被 newPage 重初始化并收集 PAGE_INIT，而 MTR rollback 不做内容 undo → 脏页/半成品）：
```
// 1) 先校验，再碰页
if (mtr == null || pageId == null) throw new DatabaseValidationException(...);
if (indexId < 0) throw new DatabaseValidationException("indexId must be non-negative: " + indexId);
if (level < 0) throw new DatabaseValidationException("level must be non-negative: " + level);
// 2) 校验通过后才创建/写页
PageGuard g = mtr.newPage(pool, pageId, PageLatchMode.EXCLUSIVE, PageType.INDEX);
PageEnvelope.writeHeader(g, new FilePageHeader(pageId.spaceId(), pageId.pageNo().value(),
        FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.INDEX));
RecordPage rp = new RecordPage(g, pageSize);
rp.format(indexId, level);
return rp;
```
`openIndexPage` 体（同样先校验）：
```
if (mtr == null || pageId == null || mode == null) throw new DatabaseValidationException(...);
PageGuard g = mtr.getPage(pool, pageId, mode);
return new RecordPage(g, pageSize);
```
（`pageSize`/`pool` 非空在构造器已校验。indexId/level 的上界等仍由 `IndexPageHeader` 构造兜底，但那是校验通过后的写阶段，不影响「非法入参在碰页前即拒」。）

## 4. 数据流 / redo

- `createIndexPage`：`PAGE_INIT(pageId, INDEX)` + 信封 PAGE_BYTES + format 的多条 PAGE_BYTES；该页入 touchedPages，commit 盖 endLsn。
- `openIndexPage(X)` + 算子写：算子经 RecordPage 的 `writeRecordBytes/setNextRecord/setHeapNo/setNOwned/setDeleted/writeHeader/directory.*` 写页 → collector 捕获 PAGE_BYTES；commit 盖 pageLSN。
- `openIndexPage(S)` + 只读：无写 → 不产 redo、不盖 pageLSN。

## 5. 依赖

`IndexPageAccess`(storage.api) → buf + mtr + storage.page + record.page + domain。RecordPage 不新增依赖（仍裸 guard）。无环（storage.api 顶层）。

## 6. 并发与 latch

- 所有方法在调用方的 MTR 内；guard 入 memo 持到 commit（盖 pageLSN 需其 guard，沿用 D3/D4a 模型）。
- 故「单 MTR 内创建/打开 N 个 INDEX 页」会同时占 N 个帧（与 D4a 同理）；测试 pool 取足够大（≥8）。
- 无新锁；不使用 synchronized。

## 7. 异常

- 复用 `DatabaseValidationException`（参数）、`MtrStateException`（MTR 生命周期）、`RecordPageOverflowException`/`PageDirectoryCorruptedException`（算子内）。
- 本片不新增异常类。

## 8. 测试（storage.api，自包含：FileChannelPageStore + LruBufferPool + MiniTransactionManager + IndexPageAccess；不需 DiskSpaceManager）

> **所有读验证的 MTR 必须显式释放**：MTR-owned guard 生命周期到 commit 结束（D3），读 MTR（m2）断言后须 `mgr.commit(m2)`（异常路径 `mgr.rollbackUncommitted(m2)`），否则 fix/latch 泄漏、`pool.close` 失败。断言在 m2 active（guard 有效）期间做，再 commit。

- `createIndexPageEmitsInitAndFormatRedo`：m1 begin → `createIndexPage(m1, p, indexId=7, level=0)` → `mgr.commit(m1)`；`mgr.redoLogManager().bufferedRecords()` 含 `PageInitRecord(p, INDEX)` 且有 PAGE_BYTES，记 `endLsn=currentLsn`；m2 begin → `rp2=openIndexPage(m2, p, SHARED)` → 断言 `rp2.header().indexId()==7`、`PageEnvelope.readPageLsn`(经 m2 的 guard 或 readHeader) ==endLsn、type=INDEX → `mgr.commit(m2)`。
- `insertThroughMtrOwnedPageEmitsRedo`：m1 begin → `rp=createIndexPage(m1, p, 7, 0)` → `new RecordPageInserter(reg).insert(rp, p, row(1,"a"), keyDef, schema)` → `mgr.commit(m1)`；redo buffer 非空且含记录写 PAGE_BYTES；m2 begin → `rp2=openIndexPage(m2, p, SHARED)` → `new RecordPageSearch(reg).findEqual(rp2, kId(1), keyDef, schema)` 命中 → `mgr.commit(m2)`。
- `openSharedProducesNoRedo`：createIndexPage(m1)+`commit(m1)` 后记 `before=currentLsn`、`pageLSN`；m2 begin → `openIndexPage(m2, p, SHARED).header()` 只读不写 → `mgr.commit(m2)`；断言 `currentLsn==before`、pageLSN 不变（S 路径无写、无盖戳）。
- 参数校验：`createIndexPage(m, p, indexId=-1, 0)` / `level=-1` / null 抛 `DatabaseValidationException`（在 newPage 前抛、页未被改）；可顺带断言 redo buffer 为空。

## 9. Impact（编辑前跑）

纯新增 `IndexPageAccess`，无既有符号修改 → 无 upstream，低风险。仍跑 `gitnexus_impact`(IndexPageAccess 预期空) + 批末 `gitnexus_detect_changes`。

## 10. 批次

单批 **D4b**：`IndexPageAccess` + 4 测试 → 全量 `clean test`（固定 JDK/Gradle）→ `npx gitnexus analyze`。D4b 全绿后报告；R1（writer/flusher/redo 文件/recovery）单独确认再开。
