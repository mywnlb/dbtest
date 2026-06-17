# Spec：mtr 最小 mini-transaction（首版切片）

- 日期：2026-06-10
- 关联设计：`C:\coding\java\self\minimysqldesign\docs\innodb-disk-manager-design.md`（§9、§13.2、§14、§17、§18、§20 第 4 步）
- 上游约束：本仓库 `AGENTS.md` / `CLAUDE.md`
- 依赖切片：buf 最小 Buffer Pool（`BufferPool`/`PageGuard`/`PageLatchMode`）已完成
- 状态：brainstorming 评审 + 7 处复查修正已并入，待自查 + 用户复核

## 1. 背景与目标

buf（受控页访问：fix + S/X page latch）已完成。本切片实现设计 §20 第 4 步「mtr：memo 栈、redo collector、commit skeleton」的最小版：mini-transaction 作为短物理临界区，用 memo 收集 page latch + buffer fix，commit/rollback 时 LIFO 释放；savepoint 提前释放局部资源；MiniTransactionManager 绑定线程、禁静默嵌套。它是 fsp 分配走 MTR 的前置。

## 2. 范围与非目标

**做**：MiniTransaction 生命周期状态机；memo（持 buf `PageGuard` = page latch + buffer fix）LIFO 释放；savepoint；`MiniTransaction.getPage/newPage` 包装 buf；MiniTransactionManager（线程绑定、禁嵌套、begin/current/commit/rollbackUncommitted）。

**不做**（注释标简化点）：
- redo 收集 / LSN 分配 / pageLSN / WAL 排序——commit 不产 redo（§9.4 的「redo→标脏→释放」顺序留 redo 切片）。
- rollback **不撤销** buffer 内已改内容（MTR 无 content undo；物理修改留待 redo/recovery 切片，首版 rollback 仅释放资源，注释标注此缺口）。
- `TABLESPACE_LATCH` / `SPACE_RESERVATION` memo 类型（fil/fsp 概念）。
- `beginReadOnly`/`beginSync` log mode 变体。
- memo 槽类型枚举：首版 memo 持通用 `AutoCloseable`。
- **同页 S→X 锁升级**：`ReentrantReadWriteLock` 无升级，同一线程持 S 再求 X 会自死锁；首版不支持，将写的页直接取 X（注释强约束）。

## 3. 包与依赖方向

- 新增类位于 `cn.zhangyis.db.storage.mtr`。
- 依赖：`mtr → buf`（`BufferPool`/`PageGuard`/`PageLatchMode`）、`mtr → domain`（`PageId`）、`mtr → common.exception`、`java.util.concurrent.atomic`。**禁止** import `fsp`/`fil` 内部/上层（§15）；redo 暂未建。依赖图 `mtr→buf→fil` 无环。
- 不用 `@Slf4j` 除非有日志点（本切片无）。

## 4. 组件设计

### 4.1 `MiniTransactionState`（枚举）

`NEW`、`ACTIVE`、`COMMITTING`、`COMMITTED`、`ROLLED_BACK`。合法流转（`canTransitTo`/`validateTransitTo`，非法抛 `MtrStateException`）：
- `NEW → ACTIVE`
- `ACTIVE → COMMITTING`、`ACTIVE → ROLLED_BACK`
- `COMMITTING → COMMITTED`
- 终态：`COMMITTED`、`ROLLED_BACK`；`COMMITTING` 在释放失败时为不可复用的半终态（§17「commit 失败不允许复用」）。

### 4.2 `MtrStateException extends DatabaseRuntimeException`（§17）

用于：嵌套 begin、终态/非 ACTIVE 状态下操作、跨线程或未绑定的 commit/rollback、无当前 MTR 时 current()、savepoint 跨 MTR 误用、非法状态流转。构造 `(message)` 与 `(message, cause)`。

### 4.3 `MtrMemo`（包内 final 类）

LIFO `AutoCloseable` 资源栈 + savepoint 支持。

- `private final ArrayDeque<AutoCloseable> stack`（作栈：push/pop 头部，LIFO）。
- `void push(AutoCloseable resource)`：入栈（resource 非空，否则 DatabaseValidationException）。
- `int depth()`：当前栈深。
- `void releaseTo(int targetDepth)`：`targetDepth<0` 抛 DatabaseValidationException；当前深 ≤ targetDepth 时 no-op；否则 `while size>targetDepth { pop; close }`，LIFO 释放。
- `void releaseAll()`：`releaseTo(0)`。
- close 异常处理：memo 持的是 `AutoCloseable`（其 `close()` 声明 `throws Exception`），故 `catch (Exception)`；收集首个异常、其余 `addSuppressed`，循环结束后若有则包成 `MtrStateException("failed to release N memo resource(s)", first)` 抛出——保证「即便某资源释放失败也继续释放其余」，不泄漏 latch/fix。
  - 实际持有的 `PageGuard.close()` 不抛受检异常、正常不抛；聚合是防御。

### 4.4 `MtrSavepoint`（public record）

`MtrSavepoint(long mtrId, int depth)`：类型化保存点，带所属 MTR id 防跨 MTR 误用。

### 4.5 `MiniTransaction`（public final 类）

- 字段：`final long id`、`MiniTransactionState state`、`final MtrMemo memo`。**单线程拥有，非线程安全。**
- 构造 `MiniTransaction(long id)`（包内）：state=`NEW`。
- `void activate()`（包内，仅 Manager 调）：`NEW→ACTIVE`。
- `MiniTransactionState state()` / `long id()`：getter。
- `PageGuard getPage(BufferPool pool, PageId pageId, PageLatchMode mode)`：`ensureActive()` + 校验非空 → `PageGuard g = pool.getPage(pageId, mode)` → `memo.push(g)` → 返回 g。
  - **契约**：返回的 guard 由 MTR 拥有生命周期，**调用方不要自行 close**（latch/fix 须 held 到 commit/savepoint 释放）；`PageGuard.close` 幂等仅防双关，不防过早 close 的逻辑错误。
  - **约束**：同页将写则直接取 X，禁 S→X 升级（自死锁，见 §2）。
- `PageGuard newPage(BufferPool pool, PageId pageId, PageLatchMode mode)`：同上，调 `pool.newPage`。
- `MtrSavepoint savepoint()`：`ensureActive()` → `new MtrSavepoint(id, memo.depth())`。
- `void rollbackToSavepoint(MtrSavepoint sp)`：`ensureActive()`；sp 非空；`sp.mtrId()!=id` → MtrStateException；`memo.releaseTo(sp.depth())`。
  - 语义：**只释放 sp 之后获取的 latch/fix**（§9.2 提前释放局部 latch），**不撤销页内容**；释放的页若写过，dirty 照常置位。建议只对未修改页用（提前释放已改页 X latch 在有 redo 后违反 WAL；首版无 redo 暂不强制，注释提醒）。
- `void commit()`（包内，仅 Manager 调）：`state!=ACTIVE`→MtrStateException；`state=COMMITTING`；`memo.releaseAll()`（LIFO close 每个 guard = 释 page latch + 释 buffer fix + 按 wrote 标脏）；`state=COMMITTED`。释放失败则 state 停在 COMMITTING（不可复用）。
- `void rollbackUncommitted()`（包内，仅 Manager 调）：`transitTo(ROLLED_BACK)`（ACTIVE→ROLLED_BACK，非 ACTIVE 则抛 MtrStateException）；`memo.releaseAll()`。与 commit「先推进状态再释放」一致；`releaseAll` 即便部分 close 失败也会排空全部资源（见 §4.3），故无论成败 ROLLED_BACK 都正确反映完成、无资源泄漏。**不撤销已写入 buffer 的内容**（简化点）。
- `private void ensureActive()`：`state!=ACTIVE` → `MtrStateException("mini transaction not active: " + state)`。

### 4.6 `MiniTransactionManager`（public final 类）

- `private final ThreadLocal<MiniTransaction> current`、`private final AtomicLong idSequence`。
- `MiniTransaction begin()`：`current.get()!=null` → MtrStateException（禁静默嵌套 §13.2，需嵌套应显式建 child）；`mtr=new MiniTransaction(idSequence.incrementAndGet())`；`mtr.activate()`；`current.set(mtr)`；返回。
- `MiniTransaction current()`：`mtr=current.get()`；null → MtrStateException；返回。
- `void commit(MiniTransaction mtr)`：`requireBound(mtr)`；`try { mtr.commit(); } finally { current.remove(); }`。
- `void rollbackUncommitted(MiniTransaction mtr)`：`requireBound(mtr)`；`try { mtr.rollbackUncommitted(); } finally { current.remove(); }`。
- `private void requireBound(MiniTransaction mtr)`：mtr 非空；`current.get()!=mtr` → MtrStateException（必须与 begin 同线程、且是当前绑定的那个）。
- **try/finally 解绑**：即便 commit/rollback 抛异常也 `current.remove()`，防 ThreadLocal 泄漏与下次 begin 误报嵌套。

## 5. 数据流

- **begin**：`mgr.begin()` → 无 current → 新 MTR(NEW) → `activate()`(ACTIVE) → 绑线程。
- **访问**：`mtr.getPage(pool,pid,X)` → `pool.getPage` → push memo → 返回 guard（调用方读写、不 close）。
- **savepoint/局部释放**：`sp=mtr.savepoint()` → 再取若干页 → `mtr.rollbackToSavepoint(sp)` → LIFO 释放 sp 之后的 latch/fix。
- **commit**：`mgr.commit(mtr)` → ACTIVE→COMMITTING → memo LIFO close（释 latch+fix，按 wrote 标脏）→ COMMITTED → 解绑。
- **rollback**：`mgr.rollbackUncommitted(mtr)` → memo LIFO close → ROLLED_BACK → 解绑（不撤销 buffer 改动）。

## 6. 并发

- MiniTransaction 绑定单线程、不共享，memo 仅属主线程访问 → **mtr 自身无需加锁**；其持有的 page latch / buffer fix 的并发由 buf 内部锁负责。
- Manager 用 `ThreadLocal` 天然按线程隔离；`idSequence` 用 `AtomicLong`。
- **同页 S→X 自死锁**：`ReentrantReadWriteLock` 无锁升级，同线程持读锁再求写锁会等自己 → 死锁。首版不支持同页升级；将写的页直接 X。S→S、X→X、X→S（降级）均安全。

## 7. 异常

- `MtrStateException`（Runtime）：状态/绑定/嵌套/savepoint 误用。
- `DatabaseValidationException`：null 参数、savepoint 深度非法。
- buf 抛出的 `BufferPoolExhaustedException` 等透传（getPage 内）。

## 8. 测试（buf `LruBufferPool` + fil `FileChannelPageStore` + @TempDir 真实驱动）

- `MiniTransactionStateTest`：合法/非法流转。
- `MtrMemoTest`（单元，假 AutoCloseable 记录释放序）：push/depth/releaseTo/releaseAll 的 LIFO 序；releaseTo 越界 no-op；某资源 close 抛异常仍释放其余并聚合抛 MtrStateException。
- `MiniTransactionManagerTest`：begin→ACTIVE；嵌套 begin→MtrStateException；current() 有/无；commit 解绑后可再 begin；跨线程 commit→MtrStateException。
- `MiniTransactionTest`（集成）：
  - commit 释放 fix：capacity=1，begin+getPage(p0)，此时 `pool.getPage(p1)`→Exhausted；commit 后 `pool.getPage(p1)` 成功。
  - savepoint 提前释放：capacity=2，getPage(p0)→savepoint→getPage(p1)，`pool.getPage(p2)`→Exhausted；`rollbackToSavepoint`→`pool.getPage(p2)` 成功；commit 释放 p0。
  - rollbackUncommitted 释放 + ROLLED_BACK。
  - 终态后 getPage → MtrStateException。
  - getPage(X)+writeInt+commit → 标脏 → `pool.flush` 落盘，新 pool 读回。
  - savepoint 跨 MTR 误用 → MtrStateException。

## 9. 简化点（注释标注）

- commit 不产 redo / LSN / pageLSN / WAL 排序；标脏在 memo 释放过程中落定。
- rollback 不撤销 buffer 改动（无 undo）。
- 不实现 TABLESPACE_LATCH / SPACE_RESERVATION memo 类型、beginReadOnly/beginSync、memo 槽类型枚举。
- 不支持同页 S→X 锁升级。

## 10. 自查修正记录

逐项追踪并发/边界后确认设计成立，补强如下：

1. **同页重复取 latch 行为复核**：X→X（同线程写锁可重入，产生 2 个 memo 条目 + fixCount=2，commit 时 LIFO 各 close 一次，写锁计数与 fix 归零）安全；X→S（持写锁可再取读锁，降级）安全；**S→X 禁止**（无升级 → 自死锁），已在 §2/§6 强约束。
2. **memo close 的受检异常**：memo 持 `AutoCloseable`（`close() throws Exception`），故 `releaseTo` 用 `catch (Exception)`、聚合成 `MtrStateException` 重抛；实际持有的 `PageGuard.close()` 不抛受检异常、正常不抛。
3. **commit 释放失败语义**：`memo.releaseAll()` 抛出时 state 停在 `COMMITTING`（不可复用，§17）；Manager 的 `finally` 无论如何 `current.remove()` 解绑，不泄漏线程绑定。
4. **误用防护**：跨线程/未绑定 commit 由 `requireBound`（`current.get()!=mtr`）拦截；commit 后若仍用已 close 的 guard，由 `PageGuard.ensureOpen` 抛 `DatabaseValidationException` 兜底。
5. **MtrSavepoint 伪造缓解**：record 必须 public（fsp 等上层要持有），其 public 构造可被外部伪造；`rollbackToSavepoint` 用 `mtrId` 校验跨 MTR、`releaseTo` 对「深度 > 当前」no-op、「负深度」抛校验异常，使伪造基本无害；Javadoc 要求只用 `savepoint()` 返回值。
6. **NEW 状态被真正使用**：构造=NEW，Manager.begin 调 `activate()`（NEW→ACTIVE），状态机 NEW→ACTIVE→COMMITTING→COMMITTED / NEW→ACTIVE→ROLLED_BACK 全程有意义。
7. **rollbackToSavepoint 非 content undo**：只释放 sp 之后的 latch/fix；释放的页若写过仍标脏（物理改动留存），§4.5/§2 已写明并提醒「提前释放已改页 X latch 在有 redo 后违反 WAL」。

## 11. 后续切片衔接

- redo 切片：commit 接入 LSN 分配、redo 收集、pageLSN、WAL 排序（redo→标脏→释放）；引入真正 rollback(undo)。
- fsp 分配走 `mtr.getPage/newPage`，并把 SPACE_RESERVATION 收入 memo。
- 补 beginReadOnly/beginSync log mode 与 TABLESPACE latch memo 类型。
