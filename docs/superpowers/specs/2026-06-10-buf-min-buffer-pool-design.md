# Spec：buf 最小 Buffer Pool（首版切片）

- 日期：2026-06-10
- 关联设计：`C:\coding\java\self\minimysqldesign\docs\innodb-disk-manager-design.md`（§3、§4、§10、§14、§18、§20 第 3 步）
- 上游约束：本仓库 `AGENTS.md` / `CLAUDE.md`
- 依赖切片：fil 物理 IO 层（`PageStore`）已完成
- 状态：brainstorming 已评审通过，待自查 + 用户复核

## 1. 背景与目标

`fil` 物理 IO 层（`PageStore` 按 PageId 读写整页）已完成。本切片实现设计 §20 第 3 步「最小 Buffer Pool、page latch」：在 `fil` 之上提供**受控页访问**——固定（fix/pin）、S/X page latch、容量受限 + LRU 淘汰、脏页淘汰写回、newPage（新页不读盘）、flush。它是后续 `fsp` 分配层（经 buf 拿受控页）和 `flush`/`recovery` 的基座。

## 2. 范围与非目标

**做**：`getPage`/`newPage`（fix + page latch + LRU）、脏页淘汰经 `PageStore` 写回、`flush`/`flushAll`、RAII `PageGuard` 释放、页内字节/整数读写。

**不做**（注释标简化点）：
- MTR/redo 集成（`getPage` 暂不带 mtr 参数）。
- WAL 门控（脏页 flush 不校验对应 redo 已 durable——redo 切片未到）。
- doublewrite（flush 模块切片）。
- PageCursor 完整类型化页 + `pageType()`/`pageLsn()`（属 record/redo 语义）。
- 预读、异步 IO、多 buffer pool 实例分片。
- buf 不懂 segment/extent/record/SQL 语义（§10/AGENTS.md）。

## 3. 包与依赖方向

- 新增类位于 `cn.zhangyis.db.storage.buf`。
- 依赖：`buf → fil(PageStore)`、`buf → domain`、`buf → common.exception`、`java.util.concurrent.locks`。**禁止** import `fsp`/`btree`/`record`/`trx`/`sql`/`session`/`dd`（§15）。
- 前置：被访问的表空间须已在 `PageStore` 中 create/open（编排层职责，不在 buf）。
- 日志用 Lombok `@Slf4j`。

## 4. 组件设计

### 4.1 `PageLatchMode`（枚举）

`SHARED`、`EXCLUSIVE`。页内容访问模式。

### 4.2 `BufferFrame`（包内 final 类）

一帧驻留页。字段及其并发归属（AGENTS.md 要求逐字段写明 owner/保护锁）：

- `PageId pageId`：当前驻留页；null 表示空闲帧。**由 poolLock 保护**。
- `final byte[] data` / `final ByteBuffer buffer`：pageSize 字节内容，帧创建时分配一次、跨驻留复用。`buffer = ByteBuffer.wrap(data)`（BIG_ENDIAN）。**内容由 page latch(S/X) 保护**；仅在帧 fixCount==0（无活跃 guard）时由 pool 在 poolLock 下读/写（淘汰、读入、写回）。
- `boolean dirty`：是否含未落盘修改。**由 poolLock 保护**：在 `PageGuard.close()` 内按 guard 是否写过 OR 置位；在 flush/淘汰写回后清零；在淘汰判断时读取。
- `int fixCount`：固定计数，>0 不可被淘汰。**由 poolLock 保护**。
- `final ReentrantReadWriteLock pageLatch`：页内容 S/X 闩，协调同一驻留页的并发 fixer（多读并发、读写互斥）。淘汰/flush 不取此闩（只作用于 fixCount==0 的帧）。

### 4.3 `PageGuard`（public final，`AutoCloseable`）

`getPage`/`newPage` 返回的 RAII 访问对象，持 fix + 一把 page latch（read 或 write lock）。

- 字段：所属 pool 回调、`BufferFrame frame`、`PageLatchMode mode`、`Lock heldLatch`、`boolean wrote`、`boolean closed`。
- 方法：
  - `PageId pageId()`
  - `int readInt(int offset)` / `byte[] readBytes(int offset, int length)`：S 或 X 均可。
  - `void writeInt(int offset, int value)` / `void writeBytes(int offset, byte[] src)`：**要求 mode==EXCLUSIVE，否则抛 DatabaseValidationException**；置 `wrote=true`；用 ByteBuffer 绝对方法（`putInt(offset,..)`/`put(offset,src,..)`）写入，不动 position。
  - `void markDirty()`：要求 EXCLUSIVE；置 `wrote=true`。
  - 所有读写先校验 `offset>=0 && offset+length<=pageSize`，越界抛 DatabaseValidationException（不暴露裸 IndexOutOfBounds）。
  - `close()`：幂等。顺序——**先释放 page latch**（`heldLatch.unlock()`），**再在 poolLock 下** `frame.dirty |= wrote; frame.fixCount--`。先释放闩再 unfix，保证在 fixCount 仍 >0 时不会被淘汰、释放闩后才允许淘汰，避免“持闩期间被淘汰重指”。
- 用 try-with-resources 使用。

### 4.4 `ReplacementPolicy`（接口，Strategy）+ `LruReplacementPolicy`

- 接口（全部在 poolLock 下被 pool 调用，实现无需自身线程安全）：
  - `void onAccess(BufferFrame frame)`：命中/固定时记录使用。
  - `void onInsert(BufferFrame frame)`：帧成为驻留。
  - `void onRemove(BufferFrame frame)`：帧被淘汰。
  - `Iterable<BufferFrame> victimOrder()`：按淘汰优先序（LRU 在前）遍历驻留帧。
- `LruReplacementPolicy`：用访问序结构（如 `LinkedHashSet<BufferFrame>`，onAccess 先 remove 再 add 移到 MRU 尾；victimOrder 从头部 LRU 起）。

### 4.5 `BufferPool`（接口）+ `LruBufferPool`（实现）

```
public interface BufferPool extends AutoCloseable {
    PageGuard getPage(PageId pageId, PageLatchMode mode);
    PageGuard newPage(PageId pageId, PageLatchMode mode);
    void flush(PageId pageId);
    void flushAll();
    int capacity();
    int residentCount();
    @Override void close();
}
```

`LruBufferPool(PageStore pageStore, PageSize pageSize, int capacity, ReplacementPolicy policy)`：
- 校验 capacity>=1、各参数非空。预分配 `capacity` 个 `BufferFrame`（各持一份 pageSize byte[]），全部入空闲列表；`residentMap`(HashMap) 空。
- 单 `ReentrantLock poolLock` 保护 residentMap、空闲列表、policy、各帧 pageId/dirty/fixCount。

### 4.6 `BufferPoolExhaustedException extends DatabaseRuntimeException`

需要新帧但所有帧都被 fix（无可淘汰受害者）时抛出。可恢复：调用方释放 guard 后可重试。

## 5. 数据流（核心）

### 5.1 getPage / newPage 公共结构（含失败处理）

两者共用骨架「poolLock 内取得 target 帧 → 释放 poolLock → 取 page latch → 返回 guard」，差别只在未命中时载入方式（getPage 读盘 / newPage 清零）：

```
poolLock.lock();
try {
    BufferFrame target = residentMap.get(pageId);
    if (target != null) {                    // 命中
        // newPage 命中已驻留页是逻辑错误 → 抛 DatabaseValidationException（应改用 getPage）
        target.fixCount++;
        policy.onAccess(target);
    } else {                                  // 未命中
        BufferFrame victim = takeFreeFrame();            // 空闲列表取一帧（pageId==null）
        if (victim == null) victim = evictVictim();      // 遍历 victimOrder() 取首个 fixCount==0；无则抛 BufferPoolExhaustedException
        // evictVictim：若 victim 仍驻留且 dirty → writePage 写回（失败直接上抛，此时 victim 仍在 map、状态一致，无需回滚）；
        //              写回成功后 residentMap.remove(victim.pageId) + policy.onRemove(victim)，使 victim 脱离 map。
        try {
            if (isGetPage) PageStore.readPage(pageId, ByteBuffer.wrap(victim.data));
            else           java.util.Arrays.fill(victim.data, (byte) 0);   // newPage：不读盘
        } catch (RuntimeException loadError) {
            // 载入失败：victim 已脱离 map，回收为干净空闲帧，避免泄漏帧
            victim.pageId = null; victim.dirty = false; freeList.add(victim);
            throw loadError;
        }
        victim.pageId = pageId; victim.dirty = false; victim.fixCount = 1;
        residentMap.put(pageId, victim); policy.onInsert(victim);
        target = victim;
    }
    chosen = target;                          // 经局部变量带出 finally
} finally {
    poolLock.unlock();                        // poolLock 一定释放
}
Lock latch = (mode == EXCLUSIVE) ? chosen.pageLatch.writeLock() : chosen.pageLatch.readLock();
latch.lock();                                 // 在 poolLock 之外获取 page latch（不嵌套）
return new PageGuard(this, chosen, mode, latch);
```

要点：
- poolLock 用 try/finally 保证释放；page latch 在 poolLock 之外获取，永不嵌套于 poolLock。
- **载入失败**把已脱离 map 的 victim 回收到空闲列表，不泄漏帧、不泄漏锁。
- **写回失败**发生在 victim 脱离 map 之前，victim 维持驻留+脏、状态一致，直接上抛。
- newPage 要求 pageId 未驻留（命中则抛 DatabaseValidationException）；其页须已被 `PageStore.extend` 在盘上分配/零填充（fsp 调用前保证）。
- IO 路径用 `ByteBuffer.wrap(victim.data)`（fresh，position 0，remaining=pageSize）读写整页；与 PageGuard 的帧级绝对访问 buffer 共用同一底层数组，因绝对 get/put 不动 position 而一致。

### 5.2 flush(pageId) / flushAll()
见 §5.3。

### 5.3 flush(pageId) / flushAll()
- flush(pageId)：poolLock 下，`frame=residentMap.get(pageId)`；若存在、`fixCount==0` 且 `dirty`：`PageStore.writePage`，清 `dirty`。**仅刷未 fix 的帧**——fixCount==0 ⟹ 无活跃 guard ⟹ 无并发写者 ⟹ poolLock 下读 data 一致。fixed 帧跳过（在其释放后由后续 flush 落盘；最小版可接受，真正 checkpoint 协调属后续切片）。
- flushAll()：poolLock 下快照驻留 pageId 列表，逐个 flush（或直接 poolLock 内对所有 `fixCount==0 && dirty` 帧写回）。

### 5.4 close()
`flushAll()` 后释放（最小版假设无活跃 guard）。

## 6. 并发与锁模型（关键，自查重点）

- **`poolLock`（ReentrantLock）**：保护帧表/空闲列表/policy/帧 pageId·dirty·fixCount；首版 miss/evict/flush 的盘 IO **在 poolLock 内串行**（简化点：后续引入 per-frame loading 状态把 IO 移出池锁）。
- **page latch（每帧 S/X）**：只协调同一驻留页活跃 fixer 的内容并发；**在释放 poolLock 之后获取**，绝不嵌套于 poolLock 之内。
- **无 poolLock↔page latch 嵌套**，故无死锁：getPage 命中/未命中都在释放 poolLock 后再取 page latch；PageGuard.close() 先放 page latch、再取 poolLock 改 dirty/fixCount。淘汰/flush 只作用于 `fixCount==0` 的帧，从不取 page latch。
- **dirty 一致性**：fixCount==0 ⟺ 无活跃 PageGuard ⟺ 上一个写者的 close() 已在 poolLock 下 OR 置 dirty 并 unfix；故 pool 在 poolLock 下读取淘汰/flush 候选的 dirty 与 data 都已稳定（写者已完成、闩已释放），无 torn flush。
- **data 可见性**：载入在 poolLock 内写 data；其它线程之后命中时在 poolLock 下 fix（与载入者的 poolLock 释放建立 happens-before），随后取 page latch 读 data，可见。
- **fixCount>0 不可淘汰**：保证使用中的页不被换出/重指。
- 锁层级落在设计 §18 的最底层（data page latch / 帧级），不与文件锁、事务锁混用。
- **page latch 阻塞获取**：`getPage` 取 page latch 为阻塞获取，符合「明确的唤醒条件」——持有者随其 PageGuard close() 必然释放（guard 生命周期有界）。需为异常长持有设上界时，后续可加 `tryAcquire(timeout)` 变体；首版阻塞足够。
- **victimOrder 迭代**：pool 遍历 `victimOrder()` 找受害者时**找到即 break**，迭代结束后才调 `policy.onRemove`，避免在 for-each 中改 LRU 结构触发 ConcurrentModificationException。
- **PageGuard 非线程安全**：仅由调用 getPage/newPage 的线程使用与 close（page latch 的 lock/unlock 须同线程）。

## 7. 异常（沿用项目异常层次，禁裸 RuntimeException/IllegalArgumentException）

- `BufferPoolExhaustedException`（Runtime）：无可淘汰帧。
- `DatabaseValidationException`：参数非空、capacity>=1、字节越界、S 模式写、newPage 命中已驻留页。
- `PageStore` 抛出的物理异常（`DataFilePhysicalException` 等）透传。

## 8. 测试（JUnit，用 `FileChannelPageStore` + @TempDir 真实文件驱动）

- 帧往返：create/open 表空间后 `getPage(X)` writeInt/writeBytes → close → `getPage(S)` 命中读回一致。
- miss 读穿：先经 buf 写一页并 flush（或 PageStore 直接写），淘汰后再 `getPage` 从盘读回。
- LRU 淘汰：capacity=2，访问 3 个不同页，最久未用被淘汰；再访问被淘汰页触发 re-read。
- 脏页淘汰写回：写一页（dirty）→ 触发淘汰 → 用另一个 PageStore.readPage 或新 BufferPool 验证字节已落盘。
- fix 全满：固定满 capacity 个帧不释放，再 getPage 新页 → `BufferPoolExhaustedException`。
- newPage：`newPage(X)` 不读盘得零页 → 写入 → flush → 验证落盘；对已驻留页 newPage → DatabaseValidationException。
- flush/flushAll：脏页 flush 后 dirty 清零且盘上可见。
- S 模式写：`getPage(S)` 上 writeInt/writeBytes/markDirty → DatabaseValidationException。
- 越界：offset+len 超 pageSize → DatabaseValidationException。
- fixCount 嵌套：同页 getPage 两次（fixCount 2），close 一次仍驻留/不可被淘汰，close 第二次后可淘汰。
- page latch 并发（集成）：两线程 `getPage(P,S)` 同时持有读不互斥；`getPage(P,X)` 与持有者互斥。

## 9. 简化点（注释标注）

- 单 poolLock 串行 miss/evict/flush 的盘 IO；无 per-frame loading 状态。
- 不带 MTR；flush 不做 WAL 门控、不做 doublewrite。
- PageGuard 只暴露 readInt/writeInt/readBytes/writeBytes/markDirty，无 pageType/pageLsn。
- flush 只刷未 fix 帧；fixed 脏页延后落盘。
- 不引入 BufferFrameState 枚举：用 dirty 标志 + 是否在 residentMap 表达 free/clean/dirty；READING/IO_PENDING 等状态待 IO 移出池锁后再加。

## 10. 自查修正记录

1. **补全 getPage/newPage 失败处理（重要）**：原 §5 未写盘 IO 失败路径。`PageStore.readPage`/`writePage` 可抛 `TablespaceNotOpenException`/`DataFilePhysicalException`，若发生在未命中临界区会泄漏 victim 帧与 poolLock。现明确：poolLock 用 try/finally 释放；载入失败把已脱离 map 的 victim 回收为干净空闲帧；写回失败发生在脱离 map 之前、状态一致直接上抛。
2. **ByteBuffer 使用澄清**：IO 路径用 `ByteBuffer.wrap(data)`（fresh，position 0），PageGuard 用帧级绝对 get/put（不动 position），共用同一底层数组而一致；避免 position 串扰。
3. **page latch 阻塞获取的唤醒条件**写明（持有者 guard close 必然释放），timed 变体留后续。
4. **victimOrder 迭代不可中途改 LRU**（找到 break 后再 onRemove），防 CME。
5. **PageGuard 单线程**约束写明。

## 11. 后续切片衔接

- `mtr` 给 `getPage` 增加 mtr 参数，把 page latch、buffer fix 放入 memo，commit 时统一释放。
- `fsp` 经 `getPage(X)`/`newPage(X)` 读写 FSP_HDR/XDES/INODE/数据页，分配后 markDirty。
- `flush`/checkpoint 切片接管 WAL 门控、doublewrite、后台刷脏，并把 IO 移出 poolLock；引入 pageLSN。
