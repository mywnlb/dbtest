package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessLease;
import cn.zhangyis.db.domain.SpaceId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

/**
 * mini-transaction memo 栈：LIFO 持有短临界区资源（page latch + buffer fix 等，均表现为 AutoCloseable），
 * commit/rollback/savepoint 时按后进先出释放。仅由属主线程访问，无需自身加锁（设计 §9.2）。
 *
 * <p>每个槽位记为 {@link MemoEntry}：除待释放的 {@link AutoCloseable} 资源外，对 page guard 还额外记录其
 * {@link PageId} 与 {@link PageLatchMode}，使 {@link MiniTransaction#getPage} 能在取 latch 前判断本 MTR 是否
 * 已持有同页的某种 latch，从而拒绝 ReentrantReadWriteLock 不支持的 S→X 升级（否则会自死锁）。非 page 资源
 * 的 pageId/mode 为 null，不参与 {@link #holds} 匹配。
 */
final class MtrMemo {

    /**
     * memo 槽位：待释放资源 + 可选的 page latch 归属信息。
     *
     * @param resource 后进先出释放的资源（page guard 或其它 AutoCloseable）。
     * @param pageId   page guard 所在页；非 page 资源为 null。
     * @param mode     page guard 持有的 latch 模式；非 page 资源为 null。
     * @param leaseSpaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     */
    private record MemoEntry(AutoCloseable resource, PageId pageId, PageLatchMode mode, SpaceId leaseSpaceId) {
    }

    /** 资源栈；push/pop 头部，构成 LIFO。 */
    private final Deque<MemoEntry> stack = new ArrayDeque<>();

    /**
     * 压入一个非 page 的待释放资源（如 buffer fix、reservation 等），无 page latch 归属信息。
     *
     * @param resource 非空资源。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void push(AutoCloseable resource) {
        if (resource == null) {
            throw new DatabaseValidationException("memo resource must not be null");
        }
        stack.push(new MemoEntry(resource, null, null, null));
    }

    /**
     * 压入一个 page guard，并记录其页与 latch 模式，供 {@link #holds} 检测同页 latch 归属。
     *
     * @param guard  非空 page guard（释放时按 LIFO close）。
     * @param pageId 非空，guard 所在页。
     * @param mode   非空，guard 持有的 latch 模式（S 或 X）。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void pushPageGuard(PageGuard guard, PageId pageId, PageLatchMode mode) {
        if (guard == null) {
            throw new DatabaseValidationException("memo page guard must not be null");
        }
        if (pageId == null) {
            throw new DatabaseValidationException("memo page id must not be null");
        }
        if (mode == null) {
            throw new DatabaseValidationException("memo page latch mode must not be null");
        }
        stack.push(new MemoEntry(guard, pageId, mode, null));
    }

    /**
     * 压入表空间共享 lease。lease 在首次 page guard 之前入栈，因而 LIFO 释放时晚于该空间全部 latch/fix，
     * 保证 truncate 看见共享 lease drain 后不存在遗留页句柄。
     *
     * @param lease 调用方持有的 {@code TablespaceAccessLease} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void pushTablespaceLease(TablespaceAccessLease lease, SpaceId spaceId) {
        if (lease == null || spaceId == null) {
            throw new DatabaseValidationException("tablespace memo lease/space id must not be null");
        }
        stack.push(new MemoEntry(lease, null, null, spaceId));
    }

    /** 同一 MTR 每个表空间只持一个共享 lease，避免重复读锁计数与释放顺序复杂化。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return {@code hasTablespaceLease} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    boolean hasTablespaceLease(SpaceId spaceId) {
        for (MemoEntry entry : stack) {
            if (spaceId.equals(entry.leaseSpaceId())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 本 MTR 当前是否仍持有指定页的指定模式 latch。只匹配仍在栈中的槽位：被 savepoint 或 commit 释放（pop）的
     * guard 自然不再命中，因此释放后再取相反模式不会被误判为升级。
     *
     * @param pageId 目标页（null 不匹配任何槽位）。
     * @param mode   目标 latch 模式。
     * @return 栈中存在 pageId 与 mode 均相等的 page guard 槽位时返回 true。
     */
    boolean holds(PageId pageId, PageLatchMode mode) {
        for (MemoEntry entry : stack) {
            if (entry.pageId() != null && entry.pageId().equals(pageId) && entry.mode() == mode) {
                return true;
            }
        }
        return false;
    }

    /** 当前 MTR 是否仍持有该页的任意 page latch。用于同页重入豁免 page latch 全序检查。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code holdsAnyPageLatch} 成功完成其命名的受控动作并发布结果时为 {@code true}；未命中、未执行或状态竞争失败时为 {@code false}
     */
    boolean holdsAnyPageLatch(PageId pageId) {
        for (MemoEntry entry : stack) {
            if (entry.pageId() != null && entry.pageId().equals(pageId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 返回当前 memo 中仍持有的最大 PageId（按 spaceId、pageNo 升序比较）。非 page 资源不参与。
     *
     * <p>MTR 层用它在获取新 page latch 前执行“独立多页默认升序”守卫；被 releaseLatch/savepoint 移除的页已不在
     * stack 中，因此不会误拦释放后的重新导航。
     *
     * @return {@code highestHeldPageId} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    PageId highestHeldPageId() {
        PageId highest = null;
        for (MemoEntry entry : stack) {
            if (entry.pageId() == null) {
                continue;
            }
            if (highest == null || comparePageId(entry.pageId(), highest) > 0) {
                highest = entry.pageId();
            }
        }
        return highest;
    }

    /**
     * 指定 guard 在 memo 中是否以 EXCLUSIVE 模式持有。供 {@link MiniTransaction#releaseLatch} 判定提前释放是否会
     * 移除某 touched 页的 pageLSN 盖戳所依赖的 X guard：释放 SHARED guard 永不影响盖戳（touched 页必由某 X guard 写过、
     * 该 X guard 仍在栈中），只有释放 X guard 才危险。按身份匹配；未找到（不该发生）按非 X 处理，交 {@link #release} 抛未找到。
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @return {@code isExclusiveGuard} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    boolean isExclusiveGuard(PageGuard guard) {
        for (MemoEntry entry : stack) {
            if (entry.resource() == guard) {
                return entry.mode() == PageLatchMode.EXCLUSIVE;
            }
        }
        return false;
    }

    /** 当前栈深，用作 savepoint 标记。 */
    int depth() {
        return stack.size();
    }

    /**
     * 释放到指定深度（LIFO）。当前深度 ≤ targetDepth 时 no-op；逐个 close，即便某资源 close 抛异常也继续释放其余，
     * 最后把首个异常包成 MtrStateException 抛出（其余 addSuppressed），避免泄漏 latch/fix。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param targetDepth 目标深度（非负）。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws MtrStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    void releaseTo(int targetDepth) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (targetDepth < 0) {
            throw new DatabaseValidationException("memo target depth must be non-negative: " + targetDepth);
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        Exception firstError = null;
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        int failures = 0;
        while (stack.size() > targetDepth) {
            MemoEntry entry = stack.pop();
            try {
                entry.resource().close();
            } catch (Exception e) {
                failures++;
                if (firstError == null) {
                    firstError = e;
                } else {
                    firstError.addSuppressed(e);
                }
            }
        }
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        if (firstError != null) {
            throw new MtrStateException("failed to release " + failures + " memo resource(s)", firstError);
        }
    }

    /** 释放全部资源（LIFO）。 */
    void releaseAll() {
        releaseTo(0);
    }

    /**
     * 选择性（非 LIFO）释放单个资源：按**身份**在栈中定位持有该 {@code resource} 的槽位，移除并 {@code close}
     * （放 page latch + buffer fix）。供 B+Tree 写路径 latch coupling（crab，设计 §10.2）提前放掉已越过的内部页
     * latch——它未必在栈顶，故不能用 LIFO {@link #releaseTo}。其余槽位次序不变，后续 commit/rollback 仍按 LIFO 释放。
     *
     * <p>身份比较（{@code ==}）而非 equals：同一 guard 实例唯一标识一次 fix，避免相等但不同的资源被误摘。
     * 未在栈中找到即视为不变量破坏（调用方只应释放本 MTR 仍持有的资源），抛 {@link MtrStateException}；
     * close 失败同样包成 {@link MtrStateException}（资源已移出栈，不会二次释放）。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param resource 要提前释放的资源（通常是内部导航页的 page guard），非空。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws MtrStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    void release(AutoCloseable resource) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (resource == null) {
            throw new DatabaseValidationException("memo release resource must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        Iterator<MemoEntry> it = stack.iterator();
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        while (it.hasNext()) {
            MemoEntry entry = it.next();
            if (entry.resource() == resource) {
                it.remove();
                try {
                    entry.resource().close();
                } catch (Exception e) {
                    throw new MtrStateException("failed to release memo resource for page " + entry.pageId(), e);
                }
                return;
            }
        }
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        throw new MtrStateException("resource not held by this mini transaction memo");
    }

    /**
     * 返回指向 {@code pageId} 的最近一次 X latch 的 page guard；无则视为不变量破坏抛 {@link MtrStateException}。
     * 供 commit 给 touched 页盖 pageLSN（touched 页必有对应 X guard，见 MiniTransaction.rollbackToSavepoint 的保护）。
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code guardFor} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws MtrStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    PageGuard guardFor(PageId pageId) {
        for (MemoEntry entry : stack) {
            if (entry.pageId() != null && entry.pageId().equals(pageId)
                    && entry.mode() == PageLatchMode.EXCLUSIVE) {
                return (PageGuard) entry.resource();
            }
        }
        throw new MtrStateException("no X page guard in memo for page " + pageId);
    }

    /** 返回深度 &gt; targetDepth（即将被 releaseTo 释放）的各槽位 pageId（非 page 资源跳过）。供 savepoint 回滚前检查 touched。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param targetDepth 参与 {@code pageIdsAbove} 的树层级或递归深度 {@code targetDepth}；必须非负且不得超过当前页结构、MTR memo 或解析器声明的最大深度
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     */
    List<PageId> pageIdsAbove(int targetDepth) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        List<PageId> result = new ArrayList<>();
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        int aboveCount = stack.size() - targetDepth;
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        int i = 0;
        for (MemoEntry entry : stack) { // ArrayDeque 迭代 head→tail = 新→旧，头部即栈顶
            if (i++ >= aboveCount) {
                break;
            }
            if (entry.pageId() != null) {
                result.add(entry.pageId());
            }
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return result;
    }

    /** PageId 全序：先表空间，后页号。
     *
     * @param left 参与 {@code comparePageId} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param right 参与 {@code comparePageId} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return 左值小于、等于或大于右值时分别返回负数、零或正数；排序规则与对应索引或无符号格式一致
     */
    static int comparePageId(PageId left, PageId right) {
        int bySpace = Integer.compare(left.spaceId().value(), right.spaceId().value());
        if (bySpace != 0) {
            return bySpace;
        }
        return Long.compare(left.pageNo().value(), right.pageNo().value());
    }
}
