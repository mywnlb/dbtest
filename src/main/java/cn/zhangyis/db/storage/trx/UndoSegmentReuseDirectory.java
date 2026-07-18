package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.undo.CachedUndoSegmentRef;
import cn.zhangyis.db.storage.undo.FreeUndoSegmentRef;
import cn.zhangyis.db.storage.undo.UndoLogKind;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * rollback segment 可复用 undo owner 的统一运行期投影。INSERT/UPDATE cache 是有界 LIFO，free list 是跨 kind FIFO；
 * page3 与 FREE 首页链才是 crash 后权威，本类只用 lease 保护“内存预留—物理 MTR—内存发布”窗口。
 *
 * <p><b>并发边界</b>：短锁只保护三个容器、三个 transition fence 和全局 drain gate，锁内不访问 Buffer Pool、
 * FSP、redo 或文件。物理写开始后的异常保留 fence，要求实例 fail-stop，禁止内存投影继续覆盖未知磁盘结果。
 */
public final class UndoSegmentReuseDirectory {

    /** 每个 kind 的持久 cache 容量；free FIFO 不设配置上限。 */
    private final int capacityPerKind;
    /** INSERT/UPDATE cache，顺序均为栈底到栈顶。 */
    private final List<CachedUndoSegmentRef> insertStack = new ArrayList<>();
    /** UPDATE cache 的运行期投影，顺序为栈底到栈顶。 */
    private final List<CachedUndoSegmentRef> updateStack = new ArrayList<>();
    /** free FIFO，迭代顺序为 head 到 tail。 */
    private final ArrayDeque<FreeUndoSegmentRef> freeQueue = new ArrayDeque<>();
    /** 保护全部运行期投影状态的短锁。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** 各 owner 集合是否已有 lease 跨出短锁执行物理 MTR。 */
    private boolean insertTransition;
    /** UPDATE cache 是否已有 lease 跨出短锁执行物理 MTR。 */
    private boolean updateTransition;
    /** free FIFO 是否已有 lease 跨出短锁执行物理 MTR。 */
    private boolean freeTransition;
    /** truncate 是否独占三个 owner 集合。 */
    private boolean draining;

    /**
     * 创建空运行期目录。cache 容量必须和 page3 format/read 使用的容量一致；free FIFO 没有配置容量，
     * 仅受 Java 集合的 {@link Integer#MAX_VALUE} 上限约束。
     *
     * @param capacityPerKind INSERT 与 UPDATE 各自可持有的最大 cache owner 数，允许为零。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoSegmentReuseDirectory(int capacityPerKind) {
        if (capacityPerKind < 0) {
            throw new DatabaseValidationException("undo cache capacity must not be negative: " + capacityPerKind);
        }
        this.capacityPerKind = capacityPerKind;
    }

    /** 返回稳定 cache top 证据；transition/drain 中不暴露候选。
     *
     * @param kind 选择 {@code peekCache} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code peekCache} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<CacheCandidate> peekCache(UndoLogKind kind) {
        requireCacheKind(kind);
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(kind);
            if (draining || cacheTransition(kind) || stack.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new CacheCandidate(stack.getLast(), stack.size()));
        } finally {
            lock.unlock();
        }
    }

    /** 返回稳定 free head、可选 successor、tail 与 length，供 page3/head-page CAS 规划。
     *
     * @return {@code peekFree} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<FreeCandidate> peekFree() {
        lock.lock();
        try {
            if (draining || freeTransition || freeQueue.isEmpty()) {
                return Optional.empty();
            }
            FreeUndoSegmentRef head = freeQueue.getFirst();
            FreeUndoSegmentRef successor = freeQueue.size() > 1
                    ? freeQueue.stream().skip(1).findFirst().orElseThrow() : null;
            return Optional.of(new FreeCandidate(head, Optional.ofNullable(successor),
                    freeQueue.getLast(), freeQueue.size()));
        } finally {
            lock.unlock();
        }
    }

    /** 为 cache top 复用建立非阻塞 transition fence。
     *
     * @param expected 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @return {@code reserveCachePop} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoWriteStalePlanException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    CachePopLease reserveCachePop(CacheCandidate expected) {
        if (expected == null) {
            throw new DatabaseValidationException("undo cache pop candidate must not be null");
        }
        UndoLogKind kind = expected.segment().kind();
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(kind);
            if (draining || cacheTransition(kind) || stack.size() != expected.expectedCount()
                    || stack.isEmpty() || !stack.getLast().equals(expected.segment())) {
                throw new UndoWriteStalePlanException("cached undo top changed before reuse: kind=" + kind);
            }
            setCacheTransition(kind, true);
            return new CachePopLease(this, expected);
        } finally {
            lock.unlock();
        }
    }

    /** 为 free head 复用建立非阻塞 transition fence。
     *
     * @param expected 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @return {@code reserveFreePop} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoWriteStalePlanException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    FreePopLease reserveFreePop(FreeCandidate expected) {
        if (expected == null) {
            throw new DatabaseValidationException("undo free pop candidate must not be null");
        }
        lock.lock();
        try {
            if (draining || freeTransition || !matchesFreeCandidate(expected)) {
                throw new UndoWriteStalePlanException("free undo head changed before reuse");
            }
            freeTransition = true;
            return new FreePopLease(this, expected);
        } finally {
            lock.unlock();
        }
    }

    /** cache 有容量且 owner 未重复时预留一次 push；忙或满时不等待。
     *
     * @param segment 参与 {@code tryReserveCachePush} 的稳定领域标识 {@code CachedUndoSegmentRef}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return 成功取得的资源、槽位或预留；资源不存在或竞争失败时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    Optional<CachePushLease> tryReserveCachePush(CachedUndoSegmentRef segment) {
        if (segment == null) {
            throw new DatabaseValidationException("cached undo push segment must not be null");
        }
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(segment.kind());
            if (draining || cacheTransition(segment.kind()) || stack.size() >= capacityPerKind) {
                return Optional.empty();
            }
            requireUniqueNewPages(List.of(segment.handle().firstPageId()));
            setCacheTransition(segment.kind(), true);
            return Optional.of(new CachePushLease(this, segment, stack.size()));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 预留一批一至两个 free tail push。free 不设配置上限，但运行期集合不能超过 Integer.MAX_VALUE；超限返回 empty，
     * finalizer 据此选择物理 drop。
     *
     * @param segments 参与 {@code tryReserveFreePush} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return 成功取得的资源、槽位或预留；资源不存在或竞争失败时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    Optional<FreePushLease> tryReserveFreePush(List<FreeUndoSegmentRef> segments) {
        if (segments == null || segments.isEmpty() || segments.size() > 2
                || segments.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("free undo push must contain one or two segments");
        }
        lock.lock();
        try {
            if (draining || freeTransition || (long) freeQueue.size() + segments.size() > Integer.MAX_VALUE) {
                return Optional.empty();
            }
            requireUniqueNewPages(segments.stream().map(item -> item.handle().firstPageId()).toList());
            freeTransition = true;
            return Optional.of(new FreePushLease(this, segments, freeQueue.size(),
                    Optional.ofNullable(freeQueue.peekLast())));
        } finally {
            lock.unlock();
        }
    }

    /** truncate 使用的非阻塞全局 gate；任何普通 owner transition 忙时立即返回 empty。
     *
     * @return {@code tryBeginDrain} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<DrainLease> tryBeginDrain() {
        lock.lock();
        try {
            if (draining || insertTransition || updateTransition || freeTransition) {
                return Optional.empty();
            }
            draining = true;
            return Optional.of(new DrainLease(this));
        } finally {
            lock.unlock();
        }
    }

    /** recovery 完成全部 page3/首页/FSP 校验后一次安装三个持久 owner 集合。
     *
     * @param insert 参与 {@code restore} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param update 参与 {@code restore} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param free 参与 {@code restore} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void restore(List<CachedUndoSegmentRef> insert, List<CachedUndoSegmentRef> update,
                        List<FreeUndoSegmentRef> free) {
        if (insert == null || update == null || free == null) {
            throw new DatabaseValidationException("recovered undo reuse collections must not be null");
        }
        lock.lock();
        try {
            if (draining || insertTransition || updateTransition || freeTransition
                    || !insertStack.isEmpty() || !updateStack.isEmpty() || !freeQueue.isEmpty()) {
                throw new DatabaseValidationException("undo reuse restore requires an empty idle directory");
            }
            validateRecoveredStack(insert, UndoLogKind.INSERT);
            validateRecoveredStack(update, UndoLogKind.UPDATE);
            if (free.size() > Integer.MAX_VALUE || free.stream().anyMatch(java.util.Objects::isNull)) {
                throw new DatabaseValidationException("recovered undo free list is invalid");
            }
            Set<PageId> pages = new HashSet<>();
            allPages(insert, update, free).forEach(page -> {
                if (!pages.add(page)) {
                    throw new DatabaseValidationException("duplicate recovered reusable undo first page: " + page);
                }
            });
            insertStack.addAll(insert);
            updateStack.addAll(update);
            freeQueue.addAll(free);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 计算 {@code cachedCount} 所表达的事务、MVCC 与锁数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @param kind 选择 {@code cachedCount} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code cachedCount} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    public int cachedCount(UndoLogKind kind) {
        requireCacheKind(kind);
        lock.lock();
        try {
            return stack(kind).size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 计算 {@code freeCount} 所表达的事务、MVCC 与锁数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @return {@code freeCount} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    public int freeCount() {
        lock.lock();
        try {
            return freeQueue.size();
        } finally {
            lock.unlock();
        }
    }

    public int capacityPerKind() {
        return capacityPerKind;
    }

    /** 复制三个 owner 集合；cache 为 bottom→top，free 为 head→tail。
     *
     * @return {@code snapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public ReuseSnapshot snapshot() {
        lock.lock();
        try {
            return new ReuseSnapshot(insertStack, updateStack, List.copyOf(freeQueue));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 执行事务、MVCC 与锁恢复或重放步骤；按持久证据校验并幂等推进状态，不执行普通 SQL 业务语义。
     *
     * @param recovered 参与 {@code validateRecoveredStack} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param expectedKind 选择 {@code validateRecoveredStack} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private void validateRecoveredStack(List<CachedUndoSegmentRef> recovered, UndoLogKind expectedKind) {
        if (recovered.size() > capacityPerKind || recovered.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("recovered undo cache exceeds configured capacity for " + expectedKind);
        }
        for (CachedUndoSegmentRef ref : recovered) {
            if (ref.kind() != expectedKind) {
                throw new DatabaseValidationException("recovered undo cache kind mismatch: expected="
                        + expectedKind + ", current=" + ref.kind());
            }
        }
    }

    /**
     * 校验 {@code requireUniqueNewPages} 涉及的事务、MVCC 与锁结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param candidates 参与 {@code requireUniqueNewPages} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private void requireUniqueNewPages(List<PageId> candidates) {
        Set<PageId> existing = new HashSet<>(allPages(insertStack, updateStack, List.copyOf(freeQueue)));
        for (PageId page : candidates) {
            if (!existing.add(page)) {
                throw new DatabaseValidationException("undo segment already exists in reuse directory: " + page);
            }
        }
    }

    /**
     * 释放本方法拥有的事务、MVCC 与锁资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * @param candidate 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @return {@code matchesFreeCandidate} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    private boolean matchesFreeCandidate(FreeCandidate candidate) {
        if (freeQueue.size() != candidate.expectedCount() || freeQueue.isEmpty()
                || !freeQueue.getFirst().equals(candidate.segment())
                || !freeQueue.getLast().equals(candidate.expectedTail())) {
            return false;
        }
        Optional<FreeUndoSegmentRef> currentSuccessor = freeQueue.size() > 1
                ? freeQueue.stream().skip(1).findFirst() : Optional.empty();
        return currentSuccessor.equals(candidate.successor());
    }

    private List<CachedUndoSegmentRef> stack(UndoLogKind kind) {
        return switch (kind) {
            case INSERT -> insertStack;
            case UPDATE -> updateStack;
            case TEMPORARY -> throw new DatabaseValidationException("temporary undo has no cache stack");
        };
    }

    private boolean cacheTransition(UndoLogKind kind) {
        return kind == UndoLogKind.INSERT ? insertTransition : updateTransition;
    }

    /**
     * 校验当前状态后推进事务、MVCC 与锁状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * @param kind 选择 {@code setCacheTransition} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param value 缓存迁移步骤是否已经完成；该事实决定失败补偿能否删除或恢复目录项，不能与物理状态相矛盾
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private void setCacheTransition(UndoLogKind kind, boolean value) {
        if (kind == UndoLogKind.INSERT) {
            insertTransition = value;
        } else if (kind == UndoLogKind.UPDATE) {
            updateTransition = value;
        } else {
            throw new DatabaseValidationException("temporary undo has no cache transition");
        }
    }

    private static void requireCacheKind(UndoLogKind kind) {
        if (kind == null || kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("ordinary undo cache kind must be INSERT or UPDATE");
        }
    }

    /**
     * 根据调用参数创建或转换 {@code allPages} 返回的 {@code List<PageId>}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param insert 参与 {@code allPages} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param update 参与 {@code allPages} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param free 参与 {@code allPages} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     */
    private static List<PageId> allPages(List<CachedUndoSegmentRef> insert,
                                         List<CachedUndoSegmentRef> update,
                                         List<FreeUndoSegmentRef> free) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        List<PageId> pages = new ArrayList<>(insert.size() + update.size() + free.size());
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        insert.forEach(item -> pages.add(item.handle().firstPageId()));
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        update.forEach(item -> pages.add(item.handle().firstPageId()));
        free.forEach(item -> pages.add(item.handle().firstPageId()));
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return pages;
    }

    /** 规划期冻结的 cache LIFO top 与持久 count。
     *
     * @param segment 参与 {@code 构造} 的稳定领域标识 {@code CachedUndoSegmentRef}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     */
    public record CacheCandidate(CachedUndoSegmentRef segment, int expectedCount) {
        public CacheCandidate {
            if (segment == null || expectedCount <= 0) {
                throw new DatabaseValidationException("invalid undo cache candidate");
            }
        }
    }

    /** 规划期冻结的 free FIFO head、successor、tail 与持久 count。
     *
     * @param segment 参与 {@code 构造} 的稳定领域标识 {@code FreeUndoSegmentRef}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param successor 可选的 {@code successor}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @param expectedTail 参与 {@code 构造} 的稳定领域标识 {@code FreeUndoSegmentRef}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     */
    public record FreeCandidate(FreeUndoSegmentRef segment, Optional<FreeUndoSegmentRef> successor,
                                FreeUndoSegmentRef expectedTail, int expectedCount) {
        public FreeCandidate {
            if (segment == null || successor == null || expectedTail == null || expectedCount <= 0
                    || (expectedCount == 1) != successor.isEmpty()) {
                throw new DatabaseValidationException("invalid undo free candidate");
            }
        }
    }

    /** 统一目录快照：cache bottom→top，free head→tail。
     *
     * @param insert 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param update 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param free 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    public record ReuseSnapshot(List<CachedUndoSegmentRef> insert, List<CachedUndoSegmentRef> update,
                                List<FreeUndoSegmentRef> free) {
        public ReuseSnapshot {
            if (insert == null || update == null || free == null) {
                throw new DatabaseValidationException("undo reuse snapshot fields must not be null");
            }
            insert = List.copyOf(insert);
            update = List.copyOf(update);
            free = List.copyOf(free);
        }
    }

    /** truncate 单批：cache 按 top→bottom，free 按 head→tail。
     *
     * @param expectedInsertCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param expectedUpdateCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param expectedFreeCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param insertTopFirst 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param updateTopFirst 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param freeHeadFirst 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    public record DrainBatch(int expectedInsertCount, int expectedUpdateCount, int expectedFreeCount,
                             List<CachedUndoSegmentRef> insertTopFirst,
                             List<CachedUndoSegmentRef> updateTopFirst,
                             List<FreeUndoSegmentRef> freeHeadFirst) {
        public DrainBatch {
            if (expectedInsertCount < 0 || expectedUpdateCount < 0 || expectedFreeCount < 0
                    || insertTopFirst == null || updateTopFirst == null || freeHeadFirst == null
                    || insertTopFirst.isEmpty() && updateTopFirst.isEmpty() && freeHeadFirst.isEmpty()) {
                throw new DatabaseValidationException("invalid undo reuse drain batch");
            }
            insertTopFirst = List.copyOf(insertTopFirst);
            updateTopFirst = List.copyOf(updateTopFirst);
            freeHeadFirst = List.copyOf(freeHeadFirst);
        }

        public int size() {
            return insertTopFirst.size() + updateTopFirst.size() + freeHeadFirst.size();
        }
    }

    /** cache top pop guard。
     *
     * 事务、MVCC 与锁的 {@code CachePopLease} 生命周期守卫；创建后独占一次受控状态转换，发布、取消或关闭路径必须恰好完成一次资源收口。
     */
    static final class CachePopLease extends TransitionLease {
        /**
         * 本对象持有的 {@code candidate} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
         */
        private final CacheCandidate candidate;

        private CachePopLease(UndoSegmentReuseDirectory owner, CacheCandidate candidate) {
            super(owner);
            this.candidate = candidate;
        }

        CacheCandidate candidate() {
            return candidate;
        }

        /**
         * 校验 {@code validate} 涉及的事务、MVCC 与锁结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
         */
        @Override void validate() { owner.validateCachePop(candidate); }
        /**
         * 推进 {@code publish} 对应的事务、MVCC 与锁阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         */
        @Override void publish() { owner.completeCachePop(candidate); }
        /**
         * 推进 {@code abortBeforePhysical} 对应的事务、MVCC 与锁阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         */
        @Override void abortBeforePhysical() { owner.releaseCacheTransition(candidate.segment().kind()); }
    }

    /** finalization cache push guard。
     *
     * 事务、MVCC 与锁的 {@code CachePushLease} 生命周期守卫；创建后独占一次受控状态转换，发布、取消或关闭路径必须恰好完成一次资源收口。
     */
    static final class CachePushLease extends TransitionLease {
        /**
         * 本对象持有的 {@code segment} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
         */
        private final CachedUndoSegmentRef segment;
        /**
         * 记录 {@code expectedCount} 的非负位置、容量或计数；写入前必须校验所属页/集合上界，溢出会破坏布局或资源记账。
         */
        private final int expectedCount;

        private CachePushLease(UndoSegmentReuseDirectory owner, CachedUndoSegmentRef segment, int expectedCount) {
            super(owner);
            this.segment = segment;
            this.expectedCount = expectedCount;
        }

        CachedUndoSegmentRef segment() { return segment; }
        int expectedCount() { return expectedCount; }
        /**
         * 校验 {@code validate} 涉及的事务、MVCC 与锁结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
         */
        @Override void validate() { owner.validateCachePush(segment, expectedCount); }
        /**
         * 推进 {@code publish} 对应的事务、MVCC 与锁阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         */
        @Override void publish() { owner.completeCachePush(segment, expectedCount); }
        /**
         * 推进 {@code abortBeforePhysical} 对应的事务、MVCC 与锁阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         */
        @Override void abortBeforePhysical() { owner.releaseCacheTransition(segment.kind()); }
    }

    /** free head pop guard。
     *
     * 事务、MVCC 与锁的 {@code FreePopLease} 生命周期守卫；创建后独占一次受控状态转换，发布、取消或关闭路径必须恰好完成一次资源收口。
     */
    static final class FreePopLease extends TransitionLease {
        /**
         * 构造时冻结的 {@code candidate} 领域快照；其身份、版本与范围来自同一次权威读取，下游步骤依赖它检测并发变化和避免发布陈旧状态。
         */
        private final FreeCandidate candidate;

        private FreePopLease(UndoSegmentReuseDirectory owner, FreeCandidate candidate) {
            super(owner);
            this.candidate = candidate;
        }

        FreeCandidate candidate() { return candidate; }
        /**
         * 校验 {@code validate} 涉及的事务、MVCC 与锁结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
         */
        @Override void validate() { owner.validateFreePop(candidate); }
        /**
         * 推进 {@code publish} 对应的事务、MVCC 与锁阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         */
        @Override void publish() { owner.completeFreePop(candidate); }
        /**
         * 推进 {@code abortBeforePhysical} 对应的事务、MVCC 与锁阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         */
        @Override void abortBeforePhysical() { owner.releaseFreeTransition(); }
    }

    /** finalization free tail batch push guard。
     *
     * 事务、MVCC 与锁的 {@code FreePushLease} 生命周期守卫；创建后独占一次受控状态转换，发布、取消或关闭路径必须恰好完成一次资源收口。
     */
    static final class FreePushLease extends TransitionLease {
        /**
         * 本对象拥有的 {@code segments} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
         */
        private final List<FreeUndoSegmentRef> segments;
        /**
         * 记录 {@code expectedCount} 的非负位置、容量或计数；写入前必须校验所属页/集合上界，溢出会破坏布局或资源记账。
         */
        private final int expectedCount;
        /**
         * 本次事务链路持有的 {@code expectedTail} undo/rollback 状态；事务身份、roll pointer 与段代际必须一致，提交、回滚和 purge 路径依赖它完成收口。
         */
        private final Optional<FreeUndoSegmentRef> expectedTail;

        /**
         * 创建 {@code FreePushLease}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
         *
         * @param owner 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
         * @param segments 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
         * @param expectedCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
         * @param expectedTail 可选的 {@code expectedTail}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
         */
        private FreePushLease(UndoSegmentReuseDirectory owner, List<FreeUndoSegmentRef> segments,
                              int expectedCount, Optional<FreeUndoSegmentRef> expectedTail) {
            super(owner);
            this.segments = List.copyOf(segments);
            this.expectedCount = expectedCount;
            this.expectedTail = expectedTail;
        }

        List<FreeUndoSegmentRef> segments() { return segments; }
        int expectedCount() { return expectedCount; }
        Optional<FreeUndoSegmentRef> expectedTail() { return expectedTail; }
        /**
         * 校验 {@code validate} 涉及的事务、MVCC 与锁结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
         */
        @Override void validate() { owner.validateFreePush(this); }
        /**
         * 推进 {@code publish} 对应的事务、MVCC 与锁阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         */
        @Override void publish() { owner.completeFreePush(this); }
        /**
         * 推进 {@code abortBeforePhysical} 对应的事务、MVCC 与锁阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         */
        @Override void abortBeforePhysical() { owner.releaseFreeTransition(); }
    }

    /** 三类 owner transition 的公共 fail-stop 状态机。 */
    private abstract static class TransitionLease implements AutoCloseable {
        /**
         * 本次事务链路持有的 {@code owner} undo/rollback 状态；事务身份、roll pointer 与段代际必须一致，提交、回滚和 purge 路径依赖它完成收口。
         */
        final UndoSegmentReuseDirectory owner;
        /**
         * 记录 {@code physicalMutationStarted} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
         */
        private boolean physicalMutationStarted;
        /**
         * 记录 {@code completed} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
         */
        private boolean completed;
        /**
         * 记录 {@code closed} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
         */
        private boolean closed;

        private TransitionLease(UndoSegmentReuseDirectory owner) { this.owner = owner; }
        /**
         * 校验 {@code validate} 涉及的事务、MVCC 与锁结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
         */
        abstract void validate();
        /**
         * 推进 {@code publish} 对应的事务、MVCC 与锁阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         */
        abstract void publish();
        /**
         * 推进 {@code abortBeforePhysical} 对应的事务、MVCC 与锁阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         */
        abstract void abortBeforePhysical();

        /**
         * 在 lease 仍开放时复核转移对象的权威状态，并标记物理修改已经开始；重复标记或关闭后的调用必须拒绝。
         */
        void physicalMutationStarted() {
            requireOpen();
            validate();
            physicalMutationStarted = true;
        }

        /**
         * 推进 {@code complete} 对应的事务、MVCC 与锁阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         *
         * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
         */
        void complete() {
            requireOpen();
            if (!physicalMutationStarted) {
                throw new DatabaseValidationException("reuse transition cannot complete before physical mutation");
            }
            publish();
            completed = true;
        }

        /**
         * 释放本方法拥有的事务、MVCC 与锁资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
         */
        @Override
        public void close() {
            if (closed) return;
            closed = true;
            if (!completed && !physicalMutationStarted) abortBeforePhysical();
        }

        private void requireOpen() {
            if (closed) throw new DatabaseValidationException("operation on closed undo reuse lease");
        }
    }

    /** lifecycle X owner 的全局 drain guard。 */
    public static final class DrainLease implements AutoCloseable {
        /**
         * 本次事务链路持有的 {@code owner} undo/rollback 状态；事务身份、roll pointer 与段代际必须一致，提交、回滚和 purge 路径依赖它完成收口。
         */
        private final UndoSegmentReuseDirectory owner;
        /**
         * 构造时冻结的 {@code inFlight} 领域快照；其身份、版本与范围来自同一次权威读取，下游步骤依赖它检测并发变化和避免发布陈旧状态。
         */
        private DrainBatch inFlight;
        /**
         * 记录 {@code physicalMutationStarted} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
         */
        private boolean physicalMutationStarted;
        /**
         * 记录 {@code finished} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
         */
        private boolean finished;
        /**
         * 记录 {@code closed} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
         */
        private boolean closed;

        private DrainLease(UndoSegmentReuseDirectory owner) { this.owner = owner; }

        /**
         * 按 INSERT cache top、UPDATE cache top、free head 顺序冻结下一批 owner；同一时刻只允许一批 in-flight。
         *
         * @param maxSegments 批次上限，必须为正数。
         * @return 空目录返回 empty，否则返回含运行期 expected count 的不可变批次。
         * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
         */
        public Optional<DrainBatch> nextBatch(int maxSegments) {
            requireOpen();
            if (maxSegments <= 0 || inFlight != null) {
                throw new DatabaseValidationException("invalid/in-flight undo reuse drain batch request");
            }
            inFlight = owner.nextDrainBatch(maxSegments);
            return Optional.ofNullable(inFlight);
        }

        /** 在任何 FSP/page3 修改前复核批次并把失败语义切换为 fail-stop。
         *
         * @param batch 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
         */
        public void physicalMutationStarted(DrainBatch batch) {
            requireMatching(batch);
            owner.validateDrainBatch(batch);
            physicalMutationStarted = true;
        }

        /** MTR commit 后从运行期目录删除本批 owner，并允许规划下一批。
         *
         * @param batch 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
         * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
         */
        public void completeBatch(DrainBatch batch) {
            requireMatching(batch);
            if (!physicalMutationStarted) {
                throw new DatabaseValidationException("reuse drain cannot complete before physical mutation");
            }
            owner.completeDrainBatch(batch);
            inFlight = null;
            physicalMutationStarted = false;
        }

        /** 全部批次完成且目录为空后释放 drain gate。
         *
         * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
         */
        public void finish() {
            requireOpen();
            if (inFlight != null) throw new DatabaseValidationException("reuse drain still has an in-flight batch");
            owner.finishDrain();
            finished = true;
        }

        /** 物理修改前退出会撤销 drain gate；越过物理边界后保留 gate，强制实例 fail-stop。 */
        @Override
        public void close() {
            if (closed) return;
            closed = true;
            if (!finished && !physicalMutationStarted) owner.cancelDrain();
        }

        private void requireMatching(DrainBatch batch) {
            requireOpen();
            if (batch == null || inFlight == null || !inFlight.equals(batch)) {
                throw new DatabaseValidationException("undo reuse drain batch does not match current lease");
            }
        }

        private void requireOpen() {
            if (closed) throw new DatabaseValidationException("operation on closed undo reuse drain lease");
        }
    }

    private void validateCachePop(CacheCandidate candidate) {
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(candidate.segment().kind());
            if (!cacheTransition(candidate.segment().kind()) || draining
                    || stack.size() != candidate.expectedCount() || !stack.getLast().equals(candidate.segment())) {
                throw new DatabaseValidationException("undo cache pop lease lost its owner");
            }
        } finally { lock.unlock(); }
    }

    private void completeCachePop(CacheCandidate candidate) {
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(candidate.segment().kind());
            if (!cacheTransition(candidate.segment().kind()) || stack.size() != candidate.expectedCount()
                    || !stack.getLast().equals(candidate.segment())) {
                throw new DatabaseValidationException("undo cache pop completion owner mismatch");
            }
            stack.removeLast();
            setCacheTransition(candidate.segment().kind(), false);
        } finally { lock.unlock(); }
    }

    private void validateCachePush(CachedUndoSegmentRef segment, int expectedCount) {
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(segment.kind());
            if (!cacheTransition(segment.kind()) || draining || stack.size() != expectedCount
                    || stack.size() >= capacityPerKind) {
                throw new DatabaseValidationException("undo cache push lease lost owner/capacity");
            }
        } finally { lock.unlock(); }
    }

    private void completeCachePush(CachedUndoSegmentRef segment, int expectedCount) {
        lock.lock();
        try {
            validateCachePushUnderLock(segment, expectedCount);
            stack(segment.kind()).add(segment);
            setCacheTransition(segment.kind(), false);
        } finally { lock.unlock(); }
    }

    private void validateCachePushUnderLock(CachedUndoSegmentRef segment, int expectedCount) {
        List<CachedUndoSegmentRef> stack = stack(segment.kind());
        if (!cacheTransition(segment.kind()) || stack.size() != expectedCount || stack.size() >= capacityPerKind) {
            throw new DatabaseValidationException("undo cache push completion owner/capacity mismatch");
        }
    }

    /**
     * 校验 {@code validateFreePop} 涉及的事务、MVCC 与锁结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param candidate 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private void validateFreePop(FreeCandidate candidate) {
        lock.lock();
        try {
            if (!freeTransition || draining || !matchesFreeCandidate(candidate)) {
                throw new DatabaseValidationException("undo free pop lease lost its owner");
            }
        } finally { lock.unlock(); }
    }

    /**
     * 推进 {@code completeFreePop} 对应的事务、MVCC 与锁阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
     *
     * @param candidate 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private void completeFreePop(FreeCandidate candidate) {
        lock.lock();
        try {
            if (!freeTransition || !matchesFreeCandidate(candidate)) {
                throw new DatabaseValidationException("undo free pop completion owner mismatch");
            }
            freeQueue.removeFirst();
            freeTransition = false;
        } finally { lock.unlock(); }
    }

    /**
     * 校验 {@code validateFreePush} 涉及的事务、MVCC 与锁结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param lease 调用方持有的 {@code FreePushLease} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     */
    private void validateFreePush(FreePushLease lease) {
        lock.lock();
        try { requireFreePushState(lease); } finally { lock.unlock(); }
    }

    /**
     * 推进 {@code completeFreePush} 对应的事务、MVCC 与锁阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
     *
     * @param lease 调用方持有的 {@code FreePushLease} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     */
    private void completeFreePush(FreePushLease lease) {
        lock.lock();
        try {
            requireFreePushState(lease);
            freeQueue.addAll(lease.segments());
            freeTransition = false;
        } finally { lock.unlock(); }
    }

    private void requireFreePushState(FreePushLease lease) {
        if (!freeTransition || draining || freeQueue.size() != lease.expectedCount()
                || !Optional.ofNullable(freeQueue.peekLast()).equals(lease.expectedTail())) {
            throw new DatabaseValidationException("undo free push lease lost owner/count");
        }
    }

    /**
     * 校验当前状态后推进事务、MVCC 与锁状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * @param kind 选择 {@code releaseCacheTransition} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private void releaseCacheTransition(UndoLogKind kind) {
        lock.lock();
        try {
            if (!cacheTransition(kind)) throw new DatabaseValidationException("cache transition fence not held");
            setCacheTransition(kind, false);
        } finally { lock.unlock(); }
    }

    /**
     * 校验当前状态后推进事务、MVCC 与锁状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private void releaseFreeTransition() {
        lock.lock();
        try {
            if (!freeTransition) throw new DatabaseValidationException("free transition fence not held");
            freeTransition = false;
        } finally { lock.unlock(); }
    }

    private DrainBatch nextDrainBatch(int maxSegments) {
        lock.lock();
        try {
            requireDraining();
            if (insertStack.isEmpty() && updateStack.isEmpty() && freeQueue.isEmpty()) return null;
            int remaining = maxSegments;
            List<CachedUndoSegmentRef> insert = topFirst(insertStack, remaining);
            remaining -= insert.size();
            List<CachedUndoSegmentRef> update = topFirst(updateStack, remaining);
            remaining -= update.size();
            List<FreeUndoSegmentRef> free = freeQueue.stream().limit(remaining).toList();
            return new DrainBatch(insertStack.size(), updateStack.size(), freeQueue.size(), insert, update, free);
        } finally { lock.unlock(); }
    }

    private void validateDrainBatch(DrainBatch batch) {
        lock.lock();
        try { requireDraining(); requireDrainTops(batch); } finally { lock.unlock(); }
    }

    private void completeDrainBatch(DrainBatch batch) {
        lock.lock();
        try {
            requireDraining();
            requireDrainTops(batch);
            removeTop(insertStack, batch.insertTopFirst());
            removeTop(updateStack, batch.updateTopFirst());
            for (FreeUndoSegmentRef expected : batch.freeHeadFirst()) {
                if (!freeQueue.removeFirst().equals(expected)) {
                    throw new DatabaseValidationException("undo free drain head changed during completion");
                }
            }
        } finally { lock.unlock(); }
    }

    private void requireDrainTops(DrainBatch batch) {
        if (insertStack.size() != batch.expectedInsertCount()
                || updateStack.size() != batch.expectedUpdateCount()
                || freeQueue.size() != batch.expectedFreeCount()
                || !topFirst(insertStack, batch.insertTopFirst().size()).equals(batch.insertTopFirst())
                || !topFirst(updateStack, batch.updateTopFirst().size()).equals(batch.updateTopFirst())
                || !freeQueue.stream().limit(batch.freeHeadFirst().size()).toList().equals(batch.freeHeadFirst())) {
            throw new DatabaseValidationException("undo reuse drain batch no longer matches directory");
        }
    }

    private void finishDrain() {
        lock.lock();
        try {
            requireDraining();
            if (!insertStack.isEmpty() || !updateStack.isEmpty() || !freeQueue.isEmpty()) {
                throw new DatabaseValidationException("undo reuse drain cannot finish before all owners are empty");
            }
            draining = false;
        } finally { lock.unlock(); }
    }

    private void cancelDrain() {
        lock.lock();
        try { requireDraining(); draining = false; } finally { lock.unlock(); }
    }

    private void requireDraining() {
        if (!draining || insertTransition || updateTransition || freeTransition) {
            throw new DatabaseValidationException("undo reuse directory is not exclusively draining");
        }
    }

    private static List<CachedUndoSegmentRef> topFirst(List<CachedUndoSegmentRef> stack, int max) {
        int count = Math.min(Math.max(max, 0), stack.size());
        List<CachedUndoSegmentRef> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) result.add(stack.get(stack.size() - 1 - i));
        return List.copyOf(result);
    }

    private static void removeTop(List<CachedUndoSegmentRef> stack, List<CachedUndoSegmentRef> expected) {
        for (CachedUndoSegmentRef item : expected) {
            if (stack.isEmpty() || !stack.getLast().equals(item)) {
                throw new DatabaseValidationException("undo cache drain top changed during completion");
            }
            stack.removeLast();
        }
    }
}
