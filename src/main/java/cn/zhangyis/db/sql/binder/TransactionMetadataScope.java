package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.service.DataDictionaryService;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.dd.service.TableMetadataLease;
import cn.zhangyis.db.sql.binder.exception.SqlBindingException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SQL transaction 的 DD pin/MDL owner。所有可变 held lease 状态由显式 lock 保护；Session 虽串行执行语句，
 * close/engine shutdown 仍可能竞争，因此不能依赖线程约定隐藏释放边界。
 */
public final class TransactionMetadataScope implements AutoCloseable {
    /**
     * 本对象持有的 {@code dictionary} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DataDictionaryService dictionary;
    /**
     * 构造时冻结的 {@code owner} 稳定领域标识；必须属于本对象的表空间、事务或日志上下文，下游定位与恢复均依赖其身份不变。
     */
    private final MdlOwnerId owner;
    /**
     * 保护本对象共享状态的显式并发闩；获取后必须在 {@code finally} 或 Guard 关闭路径释放。
     */
    private final ReentrantLock lock = new ReentrantLock();
    /** insertion order 同时是获取顺序；事务终结按其反序关闭。 */
    private final LinkedHashMap<String, HeldLease> held = new LinkedHashMap<>();
    /**
     * 记录 {@code closed} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
     */
    private boolean closed;

    /**
     * 创建 {@code TransactionMetadataScope}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param dictionary 由组合根提供的 {@code DataDictionaryService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param owner 参与 {@code 构造} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public TransactionMetadataScope(DataDictionaryService dictionary, MdlOwnerId owner) {
        if (dictionary == null || owner == null) {
            throw new DatabaseValidationException("transaction metadata dictionary/owner must not be null");
        }
        this.dictionary = dictionary;
        this.owner = owner;
    }

    /** 创建共享一个绝对 deadline 的 statement staging scope；等待 MDL 前本对象不持有内部 lock。
     *
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @return {@code beginStatement} 产生的 SQL 语句、绑定或执行对象；成功时不为 {@code null}，并保留当前 schema 版本和会话语义
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public StatementBindingScope beginStatement(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("statement metadata timeout must be positive");
        }
        lock.lock();
        try {
            ensureOpen();
            return new StatementBindingScope(this, deadline(timeout));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 定位并读取SQL 名称绑定与类型推导领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param key 传给 {@code lookup} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @return {@code lookup} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    HeldLease lookup(String key) {
        lock.lock();
        try {
            ensureOpen();
            return held.get(key);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 按SQL 名称绑定与类型推导并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param intent 由组合根提供的 {@code TableAccessIntent} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code acquire} 调用
     * @param remaining 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     * @return {@code acquire} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    TableMetadataLease acquire(QualifiedTableName name, TableAccessIntent intent, Duration remaining) {
        return dictionary.openTable(owner, name, intent, remaining);
    }

    /**
     * 接收 statement staged lease。新 lease 先进入事务权威 map，再关闭被替换的 READ lease；这样 close 失败也不会
     * 让 WRITE lease 失去 owner。若事务已关闭，则本方法反序关闭 staged lease 后抛错，保证 shutdown 竞争不泄漏。
     *
     * @param staged 参与 {@code publish} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    void publish(List<StatementBindingScope.StagedLease> staged) {
        RuntimeException failure = null;
        lock.lock();
        try {
            if (closed) {
                failure = new SqlBindingException("transaction metadata scope is already closed");
            } else {
                List<TableMetadataLease> replaced = new ArrayList<>();
                for (StatementBindingScope.StagedLease entry : staged) {
                    HeldLease previous = held.put(entry.key(), new HeldLease(entry.intent(), entry.lease()));
                    if (previous != null && previous.lease() != entry.lease()) replaced.add(previous.lease());
                }
                failure = closeReverse(replaced, failure);
                returnOrThrow(failure);
                return;
            }
        } finally {
            lock.unlock();
        }
        List<TableMetadataLease> orphaned = staged.stream().map(StatementBindingScope.StagedLease::lease).toList();
        failure = closeReverse(orphaned, failure);
        throw failure;
    }

    /** storage commit/rollback 完成后调用；先冻结 closed，再反序释放 pin 和 table/schema MDL。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     */
    @Override
    public void close() {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        List<TableMetadataLease> leases;
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        lock.lock();
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        try {
            if (closed) return;
            closed = true;
            leases = held.values().stream().map(HeldLease::lease).toList();
            held.clear();
        } finally {
            lock.unlock();
        }
        RuntimeException failure = closeReverse(leases, null);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        returnOrThrow(failure);
    }

    private void ensureOpen() {
        if (closed) throw new SqlBindingException("transaction metadata scope is already closed");
    }

    /**
     * 根据调用参数创建或转换 {@code closeReverse} 返回的 {@code RuntimeException}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param leases 参与 {@code closeReverse} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param failure 需要分类或包装的原始失败；允许为 {@code null} 表示没有底层 cause，存在时必须保留 cause 与 suppressed 异常图
     * @return {@code closeReverse} 分类或包装后的领域异常；成功时不为 {@code null}，原始 cause 与 suppressed 异常关系保持不变
     */
    private static RuntimeException closeReverse(List<TableMetadataLease> leases, RuntimeException failure) {
        for (int i = leases.size() - 1; i >= 0; i--) {
            try {
                leases.get(i).close();
            } catch (RuntimeException closeFailure) {
                if (failure == null) failure = closeFailure; else failure.addSuppressed(closeFailure);
            }
        }
        return failure;
    }

    private static void returnOrThrow(RuntimeException failure) {
        if (failure != null) throw failure;
    }

    private static long deadline(Duration timeout) {
        long nanos;
        try { nanos = timeout.toNanos(); }
        catch (ArithmeticException error) { throw new DatabaseValidationException("metadata timeout is too large", error); }
        long now = System.nanoTime();
        long result = now + nanos;
        return result < 0 && now > 0 ? Long.MAX_VALUE : result;
    }

    /**
     * 封装SQL 名称绑定与类型推导中 {@code HeldLease} 的绑定结果或元数据租约；schema 版本与释放责任在创建后固定，执行结束必须按所有权关闭。
     *
     * @param intent 由组合根提供的 {@code TableAccessIntent} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param lease 调用方持有的 {@code TableMetadataLease} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     */
    record HeldLease(TableAccessIntent intent, TableMetadataLease lease) { }
}
