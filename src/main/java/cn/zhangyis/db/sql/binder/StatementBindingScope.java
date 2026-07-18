package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.exception.MetadataLockTimeoutException;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.dd.service.TableMetadataLease;
import cn.zhangyis.db.sql.binder.exception.SqlBindingException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * 单条语句的 metadata staging guard。openTable 复用事务 lease 或暂存新 lease；publish 转移 owner，未 publish 的
 * close 是 abort，反序关闭本语句新取得的资源，不触碰事务既有 lease。
 */
public final class StatementBindingScope implements AutoCloseable {
    /**
     * 本对象关联的 {@code transaction} 事务、会话或锁状态；owner 在生命周期内稳定，等待、可见性和释放路径均依赖该关联。
     */
    private final TransactionMetadataScope transaction;
    /**
     * 记录 {@code deadline} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final long deadline;
    /**
     * 本对象拥有的 {@code staged} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
     */
    private final LinkedHashMap<String, StagedLease> staged = new LinkedHashMap<>();
    /**
     * 本对象的权威状态机字段 {@code state}；只有合法转换方法可以更新，更新受显式锁、原子发布或单一 owner 线程保护，下游据此决定可执行阶段。
     */
    private State state = State.OPEN;

    /**
     * 创建 {@code StatementBindingScope}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param transaction 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param deadline 本次绑定操作使用的单调时钟截止点；到期后必须停止等待并保持元数据 lease 可释放
     */
    StatementBindingScope(TransactionMetadataScope transaction, long deadline) {
        this.transaction = transaction;
        this.deadline = deadline;
    }

    /** 按 canonical table key 获取 exact TableDefinition；READ→WRITE 先取得新 lease，失败时保留旧 READ。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param intent 由组合根提供的 {@code TableAccessIntent} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code openTable} 调用
     * @return {@code openTable} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     * @throws SqlBindingException SQL 绑定、会话准入或事务结果无法按当前状态完成时抛出；调用方应报告错误并按事务边界回滚或关闭
     */
    public TableDefinition openTable(QualifiedTableName name, TableAccessIntent intent) {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        ensureOpen();
        if (name == null || intent == null) {
            throw new SqlBindingException("table name/access intent must not be null");
        }
        String key = name.canonicalKey();
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        StagedLease local = staged.get(key);
        if (local != null && covers(local.intent(), intent)) return local.lease().table();

        TransactionMetadataScope.HeldLease existing = transaction.lookup(key);
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
        if (existing != null && covers(existing.intent(), intent)) return existing.lease().table();

        TableMetadataLease acquired = transaction.acquire(name, intent, remaining());
        StagedLease replacedLocal = staged.put(key, new StagedLease(key, intent, acquired));
        if (replacedLocal != null) replacedLocal.lease().close();
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        return acquired.table();
    }

    /** 将 staged leases 一次转交 transaction；重复发布或发布后继续 bind 都是状态错误。 */
    public void publish() {
        ensureOpen();
        List<StagedLease> leases = List.copyOf(staged.values());
        try {
            transaction.publish(leases);
        } finally {
            // publish 要么事务接管，要么 transaction.publish 已关闭 orphan；本 scope 不再拥有这些 lease。
            staged.clear();
            state = State.PUBLISHED;
        }
    }

    /** bind 失败的 abort 路径；只关闭 staged lease，复用的事务 lease 不在此集合中。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     */
    @Override
    public void close() {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        if (state != State.OPEN) return;
        state = State.ABORTED;
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        RuntimeException failure = null;
        List<TableMetadataLease> leases = new ArrayList<>();
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
        for (StagedLease entry : staged.values()) leases.add(entry.lease());
        staged.clear();
        for (int i = leases.size() - 1; i >= 0; i--) {
            try { leases.get(i).close(); }
            catch (RuntimeException closeFailure) {
                if (failure == null) failure = closeFailure; else failure.addSuppressed(closeFailure);
            }
        }
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        if (failure != null) throw failure;
    }

    private Duration remaining() {
        long nanos = deadline == Long.MAX_VALUE ? Long.MAX_VALUE : deadline - System.nanoTime();
        if (nanos <= 0) throw new MetadataLockTimeoutException("statement metadata deadline expired");
        return Duration.ofNanos(nanos);
    }

    private void ensureOpen() {
        if (state != State.OPEN) throw new SqlBindingException("statement binding scope is no longer open: " + state);
    }

    private static boolean covers(TableAccessIntent held, TableAccessIntent requested) {
        return held == TableAccessIntent.WRITE || requested == TableAccessIntent.READ;
    }

    /**
     * 封装SQL 名称绑定与类型推导中 {@code StagedLease} 的绑定结果或元数据租约；schema 版本与释放责任在创建后固定，执行结束必须按所有权关闭。
     *
     * @param key 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @param intent 由组合根提供的 {@code TableAccessIntent} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param lease 调用方持有的 {@code TableMetadataLease} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     */
    record StagedLease(String key, TableAccessIntent intent, TableMetadataLease lease) { }
    /**
     * 定义SQL 名称绑定与类型推导的 {@code State} 状态或类别；枚举值用于显式分派领域行为，不得用声明顺序代替稳定编码。
     *
     * <p>未单独声明 Javadoc 的枚举值语义：</p>
     * <ul>
     *     <li>{@code OPEN}：表示“OPEN”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code PUBLISHED}：表示“PUBLISHED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code ABORTED}：表示“ABORTED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     * </ul>
     */
    private enum State { OPEN, PUBLISHED, ABORTED }
}
