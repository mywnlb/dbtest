package cn.zhangyis.db.dd.tx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.exception.DictionaryTransactionStateException;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;

import java.util.ArrayList;
import java.util.List;

/**
 * 单线程、一次性字典 Unit of Work。对象只 staging immutable mutations；真正并发冲突、持久 append 和 snapshot
 * publish 由 repository 的短 writer 临界区完成。close 未 commit 等价于 rollback，无物理副作用。
 */
public final class DictionaryTransaction implements AutoCloseable {

    /**
     * 本对象持有的 {@code repository} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final PersistentDictionaryRepository repository;
    /**
     * 构造或发布时固定的 {@code version} 版本、文件身份或源位置；必须来自当前对象上下文，诊断、并发校验和恢复路径依赖其稳定性。
     */
    private final DictionaryVersion version;
    /**
     * 本对象拥有的 {@code schemas} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
     */
    private final List<SchemaDefinition> schemas = new ArrayList<>();
    /**
     * 本对象拥有的 {@code tables} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
     */
    private final List<TableDefinition> tables = new ArrayList<>();
    /**
     * 本对象的权威状态机字段 {@code state}；只有合法转换方法可以更新，更新受显式锁、原子发布或单一 owner 线程保护，下游据此决定可执行阶段。
     */
    private State state = State.OPEN;

    /**
     * 创建 {@code DictionaryTransaction}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param repository 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param version 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DictionaryTransaction(PersistentDictionaryRepository repository, DictionaryVersion version) {
        if (repository == null || version == null) {
            throw new DatabaseValidationException("dictionary transaction repository/version must not be null");
        }
        this.repository = repository;
        this.version = version;
    }

    /**
     * 根据调用参数构造 {@code createSchema} 对应的数据字典领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void createSchema(SchemaDefinition schema) {
        requireOpen();
        if (schema == null) {
            throw new DatabaseValidationException("schema mutation must not be null");
        }
        schemas.add(schema);
    }

    /**
     * 根据调用参数构造 {@code createTable} 对应的数据字典领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param table 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void createTable(TableDefinition table) {
        requireOpen();
        if (table == null) {
            throw new DatabaseValidationException("table mutation must not be null");
        }
        tables.add(table);
    }

    /**
     * 暂存同一 table identity 的生命周期新版本。当前只允许 ACTIVE→DROP_PENDING→DROPPED，repository commit
     * 会重新基于最新 snapshot 校验，避免两个并发 DDL 都从陈旧状态推进。
     *
     * @param table 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void updateTable(TableDefinition table) {
        requireOpen();
        if (table == null) {
            throw new DatabaseValidationException("table update mutation must not be null");
        }
        tables.add(table);
    }

    /** 提交全部 staging mutation；成功后事务不可再次使用，失败保持 OPEN 以便调用方 close 丢弃。 */
    public void commit() {
        requireOpen();
        repository.commit(version, List.copyOf(schemas), List.copyOf(tables));
        state = State.COMMITTED;
    }

    /**
     * 释放本方法拥有的数据字典资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     */
    @Override
    public void close() {
        if (state == State.OPEN) {
            schemas.clear();
            tables.clear();
            state = State.ROLLED_BACK;
        }
    }

    private void requireOpen() {
        if (state != State.OPEN) {
            throw new DictionaryTransactionStateException("dictionary transaction is not open: " + state);
        }
    }

    /**
     * 定义数据字典的 {@code State} 状态或类别；枚举值用于显式分派领域行为，不得用声明顺序代替稳定编码。
     *
     * <p>未单独声明 Javadoc 的枚举值语义：</p>
     * <ul>
     *     <li>{@code OPEN}：表示“OPEN”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code COMMITTED}：表示“COMMITTED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code ROLLED_BACK}：表示“ROLLEDBACK”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     * </ul>
     */
    private enum State {
        OPEN,
        COMMITTED,
        ROLLED_BACK
    }
}
