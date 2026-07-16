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

    private final PersistentDictionaryRepository repository;
    private final DictionaryVersion version;
    private final List<SchemaDefinition> schemas = new ArrayList<>();
    private final List<TableDefinition> tables = new ArrayList<>();
    private State state = State.OPEN;

    public DictionaryTransaction(PersistentDictionaryRepository repository, DictionaryVersion version) {
        if (repository == null || version == null) {
            throw new DatabaseValidationException("dictionary transaction repository/version must not be null");
        }
        this.repository = repository;
        this.version = version;
    }

    public void createSchema(SchemaDefinition schema) {
        requireOpen();
        if (schema == null) {
            throw new DatabaseValidationException("schema mutation must not be null");
        }
        schemas.add(schema);
    }

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

    private enum State {
        OPEN,
        COMMITTED,
        ROLLED_BACK
    }
}
