package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;
import cn.zhangyis.db.dd.exception.DictionaryObjectExistsException;
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
import cn.zhangyis.db.dd.exception.DictionaryVersionConflictException;
import cn.zhangyis.db.dd.tx.DictionaryTransaction;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogStore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * append-only catalog 的 Repository。writerLock 只保护 expected-version 校验、单批 append 和新 snapshot 发布；
 * lookup 读取 volatile immutable snapshot，不拿锁。锁内不获取 MDL/page latch，也不等待其它数据库资源。
 */
public final class PersistentDictionaryRepository {

    private final InternalCatalogStore store;
    private final DictionaryCatalogCodec codec = new DictionaryCatalogCodec();

    /** 字典版本唯一 writer 临界区，不能跨出本 repository。 */
    private final ReentrantLock writerLock = new ReentrantLock();

    /** 已发布不可变快照；append + rebuild 完成后一次 volatile 替换。 */
    private volatile DictionarySnapshot snapshot;

    public PersistentDictionaryRepository(InternalCatalogStore store) {
        if (store == null) {
            throw new DatabaseValidationException("internal catalog store must not be null");
        }
        this.store = store;
        this.snapshot = rebuild(store.readCommittedBatches());
    }

    /** 创建指定 next version 的内部 Unit of Work；实际冲突在 commit 时重新验证。 */
    public DictionaryTransaction begin(DictionaryVersion version) {
        if (version == null) {
            throw new DatabaseValidationException("dictionary transaction version must not be null");
        }
        return new DictionaryTransaction(this, version);
    }

    public Optional<SchemaDefinition> findSchema(SchemaId id) {
        return Optional.ofNullable(snapshot.schemas().get(id));
    }

    public Optional<SchemaDefinition> findSchema(ObjectName name) {
        if (name == null) {
            throw new DatabaseValidationException("schema name must not be null");
        }
        return snapshot.schemas().values().stream().filter(schema -> schema.name().equals(name)).findFirst();
    }

    public Optional<TableDefinition> findTable(TableId id) {
        return Optional.ofNullable(snapshot.tables().get(id))
                .filter(table -> table.state() == cn.zhangyis.db.dd.domain.TableState.ACTIVE);
    }

    public Optional<TableDefinition> findTable(SchemaId schemaId, ObjectName name) {
        if (schemaId == null || name == null) {
            throw new DatabaseValidationException("table schema id/name must not be null");
        }
        return snapshot.tables().values().stream()
                .filter(table -> table.state() == cn.zhangyis.db.dd.domain.TableState.ACTIVE)
                .filter(table -> table.schemaId().equals(schemaId) && table.name().equals(name)).findFirst();
    }

    /** recovery/DDL state machine 读取包括 DROP_PENDING/DROPPED 在内的原始最新版本。 */
    public Optional<TableDefinition> findTableForRecovery(TableId id) {
        return Optional.ofNullable(snapshot.tables().get(id));
    }

    public Optional<IndexDefinition> findIndex(IndexId id) {
        return Optional.ofNullable(snapshot.indexes().get(id));
    }

    public DictionarySnapshot snapshot() {
        return snapshot;
    }

    /**
     * DictionaryTransaction 的唯一提交入口。先在当前 snapshot 上完成版本、名称和父对象校验，再编码/append；
     * append durable 后从 committed batches 重建并发布，任何前置失败都不会写 catalog。
     */
    public void commit(DictionaryVersion version, List<SchemaDefinition> schemas, List<TableDefinition> tables) {
        if (version == null || schemas == null || tables == null || schemas.isEmpty() && tables.isEmpty()) {
            throw new DatabaseValidationException("dictionary commit version/mutations must not be null or empty");
        }
        writerLock.lock();
        try {
            DictionarySnapshot before = snapshot;
            if (version.compareTo(before.publishedVersion()) <= 0) {
                throw new DictionaryVersionConflictException("dictionary version must advance monotonically: before="
                        + before.publishedVersion().value() + ", actual=" + version.value());
            }
            validateMutation(before, version, schemas, tables);
            List<CatalogRecord> records = codec.encode(version, schemas, tables);
            store.append(records);
            DictionarySnapshot rebuilt = rebuild(store.readCommittedBatches());
            if (!rebuilt.publishedVersion().equals(version)) {
                throw new DictionaryCatalogCorruptionException("dictionary append did not publish requested version: "
                        + version.value());
            }
            snapshot = rebuilt;
        } finally {
            writerLock.unlock();
        }
    }

    private static void validateMutation(DictionarySnapshot before, DictionaryVersion version,
                                         List<SchemaDefinition> schemas, List<TableDefinition> tables) {
        Map<SchemaId, SchemaDefinition> schemaView = new LinkedHashMap<>(before.schemas());
        for (SchemaDefinition schema : schemas) {
            if (schema == null || !schema.version().equals(version)) {
                throw new DictionaryVersionConflictException("schema mutation version mismatch");
            }
            boolean duplicateName = schemaView.values().stream().anyMatch(existing ->
                    existing.name().equals(schema.name()) && !existing.id().equals(schema.id()));
            if (duplicateName || before.schemas().containsKey(schema.id())) {
                throw new DictionaryObjectExistsException("schema already exists: " + schema.name());
            }
            schemaView.put(schema.id(), schema);
        }
        Map<TableId, TableDefinition> tableView = new LinkedHashMap<>(before.tables());
        for (TableDefinition table : tables) {
            if (table == null || !table.version().equals(version)) {
                throw new DictionaryVersionConflictException("table mutation version mismatch");
            }
            if (!schemaView.containsKey(table.schemaId())) {
                throw new DictionaryObjectNotFoundException("table schema does not exist: "
                        + table.schemaId().value());
            }
            TableDefinition existingById = tableView.get(table.id());
            boolean duplicateName = tableView.values().stream().anyMatch(existing ->
                    existing.state() != cn.zhangyis.db.dd.domain.TableState.DROPPED
                            &&
                    existing.schemaId().equals(table.schemaId()) && existing.name().equals(table.name())
                            && !existing.id().equals(table.id()));
            if (duplicateName) {
                throw new DictionaryObjectExistsException("table already exists: " + table.name());
            }
            if (existingById == null) {
                if (table.state() != cn.zhangyis.db.dd.domain.TableState.ACTIVE) {
                    throw new DictionaryVersionConflictException("new table must enter ACTIVE state");
                }
            } else {
                validateTableReplacement(existingById, table);
            }
            tableView.put(table.id(), table);
        }
    }

    private static void validateTableReplacement(TableDefinition before, TableDefinition after) {
        if (!before.schemaId().equals(after.schemaId()) || !before.name().equals(after.name())
                || !before.columns().equals(after.columns()) || !before.indexes().equals(after.indexes())
                || !before.storageBinding().equals(after.storageBinding())) {
            throw new DictionaryVersionConflictException(
                    "table lifecycle replacement cannot change identity/schema/storage binding");
        }
        boolean valid = before.state() == cn.zhangyis.db.dd.domain.TableState.ACTIVE
                && after.state() == cn.zhangyis.db.dd.domain.TableState.DROP_PENDING
                || before.state() == cn.zhangyis.db.dd.domain.TableState.DROP_PENDING
                && after.state() == cn.zhangyis.db.dd.domain.TableState.DROPPED;
        if (!valid) {
            throw new DictionaryVersionConflictException("invalid table lifecycle transition: "
                    + before.state() + " -> " + after.state());
        }
    }

    private DictionarySnapshot rebuild(List<CatalogBatch> batches) {
        DictionarySnapshot current = DictionarySnapshot.emptyBootstrap();
        for (CatalogBatch batch : batches) {
            Optional<DictionaryCatalogCodec.DecodedMutation> decoded = codec.decode(batch);
            if (decoded.isEmpty()) {
                continue;
            }
            DictionaryCatalogCodec.DecodedMutation mutation = decoded.get();
            if (mutation.version().compareTo(current.publishedVersion()) <= 0) {
                throw new DictionaryCatalogCorruptionException("dictionary committed versions are not monotonic: before="
                        + current.publishedVersion().value() + ", actual=" + mutation.version().value());
            }
            validateMutation(current, mutation.version(), mutation.schemas(), mutation.tables());
            Map<SchemaId, SchemaDefinition> schemas = new LinkedHashMap<>(current.schemas());
            Map<TableId, TableDefinition> tables = new LinkedHashMap<>(current.tables());
            Map<IndexId, IndexDefinition> indexes = new LinkedHashMap<>(current.indexes());
            for (SchemaDefinition schema : mutation.schemas()) {
                schemas.put(schema.id(), schema);
            }
            for (TableDefinition table : mutation.tables()) {
                TableDefinition previous = tables.get(table.id());
                if (previous != null) {
                    for (IndexDefinition oldIndex : previous.indexes()) {
                        indexes.remove(oldIndex.id());
                    }
                }
                tables.put(table.id(), table);
                for (IndexDefinition index : table.indexes()) {
                    if (indexes.putIfAbsent(index.id(), index) != null) {
                        throw new DictionaryCatalogCorruptionException("duplicate index id in dictionary: "
                                + index.id().value());
                    }
                }
            }
            current = new DictionarySnapshot(mutation.version(), schemas, tables, indexes);
        }
        return current;
    }
}
