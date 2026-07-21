package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.IndexKeyPart;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.domain.TableOptions;
import cn.zhangyis.db.dd.domain.ColumnDefaultDefinition;
import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * DD logical objects 与 storage byte catalog record 的显式 codec。key 固定为 kind/parent/object/version/ordinal/chunk，
 * payload 按 1024B 分片；最后的 CATALOG_COMMIT 保存 record count + SHA-256，只有完整 manifest 才对读取者可见。
 */
final class DictionaryCatalogCodec {

    /**
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
    private static final int KEY_BYTES = 1 + Long.BYTES * 3 + Integer.BYTES * 2;
    /**
     * 类级校验或资源上界；所有实例以该值拒绝超限输入，调整时必须复核容量、等待与格式约束。
     */
    private static final int MAX_PAYLOAD_CHUNK = 1024;
    /**
     * 类级校验或资源上界；所有实例以该值拒绝超限输入，调整时必须复核容量、等待与格式约束。
     */
    private static final int MAX_STRING_BYTES = 4096;
    /**
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
    private static final int HASH_BYTES = 32;
    /** baseline meta payload magic：ASCII {@code DDB1}。 */
    private static final int BASELINE_MAGIC = 0x44444231;
    /** 当前 baseline 逻辑格式版本。 */
    private static final int BASELINE_FORMAT_VERSION = 1;
    /** 新 table payload 的显式 envelope；旧 payload 以原始 long tableId 开头。 */
    private static final int TABLE_PAYLOAD_MAGIC = 0x44445432;
    /** table payload 当前格式。 */
    private static final int TABLE_PAYLOAD_VERSION = 2;
    /** 新 column payload 的显式 envelope。 */
    private static final int COLUMN_PAYLOAD_MAGIC = 0x44444332;
    /** column payload 当前格式。 */
    private static final int COLUMN_PAYLOAD_VERSION = 2;

    /**
     * 把调用方领域值编码为数据字典的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param version 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param schemas 参与 {@code encode} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param tables 参与 {@code encode} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code encode} 产生的非空集合容器；元素身份与顺序遵循当前模块契约，无元素时返回空集合而非 {@code null}
     */
    List<CatalogRecord> encode(DictionaryVersion version, List<SchemaDefinition> schemas,
                               List<TableDefinition> tables) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        List<CatalogRecord> records = new ArrayList<>();
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        for (SchemaDefinition schema : schemas) {
            records.addAll(fragment(new CatalogKey(CatalogEntityKind.SCHEMA, 0, schema.id().value(),
                    version.value(), 0, 0), encodeSchema(schema)));
        }
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        for (TableDefinition table : tables) {
            records.addAll(fragment(new CatalogKey(CatalogEntityKind.TABLE, table.schemaId().value(),
                    table.id().value(), version.value(), 0, 0), encodeTable(table)));
            for (ColumnDefinition column : table.columns()) {
                records.addAll(fragment(new CatalogKey(CatalogEntityKind.COLUMN, table.id().value(),
                        column.columnId(), version.value(), column.ordinal(), 0), encodeColumn(column)));
            }
            for (int ordinal = 0; ordinal < table.indexes().size(); ordinal++) {
                IndexDefinition index = table.indexes().get(ordinal);
                records.addAll(fragment(new CatalogKey(CatalogEntityKind.INDEX, table.id().value(),
                        index.id().value(), version.value(), ordinal, 0), encodeIndex(index)));
            }
        }
        records.add(commitRecord(version, records));
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return List.copyOf(records);
    }

    /**
     * 把一个完整稳定快照编码为新 catalog 的首批 baseline。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>交叉校验 map key、schema/table 名称、父子关系与全局 index 集，拒绝内部不一致快照。</li>
     *     <li>校验对象版本不超过 published version，且表已经处于 ACTIVE/DISCARDED 稳定状态。</li>
     *     <li>按 identity 排序编码 meta 与全部对象，同时以 key ordinal 保留表内 column/index 顺序。</li>
     *     <li>追加覆盖全部 records 的 baseline commit；失败不产生部分返回值或持久副作用。</li>
     * </ol>
     *
     * @param snapshot 已完成唯一性和 binding 校验的稳定快照；表只能是 ACTIVE 或 DISCARDED
     * @return 可直接作为新 catalog 首批 append 的确定性 records
     * @throws DictionaryCatalogCorruptionException 快照包含临时状态、DROPPED tombstone 或对象版本越界时抛出
     */
    List<CatalogRecord> encodeBaseline(DictionarySnapshot snapshot) {
        // 1. disaster archive 不能信任容器 key；逐项证明 snapshot 的所有聚合视图彼此一致。
        if (snapshot == null) {
            throw new DictionaryCatalogCorruptionException("dictionary baseline snapshot must not be null");
        }
        DictionaryVersion published = snapshot.publishedVersion();
        if (published == null) {
            throw new DictionaryCatalogCorruptionException("dictionary baseline published version is missing");
        }
        List<SchemaDefinition> schemas = snapshot.schemas().values().stream()
                .sorted(Comparator.comparingLong(schema -> schema.id().value())).toList();
        List<TableDefinition> tables = snapshot.tables().values().stream()
                .sorted(Comparator.comparingLong(table -> table.id().value())).toList();
        Set<ObjectName> schemaNames = new HashSet<>();
        for (Map.Entry<SchemaId, SchemaDefinition> entry : snapshot.schemas().entrySet()) {
            if (!entry.getKey().equals(entry.getValue().id()) || !schemaNames.add(entry.getValue().name())) {
                throw new DictionaryCatalogCorruptionException(
                        "dictionary baseline schema map/name is inconsistent: " + entry.getKey().value());
            }
        }
        Set<String> tableNames = new HashSet<>();
        Map<IndexId, IndexDefinition> derivedIndexes = new LinkedHashMap<>();
        for (Map.Entry<TableId, TableDefinition> entry : snapshot.tables().entrySet()) {
            TableDefinition table = entry.getValue();
            String qualifiedName = table.schemaId().value() + ":" + table.name().canonicalName();
            if (!entry.getKey().equals(table.id())
                    || !snapshot.schemas().containsKey(table.schemaId())
                    || !tableNames.add(qualifiedName)) {
                throw new DictionaryCatalogCorruptionException(
                        "dictionary baseline table map/name/parent is inconsistent: " + entry.getKey().value());
            }
            for (IndexDefinition index : table.indexes()) {
                if (derivedIndexes.putIfAbsent(index.id(), index) != null) {
                    throw new DictionaryCatalogCorruptionException(
                            "dictionary baseline contains duplicate index id: " + index.id().value());
                }
            }
        }
        if (!derivedIndexes.equals(snapshot.indexes())) {
            throw new DictionaryCatalogCorruptionException(
                    "dictionary baseline global index map differs from table aggregates");
        }

        // 2. baseline 保留对象自己的历史版本，但任何对象都不能来自 published version 的未来。
        for (SchemaDefinition schema : schemas) {
            if (schema.version().compareTo(published) > 0) {
                throw new DictionaryCatalogCorruptionException(
                        "baseline schema version exceeds published version: " + schema.id().value());
            }
        }
        for (TableDefinition table : tables) {
            if (table.version().compareTo(published) > 0
                    || table.state() != TableState.ACTIVE && table.state() != TableState.DISCARDED
                    || table.storageBinding().isEmpty()) {
                throw new DictionaryCatalogCorruptionException(
                        "baseline table is not a stable visible/recoverable object: " + table.id().value()
                                + " state=" + table.state());
            }
        }

        // 3. 按稳定 identity 编码；表内 children 的 ordinal 仍以聚合根声明顺序为准。
        List<CatalogRecord> records = new ArrayList<>();
        int indexCount = tables.stream().mapToInt(table -> table.indexes().size()).sum();
        byte[] meta = ByteBuffer.allocate(Integer.BYTES * 5 + Long.BYTES).order(ByteOrder.BIG_ENDIAN)
                .putInt(BASELINE_MAGIC)
                .putInt(BASELINE_FORMAT_VERSION)
                .putLong(published.value())
                .putInt(schemas.size())
                .putInt(tables.size())
                .putInt(indexCount)
                .array();
        records.addAll(fragment(new CatalogKey(CatalogEntityKind.CATALOG_BASELINE_META, 0, 0,
                published.value(), 0, 0), meta));
        for (SchemaDefinition schema : schemas) {
            records.addAll(fragment(new CatalogKey(CatalogEntityKind.SCHEMA, 0, schema.id().value(),
                    published.value(), 0, 0), encodeSchema(schema)));
        }
        for (TableDefinition table : tables) {
            records.addAll(fragment(new CatalogKey(CatalogEntityKind.TABLE, table.schemaId().value(),
                    table.id().value(), published.value(), 0, 0), encodeTable(table)));
            for (ColumnDefinition column : table.columns()) {
                records.addAll(fragment(new CatalogKey(CatalogEntityKind.COLUMN, table.id().value(),
                        column.columnId(), published.value(), column.ordinal(), 0), encodeColumn(column)));
            }
            for (int ordinal = 0; ordinal < table.indexes().size(); ordinal++) {
                IndexDefinition index = table.indexes().get(ordinal);
                records.addAll(fragment(new CatalogKey(CatalogEntityKind.INDEX, table.id().value(),
                        index.id().value(), published.value(), ordinal, 0), encodeIndex(index)));
            }
        }
        // 4. commit 摘要是 baseline 可见性的唯一逻辑边界，底层 store 再提供物理 batch 边界。
        records.add(commitRecord(published, records, CatalogEntityKind.CATALOG_BASELINE_COMMIT));
        return List.copyOf(records);
    }

    /**
     * 尝试解码全量 baseline；普通 mutation/DDL batch 返回 empty，baseline 损坏则 fail-closed。
     *
     * @param batch 已通过物理 frame 校验的 catalog batch
     * @return 完整字典快照，或非 baseline batch 的 empty
     */
    Optional<DictionarySnapshot> decodeBaseline(CatalogBatch batch) {
        List<CatalogRecord> records = batch.records();
        Optional<CatalogKey> lastKey = tryDecodeKey(records.getLast().key());
        if (lastKey.isEmpty() || lastKey.get().kind != CatalogEntityKind.CATALOG_BASELINE_COMMIT) {
            return Optional.empty();
        }
        CatalogKey commitKey = lastKey.get();
        validateManifest(records, commitKey, CatalogEntityKind.CATALOG_BASELINE_COMMIT);
        if (records.size() < 2) {
            throw new DictionaryCatalogCorruptionException("dictionary baseline lacks meta record");
        }

        CatalogKey metaKey = decodeKey(records.getFirst().key());
        if (metaKey.kind != CatalogEntityKind.CATALOG_BASELINE_META
                || metaKey.parentId != 0 || metaKey.objectId != 0
                || metaKey.version != commitKey.version || metaKey.ordinal != 0 || metaKey.chunk != 0) {
            throw new DictionaryCatalogCorruptionException("dictionary baseline first record is not meta");
        }
        ByteBuffer meta = ByteBuffer.wrap(records.getFirst().payload()).order(ByteOrder.BIG_ENDIAN);
        if (meta.remaining() != Integer.BYTES * 5 + Long.BYTES
                || meta.getInt() != BASELINE_MAGIC
                || meta.getInt() != BASELINE_FORMAT_VERSION) {
            throw new DictionaryCatalogCorruptionException("dictionary baseline meta format is invalid");
        }
        long publishedVersion = meta.getLong();
        int schemaCount = meta.getInt();
        int tableCount = meta.getInt();
        int indexCount = meta.getInt();
        if (publishedVersion != commitKey.version || schemaCount < 0 || tableCount < 0 || indexCount < 0) {
            throw new DictionaryCatalogCorruptionException("dictionary baseline meta counters/version are invalid");
        }

        Map<GroupKey, List<Fragment>> grouped = new LinkedHashMap<>();
        for (int i = 1; i < records.size() - 1; i++) {
            CatalogRecord record = records.get(i);
            CatalogKey key = decodeKey(record.key());
            if (key.kind == CatalogEntityKind.CATALOG_COMMIT
                    || key.kind == CatalogEntityKind.CATALOG_BASELINE_COMMIT
                    || key.kind == CatalogEntityKind.CATALOG_BASELINE_META
                    || key.version != publishedVersion) {
                throw new DictionaryCatalogCorruptionException("dictionary baseline contains invalid kind/version");
            }
            GroupKey group = new GroupKey(key.kind, key.parentId, key.objectId, key.version, key.ordinal);
            grouped.computeIfAbsent(group, ignored -> new ArrayList<>())
                    .add(new Fragment(key.chunk, record.payload()));
        }
        DecodedObjects decoded = decodeObjects(grouped);
        if (decoded.schemas().size() != schemaCount || decoded.tables().size() != tableCount
                || decoded.tables().stream().mapToInt(table -> table.indexes().size()).sum() != indexCount) {
            throw new DictionaryCatalogCorruptionException("dictionary baseline aggregate counts mismatch");
        }
        Map<SchemaId, SchemaDefinition> schemas = new LinkedHashMap<>();
        Map<TableId, TableDefinition> tables = new LinkedHashMap<>();
        Map<IndexId, IndexDefinition> indexes = new LinkedHashMap<>();
        for (SchemaDefinition schema : decoded.schemas()) {
            if (schema.version().value() > publishedVersion
                    || schemas.putIfAbsent(schema.id(), schema) != null
                    || schemas.values().stream().filter(existing -> existing.name().equals(schema.name())).count() > 1) {
                throw new DictionaryCatalogCorruptionException(
                        "duplicate/invalid schema in dictionary baseline: " + schema.id().value());
            }
        }
        for (TableDefinition table : decoded.tables()) {
            if (table.version().value() > publishedVersion
                    || table.state() != TableState.ACTIVE && table.state() != TableState.DISCARDED
                    || table.storageBinding().isEmpty()
                    || !schemas.containsKey(table.schemaId())
                    || tables.putIfAbsent(table.id(), table) != null) {
                throw new DictionaryCatalogCorruptionException(
                        "invalid table in dictionary baseline: " + table.id().value());
            }
            boolean duplicateName = tables.values().stream().anyMatch(existing ->
                    existing != table && existing.schemaId().equals(table.schemaId())
                            && existing.name().equals(table.name()));
            if (duplicateName) {
                throw new DictionaryCatalogCorruptionException(
                        "duplicate table name in dictionary baseline: " + table.name());
            }
            for (IndexDefinition index : table.indexes()) {
                if (indexes.putIfAbsent(index.id(), index) != null) {
                    throw new DictionaryCatalogCorruptionException(
                            "duplicate index id in dictionary baseline: " + index.id().value());
                }
            }
        }
        return Optional.of(new DictionarySnapshot(DictionaryVersion.of(publishedVersion), schemas, tables, indexes));
    }

    /**
     * 从稳定表示解码数据字典领域值；先校验边界、标识与长度，损坏输入以领域异常拒绝。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param batch 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @return {@code decode} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DictionaryCatalogCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    Optional<DecodedMutation> decode(CatalogBatch batch) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        List<CatalogRecord> records = batch.records();
        Optional<CatalogKey> lastKey = tryDecodeKey(records.getLast().key());
        if (lastKey.isEmpty() || lastKey.get().kind != CatalogEntityKind.CATALOG_COMMIT) {
            // storage batch 可能是 crash 前 staging/其它未来消费者的数据；没有 DD manifest 就永远不可见。
            return Optional.empty();
        }
        CatalogKey commitKey = lastKey.get();
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        validateManifest(records, commitKey, CatalogEntityKind.CATALOG_COMMIT);

        Map<GroupKey, List<Fragment>> grouped = new LinkedHashMap<>();
        for (int i = 0; i < records.size() - 1; i++) {
            CatalogRecord record = records.get(i);
            CatalogKey key = decodeKey(record.key());
            if (key.kind == CatalogEntityKind.CATALOG_COMMIT || key.version != commitKey.version) {
                throw new DictionaryCatalogCorruptionException("dictionary batch contains invalid kind/version");
            }
            GroupKey group = new GroupKey(key.kind, key.parentId, key.objectId, key.version, key.ordinal);
            grouped.computeIfAbsent(group, ignored -> new ArrayList<>())
                    .add(new Fragment(key.chunk, record.payload()));
        }

        DecodedObjects decoded = decodeObjects(grouped);
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return Optional.of(new DecodedMutation(DictionaryVersion.of(commitKey.version),
                decoded.schemas(), decoded.tables(), decoded.legacyOptionTables()));
    }

    /** 从已经按 entity key 聚合的 payload 分片重建 schema/table 聚合。 */
    private static DecodedObjects decodeObjects(Map<GroupKey, List<Fragment>> grouped) {
        List<SchemaDefinition> schemas = new ArrayList<>();
        Map<Long, TableRoot> roots = new LinkedHashMap<>();
        Map<Long, List<OrderedColumn>> columns = new LinkedHashMap<>();
        Map<Long, List<OrderedIndex>> indexes = new LinkedHashMap<>();
        for (Map.Entry<GroupKey, List<Fragment>> entry : grouped.entrySet()) {
            GroupKey key = entry.getKey();
            byte[] payload = join(entry.getValue());
            switch (key.kind) {
                case SCHEMA -> {
                    SchemaDefinition schema = decodeSchema(payload);
                    if (key.parentId != 0 || key.ordinal != 0 || key.objectId != schema.id().value()) {
                        throw new DictionaryCatalogCorruptionException(
                                "schema catalog key/payload identity mismatch: " + key.objectId);
                    }
                    schemas.add(schema);
                }
                case TABLE -> {
                    TableRoot root = decodeTable(payload);
                    if (key.ordinal != 0 || key.objectId != root.id.value()
                            || key.parentId != root.schemaId.value()) {
                        throw new DictionaryCatalogCorruptionException(
                                "table catalog key/payload identity mismatch: " + key.objectId);
                    }
                    if (roots.putIfAbsent(key.objectId, root) != null) {
                        throw new DictionaryCatalogCorruptionException(
                                "duplicate table root in dictionary batch: " + key.objectId);
                    }
                }
                case COLUMN -> {
                    ColumnDefinition column = decodeColumn(payload);
                    if (key.objectId != column.columnId() || key.ordinal != column.ordinal()) {
                        throw new DictionaryCatalogCorruptionException(
                                "column catalog key/payload identity mismatch: " + key.objectId);
                    }
                    columns.computeIfAbsent(key.parentId, ignored -> new ArrayList<>())
                            .add(new OrderedColumn(key.ordinal, column));
                }
                case INDEX -> {
                    IndexDefinition index = decodeIndex(payload);
                    if (key.objectId != index.id().value()) {
                        throw new DictionaryCatalogCorruptionException(
                                "index catalog key/payload identity mismatch: " + key.objectId);
                    }
                    indexes.computeIfAbsent(key.parentId, ignored -> new ArrayList<>())
                            .add(new OrderedIndex(key.ordinal, index));
                }
                default -> throw new DictionaryCatalogCorruptionException(
                        "unsupported dictionary object kind: " + key.kind);
            }
        }
        Set<Long> orphanParents = new HashSet<>(columns.keySet());
        orphanParents.addAll(indexes.keySet());
        orphanParents.removeAll(roots.keySet());
        if (!orphanParents.isEmpty()) {
            throw new DictionaryCatalogCorruptionException(
                    "dictionary child references missing table root: " + orphanParents.iterator().next());
        }
        List<TableDefinition> tables = new ArrayList<>();
        Set<TableId> legacyOptionTables = new HashSet<>();
        for (TableRoot root : roots.values()) {
            List<ColumnDefinition> tableColumns =
                    columns.getOrDefault(root.id.value(), List.of()).stream()
                            .sorted(Comparator.comparingInt(OrderedColumn::ordinal))
                            .map(OrderedColumn::column).toList();
            List<OrderedIndex> orderedIndexes =
                    indexes.getOrDefault(root.id.value(), List.of()).stream()
                            .sorted(Comparator.comparingInt(OrderedIndex::ordinal)).toList();
            for (int ordinal = 0; ordinal < orderedIndexes.size(); ordinal++) {
                if (orderedIndexes.get(ordinal).ordinal() != ordinal) {
                    throw new DictionaryCatalogCorruptionException(
                            "table index ordinal gap/duplicate: " + root.id.value());
                }
            }
            List<IndexDefinition> tableIndexes = orderedIndexes.stream().map(OrderedIndex::index).toList();
            if (tableColumns.size() != root.columnCount || tableIndexes.size() != root.indexCount) {
                throw new DictionaryCatalogCorruptionException("table aggregate child count mismatch: "
                        + root.id.value());
            }
            TableOptions options;
            if (root.options.isPresent()) {
                options = root.options.orElseThrow();
            } else {
                Optional<SchemaDefinition> schema = schemas.stream().filter(candidate ->
                        candidate.id().equals(root.schemaId)).findFirst();
                options = schema.map(value -> new TableOptions(
                                "", value.defaultCharsetId(), value.defaultCollationId()))
                        .orElseGet(TableOptions::legacyDefaults);
                if (schema.isEmpty()) {
                    // 普通 mutation 往往不重复写 schema；repository 会使用此前已发布 schema 精确迁移。
                    legacyOptionTables.add(root.id);
                }
            }
            tables.add(new TableDefinition(root.id, root.schemaId, root.name, root.version, root.state,
                    tableColumns, tableIndexes, root.storageBinding, options));
        }
        return new DecodedObjects(
                List.copyOf(schemas), List.copyOf(tables), Set.copyOf(legacyOptionTables));
    }

    private static List<CatalogRecord> fragment(CatalogKey base, byte[] payload) {
        List<CatalogRecord> records = new ArrayList<>();
        int chunks = Math.max(1, (payload.length + MAX_PAYLOAD_CHUNK - 1) / MAX_PAYLOAD_CHUNK);
        for (int chunk = 0; chunk < chunks; chunk++) {
            int from = chunk * MAX_PAYLOAD_CHUNK;
            int to = Math.min(payload.length, from + MAX_PAYLOAD_CHUNK);
            CatalogKey key = new CatalogKey(base.kind, base.parentId, base.objectId, base.version,
                    base.ordinal, chunk);
            records.add(new CatalogRecord(encodeKey(key), Arrays.copyOfRange(payload, from, to)));
        }
        return records;
    }

    /**
     * 校验当前状态后推进数据字典状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param version 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param records 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return {@code commitRecord} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    private static CatalogRecord commitRecord(DictionaryVersion version, List<CatalogRecord> records) {
        return commitRecord(version, records, CatalogEntityKind.CATALOG_COMMIT);
    }

    /** 以调用方指定的 commit kind 对普通 mutation 或 baseline records 统一做数量与 SHA-256 封口。 */
    private static CatalogRecord commitRecord(DictionaryVersion version, List<CatalogRecord> records,
                                              CatalogEntityKind commitKind) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        MessageDigest digest = sha256();
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        for (CatalogRecord record : records) {
            updateDigest(digest, record);
        }
        ByteBuffer payload = ByteBuffer.allocate(Integer.BYTES + HASH_BYTES).order(ByteOrder.BIG_ENDIAN);
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        payload.putInt(records.size()).put(digest.digest());
        CatalogKey key = new CatalogKey(commitKind, 0, version.value(),
                version.value(), 0, 0);
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return new CatalogRecord(encodeKey(key), payload.array());
    }

    private static void validateManifest(List<CatalogRecord> records, CatalogKey commitKey,
                                         CatalogEntityKind expectedCommitKind) {
        CatalogRecord commit = records.getLast();
        if (commitKey.kind != expectedCommitKind || commitKey.parentId != 0
                || commitKey.objectId != commitKey.version || commitKey.ordinal != 0 || commitKey.chunk != 0
                || commit.payload().length != Integer.BYTES + HASH_BYTES) {
            throw new DictionaryCatalogCorruptionException("invalid dictionary commit manifest shape");
        }
        ByteBuffer payload = ByteBuffer.wrap(commit.payload()).order(ByteOrder.BIG_ENDIAN);
        int expectedCount = payload.getInt();
        byte[] expectedHash = new byte[HASH_BYTES];
        payload.get(expectedHash);
        if (expectedCount != records.size() - 1 || expectedCount <= 0) {
            throw new DictionaryCatalogCorruptionException("dictionary commit record count mismatch");
        }
        MessageDigest digest = sha256();
        for (int i = 0; i < records.size() - 1; i++) {
            updateDigest(digest, records.get(i));
        }
        if (!Arrays.equals(expectedHash, digest.digest())) {
            throw new DictionaryCatalogCorruptionException("dictionary commit manifest hash mismatch");
        }
    }

    /**
     * 校验输入与当前状态后修改数据字典领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param digest 本次字典编码使用的摘要累加器；不得为 {@code null}，调用前必须处于对应 catalog 条目的计算上下文
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     */
    private static void updateDigest(MessageDigest digest, CatalogRecord record) {
        byte[] key = record.key();
        byte[] payload = record.payload();
        digest.update(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(key.length).array());
        digest.update(key);
        digest.update(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(payload.length).array());
        digest.update(payload);
    }

    private static byte[] encodeKey(CatalogKey key) {
        return ByteBuffer.allocate(KEY_BYTES).order(ByteOrder.BIG_ENDIAN)
                .put((byte) key.kind.stableCode()).putLong(key.parentId).putLong(key.objectId)
                .putLong(key.version).putInt(key.ordinal).putInt(key.chunk).array();
    }

    private static CatalogKey decodeKey(byte[] bytes) {
        return tryDecodeKey(bytes).orElseThrow(() -> new DictionaryCatalogCorruptionException(
                "invalid dictionary catalog key length/fields"));
    }

    /**
     * 从稳定表示解码数据字典领域值；先校验边界、标识与长度，损坏输入以领域异常拒绝。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param bytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return {@code tryDecodeKey} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    private static Optional<CatalogKey> tryDecodeKey(byte[] bytes) {
        try {
            // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
            if (bytes.length != KEY_BYTES) {
                return Optional.empty();
            }
            // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
            ByteBuffer key = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
            CatalogEntityKind kind = CatalogEntityKind.fromStableCode(Byte.toUnsignedInt(key.get()));
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            CatalogKey decoded = new CatalogKey(kind, key.getLong(), key.getLong(), key.getLong(),
                    key.getInt(), key.getInt());
            if (decoded.objectId < 0 || decoded.parentId < 0 || decoded.version <= 0
                    || decoded.ordinal < 0 || decoded.chunk < 0) {
                return Optional.empty();
            }
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            return Optional.of(decoded);
        } catch (RuntimeException ignored) {
            return Optional.empty();
        }
    }

    private static byte[] join(List<Fragment> fragments) {
        fragments.sort(Comparator.comparingInt(Fragment::chunk));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int expected = 0; expected < fragments.size(); expected++) {
            Fragment fragment = fragments.get(expected);
            if (fragment.chunk != expected || fragment.payload.length > MAX_PAYLOAD_CHUNK) {
                throw new DictionaryCatalogCorruptionException("dictionary payload chunk gap/overflow");
            }
            out.writeBytes(fragment.payload);
        }
        return out.toByteArray();
    }

    /**
     * 把调用方领域值编码为数据字典的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code encodeSchema} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    private static byte[] encodeSchema(SchemaDefinition schema) {
        return write(out -> {
            out.writeLong(schema.id().value());
            writeName(out, schema.name());
            out.writeInt(schema.defaultCharsetId());
            out.writeInt(schema.defaultCollationId());
            out.writeLong(schema.version().value());
        });
    }

    private static SchemaDefinition decodeSchema(byte[] payload) {
        return read(payload, in -> new SchemaDefinition(SchemaId.of(in.readLong()), readName(in), in.readInt(),
                in.readInt(), DictionaryVersion.of(in.readLong())));
    }

    /**
     * 把调用方领域值编码为数据字典的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * @param table 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code encodeTable} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    private static byte[] encodeTable(TableDefinition table) {
        return write(out -> {
            out.writeInt(TABLE_PAYLOAD_MAGIC);
            out.writeInt(TABLE_PAYLOAD_VERSION);
            out.writeLong(table.id().value());
            out.writeLong(table.schemaId().value());
            writeName(out, table.name());
            out.writeLong(table.version().value());
            out.writeByte(tableStateCode(table.state()));
            writeString(out, table.options().comment());
            out.writeInt(table.options().defaultCharsetId());
            out.writeInt(table.options().defaultCollationId());
            out.writeInt(table.columns().size());
            out.writeInt(table.indexes().size());
            out.writeBoolean(table.storageBinding().isPresent());
            if (table.storageBinding().isPresent()) {
                TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                        new DictionaryCatalogCorruptionException("present table binding unexpectedly missing"));
                out.writeInt(binding.spaceId().value());
                writeString(out, binding.path().toString());
                out.writeInt(binding.indexes().size());
                for (IndexStorageBinding index : binding.indexes()) {
                    out.writeLong(index.indexId());
                    out.writeLong(index.rootPageId().pageNo().value());
                    out.writeInt(index.rootLevel());
                    writeSegment(out, index.leafSegment());
                    writeSegment(out, index.nonLeafSegment());
                }
                out.writeByte(binding.lobSegment().isPresent() ? 1 : 0);
                if (binding.lobSegment().isPresent()) {
                    writeSegment(out, binding.lobSegment().orElseThrow(() ->
                            new DictionaryCatalogCorruptionException("present LOB segment unexpectedly missing")));
                }
                // metadata-only DDL 必须保留物理 record schema version，不能再从新 DD version 推导。
                out.writeLong(binding.rowFormatVersion());
            }
        });
    }

    /**
     * 从稳定表示解码数据字典领域值；先校验边界、标识与长度，损坏输入以领域异常拒绝。
     *
     * @param payload 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return {@code decodeTable} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     */
    private static TableRoot decodeTable(byte[] payload) {
        return read(payload, in -> {
            in.mark(payload.length);
            int prefix = in.readInt();
            boolean current = prefix == TABLE_PAYLOAD_MAGIC;
            if (current) {
                if (in.readInt() != TABLE_PAYLOAD_VERSION) {
                    throw new DictionaryCatalogCorruptionException(
                            "unsupported table payload format");
                }
            } else {
                in.reset();
            }
            TableId id = TableId.of(in.readLong());
            SchemaId schemaId = SchemaId.of(in.readLong());
            ObjectName name = readName(in);
            DictionaryVersion version = DictionaryVersion.of(in.readLong());
            int stateCode = in.readUnsignedByte();
            Optional<TableOptions> options = current
                    ? Optional.of(new TableOptions(readString(in), in.readInt(), in.readInt()))
                    : Optional.empty();
            int columnCount = in.readInt();
            int indexCount = in.readInt();
            if (columnCount <= 0 || indexCount <= 0) {
                throw new DictionaryCatalogCorruptionException("table child counts must be positive");
            }
            Optional<TableStorageBinding> binding = Optional.empty();
            if (in.readBoolean()) {
                SpaceId spaceId = SpaceId.of(in.readInt());
                Path path = Path.of(readString(in));
                int bindingCount = in.readInt();
                if (bindingCount != indexCount) {
                    throw new DictionaryCatalogCorruptionException("table storage binding index count mismatch");
                }
                List<IndexStorageBinding> bindings = new ArrayList<>(bindingCount);
                for (int i = 0; i < bindingCount; i++) {
                    long indexId = in.readLong();
                    PageId root = PageId.of(spaceId, PageNo.of(in.readLong()));
                    int level = in.readInt();
                    bindings.add(new IndexStorageBinding(indexId, root, level,
                            readSegment(in, spaceId), readSegment(in, spaceId)));
                }
                // 扩展前的 catalog 在最后一个 index binding 后立即 EOF；这是唯一兼容判据，不能按 DD version 猜测。
                Optional<SegmentRef> lobSegment = Optional.empty();
                long rowFormatVersion = version.value();
                if (in.available() > 0) {
                    int hasLobSegment = in.readUnsignedByte();
                    if (hasLobSegment > 1) {
                        throw new DictionaryCatalogCorruptionException(
                                "unknown table LOB segment flag: " + hasLobSegment);
                    }
                    if (hasLobSegment == 1) {
                        lobSegment = Optional.of(readSegment(in, spaceId));
                    }
                    // LOB 扩展版 payload 到此 EOF；新版本再追加固定 long，形状之外的尾部一律视为损坏。
                    if (in.available() == Long.BYTES) {
                        rowFormatVersion = in.readLong();
                    } else if (in.available() != 0) {
                        throw new DictionaryCatalogCorruptionException(
                                "invalid table row-format version tail length: " + in.available());
                    }
                }
                binding = Optional.of(new TableStorageBinding(
                        id.value(), spaceId, path, rowFormatVersion, bindings, lobSegment));
            }
            return new TableRoot(id, schemaId, name, version, tableStateFromCode(stateCode),
                    columnCount, indexCount, binding, options);
        });
    }

    private static int tableStateCode(TableState state) {
        return switch (state) {
            case ACTIVE -> 0;
            case DROP_PENDING -> 1;
            case DROPPED -> 2;
            case DISCARD_PENDING -> 3;
            case DISCARDED -> 4;
            case IMPORT_PENDING -> 5;
        };
    }

    private static TableState tableStateFromCode(int code) {
        return switch (code) {
            case 0 -> TableState.ACTIVE;
            case 1 -> TableState.DROP_PENDING;
            case 2 -> TableState.DROPPED;
            case 3 -> TableState.DISCARD_PENDING;
            case 4 -> TableState.DISCARDED;
            case 5 -> TableState.IMPORT_PENDING;
            default -> throw new DictionaryCatalogCorruptionException("unknown table state code: " + code);
        };
    }

    private static void writeSegment(DataOutputStream out, SegmentRef segment) throws IOException {
        out.writeInt(segment.inodeSlot());
        out.writeLong(segment.segmentId().value());
    }

    private static SegmentRef readSegment(DataInputStream in, SpaceId spaceId) throws IOException {
        return new SegmentRef(spaceId, in.readInt(), SegmentId.of(in.readLong()));
    }

    /**
     * 把调用方领域值编码为数据字典的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * @param column 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code encodeColumn} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    private static byte[] encodeColumn(ColumnDefinition column) {
        return write(out -> {
            out.writeInt(COLUMN_PAYLOAD_MAGIC);
            out.writeInt(COLUMN_PAYLOAD_VERSION);
            out.writeLong(column.columnId());
            writeName(out, column.name());
            out.writeInt(column.ordinal());
            ColumnTypeDefinition type = column.type();
            out.writeInt(type.typeId().stableCode());
            out.writeBoolean(type.unsigned());
            out.writeBoolean(type.nullable());
            out.writeInt(type.length());
            out.writeInt(type.scale());
            out.writeInt(type.charsetId());
            out.writeInt(type.collationId());
            out.writeInt(type.symbols().size());
            for (String symbol : type.symbols()) {
                writeString(out, symbol);
            }
            out.writeByte(columnDefaultCode(column.defaultDefinition().kind()));
            if (column.defaultDefinition().constantLiteral().isPresent()) {
                writeString(out, column.defaultDefinition().constantLiteral().orElseThrow());
            }
        });
    }

    /**
     * 从稳定表示解码数据字典领域值；先校验边界、标识与长度，损坏输入以领域异常拒绝。
     *
     * @param payload 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return {@code decodeColumn} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     */
    private static ColumnDefinition decodeColumn(byte[] payload) {
        return read(payload, in -> {
            in.mark(payload.length);
            int prefix = in.readInt();
            boolean current = prefix == COLUMN_PAYLOAD_MAGIC;
            if (current) {
                if (in.readInt() != COLUMN_PAYLOAD_VERSION) {
                    throw new DictionaryCatalogCorruptionException(
                            "unsupported column payload format");
                }
            } else {
                in.reset();
            }
            long id = in.readLong();
            ObjectName name = readName(in);
            int ordinal = in.readInt();
            DictionaryTypeId typeId = DictionaryTypeId.fromStableCode(in.readInt());
            boolean unsigned = in.readBoolean();
            boolean nullable = in.readBoolean();
            int length = in.readInt();
            int scale = in.readInt();
            int charset = in.readInt();
            int collation = in.readInt();
            int symbolCount = in.readInt();
            if (symbolCount < 0 || symbolCount > 65_535) {
                throw new DictionaryCatalogCorruptionException("invalid dictionary symbol count: " + symbolCount);
            }
            List<String> symbols = new ArrayList<>(symbolCount);
            for (int i = 0; i < symbolCount; i++) {
                symbols.add(readString(in));
            }
            ColumnTypeDefinition type = new ColumnTypeDefinition(
                    typeId, unsigned, nullable, length, scale, charset, collation, symbols);
            ColumnDefaultDefinition defaultDefinition = current
                    ? readColumnDefault(in) : nullable
                    ? ColumnDefaultDefinition.implicitNull() : ColumnDefaultDefinition.required();
            return new ColumnDefinition(id, name, type, ordinal, defaultDefinition);
        });
    }

    private static int columnDefaultCode(ColumnDefaultDefinition.Kind kind) {
        return switch (kind) {
            case REQUIRED -> 1;
            case IMPLICIT_NULL -> 2;
            case CONSTANT -> 3;
        };
    }

    private static ColumnDefaultDefinition readColumnDefault(DataInputStream in) throws IOException {
        return switch (in.readUnsignedByte()) {
            case 1 -> ColumnDefaultDefinition.required();
            case 2 -> ColumnDefaultDefinition.implicitNull();
            case 3 -> ColumnDefaultDefinition.constant(readString(in));
            default -> throw new DictionaryCatalogCorruptionException(
                    "unknown column default code");
        };
    }

    /**
     * 把调用方领域值编码为数据字典的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * @param index 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code encodeIndex} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    private static byte[] encodeIndex(IndexDefinition index) {
        return write(out -> {
            out.writeLong(index.id().value());
            writeName(out, index.name());
            out.writeBoolean(index.unique());
            out.writeBoolean(index.clustered());
            out.writeInt(index.keyParts().size());
            for (IndexKeyPart part : index.keyParts()) {
                out.writeLong(part.columnId());
                out.writeByte(part.order().ordinal());
                out.writeInt(part.prefixBytes());
            }
        });
    }

    /**
     * 从稳定表示解码数据字典领域值；先校验边界、标识与长度，损坏输入以领域异常拒绝。
     *
     * @param payload 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return {@code decodeIndex} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     */
    private static IndexDefinition decodeIndex(byte[] payload) {
        return read(payload, in -> {
            IndexId id = IndexId.of(in.readLong());
            ObjectName name = readName(in);
            boolean unique = in.readBoolean();
            boolean clustered = in.readBoolean();
            int partCount = in.readInt();
            if (partCount <= 0 || partCount > 1024) {
                throw new DictionaryCatalogCorruptionException("invalid index key part count: " + partCount);
            }
            List<IndexKeyPart> parts = new ArrayList<>(partCount);
            for (int i = 0; i < partCount; i++) {
                long columnId = in.readLong();
                int order = in.readUnsignedByte();
                if (order >= IndexOrder.values().length) {
                    throw new DictionaryCatalogCorruptionException("unknown index order code: " + order);
                }
                parts.add(new IndexKeyPart(columnId, IndexOrder.values()[order], in.readInt()));
            }
            return new IndexDefinition(id, name, unique, clustered, parts);
        });
    }

    private static void writeName(DataOutputStream out, ObjectName name) throws IOException {
        writeString(out, name.displayName());
        writeString(out, name.canonicalName());
    }

    /**
     * 根据调用参数创建或转换 {@code readName} 返回的 {@code ObjectName}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param in 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @return {@code readName} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     * @throws DictionaryCatalogCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private static ObjectName readName(DataInputStream in) throws IOException {
        String display = readString(in);
        String persistedCanonical = readString(in);
        ObjectName name = ObjectName.of(display);
        if (!name.canonicalName().equals(persistedCanonical)) {
            throw new DictionaryCatalogCorruptionException("dictionary name canonical form mismatch: " + display);
        }
        return name;
    }

    /**
     * 校验输入与当前状态后修改数据字典领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param out 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param value 传给 {@code writeString} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     * @throws DictionaryCatalogCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_BYTES) {
            throw new DictionaryCatalogCorruptionException("dictionary string exceeds codec bound");
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    /**
     * 根据调用参数创建或转换 {@code readString} 返回的 {@code String}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param in 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @return {@code readString} 生成的非空文本表示；字符顺序保持 SQL、标识符或诊断格式约定，无结果时返回空串而非 {@code null}
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     * @throws DictionaryCatalogCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_STRING_BYTES) {
            throw new DictionaryCatalogCorruptionException("invalid dictionary string length: " + length);
        }
        byte[] bytes = in.readNBytes(length);
        if (bytes.length != length) {
            throw new DictionaryCatalogCorruptionException("truncated dictionary string");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 根据调用参数创建或转换 {@code write} 返回的 {@code byte[]}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param writer 由组合根提供的 {@code IoWriter} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code write} 调用
     * @return {@code write} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     * @throws DictionaryCatalogCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private static byte[] write(IoWriter writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                writer.write(out);
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new DictionaryCatalogCorruptionException("encode dictionary catalog payload failed", e);
        }
    }

    /**
     * 根据调用参数创建或转换 {@code read} 返回的 {@code T}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param payload 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param reader 由组合根提供的 {@code IoReader<T>} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code read} 调用
     * @param <T> 调用方提供的类型参数，必须满足声明的上界约束
     * @return {@code read} 返回的已装载泛型值或受控迭代视图；成功时不为 {@code null}，元素所有权遵循调用方资源契约
     * @throws DictionaryCatalogCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private static <T> T read(byte[] payload, IoReader<T> reader) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            T value = reader.read(in);
            if (in.available() != 0) {
                throw new DictionaryCatalogCorruptionException("dictionary payload has trailing bytes");
            }
            return value;
        } catch (IOException e) {
            throw new DictionaryCatalogCorruptionException("decode dictionary catalog payload failed", e);
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new DictionaryCatalogCorruptionException("JVM does not provide SHA-256", e);
        }
    }

    /**
     * 封装数据字典持久编解码使用的 {@code DecodedMutation}；键、分片和根对象在创建时保持版本与校验摘要一致，禁止发布半解码字典对象。
     *
     * @param version 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param schemas 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param tables 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    record DecodedMutation(DictionaryVersion version, List<SchemaDefinition> schemas,
                           List<TableDefinition> tables, Set<TableId> legacyOptionTables) {
        DecodedMutation {
            schemas = List.copyOf(schemas);
            tables = List.copyOf(tables);
            legacyOptionTables = Set.copyOf(legacyOptionTables);
        }
    }

    /** schema 与 table 聚合解码的内部结果，供 mutation/baseline 两条批次协议共用。 */
    private record DecodedObjects(List<SchemaDefinition> schemas, List<TableDefinition> tables,
                                  Set<TableId> legacyOptionTables) {
    }

    /** column key ordinal 与 payload 定义的配对，用于排序并拒绝持久 key/payload 身份分裂。 */
    private record OrderedColumn(int ordinal, ColumnDefinition column) {
    }

    /** index 没有自身 ordinal 字段，必须以 catalog key ordinal 恢复表聚合中的声明顺序。 */
    private record OrderedIndex(int ordinal, IndexDefinition index) {
    }

    /**
     * 封装数据字典持久编解码使用的 {@code CatalogKey}；键、分片和根对象在创建时保持版本与校验摘要一致，禁止发布半解码字典对象。
     *
     * @param kind 选择 {@code 构造} 分支的 {@code CatalogEntityKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param parentId 参与 {@code 构造} 的原始数值身份 {@code parentId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param objectId 参与 {@code 构造} 的原始数值身份 {@code objectId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param version 参与 {@code 构造} 的单调版本值 {@code version}；必须非负，回退或与权威快照冲突时拒绝
     * @param ordinal 参与 {@code 构造} 的零基位置 {@code ordinal}；必须非负且小于所属页面、集合或持久结构的容量
     * @param chunk 参与 {@code 构造} 的零基位置 {@code chunk}；必须非负且小于所属页面、集合或持久结构的容量
     */
    private record CatalogKey(CatalogEntityKind kind, long parentId, long objectId, long version,
                              int ordinal, int chunk) {
    }

    /**
     * 封装数据字典持久编解码使用的 {@code GroupKey}；键、分片和根对象在创建时保持版本与校验摘要一致，禁止发布半解码字典对象。
     *
     * @param kind 选择 {@code 构造} 分支的 {@code CatalogEntityKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param parentId 参与 {@code 构造} 的原始数值身份 {@code parentId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param objectId 参与 {@code 构造} 的原始数值身份 {@code objectId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param version 参与 {@code 构造} 的单调版本值 {@code version}；必须非负，回退或与权威快照冲突时拒绝
     * @param ordinal 参与 {@code 构造} 的零基位置 {@code ordinal}；必须非负且小于所属页面、集合或持久结构的容量
     */
    private record GroupKey(CatalogEntityKind kind, long parentId, long objectId, long version, int ordinal) {
    }

    /**
     * 封装数据字典持久编解码使用的 {@code Fragment}；键、分片和根对象在创建时保持版本与校验摘要一致，禁止发布半解码字典对象。
     *
     * @param chunk 参与 {@code 构造} 的零基位置 {@code chunk}；必须非负且小于所属页面、集合或持久结构的容量
     * @param payload 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     */
    private record Fragment(int chunk, byte[] payload) {
    }

    /**
     * 封装数据字典持久编解码使用的 {@code TableRoot}；键、分片和根对象在创建时保持版本与校验摘要一致，禁止发布半解码字典对象。
     *
     * @param id 参与 {@code 构造} 的稳定领域标识 {@code TableId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param schemaId 参与 {@code 构造} 的稳定领域标识 {@code SchemaId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param version 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param state 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param columnCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param indexCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param storageBinding 可选的 {@code storageBinding}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     */
    private record TableRoot(TableId id, SchemaId schemaId, ObjectName name, DictionaryVersion version,
                             TableState state, int columnCount, int indexCount,
                             Optional<TableStorageBinding> storageBinding,
                             Optional<TableOptions> options) {
    }

    /**
     * 定义数据字典的 {@code IoWriter} 稳定协作契约；调用方只依赖该接口，不读取实现内部状态或资源。
     */
    @FunctionalInterface
    private interface IoWriter {
        /**
         * 校验输入与当前状态后修改数据字典领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
         *
         * @param out 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
         * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
         */
        void write(DataOutputStream out) throws IOException;
    }

    /**
     * 定义数据字典的 {@code IoReader} 稳定协作契约；调用方只依赖该接口，不读取实现内部状态或资源。
     */
    @FunctionalInterface
    private interface IoReader<T> {
        /**
         * 定位并读取数据字典领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
         *
         * @param in 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
         * @return {@code read} 返回的已装载泛型值或受控迭代视图；成功时不为 {@code null}，元素所有权遵循调用方资源契约
         * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
         */
        T read(DataInputStream in) throws IOException;
    }
}
