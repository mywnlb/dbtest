package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnDefaultDefinition;
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
import cn.zhangyis.db.dd.domain.TableOptions;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证 DDL schema digest 的稳定字节协议。测试只通过公开领域输入观察结果，不复用生产 codec 拼装期望值，
 * 防止 encoder 与测试共享同一个错误。
 */
class DdlSchemaDigestServiceTest {

    private final DdlSchemaDigestService service = new DdlSchemaDigestService();

    /** 固定复杂聚合的 SHA-256 必须跨进程稳定，后续格式演进只能新增 canonical version。 */
    @Test
    void computesTableSchemaV1GoldenDigest() {
        DdlSchemaDigest digest = service.digest(schema("Sales"), table("Orders", TableState.ACTIVE), 9);

        assertEquals(DdlDigestAlgorithm.SHA_256, digest.algorithm());
        assertEquals(DdlSchemaCanonicalFormat.TABLE_SCHEMA_V1, digest.canonicalFormat());
        assertEquals(
                "419f91ef56ea0dc95a00c6fa3f7cff451843fbe99c2e0310d7badc09f84668e8",
                HexFormat.of().formatHex(digest.bytes()));
    }

    /** ObjectName 只取 canonical identity；state、path、root、segment 与 LOB segment 均不得污染逻辑 digest。 */
    @Test
    void ignoresDisplayCaseLifecycleAndPhysicalBinding() {
        TableDefinition first = table("Orders", TableState.ACTIVE);
        TableDefinition changedPhysical = withBinding(
                table("orders", TableState.DISCARD_PENDING), physicalBinding(7, 41, 9000));

        assertEquals(
                service.digest(schema("Sales"), first, 9),
                service.digest(schema("sales"), changedPhysical, 9));
    }

    /** table/schema/version/row-format/options/column/default/index/key-part 的任一语义变化都必须改变 digest。 */
    @Test
    void changesForEveryLogicalSchemaDimension() {
        SchemaDefinition schema = schema("sales");
        TableDefinition base = table("orders", TableState.ACTIVE);
        DdlSchemaDigest expected = service.digest(schema, base, 9);

        List<DdlSchemaDigest> mutations = List.of(
                service.digest(new SchemaDefinition(SchemaId.of(4), ObjectName.of("sales"), 45, 255,
                        DictionaryVersion.of(5)), withSchema(base, SchemaId.of(4)), 9),
                service.digest(schema("archive"), base, 9),
                service.digest(schema, withName(base, "orders_2026"), 9),
                service.digest(schema, withVersion(base, 10), 9),
                service.digest(schema, base, 10),
                service.digest(schema, withOptions(base, new TableOptions("changed", 45, 255)), 9),
                service.digest(schema, mutateColumn(base, 1, column -> new ColumnDefinition(
                        column.columnId(), ObjectName.of("status"), column.type(), column.ordinal(),
                        column.defaultDefinition())), 9),
                service.digest(schema, mutateColumn(base, 1, column -> new ColumnDefinition(
                        column.columnId(), column.name(), new ColumnTypeDefinition(DictionaryTypeId.SET,
                        false, true, 8, 0, 45, 255, List.of("new", "paid", "closed")),
                        column.ordinal(), column.defaultDefinition())), 9),
                service.digest(schema, mutateColumn(base, 1, column -> new ColumnDefinition(
                        column.columnId(), column.name(), column.type(), column.ordinal(),
                        ColumnDefaultDefinition.constant("'paid'"))), 9),
                service.digest(schema, mutateIndex(base, 1, index -> new IndexDefinition(
                        index.id(), ObjectName.of("idx_state_v2"), index.unique(), index.clustered(),
                        index.keyParts())), 9),
                service.digest(schema, mutateIndex(base, 1, index -> new IndexDefinition(
                        index.id(), index.name(), true, index.clustered(), index.keyParts())), 9),
                service.digest(schema, mutateIndex(base, 1, index -> new IndexDefinition(
                        index.id(), index.name(), index.unique(), index.clustered(),
                        List.of(new IndexKeyPart(12, IndexOrder.ASC, 4)))), 9),
                service.digest(schema, mutateIndex(base, 1, index -> new IndexDefinition(
                        index.id(), index.name(), index.unique(), index.clustered(),
                        List.of(new IndexKeyPart(12, IndexOrder.DESC, 3)))), 9));

        for (DdlSchemaDigest mutation : mutations) {
            assertNotEquals(expected, mutation);
        }
    }

    /** index aggregate 顺序本身属于 schema；交换两个 secondary 即使定义集合相同也必须改变 digest。 */
    @Test
    void preservesIndexAggregateOrder() {
        TableDefinition base = tableWithTwoSecondaryIndexes();
        List<IndexDefinition> reordered = new ArrayList<>(base.indexes());
        IndexDefinition firstSecondary = reordered.remove(1);
        reordered.add(firstSecondary);
        TableDefinition changed = copy(base, base.schemaId(), base.name(), base.version(), base.state(),
                base.columns(), reordered, base.storageBinding(), base.options());

        assertNotEquals(
                service.digest(schema("sales"), base, 9),
                service.digest(schema("sales"), changed, 9));
    }

    /** Java String 中未配对 surrogate 不能由 UTF-8 replacement 静默改写后进入恢复证据。 */
    @Test
    void rejectsMalformedUtf16BeforeHashing() {
        TableDefinition malformed = withOptions(table("orders", TableState.ACTIVE),
                new TableOptions("broken-\uD800", 45, 255));

        assertThrows(DatabaseValidationException.class,
                () -> service.digest(schema("sales"), malformed, 9));
    }

    /** digest 值对象必须复制输入和输出数组，避免调用方在 map/set 或恢复比较期间篡改权威值。 */
    @Test
    void defensivelyCopiesDigestBytes() {
        byte[] bytes = new byte[32];
        bytes[0] = 7;
        DdlSchemaDigest digest = new DdlSchemaDigest(
                DdlDigestAlgorithm.SHA_256, DdlSchemaCanonicalFormat.TABLE_SCHEMA_V1, bytes);
        bytes[0] = 9;
        byte[] observed = digest.bytes();
        observed[0] = 11;

        byte[] expected = new byte[32];
        expected[0] = 7;
        assertArrayEquals(expected, digest.bytes());
        assertEquals(digest, new DdlSchemaDigest(
                DdlDigestAlgorithm.SHA_256, DdlSchemaCanonicalFormat.TABLE_SCHEMA_V1, expected));
    }

    private static SchemaDefinition schema(String name) {
        return new SchemaDefinition(SchemaId.of(3), ObjectName.of(name), 45, 255,
                DictionaryVersion.of(5));
    }

    private static TableDefinition table(String name, TableState state) {
        ColumnDefinition id = new ColumnDefinition(11, ObjectName.of("Id"),
                ColumnTypeDefinition.bigint(true, false), 0, ColumnDefaultDefinition.required());
        ColumnDefinition stateColumn = new ColumnDefinition(12, ObjectName.of("State"),
                new ColumnTypeDefinition(DictionaryTypeId.ENUM, false, true,
                        8, 0, 45, 255, List.of("new", "paid", "closed")), 1,
                ColumnDefaultDefinition.constant("'new'"));
        ColumnDefinition body = new ColumnDefinition(13, ObjectName.of("Body"),
                new ColumnTypeDefinition(DictionaryTypeId.TEXT, false, true,
                        65_535, 0, 45, 255, List.of()), 2,
                ColumnDefaultDefinition.implicitNull());
        IndexDefinition primary = new IndexDefinition(IndexId.of(21), ObjectName.of("PRIMARY"),
                true, true, List.of(new IndexKeyPart(11, IndexOrder.ASC, 0)));
        IndexDefinition secondary = new IndexDefinition(IndexId.of(22), ObjectName.of("idx_state"),
                false, false, List.of(new IndexKeyPart(12, IndexOrder.DESC, 4)));
        return new TableDefinition(TableId.of(7), SchemaId.of(3), ObjectName.of(name),
                DictionaryVersion.of(9), state, List.of(id, stateColumn, body),
                List.of(primary, secondary), Optional.of(physicalBinding(0, 64, 31)),
                new TableOptions("订单", 45, 255));
    }

    private static TableDefinition tableWithTwoSecondaryIndexes() {
        TableDefinition base = table("orders", TableState.ACTIVE);
        IndexDefinition body = new IndexDefinition(IndexId.of(23), ObjectName.of("idx_body"),
                false, false, List.of(new IndexKeyPart(13, IndexOrder.ASC, 8)));
        return copy(base, base.schemaId(), base.name(), base.version(), base.state(), base.columns(),
                List.of(base.indexes().get(0), base.indexes().get(1), body), Optional.empty(), base.options());
    }

    private static TableStorageBinding physicalBinding(int rootLevelDelta, long firstRootPage,
                                                       long firstSegmentId) {
        SpaceId spaceId = SpaceId.of(1024);
        return new TableStorageBinding(7, spaceId, Path.of("build/digest/table.ibd"), 9,
                List.of(
                        new IndexStorageBinding(21, PageId.of(spaceId, PageNo.of(firstRootPage)),
                                rootLevelDelta, segment(spaceId, 0, firstSegmentId),
                                segment(spaceId, 1, firstSegmentId + 1)),
                        new IndexStorageBinding(22, PageId.of(spaceId, PageNo.of(firstRootPage + 1)),
                                rootLevelDelta + 1, segment(spaceId, 2, firstSegmentId + 2),
                                segment(spaceId, 3, firstSegmentId + 3))),
                Optional.of(segment(spaceId, 4, firstSegmentId + 4)));
    }

    private static SegmentRef segment(SpaceId spaceId, int slot, long id) {
        return new SegmentRef(spaceId, slot, SegmentId.of(id));
    }

    private static TableDefinition mutateColumn(TableDefinition base, int ordinal,
                                                java.util.function.UnaryOperator<ColumnDefinition> mutation) {
        List<ColumnDefinition> columns = new ArrayList<>(base.columns());
        columns.set(ordinal, mutation.apply(columns.get(ordinal)));
        return copy(base, base.schemaId(), base.name(), base.version(), base.state(), columns,
                base.indexes(), base.storageBinding(), base.options());
    }

    private static TableDefinition mutateIndex(TableDefinition base, int ordinal,
                                               java.util.function.UnaryOperator<IndexDefinition> mutation) {
        List<IndexDefinition> indexes = new ArrayList<>(base.indexes());
        indexes.set(ordinal, mutation.apply(indexes.get(ordinal)));
        return copy(base, base.schemaId(), base.name(), base.version(), base.state(), base.columns(), indexes,
                Optional.empty(), base.options());
    }

    private static TableDefinition withBinding(TableDefinition base, TableStorageBinding binding) {
        return copy(base, base.schemaId(), base.name(), base.version(), base.state(), base.columns(),
                base.indexes(), Optional.of(binding), base.options());
    }

    private static TableDefinition withSchema(TableDefinition base, SchemaId schemaId) {
        return copy(base, schemaId, base.name(), base.version(), base.state(), base.columns(),
                base.indexes(), Optional.empty(), base.options());
    }

    private static TableDefinition withName(TableDefinition base, String name) {
        return copy(base, base.schemaId(), ObjectName.of(name), base.version(), base.state(), base.columns(),
                base.indexes(), base.storageBinding(), base.options());
    }

    private static TableDefinition withVersion(TableDefinition base, long version) {
        return copy(base, base.schemaId(), base.name(), DictionaryVersion.of(version), base.state(),
                base.columns(), base.indexes(), base.storageBinding(), base.options());
    }

    private static TableDefinition withOptions(TableDefinition base, TableOptions options) {
        return copy(base, base.schemaId(), base.name(), base.version(), base.state(), base.columns(),
                base.indexes(), base.storageBinding(), options);
    }

    private static TableDefinition copy(TableDefinition base, SchemaId schemaId, ObjectName name,
                                        DictionaryVersion version, TableState state,
                                        List<ColumnDefinition> columns, List<IndexDefinition> indexes,
                                        Optional<TableStorageBinding> binding, TableOptions options) {
        return new TableDefinition(base.id(), schemaId, name, version, state, columns, indexes, binding, options);
    }
}
