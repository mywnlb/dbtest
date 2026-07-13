package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.CharsetId;
import cn.zhangyis.db.storage.record.schema.CollationId;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TemporalKind;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.record.type.UnsupportedColumnTypeException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SearchKeyComparator 保序比较：验证前缀索引（{@link KeyPartDef#prefixBytes()}&gt;0）只比列的前 N 字节，
 * 与 RecordComparator 保持同序（node pointer 侧不能与 leaf 侧排序漂移）。断言只看符号（&lt;0/0/&gt;0）。
 */
class SearchKeyComparatorTest {

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final SearchKeyComparator comparator = new SearchKeyComparator(registry);

    /** 单 name(VARCHAR) 列表。 */
    private static TableSchema nameSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "name", ColumnType.varchar(20, false), 0)));
    }

    private static IndexKeyDef namePrefixKeyDef(int prefixBytes) {
        return new IndexKeyDef(7L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, prefixBytes)));
    }

    private static IndexKeyDef nameKeyDef(KeyOrder order, int prefixBytes) {
        return new IndexKeyDef(7L, List.of(new KeyPartDef(new ColumnId(0), order, prefixBytes)));
    }

    private static SearchKey nameKey(String s) {
        return new SearchKey(List.of(new ColumnValue.StringValue(s)));
    }

    @Test
    void prefixComparesOnlyLeadingBytes() {
        TableSchema schema = nameSchema();
        IndexKeyDef kd = namePrefixKeyDef(3);
        assertEquals(0, comparator.compare(nameKey("application"), nameKey("apple"), kd, schema),
                "prefix(3) 'app' == 'app'");
        assertTrue(comparator.compare(nameKey("apple"), nameKey("banana"), kd, schema) < 0, "'app' < 'ban'");
        assertTrue(comparator.compare(nameKey("apricot"), nameKey("apple"), kd, schema) > 0, "'apr' > 'app'");
    }

    @Test
    void fullColumnComparedWhenNoPrefix() {
        TableSchema schema = nameSchema();
        IndexKeyDef kd = namePrefixKeyDef(0);
        assertTrue(comparator.compare(nameKey("application"), nameKey("apple"), kd, schema) > 0,
                "no prefix → full lexicographic: 'application' > 'apple'");
    }

    @Test
    void prefixLengthRejectedOnNonByteColumn() {
        TableSchema idSchema = new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0)));
        IndexKeyDef kd = namePrefixKeyDef(2);
        SearchKey a = new SearchKey(List.of(new ColumnValue.IntValue(1)));
        SearchKey b = new SearchKey(List.of(new ColumnValue.IntValue(2)));
        assertThrows(DatabaseValidationException.class, () -> comparator.compare(a, b, kd, idSchema),
                "prefix length on a numeric column must be rejected");
    }

    /** TIME/TIMESTAMP/YEAR 的编码自然序必须直接服务 node-pointer/search-key 比较。 */
    @Test
    void inlineTemporalTypesKeepBTreeSearchKeyOrder() {
        assertTemporalOrder(ColumnType.time(false), TemporalKind.TIME, -1, 1);
        assertTemporalOrder(ColumnType.timestamp(false), TemporalKind.TIMESTAMP, -1, 1);
        assertTemporalOrder(ColumnType.year(false), TemporalKind.YEAR, 1901, 2026);
    }

    /** BIT 的 unsigned byte 次序服务 B+Tree，且 bitstring 不能误用 byte-prefix index。 */
    @Test
    void bitTypeKeepsBTreeOrderAndRejectsPrefix() {
        TableSchema schema = new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "bits", ColumnType.bit(9, false), 0)));
        SearchKey lower = new SearchKey(List.of(
                new ColumnValue.BitValue(new byte[] {0x01, (byte) 0x80})));
        SearchKey higher = new SearchKey(List.of(
                new ColumnValue.BitValue(new byte[] {0x02, 0x00})));
        assertTrue(comparator.compare(lower, higher, nameKeyDef(KeyOrder.ASC, 0), schema) < 0);
        assertTrue(comparator.compare(lower, higher, nameKeyDef(KeyOrder.DESC, 0), schema) > 0);
        assertThrows(DatabaseValidationException.class,
                () -> comparator.compare(lower, higher, nameKeyDef(KeyOrder.ASC, 1), schema));
    }

    /** ENUM ordinal 与 SET bitmap 使用物理数值序，不能把 schema member 名称排序混入索引。 */
    @Test
    void enumAndSetKeepPhysicalBTreeOrder() {
        assertEnumeratedOrder(ColumnType.enumType(List.of("Z", "A"), false),
                new ColumnValue.EnumValue(1), new ColumnValue.EnumValue(2));
        assertEnumeratedOrder(ColumnType.setType(List.of("Z", "A", "M"), false),
                new ColumnValue.SetValue(1), new ColumnValue.SetValue(4));
    }

    @Test
    void charsetCollationOrderingMatrixKeepsDirectionNullAndPrefixSemantics() {
        ColumnType binary = ColumnType.varchar(20, true, CharsetId.UTF8, CollationId.BINARY);
        ColumnType caseInsensitive = ColumnType.varchar(
                20, true, CharsetId.UTF8, CollationId.UTF8_ASCII_CI);
        TableSchema binarySchema = new TableSchema(1, List.of(new ColumnDef(new ColumnId(0), "name", binary, 0)));
        TableSchema ciSchema = new TableSchema(1, List.of(new ColumnDef(new ColumnId(0), "name", caseInsensitive, 0)));
        SearchKey upper = nameKey("Apple");
        SearchKey lower = nameKey("apple");
        SearchKey nullKey = new SearchKey(List.of(ColumnValue.NullValue.INSTANCE));

        assertTrue(comparator.compare(upper, lower, nameKeyDef(KeyOrder.ASC, 0), binarySchema) < 0);
        assertTrue(comparator.compare(upper, lower, nameKeyDef(KeyOrder.DESC, 0), binarySchema) > 0);
        assertEquals(0, comparator.compare(upper, lower, nameKeyDef(KeyOrder.ASC, 0), ciSchema));
        assertEquals(0, comparator.compare(nameKey("Apple"), nameKey("apricot"),
                nameKeyDef(KeyOrder.ASC, 1), ciSchema), "ASCII-CI prefix(1) folds A/a before comparison");
        assertTrue(comparator.compare(nullKey, lower, nameKeyDef(KeyOrder.ASC, 0), ciSchema) < 0);
        assertTrue(comparator.compare(nullKey, lower, nameKeyDef(KeyOrder.DESC, 0), ciSchema) > 0);
    }

    /** Unicode V1 权重在 leaf/node 公用入口保持等价，并让 byte-prefix 停在完整 UTF-8 字符边界。 */
    @Test
    void unicodeWeightKeepsDirectionAndSafeUtf8Prefix() {
        ColumnType unicode = ColumnType.varchar(
                20, false, CharsetId.UTF8, CollationId.UTF8_UNICODE_CI_V1);
        TableSchema schema = new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "name", unicode, 0)));
        SearchKey accented = nameKey("Éclair");
        SearchKey decomposed = nameKey("e\u0301CLAIR");
        assertEquals(0, comparator.compare(accented, decomposed, nameKeyDef(KeyOrder.ASC, 0), schema));

        SearchKey eAcute = nameKey("éx");
        SearchKey eUpper = nameKey("Éy");
        assertEquals(0, comparator.compare(eAcute, eUpper, nameKeyDef(KeyOrder.ASC, 2), schema),
                "2-byte budget includes exactly one complete precomposed UTF-8 code point");
        assertTrue(comparator.compare(nameKey("é"), nameKey("f"), nameKeyDef(KeyOrder.ASC, 0), schema) < 0);
        assertTrue(comparator.compare(nameKey("é"), nameKey("f"), nameKeyDef(KeyOrder.DESC, 0), schema) > 0);
    }

    /** LOB 只允许显式 prefix，比较的是逻辑 payload 而不是 inline/external envelope tag。 */
    @Test
    void textPrefixWorksThroughSharedKeyComparatorAndJsonIsRejected() {
        ColumnType text = ColumnType.text(false);
        TableSchema textSchema = new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "body", text, 0)));
        assertEquals(0, comparator.compare(nameKey("apple"), nameKey("apricot"),
                nameKeyDef(KeyOrder.ASC, 2), textSchema));
        assertTrue(comparator.compare(nameKey("apple"), nameKey("apricot"),
                nameKeyDef(KeyOrder.ASC, 3), textSchema) < 0);
        assertThrows(UnsupportedColumnTypeException.class, () -> comparator.compare(
                nameKey("apple"), nameKey("apricot"), nameKeyDef(KeyOrder.ASC, 0), textSchema));

        TableSchema jsonSchema = new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "doc", ColumnType.json(false), 0)));
        assertThrows(UnsupportedColumnTypeException.class, () -> comparator.compare(
                nameKey("{\"a\":1}"), nameKey("{\"a\":2}"), nameKeyDef(KeyOrder.ASC, 1), jsonSchema));
    }

    /** 用单列 schema 驱动真实 SearchKey 编码，分别复核 ASC 与 DESC 符号。 */
    private void assertTemporalOrder(ColumnType type, TemporalKind kind, long lower, long higher) {
        TableSchema schema = new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "temporal_key", type, 0)));
        SearchKey left = new SearchKey(List.of(new ColumnValue.TemporalValue(kind, lower)));
        SearchKey right = new SearchKey(List.of(new ColumnValue.TemporalValue(kind, higher)));
        assertTrue(comparator.compare(left, right, nameKeyDef(KeyOrder.ASC, 0), schema) < 0);
        assertTrue(comparator.compare(left, right, nameKeyDef(KeyOrder.DESC, 0), schema) > 0);
    }

    /** 复用单列 schema 校验两种 enumerated codec 的 ASC/DESC 与 prefix 拒绝。 */
    private void assertEnumeratedOrder(ColumnType type, ColumnValue lower, ColumnValue higher) {
        TableSchema schema = new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "enumerated_key", type, 0)));
        SearchKey left = new SearchKey(List.of(lower));
        SearchKey right = new SearchKey(List.of(higher));
        assertTrue(comparator.compare(left, right, nameKeyDef(KeyOrder.ASC, 0), schema) < 0);
        assertTrue(comparator.compare(left, right, nameKeyDef(KeyOrder.DESC, 0), schema) > 0);
        assertThrows(DatabaseValidationException.class,
                () -> comparator.compare(left, right, nameKeyDef(KeyOrder.ASC, 1), schema));
    }
}
