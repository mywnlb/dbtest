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
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
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
}
