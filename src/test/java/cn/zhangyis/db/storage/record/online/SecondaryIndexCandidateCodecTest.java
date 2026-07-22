package cn.zhangyis.db.storage.record.online;

import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexCandidate;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.SecondaryIndexLayout;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Online DML candidate 必须只编码目标 physical entry，并能供 final reconciliation 稳定解码。 */
class SecondaryIndexCandidateCodecTest {

    /** INSERT/DELETE 分别只带 after/before，UPDATE 同时保存两侧完整 physical identity。 */
    @Test
    void shouldRoundTripInsertUpdateAndDelete() {
        SecondaryIndexCandidateCodec codec = codec();
        LogicalRecord before = row(7, "alpha", "payload-a");
        LogicalRecord after = row(7, "beta", "payload-b");

        OnlineIndexCandidate insert = codec.decode(codec.encodeInsert(after).orElseThrow());
        OnlineIndexCandidate update = codec.decode(codec.encodeUpdate(before, after).orElseThrow());
        OnlineIndexCandidate delete = codec.decode(codec.encodeDelete(before).orElseThrow());

        assertTrue(insert.beforeEntry().isEmpty());
        assertEquals(List.of(new ColumnValue.StringValue("beta"), new ColumnValue.IntValue(7)),
                insert.afterEntry().orElseThrow().columnValues());
        assertEquals("alpha", ((ColumnValue.StringValue) update.beforeEntry()
                .orElseThrow().columnValues().getFirst()).value());
        assertEquals("beta", ((ColumnValue.StringValue) update.afterEntry()
                .orElseThrow().columnValues().getFirst()).value());
        assertTrue(delete.afterEntry().isEmpty());
    }

    /** UPDATE 只改变非索引 payload 时 physical key 不变，不应扩大 row log 或提交 force 集合。 */
    @Test
    void shouldSkipUpdateWhenPhysicalEntryDoesNotChange() {
        SecondaryIndexCandidateCodec codec = codec();

        assertTrue(codec.encodeUpdate(row(8, "same", "before"),
                row(8, "same", "after")).isEmpty());
    }

    /** 相同 logical secondary 值但主键不同仍是不同 physical entry，编码必须保留完整聚簇后缀。 */
    @Test
    void shouldIncludeClusterKeyInPhysicalEntry() {
        SecondaryIndexCandidateCodec codec = codec();

        byte[] left = codec.encodeInsert(row(9, "same", "x")).orElseThrow();
        byte[] right = codec.encodeInsert(row(10, "same", "x")).orElseThrow();

        assertTrue(!java.util.Arrays.equals(left, right));
        assertEquals(new ColumnValue.IntValue(9), codec.decode(left).afterEntry()
                .orElseThrow().columnValues().getLast());
        assertEquals(new ColumnValue.IntValue(10), codec.decode(right).afterEntry()
                .orElseThrow().columnValues().getLast());
    }

    private static SecondaryIndexCandidateCodec codec() {
        TableSchema table = new TableSchema(7, List.of(
                column(0, "id", ColumnType.bigint(false, false)),
                column(1, "code", ColumnType.varchar(64, false)),
                column(2, "payload", ColumnType.varchar(128, true))), true);
        SecondaryIndexLayout layout = SecondaryIndexLayout.create(table,
                new IndexKeyDef(20, List.of(new KeyPartDef(new ColumnId(1), KeyOrder.ASC, 0))),
                new IndexKeyDef(10, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0))));
        return new SecondaryIndexCandidateCodec(layout, new TypeCodecRegistry());
    }

    private static LogicalRecord row(long id, String code, String payload) {
        return new LogicalRecord(7, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue(code), new ColumnValue.StringValue(payload)),
                false, RecordType.CONVENTIONAL, null);
    }

    private static ColumnDef column(int ordinal, String name, ColumnType type) {
        return new ColumnDef(new ColumnId(ordinal), name, type, ordinal);
    }
}
