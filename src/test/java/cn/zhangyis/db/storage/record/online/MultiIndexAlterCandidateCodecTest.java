package cn.zhangyis.db.storage.record.online;

import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterCandidate;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterIndexTarget;
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

/** 一个通用candidate必须按manifest ordinal封装所有ADD INDEX目标，不能覆盖单slot或拆成多次gate。 */
class MultiIndexAlterCandidateCodecTest {

    /** INSERT捕获全部目标；只改变code的UPDATE仅携带实际变化的第一个索引。 */
    @Test
    void encodesAllAffectedTargetsInManifestOrder() {
        MultiIndexAlterCandidateCodec codec = codec();

        OnlineAlterCandidate insert = codec.decode(codec.encodeInsert(
                row(7, "alpha", "p1")).orElseThrow());
        OnlineAlterCandidate update = codec.decode(codec.encodeUpdate(
                row(7, "alpha", "p1"), row(7, "beta", "p1")).orElseThrow());

        assertEquals(List.of(1, 3), insert.entries().stream()
                .map(entry -> entry.actionOrdinal()).toList());
        assertEquals(List.of(20L, 21L), insert.entries().stream()
                .map(entry -> entry.indexId()).toList());
        assertEquals(1, update.entries().size());
        assertEquals(20, update.entries().getFirst().indexId());
    }

    /** 所有目标physical key都未变化时不追加空candidate，避免无意义提交force。 */
    @Test
    void skipsUpdateWhenNoTargetEntryChanges() {
        MultiIndexAlterCandidateCodec codec = codec();

        assertTrue(codec.encodeUpdate(row(8, "same", "same"),
                row(8, "same", "same")).isEmpty());
    }

    private static MultiIndexAlterCandidateCodec codec() {
        TypeCodecRegistry registry = new TypeCodecRegistry();
        TableSchema table = schema();
        IndexKeyDef clustered = key(10, 0);
        return new MultiIndexAlterCandidateCodec(List.of(
                new OnlineAlterIndexTarget(1, 20,
                        new SecondaryIndexCandidateCodec(
                                SecondaryIndexLayout.create(table, key(20, 1), clustered), registry)),
                new OnlineAlterIndexTarget(3, 21,
                        new SecondaryIndexCandidateCodec(
                                SecondaryIndexLayout.create(table, key(21, 2), clustered), registry))));
    }

    private static TableSchema schema() {
        return new TableSchema(7, List.of(
                column(0, "id", ColumnType.bigint(false, false)),
                column(1, "code", ColumnType.varchar(64, false)),
                column(2, "payload", ColumnType.varchar(128, false))), true);
    }

    private static IndexKeyDef key(long id, int ordinal) {
        return new IndexKeyDef(id, List.of(
                new KeyPartDef(new ColumnId(ordinal), KeyOrder.ASC, 0)));
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
