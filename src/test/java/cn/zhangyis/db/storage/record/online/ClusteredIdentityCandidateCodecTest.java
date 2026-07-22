package cn.zhangyis.db.storage.record.online;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
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

/** SHADOW candidate只编码完整聚簇identity，UPDATE不得改变v1禁止修改的主键。 */
class ClusteredIdentityCandidateCodecTest {

    @Test
    void roundTripsIdentityWithoutCopyingNonKeyPayload() {
        ClusteredIdentityCandidateCodec codec = codec();

        byte[] encoded = codec.encodeUpdate(row(7, "before"), row(7, "after"))
                .orElseThrow();

        assertEquals(List.of(new ColumnValue.IntValue(7)),
                codec.decode(encoded).values());
    }

    @Test
    void rejectsPrimaryKeyMutationBeforeJournalAppend() {
        ClusteredIdentityCandidateCodec codec = codec();

        assertThrows(cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                () -> codec.encodeUpdate(row(7, "before"), row(8, "after")));
    }

    private static ClusteredIdentityCandidateCodec codec() {
        TableSchema schema = new TableSchema(3, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.bigint(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(100, true), 1)), true);
        IndexKeyDef key = new IndexKeyDef(10,
                List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
        BTreeIndex clustered = new BTreeIndex(10,
                PageId.of(SpaceId.of(9), PageNo.of(17)), 0, key, schema, true);
        return new ClusteredIdentityCandidateCodec(clustered, new TypeCodecRegistry());
    }

    private static LogicalRecord row(long id, String payload) {
        return new LogicalRecord(3, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue(payload)), false, RecordType.CONVENTIONAL, null);
    }
}
