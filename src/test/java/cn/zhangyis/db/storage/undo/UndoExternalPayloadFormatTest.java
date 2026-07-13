package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 外置 undo 根描述符稳定性与写前页数上限测试。 */
class UndoExternalPayloadFormatTest {

    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);

    /** descriptor 是 root record 与 payload 链之间的持久协议，字段和值必须严格往返。 */
    @Test
    void descriptorRoundTripsStableBinaryFormat() {
        UndoPayloadDescriptor descriptor = new UndoPayloadDescriptor(
                UndoRecordType.UPDATE_ROW, TransactionId.of(7), UndoNo.of(11),
                PageNo.of(99), 16_500, 2, 0xFEDC_BA98L);

        byte[] encoded = descriptor.encode();
        assertEquals(UndoPayloadDescriptor.BYTES, encoded.length);
        assertEquals(UndoPayloadDescriptor.TAG, encoded[0] & 0xFF);
        assertEquals(UndoPayloadDescriptor.VERSION, encoded[1] & 0xFF);
        assertEquals(descriptor, UndoPayloadDescriptor.decode(encoded));
        assertArrayEquals(encoded, UndoPayloadDescriptor.decode(encoded).encode());
    }

    /** 未知版本或非精确长度不能被宽松接受，避免升级后误读旧链。 */
    @Test
    void descriptorRejectsVersionAndLengthCorruption() {
        byte[] encoded = new UndoPayloadDescriptor(
                UndoRecordType.INSERT_ROW, TransactionId.of(7), UndoNo.of(1),
                PageNo.of(64), 128, 1, 1).encode();
        encoded[1] = 2;

        assertThrows(UndoLogFormatException.class, () -> UndoPayloadDescriptor.decode(encoded));
        assertThrows(UndoLogFormatException.class,
                () -> UndoPayloadDescriptor.decode(new byte[UndoPayloadDescriptor.BYTES - 1]));
        assertThrows(DatabaseValidationException.class,
                () -> new UndoPayloadDescriptor(UndoRecordType.INSERT_ROW, TransactionId.of(7), UndoNo.of(1),
                        PageNo.of(0xFFFF_FFFFL), 128, 1, 1),
                "FIL_NULL 不能作为 payload 链首页");
    }

    /** 单条 payload 超出配置页数时必须在 reservation/MTR 写入前失败。 */
    @Test
    void planningRejectsPayloadBeyondConfiguredPageLimit() {
        TableSchema schema = new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "payload", ColumnType.varchar(20_000, false), 0)), true);
        IndexKeyDef keyDef = new IndexKeyDef(9L,
                List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
        UndoRecord record = UndoRecord.insert(UndoNo.of(1), TransactionId.of(7), 1L, 9L,
                List.of(new ColumnValue.StringValue("x".repeat(16_300))), RollPointer.NULL);

        assertThrows(UndoPayloadTooLargeException.class,
                () -> UndoRecordWritePlan.create(new UndoRecordCodec(new TypeCodecRegistry()),
                        PAGE_SIZE, record, keyDef, schema, 1));
    }
}
