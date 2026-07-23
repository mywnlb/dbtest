package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.domain.SegmentId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证 header 和 mutation 的跨重启稳定编码以及损坏时 fail-closed 的边界。 */
class ChangeBufferPersistentCodecTest {

    /** Header round-trip 必须保留 root/segment/sequence/count，且 CRC 覆盖全部权威字段。 */
    @Test
    void roundTripsHeaderAndRejectsCorruption() {
        ChangeBufferHeaderSnapshot expected = new ChangeBufferHeaderSnapshot(
                ChangeBufferHeaderState.ACTIVE, ChangeBufferMode.ALL,
                PageId.of(SpaceId.of(0), PageNo.of(4)), 2, ChangeBufferRecordSchema.INDEX_ID,
                new SegmentRef(SpaceId.of(0), 3, SegmentId.of(7)),
                new SegmentRef(SpaceId.of(0), 4, SegmentId.of(8)),
                19, 6, 1);
        byte[] bytes = ChangeBufferHeaderCodec.encode(expected);
        assertEquals(expected, ChangeBufferHeaderCodec.decode(bytes));

        bytes[ChangeBufferHeaderCodec.ROOT_LEVEL_OFFSET] ^= 1;
        assertThrows(DatabaseRuntimeException.class, () -> ChangeBufferHeaderCodec.decode(bytes));
    }

    /** sequence/count 达到 long 上界时必须在构造后置状态前以领域异常拒绝，不能泄漏裸算术异常。 */
    @Test
    void rejectsHeaderAppendCounterExhaustion() {
        ChangeBufferHeaderSnapshot exhausted = new ChangeBufferHeaderSnapshot(
                ChangeBufferHeaderState.ACTIVE, ChangeBufferMode.ALL,
                PageId.of(SpaceId.of(0), PageNo.of(4)), 0, ChangeBufferRecordSchema.INDEX_ID,
                new SegmentRef(SpaceId.of(0), 3, SegmentId.of(7)),
                new SegmentRef(SpaceId.of(0), 4, SegmentId.of(8)),
                Long.MAX_VALUE, 1L, 1L);

        assertThrows(ChangeBufferStateException.class, () -> exhausted.afterAppend(0));
    }

    /** Mutation 编码保存完整 stable identity 和 payload；payload 或 envelope CRC 损坏必须拒绝。 */
    @Test
    void roundTripsMutationAndRejectsPayloadCorruption() {
        byte[] entry = new byte[]{3, 1, 4, 1, 5, 9};
        ChangeBufferMutation expected = new ChangeBufferMutation(
                PageId.of(SpaceId.of(31), PageNo.of(77)), 123,
                41, 9, 52, ChangeBufferOperation.DELETE_MARK, entry);
        ChangeBufferMutationCodec codec = new ChangeBufferMutationCodec(PageSize.ofBytes(16 * 1024));
        byte[] bytes = codec.encode(expected);
        ChangeBufferMutation actual = codec.decode(bytes);
        assertEquals(expected.targetPageId(), actual.targetPageId());
        assertEquals(expected.sequence(), actual.sequence());
        assertEquals(expected.tableId(), actual.tableId());
        assertEquals(expected.schemaVersion(), actual.schemaVersion());
        assertEquals(expected.indexId(), actual.indexId());
        assertEquals(expected.operation(), actual.operation());
        assertArrayEquals(entry, actual.entryBytes());

        bytes[bytes.length - 1] ^= 1;
        assertThrows(DatabaseRuntimeException.class, () -> codec.decode(bytes));
    }
}
