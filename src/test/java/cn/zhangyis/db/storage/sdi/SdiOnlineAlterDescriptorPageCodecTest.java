package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** DDL_DESCRIPTOR页必须绑定owner/segment/chain，并为每个index binding保存独立CRC。 */
class SdiOnlineAlterDescriptorPageCodecTest {

    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);
    private final SdiOnlineAlterDescriptorPageCodec codec =
            new SdiOnlineAlterDescriptorPageCodec(PAGE_SIZE);

    /** ADD/DROP entry按action ordinal顺序往返，root页同时保存descriptor segment identity。 */
    @Test
    void roundTripsDescriptorChainPage() {
        SpaceId spaceId = SpaceId.of(9);
        SegmentRef descriptorSegment = new SegmentRef(spaceId, 3, SegmentId.of(31));
        SdiOnlineAlterDescriptorPage expected = new SdiOnlineAlterDescriptorPage(
                41, 42, 43, 44, descriptorSegment, 0, 0,
                List.of(
                        new SdiOnlineAlterDescriptorEntry(
                                SdiOnlineAlterDescriptorAction.ADD, 0, binding(spaceId, 51, 61)),
                        new SdiOnlineAlterDescriptorEntry(
                                SdiOnlineAlterDescriptorAction.DROP, 2, binding(spaceId, 52, 62))));

        SdiOnlineAlterDescriptorPage actual = codec.decode(codec.encode(expected), spaceId);

        assertEquals(expected.ddlOperationId(), actual.ddlOperationId());
        assertEquals(expected.targetDictionaryVersion(), actual.targetDictionaryVersion());
        assertEquals(expected.tableId(), actual.tableId());
        assertEquals(expected.generation(), actual.generation());
        assertEquals(expected.descriptorSegment(), actual.descriptorSegment());
        assertEquals(expected.pageOrdinal(), actual.pageOrdinal());
        assertEquals(expected.nextPageNo(), actual.nextPageNo());
        assertEquals(expected.entries(), actual.entries());
    }

    /** 页payload、entry CRC、非零尾部或跨space binding损坏都必须阻止资源清理。 */
    @Test
    void rejectsCorruptionTrailingBytesAndCrossSpaceEntry() {
        SpaceId spaceId = SpaceId.of(9);
        SegmentRef descriptorSegment = new SegmentRef(spaceId, 3, SegmentId.of(31));
        byte[] encoded = codec.encode(new SdiOnlineAlterDescriptorPage(
                41, 42, 43, 44, descriptorSegment, 0, 0,
                List.of(new SdiOnlineAlterDescriptorEntry(
                        SdiOnlineAlterDescriptorAction.ADD, 0, binding(spaceId, 51, 61)))));
        byte[] corrupt = encoded.clone();
        corrupt[100] ^= 1;
        byte[] trailing = encoded.clone();
        trailing[trailing.length - 1] = 1;

        assertThrows(DatabaseValidationException.class, () -> codec.decode(corrupt, spaceId));
        assertThrows(DatabaseValidationException.class, () -> codec.decode(trailing, spaceId));
        assertThrows(DatabaseValidationException.class, () ->
                new SdiOnlineAlterDescriptorEntry(SdiOnlineAlterDescriptorAction.ADD, 0,
                        binding(SpaceId.of(10), 51, 61)).requireSpace(spaceId));
    }

    private static IndexStorageBinding binding(SpaceId spaceId, long indexId, long rootPage) {
        return new IndexStorageBinding(indexId, PageId.of(spaceId, PageNo.of(rootPage)), 0,
                new SegmentRef(spaceId, 4, SegmentId.of(indexId + 100)),
                new SegmentRef(spaceId, 5, SegmentId.of(indexId + 200)));
    }
}
