package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** page3 索引 DDL footer 的持久格式兼容测试。 */
class SdiIndexDdlDescriptorCodecTest {

    /** v2 必须往返保留 DROP action 与完整物理 identity。 */
    @Test
    void roundTripsVersionTwoDropAction() {
        SdiIndexDdlDescriptor expected = descriptor(SdiIndexDdlAction.DROP);

        byte[] encoded = SdiPageRepository.encodeIndexDdl(expected);
        SdiIndexDdlDescriptor decoded =
                SdiPageRepository.decodeIndexDdl(SpaceId.of(77), encoded);

        assertEquals(SdiPageLayout.INDEX_DDL_FORMAT_VERSION,
                ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN).getInt(Integer.BYTES));
        assertEquals(expected, decoded);
    }

    /** 既有 v1 footer 没有 action 字段，升级后必须继续按 BUILD 解码且不得错位读取 identity。 */
    @Test
    void decodesLegacyVersionOneFooterAsBuild() {
        SdiIndexDdlDescriptor expected = descriptor(SdiIndexDdlAction.BUILD);
        byte[] legacy = encodeLegacyBuild(expected);

        SdiIndexDdlDescriptor decoded =
                SdiPageRepository.decodeIndexDdl(SpaceId.of(77), legacy);

        assertEquals(expected, decoded);
    }

    private static SdiIndexDdlDescriptor descriptor(SdiIndexDdlAction action) {
        SpaceId spaceId = SpaceId.of(77);
        return new SdiIndexDdlDescriptor(action, 91, 21, 31,
                new IndexStorageBinding(41, PageId.of(spaceId, PageNo.of(19)), 2,
                        new SegmentRef(spaceId, 3, SegmentId.of(51)),
                        new SegmentRef(spaceId, 4, SegmentId.of(52))));
    }

    /** 按冻结的 v1 字段顺序构造兼容样本；CRC 只覆盖实际字段，不包含页尾零保留区。 */
    private static byte[] encodeLegacyBuild(SdiIndexDdlDescriptor descriptor) {
        IndexStorageBinding index = descriptor.indexBinding();
        ByteBuffer footer = ByteBuffer.allocate(SdiPageLayout.INDEX_BUILD_FOOTER_BYTES)
                .order(ByteOrder.BIG_ENDIAN);
        footer.putInt(SdiPageLayout.INDEX_BUILD_MAGIC)
                .putInt(SdiPageLayout.LEGACY_INDEX_BUILD_FORMAT_VERSION)
                .putLong(descriptor.ddlOperationId())
                .putLong(descriptor.dictionaryVersion())
                .putLong(descriptor.tableId())
                .putLong(index.indexId())
                .putLong(index.rootPageId().pageNo().value())
                .putInt(index.rootLevel())
                .putInt(index.leafSegment().inodeSlot())
                .putLong(index.leafSegment().segmentId().value())
                .putInt(index.nonLeafSegment().inodeSlot())
                .putLong(index.nonLeafSegment().segmentId().value());
        int crcOffset = footer.position();
        CRC32C crc = new CRC32C();
        crc.update(footer.array(), 0, crcOffset);
        footer.putInt((int) crc.getValue());
        Arrays.fill(footer.array(), footer.position(), footer.capacity(), (byte) 0);
        return footer.array();
    }
}
