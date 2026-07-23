package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.storage.api.SegmentRef;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/** system.ibd page 3 body 的 v1 固定长度编码器；不读写页面 envelope。 */
public final class ChangeBufferHeaderCodec {

    /** 识别 v1 header body 的 ASCII {@code IBUF} magic。 */
    private static final int MAGIC = 0x49425546;
    /** 当前可读写的持久 header 格式版本。 */
    private static final int VERSION = 1;
    /** magic 字段在 body 内的字节偏移。 */
    private static final int MAGIC_OFFSET = 0;
    /** format version 字段在 body 内的字节偏移。 */
    private static final int VERSION_OFFSET = 4;
    /** encoded length 字段在 body 内的字节偏移。 */
    private static final int LENGTH_OFFSET = 8;
    /** 生命周期状态稳定 code 的字节偏移。 */
    private static final int STATE_OFFSET = 12;
    /** 配置模式稳定 code 的字节偏移。 */
    private static final int MODE_OFFSET = 16;
    /** 全局树 root SpaceId 的字节偏移。 */
    private static final int ROOT_SPACE_OFFSET = 20;
    /** 全局树 root PageNo 的字节偏移。 */
    private static final int ROOT_PAGE_OFFSET = 28;
    /** 测试与离线诊断用于验证 CRC 覆盖 root level 的稳定偏移。 */
    public static final int ROOT_LEVEL_OFFSET = 36;
    /** 全局树持久 index id 的字节偏移。 */
    private static final int INDEX_ID_OFFSET = 40;
    /** leaf segment SpaceId 的字节偏移。 */
    private static final int LEAF_SPACE_OFFSET = 48;
    /** leaf segment inode slot 的字节偏移。 */
    private static final int LEAF_SLOT_OFFSET = 56;
    /** leaf segment id 的字节偏移。 */
    private static final int LEAF_SEGMENT_OFFSET = 60;
    /** non-leaf segment SpaceId 的字节偏移。 */
    private static final int NON_LEAF_SPACE_OFFSET = 68;
    /** non-leaf segment inode slot 的字节偏移。 */
    private static final int NON_LEAF_SLOT_OFFSET = 76;
    /** non-leaf segment id 的字节偏移。 */
    private static final int NON_LEAF_SEGMENT_OFFSET = 80;
    /** 下一条全局 sequence 的字节偏移。 */
    private static final int NEXT_SEQUENCE_OFFSET = 88;
    /** 已提交 pending mutation 计数的字节偏移。 */
    private static final int PENDING_OFFSET = 96;
    /** header 格式 incarnation 的字节偏移。 */
    private static final int EPOCH_OFFSET = 104;
    /** 覆盖此前全部字段的 CRC32C 字节偏移。 */
    private static final int CRC_OFFSET = 112;
    /** v1 body 长度；后续版本只能追加并提升 version。 */
    public static final int ENCODED_LENGTH = 116;

    private ChangeBufferHeaderCodec() {
    }

    /**
     * 把已校验快照编码为独立 body 字节并计算 CRC32C。
     *
     * @param snapshot page 3 权威状态；不得为 {@code null}
     * @return 固定 116 字节的新数组
     */
    public static byte[] encode(ChangeBufferHeaderSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("change buffer header snapshot must not be null");
        }
        ByteBuffer buffer = ByteBuffer.allocate(ENCODED_LENGTH);
        buffer.putInt(MAGIC_OFFSET, MAGIC);
        buffer.putInt(VERSION_OFFSET, VERSION);
        buffer.putInt(LENGTH_OFFSET, ENCODED_LENGTH);
        buffer.putInt(STATE_OFFSET, snapshot.state().code());
        buffer.putInt(MODE_OFFSET, snapshot.configuredMode().code());
        buffer.putLong(ROOT_SPACE_OFFSET, snapshot.rootPageId().spaceId().value());
        buffer.putLong(ROOT_PAGE_OFFSET, snapshot.rootPageId().pageNo().value());
        buffer.putInt(ROOT_LEVEL_OFFSET, snapshot.rootLevel());
        buffer.putLong(INDEX_ID_OFFSET, snapshot.indexId());
        putSegment(buffer, LEAF_SPACE_OFFSET, LEAF_SLOT_OFFSET, LEAF_SEGMENT_OFFSET, snapshot.leafSegment());
        putSegment(buffer, NON_LEAF_SPACE_OFFSET, NON_LEAF_SLOT_OFFSET,
                NON_LEAF_SEGMENT_OFFSET, snapshot.nonLeafSegment());
        buffer.putLong(NEXT_SEQUENCE_OFFSET, snapshot.nextSequence());
        buffer.putLong(PENDING_OFFSET, snapshot.pendingOperations());
        buffer.putLong(EPOCH_OFFSET, snapshot.formatEpoch());
        buffer.putInt(CRC_OFFSET, crc(buffer.array(), CRC_OFFSET));
        return buffer.array();
    }

    /**
     * 校验 magic/version/length/CRC 后还原 header。任何未知版本或字段组合都在构造运行时对象前失败。
     *
     * @param bytes 从 page 3 body 复制的完整 v1 header 字节
     * @return 完整不可变快照
     * @throws ChangeBufferFormatException 截断、版本不支持、CRC 或字段 identity 损坏时抛出
     */
    public static ChangeBufferHeaderSnapshot decode(byte[] bytes) {
        if (bytes == null || bytes.length != ENCODED_LENGTH) {
            throw new ChangeBufferFormatException("change buffer header length mismatch");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if (buffer.getInt(MAGIC_OFFSET) != MAGIC
                || buffer.getInt(VERSION_OFFSET) != VERSION
                || buffer.getInt(LENGTH_OFFSET) != ENCODED_LENGTH) {
            throw new ChangeBufferFormatException("change buffer header magic/version/length mismatch");
        }
        int expectedCrc = buffer.getInt(CRC_OFFSET);
        int actualCrc = crc(bytes, CRC_OFFSET);
        if (expectedCrc != actualCrc) {
            throw new ChangeBufferFormatException("change buffer header CRC mismatch");
        }
        try {
            PageId root = PageId.of(SpaceId.of(Math.toIntExact(buffer.getLong(ROOT_SPACE_OFFSET))),
                    PageNo.of(buffer.getLong(ROOT_PAGE_OFFSET)));
            SegmentRef leaf = readSegment(buffer, LEAF_SPACE_OFFSET, LEAF_SLOT_OFFSET, LEAF_SEGMENT_OFFSET);
            SegmentRef nonLeaf = readSegment(buffer, NON_LEAF_SPACE_OFFSET,
                    NON_LEAF_SLOT_OFFSET, NON_LEAF_SEGMENT_OFFSET);
            return new ChangeBufferHeaderSnapshot(
                    ChangeBufferHeaderState.fromCode(buffer.getInt(STATE_OFFSET)),
                    ChangeBufferMode.fromCode(buffer.getInt(MODE_OFFSET)), root,
                    buffer.getInt(ROOT_LEVEL_OFFSET), buffer.getLong(INDEX_ID_OFFSET), leaf, nonLeaf,
                    buffer.getLong(NEXT_SEQUENCE_OFFSET), buffer.getLong(PENDING_OFFSET),
                    buffer.getLong(EPOCH_OFFSET));
        } catch (RuntimeException invalid) {
            throw new ChangeBufferFormatException("change buffer header field identity is invalid", invalid);
        }
    }

    private static void putSegment(ByteBuffer buffer, int spaceOffset, int slotOffset, int segmentOffset,
                                   SegmentRef segment) {
        buffer.putLong(spaceOffset, segment.spaceId().value());
        buffer.putInt(slotOffset, segment.inodeSlot());
        buffer.putLong(segmentOffset, segment.segmentId().value());
    }

    private static SegmentRef readSegment(ByteBuffer buffer, int spaceOffset, int slotOffset, int segmentOffset) {
        return new SegmentRef(SpaceId.of(Math.toIntExact(buffer.getLong(spaceOffset))), buffer.getInt(slotOffset),
                SegmentId.of(buffer.getLong(segmentOffset)));
    }

    private static int crc(byte[] bytes, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, length);
        return (int) crc.getValue();
    }
}
