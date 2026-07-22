package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;

/** `OALTDES1` descriptor page body v1 codec；FIL header/trailer由统一page envelope维护。 */
public final class SdiOnlineAlterDescriptorPageCodec {

    private static final long MAGIC = 0x4f414c5444455331L; // OALTDES1
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_BYTES = 84;
    private static final int ENTRY_BYTES = 60;

    /** 当前page size下FIL header和trailer之间的完整body容量。 */
    private final int bodyCapacity;

    public SdiOnlineAlterDescriptorPageCodec(PageSize pageSize) {
        if (pageSize == null) {
            throw new DatabaseValidationException("descriptor page codec requires page size");
        }
        this.bodyCapacity = PageEnvelopeLayout.trailerOffset(pageSize)
                - PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;
        if (bodyCapacity < HEADER_BYTES + ENTRY_BYTES) {
            throw new DatabaseValidationException("page is too small for online ALTER descriptor");
        }
    }

    /** @return 当前page body可容纳的固定entry数量；至少为1。 */
    public int maxEntriesPerPage() {
        return (bodyCapacity - HEADER_BYTES) / ENTRY_BYTES;
    }

    /**
     * 编码page body；未使用尾部保持全零，便于恢复拒绝未知扩展或残留旧entry。
     *
     * @param page 已通过owner、space和entry唯一性校验的不可变页
     * @return 精确body容量的独立字节数组
     */
    public byte[] encode(SdiOnlineAlterDescriptorPage page) {
        if (page == null || page.entries().size() > (bodyCapacity - HEADER_BYTES) / ENTRY_BYTES) {
            throw new DatabaseValidationException("online ALTER descriptor page exceeds capacity");
        }
        byte[] payload = new byte[Math.multiplyExact(page.entries().size(), ENTRY_BYTES)];
        ByteBuffer entries = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        for (SdiOnlineAlterDescriptorEntry entry : page.entries()) {
            int start = entries.position();
            IndexStorageBinding binding = entry.requireSpace(page.descriptorSegment().spaceId())
                    .indexBinding();
            entries.putInt(ENTRY_BYTES)
                    .putShort((short) entry.action().stableCode()).putShort((short) 0)
                    .putInt(entry.actionOrdinal()).putLong(binding.indexId())
                    .putLong(binding.rootPageId().pageNo().value()).putInt(binding.rootLevel())
                    .putInt(binding.leafSegment().inodeSlot())
                    .putLong(binding.leafSegment().segmentId().value())
                    .putInt(binding.nonLeafSegment().inodeSlot())
                    .putLong(binding.nonLeafSegment().segmentId().value());
            entries.putInt(crc32c(Arrays.copyOfRange(payload, start, start + ENTRY_BYTES - Integer.BYTES)));
        }

        ByteBuffer output = ByteBuffer.allocate(bodyCapacity).order(ByteOrder.BIG_ENDIAN);
        SegmentRef segment = page.descriptorSegment();
        output.putLong(MAGIC).putInt(FORMAT_VERSION).putInt(HEADER_BYTES)
                .putLong(page.ddlOperationId()).putLong(page.tableId())
                .putLong(page.targetDictionaryVersion()).putLong(page.generation())
                .putLong(segment.segmentId().value()).putInt(segment.inodeSlot())
                .putInt(page.pageOrdinal()).putLong(page.nextPageNo())
                .putInt(page.entries().size()).putInt(payload.length).putInt(crc32c(payload))
                .put(payload);
        return output.array();
    }

    /**
     * 解码完整page body并重建owner/entry不变量；非零尾部视为未知格式而非忽略。
     *
     * @param encoded FIL envelope中取出的完整body区域
     * @param expectedSpace 页信封已经验证的space identity
     * @return 完整descriptor page
     * @throws DatabaseValidationException 任一shape、CRC、identity或保留区非法时抛出
     */
    public SdiOnlineAlterDescriptorPage decode(byte[] encoded, SpaceId expectedSpace) {
        if (encoded == null || encoded.length != bodyCapacity || expectedSpace == null) {
            throw new DatabaseValidationException("online ALTER descriptor body length/space is invalid");
        }
        ByteBuffer input = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        if (input.getLong() != MAGIC || input.getInt() != FORMAT_VERSION
                || input.getInt() != HEADER_BYTES) {
            throw new DatabaseValidationException("online ALTER descriptor magic/version/header is invalid");
        }
        long ddlId = input.getLong();
        long tableId = input.getLong();
        long targetVersion = input.getLong();
        long generation = input.getLong();
        long segmentId = input.getLong();
        int inodeSlot = input.getInt();
        SegmentRef segment = new SegmentRef(
                expectedSpace, inodeSlot, SegmentId.of(segmentId));
        int pageOrdinal = input.getInt();
        long nextPageNo = input.getLong();
        int entryCount = input.getInt();
        int payloadLength = input.getInt();
        int expectedPayloadCrc = input.getInt();
        int expectedLength;
        try {
            expectedLength = Math.multiplyExact(entryCount, ENTRY_BYTES);
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("descriptor entry count overflows", overflow);
        }
        if (entryCount < 0 || expectedLength != payloadLength || payloadLength > input.remaining()) {
            throw new DatabaseValidationException("online ALTER descriptor entry geometry is invalid");
        }
        byte[] payload = new byte[payloadLength];
        input.get(payload);
        if (expectedPayloadCrc != crc32c(payload)) {
            throw new DatabaseValidationException("online ALTER descriptor payload CRC32C mismatch");
        }
        while (input.hasRemaining()) {
            if (input.get() != 0) {
                throw new DatabaseValidationException(
                        "online ALTER descriptor reserved tail is non-zero");
            }
        }

        ByteBuffer entries = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        List<SdiOnlineAlterDescriptorEntry> decoded = new ArrayList<>(entryCount);
        for (int ordinal = 0; ordinal < entryCount; ordinal++) {
            int start = entries.position();
            if (entries.getInt() != ENTRY_BYTES) {
                throw new DatabaseValidationException("online ALTER descriptor entry length is invalid");
            }
            SdiOnlineAlterDescriptorAction action = SdiOnlineAlterDescriptorAction.fromStableCode(
                    Short.toUnsignedInt(entries.getShort()));
            if (entries.getShort() != 0) {
                throw new DatabaseValidationException("online ALTER descriptor entry flags are non-zero");
            }
            int actionOrdinal = entries.getInt();
            long indexId = entries.getLong();
            long rootPageNo = entries.getLong();
            int rootLevel = entries.getInt();
            int leafSlot = entries.getInt();
            long leafId = entries.getLong();
            int nonLeafSlot = entries.getInt();
            long nonLeafId = entries.getLong();
            int expectedEntryCrc = entries.getInt();
            if (expectedEntryCrc != crc32c(Arrays.copyOfRange(
                    payload, start, start + ENTRY_BYTES - Integer.BYTES))) {
                throw new DatabaseValidationException("online ALTER descriptor entry CRC32C mismatch");
            }
            decoded.add(new SdiOnlineAlterDescriptorEntry(action, actionOrdinal,
                    new IndexStorageBinding(indexId,
                            PageId.of(expectedSpace, PageNo.of(rootPageNo)), rootLevel,
                            new SegmentRef(expectedSpace, leafSlot, SegmentId.of(leafId)),
                            new SegmentRef(expectedSpace, nonLeafSlot, SegmentId.of(nonLeafId)))));
        }
        return new SdiOnlineAlterDescriptorPage(ddlId, targetVersion, tableId, generation,
                segment, pageOrdinal, nextPageNo, decoded);
    }

    private static int crc32c(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length);
        return (int) crc.getValue();
    }
}
