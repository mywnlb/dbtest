package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/** 全局 Change Buffer tree payload 的 v1 自描述编码器。 */
public final class ChangeBufferMutationCodec {

    /** 识别 v1 mutation envelope 的 ASCII {@code IBM1} magic。 */
    private static final int MAGIC = 0x49424D31;
    /** 当前可读写的持久 mutation 格式版本。 */
    private static final int VERSION = 1;
    /** 不含 entry payload、但包含尾部 envelope CRC 的固定字节数。 */
    private static final int FIXED_WITHOUT_PAYLOAD = 76;
    /** magic 字段偏移。 */
    private static final int MAGIC_OFFSET = 0;
    /** format version 字段偏移。 */
    private static final int VERSION_OFFSET = 4;
    /** 完整 envelope 长度字段偏移。 */
    private static final int TOTAL_LENGTH_OFFSET = 8;
    /** 目标 SpaceId 字段偏移。 */
    private static final int TARGET_SPACE_OFFSET = 12;
    /** 目标 PageNo 字段偏移。 */
    private static final int TARGET_PAGE_OFFSET = 20;
    /** 全局提交顺序字段偏移。 */
    private static final int SEQUENCE_OFFSET = 28;
    /** exact-version table id 字段偏移。 */
    private static final int TABLE_ID_OFFSET = 36;
    /** exact-version schema version 字段偏移。 */
    private static final int SCHEMA_VERSION_OFFSET = 44;
    /** exact-version secondary index id 字段偏移。 */
    private static final int INDEX_ID_OFFSET = 52;
    /** 持久 operation code 字段偏移。 */
    private static final int OPERATION_OFFSET = 60;
    /** entry payload 长度字段偏移。 */
    private static final int PAYLOAD_LENGTH_OFFSET = 64;
    /** entry payload 独立 CRC32C 字段偏移。 */
    private static final int PAYLOAD_CRC_OFFSET = 68;
    /** entry payload 首字节偏移；尾随 envelope CRC 位于总长度末四字节。 */
    private static final int PAYLOAD_OFFSET = 72;

    /** 单条 payload 必须显著小于页大小，给内部 record header/key/目录保留空间。 */
    private final int maxEntryBytes;

    /**
     * @param pageSize 实例页大小；用它派生单条 mutation 的安全内存/record 上限
     */
    public ChangeBufferMutationCodec(PageSize pageSize) {
        if (pageSize == null) {
            throw new DatabaseValidationException("change buffer mutation codec page size must not be null");
        }
        this.maxEntryBytes = pageSize.bytes() - 512;
    }

    /**
     * 编码完整 stable identity、payload CRC 和 envelope CRC。
     *
     * @param mutation 已校验 mutation
     * @return 独立字节数组
     * @throws DatabaseValidationException payload 超过单页安全上限时抛出，调用方应回退直写
     */
    public byte[] encode(ChangeBufferMutation mutation) {
        if (mutation == null) {
            throw new DatabaseValidationException("change buffer mutation must not be null");
        }
        byte[] payload = mutation.entryBytes();
        if (payload.length > maxEntryBytes) {
            throw new DatabaseValidationException("change buffer entry payload exceeds safe page bound: "
                    + payload.length);
        }
        int totalLength = Math.addExact(FIXED_WITHOUT_PAYLOAD, payload.length);
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        buffer.putInt(MAGIC_OFFSET, MAGIC);
        buffer.putInt(VERSION_OFFSET, VERSION);
        buffer.putInt(TOTAL_LENGTH_OFFSET, totalLength);
        buffer.putLong(TARGET_SPACE_OFFSET, mutation.targetPageId().spaceId().value());
        buffer.putLong(TARGET_PAGE_OFFSET, mutation.targetPageId().pageNo().value());
        buffer.putLong(SEQUENCE_OFFSET, mutation.sequence());
        buffer.putLong(TABLE_ID_OFFSET, mutation.tableId());
        buffer.putLong(SCHEMA_VERSION_OFFSET, mutation.schemaVersion());
        buffer.putLong(INDEX_ID_OFFSET, mutation.indexId());
        buffer.putInt(OPERATION_OFFSET, mutation.operation().code());
        buffer.putInt(PAYLOAD_LENGTH_OFFSET, payload.length);
        buffer.putInt(PAYLOAD_CRC_OFFSET, crc(payload, payload.length));
        buffer.position(PAYLOAD_OFFSET);
        buffer.put(payload);
        buffer.putInt(totalLength - Integer.BYTES, crc(buffer.array(), totalLength - Integer.BYTES));
        return buffer.array();
    }

    /**
     * 完整校验长度、两个 CRC 和所有 identity 后解码。
     *
     * @param bytes 从内部 record 复制的 payload
     * @return 不引用输入数组的 mutation
     * @throws ChangeBufferFormatException 输入截断、损坏或超出配置安全上限时抛出
     */
    public ChangeBufferMutation decode(byte[] bytes) {
        if (bytes == null || bytes.length < FIXED_WITHOUT_PAYLOAD) {
            throw new ChangeBufferFormatException("change buffer mutation is truncated");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int declaredLength = buffer.getInt(TOTAL_LENGTH_OFFSET);
        int payloadLength = buffer.getInt(PAYLOAD_LENGTH_OFFSET);
        if (buffer.getInt(MAGIC_OFFSET) != MAGIC || buffer.getInt(VERSION_OFFSET) != VERSION
                || declaredLength != bytes.length || payloadLength <= 0 || payloadLength > maxEntryBytes
                || Math.addExact(FIXED_WITHOUT_PAYLOAD, payloadLength) != bytes.length) {
            throw new ChangeBufferFormatException("change buffer mutation format/length mismatch");
        }
        if (buffer.getInt(bytes.length - Integer.BYTES) != crc(bytes, bytes.length - Integer.BYTES)) {
            throw new ChangeBufferFormatException("change buffer mutation envelope CRC mismatch");
        }
        byte[] payload = new byte[payloadLength];
        buffer.position(PAYLOAD_OFFSET);
        buffer.get(payload);
        if (buffer.getInt(PAYLOAD_CRC_OFFSET) != crc(payload, payload.length)) {
            throw new ChangeBufferFormatException("change buffer mutation payload CRC mismatch");
        }
        try {
            return new ChangeBufferMutation(
                    PageId.of(SpaceId.of(Math.toIntExact(buffer.getLong(TARGET_SPACE_OFFSET))),
                            PageNo.of(buffer.getLong(TARGET_PAGE_OFFSET))),
                    buffer.getLong(SEQUENCE_OFFSET), buffer.getLong(TABLE_ID_OFFSET),
                    buffer.getLong(SCHEMA_VERSION_OFFSET), buffer.getLong(INDEX_ID_OFFSET),
                    ChangeBufferOperation.fromCode(buffer.getInt(OPERATION_OFFSET)), payload);
        } catch (RuntimeException invalid) {
            throw new ChangeBufferFormatException("change buffer mutation identity is invalid", invalid);
        }
    }

    private static int crc(byte[] bytes, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, length);
        return (int) crc.getValue();
    }
}
