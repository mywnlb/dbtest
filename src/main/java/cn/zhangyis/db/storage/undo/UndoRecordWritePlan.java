package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;

import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * 一条 UndoRecord 的不可变物理编码计划。它在写 MTR admission 前冻结完整编码、inline/external 选择、payload 页数和
 * CRC；执行阶段只补充分配出的首页号，避免开始写页后再发生编码、算术或配置错误。
 */
public final class UndoRecordWritePlan {

    /** 完整逻辑记录；领域对象在构造时已经复制其列表字段。 */
    private final UndoRecord record;
    /** 解码该记录所需的稳定 key/schema。 */
    private final IndexKeyDef keyDef;
    private final TableSchema schema;
    /** 完整 UndoRecord 编码；不向跨包调用者暴露可变引用。 */
    private final byte[] encodedPayload;
    /** 是否使用 external descriptor。 */
    private final boolean external;
    /** external 页数；inline 为 0。 */
    private final int externalPageCount;
    /** 完整编码 CRC32；inline 也预计算，便于计划保持单一构造路径。 */
    private final long crc32;

    private UndoRecordWritePlan(UndoRecord record, IndexKeyDef keyDef, TableSchema schema,
                                byte[] encodedPayload, boolean external, int externalPageCount, long crc32) {
        this.record = record;
        this.keyDef = keyDef;
        this.schema = schema;
        this.encodedPayload = Arrays.copyOf(encodedPayload, encodedPayload.length);
        this.external = external;
        this.externalPageCount = externalPageCount;
        this.crc32 = crc32;
    }

    /** 在任何写 MTR/页分配前形成物理计划，并执行单条 external 页数上限。 */
    static UndoRecordWritePlan create(UndoRecordCodec codec, PageSize pageSize, UndoRecord record,
                                      IndexKeyDef keyDef, TableSchema schema, int maxExternalPages) {
        if (codec == null || pageSize == null || record == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("undo record write plan args must not be null");
        }
        if (maxExternalPages <= 0) {
            throw new DatabaseValidationException("max external undo payload pages must be positive: "
                    + maxExternalPages);
        }
        byte[] encoded = codec.encode(record, keyDef, schema);
        int freshInlineCapacity = pageSize.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES
                - UndoPageLayout.RECORD_AREA_START - Short.BYTES;
        boolean external = encoded.length > freshInlineCapacity;
        int pageCount = external ? pageCount(encoded.length, UndoPayloadPageLayout.payloadCapacity(pageSize)) : 0;
        if (pageCount > maxExternalPages) {
            throw new UndoPayloadTooLargeException("external undo payload requires " + pageCount
                    + " pages, configured maximum is " + maxExternalPages);
        }
        CRC32 checksum = new CRC32();
        checksum.update(encoded);
        return new UndoRecordWritePlan(record, keyDef, schema, encoded, external, pageCount, checksum.getValue());
    }

    /** 返回已冻结的逻辑 undo record。 */
    public UndoRecord record() {
        return record;
    }

    /** 是否需要把完整编码写入独立 payload 页链。 */
    public boolean external() {
        return external;
    }

    /** external 页链精确页数；inline 模式为 0。 */
    public int externalPageCount() {
        return externalPageCount;
    }

    /** 普通 UNDO 页 record 槽中实际保存的 payload 长度。 */
    public int rootPayloadLength() {
        return external ? UndoPayloadDescriptor.BYTES : encodedPayload.length;
    }

    /** 跨包只提供防御性副本，避免计划形成后被调用方改写。 */
    public byte[] encodedPayload() {
        return Arrays.copyOf(encodedPayload, encodedPayload.length);
    }

    IndexKeyDef keyDef() {
        return keyDef;
    }

    TableSchema schema() {
        return schema;
    }

    byte[] encodedPayloadUnsafe() {
        return encodedPayload;
    }

    long crc32() {
        return crc32;
    }

    private static int pageCount(int length, int capacity) {
        return (int) (((long) length + capacity - 1L) / capacity);
    }
}
