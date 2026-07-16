package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;

/** Inline/external record 槽的唯一解码入口，保证 direct RollPointer、segment read 和诊断遍历执行同一组校验。 */
final class UndoStoredRecordResolver {

    private final UndoRecordCodec codec;
    private final UndoPayloadStorage payloadStorage;
    private final int maxExternalPages;

    UndoStoredRecordResolver(UndoRecordCodec codec, UndoPayloadStorage payloadStorage, int maxExternalPages) {
        if (codec == null || payloadStorage == null || maxExternalPages <= 0) {
            throw new DatabaseValidationException("undo stored record resolver dependencies/limit invalid");
        }
        this.codec = codec;
        this.payloadStorage = payloadStorage;
        this.maxExternalPages = maxExternalPages;
    }

    /** 解码 record 槽；external 模式先完整验证页链，再校验 descriptor 与最终 UndoRecord 身份一致。 */
    UndoRecord resolve(MiniTransaction mtr, SpaceId spaceId, UndoPayloadStorage.SegmentIdentity owner,
                       byte[] storedPayload, IndexKeyDef keyDef, TableSchema schema) {
        UndoPayloadDescriptor descriptor = UndoPayloadDescriptor.isExternal(storedPayload)
                ? UndoPayloadDescriptor.decode(storedPayload) : null;
        byte[] encoded = materialize(mtr, spaceId, owner, storedPayload, descriptor);
        UndoRecord record = codec.decode(encoded, 0, keyDef, schema);
        if (descriptor != null && (record.type() != descriptor.type()
                || !record.transactionId().equals(descriptor.transactionId())
                || !record.undoNo().equals(descriptor.undoNo()))) {
            throw new UndoLogFormatException("external undo descriptor does not match decoded record identity");
        }
        return record;
    }

    /** 在完整 typed decode 前只解析 table/index identity；external 页链仍执行同样的 owner/长度/hash 校验。 */
    UndoRecordIdentity identity(MiniTransaction mtr, SpaceId spaceId, UndoPayloadStorage.SegmentIdentity owner,
                                byte[] storedPayload) {
        UndoPayloadDescriptor descriptor = UndoPayloadDescriptor.isExternal(storedPayload)
                ? UndoPayloadDescriptor.decode(storedPayload) : null;
        byte[] encoded = materialize(mtr, spaceId, owner, storedPayload, descriptor);
        UndoRecordIdentity identity = codec.peekIdentity(encoded, 0);
        if (descriptor != null && (identity.type() != descriptor.type()
                || !identity.transactionId().equals(descriptor.transactionId())
                || !identity.undoNo().equals(descriptor.undoNo()))) {
            throw new UndoLogFormatException("external undo descriptor does not match identity prefix");
        }
        return identity;
    }

    private byte[] materialize(MiniTransaction mtr, SpaceId spaceId, UndoPayloadStorage.SegmentIdentity owner,
                               byte[] storedPayload, UndoPayloadDescriptor descriptor) {
        return descriptor == null ? storedPayload
                : payloadStorage.read(mtr, spaceId, owner, descriptor, maxExternalPages);
    }
}
