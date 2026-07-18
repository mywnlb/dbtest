package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;

/** Inline/external record 槽的唯一解码入口，保证 direct RollPointer、segment read 和诊断遍历执行同一组校验。 */
final class UndoStoredRecordResolver {

    /**
     * 本对象持有的 {@code codec} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final UndoRecordCodec codec;
    /**
     * 本次事务链路持有的 {@code payloadStorage} undo/rollback 状态；事务身份、roll pointer 与段代际必须一致，提交、回滚和 purge 路径依赖它完成收口。
     */
    private final UndoPayloadStorage payloadStorage;
    /**
     * 记录 {@code maxExternalPages} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final int maxExternalPages;

    /**
     * 创建 {@code UndoStoredRecordResolver}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param codec 由组合根提供的 {@code UndoRecordCodec} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param payloadStorage 由组合根提供的 {@code UndoPayloadStorage} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param maxExternalPages 参与 {@code 构造} 的上界或规格值 {@code maxExternalPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    UndoStoredRecordResolver(UndoRecordCodec codec, UndoPayloadStorage payloadStorage, int maxExternalPages) {
        if (codec == null || payloadStorage == null || maxExternalPages <= 0) {
            throw new DatabaseValidationException("undo stored record resolver dependencies/limit invalid");
        }
        this.codec = codec;
        this.payloadStorage = payloadStorage;
        this.maxExternalPages = maxExternalPages;
    }

    /** 解码 record 槽；external 模式先完整验证页链，再校验 descriptor 与最终 UndoRecord 身份一致。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param owner 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param storedPayload 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param keyDef 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code resolve} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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

    /** 在完整 typed decode 前只解析 table/index identity；external 页链仍执行同样的 owner/长度/hash 校验。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param owner 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param storedPayload 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return {@code identity} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
