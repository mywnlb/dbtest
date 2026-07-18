package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/**
 * storage.sdi 内部不可变页值；由上层 facade 转换为稳定 API DTO。
 *
 * @param tableId           页上 table identity
 * @param dictionaryVersion 页上 DD version
 * @param payload           已通过 CRC32C 的 opaque payload
 */
public record SdiPageSnapshot(long tableId, long dictionaryVersion, byte[] payload) {

    public SdiPageSnapshot {
        if (tableId <= 0 || dictionaryVersion <= 0 || payload == null || payload.length == 0) {
            throw new DatabaseValidationException("SDI page snapshot identity/payload is invalid");
        }
        payload = Arrays.copyOf(payload, payload.length);
    }

    @Override
    public byte[] payload() {
        return Arrays.copyOf(payload, payload.length);
    }

    /**
     * 实现 {@code equals} 的稳定值语义；比较只读取输入与本对象，不改变数据库内核状态。
     *
     * @param other 待比较对象；允许为 {@code null} 或其他类型，此时按 {@code equals} 契约返回 {@code false}
     * @return 比较对象类型与全部值语义相等时为 {@code true}；对象为 {@code null}、类型不同或任一组件不等时为 {@code false}
     */
    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof SdiPageSnapshot that
                && tableId == that.tableId
                && dictionaryVersion == that.dictionaryVersion
                && Arrays.equals(payload, that.payload);
    }

    /**
     * 实现 {@code hashCode} 的稳定值语义；比较只读取输入与本对象，不改变数据库内核状态。
     *
     * @return 由参与值语义的全部组件计算出的稳定哈希值；与 {@code equals} 相等的对象必须返回相同结果
     */
    @Override
    public int hashCode() {
        int result = Long.hashCode(tableId);
        result = 31 * result + Long.hashCode(dictionaryVersion);
        return 31 * result + Arrays.hashCode(payload);
    }
}
