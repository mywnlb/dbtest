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

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof SdiPageSnapshot that
                && tableId == that.tableId
                && dictionaryVersion == that.dictionaryVersion
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(tableId);
        result = 31 * result + Long.hashCode(dictionaryVersion);
        return 31 * result + Arrays.hashCode(payload);
    }
}
