package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/**
 * DD 与 storage.api 之间的 SDI 载体。storage 只解释 table/version identity 和 payload 完整性，
 * 不解析 payload 内的列、索引或对象名。
 *
 * @param tableId           payload 所描述的稳定 table identity，必须为正
 * @param dictionaryVersion committed DD 的单调版本，必须为正
 * @param payload           DD codec 产生的完整、非空、单页有界字节
 */
public record SerializedDictionaryInfo(long tableId, long dictionaryVersion, byte[] payload) {

    public SerializedDictionaryInfo {
        if (tableId <= 0 || dictionaryVersion <= 0 || payload == null || payload.length == 0) {
            throw new DatabaseValidationException("serialized dictionary info identity/payload is invalid");
        }
        payload = Arrays.copyOf(payload, payload.length);
    }

    /**
     * 返回 payload 防御副本，防止调用方在 CRC 校验或 MTR 写入后修改底层数组。
     *
     * @return 与构造时内容相同、由调用方独占的新数组
     */
    @Override
    public byte[] payload() {
        return Arrays.copyOf(payload, payload.length);
    }

    /**
     * 以 payload 内容而不是数组引用比较 SDI 值，保证跨读盘得到的等价快照具有值对象语义。
     *
     * @param other 待比较对象
     * @return identity、version 与 payload 字节全部相同时为 {@code true}
     */
    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof SerializedDictionaryInfo that
                && tableId == that.tableId
                && dictionaryVersion == that.dictionaryVersion
                && Arrays.equals(payload, that.payload);
    }

    /**
     * 生成与内容相等语义一致的哈希值。
     *
     * @return identity、version 和 payload 内容的组合哈希
     */
    @Override
    public int hashCode() {
        int result = Long.hashCode(tableId);
        result = 31 * result + Long.hashCode(dictionaryVersion);
        return 31 * result + Arrays.hashCode(payload);
    }
}
