package cn.zhangyis.db.storage.api.catalog;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/**
 * 内部 catalog 的一个无语义 byte record。storage 只保证 key/payload 边界和批次 durability，不解析 DD kind。
 */
public record CatalogRecord(byte[] key, byte[] payload) {
    public CatalogRecord {
        if (key == null || payload == null || key.length == 0) {
            throw new DatabaseValidationException("catalog record key/payload must not be null and key must not be empty");
        }
        key = Arrays.copyOf(key, key.length);
        payload = Arrays.copyOf(payload, payload.length);
    }

    @Override
    public byte[] key() {
        return Arrays.copyOf(key, key.length);
    }

    @Override
    public byte[] payload() {
        return Arrays.copyOf(payload, payload.length);
    }
}
