package cn.zhangyis.db.dd.recovery.backup;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.UUID;

/** 本实例可信恢复备份的不可变签名身份；密钥只以防御性副本离开对象。 */
public final class RecoveryIdentity {

    /** 签名实例 UUID，manifest 必须与之逐字匹配。 */
    private final UUID instanceId;
    /** 256-bit HMAC-SHA256 key；只由 recovery identity 文件持久化。 */
    private final byte[] hmacKey;

    /**
     * @param instanceId 本实例随机且跨重启稳定的恢复身份
     * @param hmacKey 恰好 32 字节的随机 HMAC key
     */
    public RecoveryIdentity(UUID instanceId, byte[] hmacKey) {
        if (instanceId == null || hmacKey == null || hmacKey.length != 32) {
            throw new DatabaseValidationException(
                    "recovery identity requires UUID and 256-bit HMAC key");
        }
        this.instanceId = instanceId;
        this.hmacKey = hmacKey.clone();
    }

    /** @return 本实例跨重启稳定 UUID。 */
    public UUID instanceId() {
        return instanceId;
    }

    /** @return 供单次 HMAC 运算使用的密钥副本；调用方不得持久化到 manifest。 */
    public byte[] hmacKey() {
        return hmacKey.clone();
    }
}
