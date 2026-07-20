package cn.zhangyis.db.engine.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 绑定 manifest、完整候选扫描和冲突集合的离线授权 token。
 *
 * @param scanId state digest 的稳定短前缀，也作为 quarantine 子目录名
 * @param manifestSequence manifest journal 最新 committed event sequence
 * @param manifestDigest clean manifest body SHA-256 十六进制
 * @param stateDigest 排序后的目录 fingerprint 与冲突集合 SHA-256 十六进制
 */
public record CatalogRecoveryToken(
        String scanId,
        long manifestSequence,
        String manifestDigest,
        String stateDigest) {

    public CatalogRecoveryToken {
        if (scanId == null || scanId.isBlank() || manifestSequence <= 0
                || manifestDigest == null || manifestDigest.isBlank()
                || stateDigest == null || stateDigest.isBlank()) {
            throw new DatabaseValidationException("catalog recovery token is invalid");
        }
    }
}
