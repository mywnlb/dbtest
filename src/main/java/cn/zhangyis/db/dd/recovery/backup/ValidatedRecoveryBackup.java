package cn.zhangyis.db.dd.recovery.backup;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;

import java.nio.file.Path;

/**
 * 在 DDL marker 产生前已通过 HMAC、实例、表定义、文件 hash 与 page0 identity 全部检查的 incoming pair。
 *
 * @param dataPath 固定 recovery-incoming 数据路径
 * @param manifestPath 固定 recovery-incoming manifest 路径
 * @param fileIdentity 已与 manifest 和文件 page0 双向匹配的身份
 * @param manifest 已验证签名 manifest
 */
public record ValidatedRecoveryBackup(
        Path dataPath,
        Path manifestPath,
        TablespaceFileIdentity fileIdentity,
        RecoveryBackupManifest manifest) {

    public ValidatedRecoveryBackup {
        if (dataPath == null || manifestPath == null
                || fileIdentity == null || manifest == null) {
            throw new DatabaseValidationException(
                    "validated recovery backup fields must not be null");
        }
        dataPath = dataPath.toAbsolutePath().normalize();
        manifestPath = manifestPath.toAbsolutePath().normalize();
    }
}
