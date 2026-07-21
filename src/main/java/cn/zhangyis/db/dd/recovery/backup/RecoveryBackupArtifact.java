package cn.zhangyis.db.dd.recovery.backup;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.nio.file.Path;

/**
 * 管理员可复制到固定 incoming 位置的已完成 archive pair。
 *
 * @param dataPath 已 force 且 page0 为 DISCARDED 的备份数据文件
 * @param manifestPath 最后原子发布的签名 manifest
 * @param manifest 内存中的同一签名证据
 */
public record RecoveryBackupArtifact(
        Path dataPath, Path manifestPath, RecoveryBackupManifest manifest) {

    public RecoveryBackupArtifact {
        if (dataPath == null || manifestPath == null || manifest == null) {
            throw new DatabaseValidationException(
                    "recovery backup artifact fields must not be null");
        }
        dataPath = dataPath.toAbsolutePath().normalize();
        manifestPath = manifestPath.toAbsolutePath().normalize();
    }
}
