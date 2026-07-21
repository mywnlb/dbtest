package cn.zhangyis.db.dd.recovery.backup;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;

import java.util.UUID;

/**
 * 可信 clean backup 的签名 manifest。HMAC 覆盖其余全部字段，且文件必须晚于数据文件原子发布。
 *
 * @param formatVersion manifest 持久格式版本
 * @param backupId 单次备份 UUID，也是 archive 文件名前缀
 * @param sourceInstanceId 签发该备份的实例恢复身份 UUID
 * @param tableId 源表稳定 DD identity
 * @param spaceId 源表空间稳定 identity
 * @param sourceDictionaryVersion 备份时 ACTIVE aggregate 的 DD 版本
 * @param fileIdentity 备份副本 DISCARDED page0 的稳定身份
 * @param tableDefinitionSha256 不含 lifecycle/version 的逻辑与物理定义摘要
 * @param cleanLsn 复制前 drain/checkpoint 安全边界
 * @param fileLengthBytes 数据文件精确长度
 * @param fileSha256 数据文件整文件摘要
 * @param hmacSha256 由实例 256-bit key 计算的 manifest HMAC
 */
public record RecoveryBackupManifest(
        int formatVersion,
        UUID backupId,
        UUID sourceInstanceId,
        TableId tableId,
        SpaceId spaceId,
        DictionaryVersion sourceDictionaryVersion,
        TablespaceFileIdentity fileIdentity,
        String tableDefinitionSha256,
        Lsn cleanLsn,
        long fileLengthBytes,
        String fileSha256,
        String hmacSha256) {

    public static final int CURRENT_FORMAT = 1;

    /** 拒绝不完整或无法进入稳定二进制格式的签名证据。 */
    public RecoveryBackupManifest {
        if (formatVersion != CURRENT_FORMAT || backupId == null || sourceInstanceId == null
                || tableId == null || spaceId == null || sourceDictionaryVersion == null
                || fileIdentity == null || tableDefinitionSha256 == null || fileSha256 == null
                || hmacSha256 == null || cleanLsn == null || fileLengthBytes <= 0
                || tableDefinitionSha256.length() != 64 || fileSha256.length() != 64
                || (!hmacSha256.isEmpty() && hmacSha256.length() != 64)
                || !spaceId.equals(fileIdentity.spaceId())) {
            throw new DatabaseValidationException(
                    "recovery backup manifest fields are invalid");
        }
    }

    /** @return 复制全部签名字段但替换 HMAC 的新 manifest。 */
    public RecoveryBackupManifest withHmac(String hmac) {
        return new RecoveryBackupManifest(
                formatVersion, backupId, sourceInstanceId, tableId, spaceId,
                sourceDictionaryVersion, fileIdentity, tableDefinitionSha256,
                cleanLsn, fileLengthBytes, fileSha256, hmac);
    }
}
