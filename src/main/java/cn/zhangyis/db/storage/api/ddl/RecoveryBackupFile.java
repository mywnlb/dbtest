package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;

import java.nio.file.Path;

/**
 * 物理层完成稳定复制、离线 DISCARDED 标记与 force 后返回的可信备份文件证据。
 *
 * @param path 已原子发布且由调用方随后写 manifest 的备份数据路径
 * @param identity 从备份副本 page0 重新检查得到的稳定身份
 * @param cleanLsn 复制前 drain 得到的 checkpoint 安全边界
 * @param lengthBytes 完整备份文件长度
 * @param sha256 整文件小写十六进制 SHA-256
 */
public record RecoveryBackupFile(
        Path path,
        TablespaceFileIdentity identity,
        Lsn cleanLsn,
        long lengthBytes,
        String sha256) {

    /** 固化绝对路径并拒绝不完整物理证据。 */
    public RecoveryBackupFile {
        if (path == null || identity == null || cleanLsn == null || sha256 == null
                || sha256.length() != 64 || lengthBytes <= 0) {
            throw new DatabaseValidationException(
                    "recovery backup file evidence is incomplete");
        }
        path = path.toAbsolutePath().normalize();
    }
}
