package cn.zhangyis.db.storage.api.tablespace;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.nio.file.Path;
import java.time.Duration;

/**
 * 单个 file-per-table 候选的离线只读扫描请求。
 *
 * @param path 待扫描文件的规范候选路径；scanner 会再次 NOFOLLOW 校验
 * @param expectedPageSize 实例配置的固定页大小
 * @param expectedTableId 文件名/manifest 声明的正数 table identity
 * @param expectedSpaceId 文件名/manifest 声明的正数 space identity
 * @param timeout 整个属性读取、顺序页扫描与末尾复核共享的正超时
 */
public record TablespaceFullScrubRequest(
        Path path,
        PageSize expectedPageSize,
        long expectedTableId,
        SpaceId expectedSpaceId,
        Duration timeout) {

    public TablespaceFullScrubRequest {
        if (path == null || expectedPageSize == null || expectedTableId <= 0 || expectedSpaceId == null
                || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("tablespace full scrub request is invalid");
        }
        path = path.toAbsolutePath().normalize();
    }
}
