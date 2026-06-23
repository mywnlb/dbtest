package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

/**
 * page-0 SpaceHeader 的 raw 物理字段子集。
 *
 * <p>该对象只承载 FSP 层能直接理解的物理值；{@code spaceFlags} 保留为原始 int，不在 fsp 中解释表空间类型、
 * 压缩或加密语义，避免 fsp 反向依赖 fil 的逻辑模型。loader 需要的 FLST base 之外字段通过本对象跨包传递。
 *
 * @param spaceId            表空间编号；用于 loader 校验请求 spaceId 与 page0 自描述一致。
 * @param pageSize           page0 声明的页大小；用于 metadata 快照和基本损坏校验。
 * @param spaceFlags         原始标志位，低位可由上层 fil/api 解 type，fsp 不解释。
 * @param currentSizeInPages page0 记录的当前表空间大小；DiskSpaceManager autoextend 后该字段才是权威 size。
 * @param freeLimitPageNo    FSP 已纳入管理的页号上界。
 * @param spaceVersion       表空间元数据版本；后续 DDL/recovery 可用它识别 stale handle。
 */
public record SpaceHeaderPhysical(
        SpaceId spaceId,
        PageSize pageSize,
        int spaceFlags,
        PageNo currentSizeInPages,
        PageNo freeLimitPageNo,
        long spaceVersion) {

    public SpaceHeaderPhysical {
        if (spaceId == null || pageSize == null || currentSizeInPages == null || freeLimitPageNo == null) {
            throw new DatabaseValidationException("space header physical fields must not be null");
        }
    }
}
