package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.domain.SpaceId;

import java.util.Optional;

/**
 * 表空间元数据加载接口。实现可以来自 page 0 FSP_HDR、数据字典、undo/temp/system 启动配置或测试假源。
 *
 * <p>首版简化点（与设计文档差异，后续补齐）：只提供按 SpaceId 单条加载，假设单一来源已产出对账好的完整元数据，
 * 未建模数据字典与 FSP_HDR 的跨源对账（真实 InnoDB 会在 space_id/flags 不一致时拒绝启动），也未提供
 * loadAll/扫描发现入口。后续可由 CompositeTablespaceMetadataLoader 按 TablespaceType 组合多源
 * （config 管 SYSTEM/UNDO/TEMPORARY，DD+disk 管 FILE_PER_TABLE/GENERAL）并补充 discovery 扫描。
 */
public interface TablespaceMetadataLoader {

    /**
     * 从权威来源加载指定表空间元数据。接口只返回元数据快照，不打开文件句柄，也不缓存运行时状态。
     *
     * @param spaceId 表空间编号。
     * @return 找到时返回元数据；找不到时返回空。
     */
    Optional<TablespaceMetadata> load(SpaceId spaceId);
}
