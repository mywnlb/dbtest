package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.domain.SpaceId;

import java.util.List;
import java.util.Optional;

/**
 * 运行时表空间注册表。Registry 缓存已加载句柄，但权威元数据仍来自磁盘、数据字典或启动配置。
 */
public interface TablespaceRegistry {

    /**
     * 打开或加载表空间。数据从 SpaceId 进入，Registry 通过 loader 读取权威元数据并建立运行时句柄。
     *
     * @param spaceId 表空间编号。
     * @return 运行时表空间句柄。
     */
    TablespaceHandle open(SpaceId spaceId);

    /**
     * 查找运行时已打开的表空间句柄，不触发磁盘/DD/配置加载。
     *
     * @param spaceId 表空间编号。
     * @return 已打开时返回句柄，否则返回空。
     */
    Optional<TablespaceHandle> find(SpaceId spaceId);

    /**
     * 获取普通 IO 路径可用的表空间句柄。找不到或损坏时抛项目异常，避免 PageStore 误用非法状态。
     *
     * @param spaceId 表空间编号。
     * @return 运行时表空间句柄。
     */
    TablespaceHandle require(SpaceId spaceId);

    /**
     * 获取恢复路径可用的表空间句柄。该方法允许返回 CORRUPTED 状态，因为 recovery 正是负责修复损坏状态。
     *
     * @param spaceId 表空间编号。
     * @return 恢复路径表空间句柄。
     */
    TablespaceHandle requireForRecovery(SpaceId spaceId);

    /**
     * 从权威元数据来源重新加载并替换运行时句柄。
     *
     * @param spaceId 表空间编号。
     * @return 刷新后的运行时句柄。
     */
    TablespaceHandle refresh(SpaceId spaceId);

    /**
     * 用新的权威元数据替换运行时句柄，供恢复、DDL 或启动加载流程发布新快照。
     *
     * @param metadata 新元数据快照。
     * @return 替换后的运行时句柄。
     */
    TablespaceHandle replace(TablespaceMetadata metadata);

    /**
     * 标记表空间损坏并阻断普通 IO 路径。
     *
     * @param spaceId 表空间编号。
     * @param reason 损坏原因，用于日志和诊断。
     * @return 损坏状态的运行时句柄。
     */
    TablespaceHandle markCorrupted(SpaceId spaceId, String reason);

    /**
     * 标记表空间为 INACTIVE，阻断普通 IO/空间管理准入，但允许 recovery 路径继续读取该句柄。
     *
     * @param spaceId 表空间编号。
     * @return inactive 状态的运行时句柄。
     */
    TablespaceHandle markInactive(SpaceId spaceId);

    /**
     * 标记表空间已 discard，普通 IO 路径后续不能返回该句柄。
     *
     * @param spaceId 表空间编号。
     * @return discarded 状态的运行时句柄。
     */
    TablespaceHandle markDiscarded(SpaceId spaceId);

    /**
     * 关闭运行时句柄。首版仅移除缓存；真实文件关闭后续由 DataFileHandle 接入。
     *
     * @param spaceId 表空间编号。
     */
    void close(SpaceId spaceId);

    /**
     * 移除运行时句柄，供 drop 完成后清理 registry 映射。
     *
     * @param spaceId 表空间编号。
     */
    void remove(SpaceId spaceId);

    /**
     * 返回当前打开表空间的运行时快照列表。
     *
     * @return 已打开表空间句柄快照。
     */
    List<TablespaceHandle> listOpenTablespaces();

    /**
     * 判断表空间是否已存在运行时句柄。
     *
     * @param spaceId 表空间编号。
     * @return 已打开返回 true。
     */
    boolean isOpen(SpaceId spaceId);
}
