package cn.zhangyis.db.storage.fil.meta;
import cn.zhangyis.db.storage.fil.state.TablespaceType;


import cn.zhangyis.db.domain.SpaceId;

import java.util.Optional;

/**
 * 根据 {@link SpaceId} 重建单个 registry 元数据快照的端口。
 *
 * <p>当前生产实现是 {@code PageZeroTablespaceMetadataLoader}：目标
 * {@link cn.zhangyis.db.storage.fil.io.PageStore} handle 必须已打开，
 * loader 在表空间共享 access lease 内取得路径、读取 page0，并校验 FSP_HDR envelope、checksum/trailer、
 * space identity、flags/type 和 lifecycle；普通入口还校验 freeLimit 已发布的 XDES 管理目录，恢复入口只建立
 * redo 所需最小根。未打开时返回空，registry 不关心这些物理细节，只跟踪返回值是否 recovery-only。</p>
 *
 * <p>当前接口只支持已知 SpaceId 的单条加载，不承担文件系统扫描、DD discovery 或 load-all。
 * 上层 discovery 负责先确定 SpaceId/path 并打开物理 handle。{@link TablespaceType} 的类型专属来源对账
 * 也不在此端口中建模。</p>
 */
public interface TablespaceMetadataLoader {

    /**
     * 从实现负责的来源加载并校验指定表空间元数据。
     *
     * <p>调用不修改 registry；是否需要物理 handle、共享 lease 或 page0 IO 由实现决定。返回快照必须已经
     * 完成实现承诺的跨源校验，registry 只执行运行期状态准入。</p>
     *
     * @param spaceId 待加载表空间的稳定标识；不能为空
     * @return 找到并校验成功时返回 metadata；来源未知或当前生产 PageStore 未打开时返回空，
     *         不返回 {@code null}
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException spaceId 或已加载字段非法时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.TablespaceCorruptedException 已发布 page0 物理信封或
     *         checksum/trailer 损坏时抛出，调用方不得缓存为普通可用状态
     */
    Optional<TablespaceMetadata> load(SpaceId spaceId);

    /**
     * 为 crash recovery 加载最小可信 metadata，不要求由 redo/doublewrite 负责修复的派生管理页已经稳定。
     *
     * <p>默认实现委托 {@link #load(SpaceId)}，因此已有 loader 与测试 lambda 仍维持原有严格语义。只有明确区分
     * “page0 身份准入”和“普通流量完整准入”的物理 loader 才应覆盖本方法。registry 会把该入口产生的句柄标为
     * recovery-only；任何普通 {@code require} 都必须重新调用 {@link #load(SpaceId)}，不能借 cache hit 绕过严格校验。</p>
     *
     * @param spaceId 待恢复表空间的稳定标识；不能为空
     * @return 最小恢复身份校验成功时返回 metadata；来源未知或物理句柄未打开时返回空
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException identity 或 page0 持久格式非法时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.TablespaceCorruptedException page0 本身无法作为恢复根时抛出
     */
    default Optional<TablespaceMetadata> loadForRecovery(SpaceId spaceId) {
        return load(spaceId);
    }
}
