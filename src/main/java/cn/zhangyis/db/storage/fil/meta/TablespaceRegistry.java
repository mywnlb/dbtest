package cn.zhangyis.db.storage.fil.meta;
import cn.zhangyis.db.storage.fil.io.PageStore;


import cn.zhangyis.db.domain.SpaceId;

import java.util.List;
import java.util.Optional;

/**
 * 表空间运行期 metadata 句柄的注册与状态准入端口。
 *
 * <p>registry 缓存 {@link SpaceId} 到不可变 {@link TablespaceHandle} 的映射，但不拥有
 * {@link PageStore} 物理 handle，也不替代 page0 持久状态。普通操作在持有表空间 access lease 后调用
 * {@link #require(SpaceId)} 复核 {@code NORMAL/ACTIVE} 白名单；恢复路径使用
 * {@link #requireForRecovery(SpaceId)} 观察非服务状态。</p>
 *
 * <p>“open/close”在本接口中只表示逻辑 cache 的发布/移除。物理文件的 open/close 由
 * {@code DiskSpaceManager/PageStore} 在外层按正确顺序编排。</p>
 */
public interface TablespaceRegistry {

    /**
     * 无条件从 loader 加载指定表空间，并替换同键的逻辑 cache。
     *
     * <p>该方法不执行普通状态白名单校验，可发布 recovery 需要观察的非服务状态；也不打开物理文件，
     * 调用方必须先完成 PageStore open。</p>
     *
     * @param spaceId 待加载的非空表空间标识
     * @return loader 返回并转换后的新逻辑句柄
     * @throws cn.zhangyis.db.storage.fil.exception.TablespaceNotFoundException loader 没有该空间时抛出
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException spaceId 为空或 loader 返回
     *         metadata 的 SpaceId 不匹配时抛出
     */
    TablespaceHandle open(SpaceId spaceId);

    /**
     * 查找已经缓存的逻辑句柄，不触发 loader，也不做生命周期状态白名单校验。
     *
     * @param spaceId 待查询的非空表空间标识
     * @return cache 命中时返回当时发布的句柄，否则返回空；命中值可能处于非服务状态
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException spaceId 为空时抛出
     */
    Optional<TablespaceHandle> find(SpaceId spaceId);

    /**
     * 获取可供普通空间管理/页访问路径使用的逻辑句柄。
     *
     * <p>cache 未命中时按 SpaceId 懒加载；无论命中或加载，返回前都要求状态为
     * {@code NORMAL} 或 {@code ACTIVE}。该检查是运行期准入，不代替调用方的 access lease。</p>
     *
     * @param spaceId 目标非空表空间标识
     * @return 通过普通状态白名单的当前缓存句柄
     * @throws cn.zhangyis.db.storage.fil.exception.TablespaceNotFoundException loader 未找到或状态为
     *         {@code DISCARDED} 时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.TablespaceCorruptedException 状态为
     *         {@code CORRUPTED} 时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.TablespaceUnavailableException 状态为其它非服务状态时抛出
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException spaceId 为空或 loader identity
     *         不一致时抛出
     */
    TablespaceHandle require(SpaceId spaceId);

    /**
     * 获取恢复/诊断路径使用的逻辑句柄，不执行普通状态白名单。
     *
     * <p>该方法可以返回 {@code EMPTY/INACTIVE/TRUNCATING/DISCARDED/CORRUPTED} 等状态，
     * 只是让 recovery 读取并裁决 metadata，不表示这些状态可用于普通 IO。</p>
     *
     * @param spaceId 目标非空表空间标识
     * @return cache 命中或按 loader 懒加载得到的当前句柄
     * @throws cn.zhangyis.db.storage.fil.exception.TablespaceNotFoundException loader 没有该空间时抛出
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException spaceId 为空或 loader identity
     *         不一致时抛出
     */
    TablespaceHandle requireForRecovery(SpaceId spaceId);

    /**
     * 无条件重新调用 loader，并替换同键逻辑 cache。
     *
     * <p>与 {@link #open(SpaceId)} 的当前语义相同，方法名强调已有 cache 的刷新场景；不校验版本单调性
     * 或普通状态白名单。</p>
     *
     * @param spaceId 待刷新的非空表空间标识
     * @return loader 最新结果转换并发布的新句柄
     * @throws cn.zhangyis.db.storage.fil.exception.TablespaceNotFoundException loader 没有该空间时抛出
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException spaceId 为空或 loader identity
     *         不一致时抛出
     */
    TablespaceHandle refresh(SpaceId spaceId);

    /**
     * 把调用方已经构造并校验的 metadata 直接转换为逻辑句柄并替换 cache。
     *
     * <p>该方法不调用 loader、不写 page0，也不比较旧/新 version 或状态迁移；持久化顺序和覆盖裁决
     * 完全由 recovery/DDL/创建编排方负责。</p>
     *
     * @param metadata 待发布的非空表空间 metadata
     * @return 已写入 cache 的新逻辑句柄
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException metadata 为空时抛出
     */
    TablespaceHandle replace(TablespaceMetadata metadata);

    /**
     * 在当前缓存快照上原子派生 {@code CORRUPTED} 状态并发布。
     *
     * <p>cache 缺失时先加载；方法只改运行期逻辑状态并记录原因，不把原因或状态写入 page0。
     * 调用方必须在外层完成 durable marker 与 access lease 编排。</p>
     *
     * @param spaceId 目标非空表空间标识
     * @param reason 非空白诊断原因，仅进入运行日志
     * @return 新发布的 corrupted 句柄
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 参数非法或当前状态不能迁移到
     *         {@code CORRUPTED} 时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.TablespaceNotFoundException cache 缺失且 loader 未找到时抛出
     */
    TablespaceHandle markCorrupted(SpaceId spaceId, String reason);

    /**
     * 在当前缓存快照上原子派生 {@code INACTIVE} 状态并发布。
     *
     * <p>方法本身不验证表空间类型、不持久化 lifecycle，也不获取物理准入 lease；这些是 UNDO
     * 生命周期编排方的责任。</p>
     *
     * @param spaceId 目标非空表空间标识
     * @return 新发布的 inactive 句柄
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 参数非法或当前状态不能迁移到
     *         {@code INACTIVE} 时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.TablespaceNotFoundException cache 缺失且 loader 未找到时抛出
     */
    TablespaceHandle markInactive(SpaceId spaceId);

    /**
     * 在当前缓存快照上原子派生 {@code DISCARDED} 状态并发布。
     *
     * <p>发布后普通 {@link #require(SpaceId)} 按 NotFound 拒绝。方法本身不写 durable marker、不失效
     * buffer pool，也不关闭或删除数据文件。</p>
     *
     * @param spaceId 目标非空表空间标识
     * @return 新发布的 discarded 句柄
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 参数非法或当前状态不能迁移到
     *         {@code DISCARDED} 时抛出
     * @throws cn.zhangyis.db.storage.fil.exception.TablespaceNotFoundException cache 缺失且 loader 未找到时抛出
     */
    TablespaceHandle markDiscarded(SpaceId spaceId);

    /**
     * 从 registry 移除逻辑句柄。
     *
     * <p>当前实现与 {@link #remove(SpaceId)} 都是幂等 cache remove，不调用
     * {@link PageStore#close(SpaceId)}；外层必须先/后按生命周期协议单独关闭物理文件。</p>
     *
     * @param spaceId 待移除的非空表空间标识；未缓存时无操作
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException spaceId 为空时抛出
     */
    void close(SpaceId spaceId);

    /**
     * 在 drop 完成阶段移除逻辑句柄。
     *
     * <p>当前行为与 {@link #close(SpaceId)} 相同，独立方法名只表达调用阶段，不删除 page0 或物理文件。</p>
     *
     * @param spaceId 待移除的非空表空间标识；未缓存时无操作
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException spaceId 为空时抛出
     */
    void remove(SpaceId spaceId);

    /**
     * 返回调用时刻所有缓存逻辑句柄的不可变列表快照。
     *
     * @return 不保证 SpaceId 排序的句柄列表；空 cache 返回空列表
     */
    List<TablespaceHandle> listOpenTablespaces();

    /**
     * 判断 registry cache 是否包含指定 SpaceId，不触发 loader。
     *
     * <p>返回 {@code true} 只表示逻辑句柄存在，可能是非服务状态，也不证明 PageStore 物理 handle 已打开。</p>
     *
     * @param spaceId 待查询的非空表空间标识
     * @return cache 中存在任意状态句柄时返回 {@code true}
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException spaceId 为空时抛出
     */
    boolean isOpen(SpaceId spaceId);
}
