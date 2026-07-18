package cn.zhangyis.db.storage.fil.meta;
import cn.zhangyis.db.storage.fil.exception.TablespaceCorruptedException;
import cn.zhangyis.db.storage.fil.exception.TablespaceNotFoundException;
import cn.zhangyis.db.storage.fil.exception.TablespaceUnavailableException;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 基于并发映射的表空间运行期 metadata registry。
 *
 * <p>本类缓存 {@link SpaceId} 到不可变 {@link TablespaceHandle} 的映射，并把 cache miss/reload
 * 委托给 {@link TablespaceMetadataLoader}。当前生产 loader 从已经打开的 {@link PageStore} 读取 page0；
 * registry 本身不持 {@code FileChannel}，也不拥有物理文件生命周期。</p>
 *
 * <p>并发边界：{@link ConcurrentHashMap#compute(Object, java.util.function.BiFunction)} 只保证单次
 * mark 方法内部的“读当前值—派生状态—写回”对同一 key 原子；{@code open/refresh/replace} 使用独立
 * {@code put}，可以在没有外层协调时与 mark 相互覆盖。生产生命周期路径必须先用
 * {@code TablespaceAccessController} 的同空间 lease 串行化持久状态、物理文件和 registry 发布，再调用本类。
 * {@code computeIfAbsent} 只对一次成功映射计算提供并发合并；加载抛异常、cache remove 或 refresh 后仍可再次加载。</p>
 *
 * <p>registry 只实施运行期状态白名单，不验证 {@link TablespaceType} 专属状态、不持久化 lifecycle，
 * 也不执行文件 discovery；这些责任位于 page0 loader、DiskSpaceManager、DDL/recovery 编排层。</p>
 */
@Slf4j
public final class CachingTablespaceRegistry implements TablespaceRegistry {

    /**
     * cache miss 与显式 reload 的唯一 metadata 来源。实现必须返回与请求 SpaceId 一致且已经完成自身
     * 物理校验的快照；本字段构造后不可替换。
     */
    private final TablespaceMetadataLoader metadataLoader;

    /**
     * 运行期逻辑句柄 cache。并发映射安全发布不可变 handle，但不把多个 registry 方法、page0 写入或
     * PageStore 操作组合成原子事务；跨模块顺序由外层 access lease 保护。
     */
    private final ConcurrentMap<SpaceId, TablespaceHandle> handles = new ConcurrentHashMap<>();

    /**
     * 创建使用指定加载端口的空 registry。
     *
     * @param metadataLoader 非空 metadata 加载端口，供 cache miss/open/refresh 使用
     * @throws DatabaseValidationException metadataLoader 为空时抛出；registry 不会被创建
     */
    public CachingTablespaceRegistry(TablespaceMetadataLoader metadataLoader) {
        if (metadataLoader == null) {
            throw new DatabaseValidationException("tablespace metadata loader must not be null");
        }
        this.metadataLoader = metadataLoader;
    }

    /**
     * 无条件加载并发布指定表空间的逻辑句柄。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>拒绝空 SpaceId，避免 loader/cache 使用无身份键。</li>
     *     <li>调用 loader 并核对返回 metadata identity；未找到或不一致时不修改 cache。</li>
     *     <li>用 {@code put} 替换同键句柄并返回；不执行普通状态白名单，也不打开/关闭 PageStore。</li>
     * </ol>
     *
     * @param spaceId 待逻辑打开的非空表空间标识；物理 handle 应已由外层打开
     * @return 本次加载并写入 cache 的新句柄
     * @throws DatabaseValidationException spaceId 为空或 loader identity 不一致时抛出
     * @throws TablespaceNotFoundException loader 没有该空间时抛出
     */
    @Override
    public TablespaceHandle open(SpaceId spaceId) {
        // 1. cache 与 loader 都只能使用具有领域身份的 SpaceId。
        validateSpaceId(spaceId);

        // 2. 完整加载和 identity 校验先成功，避免失败时覆盖仍可诊断的旧 cache。
        TablespaceHandle handle = loadHandle(spaceId);

        // 3. 逻辑发布不隐式执行普通状态准入或物理 open。
        handles.put(spaceId, handle);
        return handle;
    }

    /**
     * 查找运行时 cache，不触发 loader 或状态白名单。
     *
     * @param spaceId 待查询的非空表空间标识
     * @return cache 命中时返回任意生命周期状态的句柄，否则返回空
     * @throws DatabaseValidationException spaceId 为空时抛出
     */
    @Override
    public Optional<TablespaceHandle> find(SpaceId spaceId) {
        validateSpaceId(spaceId);
        return Optional.ofNullable(handles.get(spaceId));
    }

    /**
     * 获取通过普通运行期状态准入的句柄。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 SpaceId，保证 cache key 与异常上下文有效。</li>
     *     <li>cache miss 时在同键的 {@code computeIfAbsent} 内调用 loader；成功值安全发布，失败不留半句柄。</li>
     *     <li>对命中/新加载快照统一执行 {@code NORMAL/ACTIVE} 白名单，按损坏、丢弃或暂不可用分类失败。</li>
     * </ol>
     *
     * @param spaceId 目标非空表空间标识
     * @return 当前 cache 中通过普通状态白名单的句柄
     * @throws DatabaseValidationException spaceId 为空或 loader identity 不一致时抛出
     * @throws TablespaceNotFoundException loader 未找到或状态为 DISCARDED 时抛出
     * @throws TablespaceCorruptedException 状态为 CORRUPTED 时抛出
     * @throws TablespaceUnavailableException 状态为其它非服务状态时抛出
     */
    @Override
    public TablespaceHandle require(SpaceId spaceId) {
        // 1. 无效 cache key 在触发加载之前失败。
        validateSpaceId(spaceId);

        // 2. 同键 cache miss 的成功加载由 ConcurrentHashMap 发布；异常不会写入映射。
        TablespaceHandle handle = handles.computeIfAbsent(spaceId, this::loadHandle);

        // 3. 即使 cache 命中也重新复核状态，禁止返回已 mark 的非服务句柄。
        ensureOrdinaryAccessAllowed(handle.tablespace());
        return handle;
    }

    /**
     * 获取恢复路径句柄，cache miss 时加载，但不执行普通状态白名单。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先校验 SpaceId，拒绝无身份恢复请求。</li>
     *     <li>cache miss 时加载并发布；命中时原样返回，不过滤状态。返回非服务状态只是让
     *     recovery/诊断层裁决后续动作，不授予普通 PageStore 访问权限。</li>
     * </ol>
     *
     * @param spaceId 目标非空表空间标识
     * @return 任意生命周期状态的缓存或新加载句柄
     * @throws DatabaseValidationException spaceId 为空或 loader identity 不一致时抛出
     * @throws TablespaceNotFoundException loader 没有该空间时抛出
     */
    @Override
    public TablespaceHandle requireForRecovery(SpaceId spaceId) {
        // 1. 恢复映射同样必须使用稳定 SpaceId。
        validateSpaceId(spaceId);

        // 2. 只确保句柄存在，不套用普通状态白名单。
        return handles.computeIfAbsent(spaceId, this::loadHandle);
    }

    /**
     * 无条件重新加载 metadata 并替换逻辑 cache。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 SpaceId。</li>
     *     <li>从 loader 完整读取并核对 identity；失败时保留旧 cache。</li>
     *     <li>用 {@code put} 发布新 handle。方法不比较旧新 version，也不会自动与并发 mark 合并；
     *     生命周期调用方必须持外层 lease。</li>
     * </ol>
     *
     * @param spaceId 待刷新的非空表空间标识
     * @return 本次 loader 结果转换出的新句柄
     * @throws DatabaseValidationException spaceId 为空或 loader identity 不一致时抛出
     * @throws TablespaceNotFoundException loader 没有该空间时抛出
     */
    @Override
    public TablespaceHandle refresh(SpaceId spaceId) {
        // 1. 无效键在 loader IO 前失败。
        validateSpaceId(spaceId);

        // 2. 新快照完整构造成功前不触碰旧 cache。
        TablespaceHandle handle = loadHandle(spaceId);

        // 3. 直接替换逻辑值；外层 lease 负责与状态发布串行化。
        handles.put(spaceId, handle);
        return handle;
    }

    /**
     * 把调用方提供的 metadata 直接转换并发布到逻辑 cache。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>拒绝空 metadata，避免发布无 identity 快照。</li>
     *     <li>转换为不持物理资源的运行时 handle；构造过程复用字段结构校验。</li>
     *     <li>按 metadata 自身 SpaceId 直接覆盖旧值；不调用 loader、不写 page0，也不校验 version 单调或状态迁移。</li>
     * </ol>
     *
     * @param metadata 上层已完成持久化顺序和跨源校验的非空快照
     * @return 写入 cache 的新句柄
     * @throws DatabaseValidationException metadata 或其结构字段非法时抛出；cache 保持原值
     */
    @Override
    public TablespaceHandle replace(TablespaceMetadata metadata) {
        // 1. 无 metadata 就没有可发布的 registry identity/state。
        if (metadata == null) {
            throw new DatabaseValidationException("tablespace metadata must not be null");
        }

        // 2. 在改映射前完成运行时 record/handle 构造，失败不会污染旧 cache。
        TablespaceHandle handle = new TablespaceHandle(metadata.toTablespace());

        // 3. 直接发布调用方给出的代次；持久化和并发覆盖裁决属于外层 lifecycle lease。
        handles.put(metadata.spaceId(), handle);
        return handle;
    }

    /**
     * 原子派生并发布 {@code CORRUPTED} 运行期状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 SpaceId 与非空白诊断原因；失败不读取 loader 或修改 cache。</li>
     *     <li>在同键 {@code compute} 内读取已有 handle，缺失时加载，然后通过状态图派生 corrupted 快照。</li>
     *     <li>compute 成功后记录包含 SpaceId/reason 的 WARN；reason 不进入 handle/page0，方法也不关闭物理文件。</li>
     * </ol>
     *
     * <p>compute 只与同键其它 compute 原子；无外层 lease 时，独立 put 的 open/refresh/replace 仍可能随后覆盖结果。</p>
     *
     * @param spaceId 目标非空表空间标识
     * @param reason 非空白损坏诊断，只写运行日志
     * @return 新发布的 corrupted 句柄
     * @throws DatabaseValidationException 参数非法、loader identity 不一致或当前状态不允许迁移时抛出
     * @throws TablespaceNotFoundException cache 缺失且 loader 未找到时抛出
     */
    @Override
    public TablespaceHandle markCorrupted(SpaceId spaceId, String reason) {
        // 1. 在进入映射原子区前完成参数检查，避免非法 reason 触发不必要加载。
        validateSpaceId(spaceId);
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("tablespace corrupted reason must not be blank");
        }

        // 2. 同键 compute 内读取/加载当前代次并执行状态机迁移，失败不会写入半成品。
        TablespaceHandle corrupted = handles.compute(spaceId, (id, existing) -> {
            Tablespace current = (existing != null ? existing : loadHandle(id)).tablespace();
            return new TablespaceHandle(current.transitTo(TablespaceState.CORRUPTED));
        });

        // 3. 状态发布后再留诊断日志；日志不替代 durable lifecycle marker。
        log.warn("tablespace {} marked corrupted: {}", spaceId.value(), reason);
        return corrupted;
    }

    /**
     * 原子派生并发布 {@code INACTIVE} 运行期状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 SpaceId，避免无身份状态更新。</li>
     *     <li>在同键 compute 内读取/加载当前快照，经通用状态图派生 INACTIVE 并发布新 handle。</li>
     *     <li>成功后记录低频生命周期 INFO；本方法不校验 UNDO 类型、不写 page0，也不取得 access lease。</li>
     * </ol>
     *
     * @param spaceId 目标非空表空间标识
     * @return 新发布的 inactive 句柄
     * @throws DatabaseValidationException 参数非法、loader identity 不一致或当前状态不能迁移时抛出
     * @throws TablespaceNotFoundException cache 缺失且 loader 未找到时抛出
     */
    @Override
    public TablespaceHandle markInactive(SpaceId spaceId) {
        // 1. 生命周期更新必须有稳定 cache key。
        validateSpaceId(spaceId);

        // 2. 同键原子读改写；类型专属状态校验由外层 UNDO 编排负责。
        TablespaceHandle inactive = handles.compute(spaceId, (id, existing) -> {
            Tablespace current = (existing != null ? existing : loadHandle(id)).tablespace();
            return new TablespaceHandle(current.transitTo(TablespaceState.INACTIVE));
        });

        // 3. 只记录运行期发布事件，不能替代 page0 durable lifecycle。
        log.info("tablespace {} marked inactive", spaceId.value());
        return inactive;
    }

    /**
     * 原子派生并发布 {@code DISCARDED} 运行期状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 SpaceId。</li>
     *     <li>在同键 compute 内读取/加载当前快照，经状态图迁移为 DISCARDED 并发布。</li>
     *     <li>成功后记录 INFO；本方法不写 durable discard marker、不失效 Buffer Pool、不关闭或删除文件。</li>
     * </ol>
     *
     * @param spaceId 目标非空表空间标识
     * @return 新发布的 discarded 句柄；后续普通 require 会按 NotFound 拒绝
     * @throws DatabaseValidationException 参数非法、loader identity 不一致或当前状态不能迁移时抛出
     * @throws TablespaceNotFoundException cache 缺失且 loader 未找到时抛出
     */
    @Override
    public TablespaceHandle markDiscarded(SpaceId spaceId) {
        // 1. discard 更新必须绑定稳定 SpaceId。
        validateSpaceId(spaceId);

        // 2. 同键原子派生终态；物理 drain 和持久 marker 必须已由外层协议协调。
        TablespaceHandle discarded = handles.compute(spaceId, (id, existing) -> {
            Tablespace current = (existing != null ? existing : loadHandle(id)).tablespace();
            return new TablespaceHandle(current.transitTo(TablespaceState.DISCARDED));
        });

        // 3. 日志只记录运行期状态发布，不证明文件删除已经完成。
        log.info("tablespace {} marked discarded", spaceId.value());
        return discarded;
    }

    /**
     * 幂等移除指定逻辑 cache 项。
     *
     * <p>不调用 {@link PageStore#close(SpaceId)}，也不等待普通 IO；外层 lifecycle 编排负责物理 close 顺序。
     * 已经返回给其它线程的旧 handle 仍是可读不可变值，所以普通路径还必须依赖 access lease 后重新 require。</p>
     *
     * @param spaceId 待移除的非空表空间标识；cache 未命中时无操作
     * @throws DatabaseValidationException spaceId 为空时抛出
     */
    @Override
    public void close(SpaceId spaceId) {
        validateSpaceId(spaceId);
        handles.remove(spaceId);
    }

    /**
     * 在 drop 清理阶段移除逻辑 cache；当前委托 {@link #close(SpaceId)}，无额外物理副作用。
     *
     * @param spaceId 待移除的非空表空间标识；cache 未命中时无操作
     * @throws DatabaseValidationException spaceId 为空时抛出
     */
    @Override
    public void remove(SpaceId spaceId) {
        close(spaceId);
    }

    /**
     * 复制并返回当前可遍历到的逻辑句柄集合。
     *
     * <p>结果列表不可修改，但 ConcurrentHashMap 遍历是弱一致的：并发 replace/remove 时不保证所有元素
     * 来自同一个全局瞬间，也不保证 SpaceId 排序。</p>
     *
     * @return 不可变、无顺序保证的句柄列表；cache 为空时返回空列表
     */
    @Override
    public List<TablespaceHandle> listOpenTablespaces() {
        return List.copyOf(handles.values());
    }

    /**
     * 判断逻辑 cache 是否包含指定 SpaceId，不触发 loader 或状态白名单。
     *
     * @param spaceId 待查询的非空表空间标识
     * @return 存在任意生命周期状态的 handle 时返回 {@code true}
     * @throws DatabaseValidationException spaceId 为空时抛出
     */
    @Override
    public boolean isOpen(SpaceId spaceId) {
        validateSpaceId(spaceId);
        return handles.containsKey(spaceId);
    }

    /**
     * 调用 loader，核对 identity，并转换为不持物理资源的运行时 handle。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按请求 SpaceId 调用 loader；空结果转换为明确 NotFound。</li>
     *     <li>比较 metadata 自带 SpaceId，拒绝把其它空间的 page0 快照缓存到当前 key。</li>
     *     <li>转换为 Tablespace/Handle；成功仅返回候选值，是否写入 cache 由上层缓存装载流程决定。</li>
     * </ol>
     *
     * @param spaceId 已通过非空校验的请求标识
     * @return 与请求 identity 一致的新运行时 handle
     * @throws TablespaceNotFoundException loader 返回空时抛出
     * @throws DatabaseValidationException loader 返回其它 SpaceId 或 metadata 结构非法时抛出
     */
    private TablespaceHandle loadHandle(SpaceId spaceId) {
        // 1. 未找到的 loader 结果不能用空 handle 污染 cache。
        TablespaceMetadata metadata = metadataLoader.load(spaceId)
                .orElseThrow(() -> new TablespaceNotFoundException("tablespace not found: " + spaceId.value()));

        // 2. cache key 必须与 page0/metadata identity 精确一致。
        if (!metadata.spaceId().equals(spaceId)) {
            throw new DatabaseValidationException("loaded tablespace metadata space id mismatch");
        }

        // 3. 转换只创建不可变逻辑值；物理 handle ownership 仍在 PageStore。
        return new TablespaceHandle(metadata.toTablespace());
    }

    /**
     * 对运行期快照实施普通操作状态白名单。
     *
     * <p>{@code NORMAL/ACTIVE} 直接通过；{@code CORRUPTED} 与 {@code DISCARDED} 分别映射为损坏和
     * 不存在异常；{@code EMPTY/INACTIVE/TRUNCATING} 映射为暂不可用。方法只读不可变快照，不访问
     * PageStore，也不尝试修复或迁移状态。</p>
     *
     * @param tablespace 待准入的非空当前运行时快照
     * @throws TablespaceCorruptedException 状态为 CORRUPTED 时抛出
     * @throws TablespaceNotFoundException 状态为 DISCARDED 时抛出
     * @throws TablespaceUnavailableException 状态为其它非服务状态时抛出
     */
    private void ensureOrdinaryAccessAllowed(Tablespace tablespace) {
        TablespaceState state = tablespace.state();
        if (state == TablespaceState.NORMAL || state == TablespaceState.ACTIVE) {
            return;
        }
        long spaceId = tablespace.spaceId().value();
        switch (state) {
            case CORRUPTED -> throw new TablespaceCorruptedException("tablespace is corrupted: " + spaceId);
            case DISCARDED -> throw new TablespaceNotFoundException("tablespace is discarded: " + spaceId);
            // EMPTY/INACTIVE：表空间存在但当前不可用于普通 IO，给出明确领域异常而非 NotFound，避免误导调用方。
            default -> throw new TablespaceUnavailableException(
                    "tablespace not available for ordinary io: space=" + spaceId + ", state=" + state);
        }
    }

    /**
     * 在访问 loader/cache 前拒绝空 SpaceId。
     *
     * @param spaceId registry 操作的目标标识
     * @throws DatabaseValidationException spaceId 为空时抛出
     */
    private void validateSpaceId(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("tablespace space id must not be null");
        }
    }
}
