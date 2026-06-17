package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 运行时表空间注册表的缓存实现。它只做两件事：缓存 SpaceId 到运行时句柄的映射、把权威元数据的获取委托给
 * 注入的 {@link TablespaceMetadataLoader}。它本身不绑定任何具体存储介质——loader 可以来自磁盘 page 0
 * FSP_HDR、数据字典或启动配置，因此命名为 Caching 而非 DiskBacked，避免类名 over-commit 到单一来源。
 *
 * <p>并发：handles 用 ConcurrentHashMap 只保护映射本身；require/requireForRecovery 用 computeIfAbsent 保证
 * “至多加载一次”，markCorrupted/markDiscarded 用 compute 保证“读当前快照 -> 状态机切换 -> 发布”这一读改写
 * 在桶级原子内完成。真实文件生命周期锁（设计文档 §8.1 的 TablespaceLifecycleLatch/DataFileHandleLock 等）
 * 后续由 TablespaceHandle/DataFileHandle 接入，届时再把生命周期串行化下沉到 fil 物理锁层。
 *
 * <p>首版简化点（与设计文档差异，后续补齐）：
 * <ul>
 *   <li>单一 loader 走 load(SpaceId)：假设某一个源已产出对账好的完整元数据，未做 DD↔FSP_HDR 跨源对账
 *       （真实 InnoDB 会在 space_id/flags 不一致时拒绝启动）。后续可引入 CompositeTablespaceMetadataLoader
 *       按 TablespaceType 路由（config 管 SYSTEM/UNDO/TEMPORARY，DD+disk 管 FILE_PER_TABLE/GENERAL）。</li>
 *   <li>只支持 open-by-id：未建模“扫描全部 .ibd / 枚举数据字典发现 space_id 并批量注册”的 discovery 路径
 *       （loader 缺 loadAll）。</li>
 * </ul>
 */
@Slf4j
public final class CachingTablespaceRegistry implements TablespaceRegistry {

    /**
     * 表空间权威元数据加载入口。它代表磁盘 FSP_HDR、数据字典或启动配置，Registry 不能自行伪造权威状态。
     */
    private final TablespaceMetadataLoader metadataLoader;

    /**
     * 运行时打开表空间 cache。ConcurrentHashMap 只保护映射本身；真实文件生命周期锁后续由 TablespaceHandle/DataFileHandle 持有。
     */
    private final ConcurrentMap<SpaceId, TablespaceHandle> handles = new ConcurrentHashMap<>();

    /**
     * 创建运行时表空间注册表。
     *
     * @param metadataLoader 表空间权威元数据加载器。
     */
    public CachingTablespaceRegistry(TablespaceMetadataLoader metadataLoader) {
        if (metadataLoader == null) {
            throw new DatabaseValidationException("tablespace metadata loader must not be null");
        }
        this.metadataLoader = metadataLoader;
    }

    /**
     * 打开表空间并建立运行时句柄。数据流为 SpaceId 进入，直接调用 loader 获取权威元数据，然后替换缓存中的句柄。
     * 首版没有真实 FileChannel，因此没有文件句柄副作用；后续接入 DataFileHandle 时在这里补充生命周期锁和打开动作。
     *
     * @param spaceId 表空间编号。
     * @return 打开后的运行时句柄。
     */
    @Override
    public TablespaceHandle open(SpaceId spaceId) {
        validateSpaceId(spaceId);
        TablespaceHandle handle = loadHandle(spaceId);
        handles.put(spaceId, handle);
        return handle;
    }

    /**
     * 查找运行时缓存中的句柄，不触发 loader。诊断、DDL 状态判断和测试可用它避免隐式打开表空间。
     *
     * @param spaceId 表空间编号。
     * @return 已缓存时返回句柄，否则返回空。
     */
    @Override
    public Optional<TablespaceHandle> find(SpaceId spaceId) {
        validateSpaceId(spaceId);
        return Optional.ofNullable(handles.get(spaceId));
    }

    /**
     * 获取普通 IO 路径可用的表空间句柄。数据流为 SpaceId 进入，先查运行时缓存；未命中时调用 loader
     * 从磁盘/DD/配置加载元数据，转换为 TablespaceHandle 后缓存；随后按白名单校验状态是否允许普通访问。
     *
     * @param spaceId 表空间编号。
     * @return 可供普通 PageStore 路径使用的表空间句柄。
     */
    @Override
    public TablespaceHandle require(SpaceId spaceId) {
        validateSpaceId(spaceId);
        TablespaceHandle handle = handles.computeIfAbsent(spaceId, this::loadHandle);
        ensureOrdinaryAccessAllowed(handle.tablespace());
        return handle;
    }

    /**
     * 获取恢复路径句柄。数据流与普通 require 相同，但不执行状态白名单阻断；恢复流程需要读取损坏/未初始化状态以决定修复策略。
     *
     * @param spaceId 表空间编号。
     * @return 恢复路径可用句柄。
     */
    @Override
    public TablespaceHandle requireForRecovery(SpaceId spaceId) {
        validateSpaceId(spaceId);
        return handles.computeIfAbsent(spaceId, this::loadHandle);
    }

    /**
     * 重新加载权威元数据并替换运行时句柄。用于 DDL、恢复或启动扫描发现磁盘状态变化后的快照发布。
     *
     * @param spaceId 表空间编号。
     * @return 刷新后的句柄。
     */
    @Override
    public TablespaceHandle refresh(SpaceId spaceId) {
        validateSpaceId(spaceId);
        TablespaceHandle handle = loadHandle(spaceId);
        handles.put(spaceId, handle);
        return handle;
    }

    /**
     * 用调用方提供的新元数据替换运行时句柄。数据通常来自 recovery 或 DDL 已校验的权威元数据快照。
     *
     * @param metadata 新表空间元数据。
     * @return 替换后的句柄。
     */
    @Override
    public TablespaceHandle replace(TablespaceMetadata metadata) {
        if (metadata == null) {
            throw new DatabaseValidationException("tablespace metadata must not be null");
        }
        TablespaceHandle handle = new TablespaceHandle(metadata.toTablespace());
        handles.put(metadata.spaceId(), handle);
        return handle;
    }

    /**
     * 标记表空间损坏并阻断普通 IO 路径。
     *
     * <p>数据流：SpaceId 进入，在 ConcurrentHashMap 桶级原子内完成“读当前快照（缺失则按权威来源加载）->
     * 状态机切到 CORRUPTED -> 发布新句柄”的读改写，避免与并发 refresh/replace/markDiscarded 互相覆盖造成
     * 丢更新。首版以桶级原子替代设计文档 §8.1 要求的 TablespaceLifecycleLatch(X)，后续接入真实生命周期 latch
     * 后再下沉到 fil 物理锁层。
     *
     * @param spaceId 表空间编号。
     * @param reason 损坏原因，进入日志诊断；首版不持久化，后续会进入恢复诊断上下文。
     * @return 损坏状态句柄。
     */
    @Override
    public TablespaceHandle markCorrupted(SpaceId spaceId, String reason) {
        validateSpaceId(spaceId);
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("tablespace corrupted reason must not be blank");
        }
        TablespaceHandle corrupted = handles.compute(spaceId, (id, existing) -> {
            Tablespace current = (existing != null ? existing : loadHandle(id)).tablespace();
            return new TablespaceHandle(current.transitTo(TablespaceState.CORRUPTED));
        });
        // 标记损坏属于关键生命周期/不可恢复诊断事件，必须留痕：带上 spaceId 与原因，供恢复和人工排查。
        log.warn("tablespace {} marked corrupted: {}", spaceId.value(), reason);
        return corrupted;
    }

    /**
     * 标记表空间已 discard。该操作只更新运行时快照；真实 discard 的文件关闭和 Buffer Pool stale 后续由生命周期服务补充。
     *
     * <p>与 markCorrupted 一致，状态读改写在桶级原子内完成，避免与并发状态发布丢更新。
     *
     * @param spaceId 表空间编号。
     * @return discarded 状态句柄。
     */
    @Override
    public TablespaceHandle markDiscarded(SpaceId spaceId) {
        validateSpaceId(spaceId);
        TablespaceHandle discarded = handles.compute(spaceId, (id, existing) -> {
            Tablespace current = (existing != null ? existing : loadHandle(id)).tablespace();
            return new TablespaceHandle(current.transitTo(TablespaceState.DISCARDED));
        });
        // discard 是显式 DDL 生命周期转换，记录一条便于追踪表空间下线；非高频路径，不会刷屏。
        log.info("tablespace {} marked discarded", spaceId.value());
        return discarded;
    }

    /**
     * 关闭运行时表空间句柄。首版没有文件句柄，因此只移除 cache；后续接入 DataFileHandle 后需在生命周期 X latch 下关闭文件。
     *
     * @param spaceId 表空间编号。
     */
    @Override
    public void close(SpaceId spaceId) {
        validateSpaceId(spaceId);
        handles.remove(spaceId);
    }

    /**
     * 移除运行时句柄。语义上用于 drop 完成后的 registry 清理，首版与 close 相同但保留不同方法名表达业务阶段。
     *
     * @param spaceId 表空间编号。
     */
    @Override
    public void remove(SpaceId spaceId) {
        close(spaceId);
    }

    /**
     * 返回已打开表空间句柄快照。返回 List.copyOf 结果，避免调用方修改 registry 内部 cache。
     *
     * @return 已打开句柄列表。
     */
    @Override
    public List<TablespaceHandle> listOpenTablespaces() {
        return List.copyOf(handles.values());
    }

    /**
     * 判断运行时 cache 中是否存在指定表空间句柄，不触发 loader。
     *
     * @param spaceId 表空间编号。
     * @return 存在句柄返回 true。
     */
    @Override
    public boolean isOpen(SpaceId spaceId) {
        validateSpaceId(spaceId);
        return handles.containsKey(spaceId);
    }

    private TablespaceHandle loadHandle(SpaceId spaceId) {
        TablespaceMetadata metadata = metadataLoader.load(spaceId)
                .orElseThrow(() -> new TablespaceNotFoundException("tablespace not found: " + spaceId.value()));
        if (!metadata.spaceId().equals(spaceId)) {
            throw new DatabaseValidationException("loaded tablespace metadata space id mismatch");
        }
        return new TablespaceHandle(metadata.toTablespace());
    }

    /**
     * 普通 PageStore 路径的状态白名单校验：仅 NORMAL/ACTIVE 允许（与 {@link TablespaceState} 注释和设计 §5.2 一致）。
     * EMPTY(未初始化)、INACTIVE(如 undo 待截断)、DISCARDED、CORRUPTED 都不能走普通 IO，否则可能读到未初始化页、
     * 正在截断的 undo 或损坏数据。CORRUPTED/DISCARDED 给出更具体的领域异常，其余不可用状态给 TablespaceUnavailableException。
     *
     * @param tablespace 当前运行时快照。
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

    private void validateSpaceId(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("tablespace space id must not be null");
        }
    }
}
