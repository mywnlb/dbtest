package cn.zhangyis.db.storage.api.undotruncate;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteRecoveryScanner;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderPhysical;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRawCodec;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleRawCodec;
import cn.zhangyis.db.storage.recovery.UndoTablespaceRecoveryParticipant;
import cn.zhangyis.db.storage.redo.RedoLogManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link UndoTablespaceRecoveryParticipant} 的生产实现。
 *
 * <p>构造时必须显式给出配置的 undo SpaceId 集合，不做文件 discovery。doublewrite 前逐个修复 page0 并读取
 * TRUNCATING target；已准备的 page0 不重复扫描，且 target 之后尾页禁止恢复。redo replay 后再次 raw 扫描全部配置空间，
 * 以发现“marker redo durable 但 page0 尚未刷出”的崩溃窗口；随后安装 recovered LSN 并调用截断服务续作。
 */
public final class UndoTablespaceTruncationRecovery implements UndoTablespaceRecoveryParticipant {

    /**
     * 本对象拥有的 {@code configuredUndoSpaces} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
     */
    private final List<SpaceId> configuredUndoSpaces;
    /**
     * 本对象持有的 {@code pageStore} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final PageStore pageStore;
    /**
     * 本对象持有的 {@code pageSize} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final PageSize pageSize;
    /**
     * 本对象持有的 {@code registry} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final TablespaceRegistry registry;
    /**
     * 本对象持有的 {@code redo} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final RedoLogManager redo;
    /**
     * 本对象持有的 {@code truncationService} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final UndoTablespaceTruncationService truncationService;

    /** prepare/rescan 生成的 durable TRUNCATING marker；RecoveryService 单线程调用，无需额外锁。 */
    private final Map<SpaceId, TablespaceLifecycleHeader> truncating = new HashMap<>();
    /** doublewrite prepare 已处理的 page0，普通扫描必须跳过，避免修复计数重复和 marker 读取后再次改页。 */
    private final Set<PageId> preparedPageZeros = new HashSet<>();

    /**
     * 创建 {@code UndoTablespaceTruncationRecovery}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param configuredUndoSpaces 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param pageStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param registry 由组合根提供的 {@code TablespaceRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param redo 由组合根提供的 {@code RedoLogManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param truncationService 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoTablespaceTruncationRecovery(
            Set<SpaceId> configuredUndoSpaces,
            PageStore pageStore,
            PageSize pageSize,
            TablespaceRegistry registry,
            RedoLogManager redo,
            UndoTablespaceTruncationService truncationService) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (configuredUndoSpaces == null || pageStore == null || pageSize == null || registry == null
                || redo == null || truncationService == null) {
            throw new DatabaseValidationException("undo truncation recovery dependencies must not be null");
        }
        if (configuredUndoSpaces.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("configured undo space id must not be null");
        }
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.configuredUndoSpaces = configuredUndoSpaces.stream()
                .sorted(Comparator.comparingInt(SpaceId::value))
                .toList();
        this.pageStore = pageStore;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.pageSize = pageSize;
        this.registry = registry;
        this.redo = redo;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.truncationService = truncationService;
    }

    /**
     * 先逐个处理配置空间 page0，再建立 tail filter。任何配置空间未打开、类型不是 UNDO 或页头损坏都会 fail closed。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param scanner 恢复、checkpoint、doublewrite 或刷脏阶段的协作状态；不得为 {@code null}，阶段和持久化边界必须与当前实例的恢复状态机一致
     * @return {@code prepareDoublewrite} 实际完成的资源、绑定、页或槽位数量；未处理任何对象时为零，结果不得超过输入候选数
     */
    @Override
    public int prepareDoublewrite(DoublewriteRecoveryScanner scanner) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        truncating.clear();
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        preparedPageZeros.clear();
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        int repaired = 0;
        for (SpaceId spaceId : configuredUndoSpaces) {
            // pathOf 是显式“已打开”校验；不允许静默跳过配置空间，否则可能带未续作 marker 开放流量。
            pageStore.pathOf(spaceId);
            PageId page0 = PageId.of(spaceId, PageNo.of(0));
            if (scanner != null && scanner.repairPageIfNeeded(page0)) {
                repaired++;
            }
            preparedPageZeros.add(page0);
            readAndRegister(spaceId);
        }
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        return repaired;
    }

    /** page0 已准备则跳过；TRUNCATING 空间的 target 之后页属于已声明丢弃尾部，不允许 doublewrite 复活。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code shouldRepairDoublewritePage} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public boolean shouldRepairDoublewritePage(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("doublewrite candidate page id must not be null");
        }
        if (preparedPageZeros.contains(pageId)) {
            return false;
        }
        TablespaceLifecycleHeader marker = truncating.get(pageId.spaceId());
        return marker == null || pageId.pageNo().value() < marker.targetSizeInPages().value();
    }

    /**
     * 安装 redo 完整边界后重新扫描 page0，并按 SpaceId 顺序续作。重扫可发现 prepare 时尚未刷到 page0、
     * 但刚由 redo replay 写出的 TRUNCATING marker。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param recoveredToLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     */
    @Override
    public void resumeAfterRedo(Lsn recoveredToLsn) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        redo.restoreRecoveredBoundary(recoveredToLsn);
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        truncating.clear();
        for (SpaceId spaceId : configuredUndoSpaces) {
            readAndRegister(spaceId);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        List<Map.Entry<SpaceId, TablespaceLifecycleHeader>> work = new ArrayList<>(truncating.entrySet());
        work.sort(Map.Entry.comparingByKey(Comparator.comparingInt(SpaceId::value)));
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        for (Map.Entry<SpaceId, TablespaceLifecycleHeader> entry : work) {
            truncationService.truncate(entry.getKey(), entry.getValue().finishState());
        }
    }

    /**
     * 定位并读取存储引擎稳定 API领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @throws UndoTablespaceTruncationException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private void readAndRegister(SpaceId spaceId) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        ByteBuffer page0 = ByteBuffer.allocate(pageSize.bytes());
        pageStore.readPage(PageId.of(spaceId, PageNo.of(0)), page0);
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        SpaceHeaderPhysical physical = SpaceHeaderRawCodec.readPhysical(page0);
        if (!physical.spaceId().equals(spaceId)) {
            throw new UndoTablespaceTruncationException("configured UNDO page0 space id mismatch: expected="
                    + spaceId.value() + ", actual=" + physical.spaceId().value());
        }
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        TablespaceType type = TablespaceTypeFlags.decode(physical.spaceFlags());
        if (type != TablespaceType.UNDO) {
            throw new UndoTablespaceTruncationException("configured undo SpaceId is not UNDO on disk: "
                    + spaceId.value() + " type=" + type);
        }
        TablespaceLifecycleRawCodec.read(page0).ifPresent(lifecycle -> {
            if (lifecycle.state() == TablespaceState.TRUNCATING) {
                truncating.put(spaceId, lifecycle);
            }
        });
        // raw page0 已验证后刷新 Registry，使截断服务和最终普通准入观察同一磁盘状态。
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        registry.refresh(spaceId);
    }
}
