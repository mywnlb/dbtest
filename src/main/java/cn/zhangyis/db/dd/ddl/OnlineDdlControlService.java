package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DdlId;
import cn.zhangyis.db.dd.repo.PersistentDdlLogRepository;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTablePhase;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Online DDL稳定Java/admin facade。只读方法合并轻量registry与marker，不建立用户事务、不获取table MDL、
 * 不打开row-log/表空间；取消先通过prepare handoff，再以repository CAS形成唯一durable方向。
 */
@Slf4j
public final class OnlineDdlControlService {

    /** durable phase/control的只读与CAS owner。 */
    private final PersistentDdlLogRepository logs;
    /** 本进程live/recovery runtime与有界terminal history。 */
    private final OnlineDdlOperationRegistry registry;
    /** durable CAS 锁外唤醒 live pending wait的窄端口。 */
    private final OnlineDdlCancelWakeup cancelWakeup;

    /**
     * @param logs 与DictionaryDdlService共享的persistent marker repository
     * @param registry DatabaseEngine唯一创建并由live/recovery共享的tracker registry
     */
    public OnlineDdlControlService(
            PersistentDdlLogRepository logs, OnlineDdlOperationRegistry registry) {
        this(logs, registry, OnlineDdlCancelWakeup.NO_OP);
    }

    /**
     * 构造带 live wait 唤醒的生产控制面。回调只在 durable cancel 已成功且 repository 锁已释放后执行。
     *
     * @param logs 与 DDL coordinator 共享的 persistent marker repository
     * @param registry 组合根唯一拥有的 live/recovery tracker registry
     * @param cancelWakeup 精确取消 pending wait 的无持久副作窄端口
     */
    public OnlineDdlControlService(
            PersistentDdlLogRepository logs, OnlineDdlOperationRegistry registry,
            OnlineDdlCancelWakeup cancelWakeup) {
        if (logs == null || registry == null || cancelWakeup == null) {
            throw new DatabaseValidationException(
                    "Online DDL control repository/registry/wakeup must not be null");
        }
        this.logs = logs;
        this.registry = registry;
        this.cancelWakeup = cancelWakeup;
    }

    /**
     * 返回active、进程内history和durable online marker的合并弱一致视图。
     *
     * @return 按DDL id升序的不可变snapshot列表
     */
    public List<OnlineDdlOperationSnapshot> list() {
        Map<DdlId, OnlineDdlOperationSnapshot> merged = new LinkedHashMap<>();
        for (OnlineDdlOperationSnapshot snapshot : registry.list()) {
            merged.put(snapshot.identity().ddlId(), snapshot);
        }
        for (DdlLogRecord record : logs.records()) {
            if (!record.executionProtocol().cancelCapable()) {
                continue;
            }
            DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
            Optional<OnlineDdlOperationTracker> active = registry.tracker(ddlId);
            if (active.isPresent()) {
                OnlineDdlOperationTracker tracker = active.orElseThrow();
                tracker.observeDurable(record);
                merged.put(ddlId, tracker.snapshot());
            } else {
                merged.putIfAbsent(ddlId, durableOnly(record));
            }
        }
        List<OnlineDdlOperationSnapshot> result = new ArrayList<>(merged.values());
        result.sort(Comparator.comparingLong(snapshot -> snapshot.identity().ddlId().value()));
        return List.copyOf(result);
    }

    /**
     * 查询单个Online DDL，不读取物理文件；active tracker会先用当前marker刷新durable字段。
     *
     * @param ddlId operation identity
     * @return active/history/durable-only snapshot；不存在或不是online protocol时为空
     */
    public Optional<OnlineDdlOperationSnapshot> find(DdlId ddlId) {
        requireId(ddlId);
        Optional<DdlLogRecord> durable = logs.find(ddlId);
        Optional<OnlineDdlOperationTracker> active = registry.tracker(ddlId);
        if (active.isPresent()) {
            durable.ifPresent(active.orElseThrow()::observeDurable);
            return Optional.of(active.orElseThrow().snapshot());
        }
        Optional<OnlineDdlOperationSnapshot> process = registry.find(ddlId);
        if (process.isPresent()) {
            return process;
        }
        return durable.filter(record -> record.executionProtocol().cancelCapable())
                .map(OnlineDdlControlService::durableOnly);
    }

    /**
     * 请求取消一个Online DDL。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先验证admin/system权限与deadline；未授权请求不查询identity，避免控制面信息泄漏。</li>
     *     <li>若live tracker仍在prepare handoff，cancel可在无durable资源时胜出；PREPARING则有界等待append结果。</li>
     *     <li>marker存在时只以PREPARED/OPEN执行CANCEL_REQUESTED CAS；失败使用锁内observed record分类。</li>
     *     <li>刷新轻量tracker并返回稳定结果；本方法不在repository锁内回调gate、MDL或row-log。</li>
     * </ol>
     *
     * @param ddlId 目标Online DDL identity
     * @param request 已由上层构造的固定宽度取消请求
     * @param timeout prepare handoff的正等待上界；catalog force失败直接抛保留cause的领域异常
     * @return 稳定取消结果与可选snapshot
     * @throws OnlineDdlControlAccessException 请求未获admin/system授权时抛出，且不查询或修改operation
     * @throws DictionaryDdlLogStateException marker protocol不支持取消或状态损坏时抛出
     */
    public OnlineDdlCancelResult requestCancel(
            DdlId ddlId, OnlineDdlCancelRequest request, Duration timeout) {
        // 1、权限必须先于identity lookup；timeout只约束可中断的prepare handoff，不能伪装FileChannel.force已取消。
        requireId(ddlId);
        if (request == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("Online DDL cancel request/timeout is invalid");
        }
        if (!request.privileged()) {
            throw new OnlineDdlControlAccessException(
                    "Online DDL cancellation requires admin/system privilege");
        }

        // 2、尚未append marker时只改变tracker handoff；若coordinator已开始append，则等待其明确发布durable/failed结果。
        Optional<OnlineDdlOperationTracker> active = registry.tracker(ddlId);
        if (active.isPresent()
                && active.orElseThrow().requestCancelBeforePrepare(timeout)) {
            return new OnlineDdlCancelResult(
                    OnlineDdlCancelOutcome.ACCEPTED_BEFORE_PREPARE,
                    Optional.of(active.orElseThrow().snapshot()));
        }

        // 3、没有marker时可能是本进程history或未知identity；terminal history与NOT_FOUND严格区分。
        DdlLogRecord current = logs.find(ddlId).orElse(null);
        if (current == null) {
            Optional<OnlineDdlOperationSnapshot> process = registry.find(ddlId);
            if (process.isPresent() && process.orElseThrow().terminal()) {
                return new OnlineDdlCancelResult(
                        OnlineDdlCancelOutcome.TERMINAL, process);
            }
            return new OnlineDdlCancelResult(
                    OnlineDdlCancelOutcome.NOT_FOUND, Optional.empty());
        }
        if (!current.executionProtocol().cancelCapable()) {
            throw new DictionaryDdlLogStateException(
                    "DDL execution protocol is not cancel-capable: ddl=" + ddlId.value()
                            + " protocol=" + current.executionProtocol());
        }
        if (current.phase().terminal()) {
            return result(OnlineDdlCancelOutcome.TERMINAL, current, active);
        }
        if (current.controlState() == DdlControlState.CANCEL_REQUESTED) {
            return result(OnlineDdlCancelOutcome.ALREADY_REQUESTED, current, active);
        }
        if (current.controlState() == DdlControlState.FORWARD_ONLY
                || current.phase() != DdlLogPhase.PREPARED) {
            return result(OnlineDdlCancelOutcome.TOO_LATE_FORWARD_ONLY, current, active);
        }
        DdlControlCasResult cas = logs.compareAndSetControl(
                ddlId, DdlLogPhase.PREPARED, DdlControlState.OPEN,
                DdlControlState.CANCEL_REQUESTED,
                Optional.of(new DdlCancellation(
                        request.reasonCode(), System.currentTimeMillis(), request.requesterId())));
        DdlLogRecord observed = cas.observedRecord();
        OnlineDdlCancelOutcome outcome = cas.changed()
                ? OnlineDdlCancelOutcome.ACCEPTED_DURABLE
                : switch (observed.controlState()) {
                    case CANCEL_REQUESTED -> OnlineDdlCancelOutcome.ALREADY_REQUESTED;
                    case FORWARD_ONLY -> OnlineDdlCancelOutcome.TOO_LATE_FORWARD_ONLY;
                    case OPEN -> {
                        if (observed.phase().terminal()) {
                            yield OnlineDdlCancelOutcome.TERMINAL;
                        }
                        throw new DictionaryDdlLogStateException(
                                "Online DDL control CAS failed without a competing direction: ddl="
                                        + ddlId.value());
                    }
                };

        // 4、repository锁已释放，刷新diagnostic tracker不会形成control→gate/file锁序。
        return result(outcome, observed, active);
    }

    /**
     * 有界等待本进程operation terminal；重启遗留非终态marker没有live tracker时返回empty供调用方轮询。
     *
     * @param ddlId 目标identity
     * @param timeout 正等待上界
     * @return terminal snapshot；未知、非本进程active或超时为空
     */
    public Optional<OnlineDdlOperationSnapshot> awaitTerminal(
            DdlId ddlId, Duration timeout) {
        requireId(ddlId);
        Optional<DdlLogRecord> current = logs.find(ddlId);
        if (current.isPresent() && current.orElseThrow().phase().terminal()) {
            return Optional.of(durableOnly(current.orElseThrow()));
        }
        return registry.awaitTerminal(ddlId, timeout);
    }

    private OnlineDdlCancelResult result(
            OnlineDdlCancelOutcome outcome, DdlLogRecord record,
            Optional<OnlineDdlOperationTracker> tracker) {
        OnlineDdlOperationSnapshot snapshot;
        if (tracker.isPresent()) {
            tracker.orElseThrow().observeDurable(record);
            snapshot = tracker.orElseThrow().snapshot();
        } else {
            snapshot = durableOnly(record);
        }
        if (tracker.isPresent()
                && (outcome == OnlineDdlCancelOutcome.ACCEPTED_DURABLE
                || outcome == OnlineDdlCancelOutcome.ALREADY_REQUESTED)) {
            try {
                cancelWakeup.wake(snapshot.identity());
            } catch (RuntimeException wakeFailure) {
                // durable marker已是恢复真相；唤醒失败只影响本进程退出延迟，不能把已接受取消改报为失败。
                log.warn("Online DDL durable cancel wakeup failed: ddl={} owner={}",
                        snapshot.identity().ddlId().value(), snapshot.identity().ownerId(), wakeFailure);
            }
        }
        return new OnlineDdlCancelResult(outcome, Optional.of(snapshot));
    }

    /** 从单条marker构造不声称拥有runtime/gate/file详情的durable-only恢复视图。 */
    private static OnlineDdlOperationSnapshot durableOnly(DdlLogRecord record) {
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        OnlineDdlTerminalResult terminal = switch (record.phase()) {
            case COMMITTED -> OnlineDdlTerminalResult.COMPLETED;
            case ROLLED_BACK -> OnlineDdlTerminalResult.ROLLED_BACK;
            default -> OnlineDdlTerminalResult.NONE;
        };
        OnlineDdlRuntimePhase runtime = switch (terminal) {
            case COMPLETED -> OnlineDdlRuntimePhase.COMPLETED;
            case ROLLED_BACK -> OnlineDdlRuntimePhase.ROLLED_BACK;
            case FAILED_CLOSED -> OnlineDdlRuntimePhase.FAILED_CLOSED;
            case NONE -> record.controlState() == DdlControlState.FORWARD_ONLY
                    ? OnlineDdlRuntimePhase.FORWARD_FENCED
                    : record.controlState() == DdlControlState.CANCEL_REQUESTED
                    ? OnlineDdlRuntimePhase.ABORTING
                    : OnlineDdlRuntimePhase.RECOVERING_SOURCE;
        };
        boolean indexOperation = record.operation() == DdlLogOperation.CREATE_INDEX
                || record.operation() == DdlLogOperation.DROP_INDEX;
        OnlineDdlOperationIdentity identity = new OnlineDdlOperationIdentity(
                ddlId, record.operation(), record.marker().affectedObjectId(),
                indexOperation ? record.secondaryObjectId() : 0L, "", "", 0,
                record.marker().dictionaryVersion(), 0, true, OptionalLong.empty());
        long now = System.currentTimeMillis();
        return new OnlineDdlOperationSnapshot(
                1, identity, runtime, OnlineDdlTablePhase.ABSENT, now, now,
                OnlineDdlWaitReason.NONE, Optional.of(record.phase()),
                record.controlState(), 0,
                record.cancellation().map(DdlCancellation::reasonCode),
                0, 0, OptionalLong.empty(), Optional.empty(),
                0, 0, 0, 0, 0, 0, 0, 0, 0,
                record.executionProtocol().cancelCapable(),
                record.retirementFence().isPresent(), false, terminal,
                Optional.empty(), record.controlState() == DdlControlState.FORWARD_ONLY
                && !record.phase().terminal());
    }

    private static void requireId(DdlId ddlId) {
        if (ddlId == null) {
            throw new DatabaseValidationException("Online DDL identity must not be null");
        }
    }
}
