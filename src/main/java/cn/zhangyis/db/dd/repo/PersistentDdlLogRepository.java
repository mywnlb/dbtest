package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.ddl.DdlCancellation;
import cn.zhangyis.db.dd.ddl.DdlControlCasResult;
import cn.zhangyis.db.dd.ddl.DdlControlState;
import cn.zhangyis.db.dd.ddl.DictionaryDdlLogStateException;
import cn.zhangyis.db.dd.ddl.DdlExecutionProtocol;
import cn.zhangyis.db.dd.ddl.DdlLogOperation;
import cn.zhangyis.db.dd.ddl.DdlLogPhase;
import cn.zhangyis.db.dd.ddl.DdlLogRecord;
import cn.zhangyis.db.dd.ddl.DdlRetirementFence;
import cn.zhangyis.db.dd.domain.DdlId;
import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogStore;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * append-only DDL phase repository。writerLock 只覆盖 expected-phase 校验、单 batch append 与快照替换；
 * 不跨越 MDL、物理 DDL、purge barrier 或 dictionary transaction。
 */
public final class PersistentDdlLogRepository {

    /** DDL phase batch 与普通 DD batch 共享的 durable append/read 边界。 */
    private final InternalCatalogStore store;
    /** 解释 DDL_LOG(7) 的 v1-v5 格式；v4 起一个逻辑 marker 可以占用同一 batch 中的多个 chunk。 */
    private final DdlLogCatalogCodec codec = new DdlLogCatalogCodec();
    /** 同一进程 DDL phase CAS 的唯一 writer 临界区。 */
    private final ReentrantLock writerLock = new ReentrantLock();
    /** 按 ddl id 保存最新 phase 的不可变快照。 */
    private volatile Map<DdlId, DdlLogRecord> latest;

    /**
     * 从全部 durable catalog batches 重建并校验 DDL 状态机。
     *
     * @param store 与普通 DD repository 共享的稳定 catalog store。
     * @throws DatabaseValidationException store 为空时抛出。
     * @throws DictionaryCatalogCorruptionException durable DDL history 非法时抛出并阻止启动。
     */
    public PersistentDdlLogRepository(InternalCatalogStore store) {
        if (store == null) {
            throw new DatabaseValidationException("DDL log catalog store must not be null");
        }
        this.store = store;
        this.latest = rebuild();
    }

    /**
     * 发布新 DDL 的 PREPARED marker。
     *
     * @param prepared phase 必须为 PREPARED 且 ddl id 尚不存在。
     * @return durable 后发布的同一不可变 marker。
     * @throws DatabaseValidationException 输入为空时抛出，且不写 catalog。
     * @throws DictionaryDdlLogStateException 重复 identity 或非 PREPARED 输入时抛出。
     */
    public DdlLogRecord prepare(DdlLogRecord prepared) {
        if (prepared == null) {
            throw new DatabaseValidationException("prepared DDL log record must not be null");
        }
        writerLock.lock();
        try {
            if (prepared.phase() != DdlLogPhase.PREPARED
                    || latest.containsKey(DdlId.of(prepared.marker().ddlOperationId()))) {
                throw new DictionaryDdlLogStateException(
                        "DDL log prepare requires unused id and PREPARED phase: "
                                + prepared.marker().ddlOperationId());
            }
            requireProductionPrepare(prepared);
            appendAndPublish(prepared);
            return prepared;
        } finally {
            writerLock.unlock();
        }
    }

    /**
     * 以 expected phase CAS 推进一个 operation-specific durable 阶段。
     *
     * @param ddlId DDL identity。
     * @param expected 调用方观察并要求仍为当前值的阶段。
     * @param next operation 状态机允许的直接后继。
     * @return durable 后的新阶段 record。
     * @throws DatabaseValidationException 任一 identity/phase 输入为空时抛出，且不写 catalog。
     * @throws DictionaryDdlLogStateException identity 缺失、expected 不匹配、非法跳转或终态推进时抛出。
     */
    public DdlLogRecord transition(DdlId ddlId, DdlLogPhase expected, DdlLogPhase next) {
        if (ddlId == null || expected == null || next == null) {
            throw new DatabaseValidationException("DDL transition identity/phases must not be null");
        }
        writerLock.lock();
        try {
            DdlLogRecord current = latest.get(ddlId);
            if (current == null || current.phase() != expected || !allowed(current, expected, next)) {
                throw new DictionaryDdlLogStateException("invalid DDL log transition: ddl=" + ddlId.value()
                        + " expected=" + expected + " actual=" + (current == null ? "ABSENT" : current.phase())
                        + " next=" + next);
            }
            DdlLogRecord advanced = current.withPhase(next);
            appendAndPublish(advanced);
            return advanced;
        } finally {
            writerLock.unlock();
        }
    }

    /**
     * 在线DDL在PREPARED窗口内以同一个writer fence竞争取消或前滚方向。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在writer lock内读取latest，先证明marker属于可取消protocol且仍处于非终态PREPARED。</li>
     *     <li>比较调用方观察的phase/control；不匹配时不写catalog，并返回锁内观察到的完整record。</li>
     *     <li>只允许OPEN单调进入CANCEL_REQUESTED或FORWARD_ONLY，并校验取消payload的交叉约束。</li>
     *     <li>把完整当前版本 marker 作为一个原子 catalog batch 持久化后替换内存快照，形成唯一线性化结果。</li>
     * </ol>
     *
     * @param ddlId 已经prepare且由在线DDL runtime持有的正identity
     * @param expectedPhase 调用方要求仍为PREPARED的phase观察值；其它phase没有取消窗口
     * @param expectedControl 调用方要求仍为当前值的control；正常竞争固定为OPEN
     * @param nextControl 竞争者申请的最终方向，只允许CANCEL_REQUESTED或FORWARD_ONLY
     * @param cancellation 取消胜出时必须存在的固定宽度诊断；前滚时必须为空
     * @return 是否成功追加，以及writer fence内最终观察到的完整marker
     * @throws DatabaseValidationException 参数为空时抛出，catalog保持不变
     * @throws DictionaryDdlLogStateException marker缺失、protocol不支持取消、终态或转换方向非法时抛出
     */
    public DdlControlCasResult compareAndSetControl(
            DdlId ddlId,
            DdlLogPhase expectedPhase,
            DdlControlState expectedControl,
            DdlControlState nextControl,
            Optional<DdlCancellation> cancellation) {
        if (ddlId == null || expectedPhase == null || expectedControl == null
                || nextControl == null || cancellation == null) {
            throw new DatabaseValidationException("DDL control CAS fields must not be null");
        }
        writerLock.lock();
        try {
            // 1、控制位只属于可取消在线协议，并且当前实现把唯一竞争窗口固定在PREPARED。
            DdlLogRecord current = latest.get(ddlId);
            if (current == null || !current.executionProtocol().cancelCapable()
                    || current.phase().terminal() || expectedPhase != DdlLogPhase.PREPARED) {
                throw new DictionaryDdlLogStateException(
                        "DDL control CAS requires cancel-capable PREPARED marker: ddl=" + ddlId.value());
            }

            // 2、CAS失败方直接获得锁内权威record，避免二次无锁读取后误判竞争方向。
            if (current.phase() != expectedPhase || current.controlState() != expectedControl) {
                return new DdlControlCasResult(false, current);
            }

            // 3、OPEN只有两个最终后继；DdlLogRecord构造器再强制cancel payload与control同现。
            if (expectedControl != DdlControlState.OPEN
                    || nextControl == DdlControlState.OPEN
                    || (nextControl == DdlControlState.CANCEL_REQUESTED) != cancellation.isPresent()) {
                throw new DictionaryDdlLogStateException(
                        "invalid DDL control transition: ddl=" + ddlId.value()
                                + " " + expectedControl + " -> " + nextControl);
            }
            DdlLogRecord advanced = current.withControl(nextControl, cancellation);

            // 4、append完成才发布latest；append失败时内存仍指向旧OPEN marker，调用方可重试或恢复。
            appendAndPublish(advanced);
            return new DdlControlCasResult(true, advanced);
        } finally {
            writerLock.unlock();
        }
    }

    /**
     * 为在线DROP/shadow swap安装一次不可变的延迟回收边界。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在writer fence内核对expected phase/control，防止把旧资源集合绑定到已经变化的方向。</li>
     *     <li>要求fence此前不存在且owner/table identity与marker一致，拒绝覆盖第一次持久证据。</li>
     *     <li>以完整当前版本 marker 单 batch append，成功后才发布新的 latest 快照。</li>
     * </ol>
     *
     * @param ddlId 拥有待退休资源descriptor的DDL identity
     * @param expectedPhase 捕获fence时调用方持有的phase
     * @param expectedControl 捕获fence时调用方持有的control；协议只接受OPEN，方向裁决后禁止补写
     * @param fence final X下冻结且资源有序去重的持久回收边界
     * @return durable后包含fence的完整marker
     * @throws DatabaseValidationException 参数为空时抛出，catalog保持不变
     * @throws DictionaryDdlLogStateException identity不匹配、CAS观察失效或重复安装时抛出
     */
    public DdlLogRecord installRetirementFence(
            DdlId ddlId,
            DdlLogPhase expectedPhase,
            DdlControlState expectedControl,
            DdlRetirementFence fence) {
        if (ddlId == null || expectedPhase == null || expectedControl == null || fence == null) {
            throw new DatabaseValidationException("DDL retirement fence CAS fields must not be null");
        }
        writerLock.lock();
        try {
            // 1、phase/control共同标识捕获点；任一漂移都必须由调用方重新分类，不能覆盖最新证据。
            DdlLogRecord current = latest.get(ddlId);
            if (current == null || current.phase() != expectedPhase
                    || current.controlState() != expectedControl || current.phase().terminal()
                    || current.phase() != DdlLogPhase.PREPARED
                    || current.controlState() != DdlControlState.OPEN) {
                throw new DictionaryDdlLogStateException(
                        "DDL retirement fence CAS observation changed: ddl=" + ddlId.value());
            }

            // 2、fence只能从absent写一次，并绑定同一DDL/table；resource语义由具体operation在调用前验证。
            if (current.retirementFence().isPresent()
                    || fence.ownerDdlId() != ddlId.value()
                    || fence.tableId() != current.marker().affectedObjectId()) {
                throw new DictionaryDdlLogStateException(
                        "DDL retirement fence identity/rewrite is invalid: ddl=" + ddlId.value());
            }

            // 3、持久化失败不改变latest，避免内存宣称已有fence而恢复日志缺失。
            DdlLogRecord advanced = current.withRetirementFence(fence);
            appendAndPublish(advanced);
            return advanced;
        } finally {
            writerLock.unlock();
        }
    }

    /**
     * 查询指定 DDL 的最新 durable phase。
     *
     * @param ddlId control 分配且可能出现在 marker history 中的正 identity。
     * @return 最新不可变 marker；identity 尚未 prepare 时为空。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public Optional<DdlLogRecord> find(DdlId ddlId) {
        if (ddlId == null) {
            throw new DatabaseValidationException("DDL log lookup id must not be null");
        }
        return Optional.ofNullable(latest.get(ddlId));
    }

    /**
     * 冻结当前全部非终态 marker。
     *
     * @return 按 ddl id 升序排列的不可变列表，供启动恢复单线程消费；没有待恢复 DDL 时为空列表。
     */
    public List<DdlLogRecord> unresolved() {
        return latest.values().stream().filter(record -> !record.phase().terminal())
                .sorted(Comparator.comparingLong(record -> record.marker().ddlOperationId())).toList();
    }

    /**
     * 读取 durable history 的 DDL identity 高水位证据。
     *
     * @return 最大 ddl id；空日志返回 0，调用方须转换为 next-id 并结合未记录 marker 的 DDL 下界。
     */
    public long highestDdlId() {
        return latest.keySet().stream().mapToLong(DdlId::value).max().orElse(0L);
    }

    /**
     * 冻结全部DDL identity的最新durable marker，供只读admin facade合并active与terminal投影。
     *
     * @return 按DDL id升序的不可变完整record列表；不读取物理文件或获取MDL
     */
    public List<DdlLogRecord> records() {
        return latest.values().stream()
                .sorted(Comparator.comparingLong(record -> record.marker().ddlOperationId()))
                .toList();
    }

    private void appendAndPublish(DdlLogRecord record) {
        store.append(codec.encode(record));
        Map<DdlId, DdlLogRecord> copy = new LinkedHashMap<>(latest);
        copy.put(DdlId.of(record.marker().ddlOperationId()), record);
        latest = Map.copyOf(copy);
    }

    private Map<DdlId, DdlLogRecord> rebuild() {
        Map<DdlId, DdlLogRecord> rebuilt = new LinkedHashMap<>();
        store.readCommittedBatches().forEach(batch -> codec.decode(batch).ifPresent(record -> {
            DdlId id = DdlId.of(record.marker().ddlOperationId());
            DdlLogRecord previous = rebuilt.get(id);
            if (previous == null) {
                if (record.phase() != DdlLogPhase.PREPARED) {
                    throw new DictionaryCatalogCorruptionException(
                            "DDL history must start at PREPARED: " + id.value());
                }
                if (record.executionProtocol() != DdlExecutionProtocol.LEGACY_PHASE_ONLY) {
                    try {
                        // 启动重放必须与live prepare执行同一checkpoint/control门禁，不能因进程重启接受非法首帧。
                        requireProductionPrepare(record);
                    } catch (DictionaryDdlLogStateException invalidPrepare) {
                        throw new DictionaryCatalogCorruptionException(
                                "invalid initial DDL v4 marker: " + id.value(), invalidPrepare);
                    }
                }
            } else {
                requireSameIdentity(previous, record);
                requireMonotonicSuccessor(previous, record);
            }
            rebuilt.put(id, record);
        }));
        return Map.copyOf(rebuilt);
    }

    private static void requireSameIdentity(DdlLogRecord before, DdlLogRecord after) {
        if (!before.marker().equals(after.marker()) || before.operation() != after.operation()
                || before.secondaryObjectId() != after.secondaryObjectId()
                || !before.spaceId().equals(after.spaceId()) || !before.path().equals(after.path())
                || !before.auxiliaryPath().equals(after.auxiliaryPath())
                || !before.fileIdentity().equals(after.fileIdentity())
                || before.executionProtocol() != after.executionProtocol()
                || !before.sourceSchemaDigest().equals(after.sourceSchemaDigest())
                || !before.intermediateSchemaDigest().equals(after.intermediateSchemaDigest())
                || !before.targetSchemaDigest().equals(after.targetSchemaDigest())
                || !before.batchManifest().equals(after.batchManifest())) {
            throw new DictionaryCatalogCorruptionException(
                    "DDL log identity changed across phases: " + before.marker().ddlOperationId());
        }
    }

    /** 校验 prepare 时的当前 marker 初态与逐 operation schema checkpoint 策略。 */
    private static void requireProductionPrepare(DdlLogRecord record) {
        if (record.executionProtocol() == DdlExecutionProtocol.LEGACY_PHASE_ONLY
                || record.controlState() != DdlControlState.OPEN
                || record.cancellation().isPresent() || record.retirementFence().isPresent()) {
            throw new DictionaryDdlLogStateException(
                    "new DDL marker requires v4 protocol and OPEN/empty monotonic fields: ddl="
                            + record.marker().ddlOperationId());
        }
        boolean source = record.sourceSchemaDigest().isPresent();
        boolean intermediate = record.intermediateSchemaDigest().isPresent();
        boolean target = record.targetSchemaDigest().isPresent();
        boolean valid = switch (record.operation()) {
            case CREATE_TABLE -> !source && !intermediate && target;
            case DROP_TABLE, DISCARD_TABLESPACE, IMPORT_TABLESPACE -> source && intermediate && target;
            case CREATE_INDEX, DROP_INDEX, REBUILD_TABLE, ALTER_TABLE_INPLACE,
                 DISCARD_RECOVERY_UNAVAILABLE, DROP_RECOVERY_UNAVAILABLE,
                 IMPORT_RECOVERY_REPLACEMENT -> source && !intermediate && target;
            case DROP_TABLE_BATCH -> !source && !intermediate && !target
                    && record.batchManifest().isPresent()
                    && record.batchManifest().orElseThrow().schema().isEmpty()
                    && !record.batchManifest().orElseThrow().tables().isEmpty();
            case DROP_SCHEMA_CASCADE -> !source && !intermediate && !target
                    && record.batchManifest().isPresent()
                    && record.batchManifest().orElseThrow().schema().isPresent();
        };
        if (!valid) {
            throw new DictionaryDdlLogStateException(
                    "DDL marker violates operation schema checkpoint policy: ddl="
                            + record.marker().ddlOperationId() + " operation=" + record.operation());
        }
    }

    /** 区分同phase的control/fence追加与跨phase推进，并拒绝任意字段回退或合并改写。 */
    private static void requireMonotonicSuccessor(DdlLogRecord before, DdlLogRecord after) {
        boolean phaseChanged = before.phase() != after.phase();
        boolean controlChanged = before.controlState() != after.controlState()
                || !before.cancellation().equals(after.cancellation());
        boolean fenceChanged = !before.retirementFence().equals(after.retirementFence());
        if (phaseChanged) {
            if (controlChanged || fenceChanged || !allowed(before, before.phase(), after.phase())) {
                throw illegalHistory(before, after);
            }
            return;
        }
        if (controlChanged == fenceChanged) {
            throw illegalHistory(before, after);
        }
        if (controlChanged) {
            boolean validControl = before.executionProtocol().cancelCapable()
                    && before.phase() == DdlLogPhase.PREPARED
                    && before.controlState() == DdlControlState.OPEN
                    && after.controlState() != DdlControlState.OPEN
                    && ((after.controlState() == DdlControlState.CANCEL_REQUESTED)
                    == after.cancellation().isPresent())
                    && before.retirementFence().equals(after.retirementFence());
            if (!validControl) {
                throw illegalHistory(before, after);
            }
            return;
        }
        boolean validFence = before.retirementFence().isEmpty()
                && after.retirementFence().isPresent()
                && before.phase() == DdlLogPhase.PREPARED
                && before.controlState() == DdlControlState.OPEN
                && before.controlState() == after.controlState()
                && before.cancellation().equals(after.cancellation());
        if (!validFence) {
            throw illegalHistory(before, after);
        }
    }

    /** 构造包含完整前后状态的启动期损坏异常。 */
    private static DictionaryCatalogCorruptionException illegalHistory(
            DdlLogRecord before, DdlLogRecord after) {
        return new DictionaryCatalogCorruptionException("illegal durable DDL marker transition: ddl="
                + before.marker().ddlOperationId() + " phase=" + before.phase() + "->" + after.phase()
                + " control=" + before.controlState() + "->" + after.controlState());
    }

    /** operation phase图与control×phase交叉约束。 */
    private static boolean allowed(DdlLogRecord record, DdlLogPhase from, DdlLogPhase to) {
        if (from.terminal()) {
            return false;
        }
        if (record.controlState() == DdlControlState.CANCEL_REQUESTED) {
            return from == DdlLogPhase.PREPARED && to == DdlLogPhase.ROLLED_BACK;
        }
        if (record.controlState() == DdlControlState.FORWARD_ONLY && to == DdlLogPhase.ROLLED_BACK) {
            return false;
        }
        if (record.executionProtocol().cancelCapable() && from == DdlLogPhase.PREPARED
                && to != DdlLogPhase.ROLLED_BACK
                && record.controlState() != DdlControlState.FORWARD_ONLY) {
            return false;
        }
        return switch (record.operation()) {
            case CREATE_TABLE, CREATE_INDEX, REBUILD_TABLE, DISCARD_RECOVERY_UNAVAILABLE,
                 DROP_RECOVERY_UNAVAILABLE, IMPORT_RECOVERY_REPLACEMENT -> (from == DdlLogPhase.PREPARED
                    && (to == DdlLogPhase.ENGINE_DONE || to == DdlLogPhase.ROLLED_BACK))
                    || (from == DdlLogPhase.ENGINE_DONE
                    && (to == DdlLogPhase.DICTIONARY_COMMITTED || to == DdlLogPhase.ROLLED_BACK))
                    || (from == DdlLogPhase.DICTIONARY_COMMITTED && to == DdlLogPhase.COMMITTED);
            case DROP_TABLE, DROP_INDEX, DISCARD_TABLESPACE, IMPORT_TABLESPACE,
                 DROP_TABLE_BATCH, DROP_SCHEMA_CASCADE -> (from == DdlLogPhase.PREPARED
                    && (to == DdlLogPhase.DICTIONARY_COMMITTED || to == DdlLogPhase.ROLLED_BACK))
                    || (from == DdlLogPhase.DICTIONARY_COMMITTED && to == DdlLogPhase.ENGINE_DONE)
                    || (from == DdlLogPhase.ENGINE_DONE && to == DdlLogPhase.COMMITTED);
            case ALTER_TABLE_INPLACE -> record.executionProtocol()
                    == cn.zhangyis.db.dd.ddl.DdlExecutionProtocol.ONLINE_ALTER_INPLACE_V1
                    && record.auxiliaryPath().isPresent()
                    ? (from == DdlLogPhase.PREPARED
                    && (to == DdlLogPhase.ENGINE_DONE || to == DdlLogPhase.ROLLED_BACK))
                    || (from == DdlLogPhase.ENGINE_DONE
                    && to == DdlLogPhase.DICTIONARY_COMMITTED)
                    || (from == DdlLogPhase.DICTIONARY_COMMITTED
                    && to == DdlLogPhase.COMMITTED)
                    : (from == DdlLogPhase.PREPARED
                    && (to == DdlLogPhase.DICTIONARY_COMMITTED || to == DdlLogPhase.ROLLED_BACK))
                    || (from == DdlLogPhase.DICTIONARY_COMMITTED && to == DdlLogPhase.COMMITTED);
        };
    }
}
