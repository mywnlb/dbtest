package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.ddl.DictionaryDdlLogStateException;
import cn.zhangyis.db.dd.ddl.DdlLogOperation;
import cn.zhangyis.db.dd.ddl.DdlLogPhase;
import cn.zhangyis.db.dd.ddl.DdlLogRecord;
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
    /** 只解释 DDL_LOG(7) 单记录批次的稳定 v1 codec。 */
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
            if (current == null || current.phase() != expected || !allowed(current.operation(), expected, next)) {
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

    private void appendAndPublish(DdlLogRecord record) {
        store.append(List.of(codec.encode(record)));
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
            } else {
                requireSameIdentity(previous, record);
                if (!allowed(previous.operation(), previous.phase(), record.phase())) {
                    throw new DictionaryCatalogCorruptionException("illegal durable DDL phase transition: ddl="
                            + id.value() + " " + previous.phase() + " -> " + record.phase());
                }
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
                || !before.fileIdentity().equals(after.fileIdentity())) {
            throw new DictionaryCatalogCorruptionException(
                    "DDL log identity changed across phases: " + before.marker().ddlOperationId());
        }
    }

    private static boolean allowed(DdlLogOperation operation, DdlLogPhase from, DdlLogPhase to) {
        if (from.terminal()) {
            return false;
        }
        return switch (operation) {
            case CREATE_TABLE, CREATE_INDEX, REBUILD_TABLE -> (from == DdlLogPhase.PREPARED
                    && (to == DdlLogPhase.ENGINE_DONE || to == DdlLogPhase.ROLLED_BACK))
                    || (from == DdlLogPhase.ENGINE_DONE
                    && (to == DdlLogPhase.DICTIONARY_COMMITTED || to == DdlLogPhase.ROLLED_BACK))
                    || (from == DdlLogPhase.DICTIONARY_COMMITTED && to == DdlLogPhase.COMMITTED);
            case DROP_TABLE, DROP_INDEX, DISCARD_TABLESPACE, IMPORT_TABLESPACE -> (from == DdlLogPhase.PREPARED
                    && (to == DdlLogPhase.DICTIONARY_COMMITTED || to == DdlLogPhase.ROLLED_BACK))
                    || (from == DdlLogPhase.DICTIONARY_COMMITTED && to == DdlLogPhase.ENGINE_DONE)
                    || (from == DdlLogPhase.ENGINE_DONE && to == DdlLogPhase.COMMITTED);
        };
    }
}
