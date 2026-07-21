package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.repo.DictionaryControlStore;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 生产 DDL 的 clean manifest 发布协调器。
 *
 * <p>首次发布失败后以原异常作为 fence，后续 DDL 在任何 identity/catalog/file 副作用前失败。当前已经完成
 * durable DDL 的结果不回滚；重启时普通 catalog 仍是权威，DDL recovery 收敛后会用新协调器重新发布 clean。</p>
 */
public final class DictionaryRecoverySnapshotPublisher implements DictionaryCleanSnapshotPublisher {

    /** 当前权威字典快照来源。 */
    private final PersistentDictionaryRepository repository;
    /** 当前 durable control 高水位来源。 */
    private final DictionaryControlStore control;
    /** 独立 manifest event 仓储。 */
    private final DictionaryRecoveryManifestRepository manifest;
    /** ACTIVE binding 必须位于其中的规范目录。 */
    private final Path tablesDirectory;
    /** 首个 publish 失败；原子设置后只读，作为后续 DDL fail-closed fence。 */
    private final AtomicReference<RuntimeException> failure = new AtomicReference<>();

    /**
     * 绑定生产组合根中生命周期一致的 DD/control/manifest。
     *
     * @param repository 当前实例唯一 persistent DD repository
     * @param control 当前实例唯一 control store
     * @param manifest 当前实例唯一 recovery manifest repository
     * @param tablesDirectory 受控 file-per-table 根目录
     */
    public DictionaryRecoverySnapshotPublisher(
            PersistentDictionaryRepository repository,
            DictionaryControlStore control,
            DictionaryRecoveryManifestRepository manifest,
            Path tablesDirectory) {
        if (repository == null || control == null || manifest == null || tablesDirectory == null) {
            throw new DatabaseValidationException(
                    "dictionary recovery snapshot publisher collaborators/path must not be null");
        }
        this.repository = repository;
        this.control = control;
        this.manifest = manifest;
        this.tablesDirectory = tablesDirectory.toAbsolutePath().normalize();
    }

    /**
     * 在 DDL 前检查已有发布 fence。
     *
     * @throws DictionaryRecoveryManifestException 先前 clean publish 失败时抛出并保留原异常 cause
     */
    @Override
    public void assertAvailable() {
        RuntimeException previous = failure.get();
        if (previous != null) {
            throw new DictionaryRecoveryManifestException(
                    "dictionary recovery manifest is fenced after a prior clean publish failure", previous);
        }
    }

    /**
     * 从 repository writer fence 内读取 immutable DD/control 快照并交给 manifest 原子发布。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>检查历史 publish fence，然后取得 repository writer lock，阻止 intent/catalog commit 插入。</li>
     *     <li>若其它 DDL 正处于临时表状态，则不发布也不清除 intent；该 DDL 的终态发布负责最终收敛。</li>
     *     <li>稳定时在同一 writer fence 内读取 control 并 append clean，锁序固定为 repository→manifest。</li>
     *     <li>持久化/格式失败原子建立后续 DDL fence；并发临时状态跳过不是失败，不污染 fence。</li>
     * </ol>
     *
     * <p>调用方持有目标 DDL 的 MDL，当前 DDL 已进入稳定终态。失败不会清除之前的 mutation intent。</p>
     */
    @Override
    public void publish() {
        // 1. 已知 manifest 故障必须在再次取得 writer fence 前拒绝。
        assertAvailable();
        try {
            repository.withSnapshotWriterFence(snapshot -> {
                // 2. 并发 DDL 的持久临时状态说明全局字典尚未 clean；保留 intent 等待其终态发布。
                boolean transientState = snapshot.tables().values().stream().anyMatch(table ->
                        table.state() != TableState.ACTIVE
                                && table.state() != TableState.DISCARDED
                                && table.state() != TableState.RECOVERY_UNAVAILABLE
                                && table.state() != TableState.RECOVERY_DISCARDED
                                && table.state() != TableState.DROPPED);
                if (transientState) {
                    return null;
                }
                // 3. catalog mutation 使用相同 writer→manifest 锁序，clean 不可能越过未提交的 intent。
                manifest.publishCleanSnapshot(snapshot, control.snapshot(), tablesDirectory);
                return null;
            });
        } catch (RuntimeException publishFailure) {
            // 4. 真正的 manifest/编码失败会阻断下一次 DDL，避免 sidecar 继续静默落后。
            failure.compareAndSet(null, publishFailure);
            throw publishFailure;
        }
    }
}
