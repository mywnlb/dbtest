package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.repo.DictionaryControlStore;
import cn.zhangyis.db.dd.repo.DictionaryControlSnapshot;
import cn.zhangyis.db.dd.repo.DictionaryDurabilityWitness;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.fil.catalog.FileDictionaryRecoveryManifestStore;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * clean snapshot 发布与 catalog writer 的锁序测试。
 */
class DictionaryRecoverySnapshotPublisherTest {

    @TempDir
    Path directory;

    /**
     * mutation 已取得 repository writer lock、但 intent 尚未 append 时，clean publisher 必须等待；
     * commit 完成后发布的 clean archive 必须包含新对象并裁决该 intent。
     */
    @Test
    void writerFencePreventsCleanSnapshotFromOvertakingMutationIntent() throws Exception {
        Path catalogPath = directory.resolve("mysql.ibd");
        Path manifestPath = directory.resolve("mysql.dd.manifest");
        Path controlPath = directory.resolve("mysql.dd.ctrl");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(catalogPath);
             DictionaryRecoveryManifestRepository manifest = new DictionaryRecoveryManifestRepository(
                     FileDictionaryRecoveryManifestStore.openOrCreate(manifestPath));
             DictionaryControlStore control =
                     DictionaryControlStore.openOrCreate(controlPath, SpaceId.of(1), 1024);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            BlockingMutationWitness witness = new BlockingMutationWitness(manifest);
            PersistentDictionaryRepository repository =
                    new PersistentDictionaryRepository(catalog, witness);
            control.advanceTo(new DictionaryControlSnapshot(
                    1, SpaceId.of(1), 3, 1, 1, 1024, 1, 3));
            DictionaryRecoverySnapshotPublisher publisher =
                    new DictionaryRecoverySnapshotPublisher(
                            repository, control, manifest, directory.resolve("tables"));
            SchemaDefinition schema = new SchemaDefinition(
                    SchemaId.of(2), ObjectName.of("app"), 1, 1, DictionaryVersion.of(2));

            var commit = executor.submit(() ->
                    repository.commit(DictionaryVersion.of(2), List.of(schema), List.of()));
            assertTrue(witness.intentEntered.await(5, TimeUnit.SECONDS));
            var clean = executor.submit(publisher::publish);

            assertThrows(TimeoutException.class, () -> clean.get(100, TimeUnit.MILLISECONDS));
            assertTrue(manifest.view().latestClean().isEmpty(),
                    "writer fence 内未完成 mutation 期间不能发布旧 clean");

            witness.releaseIntent.countDown();
            commit.get(5, TimeUnit.SECONDS);
            clean.get(5, TimeUnit.SECONDS);

            assertFalse(manifest.view().unresolvedCatalogMutation());
            assertTrue(manifest.view().latestClean().orElseThrow()
                    .dictionarySnapshot().schemas().containsKey(schema.id()));
        }
    }

    /** 在 catalog writer lock 内制造可控 intent 窗口，释放后仍委托真实 manifest durable append。 */
    private static final class BlockingMutationWitness implements DictionaryDurabilityWitness {

        /** commit 已进入写前 witness、仍持有 repository writer lock。 */
        private final CountDownLatch intentEntered = new CountDownLatch(1);
        /** 测试允许真实 intent append 的单次门闩。 */
        private final CountDownLatch releaseIntent = new CountDownLatch(1);
        /** 真实 manifest witness。 */
        private final DictionaryRecoveryManifestRepository delegate;

        private BlockingMutationWitness(DictionaryRecoveryManifestRepository delegate) {
            this.delegate = delegate;
        }

        @Override
        public void beforeControlReservation(cn.zhangyis.db.dd.repo.DictionaryControlSnapshot target) {
            delegate.beforeControlReservation(target);
        }

        @Override
        public void beforeCatalogMutation(DictionaryVersion version, List<CatalogRecord> records) {
            intentEntered.countDown();
            try {
                if (!releaseIntent.await(Duration.ofSeconds(5).toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new DatabaseRuntimeException("timed out waiting to release catalog mutation witness");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new DatabaseRuntimeException(
                        "catalog mutation witness test interrupted", interrupted);
            }
            delegate.beforeCatalogMutation(version, records);
        }
    }
}
