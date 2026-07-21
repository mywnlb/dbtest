package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.IndexKeyPart;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.repo.DictionaryControlStore;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.tx.DictionaryTransaction;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;
import cn.zhangyis.db.storage.recovery.RecoveryMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 对象级 FORCE 规划器测试：所有归属歧义必须在 DD 写入和用户表空间 discovery 前 fail-closed。 */
class DictionaryRecoveryIsolationPlannerTest {

    @TempDir
    Path directory;

    /**
     * 两个 committed 对象即使 SpaceId 不同，只要共享同一物理路径也不能继续规划；否则后续 raw
     * DISCARD/DROP 会让一个对象删除另一个对象的文件。
     */
    @Test
    void rejectsDistinctSpacesSharingOnePhysicalPathBeforeIsolationCommit() {
        Path tables = directory.resolve("tables").toAbsolutePath().normalize();
        Path shared = tables.resolve("shared.ibd");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            try (DictionaryTransaction transaction = repository.begin(DictionaryVersion.of(2))) {
                transaction.createSchema(new SchemaDefinition(
                        SchemaId.of(1), ObjectName.of("app"), 1, 1,
                        DictionaryVersion.of(2)));
                transaction.createTable(table(2, 11, "left_table", 1024, shared));
                transaction.createTable(table(3, 21, "right_table", 1025, shared));
                transaction.commit();
            }

            DictionaryRecoveryIsolationPlanner planner = new DictionaryRecoveryIsolationPlanner(
                    control, repository, SpaceId.of(1), SpaceId.of(2), tables);

            RecoveryIsolationException failure = assertThrows(
                    RecoveryIsolationException.class,
                    () -> planner.plan(
                            RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE,
                            Set.of(SpaceId.of(1024))));
            assertTrue(failure.getMessage().contains("path must have exactly one"));
            assertTrue(repository.findTable(TableId.of(2)).isPresent(),
                    "归属证明失败不得把任何 ACTIVE aggregate 部分提交为隔离态");
        }
    }

    /** 构造单列、单聚簇索引表；所有物理 identity 随表独立，只有测试指定的文件路径相同。 */
    private static TableDefinition table(
            long tableId, long indexId, String name, int spaceValue, Path path) {
        SpaceId spaceId = SpaceId.of(spaceValue);
        ColumnDefinition column = new ColumnDefinition(
                1, ObjectName.of("id"), ColumnTypeDefinition.bigint(false, false), 0);
        IndexDefinition index = new IndexDefinition(
                IndexId.of(indexId), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        SegmentRef leaf = new SegmentRef(spaceId, 1, SegmentId.of(indexId + 1));
        SegmentRef nonLeaf = new SegmentRef(spaceId, 2, SegmentId.of(indexId + 2));
        TableStorageBinding binding = new TableStorageBinding(
                tableId, spaceId, path, 1,
                List.of(new IndexStorageBinding(indexId,
                        PageId.of(spaceId, PageNo.of(64)), 0, leaf, nonLeaf)),
                Optional.empty());
        return new TableDefinition(
                TableId.of(tableId), SchemaId.of(1), ObjectName.of(name),
                DictionaryVersion.of(2), TableState.ACTIVE,
                List.of(column), List.of(index), Optional.of(binding));
    }
}
