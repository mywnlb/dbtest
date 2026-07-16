package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.engine.StorageEngine;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleRawCodec;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 物理 DDL TDD：CREATE 必须产生真实 GENERAL tablespace/INDEX root，DROP 必须排空并删除文件。 */
class TableDdlStorageServiceTest {

    @TempDir
    Path directory;

    /** CREATE 为聚簇与二级索引分别创建持久 root/binding；DROP 后文件和运行时准入都消失。 */
    @Test
    void createsIndexRootAndDropsTablespaceThroughSafeLifecycleBoundary() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("app_orders_1024.ibd");
        try {
            StorageTableDefinition definition = new StorageTableDefinition(2, SpaceId.of(1024), tableFile,
                    2, PageNo.of(128),
                    List.of(new StorageColumnDefinition(1, "id", 0,
                            StorageColumnType.bigint(false, false))),
                    List.of(new StorageIndexDefinition(3, "PRIMARY", true, true,
                                    List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0))),
                            new StorageIndexDefinition(4, "idx_id", false, false,
                                    List.of(new StorageIndexKeyPart(1, StorageIndexOrder.DESC, 0)))));

            TableStorageBinding binding = engine.tableDdlStorageService().createTable(definition);

            assertTrue(Files.exists(tableFile));
            assertEquals(SpaceId.of(1024), binding.spaceId());
            assertEquals(2, binding.indexes().size());
            assertTrue(binding.lobSegment().isEmpty(), "没有 LOB-capable 列时不得浪费 inode 槽");
            assertEquals(3, binding.indexes().getFirst().indexId());
            MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
            assertEquals(3, engine.indexPageAccess().openIndexPage(read,
                    binding.indexes().getFirst().rootPageId(), PageLatchMode.SHARED).header().indexId());
            assertEquals(4, engine.indexPageAccess().openIndexPage(read,
                    binding.indexes().get(1).rootPageId(), PageLatchMode.SHARED).header().indexId());
            engine.miniTransactionManager().commit(read);

            engine.tableDdlStorageService().dropTable(binding, Duration.ofSeconds(5));

            assertFalse(Files.exists(tableFile));
        } finally {
            engine.close();
        }
    }

    /** 多个 LOB-capable 列共享一个表级 LOB segment；其 identity/purpose 必须与索引 segment 隔离。 */
    @Test
    void createsExactlyOneDedicatedLobSegmentForLobCapableTable() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("lob_orders_1027.ibd");
        try {
            StorageTableDefinition definition = new StorageTableDefinition(7, SpaceId.of(1027), tableFile,
                    2, PageNo.of(128),
                    List.of(new StorageColumnDefinition(1, "id", 0,
                                    StorageColumnType.bigint(false, false)),
                            new StorageColumnDefinition(2, "description", 1,
                                    lobType(StorageColumnTypeId.TEXT, 65_535)),
                            new StorageColumnDefinition(3, "document", 2,
                                    lobType(StorageColumnTypeId.JSON, Integer.MAX_VALUE))),
                    List.of(new StorageIndexDefinition(17, "PRIMARY", true, true,
                            List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0)))));

            TableStorageBinding binding = engine.tableDdlStorageService().createTable(definition);
            SegmentRef lob = binding.lobSegment().orElseThrow();
            IndexStorageBinding primary = binding.indexes().getFirst();

            assertEquals(binding.spaceId(), lob.spaceId());
            assertNotEquals(primary.leafSegment(), lob);
            assertNotEquals(primary.nonLeafSegment(), lob);
            MiniTransaction inspect = engine.miniTransactionManager().beginReadOnly();
            engine.diskSpaceManager().requireSegmentPurposeForWrite(inspect, lob, SegmentPurpose.LOB);
            engine.miniTransactionManager().commit(inspect);

            engine.tableDdlStorageService().dropTable(binding, Duration.ofSeconds(5));
            assertFalse(Files.exists(tableFile), "DROP 仍以整个 tablespace 为物理回收边界");
        } finally {
            engine.close();
        }
    }

    /** durable DISCARDED 之后崩溃必须在 page0 留下可解释意图，重试 DROP 可跳过 marker 并完成删文件。 */
    @Test
    void resumesDropAfterDurableDiscardedMarker() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("discarded_1025.ibd");
        try {
            TableStorageBinding binding = engine.tableDdlStorageService().createTable(definition(
                    5, SpaceId.of(1025), tableFile));

            assertThrows(TableDdlStorageException.class, () -> engine.tableDdlStorageService().dropTable(
                    binding, Duration.ofSeconds(5), ignored -> {
                        throw new TableDdlStorageException("injected crash after durable DISCARDED");
                    }));

            assertTrue(Files.exists(tableFile));
            assertEquals(TablespaceState.DISCARDED, lifecycle(tableFile).state());
            engine.tableDdlStorageService().dropTable(binding, Duration.ofSeconds(5));
            assertFalse(Files.exists(tableFile));
        } finally {
            engine.close();
        }
    }

    /** catalog binding 路径必须与已打开 space 的真实文件一致，否则不得标记/关闭错误对象。 */
    @Test
    void rejectsDropBindingWhosePathDoesNotMatchOpenTablespace() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("bound_1026.ibd");
        try {
            TableStorageBinding binding = engine.tableDdlStorageService().createTable(definition(
                    6, SpaceId.of(1026), tableFile));
            TableStorageBinding mismatched = new TableStorageBinding(binding.tableId(), binding.spaceId(),
                    directory.resolve("other_1026.ibd"), binding.indexes(), binding.lobSegment());

            assertThrows(TableDdlStorageException.class, () -> engine.tableDdlStorageService()
                    .dropTable(mismatched, Duration.ofSeconds(5)));
            assertTrue(Files.exists(tableFile));

            engine.tableDdlStorageService().dropTable(binding, Duration.ofSeconds(5));
        } finally {
            engine.close();
        }
    }

    private static StorageTableDefinition definition(long tableId, SpaceId spaceId, Path path) {
        return new StorageTableDefinition(tableId, spaceId, path, 2, PageNo.of(128),
                List.of(new StorageColumnDefinition(1, "id", 0,
                        StorageColumnType.bigint(false, false))),
                List.of(new StorageIndexDefinition(tableId + 10, "PRIMARY", true, true,
                        List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0)))));
    }

    /** 构造 storage DTO 的 LOB 类型；长度语义与 Record ColumnType 保持一致。 */
    private static StorageColumnType lobType(StorageColumnTypeId typeId, int maxBytes) {
        return new StorageColumnType(typeId, true, maxBytes, 0, false, 1, 1, List.of());
    }

    private static cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader lifecycle(Path path) {
        ByteBuffer page = ByteBuffer.allocate(16 * 1024);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long position = 0;
            while (page.hasRemaining()) {
                int read = channel.read(page, position);
                if (read < 0) {
                    throw new AssertionError("unexpected EOF while reading discarded page0");
                }
                position += read;
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return TablespaceLifecycleRawCodec.read(page).orElseThrow();
    }

    private EngineConfig config() {
        return new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 256,
                SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }
}
