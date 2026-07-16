package cn.zhangyis.db.storage.fil.catalog;

import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogCorruptionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * mysql.ibd v1 内部 catalog 文件协议测试。物理层只理解 byte key/payload 和批次，不依赖 DD 对象或 DDL phase。
 */
class FileInternalCatalogStoreTest {

    @TempDir
    Path directory;

    /** 已提交批次跨重启保持顺序、边界和 payload，不依赖 Java serialization。 */
    @Test
    void appendsAndReopensCommittedCatalogBatches() {
        Path path = directory.resolve("mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(path)) {
            store.append(List.of(record("schema/1", "s1"), record("table/2", "t2")));
            store.append(List.of(record("ddl/3", "completed")));
        }

        try (FileInternalCatalogStore reopened = FileInternalCatalogStore.openExisting(path)) {
            List<CatalogBatch> batches = reopened.readCommittedBatches();
            assertEquals(List.of(1L, 2L), batches.stream().map(CatalogBatch::sequence).toList());
            assertArrayEquals(bytes("t2"), batches.getFirst().records().get(1).payload());
        }
    }

    /** data 已落盘但 header generation 未发布模拟 crash tail；重启只能看到旧 committedLength。 */
    @Test
    void ignoresBytesBeyondDurableCommittedLength() throws IOException {
        Path path = directory.resolve("mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(path)) {
            store.append(List.of(record("schema/1", "s1")));
        }
        long committedLength = Files.size(path);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(bytes("unpublished-tail")), committedLength);
            channel.force(true);
        }

        try (FileInternalCatalogStore reopened = FileInternalCatalogStore.openExisting(path)) {
            assertEquals(1, reopened.readCommittedBatches().size());
            assertEquals(committedLength, reopened.committedLength());
        }
    }

    /** committedLength 内的 frame CRC 错误不是可忽略尾部，而是已提交字典损坏，必须 fail-closed。 */
    @Test
    void rejectsCorruptionInsideCommittedCatalogRegion() throws IOException {
        Path path = directory.resolve("mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(path)) {
            store.append(List.of(record("schema/1", "s1")));
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[]{0x44}), FileInternalCatalogStore.DATA_START + 20L);
            channel.force(true);
        }

        assertThrows(InternalCatalogCorruptionException.class,
                () -> FileInternalCatalogStore.openExisting(path));
    }

    private static CatalogRecord record(String key, String payload) {
        return new CatalogRecord(bytes(key), bytes(payload));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
