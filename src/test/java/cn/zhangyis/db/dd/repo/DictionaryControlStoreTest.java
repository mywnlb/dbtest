package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * DD control 双槽 TDD：ID 高水位必须在返回给 DDL 前 force，最新槽损坏时只能回退到上一完整 generation。
 */
class DictionaryControlStoreTest {

    @TempDir
    Path directory;

    /** 一次 reservation 只 force 一次 control，重启后所有 next-counter 继续前进而不复用。 */
    @Test
    void persistsReservedIdentifierRangesAcrossReopen() {
        Path path = directory.resolve("mysql.dd.ctrl");
        DictionaryIdAllocation first;
        try (DictionaryControlStore store = DictionaryControlStore.openOrCreate(path, SpaceId.of(1), 1024)) {
            first = store.reserve(new DictionaryIdRequest(1, 1, 1, 1, 1, 1));
            assertEquals(2L, first.firstSchemaId());
            assertEquals(1024, first.firstSpaceId());
        }

        try (DictionaryControlStore reopened = DictionaryControlStore.openExisting(path, SpaceId.of(1))) {
            DictionaryIdAllocation second = reopened.reserve(new DictionaryIdRequest(1, 1, 1, 1, 1, 1));
            assertEquals(first.firstSchemaId() + 1, second.firstSchemaId());
            assertEquals(first.firstTableId() + 1, second.firstTableId());
            assertEquals(first.firstSpaceId() + 1, second.firstSpaceId());
            assertEquals(first.dictionaryVersion() + 1, second.dictionaryVersion());
        }
    }

    /** 最新槽 torn/corrupt 时读取上一槽，允许产生 ID 空洞但绝不能复用已由上一槽之外证明的编号。
     *
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    @Test
    void fallsBackToPreviousValidSlotWhenNewestSlotIsCorrupted() throws IOException {
        Path path = directory.resolve("mysql.dd.ctrl");
        try (DictionaryControlStore store = DictionaryControlStore.openOrCreate(path, SpaceId.of(1), 1024)) {
            store.reserve(new DictionaryIdRequest(1, 1, 1, 1, 1, 1));
            store.reserve(new DictionaryIdRequest(1, 1, 1, 1, 1, 1));
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[]{0x55}), DictionaryControlStore.SLOT_BYTES);
            channel.force(true);
        }

        try (DictionaryControlStore reopened = DictionaryControlStore.openExisting(path, SpaceId.of(1))) {
            assertEquals(2L, reopened.snapshot().generation());
        }
    }
}
