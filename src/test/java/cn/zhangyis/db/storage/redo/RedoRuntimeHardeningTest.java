package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.page.PageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R1 redo runtime 防御性测试：完整损坏必须按 redo 损坏处理，页内偏移与 batch LSN 不变量不能泄漏 JVM 裸异常。
 */
class RedoRuntimeHardeningTest {

    private static final int MAGIC = 0x524C4731;
    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId P = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    @Test
    void batchRejectsRangeNotMatchingRecordLength() {
        PageInitRecord init = new PageInitRecord(P, PageType.INDEX);
        LogRange wrong = new LogRange(Lsn.of(10), Lsn.of(11));

        assertThrows(DatabaseValidationException.class, () -> new RedoLogBatch(wrong, List.of(init)));
    }

    @Test
    void waitFlushedDoesNotOverflowTimeoutWhenTargetAlreadyDurable() {
        RedoLogManager mgr = new RedoLogManager();

        assertTrue(mgr.waitFlushed(Lsn.of(0), Duration.ofSeconds(Long.MAX_VALUE)));
    }

    @Test
    void pageBytesOverflowIsReportedAsRedoCorruption() {
        PageBytesRecord overflow = new PageBytesRecord(P, Integer.MAX_VALUE, new byte[]{1});
        RedoLogBatch batch = new RedoLogBatch(new LogRange(Lsn.of(0), Lsn.of(overflow.byteLength())),
                List.of(overflow));

        try (PageStore store = createStore()) {
            assertThrows(RedoLogCorruptedException.class,
                    () -> RedoApplyDispatcher.pageDispatcher().apply(batch, new RedoApplyContext(store, PS)));
        }
    }

    @Test
    void readerWrapsCompleteInvalidPayloadAsRedoCorruption() throws Exception {
        Path redoPath = dir.resolve("redo.log");
        byte[] payload = payloadWithUnknownPageType();
        ByteBuffer frame = ByteBuffer.allocate(12 + payload.length);
        frame.putInt(MAGIC);
        frame.putInt(payload.length);
        frame.putInt(crc32(payload));
        frame.put(payload);
        Files.write(redoPath, frame.array());

        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoRecoveryReader reader = new RedoRecoveryReader(repo);
            assertThrows(RedoLogCorruptedException.class, reader::readBatches);
        }
    }

    private PageStore createStore() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(8));
        return store;
    }

    private static byte[] payloadWithUnknownPageType() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeLong(0L);
        out.writeLong(13L);
        out.writeInt(1);
        out.writeByte(RedoRecordType.PAGE_INIT.tag());
        out.writeInt(P.spaceId().value());
        out.writeLong(P.pageNo().value());
        out.writeInt(999);
        out.flush();
        return bytes.toByteArray();
    }

    private static int crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
