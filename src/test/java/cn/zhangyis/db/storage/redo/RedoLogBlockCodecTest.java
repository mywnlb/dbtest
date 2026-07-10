package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.OptionalLong;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** LogBlock v1 格式测试：固定布局、多块重组、torn-tail 与完整语义损坏必须严格区分。 */
class RedoLogBlockCodecTest {

    private static final PageId PAGE = PageId.of(SpaceId.of(1), PageNo.of(7));

    /** 单块和多块 batch 都必须保留原始逻辑 LSN/record，同时输出严格 512B 对齐的 block chain。 */
    @Test
    void roundTripsSingleAndMultiBlockBatches() {
        assertRoundTrip(batch(0, 8), 0);
        RedoLogBatch large = batch(0, 1_400);
        RedoLogBlockCodec.EncodedBatch encoded = RedoLogBlockCodec.encodeBatch(large, 11);

        assertTrue(encoded.blockCount() >= 3);
        assertEquals(0, encoded.bytes().remaining() % RedoLogBlockCodec.BLOCK_BYTES);
        RedoLogBlockScanResult scanned = RedoLogBlockScanner.scan(
                encoded.bytes(), true, OptionalLong.of(11), 0L);
        assertBatchEquals(large, scanned.batches().getFirst());
        assertEquals(encoded.bytes().remaining(), scanned.validBytes());
        assertEquals(encoded.nextBlockNo(), scanned.nextBlockNo());
    }

    /** 最后物理块 checksum 失败按 torn tail 丢弃整个未完成 chain；同样损坏出现在中段必须致命。 */
    @Test
    void onlyFinalPhysicalChecksumFailureIsToleratedAsTornTail() {
        RedoLogBlockCodec.EncodedBatch encoded = RedoLogBlockCodec.encodeBatch(batch(0, 1_400), 0);
        byte[] finalDamage = bytes(encoded.bytes());
        finalDamage[finalDamage.length - RedoLogBlockCodec.BLOCK_BYTES + 40] ^= 1;

        RedoLogBlockScanResult torn = RedoLogBlockScanner.scan(
                ByteBuffer.wrap(finalDamage), true, OptionalLong.of(0), 0L);
        assertTrue(torn.tornTail());
        assertEquals(0, torn.validBytes());
        assertTrue(torn.batches().isEmpty());

        byte[] middleDamage = bytes(encoded.bytes());
        middleDamage[RedoLogBlockCodec.BLOCK_BYTES + 40] ^= 1;
        assertThrows(RedoLogCorruptedException.class, () -> RedoLogBlockScanner.scan(
                ByteBuffer.wrap(middleDamage), true, OptionalLong.of(0), 0L));
    }

    /** CRC 正确但 START/END 语义非法不是 torn write，即使位于物理尾部也必须 fail-closed。 */
    @Test
    void checksumValidSemanticCorruptionIsFatalAtTail() {
        byte[] bytes = bytes(RedoLogBlockCodec.encodeBatch(batch(0, 8), 0).bytes());
        ByteBuffer.wrap(bytes).putInt(28, 0);
        putChecksum(bytes);

        assertThrows(RedoLogCorruptedException.class, () -> RedoLogBlockScanner.scan(
                ByteBuffer.wrap(bytes), true, OptionalLong.of(0), 0L));
    }

    /** trailer 镜像、firstRecordOffset 与零 padding 都受外层 CRC 和语义双重校验。 */
    @Test
    void rejectsChecksumValidTrailerOffsetAndPaddingCorruption() {
        byte[] trailer = encodedSingleBlock();
        ByteBuffer.wrap(trailer).putInt(RedoLogBlockCodec.TRAILER_BLOCK_NO_OFFSET, 7);
        putChecksum(trailer);
        assertSemanticFailure(trailer);

        byte[] recordOffset = encodedSingleBlock();
        ByteBuffer.wrap(recordOffset).putShort(26, (short) 0);
        putChecksum(recordOffset);
        assertSemanticFailure(recordOffset);

        byte[] padding = encodedSingleBlock();
        int dataLength = Short.toUnsignedInt(ByteBuffer.wrap(padding).getShort(24));
        padding[RedoLogBlockCodec.HEADER_BYTES + dataLength] = 1;
        putChecksum(padding);
        assertSemanticFailure(padding);
    }

    /** 未闭合 chain 只能作为逻辑尾部 torn 丢弃整批，blockNo 空间耗尽必须在编码前拒绝。 */
    @Test
    void dropsUnclosedTailChainAndRejectsBlockNumberOverflow() {
        RedoLogBlockCodec.EncodedBatch encoded = RedoLogBlockCodec.encodeBatch(batch(0, 1_400), 5);
        byte[] incomplete = bytes(encoded.bytes());
        ByteBuffer withoutEnd = ByteBuffer.wrap(
                incomplete, 0, incomplete.length - RedoLogBlockCodec.BLOCK_BYTES).slice();

        RedoLogBlockScanResult torn = RedoLogBlockScanner.scan(
                withoutEnd, true, OptionalLong.of(5), 0L);

        assertTrue(torn.tornTail());
        assertTrue(torn.batches().isEmpty());
        assertEquals(5, torn.nextBlockNo());
        assertThrows(RedoLogCorruptedException.class,
                () -> RedoLogBlockCodec.encodeBatch(batch(0, 8), Long.MAX_VALUE));
    }

    /** 裸 RLG1 是已知旧格式，不能被误判成可忽略 torn tail。 */
    @Test
    void legacyRawFrameIsRejectedExplicitly() {
        byte[] legacy = new byte[RedoLogBlockCodec.BLOCK_BYTES];
        ByteBuffer.wrap(legacy).putInt(RedoBatchFrameCodec.MAGIC);
        assertThrows(RedoLogFormatException.class, () -> RedoLogBlockScanner.scan(
                ByteBuffer.wrap(legacy), true, OptionalLong.of(0), 0L));
    }

    private static void assertRoundTrip(RedoLogBatch batch, long firstBlockNo) {
        RedoLogBlockCodec.EncodedBatch encoded = RedoLogBlockCodec.encodeBatch(batch, firstBlockNo);
        RedoLogBlockScanResult scanned = RedoLogBlockScanner.scan(
                encoded.bytes(), true, OptionalLong.of(firstBlockNo), batch.range().start().value());
        assertEquals(1, scanned.batches().size());
        assertBatchEquals(batch, scanned.batches().getFirst());
    }

    private static void assertBatchEquals(RedoLogBatch batch, RedoLogBatch decoded) {
        assertEquals(batch.range(), decoded.range());
        PageBytesRecord expected = (PageBytesRecord) batch.records().getFirst();
        PageBytesRecord actual = (PageBytesRecord) decoded.records().getFirst();
        assertEquals(expected.pageId(), actual.pageId());
        assertEquals(expected.offset(), actual.offset());
        assertArrayEquals(expected.bytes(), actual.bytes());
    }

    private static RedoLogBatch batch(long start, int bytes) {
        PageBytesRecord record = new PageBytesRecord(PAGE, 100, new byte[bytes]);
        return new RedoLogBatch(new LogRange(Lsn.of(start), Lsn.of(start + record.byteLength())), List.of(record));
    }

    private static byte[] bytes(ByteBuffer source) {
        ByteBuffer copy = source.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static void putChecksum(byte[] block) {
        CRC32 crc = new CRC32();
        crc.update(block, 0, RedoLogBlockCodec.CHECKSUM_OFFSET);
        ByteBuffer.wrap(block).putInt(RedoLogBlockCodec.CHECKSUM_OFFSET, (int) crc.getValue());
    }

    private static byte[] encodedSingleBlock() {
        return bytes(RedoLogBlockCodec.encodeBatch(batch(0, 8), 0).bytes());
    }

    private static void assertSemanticFailure(byte[] bytes) {
        assertThrows(RedoLogCorruptedException.class, () -> RedoLogBlockScanner.scan(
                ByteBuffer.wrap(bytes), true, OptionalLong.of(0), 0L));
    }
}
