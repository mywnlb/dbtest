package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Buffer Pool warmup（§11）：close 时把驻留页定位信息 dump 到文件，open 时读回并预取，缩短重启 warmup。
 *
 * <p>只存 {@code SpaceId + PageNo}（不含页体）；dump 不参与 crash recovery、损坏可丢弃——load 读到缺失/损坏文件返回空、
 * 不抛。load 对每个 PageId 调 {@link BufferPool#prefetch}（free-frame-only、不 fix）：未打开空间 / 不存在 / 无空闲帧的页
 * 由 prefetch 自身跳过/丢弃，故 warmup 只暖已打开空间且有余量的页。
 *
 * <p>简化点（§11.2 延后）：load 同步预取，未做 IO 速率控制 / 分批 / 后台 load worker / space version 校验。
 *
 * <p>文件布局：{@code magic(4) + version(4) + count(4) + count×[spaceId(4)+pageNo(8)] + crc32(4)}。
 */
public final class BufferPoolWarmupService {

    /** dump 文件 magic：ASCII "BPDP"。 */
    private static final int MAGIC = 0x42504450;
    private static final int VERSION = 1;
    /** magic + version + count。 */
    private static final int HEADER_BYTES = 12;
    /** spaceId(4) + pageNo(8)。 */
    private static final int ENTRY_BYTES = 12;
    private static final int CRC_BYTES = 4;

    /**
     * 把当前驻留页定位信息写入 dump 文件。最佳努力：IO 失败不抛（warmup 可丢弃），返回 0。
     *
     * @param pool 源 buffer pool。
     * @param file dump 文件路径（存在则覆盖）。
     * @return 写出的页定位条目数；失败为 0。
     */
    public int dump(BufferPool pool, Path file) {
        if (pool == null || file == null) {
            throw new DatabaseValidationException("warmup dump pool/file must not be null");
        }
        List<PageId> ids = pool.residentPageIds();
        try {
            Files.write(file, encode(ids));
            return ids.size();
        } catch (IOException e) {
            return 0; // warmup 可丢弃：dump 失败不影响 close。
        }
    }

    /**
     * 读 dump 文件并对每个页定位调 {@link BufferPool#prefetch} 预取。缺失/损坏文件返回 0、不抛。
     *
     * @param pool 目标 buffer pool。
     * @param file dump 文件路径。
     * @return 从 dump 读到并尝试预取的条目数。
     */
    public int load(BufferPool pool, Path file) {
        if (pool == null || file == null) {
            throw new DatabaseValidationException("warmup load pool/file must not be null");
        }
        List<PageId> ids = readBestEffort(file);
        for (PageId id : ids) {
            // prefetch 自身保证最佳努力：已驻留/无空闲帧/未打开空间/IO 失败均跳过，warmup 不会因个别页失败而中断。
            pool.prefetch(id);
        }
        return ids.size();
    }

    private static byte[] encode(List<PageId> ids) {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_BYTES + ids.size() * ENTRY_BYTES + CRC_BYTES);
        buf.putInt(MAGIC);
        buf.putInt(VERSION);
        buf.putInt(ids.size());
        for (PageId id : ids) {
            buf.putInt(id.spaceId().value());
            buf.putLong(id.pageNo().value());
        }
        int crcOffset = HEADER_BYTES + ids.size() * ENTRY_BYTES;
        buf.putInt(crc32(buf.array(), 0, crcOffset));
        return buf.array();
    }

    private static List<PageId> readBestEffort(Path file) {
        try {
            if (!Files.exists(file)) {
                return List.of();
            }
            return decode(Files.readAllBytes(file));
        } catch (IOException | RuntimeException e) {
            // 损坏/非法 dump 一律丢弃（§11.2）：warmup 不参与正确性，宁可不暖也不抛。
            return List.of();
        }
    }

    private static List<PageId> decode(byte[] bytes) {
        if (bytes.length < HEADER_BYTES + CRC_BYTES) {
            return List.of();
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int magic = buf.getInt();
        int version = buf.getInt();
        int count = buf.getInt();
        if (magic != MAGIC || version != VERSION || count < 0) {
            return List.of();
        }
        long expectedLength = (long) HEADER_BYTES + (long) count * ENTRY_BYTES + CRC_BYTES;
        if (expectedLength != bytes.length) {
            return List.of();
        }
        int crcOffset = HEADER_BYTES + count * ENTRY_BYTES;
        int storedCrc = ByteBuffer.wrap(bytes, crcOffset, CRC_BYTES).getInt();
        if (crc32(bytes, 0, crcOffset) != storedCrc) {
            return List.of();
        }
        List<PageId> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int spaceId = buf.getInt();
            long pageNo = buf.getLong();
            ids.add(PageId.of(SpaceId.of(spaceId), PageNo.of(pageNo)));
        }
        return ids;
    }

    private static int crc32(byte[] bytes, int offset, int length) {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }
}
