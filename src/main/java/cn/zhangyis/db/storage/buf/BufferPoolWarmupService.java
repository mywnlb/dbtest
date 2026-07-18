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
    /**
     * 当前稳定格式版本；编解码与恢复路径共同依赖该值，升级时必须保留旧版本判定。
     */
    private static final int VERSION = 1;
    /** magic + version + count。
     *
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
    private static final int HEADER_BYTES = 12;
    /** spaceId(4) + pageNo(8)。
     *
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
    private static final int ENTRY_BYTES = 12;
    /**
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
    private static final int CRC_BYTES = 4;

    /**
     * 把当前驻留页定位信息写入 dump 文件。最佳努力：IO 失败不抛（warmup 可丢弃），返回 0。
     *
     * @param pool 源 buffer pool。
     * @param file dump 文件路径（存在则覆盖）。
     * @return 写出的页定位条目数；失败为 0。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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

    /**
     * 把调用方领域值编码为Buffer Pool的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param ids 参与 {@code encode} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code encode} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    private static byte[] encode(List<PageId> ids) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        ByteBuffer buf = ByteBuffer.allocate(HEADER_BYTES + ids.size() * ENTRY_BYTES + CRC_BYTES);
        buf.putInt(MAGIC);
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        buf.putInt(VERSION);
        buf.putInt(ids.size());
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        for (PageId id : ids) {
            buf.putInt(id.spaceId().value());
            buf.putLong(id.pageNo().value());
        }
        int crcOffset = HEADER_BYTES + ids.size() * ENTRY_BYTES;
        buf.putInt(crc32(buf.array(), 0, crcOffset));
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return buf.array();
    }

    /**
     * 根据调用参数创建或转换 {@code readBestEffort} 返回的 {@code List<PageId>}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param file 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     */
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

    /**
     * 从稳定表示解码Buffer Pool领域值；先校验边界、标识与长度，损坏输入以领域异常拒绝。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param bytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     */
    private static List<PageId> decode(byte[] bytes) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (bytes.length < HEADER_BYTES + CRC_BYTES) {
            return List.of();
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int magic = buf.getInt();
        int version = buf.getInt();
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        int count = buf.getInt();
        if (magic != MAGIC || version != VERSION || count < 0) {
            return List.of();
        }
        long expectedLength = (long) HEADER_BYTES + (long) count * ENTRY_BYTES + CRC_BYTES;
        if (expectedLength != bytes.length) {
            return List.of();
        }
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
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
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return ids;
    }

    private static int crc32(byte[] bytes, int offset, int length) {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }
}
