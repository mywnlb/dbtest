package cn.zhangyis.db.storage.api.tablespace;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderPhysical;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRawCodec;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleRawCodec;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import cn.zhangyis.db.storage.page.PageType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 不占用主 PageStore SpaceId 的只读表空间文件检查器。
 *
 * <p>IMPORT 在文件尚未挂载时使用该检查器完成 page0 身份校验，避免临时把外部文件注册到
 * BufferPool 或 registry。检查失败必须阻止文件复制和字典 ACTIVE 发布。</p>
 */
public final class TablespaceFileInspection {

    /** 检查候选文件并返回其 page0 identity。
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param expectedSpaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param expectedPageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @return {@code inspect} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TablespaceInspectionException 表空间生命周期、文件身份或导入导出协作失败时抛出；调用方应关闭准入并保留文件证据
     */
    public TablespaceFileIdentity inspect(Path path, SpaceId expectedSpaceId, PageSize expectedPageSize) {
        if (path == null || expectedSpaceId == null || expectedPageSize == null) {
            throw new DatabaseValidationException("tablespace inspection arguments must not be null");
        }
        Path normalized = path.toAbsolutePath().normalize();
        try {
            if (!Files.isRegularFile(normalized) || Files.isSymbolicLink(normalized)) {
                throw new TablespaceInspectionException("tablespace import source is not a regular file: " + normalized);
            }
            long length = Files.size(normalized);
            if (length < expectedPageSize.bytes() || length % expectedPageSize.bytes() != 0) {
                throw new TablespaceInspectionException("tablespace import file length is not page aligned: " + normalized);
            }
            ByteBuffer page = ByteBuffer.allocate(expectedPageSize.bytes()).order(ByteOrder.BIG_ENDIAN);
            try (FileChannel channel = FileChannel.open(normalized, StandardOpenOption.READ)) {
                readFully(channel, page);
            }
            if (!PageImageChecksum.verify(page.array(), expectedPageSize)
                    && !PageImageChecksum.hasLegacyZeroChecksums(page, expectedPageSize)) {
                throw new TablespaceInspectionException("tablespace import page0 checksum mismatch: " + normalized);
            }
            int pageNo = page.getInt(PageEnvelopeLayout.PAGE_NO);
            int pageType = page.getInt(PageEnvelopeLayout.PAGE_TYPE);
            if (pageNo != 0 || PageType.fromCode(pageType) != PageType.FSP_HDR) {
                throw new TablespaceInspectionException("tablespace import page0 envelope mismatch: " + normalized);
            }
            SpaceHeaderPhysical physical = SpaceHeaderRawCodec.readPhysical(page);
            if (!physical.spaceId().equals(expectedSpaceId) || !physical.pageSize().equals(expectedPageSize)) {
                throw new TablespaceInspectionException("tablespace import identity mismatch: " + normalized);
            }
            TablespaceLifecycleHeader lifecycle = TablespaceLifecycleRawCodec.read(page)
                    .orElseThrow(() -> new TablespaceInspectionException("tablespace import lacks lifecycle marker: " + normalized));
            if (lifecycle.state() != cn.zhangyis.db.storage.fil.state.TablespaceState.DISCARDED) {
                throw new TablespaceInspectionException("tablespace import source is not DISCARDED: " + lifecycle.state());
            }
            return new TablespaceFileIdentity(physical.spaceId(), physical.pageSize(),
                    TablespaceTypeFlags.decode(physical.spaceFlags()), 80046, physical.spaceVersion());
        } catch (IOException | RuntimeException failure) {
            if (failure instanceof TablespaceInspectionException inspection) {
                throw inspection;
            }
            throw new TablespaceInspectionException("inspect tablespace import file failed: " + normalized, failure);
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer target) throws IOException {
        while (target.hasRemaining()) {
            if (channel.read(target) < 0) {
                throw new IOException("unexpected EOF while reading tablespace page0");
            }
        }
    }

    /** 候选表空间文件无法安全解释时抛出的可诊断运行时异常。 */
    public static final class TablespaceInspectionException extends DatabaseRuntimeException {
        /**
         * 创建 {@code TablespaceInspectionException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
         *
         * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
         */
        public TablespaceInspectionException(String message) { super(message); }
        /**
         * 创建 {@code TablespaceInspectionException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
         *
         * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
         * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
         */
        public TablespaceInspectionException(String message, Throwable cause) { super(message, cause); }
    }
}
