package cn.zhangyis.db.storage.api.tablespace;
import cn.zhangyis.db.storage.fil.exception.TablespaceNotOpenException;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.exception.TablespaceCorruptedException;
import cn.zhangyis.db.storage.fil.io.DataFileDescriptor;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.state.SpaceFlags;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.fil.meta.TablespaceMetadata;
import cn.zhangyis.db.storage.fil.meta.TablespaceMetadataLoader;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessLease;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderPhysical;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRawCodec;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleRawCodec;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * 从已打开表空间的 page-0 raw 字节重建 {@link TablespaceMetadata}。
 *
 * <p>数据流：先通过 {@link PageStore#pathOf(SpaceId)} 取得已打开文件路径，再用 {@link PageStore#readPage}
 * raw 读取 page0，不经 BufferPool/MTR，避免 registry 懒加载发生在外层 MiniTransaction 内时引入嵌套 MTR。
 * raw 字节交给 fsp 的 {@link SpaceHeaderRawCodec} 解物理字段，type 由 fil 的 {@link TablespaceTypeFlags} 解码。
 *
 * <p>page0 携带统一 FilePageHeader 信封（由 {@code SpaceHeaderRepository.initialize} 盖），本 loader 在按 FSP
 * header 解读前先做信封校验：pageType 必须为 {@link PageType#FSP_HDR}、pageNo 必须为 0，再叠加 page0 自描述
 * spaceId 与请求一致。任一不符表示物理页被覆盖/绑定错误，抛 {@link TablespaceCorruptedException} 阻止注册。
 * GENERAL 从 lifecycle marker 恢复 NORMAL/CORRUPTED；旧 GENERAL 没有 marker 时仍按 NORMAL 兼容打开。
 * 新建 UNDO 从生命周期头恢复 state；旧 UNDO 没有该头时仍以 NORMAL 打开，但后续截断服务会拒绝它，避免猜测初始尺寸。
 *
 * <p>checksum/trailer 语义：新写盘页必须通过 {@link PageImageChecksum} 校验；为兼容早期切片写出的 page0，
 * header checksum 与 trailer checksum 同为 0 的页在通过 FSP_HDR 信封校验后按 legacy unstamped 接受。这个兼容点只保护
 * 历史文件打开，不表示 checksum=0 的任意损坏页可被检测出来；后续文件经 FlushCoordinator 刷出后会进入严格校验路径。
 */
public final class PageZeroTablespaceMetadataLoader implements TablespaceMetadataLoader {

    /**
     * 已打开的数据文件访问入口。loader 只通过 PageStore 的稳定 API 获取 path 与 raw page，不接触 FileChannel。
     */
    private final PageStore pageStore;

    /**
     * 实例页大小。用于分配 raw page 缓冲；page0 内自描述 pageSize 仍以 {@link SpaceHeaderPhysical#pageSize()} 发布。
     */
    private final PageSize pageSize;

    /** 与 MTR/flush/truncate 共享；raw loader 读取 page0 的整个窗口持共享 lease。 */
    private final TablespaceAccessController accessController;

    /**
     * 创建 page-0 metadata loader。
     *
     * @param pageStore 已打开表空间的物理页访问门面。
     * @param pageSize 实例页大小。
     */
    public PageZeroTablespaceMetadataLoader(PageStore pageStore, PageSize pageSize) {
        this(pageStore, pageSize, new TablespaceAccessController());
    }

    /** 创建可与 lifecycle 编排共享 operation lease 的 loader。 */
    public PageZeroTablespaceMetadataLoader(PageStore pageStore, PageSize pageSize,
                                            TablespaceAccessController accessController) {
        if (pageStore == null || pageSize == null || accessController == null) {
            throw new DatabaseValidationException("page zero loader pageStore/pageSize must not be null");
        }
        this.pageStore = pageStore;
        this.pageSize = pageSize;
        this.accessController = accessController;
    }

    /**
     * 加载单个表空间 metadata。未打开表空间返回 empty，交由 Registry 转换为 not found；打开但 page0 自描述不一致
     * 表示物理文件与 spaceId 绑定错误，抛领域校验异常阻止继续注册。
     *
     * @param spaceId 表空间编号。
     * @return 可从 page0 重建时返回 metadata，否则返回 empty。
     */
    @Override
    public Optional<TablespaceMetadata> load(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        try (TablespaceAccessLease ignored = accessController.acquireShared(spaceId)) {
            return loadUnderLease(spaceId);
        }
    }

    /** 调用方已持目标空间共享 lease；path/read/decode 不能跨越物理 truncate。 */
    private Optional<TablespaceMetadata> loadUnderLease(SpaceId spaceId) {
        Path path;
        try {
            path = pageStore.pathOf(spaceId);
        } catch (TablespaceNotOpenException notOpen) {
            return Optional.empty();
        }
        ByteBuffer page = ByteBuffer.allocate(pageSize.bytes());
        pageStore.readPage(PageId.of(spaceId, PageNo.of(0)), page);
        validateFspHdrEnvelope(spaceId, page);
        validateChecksumOrLegacyUnstamped(spaceId, page);
        SpaceHeaderPhysical physical = SpaceHeaderRawCodec.readPhysical(page);
        if (!physical.spaceId().equals(spaceId)) {
            throw new DatabaseValidationException("page0 space id mismatch: expected " + spaceId.value()
                    + " got " + physical.spaceId().value());
        }
        TablespaceType type = TablespaceTypeFlags.decode(physical.spaceFlags());
        Optional<TablespaceLifecycleHeader> lifecycle = TablespaceLifecycleRawCodec.read(page);
        TablespaceState state = lifecycle.map(TablespaceLifecycleHeader::state).orElse(TablespaceState.NORMAL);
        validateLifecycleState(spaceId, type, state, lifecycle.isPresent());
        DataFileDescriptor dataFile = DataFileDescriptor.single(path, PageNo.of(0), physical.currentSizeInPages());
        TablespaceMetadata metadata = new TablespaceMetadata(spaceId, "space-" + spaceId.value(), type,
                physical.pageSize(), state, List.of(dataFile), new SpaceFlags(physical.spaceFlags()),
                physical.currentSizeInPages(), physical.freeLimitPageNo(), physical.spaceVersion());
        return Optional.of(metadata);
    }

    /**
     * 根据表空间类型校验 page0 lifecycle 状态。GENERAL 只接受稳定 NORMAL/CORRUPTED；UNDO 只接受 ACTIVE/
     * INACTIVE/TRUNCATING；其它类型首版不支持 lifecycle marker。这样避免把一个模块的生命周期协议误解释成另一个模块的状态。
     */
    private void validateLifecycleState(SpaceId spaceId, TablespaceType type,
                                        TablespaceState state, boolean lifecyclePresent) {
        if (type == TablespaceType.GENERAL) {
            if (state != TablespaceState.NORMAL && state != TablespaceState.CORRUPTED) {
                throw new DatabaseValidationException("invalid GENERAL lifecycle state for space "
                        + spaceId.value() + ": " + state);
            }
            return;
        }
        if (type == TablespaceType.UNDO) {
            if (lifecyclePresent && state != TablespaceState.ACTIVE
                    && state != TablespaceState.INACTIVE
                    && state != TablespaceState.TRUNCATING) {
                throw new DatabaseValidationException("invalid UNDO lifecycle state for space "
                        + spaceId.value() + ": " + state);
            }
            return;
        }
        if (lifecyclePresent) {
            throw new DatabaseValidationException("unsupported lifecycle marker for tablespace type: " + type);
        }
    }

    /**
     * page0 物理信封校验：必须是 {@link PageType#FSP_HDR} 页型且 pageNo==0。这是 raw loader 能做的最外层损坏拦截，
     * 在按 FSP header 解读元数据之前确认 page0 真为表空间头页。pageType/pageNo 偏移取自 {@link PageEnvelopeLayout}，
     * 用绝对位置读取，不依赖 buffer 的 position/limit。
     *
     * @param spaceId 请求打开的表空间编号，仅用于错误信息。
     * @param page 已读入的 page0 raw 字节缓冲。
     * @throws TablespaceCorruptedException page0 不是 FSP_HDR 头页或 pageNo 非 0。
     */
    private void validateFspHdrEnvelope(SpaceId spaceId, ByteBuffer page) {
        int pageType = page.getInt(PageEnvelopeLayout.PAGE_TYPE);
        if (pageType != PageType.FSP_HDR.code()) {
            throw new TablespaceCorruptedException("page0 is not FSP_HDR for space " + spaceId.value()
                    + ": pageType code=" + pageType);
        }
        long pageNo = page.getInt(PageEnvelopeLayout.PAGE_NO) & 0xFFFFFFFFL;
        if (pageNo != 0) {
            throw new TablespaceCorruptedException("page0 envelope pageNo is not 0 for space "
                    + spaceId.value() + ": pageNo=" + pageNo);
        }
    }

    /**
     * page0 checksum/trailer 校验。新格式落盘页必须通过 CRC32 + trailer low32 LSN 校验；旧格式未盖 checksum
     * 的 page0 仅在 header/trailer checksum 均为 0 时兼容放行。兼容判断放在 FSP_HDR 信封之后，避免把全零页或错位页
     * 当成历史合法页。
     *
     * @param spaceId 请求打开的表空间编号，仅用于错误信息。
     * @param page 已读入的 page0 raw 字节缓冲。
     * @throws TablespaceCorruptedException checksum 或 FIL trailer 不匹配。
     */
    private void validateChecksumOrLegacyUnstamped(SpaceId spaceId, ByteBuffer page) {
        if (PageImageChecksum.verify(page, pageSize)) {
            return;
        }
        if (PageImageChecksum.hasLegacyZeroChecksums(page, pageSize)) {
            return;
        }
        throw new TablespaceCorruptedException("page0 checksum/trailer mismatch for space " + spaceId.value());
    }
}
