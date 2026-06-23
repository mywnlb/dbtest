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
import cn.zhangyis.db.storage.fil.io.DataFileDescriptor;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.state.SpaceFlags;
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
 * <p>简化点：page0 当前没有完整 FilePageHeader 信封，本 loader 以 page0 自描述 spaceId 与请求 spaceId
 * 一致作为基本损坏校验。新建 UNDO 从生命周期头恢复 state；旧 UNDO 没有该头时仍以 NORMAL 打开，
 * 但后续截断服务会拒绝它，避免猜测初始尺寸。
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
        SpaceHeaderPhysical physical = SpaceHeaderRawCodec.readPhysical(page);
        if (!physical.spaceId().equals(spaceId)) {
            throw new DatabaseValidationException("page0 space id mismatch: expected " + spaceId.value()
                    + " got " + physical.spaceId().value());
        }
        TablespaceType type = TablespaceTypeFlags.decode(physical.spaceFlags());
        Optional<TablespaceLifecycleHeader> lifecycle = TablespaceLifecycleRawCodec.read(page);
        if (type != TablespaceType.UNDO && lifecycle.isPresent()) {
            throw new DatabaseValidationException("non-UNDO tablespace contains UNDO lifecycle header: "
                    + spaceId.value());
        }
        TablespaceState state = lifecycle.map(TablespaceLifecycleHeader::state).orElse(TablespaceState.NORMAL);
        DataFileDescriptor dataFile = DataFileDescriptor.single(path, PageNo.of(0), physical.currentSizeInPages());
        TablespaceMetadata metadata = new TablespaceMetadata(spaceId, "space-" + spaceId.value(), type,
                physical.pageSize(), state, List.of(dataFile), new SpaceFlags(physical.spaceFlags()),
                physical.currentSizeInPages(), physical.freeLimitPageNo(), physical.spaceVersion());
        return Optional.of(metadata);
    }
}
