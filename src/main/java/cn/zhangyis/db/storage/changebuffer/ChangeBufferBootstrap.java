package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.PageType;

/**
 * 新实例 system.ibd 的 Change Buffer 固定页引导器。它不创建文件；调用方必须先在同一 boot MTR
 * 创建 SYSTEM tablespace，使 page0..4 已被 XDES 标记为系统保留。
 */
public final class ChangeBufferBootstrap {

    /** 全局 Change Buffer 树稳定 root，split 只提升 level 而不改变该页号。 */
    public static final PageId ROOT_PAGE_ID = PageId.of(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID, PageNo.of(4));

    /** FSP segment 创建入口。 */
    private final DiskSpaceManager disk;
    /** 固定 root RecordPage 的专用格式化入口。 */
    private final IndexPageAccess pageAccess;
    /** page 3 权威 header 仓储。 */
    private final ChangeBufferHeaderRepository headerRepository;
    /** 构造内部 schema 与 descriptor 使用的实例页大小。 */
    private final PageSize pageSize;

    /**
     * @param disk 已绑定 system.ibd registry 的空间管理入口
     * @param pageAccess 与相同 Buffer Pool/registry 绑定的索引页入口
     * @param headerRepository 固定 page 3 仓储
     * @param pageSize 实例页大小
     */
    public ChangeBufferBootstrap(DiskSpaceManager disk, IndexPageAccess pageAccess,
                                 ChangeBufferHeaderRepository headerRepository, PageSize pageSize) {
        if (disk == null || pageAccess == null || headerRepository == null || pageSize == null) {
            throw new DatabaseValidationException("change buffer bootstrap dependencies must not be null");
        }
        this.disk = disk;
        this.pageAccess = pageAccess;
        this.headerRepository = headerRepository;
        this.pageSize = pageSize;
    }

    /**
     * 创建 leaf/non-leaf segment、page 3 header 与 page 4 空 root，所有副作用进入同一个 boot MTR。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 MTR 与配置模式；system.ibd 文件及 page0..4 保留是调用方已完成的前置条件。</li>
     *     <li>经 FSP 在 SpaceId 0 创建 leaf/non-leaf segment，segment inode 是后续 split 分配的权威 owner。</li>
     *     <li>先按页号顺序格式化 page 3 header，再格式化 page 4 IBUF_INDEX 空 root，避免 page latch 逆序。</li>
     *     <li>返回与持久 header 一致的 descriptor；只有 boot MTR commit/flush 后才能对普通流量启用。</li>
     * </ol>
     *
     * @param mtr 已创建 system.ibd 的活动 boot MTR；不得为 {@code null}
     * @param configuredMode 首次持久化的 Change Buffer 模式；不得为 {@code null}
     * @return 可用于全局树操作的稳定 descriptor
     * @throws DatabaseValidationException 参数为空时抛出
     */
    public BTreeIndex initialize(MiniTransaction mtr, ChangeBufferMode configuredMode) {
        // 1、所有可判定输入先于 FSP segment id 推进校验。
        if (mtr == null || configuredMode == null) {
            throw new DatabaseValidationException("change buffer bootstrap MTR/mode must not be null");
        }
        // 2、两个 segment 分离 leaf 与 internal extent 所有权，为后续长高和回收保留边界。
        SegmentRef leaf = disk.createSegment(mtr, ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID,
                SegmentPurpose.INDEX_LEAF);
        SegmentRef nonLeaf = disk.createSegment(mtr, ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID,
                SegmentPurpose.INDEX_NON_LEAF);
        ChangeBufferHeaderSnapshot snapshot = new ChangeBufferHeaderSnapshot(
                ChangeBufferHeaderState.ACTIVE, configuredMode, ROOT_PAGE_ID, 0,
                ChangeBufferRecordSchema.INDEX_ID, leaf, nonLeaf, 1L, 0L, 1L);
        // 3、page3 后 page4 的稳定升序锁序与普通 MTR 默认守卫一致。
        headerRepository.format(mtr, snapshot);
        pageAccess.createIndexPage(mtr, ROOT_PAGE_ID, ChangeBufferRecordSchema.INDEX_ID,
                0, PageType.IBUF_INDEX);
        // 4、descriptor 仅投影持久 identity，不额外保存可变 root level 权威态。
        return index(snapshot, pageSize);
    }

    /**
     * 从 page 3 快照构造全局树 descriptor；root level 以每次读取的 header 为准。
     *
     * @param snapshot 已通过 repository 校验的 header
     * @param pageSize 实例页大小
     * @return IBUF_INDEX、物理唯一、非聚簇的 B+Tree descriptor
     */
    public static BTreeIndex index(ChangeBufferHeaderSnapshot snapshot, PageSize pageSize) {
        if (snapshot == null || pageSize == null) {
            throw new DatabaseValidationException("change buffer index snapshot/pageSize must not be null");
        }
        return new BTreeIndex(snapshot.indexId(), snapshot.rootPageId(), snapshot.rootLevel(),
                ChangeBufferRecordSchema.keyDef(), ChangeBufferRecordSchema.schema(pageSize), true,
                snapshot.leafSegment(), snapshot.nonLeafSegment(), PageType.IBUF_INDEX);
    }
}
