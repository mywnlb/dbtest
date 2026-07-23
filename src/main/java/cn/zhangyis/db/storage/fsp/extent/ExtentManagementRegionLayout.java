package cn.zhangyis.db.storage.fsp.extent;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;

/**
 * XDES/IBUF 重复管理区的唯一纯寻址规则。
 *
 * <p>兼容约束：旧格式 page0 已能描述的 {@code [0,C)} extent 永久保留原槽位；只有后续 extent 才进入
 * 独立 XDES 页。管理区以 {@code pageSize.bytes()} 个物理页为跨度，primary 位于区首，bitmap 位于 +1，
 * overflow 固定位于 +5。该类不访问 Buffer Pool 或文件，因此 repository、scrubber 和 Change Buffer 可以共享
 * 同一公式而不形成 IO 反向依赖。</p>
 */
public final class ExtentManagementRegionLayout {

    /** 实例页大小；同时决定 extent 大小、管理区跨度和 descriptor 单页容量。 */
    private final PageSize pageSize;
    /** 保留旧 page0 算法得到的槽位数，任何升级都不能改变该值或旧 extent 的物理地址。 */
    private final long entriesPerDescriptorPage;
    /** 一个重复 bitmap/XDES 管理区覆盖的完整 extent 数。 */
    private final long extentsPerManagementRegion;

    /**
     * 创建纯布局并验证双页容量足以描述一个完整管理区。
     *
     * @param pageSize 实例固定页大小；不得为空，且必须是 PageSize 支持的值
     * @throws DatabaseValidationException 双页容量不足或公式不能整除时抛出，实例不得继续打开
     */
    public ExtentManagementRegionLayout(PageSize pageSize) {
        if (pageSize == null) {
            throw new DatabaseValidationException("extent management page size must not be null");
        }
        this.pageSize = pageSize;
        this.entriesPerDescriptorPage = ExtentDescriptorLayout.maxEntriesInPage0(pageSize);
        if (pageSize.bytes() % pageSize.pagesPerExtent() != 0) {
            throw new DatabaseValidationException("management region pages are not extent aligned");
        }
        this.extentsPerManagementRegion = pageSize.bytes() / pageSize.pagesPerExtent();
        if (extentsPerManagementRegion > entriesPerDescriptorPage * 2L) {
            throw new DatabaseValidationException("two XDES pages cannot describe one management region: extents="
                    + extentsPerManagementRegion + " capacity=" + entriesPerDescriptorPage);
        }
    }

    /** @return 实例固定页大小；调用方不得据此修改运行期配置 */
    public PageSize pageSize() {
        return pageSize;
    }

    /** @return 与旧 page0 完全相同的 descriptor 单页槽位数 */
    public long entriesPerDescriptorPage() {
        return entriesPerDescriptorPage;
    }

    /** @return 一个重复管理区覆盖的 extent 数 */
    public long extentsPerManagementRegion() {
        return extentsPerManagementRegion;
    }

    /**
     * 把全局 ExtentId 映射为 page0 或独立 XDES 页内槽位。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先保留 {@code extentNo<C} 的旧 page0 地址，保证已有文件无需重写。</li>
     *     <li>其余 extent 按管理区计算当前区真正由独立页承载的 first extent。</li>
     *     <li>选择 group0 page5、后续 primary 或 overflow，并生成 FLST node 地址。</li>
     * </ol>
     *
     * @param extentId 待定位的稳定 extent identity；不得为空
     * @return 包含物理页、槽位、entry 偏移和 list-node 地址的不可变结果
     * @throws DatabaseValidationException 页号或偏移算术溢出时抛出，调用方不得访问截断地址
     */
    public ExtentDescriptorLocation locate(ExtentId extentId) {
        if (extentId == null) {
            throw new DatabaseValidationException("extent id must not be null");
        }
        long extentNo = extentId.extentNo();
        long pageNo;
        long slot;
        if (extentNo < entriesPerDescriptorPage) {
            pageNo = 0L;
            slot = extentNo;
        } else {
            long region = extentNo / extentsPerManagementRegion;
            long first = firstStandaloneExtent(region);
            long index = extentNo - first;
            if (index < 0) {
                throw new FspMetadataException("extent precedes standalone XDES range: " + extentNo);
            }
            if (region == 0L) {
                pageNo = 5L;
                slot = index;
            } else if (index < entriesPerDescriptorPage) {
                pageNo = pageNoOfRegion(region);
                slot = index;
            } else {
                pageNo = addExact(pageNoOfRegion(region), 5L, "XDES overflow page");
                slot = index - entriesPerDescriptorPage;
            }
        }
        if (slot < 0 || slot >= entriesPerDescriptorPage) {
            throw new FspMetadataException("XDES slot exceeds descriptor page capacity: extent="
                    + extentNo + " slot=" + slot);
        }
        int intSlot = Math.toIntExact(slot);
        int entryOffset = ExtentDescriptorLayout.entryOffsetInPage(intSlot);
        PageNo descriptorPageNo = PageNo.of(pageNo);
        PageId descriptorPageId = PageId.of(extentId.spaceId(), descriptorPageNo);
        FileAddress node = FileAddress.of(descriptorPageNo, entryOffset + ExtentDescriptorLayout.PREV);
        return new ExtentDescriptorLocation(extentId, descriptorPageId, intSlot, entryOffset, node);
    }

    /**
     * 从 FLST 节点地址反解 extent identity；只接受本布局声明且槽位落在该页 entryCount 内的地址。
     *
     * @param spaceId 节点所属表空间；不得为空
     * @param nodeAddr 从持久 FLST 读取的非空地址
     * @return 与该 node 唯一对应的 ExtentId
     * @throws FspMetadataException 页号、对齐或槽位不属于 XDES 布局时抛出，禁止继续遍历该链
     */
    public ExtentId extentIdOfNode(SpaceId spaceId, FileAddress nodeAddr) {
        if (spaceId == null || nodeAddr == null || nodeAddr.isNull()) {
            throw new DatabaseValidationException("space id and concrete XDES node address are required");
        }
        int relative = nodeAddr.offset() - ExtentDescriptorLayout.ENTRIES_BASE
                - ExtentDescriptorLayout.PREV;
        if (relative < 0 || relative % ExtentDescriptorLayout.ENTRY_SIZE != 0) {
            throw new FspMetadataException("misaligned XDES list node offset: " + nodeAddr.offset());
        }
        long slot = relative / ExtentDescriptorLayout.ENTRY_SIZE;
        long pageNo = nodeAddr.pageNo().value();
        long first;
        long count;
        if (pageNo == 0L) {
            first = 0L;
            count = entriesPerDescriptorPage;
        } else if (pageNo == 5L) {
            first = entriesPerDescriptorPage;
            count = overflowEntryCount(0L);
        } else if (pageNo % pageSize.bytes() == 0L) {
            long region = pageNo / pageSize.bytes();
            if (region <= 0L) {
                throw new FspMetadataException("invalid standalone XDES primary page: " + pageNo);
            }
            first = firstStandaloneExtent(region);
            count = primaryEntryCount(region);
        } else if (pageNo >= 5L && (pageNo - 5L) % pageSize.bytes() == 0L) {
            long region = (pageNo - 5L) / pageSize.bytes();
            first = addExact(firstStandaloneExtent(region), entriesPerDescriptorPage,
                    "XDES overflow first extent");
            count = overflowEntryCount(region);
        } else {
            throw new FspMetadataException("file address does not point to an XDES page: " + nodeAddr);
        }
        if (slot >= count) {
            throw new FspMetadataException("XDES node slot is outside page entry count: page="
                    + pageNo + " slot=" + slot + " count=" + count);
        }
        return ExtentId.of(spaceId, addExact(first, slot, "XDES node extent"));
    }

    /** @return region 的 primary XDES 页号；region0 返回 page0 */
    public PageNo primaryXdesPageNo(long region) {
        requireRegion(region);
        return PageNo.of(pageNoOfRegion(region));
    }

    /** @return region 的重复 IBUF_BITMAP 页号，即 groupBase+1 */
    public PageNo bitmapPageNo(long region) {
        requireRegion(region);
        return PageNo.of(addExact(pageNoOfRegion(region), 1L, "management bitmap page"));
    }

    /** @return region 的固定 overflow 页号，即 groupBase+5；即使当前无需使用也只表达公式位置 */
    public PageNo overflowXdesPageNo(long region) {
        requireRegion(region);
        return PageNo.of(addExact(pageNoOfRegion(region), 5L, "management overflow page"));
    }

    /** @return 指定 region 是否确实存在超过 primary 容量的 descriptor */
    public boolean requiresOverflowPage(long region) {
        requireRegion(region);
        return overflowEntryCount(region) > 0L;
    }

    /** @return extent 是否是某个管理区首 extent；这些 extent 永不进入普通 FREE 链 */
    public boolean isManagementExtent(long extentNo) {
        if (extentNo < 0) {
            throw new DatabaseValidationException("extent no must not be negative");
        }
        return extentNo % extentsPerManagementRegion == 0L;
    }

    /** @return extent 所属管理区的零基编号 */
    public long regionIndexOfExtent(long extentNo) {
        if (extentNo < 0) {
            throw new DatabaseValidationException("extent no must not be negative");
        }
        return extentNo / extentsPerManagementRegion;
    }

    /** @return standalone primary 页 header 应记录的首个 extentNo；空页返回区间 exclusive end */
    public long firstStandaloneExtent(long region) {
        requireRegion(region);
        long start = multiplyExact(region, extentsPerManagementRegion, "management region first extent");
        long end = addExact(start, extentsPerManagementRegion, "management region end extent");
        return Math.min(Math.max(entriesPerDescriptorPage, start), end);
    }

    /** @return standalone primary 页实际持有的 entry 数；region0 的 primary 是旧 page0，不使用该值编码 header */
    public long primaryEntryCount(long region) {
        requireRegion(region);
        long remaining = standaloneEntryCount(region);
        return Math.min(entriesPerDescriptorPage, remaining);
    }

    /** @return 固定 +5 页实际持有的 entry 数；返回 0 时该页不得被格式化为 XDES */
    public long overflowEntryCount(long region) {
        requireRegion(region);
        if (region == 0L) {
            return Math.max(0L, extentsPerManagementRegion - entriesPerDescriptorPage);
        }
        long remaining = standaloneEntryCount(region);
        return Math.max(0L, remaining - entriesPerDescriptorPage);
    }

    private long standaloneEntryCount(long region) {
        long start = multiplyExact(region, extentsPerManagementRegion, "management region first extent");
        long end = addExact(start, extentsPerManagementRegion, "management region end extent");
        return Math.max(0L, end - firstStandaloneExtent(region));
    }

    private long pageNoOfRegion(long region) {
        return multiplyExact(region, pageSize.bytes(), "management region page");
    }

    private static void requireRegion(long region) {
        if (region < 0) {
            throw new DatabaseValidationException("management region index must not be negative: " + region);
        }
    }

    private static long addExact(long left, long right, String what) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException(what + " overflows", error);
        }
    }

    private static long multiplyExact(long left, long right, String what) {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException(what + " overflows", error);
        }
    }
}
