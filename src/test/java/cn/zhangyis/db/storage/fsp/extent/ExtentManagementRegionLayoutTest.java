package cn.zhangyis.db.storage.fsp.extent;

import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * XDES 管理区纯寻址回归测试。测试不打开文件，专门固定旧 page0 槽位兼容性、双页容量以及
 * {@link FileAddress} 与 {@link ExtentId} 的双向映射，防止分配层和 scrubber 各自复制公式后发生漂移。
 */
class ExtentManagementRegionLayoutTest {

    /** 用于验证 node 地址反解时保留 tablespace identity。 */
    private static final SpaceId SPACE = SpaceId.of(17);

    /**
     * 验证五种实例页大小都能把大量连续 extent 无冲突地映射到 page0、primary 或 overflow 页。
     *
     * @param pageBytes 受支持的实例页大小；由参数源覆盖 4KB 到 64KB
     */
    @ParameterizedTest
    @ValueSource(ints = {4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024})
    void shouldRoundTripDescriptorLocationsAcrossManagementRegions(int pageBytes) {
        PageSize pageSize = PageSize.ofBytes(pageBytes);
        ExtentManagementRegionLayout layout = new ExtentManagementRegionLayout(pageSize);
        long legacyCapacity = layout.entriesPerDescriptorPage();

        for (long extentNo = 0; extentNo < 100_000; extentNo++) {
            ExtentId expected = ExtentId.of(SPACE, extentNo);
            ExtentDescriptorLocation location = layout.locate(expected);
            assertTrue(location.slotInPage() >= 0);
            assertTrue(location.slotInPage() < legacyCapacity);
            assertEquals(expected, layout.extentIdOfNode(SPACE, location.listNodeAddress()));

            if (extentNo < legacyCapacity) {
                assertEquals(PageNo.of(0), location.descriptorPageId().pageNo());
                assertEquals(extentNo, location.slotInPage());
            }
        }
    }

    /**
     * 验证管理区页号公式、管理 extent 判定和不同页大小是否需要 overflow 的边界。
     *
     * @param pageBytes 受支持的实例页大小；不同值覆盖 C 大于或小于 G 的两类布局
     */
    @ParameterizedTest
    @ValueSource(ints = {4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024})
    void shouldClassifyPrimaryBitmapOverflowAndReservedExtent(int pageBytes) {
        PageSize pageSize = PageSize.ofBytes(pageBytes);
        ExtentManagementRegionLayout layout = new ExtentManagementRegionLayout(pageSize);
        long groupPages = pageSize.bytes();
        long extentsPerGroup = layout.extentsPerManagementRegion();

        assertEquals(PageNo.of(groupPages), layout.primaryXdesPageNo(1));
        assertEquals(PageNo.of(groupPages + 1), layout.bitmapPageNo(1));
        assertEquals(PageNo.of(groupPages + 5), layout.overflowXdesPageNo(1));
        assertTrue(layout.isManagementExtent(extentsPerGroup));
        assertFalse(layout.isManagementExtent(extentsPerGroup + 1));
        assertEquals(extentsPerGroup > layout.entriesPerDescriptorPage(), layout.requiresOverflowPage(1));
    }

    /**
     * 验证合法 descriptor 页之外的地址不能被解释成 XDES node，避免损坏链指针把 FLST 写入业务页。
     */
    @ParameterizedTest
    @ValueSource(ints = {4 * 1024, 16 * 1024, 64 * 1024})
    void shouldRejectNonXdesAndMisalignedNodeAddresses(int pageBytes) {
        ExtentManagementRegionLayout layout = new ExtentManagementRegionLayout(PageSize.ofBytes(pageBytes));
        assertThrows(FspMetadataException.class, () -> layout.extentIdOfNode(
                SPACE, FileAddress.of(PageNo.of(7), ExtentDescriptorLayout.PREV)));
        assertThrows(FspMetadataException.class, () -> layout.extentIdOfNode(
                SPACE, FileAddress.of(PageNo.of(0), ExtentDescriptorLayout.PREV + 1)));
    }

    /**
     * 64KB 页最后一个兼容槽若错误清理完整 32-byte padding 会覆盖 FIL trailer；有效 bitmap 长度必须
     * 只覆盖八个字节，同时保留 68-byte stride 兼容性。
     */
    @Test
    void activeBitmapOfLast64kCompatibilitySlotStopsBeforeTrailer() {
        PageSize pageSize = PageSize.ofBytes(64 * 1024);
        int lastSlot = Math.toIntExact(ExtentDescriptorLayout.maxEntriesInPage0(pageSize) - 1L);
        int bitmapStart = ExtentDescriptorLayout.entryOffsetInPage(lastSlot)
                + ExtentDescriptorLayout.BITMAP;
        int trailer = PageEnvelopeLayout.trailerOffset(pageSize);

        assertTrue(bitmapStart + ExtentDescriptorLayout.activeBitmapBytes(pageSize) <= trailer);
        assertTrue(bitmapStart + ExtentDescriptorLayout.BITMAP_BYTES > trailer,
                "full padding clear would reproduce the trailer-overwrite bug");
    }
}
