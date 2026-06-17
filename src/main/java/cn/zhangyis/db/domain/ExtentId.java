package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Objects;

/**
 * 区编号。Extent 是物理连续页范围，segment 只是 inode 记录的逻辑归属，二者不能混为一谈。
 *
 * @param spaceId extent 所属表空间，避免不同 tablespace 的相同 extentNo 被误认为同一物理范围。
 * @param extentNo 表空间内 extent 序号；FSP/XDES 后续用它定位 descriptor 和位图。
 */
public record ExtentId(SpaceId spaceId, long extentNo) {

    public ExtentId {
        Objects.requireNonNull(spaceId, "spaceId");
        if (extentNo < 0) {
            throw new DatabaseValidationException("extent no must be non-negative");
        }
    }

    /**
     * 创建区编号；extentNo 必须非负，且只在指定表空间内有意义。
     *
     * @param spaceId 所属表空间。
     * @param extentNo 表空间内区编号。
     * @return 通过校验的区编号。
     */
    public static ExtentId of(SpaceId spaceId, long extentNo) {
        return new ExtentId(spaceId, extentNo);
    }

    /**
     * 从物理页定位所属 extent。数据流为 PageId.pageNo 进入，按 PageSize.pagesPerExtent 分组，输出同一表空间内的 ExtentId。
     *
     * @param pageId 物理页定位键。
     * @param pageSize 实例级页大小。
     * @return 页所属的 extent。
     */
    public static ExtentId from(PageId pageId, PageSize pageSize) {
        Objects.requireNonNull(pageId, "pageId");
        Objects.requireNonNull(pageSize, "pageSize");
        return new ExtentId(pageId.spaceId(), pageId.pageNo().value() / pageSize.pagesPerExtent());
    }

    /**
     * 计算 extent 的第一个页号。FSP 初始化和 XDES 定位用该边界决定物理页范围。
     *
     * @param pageSize 实例级页大小。
     * @return extent 起始页号。
     */
    public PageNo firstPageNo(PageSize pageSize) {
        Objects.requireNonNull(pageSize, "pageSize");
        return PageNo.of(Math.multiplyExact(extentNo, pageSize.pagesPerExtent()));
    }
}
