package cn.zhangyis.db.domain;

import java.util.Objects;

/**
 * 物理页定位键。FIL 层只能通过 SpaceId + PageNo 计算文件偏移，不理解 segment 或 record 语义。
 *
 * @param spaceId 页所属的表空间，决定 PageStore 应访问哪个 tablespace 文件或句柄。
 * @param pageNo 表空间内页号，必须和 spaceId 一起使用才有全局定位意义。
 */
public record PageId(SpaceId spaceId, PageNo pageNo) {

    public PageId {
        Objects.requireNonNull(spaceId, "spaceId");
        Objects.requireNonNull(pageNo, "pageNo");
    }

    public static PageId of(SpaceId spaceId, PageNo pageNo) {
        return new PageId(spaceId, pageNo);
    }

    /**
     * 计算物理文件页偏移。数据从 PageId 的页号进入，按实例 PageSize 做乘法换算；FIL 层据此做 positional IO。
     *
     * @param pageSize 实例级页大小。
     * @return 该页在 tablespace 文件中的起始字节偏移。
     */
    public long offset(PageSize pageSize) {
        Objects.requireNonNull(pageSize, "pageSize");
        return Math.multiplyExact(pageNo.value(), pageSize.bytes());
    }
}
