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

    /**
     * 根据调用参数构造 {@code of} 对应的数据库内核值对象领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param pageNo 参与 {@code of} 的稳定领域标识 {@code PageNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code of} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
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
