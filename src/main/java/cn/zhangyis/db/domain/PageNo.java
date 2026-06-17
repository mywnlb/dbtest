package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 表空间内页号。页号只在所属 SpaceId 内有意义，跨模块定位物理页时应使用 PageId。
 *
 * @param value tablespace 内的页序号；FIL 层用它乘以 PageSize 得到物理文件偏移。
 */
public record PageNo(long value) {

    public PageNo {
        if (value < 0) {
            throw new DatabaseValidationException("page no must be non-negative");
        }
    }

    /**
     * 创建表空间内页号；页号必须非负，因为文件偏移由 pageNo * pageSize 直接推导。
     *
     * @param value 表空间内页号。
     * @return 通过校验的页号。
     */
    public static PageNo of(long value) {
        return new PageNo(value);
    }
}
