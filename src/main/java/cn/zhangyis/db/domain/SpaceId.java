package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 表空间编号。FIL/FSP 层通过它定位 tablespace，禁止用裸 int 在模块间传递以免混淆页号、段号和空间号。
 *
 * @param value 实例内的 tablespace 编号；后续 TablespaceRegistry、PageStore、FSP 元数据都以它作为第一层定位键。
 */
public record SpaceId(int value) {

    public SpaceId {
        if (value < 0) {
            throw new DatabaseValidationException("space id must be non-negative");
        }
    }

    /**
     * 创建表空间编号；负数不是合法 tablespace 标识，会抛出项目校验异常。
     *
     * @param value 表空间编号。
     * @return 通过校验的表空间编号。
     */
    public static SpaceId of(int value) {
        return new SpaceId(value);
    }
}
