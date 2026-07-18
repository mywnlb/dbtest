package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 稳定 table 身份；名称改变时仍保持不变，undo 和 storage binding 依赖该值。
 *
 * @param value 由 {@code 构造} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
 */
public record TableId(long value) {
    public TableId {
        if (value <= 0) {
            throw new DatabaseValidationException("table id must be positive: " + value);
        }
    }

    /**
     * 根据调用参数构造 {@code of} 对应的数据字典领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param value 由 {@code of} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     * @return {@code of} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public static TableId of(long value) {
        return new TableId(value);
    }
}
