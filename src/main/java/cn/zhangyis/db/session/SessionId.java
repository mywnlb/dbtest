package cn.zhangyis.db.session;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 数据库实例内单调分配的 Session 身份，同时可安全映射为稳定 MDL owner。
 *
 * @param value 由 {@code 构造} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
 */
public record SessionId(long value) {
    public SessionId {
        if (value <= 0) throw new DatabaseValidationException("session id must be positive");
    }
    /**
     * 根据调用参数构造 {@code of} 对应的会话与事务边界领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param value 由 {@code of} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     * @return {@code of} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public static SessionId of(long value) { return new SessionId(value); }
}
