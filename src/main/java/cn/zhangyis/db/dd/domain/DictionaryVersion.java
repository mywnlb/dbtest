package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 字典发布版本；只增不减，cache read view 和 stale pin 以它判断可见性。
 *
 * @param value 由 {@code 构造} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
 */
public record DictionaryVersion(long value) implements Comparable<DictionaryVersion> {
    public DictionaryVersion {
        if (value <= 0) {
            throw new DatabaseValidationException("dictionary version must be positive: " + value);
        }
    }

    /**
     * 根据调用参数构造 {@code of} 对应的数据字典领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param value 由 {@code of} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     * @return {@code of} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public static DictionaryVersion of(long value) {
        return new DictionaryVersion(value);
    }

    /**
     * 实现 {@code compareTo} 的稳定值语义；比较只读取输入与本对象，不改变数据字典状态。
     *
     * @param other 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return 左值小于、等于或大于右值时分别返回负数、零或正数；排序规则与对应索引或无符号格式一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public int compareTo(DictionaryVersion other) {
        if (other == null) {
            throw new DatabaseValidationException("dictionary version comparison target must not be null");
        }
        return Long.compare(value, other.value);
    }
}
