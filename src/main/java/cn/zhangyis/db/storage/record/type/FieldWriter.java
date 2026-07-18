package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 字段顺序写入器：从 backing 的 start 处顺序写字节。供 codec 编码，不依赖 buf/页。
 */
public final class FieldWriter {

    /**
     * 本对象独占的 {@code backing} 数据缓冲；构造和访问路径必须遵守防御性复制或受控视图约束，不能泄漏可变数组。
     */
    private final byte[] backing;
    /**
     * 记录 {@code start} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final int start;
    /**
     * 记录 {@code pos} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private int pos;

    /**
     * 创建 {@code FieldWriter}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param backing 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param start 参与 {@code 构造} 的零基位置 {@code start}；必须非负且小于所属页面、集合或持久结构的容量
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public FieldWriter(byte[] backing, int start) {
        if (backing == null) {
            throw new DatabaseValidationException("field writer backing must not be null");
        }
        if (start < 0 || start > backing.length) {
            throw new DatabaseValidationException("field writer start out of range: " + start);
        }
        this.backing = backing;
        this.start = start;
    }

    /** 写一字节（取低 8 位）。
     *
     * @param b 参与 {@code putByte} 的单字节编码 {@code b}；必须符合目标格式的 tag、填充值或无符号字节范围约定
     */
    public void putByte(int b) {
        backing[start + pos] = (byte) b;
        pos++;
    }

    /** 写一段字节。
     *
     * @param src 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void putBytes(byte[] src) {
        if (src == null) {
            throw new DatabaseValidationException("write source must not be null");
        }
        System.arraycopy(src, 0, backing, start + pos, src.length);
        pos += src.length;
    }

    /** 已写字节数。 */
    public int written() {
        return pos;
    }
}
