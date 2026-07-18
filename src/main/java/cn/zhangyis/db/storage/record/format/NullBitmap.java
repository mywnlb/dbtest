package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/**
 * NULL 位图（innodb-record-design §5.2）。位数 = schema 中 nullable 列数，按 nullable 列在 schema 中的顺序编号；1=NULL。
 */
public final class NullBitmap {

    /**
     * 记录 {@code count} 的非负位置、容量或计数；写入前必须校验所属页/集合上界，溢出会破坏布局或资源记账。
     */
    private final int count;
    /**
     * 本对象独占的 {@code bits} 数据缓冲；构造和访问路径必须遵守防御性复制或受控视图约束，不能泄漏可变数组。
     */
    private final byte[] bits;

    /** 新建全 0（无 NULL）位图。
     *
     * @param nullableCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public NullBitmap(int nullableCount) {
        if (nullableCount < 0) {
            throw new DatabaseValidationException("nullable count must be non-negative: " + nullableCount);
        }
        this.count = nullableCount;
        this.bits = new byte[byteLength(nullableCount)];
    }

    private NullBitmap(int count, byte[] bits) {
        this.count = count;
        this.bits = bits;
    }

    /** count 个 nullable 列所需字节数。
     *
     * @param count 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code byteLength} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    public static int byteLength(int count) {
        return (count + 7) / 8;
    }

    /** 置第 i 个 nullable 列为 NULL。
     *
     * @param i 参与 {@code set} 的零基位置 {@code i}；必须非负且小于所属页面、集合或持久结构的容量
     */
    public void set(int i) {
        check(i);
        bits[i / 8] |= (byte) (1 << (i % 8));
    }

    /** 第 i 个 nullable 列是否 NULL。
     *
     * @param i 参与 {@code get} 的零基位置 {@code i}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code get} 成功完成其命名的受控动作并发布结果时为 {@code true}；未命中、未执行或状态竞争失败时为 {@code false}
     */
    public boolean get(int i) {
        check(i);
        return (bits[i / 8] & (1 << (i % 8))) != 0;
    }

    /** 位图字节数。 */
    public int byteLength() {
        return bits.length;
    }

    /** 写位图到 buf 的 at 处。
     *
     * @param buf 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param at 参与 {@code writeTo} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     */
    public void writeTo(byte[] buf, int at) {
        System.arraycopy(bits, 0, buf, at, bits.length);
    }

    /** 从 buf 的 at 处读 count 列的位图。
     *
     * @param buf 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param at 参与 {@code readFrom} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     * @param count 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code readFrom} 准备或解码出的中间领域对象；成功时不为 {@code null}，其边界、资源归属和后续发布阶段已明确
     */
    public static NullBitmap readFrom(byte[] buf, int at, int count) {
        byte[] b = Arrays.copyOfRange(buf, at, at + byteLength(count));
        return new NullBitmap(count, b);
    }

    private void check(int i) {
        if (i < 0 || i >= count) {
            throw new DatabaseValidationException("null bitmap index out of range: " + i + " (count " + count + ")");
        }
    }
}
