package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 变长字段目录（innodb-record-design §5.2）。按非 NULL 变长列的列序，每列 2 字节长度（u16）。
 */
public final class VarLenDirectory {

    /**
     * 构造时冻结的 {@code lengths} 布局或分片数组；元素顺序和长度具有领域含义，构造时必须防御性复制，之后不得替换或越界访问。
     */
    private final int[] lengths;

    /**
     * 创建 {@code VarLenDirectory}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param lengths 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public VarLenDirectory(int[] lengths) {
        if (lengths == null) {
            throw new DatabaseValidationException("var len directory lengths must not be null");
        }
        for (int len : lengths) {
            if (len < 0 || len > 0xFFFF) {
                throw new DatabaseValidationException("var field length out of u16 range: " + len);
            }
        }
        this.lengths = lengths.clone();
    }

    /** 条目数（= 非 NULL 变长列数）。 */
    public int count() {
        return lengths.length;
    }

    /** 第 i 条长度。
     *
     * @param i 参与 {@code length} 的零基位置 {@code i}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code length} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    public int length(int i) {
        return lengths[i];
    }

    /** 目录字节数。 */
    public int byteLength() {
        return lengths.length * 2;
    }

    /** 写目录到 buf 的 at 处。
     *
     * @param buf 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param at 参与 {@code writeTo} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     */
    public void writeTo(byte[] buf, int at) {
        for (int i = 0; i < lengths.length; i++) {
            U16.put(buf, at + i * 2, lengths[i]);
        }
    }

    /** 从 buf 的 at 处读 count 条目录。
     *
     * @param buf 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param at 参与 {@code readFrom} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     * @param count 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code readFrom} 准备或解码出的中间领域对象；成功时不为 {@code null}，其边界、资源归属和后续发布阶段已明确
     */
    public static VarLenDirectory readFrom(byte[] buf, int at, int count) {
        int[] l = new int[count];
        for (int i = 0; i < count; i++) {
            l[i] = U16.get(buf, at + i * 2);
        }
        return new VarLenDirectory(l);
    }
}
