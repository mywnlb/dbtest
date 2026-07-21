package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;
import java.util.HexFormat;

/**
 * XA 全局事务分支身份。字节数组在构造和读取时均复制，避免 registry key 因调用方修改数组而漂移。
 *
 * <p>边界对齐 MySQL XA SQL 接口常用限制：gtrid 1..64 字节、bqual 0..64 字节，
 * 两者合计不超过 128 字节；formatId 保留完整 signed int。</p>
 */
public final class XaId {

    /** 全局事务身份最大字节数。 */
    public static final int MAX_GTRID_BYTES = 64;
    /** 分支限定符最大字节数。 */
    public static final int MAX_BQUAL_BYTES = 64;
    /** gtrid 与 bqual 合计上限。 */
    public static final int MAX_TOTAL_BYTES = 128;

    /** 外部事务管理器定义的 signed 格式身份。 */
    private final int formatId;
    /** XA 全局事务身份，不允许为空。 */
    private final byte[] gtrid;
    /** 当前资源分支限定符；空数组表示默认分支。 */
    private final byte[] bqual;

    /**
     * 创建稳定 XID。
     *
     * @param formatId 外部格式编号；允许任意 signed int
     * @param gtrid 全局事务字节；长度必须为 1..64
     * @param bqual 分支字节；长度必须为 0..64
     * @throws DatabaseValidationException 数组缺失或长度越界时抛出
     */
    public XaId(int formatId, byte[] gtrid, byte[] bqual) {
        if (gtrid == null || bqual == null
                || gtrid.length == 0 || gtrid.length > MAX_GTRID_BYTES
                || bqual.length > MAX_BQUAL_BYTES
                || gtrid.length + bqual.length > MAX_TOTAL_BYTES) {
            throw new DatabaseValidationException("XA XID byte lengths are invalid");
        }
        this.formatId = formatId;
        this.gtrid = Arrays.copyOf(gtrid, gtrid.length);
        this.bqual = Arrays.copyOf(bqual, bqual.length);
    }

    /** @return XID signed format id */
    public int formatId() {
        return formatId;
    }

    /** @return gtrid 防御性副本 */
    public byte[] gtrid() {
        return Arrays.copyOf(gtrid, gtrid.length);
    }

    /** @return bqual 防御性副本；默认分支为空数组 */
    public byte[] bqual() {
        return Arrays.copyOf(bqual, bqual.length);
    }

    /** @return gtrid 的稳定大写十六进制诊断文本 */
    public String gtridHex() {
        return HexFormat.of().withUpperCase().formatHex(gtrid);
    }

    /** @return bqual 的稳定大写十六进制诊断文本 */
    public String bqualHex() {
        return HexFormat.of().withUpperCase().formatHex(bqual);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof XaId that
                && formatId == that.formatId
                && Arrays.equals(gtrid, that.gtrid)
                && Arrays.equals(bqual, that.bqual);
    }

    @Override
    public int hashCode() {
        int result = Integer.hashCode(formatId);
        result = 31 * result + Arrays.hashCode(gtrid);
        return 31 * result + Arrays.hashCode(bqual);
    }

    @Override
    public String toString() {
        return "XaId[formatId=" + formatId + ",gtrid=" + gtridHex() + ",bqual=" + bqualHex() + "]";
    }
}
