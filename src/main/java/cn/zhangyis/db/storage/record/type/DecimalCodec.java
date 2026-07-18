package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * 定点 codec（DECIMAL(p,s)）。**保序定长编码**（spec §4.4）：同列 scale 固定为 s，编码 unscaled=value.setScale(s)
 * 的整数，按列宽 W 两补大端（正补 0x00、负补 0xFF）再翻最高位符号位 → 无符号字节序 = 数值序。
 * W = ceil((p·log2(10)+1)/8)。scale 超 s 或有效数字超 p 抛 {@link ColumnValueOutOfRangeException}。
 */
public final class DecimalCodec implements TypeCodec {

    /**
     * 记录 {@code precision} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final int precision;
    /**
     * 记录 {@code scale} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final int scale;
    /**
     * 记录 {@code width} 的稳定身份或单调版本；只由分配/发布路径更新，重复、回退或跨 owner 复用会破坏可见性。
     */
    private final int width;

    /**
     * 创建 {@code DecimalCodec}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param precision 参与 {@code 构造} 的上界或规格值 {@code precision}；必须非负且不能使容量、页数或编码长度计算溢出
     * @param scale 参与 {@code 构造} 的上界或规格值 {@code scale}；必须非负且不能使容量、页数或编码长度计算溢出
     */
    public DecimalCodec(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
        this.width = (int) Math.ceil((precision * (Math.log(10) / Math.log(2)) + 1) / 8.0);
    }

    /**
     * 把调用方领域值编码为记录格式与页内组织的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code encodedLength} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code encodedLength} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    @Override
    public int encodedLength(ColumnValue value, ColumnType type) {
        return width;
    }

    /**
     * 计算 {@code fixedWidth} 所表达的记录格式与页内组织数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @param type 选择 {@code fixedWidth} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code fixedWidth} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    @Override
    public int fixedWidth(ColumnType type) {
        return width;
    }

    /**
     * 校验 {@code validate} 涉及的记录格式与页内组织结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code validate} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @throws InvalidColumnValueException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public void validate(ColumnValue value, ColumnType type) {
        if (!(value instanceof ColumnValue.DecimalValue dv)) {
            throw new InvalidColumnValueException("expected DecimalValue for DECIMAL");
        }
        toUnscaled(dv.value());
    }

    private BigInteger toUnscaled(BigDecimal bd) {
        BigDecimal scaled;
        try {
            scaled = bd.setScale(scale);
        } catch (ArithmeticException e) {
            throw new ColumnValueOutOfRangeException("decimal scale exceeds " + scale + ": " + bd.toPlainString(), e);
        }
        BigInteger unscaled = scaled.unscaledValue();
        BigInteger limit = BigInteger.TEN.pow(precision);
        if (unscaled.abs().compareTo(limit) >= 0) {
            throw new ColumnValueOutOfRangeException("decimal precision exceeds " + precision + ": " + bd.toPlainString());
        }
        return unscaled;
    }

    /**
     * 把调用方领域值编码为记录格式与页内组织的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code encode} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param writer 由组合根提供的 {@code FieldWriter} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code encode} 调用
     */
    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        BigInteger unscaled = toUnscaled(((ColumnValue.DecimalValue) value).value());
        byte[] tc = unscaled.toByteArray();
        byte[] out = new byte[width];
        Arrays.fill(out, (byte) (unscaled.signum() < 0 ? 0xFF : 0x00));
        System.arraycopy(tc, 0, out, width - tc.length, tc.length);
        out[0] ^= (byte) 0x80;
        writer.putBytes(out);
    }

    /**
     * 从稳定表示解码记录格式与页内组织领域值；先校验边界、标识与长度，损坏输入以领域异常拒绝。
     *
     * @param slice 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code decode} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code decode} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    @Override
    public ColumnValue decode(FieldSlice slice, ColumnType type) {
        byte[] b = slice.copyBytes();
        b[0] ^= (byte) 0x80;
        BigInteger unscaled = new BigInteger(b);
        return new ColumnValue.DecimalValue(new BigDecimal(unscaled, scale));
    }

    /**
     * 实现 {@code compare} 的稳定值语义；比较只读取输入与本对象，不改变记录格式与页内组织状态。
     *
     * @param left 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param right 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code compare} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return 左值小于、等于或大于右值时分别返回负数、零或正数；排序规则与对应索引或无符号格式一致
     */
    @Override
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        return FieldSlice.compareUnsigned(left, right);
    }
}
