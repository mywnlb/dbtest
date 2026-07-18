package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TypeId;

/**
 * `BIT(n)` 定长编码策略。位串从首字节最高位开始左对齐，最后字节未使用低位必须为 0；这一 canonical 约束确保
 * 同一逻辑位串只有一种页内字节表示。固定宽度下 unsigned byte lexicographic 即 bit string 自然序。
 */
public final class BitCodec implements TypeCodec {

    /** schema 声明的有效 bit 数。 */
    private final int bitWidth;

    /** 页内固定 byte 数，等于 ceil(bitWidth/8)。 */
    private final int byteWidth;

    /** 最后字节中必须保持为 0 的低位掩码；byte-aligned 时为 0。 */
    private final int unusedLowBitMask;

    /** 为 1..64 位的单一 schema 宽度创建无状态 codec。
     *
     * @param bitWidth 参与 {@code 构造} 的原始数值身份 {@code bitWidth}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public BitCodec(int bitWidth) {
        if (bitWidth < 1 || bitWidth > 64) {
            throw new DatabaseValidationException("BIT width out of range 1..64: " + bitWidth);
        }
        this.bitWidth = bitWidth;
        this.byteWidth = (bitWidth + 7) / 8;
        int unusedLowBits = byteWidth * 8 - bitWidth;
        this.unusedLowBitMask = unusedLowBits == 0 ? 0 : (1 << unusedLowBits) - 1;
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
        return byteWidth;
    }

    /**
     * 计算 {@code fixedWidth} 所表达的记录格式与页内组织数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @param type 选择 {@code fixedWidth} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code fixedWidth} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    @Override
    public int fixedWidth(ColumnType type) {
        return byteWidth;
    }

    /**
     * 校验逻辑值、schema bitWidth、精确 byteWidth 和 canonical 尾位；失败时不写任何目标字节。
     *
     * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code validate} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @throws InvalidColumnValueException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public void validate(ColumnValue value, ColumnType type) {
        if (!(value instanceof ColumnValue.BitValue bitValue)) {
            throw new InvalidColumnValueException("expected BitValue for " + type.typeId());
        }
        if (type.typeId() != TypeId.BIT || type.length() != bitWidth) {
            throw new InvalidColumnValueException(
                    "BIT codec/type width mismatch: codec=" + bitWidth + " type=" + type.typeId() + "(" + type.length() + ")");
        }
        byte[] bytes = bitValue.value();
        if (bytes.length != byteWidth) {
            throw new InvalidColumnValueException(
                    "BIT(" + bitWidth + ") requires " + byteWidth + " bytes, got " + bytes.length);
        }
        if ((bytes[byteWidth - 1] & unusedLowBitMask) != 0) {
            throw new InvalidColumnValueException(
                    "BIT(" + bitWidth + ") unused low bits must be zero");
        }
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
        validate(value, type);
        writer.putBytes(((ColumnValue.BitValue) value).value());
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
        return new ColumnValue.BitValue(slice.copyBytes());
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
