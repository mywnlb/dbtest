package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TypeId;

/** ENUM 1-based ordinal 定长 codec：N<=255 使用 1B，否则使用 2B unsigned big-endian。 */
public final class EnumCodec implements TypeCodec {

    /** schema dictionary member 数。 */
    private final int memberCount;

    /** ordinal 的页内固定宽度。 */
    private final int width;

    /** 创建绑定单一字典规模的 codec。
     *
     * @param memberCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public EnumCodec(int memberCount) {
        if (memberCount < 1 || memberCount > 65_535) {
            throw new DatabaseValidationException("ENUM member count out of range: " + memberCount);
        }
        this.memberCount = memberCount;
        this.width = memberCount <= 255 ? 1 : 2;
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
     * @throws ColumnValueOutOfRangeException 输入值或资源需求超出编码、页面或容量上限时抛出；调用方应缩小请求、回滚或等待资源释放
     */
    @Override
    public void validate(ColumnValue value, ColumnType type) {
        if (!(value instanceof ColumnValue.EnumValue enumValue)) {
            throw new InvalidColumnValueException("expected EnumValue for " + type.typeId());
        }
        if (type.typeId() != TypeId.ENUM || type.symbols().size() != memberCount) {
            throw new InvalidColumnValueException("ENUM codec/type dictionary mismatch");
        }
        if (enumValue.ordinal() < 1 || enumValue.ordinal() > memberCount) {
            throw new ColumnValueOutOfRangeException(
                    "ENUM ordinal out of range 1.." + memberCount + ": " + enumValue.ordinal());
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
        int ordinal = ((ColumnValue.EnumValue) value).ordinal();
        for (int i = width - 1; i >= 0; i--) {
            writer.putByte(ordinal >>> (i * 8));
        }
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
        int ordinal = 0;
        for (int i = 0; i < width; i++) {
            ordinal = (ordinal << 8) | slice.byteAt(i);
        }
        return new ColumnValue.EnumValue(ordinal);
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
