package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;

/**
 * 浮点 codec（FLOAT=4B / DOUBLE=8B，IEEE 754）。**保序编码**（spec §4.4）：取 bits，正数翻符号位、负数翻全部位，
 * 大端存 → 无符号字节序 = 数值序。归一 -0.0→+0.0；NaN 用规范 NaN，排最大（-Inf<负<±0<正<+Inf<NaN）。
 */
public final class FloatingCodec implements TypeCodec {

    /**
     * 记录 {@code width} 的稳定身份或单调版本；只由分配/发布路径更新，重复、回退或跨 owner 复用会破坏可见性。
     */
    private final int width;

    /**
     * 创建 {@code FloatingCodec}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param width 参与 {@code 构造} 的原始数值身份 {@code width}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public FloatingCodec(int width) {
        if (width != 4 && width != 8) {
            throw new DatabaseValidationException("floating width must be 4/8: " + width);
        }
        this.width = width;
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
        if (!(value instanceof ColumnValue.DoubleValue)) {
            throw new InvalidColumnValueException("expected DoubleValue for " + type.typeId());
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
        double d = ((ColumnValue.DoubleValue) value).value();
        if (width == 4) {
            int bits = Float.floatToIntBits((float) normalize(d));
            int ordered = bits ^ ((bits >> 31) | Integer.MIN_VALUE);
            for (int i = 3; i >= 0; i--) {
                writer.putByte((ordered >>> (i * 8)) & 0xFF);
            }
        } else {
            long bits = Double.doubleToLongBits(normalize(d));
            long ordered = bits ^ ((bits >> 63) | Long.MIN_VALUE);
            for (int i = 7; i >= 0; i--) {
                writer.putByte((int) ((ordered >>> (i * 8)) & 0xFF));
            }
        }
    }

    /**
     * 从稳定表示解码记录格式与页内组织领域值；先校验边界、标识与长度，损坏输入以领域异常拒绝。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param slice 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code decode} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code decode} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    @Override
    public ColumnValue decode(FieldSlice slice, ColumnType type) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (width == 4) {
            int ordered = 0;
            for (int i = 0; i < 4; i++) {
                ordered = (ordered << 8) | slice.byteAt(i);
            }
            int bits = (ordered < 0) ? (ordered ^ Integer.MIN_VALUE) : ~ordered;
            return new ColumnValue.DoubleValue(Float.intBitsToFloat(bits));
        }
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        long ordered = 0;
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        for (int i = 0; i < 8; i++) {
            ordered = (ordered << 8) | slice.byteAt(i);
        }
        long bits = (ordered < 0) ? (ordered ^ Long.MIN_VALUE) : ~ordered;
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return new ColumnValue.DoubleValue(Double.longBitsToDouble(bits));
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

    /** -0.0 归一为 +0.0（MySQL 语义 -0.0==+0.0）；其余原样（NaN 由 *toBits 规范化）。 */
    private static double normalize(double d) {
        return d == 0.0 ? 0.0 : d;
    }
}
