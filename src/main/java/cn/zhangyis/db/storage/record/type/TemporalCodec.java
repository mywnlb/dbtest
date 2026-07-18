package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TypeId;

/**
 * 时间 codec：DATE=4B signed epochDay，TIME/DATETIME/TIMESTAMP=8B signed long，YEAR=2B unsigned。
 * signed 值翻转最高符号位后大端，YEAR 原值大端，因此所有类型的 unsigned byte 次序都等于其时间线性序。
 *
 * <p><b>教学简化</b>：TIME 的 MySQL 业务范围、TIMESTAMP 的 session 时区转换和 YEAR 的 MySQL 合法显示范围
 * 均属于 SQL/session 层；Record 只维护定长物理范围与保序不变量。
 */
public final class TemporalCodec implements TypeCodec {

    /** 当前 codec 唯一接受并在 decode 时恢复的逻辑时间种类。 */
    private final TemporalKind kind;

    /** 字段固定物理宽度；RecordFieldResolver 依赖它定位后续定长列。 */
    private final int width;

    /** YEAR 为 unsigned，其他时间归一值均按 signed 编码。 */
    private final boolean unsigned;

    /**
     * 为单一时间种类创建无状态 codec；宽度和 signedness 在构造时冻结，实例可安全共享。
     *
     * @param kind 选择 {@code 构造} 分支的 {@code TemporalKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public TemporalCodec(TemporalKind kind) {
        if (kind == null) {
            throw new DatabaseValidationException("temporal kind must not be null");
        }
        this.kind = kind;
        this.width = switch (kind) {
            case YEAR -> 2;
            case DATE -> 4;
            case TIME, DATETIME, TIMESTAMP -> 8;
        };
        this.unsigned = kind == TemporalKind.YEAR;
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
        if (!(value instanceof ColumnValue.TemporalValue tv)) {
            throw new InvalidColumnValueException("expected TemporalValue for " + type.typeId());
        }
        TypeId expectedType = switch (kind) {
            case DATE -> TypeId.DATE;
            case TIME -> TypeId.TIME;
            case DATETIME -> TypeId.DATETIME;
            case TIMESTAMP -> TypeId.TIMESTAMP;
            case YEAR -> TypeId.YEAR;
        };
        if (type.typeId() != expectedType) {
            throw new InvalidColumnValueException(
                    "temporal column type mismatch: expected " + expectedType + " got " + type.typeId());
        }
        if (tv.kind() != kind) {
            throw new InvalidColumnValueException("temporal kind mismatch: expected " + kind + " got " + tv.kind());
        }
        long n = tv.normalized();
        if (kind == TemporalKind.DATE && (n < Integer.MIN_VALUE || n > Integer.MAX_VALUE)) {
            throw new ColumnValueOutOfRangeException("DATE epochDay out of int range: " + n);
        }
        if (kind == TemporalKind.YEAR && (n < 0 || n > 0xFFFFL)) {
            throw new ColumnValueOutOfRangeException("YEAR unsigned 2B out of range: " + n);
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
        long n = ((ColumnValue.TemporalValue) value).normalized();
        long stored = unsigned ? n : n ^ signBit();
        for (int i = width - 1; i >= 0; i--) {
            writer.putByte((int) ((stored >>> (i * 8)) & 0xFF));
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
        long stored = 0;
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        for (int i = 0; i < width; i++) {
            stored = (stored << 8) | slice.byteAt(i);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        long n;
        if (unsigned) {
            n = stored;
        } else {
            long x = stored ^ signBit();
            int shift = 64 - width * 8;
            n = (shift == 0) ? x : (x << shift) >> shift;
        }
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return new ColumnValue.TemporalValue(kind, n);
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

    /** 返回当前 signed 宽度的最高位掩码，供保序翻转与恢复共用。 */
    private long signBit() {
        return 1L << (width * 8 - 1);
    }
}
