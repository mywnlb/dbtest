package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;

/**
 * 变长字节 codec：VARCHAR(asString=true) / VARBINARY(asString=false)。字段内只存字节本身，长度由 record.format 变长目录记录。
 * 超过 maxBytes 抛 {@link ColumnValueOutOfRangeException}。字符模式按 schema charset/collation，二进制模式按原始字节。
 */
public final class VarBytesCodec implements TypeCodec {

    /**
     * 记录 {@code maxBytes} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final int maxBytes;
    /**
     * 记录 {@code asString} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
     */
    private final boolean asString;

    /** 只读字符服务；字符模式的严格编解码与排序策略均由它提供。 */
    private final CharacterTypeRegistry characters;

    /**
     * 创建 {@code VarBytesCodec}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param maxBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param asString 值的编码或展示形态；为 {@code true} 时按名称所示文本、JSON 或外部引用格式处理
     */
    public VarBytesCodec(int maxBytes, boolean asString) {
        this(maxBytes, asString, CharacterTypeRegistry.defaults());
    }

    /** 创建绑定共享字符服务的变长 codec；由类型 registry 使用。
     *
     * @param maxBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param asString 值的编码或展示形态；为 {@code true} 时按名称所示文本、JSON 或外部引用格式处理
     * @param characters 由组合根提供的 {@code CharacterTypeRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    VarBytesCodec(int maxBytes, boolean asString, CharacterTypeRegistry characters) {
        if (maxBytes < 1) {
            throw new DatabaseValidationException("var bytes max length must be positive: " + maxBytes);
        }
        if (characters == null) {
            throw new DatabaseValidationException("character type registry must not be null");
        }
        this.maxBytes = maxBytes;
        this.asString = asString;
        this.characters = characters;
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
        return bytesOf(value, type).length;
    }

    /**
     * 计算 {@code fixedWidth} 所表达的记录格式与页内组织数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @param type 选择 {@code fixedWidth} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code fixedWidth} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public int fixedWidth(ColumnType type) {
        throw new DatabaseValidationException("variable type has no fixed width: " + type.typeId());
    }

    /**
     * 校验 {@code validate} 涉及的记录格式与页内组织结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code validate} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @throws ColumnValueOutOfRangeException 输入值或资源需求超出编码、页面或容量上限时抛出；调用方应缩小请求、回滚或等待资源释放
     */
    @Override
    public void validate(ColumnValue value, ColumnType type) {
        byte[] b = bytesOf(value, type);
        if (b.length > maxBytes) {
            throw new ColumnValueOutOfRangeException("value too long for var(" + maxBytes + "): " + b.length);
        }
    }

    /**
     * 把调用方领域值编码为记录格式与页内组织的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code encode} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param writer 由组合根提供的 {@code FieldWriter} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code encode} 调用
     * @throws ColumnValueOutOfRangeException 输入值或资源需求超出编码、页面或容量上限时抛出；调用方应缩小请求、回滚或等待资源释放
     */
    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        byte[] b = bytesOf(value, type);
        if (b.length > maxBytes) {
            throw new ColumnValueOutOfRangeException("value too long for var(" + maxBytes + "): " + b.length);
        }
        writer.putBytes(b);
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
        return asString ? new ColumnValue.StringValue(characters.decode(new FieldSlice(b, 0, b.length), type.charset()))
                : new ColumnValue.BinaryValue(b);
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
        CollationStrategy collation = asString
                ? characters.collationFor(type.charset(), type.collation())
                : BinaryCollation.INSTANCE;
        return collation.compare(left.backing(), left.offset(), left.length(),
                right.backing(), right.offset(), right.length());
    }

    private byte[] bytesOf(ColumnValue value, ColumnType type) {
        if (asString) {
            if (!(value instanceof ColumnValue.StringValue sv)) {
                throw new InvalidColumnValueException("expected StringValue for " + type.typeId());
            }
            return characters.encode(sv.value(), type.charset());
        }
        if (!(value instanceof ColumnValue.BinaryValue bv)) {
            throw new InvalidColumnValueException("expected BinaryValue for " + type.typeId());
        }
        return bv.value();
    }
}
