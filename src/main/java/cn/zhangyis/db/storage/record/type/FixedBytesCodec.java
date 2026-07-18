package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;

/**
 * 定长字节 codec：CHAR(padByte=空格, asString=true) / BINARY(padByte=0x00, asString=false)。
 * 编码补齐到 n 字节；超长抛 {@link ColumnValueOutOfRangeException}。CHAR 解码去尾部空格（MySQL 语义），BINARY 保留全 n 字节。
 * 字符模式按 ColumnType 声明的 charset 严格编解码并经其 collation 比较；二进制模式保持原始无符号字节序。
 */
public final class FixedBytesCodec implements TypeCodec {

    /**
     * 记录 {@code nBytes} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final int nBytes;
    /**
     * 记录 {@code padByte} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final byte padByte;
    /**
     * 记录 {@code asString} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
     */
    private final boolean asString;

    /** 只读字符服务；仅字符模式使用，负责严格 charset 与精确 collation pair。 */
    private final CharacterTypeRegistry characters;

    /**
     * 创建 {@code FixedBytesCodec}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param nBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param padByte 参与 {@code 构造} 的单字节编码 {@code padByte}；必须符合目标格式的 tag、填充值或无符号字节范围约定
     * @param asString 值的编码或展示形态；为 {@code true} 时按名称所示文本、JSON 或外部引用格式处理
     */
    public FixedBytesCodec(int nBytes, byte padByte, boolean asString) {
        this(nBytes, padByte, asString, CharacterTypeRegistry.defaults());
    }

    /**
     * 创建绑定字符服务的定长 codec；由 {@link TypeCodecRegistry} 注入其共享只读 registry。
     * @param nBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param padByte 参与 {@code 构造} 的单字节编码 {@code padByte}；必须符合目标格式的 tag、填充值或无符号字节范围约定
     * @param asString 值的编码或展示形态；为 {@code true} 时按名称所示文本、JSON 或外部引用格式处理
     * @param characters 由组合根提供的 {@code CharacterTypeRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    FixedBytesCodec(int nBytes, byte padByte, boolean asString, CharacterTypeRegistry characters) {
        if (nBytes < 1) {
            throw new DatabaseValidationException("fixed bytes length must be positive: " + nBytes);
        }
        if (characters == null) {
            throw new DatabaseValidationException("character type registry must not be null");
        }
        this.nBytes = nBytes;
        this.padByte = padByte;
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
        return nBytes;
    }

    /**
     * 计算 {@code fixedWidth} 所表达的记录格式与页内组织数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @param type 选择 {@code fixedWidth} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code fixedWidth} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    @Override
    public int fixedWidth(ColumnType type) {
        return nBytes;
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
        if (b.length > nBytes) {
            throw new ColumnValueOutOfRangeException("value too long for fixed(" + nBytes + "): " + b.length);
        }
    }

    /**
     * 把调用方领域值编码为记录格式与页内组织的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code encode} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param writer 由组合根提供的 {@code FieldWriter} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code encode} 调用
     * @throws ColumnValueOutOfRangeException 输入值或资源需求超出编码、页面或容量上限时抛出；调用方应缩小请求、回滚或等待资源释放
     */
    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        byte[] b = bytesOf(value, type);
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        if (b.length > nBytes) {
            throw new ColumnValueOutOfRangeException("value too long for fixed(" + nBytes + "): " + b.length);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        writer.putBytes(b);
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        for (int i = b.length; i < nBytes; i++) {
            writer.putByte(padByte & 0xFF);
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
        byte[] b = slice.copyBytes();
        if (asString) {
            int end = b.length;
            while (end > 0 && b[end - 1] == 0x20) {
                end--;
            }
            return new ColumnValue.StringValue(
                    characters.decode(new FieldSlice(b, 0, end), type.charset()));
        }
        return new ColumnValue.BinaryValue(b);
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
