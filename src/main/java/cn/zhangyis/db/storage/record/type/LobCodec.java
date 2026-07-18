package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.json.StrictJsonValidator;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.LobReference;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TypeId;

/**
 * TEXT/BLOB/JSON 的记录字段 codec。字段首字节区分 inline payload 与 external reference；因此页内记录不需要
 * 理解 FSP 或 Buffer Pool。external envelope 保留短 prefix，但完整值只能由 storage.api.lob.LobStorage 读取。
 */
public final class LobCodec implements TypeCodec {

    /** 记录内允许直接保存的最大逻辑 payload。 */
    public static final int INLINE_PAYLOAD_LIMIT = 256;
    /** external envelope 最多保存的逻辑前缀字节。 */
    public static final int EXTERNAL_PREFIX_LIMIT = 32;

    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏记录格式与页内组织的不变量。
     */
    private static final int INLINE_TAG = 0;
    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏记录格式与页内组织的不变量。
     */
    private static final int EXTERNAL_TAG = 1;
    /**
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
    private static final int REFERENCE_BYTES = 32;
    /**
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
    private static final int EXTERNAL_HEADER_BYTES = 1 + REFERENCE_BYTES + 1;

    /** 该 codec 绑定的物理类型，阻止不同 LOB family 的 external reference 交叉解释。 */
    private final TypeId typeId;
    /** 字符类型为 true，BLOB family 为 false。 */
    private final boolean text;
    /** JSON 需要额外语法校验且永远不可进入核心索引比较。 */
    private final boolean json;
    /** 共享、只读的严格字符服务。 */
    private final CharacterTypeRegistry characters;

    /**
     * 创建 {@code LobCodec}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param typeId 参与 {@code 构造} 的稳定领域标识 {@code TypeId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param text 值的编码或展示形态；为 {@code true} 时按名称所示文本、JSON 或外部引用格式处理
     * @param json 值的编码或展示形态；为 {@code true} 时按名称所示文本、JSON 或外部引用格式处理
     * @param characters 由组合根提供的 {@code CharacterTypeRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    LobCodec(TypeId typeId, boolean text, boolean json, CharacterTypeRegistry characters) {
        if (typeId == null || characters == null) {
            throw new DatabaseValidationException("LOB codec type/character registry must not be null");
        }
        this.typeId = typeId;
        this.text = text;
        this.json = json;
        this.characters = characters;
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
     * @param type 选择 {@code encodedLength} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code encodedLength} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    @Override
    public int encodedLength(ColumnValue value, ColumnType type) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        requireBoundType(type);
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        if (value instanceof ColumnValue.ExternalValue external) {
            validateExternal(external, type);
            return EXTERNAL_HEADER_BYTES + external.inlinePrefix().length;
        }
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        byte[] bytes = logicalBytes(value, type);
        requireInline(bytes.length, type);
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return 1 + bytes.length;
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
        throw new DatabaseValidationException("overflow-capable type has no fixed width: " + type.typeId());
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
     */
    @Override
    public void encode(ColumnValue value, ColumnType type, FieldWriter writer) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        requireBoundType(type);
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        if (value instanceof ColumnValue.ExternalValue external) {
            validateExternal(external, type);
            writer.putByte(EXTERNAL_TAG);
            writeReference(writer, external.reference());
            byte[] prefix = external.inlinePrefix();
            writer.putByte(prefix.length);
            writer.putBytes(prefix);
            return;
        }
        byte[] bytes = logicalBytes(value, type);
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        requireInline(bytes.length, type);
        writer.putByte(INLINE_TAG);
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        writer.putBytes(bytes);
    }

    /**
     * 从稳定表示解码记录格式与页内组织领域值；先校验边界、标识与长度，损坏输入以领域异常拒绝。
     *
     * @param slice 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code decode} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code decode} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws InvalidColumnValueException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public ColumnValue decode(FieldSlice slice, ColumnType type) {
        requireBoundType(type);
        if (slice.length() < 1) {
            throw new InvalidColumnValueException("empty LOB record envelope for " + typeId);
        }
        return switch (slice.byteAt(0)) {
            case INLINE_TAG -> logicalValue(subSlice(slice, 1, slice.length() - 1), type);
            case EXTERNAL_TAG -> decodeExternal(slice, type);
            default -> throw new InvalidColumnValueException(
                    "unknown LOB record envelope tag " + slice.byteAt(0) + " for " + typeId);
        };
    }

    /**
     * 实现 {@code compare} 的稳定值语义；比较只读取输入与本对象，不改变记录格式与页内组织状态。
     *
     * @param left 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param right 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code compare} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return 左值小于、等于或大于右值时分别返回负数、零或正数；排序规则与对应索引或无符号格式一致
     * @throws UnsupportedColumnTypeException 请求的语法形状、类型或运行模式超出当前实现范围时抛出；调用方应改写请求或选择受支持配置
     */
    @Override
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        throw new UnsupportedColumnTypeException(
                type.typeId() + " requires an explicit prefix index; full LOB/JSON comparison is unsupported");
    }

    /**
     * LOB key 只比较 envelope 中可得的逻辑 prefix。prefix 超过 external 保存量时拒绝，绝不在持索引页 latch 时
     * 追读 LOB 页链；JSON 无论 prefix 与否都不可进入核心索引。
     *
     * @param left 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param right 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code compareKeyPart} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param prefixBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return 左值小于、等于或大于右值时分别返回负数、零或正数；排序规则与对应索引或无符号格式一致
     * @throws UnsupportedColumnTypeException 请求的语法形状、类型或运行模式超出当前实现范围时抛出；调用方应改写请求或选择受支持配置
     */
    @Override
    public int compareKeyPart(FieldSlice left, FieldSlice right, ColumnType type, int prefixBytes) {
        requireBoundType(type);
        if (json) {
            throw new UnsupportedColumnTypeException("JSON cannot participate in core index comparison");
        }
        if (prefixBytes <= 0) {
            return compare(left, right, type);
        }
        FieldSlice leftPayload = comparablePayload(left, type, prefixBytes);
        FieldSlice rightPayload = comparablePayload(right, type, prefixBytes);
        int leftLength = safePrefixLength(leftPayload, type, prefixBytes);
        int rightLength = safePrefixLength(rightPayload, type, prefixBytes);
        FieldSlice leftPrefix = subSlice(leftPayload, 0, leftLength);
        FieldSlice rightPrefix = subSlice(rightPayload, 0, rightLength);
        CollationStrategy strategy = text
                ? characters.collationFor(type.charset(), type.collation()) : BinaryCollation.INSTANCE;
        return strategy.compare(leftPrefix.backing(), leftPrefix.offset(), leftPrefix.length(),
                rightPrefix.backing(), rightPrefix.offset(), rightPrefix.length());
    }

    /**
     * 校验 {@code validate} 涉及的记录格式与页内组织结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code validate} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     */
    @Override
    public void validate(ColumnValue value, ColumnType type) {
        requireBoundType(type);
        if (value instanceof ColumnValue.ExternalValue external) {
            validateExternal(external, type);
            return;
        }
        byte[] bytes = logicalBytes(value, type);
        requireInline(bytes.length, type);
    }

    /**
     * 把待 externalize 的完整逻辑值转成精确 payload；只校验 schema 最大长度，不施加记录 inline 上限。
     *
     * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code logicalBytesForStorage} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code logicalBytesForStorage} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     * @throws InvalidColumnValueException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public byte[] logicalBytesForStorage(ColumnValue value, ColumnType type) {
        requireBoundType(type);
        if (value instanceof ColumnValue.ExternalValue) {
            throw new InvalidColumnValueException("cannot externalize an existing external LOB reference");
        }
        return logicalBytes(value, type);
    }

    /** 把从页链校验完成的完整 payload 恢复为字符串或二进制逻辑值。
     *
     * @param payload 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param type 选择 {@code logicalValueFromStorage} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code logicalValueFromStorage} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws ColumnValueOutOfRangeException 输入值或资源需求超出编码、页面或容量上限时抛出；调用方应缩小请求、回滚或等待资源释放
     */
    public ColumnValue logicalValueFromStorage(byte[] payload, ColumnType type) {
        if (payload == null) {
            throw new DatabaseValidationException("LOB payload must not be null");
        }
        requireBoundType(type);
        if (payload.length > type.length()) {
            throw new ColumnValueOutOfRangeException("LOB payload exceeds " + typeId + " max bytes: " + payload.length);
        }
        return logicalValue(new FieldSlice(payload, 0, payload.length), type);
    }

    /** 在最多 32B budget 内生成字符边界安全的 inline prefix。
     *
     * @param payload 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param type 选择 {@code inlinePrefix} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code inlinePrefix} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public byte[] inlinePrefix(byte[] payload, ColumnType type) {
        if (payload == null) {
            throw new DatabaseValidationException("LOB prefix payload must not be null");
        }
        int length = Math.min(payload.length, EXTERNAL_PREFIX_LIMIT);
        if (text) {
            characters.decode(new FieldSlice(payload, 0, payload.length), type.charset());
            while (length > 0) {
                try {
                    characters.decode(new FieldSlice(payload, 0, length), type.charset());
                    break;
                } catch (InvalidCharacterEncodingException incompleteCharacter) {
                    length--;
                }
            }
        }
        byte[] prefix = new byte[length];
        System.arraycopy(payload, 0, prefix, 0, length);
        return prefix;
    }

    private byte[] logicalBytes(ColumnValue value, ColumnType type) {
        byte[] bytes;
        if (text) {
            if (!(value instanceof ColumnValue.StringValue stringValue)) {
                throw new InvalidColumnValueException("expected StringValue for " + typeId);
            }
            if (json) {
                validateJson(stringValue.value());
            }
            bytes = characters.encode(stringValue.value(), type.charset());
        } else {
            if (!(value instanceof ColumnValue.BinaryValue binaryValue)) {
                throw new InvalidColumnValueException("expected BinaryValue for " + typeId);
            }
            bytes = binaryValue.value();
        }
        if (bytes.length > type.length()) {
            throw new ColumnValueOutOfRangeException(
                    "LOB payload exceeds " + typeId + " max bytes " + type.length() + ": " + bytes.length);
        }
        return bytes;
    }

    private ColumnValue logicalValue(FieldSlice payload, ColumnType type) {
        if (payload.length() > type.length()) {
            throw new ColumnValueOutOfRangeException("encoded LOB exceeds schema max bytes: " + payload.length());
        }
        if (!text) {
            return new ColumnValue.BinaryValue(payload.copyBytes());
        }
        String value = characters.decode(payload, type.charset());
        if (json) {
            validateJson(value);
        }
        return new ColumnValue.StringValue(value);
    }

    private void validateExternal(ColumnValue.ExternalValue external, ColumnType type) {
        if (external.typeId() != typeId) {
            throw new InvalidColumnValueException(
                    "external LOB type mismatch: expected=" + typeId + ", actual=" + external.typeId());
        }
        byte[] prefix = external.inlinePrefix();
        if (prefix.length > EXTERNAL_PREFIX_LIMIT || prefix.length > external.reference().totalLength()) {
            throw new InvalidColumnValueException("external LOB inline prefix length is invalid: " + prefix.length);
        }
        if (external.reference().totalLength() > type.length()) {
            throw new ColumnValueOutOfRangeException("external LOB length exceeds schema max: "
                    + external.reference().totalLength());
        }
        if (text && prefix.length > 0) {
            characters.decode(new FieldSlice(prefix, 0, prefix.length), type.charset());
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
     * @param type 选择 {@code decodeExternal} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code decodeExternal} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws InvalidColumnValueException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private ColumnValue.ExternalValue decodeExternal(FieldSlice slice, ColumnType type) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (slice.length() < EXTERNAL_HEADER_BYTES) {
            throw new InvalidColumnValueException("truncated external LOB envelope: " + slice.length());
        }
        LobReference reference = readReference(slice, 1);
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        int prefixLength = slice.byteAt(1 + REFERENCE_BYTES);
        if (prefixLength > EXTERNAL_PREFIX_LIMIT || slice.length() != EXTERNAL_HEADER_BYTES + prefixLength) {
            throw new InvalidColumnValueException("external LOB prefix/envelope length mismatch: " + prefixLength);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        ColumnValue.ExternalValue value = new ColumnValue.ExternalValue(typeId, reference,
                subSlice(slice, EXTERNAL_HEADER_BYTES, prefixLength).copyBytes());
        validateExternal(value, type);
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return value;
    }

    /**
     * 定位并读取记录格式与页内组织领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param envelope 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param type 选择 {@code comparablePayload} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param prefixBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return {@code comparablePayload} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws UnsupportedColumnTypeException 请求的语法形状、类型或运行模式超出当前实现范围时抛出；调用方应改写请求或选择受支持配置
     */
    private FieldSlice comparablePayload(FieldSlice envelope, ColumnType type, int prefixBytes) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        ColumnValue decoded = decode(envelope, type);
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        byte[] payload;
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        if (decoded instanceof ColumnValue.ExternalValue external) {
            payload = external.inlinePrefix();
            if (prefixBytes > payload.length && external.reference().totalLength() > payload.length) {
                throw new UnsupportedColumnTypeException("LOB prefix " + prefixBytes
                        + " exceeds inline external prefix " + payload.length);
            }
        } else {
            payload = logicalBytes(decoded, type);
        }
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return new FieldSlice(payload, 0, payload.length);
    }

    private int safePrefixLength(FieldSlice payload, ColumnType type, int prefixBytes) {
        int length = Math.min(payload.length(), prefixBytes);
        if (!text) {
            return length;
        }
        characters.decode(payload, type.charset());
        while (length > 0) {
            try {
                characters.decode(subSlice(payload, 0, length), type.charset());
                return length;
            } catch (InvalidCharacterEncodingException incompleteCharacter) {
                length--;
            }
        }
        return 0;
    }

    private void requireInline(int length, ColumnType type) {
        if (length > INLINE_PAYLOAD_LIMIT) {
            throw new LobExternalizationRequiredException(
                    type.typeId() + " payload " + length + "B exceeds inline limit " + INLINE_PAYLOAD_LIMIT);
        }
    }

    private void requireBoundType(ColumnType type) {
        if (type == null || type.typeId() != typeId) {
            throw new DatabaseValidationException("LOB codec bound type mismatch: expected=" + typeId
                    + ", actual=" + (type == null ? null : type.typeId()));
        }
    }

    private static void validateJson(String value) {
        if (value == null || value.isBlank()) {
            throw new InvalidColumnValueException("JSON text must not be blank");
        }
        try {
            StrictJsonValidator.validate(value);
        } catch (RuntimeException parseFailure) {
            throw new InvalidColumnValueException("invalid JSON text", parseFailure);
        }
    }

    /**
     * 校验输入与当前状态后修改记录格式与页内组织领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param writer 由组合根提供的 {@code FieldWriter} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code writeReference} 调用
     * @param reference LOB 数据或其稳定外部引用；不得为 {@code null}，引用身份、长度与校验信息必须匹配所属记录
     */
    private static void writeReference(FieldWriter writer, LobReference reference) {
        putInt(writer, reference.spaceId().value());
        putInt(writer, checkedU32PageNo(reference.firstPageNo()));
        putInt(writer, reference.totalLength());
        putInt(writer, reference.pageCount());
        putLong(writer, reference.segmentId().value());
        putInt(writer, reference.inodeSlot());
        putInt(writer, (int) reference.crc32());
    }

    /**
     * 根据调用参数创建或转换 {@code readReference} 返回的 {@code LobReference}；输入先完成领域校验，成功结果不为 {@code null}。
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
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @return {@code readReference} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    private static LobReference readReference(FieldSlice slice, int offset) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        int spaceId = readInt(slice, offset);
        long pageNo = readInt(slice, offset + 4) & 0xFFFF_FFFFL;
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        int totalLength = readInt(slice, offset + 8);
        int pageCount = readInt(slice, offset + 12);
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        long segmentId = readLong(slice, offset + 16);
        int inodeSlot = readInt(slice, offset + 24);
        long crc32 = readInt(slice, offset + 28) & 0xFFFF_FFFFL;
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return new LobReference(SpaceId.of(spaceId), PageNo.of(pageNo), totalLength, pageCount,
                SegmentId.of(segmentId), inodeSlot, crc32);
    }

    private static int checkedU32PageNo(PageNo pageNo) {
        if (pageNo.value() > 0xFFFF_FFFFL) {
            throw new DatabaseValidationException("LOB first page exceeds FIL u32 range: " + pageNo.value());
        }
        return (int) pageNo.value();
    }

    private static void putInt(FieldWriter writer, int value) {
        writer.putByte(value >>> 24);
        writer.putByte(value >>> 16);
        writer.putByte(value >>> 8);
        writer.putByte(value);
    }

    private static void putLong(FieldWriter writer, long value) {
        putInt(writer, (int) (value >>> 32));
        putInt(writer, (int) value);
    }

    private static int readInt(FieldSlice slice, int offset) {
        return (slice.byteAt(offset) << 24) | (slice.byteAt(offset + 1) << 16)
                | (slice.byteAt(offset + 2) << 8) | slice.byteAt(offset + 3);
    }

    private static long readLong(FieldSlice slice, int offset) {
        return ((long) readInt(slice, offset) << 32) | (readInt(slice, offset + 4) & 0xFFFF_FFFFL);
    }

    private static FieldSlice subSlice(FieldSlice source, int relativeOffset, int length) {
        if (relativeOffset < 0 || length < 0 || relativeOffset + length > source.length()) {
            throw new InvalidColumnValueException("LOB envelope slice out of bounds");
        }
        return new FieldSlice(source.backing(), source.offset() + relativeOffset, length);
    }
}
