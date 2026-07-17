package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TypeId;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.FieldSlice;
import cn.zhangyis.db.storage.record.type.FieldWriter;
import cn.zhangyis.db.storage.record.type.KeyPrefix;
import cn.zhangyis.db.storage.record.type.TypeCodec;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.io.ByteArrayOutputStream;
import java.util.Base64;

/**
 * 把 logical secondary key 归一化为事务锁 identity。编码复用 record 类型 codec、KeyPrefix 与 collation
 * equality key，保证 leaf comparator 判等与事务互斥判等一致；token 只用于进程内锁表，不进入 B+Tree 或磁盘格式。
 */
public final class SecondaryLogicalKeyLockTokenFactory {

    /** 列值编码与 collation 策略的只读注册表。 */
    private final TypeCodecRegistry registry;

    /**
     * 创建 logical-key token 工厂。
     *
     * @param registry 必须与目标 B+Tree comparator 使用同一类型 codec、charset 和 collation 配置。
     * @throws DatabaseValidationException 注册表为空时抛出，防止事务锁判等与 leaf comparator 判等分叉。
     */
    public SecondaryLogicalKeyLockTokenFactory(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("secondary lock token registry must not be null");
        }
        this.registry = registry;
    }

    /**
     * 生成 logical key 等价 token。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>核对 key part 数与 metadata layout，避免用截断/额外字段形成错误锁 identity。</li>
     *     <li>逐 part 用声明 ColumnType 编码；NULL 写独立标记，非 NULL 先做完整类型校验。</li>
     *     <li>按 KeyPartDef.prefixBytes 截断；字符列再转为 collation equality key，DESC 不改变相等关系。</li>
     *     <li>对每段写长度 framing 并 Base64，防止不同 part 边界拼接碰撞；全程无锁、IO 或 redo 副作用。</li>
     * </ol>
     *
     * @param metadata   目标二级索引 exact-version metadata，提供 logical part、类型、prefix 与 collation。
     * @param logicalKey 仅含声明二级 part 的完整物化 key；不得附带聚簇主键后缀。
     * @return 可作为 {@link cn.zhangyis.db.storage.trx.lock.SecondaryUniqueKeyLockKey} identity 的稳定 Base64 token。
     * @throws DatabaseValidationException metadata/key 缺失、part 数错配或列值不符合声明类型时抛出。
     */
    public String create(SecondaryIndexMetadata metadata, SearchKey logicalKey) {
        // 1. metadata 与 key 形状必须完整一致。
        if (metadata == null || logicalKey == null
                || logicalKey.size() != metadata.layout().logicalKeyPartCount()) {
            throw new DatabaseValidationException("secondary lock token metadata/key shape mismatch");
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (int i = 0; i < logicalKey.size(); i++) {
            KeyPartDef part = metadata.index().keyDef().parts().get(i);
            ColumnType type = metadata.index().schema().column(part.columnId().value()).type();
            ColumnValue value = logicalKey.value(i);

            // 2. NULL marker 与非 NULL payload 分开 framing；logical unique 调用方通常会对 NULL 直接跳过冲突检查。
            if (value instanceof ColumnValue.NullValue) {
                output.write(1);
                writeLength(output, 0);
                continue;
            }
            output.write(0);
            TypeCodec codec = registry.codecFor(type);
            codec.validate(value, type);
            byte[] encoded = new byte[codec.encodedLength(value, type)];
            codec.encode(value, type, new FieldWriter(encoded, 0));

            // 3. 与 leaf comparator 相同地先应用 prefix；字符 collation 再生成 compare==0 等价类的稳定 key。
            FieldSlice prefixed = KeyPrefix.apply(new FieldSlice(encoded, 0, encoded.length),
                    type, part.prefixBytes());
            byte[] equality = isCharacter(type.typeId())
                    ? registry.collationFor(type.charset(), type.collation()).equalityKey(
                            prefixed.backing(), prefixed.offset(), prefixed.length())
                    : java.util.Arrays.copyOfRange(prefixed.backing(), prefixed.offset(),
                            prefixed.offset() + prefixed.length());

            // 4. u32 长度消除多 part 拼接歧义，最终字符串只作为 LockManager 不透明 key。
            writeLength(output, equality.length);
            output.writeBytes(equality);
        }
        return Base64.getEncoder().encodeToString(output.toByteArray());
    }

    /**
     * 判断类型是否需要通过 collation equality key 归一化。
     *
     * @param typeId key part 的稳定类型 id。
     * @return CHAR/VARCHAR 返回 {@code true}；数值、二进制及其它类型返回 {@code false}。
     */
    private static boolean isCharacter(TypeId typeId) {
        return typeId == TypeId.CHAR || typeId == TypeId.VARCHAR;
    }

    /**
     * 写入一个 part 的 big-endian u32 长度 framing，防止多字段字节拼接产生边界碰撞。
     *
     * @param output token 的进程内字节输出；方法不关闭该对象。
     * @param length 当前 equality payload 的非负数组长度；Java 数组保证不超过 signed int。
     */
    private static void writeLength(ByteArrayOutputStream output, int length) {
        output.write((length >>> 24) & 0xFF);
        output.write((length >>> 16) & 0xFF);
        output.write((length >>> 8) & 0xFF);
        output.write(length & 0xFF);
    }
}
