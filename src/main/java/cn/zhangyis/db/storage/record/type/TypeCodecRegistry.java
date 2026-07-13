package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.CharsetId;
import cn.zhangyis.db.storage.record.schema.CollationId;
import cn.zhangyis.db.storage.record.schema.ColumnType;

/**
 * 类型 codec 入口（innodb-record-design §14.3）。按 {@link ColumnType} 的 typeId/width/p/s/n 参数化返回 codec，
 * 字符类型同时绑定稳定 charset/collation pair。只读、线程安全（codec 无状态、按需创建）。
 */
public final class TypeCodecRegistry {

    /** 生产与测试共享的只读字符语义；构造后不允许替换，避免索引生命周期中排序规则漂移。 */
    private final CharacterTypeRegistry characters;

    /** 使用默认稳定 charset/collation 注册创建类型入口。 */
    public TypeCodecRegistry() {
        this.characters = CharacterTypeRegistry.defaults();
    }

    /** 按列类型返回 codec。 */
    public TypeCodec codecFor(ColumnType type) {
        if (type == null) {
            throw new DatabaseValidationException("column type must not be null");
        }
        return switch (type.typeId()) {
            case TINYINT -> new IntegerCodec(1, type.unsigned());
            case SMALLINT -> new IntegerCodec(2, type.unsigned());
            case INT -> new IntegerCodec(4, type.unsigned());
            case BIGINT -> new IntegerCodec(8, type.unsigned());
            case FLOAT -> new FloatingCodec(4);
            case DOUBLE -> new FloatingCodec(8);
            case DECIMAL -> new DecimalCodec(type.length(), type.scale());
            case CHAR -> {
                characters.collationFor(type.charset(), type.collation());
                yield new FixedBytesCodec(type.length(), (byte) 0x20, true, characters);
            }
            case BINARY -> new FixedBytesCodec(type.length(), (byte) 0x00, false);
            case VARCHAR -> {
                characters.collationFor(type.charset(), type.collation());
                yield new VarBytesCodec(type.length(), true, characters);
            }
            case VARBINARY -> new VarBytesCodec(type.length(), false);
            case DATE -> new TemporalCodec(TemporalKind.DATE);
            case TIME -> new TemporalCodec(TemporalKind.TIME);
            case DATETIME -> new TemporalCodec(TemporalKind.DATETIME);
            case TIMESTAMP -> new TemporalCodec(TemporalKind.TIMESTAMP);
            case YEAR -> new TemporalCodec(TemporalKind.YEAR);
            case BIT -> new BitCodec(type.length());
            case ENUM -> new EnumCodec(type.symbols().size());
            case SET -> new SetCodec(type.symbols().size());
            case TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT -> new LobCodec(
                    type.typeId(), true, false, characters);
            case TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB -> new LobCodec(
                    type.typeId(), false, false, characters);
            case JSON -> new LobCodec(type.typeId(), true, true, characters);
        };
    }

    /** 比较两个已编码切片（按列类型 codec）。 */
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        return codecFor(type).compare(left, right, type);
    }

    /**
     * 返回精确 charset/collation pair 的比较策略；缺失 pair 禁止回退。
     */
    public CollationStrategy collationFor(CharsetId charsetId, CollationId collationId) {
        return characters.collationFor(charsetId, collationId);
    }

    /** 校验值与类型相容。 */
    public void validate(ColumnValue value, ColumnType type) {
        codecFor(type).validate(value, type);
    }
}
