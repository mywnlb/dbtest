package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;

/**
 * 类型 codec 入口（innodb-record-design §14.3）。按 {@link ColumnType} 的 typeId/width/p/s/n 参数化返回 codec。
 * 只读、线程安全（codec 无状态、按需创建）。未知类型 → {@link UnsupportedColumnTypeException}。
 */
public final class TypeCodecRegistry {

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
            case CHAR -> new FixedBytesCodec(type.length(), (byte) 0x20, true);
            case BINARY -> new FixedBytesCodec(type.length(), (byte) 0x00, false);
            case VARCHAR -> new VarBytesCodec(type.length(), true);
            case VARBINARY -> new VarBytesCodec(type.length(), false);
            case DATE -> new TemporalCodec(TemporalKind.DATE);
            case DATETIME -> new TemporalCodec(TemporalKind.DATETIME);
        };
    }

    /** 比较两个已编码切片（按列类型 codec）。 */
    public int compare(FieldSlice left, FieldSlice right, ColumnType type) {
        return codecFor(type).compare(left, right, type);
    }

    /** 校验值与类型相容。 */
    public void validate(ColumnValue value, ColumnType type) {
        codecFor(type).validate(value, type);
    }
}
