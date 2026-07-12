package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TypeId;

/**
 * 前缀索引（{@code KeyPartDef.prefixBytes>0}）的比较辅助（innodb-record-design §11 prefix key）。
 *
 * <p>前缀只对**字节类型**（CHAR/VARCHAR/BINARY/VARBINARY）有意义：先把原始编码切片截到前
 * {@code prefixBytes} 字节，再交给列声明的 binary 或 byte-local ASCII-CI collation 比较。
 * 数值/时间/浮点/DECIMAL 的编码字节没有「列前缀」语义（截断会破坏保序性），对其指定 {@code prefixBytes} 属
 * schema 误用，直接拒绝——与 MySQL「前缀长度只允许 string/blob 列」一致。
 *
 * <p><b>简化点</b>：{@code prefixBytes} 以**字节**计而非字符，可能落在多字节 UTF-8 字符中间。当前两种
 * collation 都是逐字节函数，因此仍有确定顺序；未来引入 Unicode weight 时必须先重新定义字符级 prefix。
 */
public final class KeyPrefix {

    private KeyPrefix() {
    }

    /**
     * 按 key part 的 {@code prefixBytes} 截断一个**已编码**字段切片，供 codec 保序比较。
     *
     * @param encoded     列值的保序编码切片（record 侧列切片，或 key 侧临时编码）。
     * @param type        列类型，用于判定是否可前缀。
     * @param prefixBytes key part 的前缀字节数；{@code <=0} 表示整列（原样返回）。
     * @return 截到 {@code min(length, prefixBytes)} 的切片（未截断时返回原切片）。
     * @throws DatabaseValidationException 当 {@code prefixBytes>0} 但列不是字节类型时。
     */
    public static FieldSlice apply(FieldSlice encoded, ColumnType type, int prefixBytes) {
        if (encoded == null || type == null) {
            throw new DatabaseValidationException("key prefix slice/type must not be null");
        }
        if (prefixBytes <= 0) {
            return encoded;
        }
        if (!isBytePrefixable(type.typeId())) {
            throw new DatabaseValidationException(
                    "prefix index length only applies to CHAR/VARCHAR/BINARY/VARBINARY, not " + type.typeId());
        }
        int len = Math.min(encoded.length(), prefixBytes);
        if (len == encoded.length()) {
            return encoded;
        }
        return new FieldSlice(encoded.backing(), encoded.offset(), len);
    }

    private static boolean isBytePrefixable(TypeId typeId) {
        return switch (typeId) {
            case CHAR, VARCHAR, BINARY, VARBINARY -> true;
            default -> false;
        };
    }
}
