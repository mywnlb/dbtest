package cn.zhangyis.db.sql.executor.row;

import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.sql.executor.exception.SqlExecutionException;
import cn.zhangyis.db.sql.type.SqlValue;

import java.nio.charset.StandardCharsets;

/**
 * 对已经由 Binder 转换到同一 exact DD 类型的两个非 NULL SQL 值执行确定性比较。
 *
 * <p>字符比较复现项目当前四个稳定 collation id 的主要规则，不使用默认 locale；
 * 其它类型按其 canonical SQL 值比较。NULL 的方向相关位置由 SortNode 在调用前处理。</p>
 */
public final class SqlValueOrder {
    private SqlValueOrder() {
    }

    /**
     * 比较同一列的两个非 NULL typed value。
     *
     * @param left 左值；必须与 type 对应且不能是 SQL NULL
     * @param right 右值；必须与 left/type 属于同一 SQL variant
     * @param type exact result column type，字符比较读取其中稳定 collation id
     * @return 左值小于、等于或大于右值时的负数、零或正数
     * @throws SqlExecutionException 值类型不一致或出现不支持的 collation id 时抛出
     */
    public static int compare(
            SqlValue left, SqlValue right, ColumnTypeDefinition type) {
        if (left == null || right == null || type == null
                || left instanceof SqlValue.NullValue
                || right instanceof SqlValue.NullValue) {
            throw new SqlExecutionException(
                    "typed SQL comparison requires non-null values and type");
        }
        if (left instanceof SqlValue.IntegerValue a
                && right instanceof SqlValue.IntegerValue b) {
            return a.value().compareTo(b.value());
        }
        if (left instanceof SqlValue.FloatingValue a
                && right instanceof SqlValue.FloatingValue b) {
            return Double.compare(a.value(), b.value());
        }
        if (left instanceof SqlValue.DecimalValue a
                && right instanceof SqlValue.DecimalValue b) {
            return a.value().compareTo(b.value());
        }
        if (left instanceof SqlValue.StringValue a
                && right instanceof SqlValue.StringValue b) {
            return compareText(a.value(), b.value(), type.collationId());
        }
        if (left instanceof SqlValue.BytesValue a
                && right instanceof SqlValue.BytesValue b) {
            return compareUnsigned(a.value(), b.value());
        }
        if (left instanceof SqlValue.TemporalValue a
                && right instanceof SqlValue.TemporalValue b
                && a.kind() == b.kind()) {
            return Long.compare(a.value(), b.value());
        }
        if (left instanceof SqlValue.BitValue a
                && right instanceof SqlValue.BitValue b) {
            int compared = compareUnsigned(a.bytes(), b.bytes());
            return compared != 0 ? compared
                    : Integer.compare(a.bitWidth(), b.bitWidth());
        }
        if (left instanceof SqlValue.EnumValue a
                && right instanceof SqlValue.EnumValue b) {
            return Integer.compare(a.ordinal(), b.ordinal());
        }
        if (left instanceof SqlValue.SetValue a
                && right instanceof SqlValue.SetValue b) {
            return Long.compareUnsigned(a.bitmap(), b.bitmap());
        }
        throw new SqlExecutionException(
                "SQL sort values do not share one supported exact type: "
                        + left.getClass().getSimpleName() + "/"
                        + right.getClass().getSimpleName());
    }

    /**
     * 按项目稳定 collation id 比较字符值。
     */
    private static int compareText(String left, String right, int collationId) {
        byte[] a = left.getBytes(StandardCharsets.UTF_8);
        byte[] b = right.getBytes(StandardCharsets.UTF_8);
        return switch (collationId) {
            case 0, 1 -> compareUnsigned(a, b);
            case 2, 3 -> compareAsciiFold(a, b);
            case 4 -> compareUnicodeWeights(left, right);
            default -> throw new SqlExecutionException(
                    "SQL sort received unknown collation id: " + collationId);
        };
    }

    private static int compareUnsigned(byte[] left, byte[] right) {
        int common = Math.min(left.length, right.length);
        for (int index = 0; index < common; index++) {
            int compared = Integer.compare(
                    left[index] & 0xFF, right[index] & 0xFF);
            if (compared != 0) {
                return compared;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static int compareAsciiFold(byte[] left, byte[] right) {
        int common = Math.min(left.length, right.length);
        for (int index = 0; index < common; index++) {
            int compared = Integer.compare(
                    asciiFold(left[index] & 0xFF),
                    asciiFold(right[index] & 0xFF));
            if (compared != 0) {
                return compared;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static int asciiFold(int value) {
        return value >= 'A' && value <= 'Z'
                ? value + ('a' - 'A') : value;
    }

    private static int compareUnicodeWeights(String left, String right) {
        int[] a = left.codePoints().map(SqlValueOrder::unicodeWeight)
                .filter(weight -> weight >= 0).toArray();
        int[] b = right.codePoints().map(SqlValueOrder::unicodeWeight)
                .filter(weight -> weight >= 0).toArray();
        int common = Math.min(a.length, b.length);
        for (int index = 0; index < common; index++) {
            int compared = Integer.compare(a[index], b[index]);
            if (compared != 0) {
                return compared;
            }
        }
        return Integer.compare(a.length, b.length);
    }

    /**
     * 与 storage UnicodeWeightCollationV1 相同的冻结主权重子集；未来变更必须新增 collation id。
     */
    private static int unicodeWeight(int codePoint) {
        if (codePoint >= 0x0300 && codePoint <= 0x036F) return -1;
        if (codePoint >= 'A' && codePoint <= 'Z') return codePoint + 32;
        if ((codePoint >= 0x0391 && codePoint <= 0x03A1)
                || (codePoint >= 0x03A3 && codePoint <= 0x03AB)) return codePoint + 0x20;
        if (codePoint == 0x03C2) return 0x03C3;
        if (codePoint >= 0x0410 && codePoint <= 0x042F) return codePoint + 0x20;
        if (codePoint == 0x0401) return 0x0451;
        return switch (codePoint) {
            case 0x00C0, 0x00C1, 0x00C2, 0x00C3, 0x00C4, 0x00C5,
                    0x00E0, 0x00E1, 0x00E2, 0x00E3, 0x00E4, 0x00E5 -> 'a';
            case 0x00C7, 0x00E7 -> 'c';
            case 0x00C8, 0x00C9, 0x00CA, 0x00CB,
                    0x00E8, 0x00E9, 0x00EA, 0x00EB -> 'e';
            case 0x00CC, 0x00CD, 0x00CE, 0x00CF,
                    0x00EC, 0x00ED, 0x00EE, 0x00EF -> 'i';
            case 0x00D1, 0x00F1 -> 'n';
            case 0x00D2, 0x00D3, 0x00D4, 0x00D5, 0x00D6, 0x00D8,
                    0x00F2, 0x00F3, 0x00F4, 0x00F5, 0x00F6, 0x00F8 -> 'o';
            case 0x00D9, 0x00DA, 0x00DB, 0x00DC,
                    0x00F9, 0x00FA, 0x00FB, 0x00FC -> 'u';
            case 0x00DD, 0x00FD, 0x00FF -> 'y';
            default -> codePoint;
        };
    }
}
