package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;

/**
 * 单个已编码索引 key part 的权威排序器。Record 与 B+Tree 的两个复合 key 比较器必须共同委托本类，防止
 * NULL、prefix、collation 或 DESC 中任一规则在 leaf record 与 node pointer 路径发生漂移。
 */
public final class EncodedKeyPartComparator {

    /** 类型与字符策略入口；只读且由上层比较器共享。 */
    private final TypeCodecRegistry registry;

    /**
     * 创建 {@code EncodedKeyPartComparator}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param registry 由组合根提供的 {@code TypeCodecRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public EncodedKeyPartComparator(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("type codec registry must not be null");
        }
        this.registry = registry;
    }

    /**
     * 比较一个 key part，并把自然序转换为索引声明的 ASC/DESC 序。
     *
     * <p>数据流：先通过 {@link TypeCodecRegistry#codecFor(ColumnType)} 验证类型及字符 pair；再处理 NULL
     * （ASC 中 NULL 在前）；非 NULL 两侧先按相同 byte-prefix 截断，再由类型 codec/collation 比较；最后把结果
     * 规范化为 -1/0/1 并按 DESC 反转。这样策略返回值的 magnitude 不会泄漏到复合比较器。
     *
     * @param leftNull 左侧是否为 SQL NULL。
     * @param left 左侧原始编码切片；leftNull=true 时必须为 null。
     * @param rightNull 右侧是否为 SQL NULL。
     * @param right 右侧原始编码切片；rightNull=true 时必须为 null。
     * @param type key part 列类型。
     * @param part key part 的列、方向与 byte-prefix 定义。
     * @return 仅为 -1、0、1 的索引序比较结果。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public int compare(boolean leftNull, FieldSlice left, boolean rightNull, FieldSlice right,
                       ColumnType type, KeyPartDef part) {
        if (type == null || part == null) {
            throw new DatabaseValidationException("key part type/definition must not be null");
        }
        validateNullSlice(leftNull, left, "left");
        validateNullSlice(rightNull, right, "right");

        TypeCodec codec = registry.codecFor(type);
        int naturalOrder;
        if (leftNull && rightNull) {
            naturalOrder = 0;
        } else if (leftNull || rightNull) {
            naturalOrder = leftNull ? -1 : 1;
        } else {
            naturalOrder = Integer.signum(codec.compareKeyPart(left, right, type, part.prefixBytes()));
        }
        return part.order() == KeyOrder.DESC ? -naturalOrder : naturalOrder;
    }

    /** NULL bitmap 状态与物理 slice 必须一致，避免调用方把空字符串误当 NULL。 */
    private static void validateNullSlice(boolean isNull, FieldSlice slice, String side) {
        if (isNull != (slice == null)) {
            throw new DatabaseValidationException(
                    side + " key part null flag/slice mismatch: null=" + isNull);
        }
    }
}
