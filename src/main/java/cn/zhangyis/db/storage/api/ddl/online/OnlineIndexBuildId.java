package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Online index build 的 storage 层稳定身份。该值在 DD 边界由正 {@code DdlId} 显式转换，避免 storage
 * 反向依赖 DD 包，也避免普通 long 在多日志 force 排序中混入 table/index identity。
 *
 * @param value 跨 manifest、DDL marker、page3 descriptor 和 row-log 文件保持不变的正 identity
 */
public record OnlineIndexBuildId(long value) implements Comparable<OnlineIndexBuildId> {

    /**
     * 校验持久身份；本类型不提供 NONE，尚未分配必须用 Optional 表达。
     *
     * @throws DatabaseValidationException value 非正时抛出
     */
    public OnlineIndexBuildId {
        if (value <= 0) {
            throw new DatabaseValidationException("online index build id must be positive: " + value);
        }
    }

    /** @param value DD control 已预留的正 ddl identity；@return 对应 storage build identity。 */
    public static OnlineIndexBuildId of(long value) {
        return new OnlineIndexBuildId(value);
    }

    /** 多 row-log force 固定按 identity 升序取得文件锁。 */
    @Override
    public int compareTo(OnlineIndexBuildId other) {
        if (other == null) {
            throw new DatabaseValidationException("online index build compare target must not be null");
        }
        return Long.compare(value, other.value);
    }
}
