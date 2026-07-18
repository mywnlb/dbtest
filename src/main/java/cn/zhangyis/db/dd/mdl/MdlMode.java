package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * MySQL 风格六种 metadata lock mode。COMPATIBILITY 是唯一授锁矩阵，行/列顺序与 enum 声明严格一致。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code INTENTION_EXCLUSIVE}：表示“INTENTIONEXCLUSIVE”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code SHARED_READ}：表示“SHARED读取”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code SHARED_WRITE}：表示“SHARED写入”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code SHARED_UPGRADABLE}：表示“SHAREDUPGRADABLE”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code SHARED_NO_WRITE}：表示“SHAREDNO写入”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code EXCLUSIVE}：表示“EXCLUSIVE”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 * </ul>
 */
public enum MdlMode {
    INTENTION_EXCLUSIVE,
    SHARED_READ,
    SHARED_WRITE,
    SHARED_UPGRADABLE,
    SHARED_NO_WRITE,
    EXCLUSIVE;

    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏数据字典的不变量。
     */
    private static final boolean[][] COMPATIBILITY = {
            {true, true, true, true, true, false},
            {true, true, true, true, true, false},
            {true, true, true, true, false, false},
            {true, true, true, false, false, false},
            {true, true, false, false, true, false},
            {false, false, false, false, false, false}
    };

    /** requested mode 与已授予 mode 是否能并存；矩阵对称。
     *
     * @param granted 选择 {@code compatibleWith} 分支的 {@code MdlMode} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code compatibleWith} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public boolean compatibleWith(MdlMode granted) {
        if (granted == null) {
            throw new DatabaseValidationException("granted metadata lock mode must not be null");
        }
        return COMPATIBILITY[ordinal()][granted.ordinal()];
    }
}
