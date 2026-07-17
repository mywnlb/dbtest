package cn.zhangyis.db.storage.btree;

/**
 * 二级索引 entry 的 delete-mark 状态转换结果。该枚举区分真正写页、幂等重试和目标缺失，
 * 使 DML/rollback 能在不读取 BufferFrame 或页内游标的前提下判断物理动作是否已经完成。
 */
public enum SecondaryDeleteMarkStatus {

    /** 命中完整物理 key，且记录头 delete 位已从旧状态翻转为目标状态。 */
    CHANGED,

    /** 命中完整物理 key，但 entry 已处于目标状态；页内容未修改，可作为幂等完成证明。 */
    ALREADY_IN_STATE,

    /** 完整物理 key 不存在；页内容未修改，调用方需结合 undo action 判断是已完成还是损坏。 */
    ABSENT
}
