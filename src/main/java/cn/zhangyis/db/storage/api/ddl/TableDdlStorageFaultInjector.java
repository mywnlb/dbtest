package cn.zhangyis.db.storage.api.ddl;

/**
 * 物理 DDL 持久边界的测试故障注入端口。生产入口始终使用 {@link #NO_OP}；该端口不改变
 * DDL 阶段顺序，只用于证明 durable marker 之后的崩溃可续作。
 */
@FunctionalInterface
interface TableDdlStorageFaultInjector {

    /** 生产空实现。 */
    TableDdlStorageFaultInjector NO_OP = binding -> { };

    /** DISCARDED marker 的 redo 和 page0 已 force，但 frame 尚未 invalidate、文件尚未删除。 */
    void afterDiscardedDurable(TableStorageBinding binding);
}
