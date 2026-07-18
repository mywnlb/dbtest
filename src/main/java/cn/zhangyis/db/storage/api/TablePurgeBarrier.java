package cn.zhangyis.db.storage.api;

import java.time.Duration;

/**
 * 数据字典等待 persistent history 不再引用目标表的稳定存储 API。DD 只传稳定 table id 与有界 timeout，
 * 不读取 rollback segment、undo page 或运行时 history 队列内部状态。
 */
public interface TablePurgeBarrier {

    /**
     * 等待目标表的 committed UPDATE history 引用归零。
     *
     * @param tableId 稳定表 id，必须为正数。
     * @param timeout 最大等待时间，必须为正数；等待期间不持有 page latch、MTR 或文件锁。
     * @throws TablePurgeBarrierTimeoutException timeout 内引用未归零时抛出，调用方不得发布 DROP_PENDING。
     */
    void awaitUnreferenced(long tableId, Duration timeout);

    /**
     * 返回目标表当前被多少条 committed history entry 引用，仅用于诊断和测试。
     *
     * @param tableId 稳定表 id。
     * @return 非负引用数；同一 history entry 即使含该表多条 undo record 也只计一次。
     */
    int referenceCount(long tableId);

    /**
     * 不维护 persistent history 的低层 DDL 测试兼容实现。生产 DatabaseEngine 必须注入 StorageEngine 的真实 barrier。
     */
    TablePurgeBarrier NONE = new TablePurgeBarrier() {
        /**
         * 按存储引擎稳定 API并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
         *
         * @param tableId 目标表的原始字典标识；必须为已分配的正数并与当前元数据和物理绑定一致
         * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
         */
        @Override
        public void awaitUnreferenced(long tableId, Duration timeout) {
            // 兼容层没有 history owner；参数校验由真实实现和 DDL 入口负责。
        }

        /**
         * 计算 {@code referenceCount} 所表达的存储引擎稳定 API数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
         *
         * @param tableId 目标表的原始字典标识；必须为已分配的正数并与当前元数据和物理绑定一致
         * @return {@code referenceCount} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
         */
        @Override
        public int referenceCount(long tableId) {
            return 0;
        }
    };
}
