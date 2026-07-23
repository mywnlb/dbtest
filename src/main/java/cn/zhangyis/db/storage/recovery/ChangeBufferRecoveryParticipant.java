package cn.zhangyis.db.storage.recovery;

/**
 * redo replay 后、undo rollback 前校验 Change Buffer 持久证据的恢复参与者。
 * 实现只能读取/验证 system.ibd 与全局树，不启动后台线程、不开放流量，也不执行普通 SQL 语义。
 */
@FunctionalInterface
public interface ChangeBufferRecoveryParticipant {

    /**
     * 验证 header、root/segment、全局 record 顺序与 pending 计数。任何不一致必须抛领域异常并保持恢复 fail-closed。
     */
    void validateAfterRedo();
}
