package cn.zhangyis.db.dd.recovery;

/**
 * DDL 协调器在进入副作用前检查、并在全局稳定终点发布 recovery clean snapshot 的窄端口。
 */
public interface DictionaryCleanSnapshotPublisher {

    /**
     * 在本次 DDL 分配 identity、写 marker 或修改文件前检查先前 manifest publish 是否失败。
     * 失败实现必须持续 fail-closed，直到重启由有效 catalog 重新生成 clean snapshot。
     */
    void assertAvailable();

    /**
     * catalog、物理文件、SDI 和 terminal DDL marker 全部收敛后发布新的 clean snapshot。
     */
    void publish();

    /**
     * 返回不产生 sidecar 的兼容实现，仅供低层独立 DDL 测试。
     *
     * @return 无状态线程安全 no-op publisher
     */
    static DictionaryCleanSnapshotPublisher noOp() {
        return NoOpHolder.INSTANCE;
    }

    /** 延迟持有共享 no-op 实例。 */
    final class NoOpHolder {
        private static final DictionaryCleanSnapshotPublisher INSTANCE = new DictionaryCleanSnapshotPublisher() {
            @Override
            public void assertAvailable() {
                // 兼容测试组合不维护灾难恢复 manifest fence。
            }

            @Override
            public void publish() {
                // 兼容测试组合不维护灾难恢复 manifest。
            }
        };

        private NoOpHolder() {
        }
    }
}
