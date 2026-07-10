package cn.zhangyis.db.storage.recovery;

import java.util.Optional;

/**
 * crash recovery 读取事务 counter baseline 的窄端口。NORMAL 可由可写 store 实现；READ_ONLY_VALIDATE
 * 使用只读 store 或 empty source，避免为了诊断缺失文件而创建 sidecar。
 */
@FunctionalInterface
public interface TransactionRecoveryCheckpointSource {

    /** 读取最新有效基线；文件缺失/无有效 slot 返回 empty。 */
    Optional<TransactionRecoveryCheckpoint> readLatest();

    /** 表达文件不存在的只读 source；不执行任何文件系统写入。 */
    static TransactionRecoveryCheckpointSource empty() {
        return Optional::empty;
    }
}
