package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * B+Tree 模块运行时异常基类。表示索引结构、查找或写入过程中出现的可诊断领域错误；
 * 调用方可通过重试、触发 split、回滚当前语句或上报损坏继续控制流程。
 */
public class BTreeException extends DatabaseRuntimeException {

    /**
     * 创建只包含领域错误消息的 B+Tree 异常。
     *
     * @param message 描述违反的 B+Tree 结构或操作约束。
     */
    public BTreeException(String message) {
        super(message);
    }

    /**
     * 创建保留根因的 B+Tree 异常。
     *
     * @param message 描述违反的 B+Tree 结构或操作约束。
     * @param cause   触发该错误的底层异常。
     */
    public BTreeException(String message, Throwable cause) {
        super(message, cause);
    }
}
