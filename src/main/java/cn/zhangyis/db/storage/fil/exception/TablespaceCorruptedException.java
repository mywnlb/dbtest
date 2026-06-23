package cn.zhangyis.db.storage.fil.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 表空间损坏异常。普通 IO 路径遇到该异常后不能继续读写该表空间，必须交由恢复或人工处理流程接管。
 *
 * <p>分类为可恢复运行时异常而非致命异常：单个 file-per-table/general 表空间损坏并不破坏整个实例继续运行的
 * 安全性（InnoDB 也是标记该表空间 corrupt 后继续服务其它表空间），调用方可报告错误、回滚或跳过该表空间。
 * 若损坏的是系统/undo 等实例级关键表空间需要终止启动，由恢复/启动编排层判断后升级为致命错误，
 * 不在本异常类型上写死“致命”语义。
 */
public class TablespaceCorruptedException extends DatabaseRuntimeException {

    /**
     * 创建表空间损坏异常。
     *
     * @param message 表空间损坏的领域描述。
     */
    public TablespaceCorruptedException(String message) {
        super(message);
    }

    /**
     * 创建保留根因的表空间损坏异常。
     *
     * @param message 表空间损坏的领域描述。
     * @param cause 底层校验失败或 IO 失败原因。
     */
    public TablespaceCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
