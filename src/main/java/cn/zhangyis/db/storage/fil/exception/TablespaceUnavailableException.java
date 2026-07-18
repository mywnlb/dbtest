package cn.zhangyis.db.storage.fil.exception;
import cn.zhangyis.db.storage.fil.io.PageStore;


import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 表空间存在但当前生命周期状态不允许普通 IO 的异常。
 *
 * <p>{@code CachingTablespaceRegistry} 对 {@code EMPTY}、{@code INACTIVE}、{@code TRUNCATING}
 * 等非服务状态抛出该异常，阻止普通 {@link PageStore} 路径读取未初始化页或与 undo truncate/recovery
 * 交叉。它与“registry 中不存在/已丢弃”的 {@link TablespaceNotFoundException}、已确认损坏的
 * {@link TablespaceCorruptedException} 有意区分。</p>
 *
 * <p>该错误可以随生命周期编排推进而消失，但普通调用方不得自行改状态或绕过 registry；
 * 只有状态由权威流程恢复为 {@code NORMAL}/{@code ACTIVE} 后，原操作才可以重试。</p>
 */
public class TablespaceUnavailableException extends DatabaseRuntimeException {

    /**
     * 创建表空间不可用异常。
     *
     * @param message 应包含 SpaceId 与拒绝普通 IO 的当前生命周期状态
     */
    public TablespaceUnavailableException(String message) {
        super(message);
    }

    /**
     * 创建保留根因的表空间不可用异常。
     *
     * @param message 应包含 SpaceId、生命周期状态及判定阶段
     * @param cause 触发不可用判定的原始原因；包装时不得丢弃
     */
    public TablespaceUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
