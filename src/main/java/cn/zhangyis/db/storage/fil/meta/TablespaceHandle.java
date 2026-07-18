package cn.zhangyis.db.storage.fil.meta;
import cn.zhangyis.db.storage.fil.io.PageStore;


import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * {@link TablespaceRegistry} 返回的运行时逻辑句柄。
 *
 * <p>它只包装一次不可变 {@link Tablespace} 快照，不持有 {@link PageStore} 文件句柄、access lease、
 * buffer fix 或引用计数，关闭/丢弃该 Java 值不会改变物理文件生命周期。registry 的 replace/mark/refresh
 * 会发布新的 handle，已经返回给调用方的旧 handle 不会原地更新；需要执行普通 IO 的路径必须在相应
 * access lease 内重新 {@code require}，不能把长期保存的 handle 当作最新状态。</p>
 *
 * @param tablespace 构造时取得的非空运行时 metadata 快照；其 version/state 只代表该 handle 的观察代次
 */
public record TablespaceHandle(Tablespace tablespace) {

    /**
     * 创建不拥有物理资源的逻辑句柄。
     *
     * @throws DatabaseValidationException tablespace 为空时抛出，避免 registry 发布无状态 handle
     */
    public TablespaceHandle {
        if (tablespace == null) {
            throw new DatabaseValidationException("tablespace handle snapshot must not be null");
        }
    }
}
