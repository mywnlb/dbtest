package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 截断排空发现目标表空间仍有脏帧；调用方必须先完成 WAL-safe flush，不能丢弃脏数据。 */
public final class DirtyTablespaceInvalidationException extends DatabaseRuntimeException {

    /**
     * 创建 {@code DirtyTablespaceInvalidationException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public DirtyTablespaceInvalidationException(String message) {
        super(message);
    }
}
