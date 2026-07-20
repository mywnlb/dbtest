package cn.zhangyis.db.engine.recovery;

/**
 * 管理员提交的 complete-scan token 已不再描述当前目录/manifest/conflict 状态时抛出。
 */
public final class CatalogRecoveryStaleTokenException extends CatalogRecoveryException {

    /**
     * @param message 说明 token 与重扫结果不一致的诊断文本
     */
    public CatalogRecoveryStaleTokenException(String message) {
        super(message);
    }
}
