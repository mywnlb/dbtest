package cn.zhangyis.db.storage.flush.cleaner;

import cn.zhangyis.db.storage.flush.FlushWriteException;

/**
 * page cleaner 已停止或失败后仍提交请求的异常。调用方应重新启动新的 worker 或改走前台 flush。
 */
public class PageCleanerStoppedException extends FlushWriteException {

    /**
     * 创建 {@code PageCleanerStoppedException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public PageCleanerStoppedException(String message) {
        super(message);
    }
}
