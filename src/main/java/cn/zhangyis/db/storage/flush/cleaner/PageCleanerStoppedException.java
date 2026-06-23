package cn.zhangyis.db.storage.flush.cleaner;

import cn.zhangyis.db.storage.flush.FlushWriteException;

/**
 * page cleaner 已停止或失败后仍提交请求的异常。调用方应重新启动新的 worker 或改走前台 flush。
 */
public class PageCleanerStoppedException extends FlushWriteException {

    public PageCleanerStoppedException(String message) {
        super(message);
    }
}
