package cn.zhangyis.db.storage.buf;

/**
 * 磁盘页在 Buffer Pool 中发布前的唯一扩展点。回调发生在 PageStore 整页读取完成之后、LOADING future 完成之前，
 * 且不持 page-hash、frame、LRU 或 flush-list 内部锁；Change Buffer 用它保证旧磁盘页先合并再对普通 fixer 可见。
 */
@FunctionalInterface
public interface PageLoadInterceptor {

    /**
     * 检查并按需修改尚未发布的页面。实现若要写页，必须通过
     * {@link PendingPagePublication#claimExclusive()} 取得受控 X guard，不能保存 publication/frame 引用越过本次回调。
     *
     * @param publication 本次 LOADING owner 的单次发布能力；不得为 {@code null}
     */
    void beforePublish(PendingPagePublication publication);
}
