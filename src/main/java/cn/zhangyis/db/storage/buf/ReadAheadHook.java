package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageId;

/**
 * Read-ahead 访问钩子：Buffer Pool 在 {@code getPage} 命中/未命中后回调它上报一次页访问，使 read-ahead 能在前台线程
 * 之外检测顺序访问并调度预取。
 *
 * <p>实现必须**廉价且绝不抛异常**（它在 demand read 热路径上被调用）：典型实现只喂检测器、必要时入队一个预取请求，
 * 不在调用线程做盘 IO。Buffer Pool 经 {@code attachReadAheadHook} 注入它，从而与具体 read-ahead 实现解耦（无环依赖）。
 */
@FunctionalInterface
public interface ReadAheadHook {

    /**
     * 上报一次页访问。
     *
     * @param pageId 被 {@code getPage} 访问的页。
     */
    void recordAccess(PageId pageId);
}
