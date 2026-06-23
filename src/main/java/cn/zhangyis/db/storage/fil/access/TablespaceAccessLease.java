package cn.zhangyis.db.storage.fil.access;
import cn.zhangyis.db.storage.fil.lock.ResourceGuard;


/**
 * 表空间 operation-level 准入令牌。普通页访问持共享令牌，truncate/discard 持独占令牌；
 * 令牌必须由获取线程关闭，建议始终通过 try-with-resources 或 MTR memo 管理。
 */
public interface TablespaceAccessLease extends ResourceGuard {
}
