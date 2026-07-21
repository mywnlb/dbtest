package cn.zhangyis.db.session.xa;

import cn.zhangyis.db.domain.XaId;
import cn.zhangyis.db.sql.executor.storage.SqlStorageGateway;
import cn.zhangyis.db.sql.executor.storage.SqlTransactionHandle;
import cn.zhangyis.db.session.exception.SessionStateException;

import java.time.Duration;
import java.util.List;

/**
 * Session 到实例级 XA coordinator 的稳定端口。实现负责 registry durability 与共享 prepared branch，
 * Session policy 负责活动 branch、metadata scope 和普通只读/one-phase 终结。
 */
public interface SessionXaCoordinator {

    /** 未配置 server XA 的显式拒绝实现，供独立 Session 单元测试与嵌入式替身使用。 */
    SessionXaCoordinator UNSUPPORTED = new SessionXaCoordinator() {
        @Override
        public void prepare(XaId xid, long transactionId, SqlStorageGateway gateway,
                            SqlTransactionHandle handle, Duration timeout) {
            throw unsupported();
        }

        @Override
        public void commitPrepared(XaId xid, Duration timeout) {
            throw unsupported();
        }

        @Override
        public void rollbackPrepared(XaId xid, Duration timeout) {
            throw unsupported();
        }

        @Override
        public List<XaRecoverEntry> recover() {
            throw unsupported();
        }

        private SessionStateException unsupported() {
            return new SessionStateException("this Session has no XA coordinator");
        }
    };

    /**
     * 执行 durable PREPARING -> storage prepare -> durable PREPARED。
     *
     * @param xid 待 prepare 分支
     * @param transactionId gateway 已确认的正 write transaction id
     * @param gateway 创建 opaque handle 的 adapter
     * @param handle 仍为 ACTIVE 的 opaque write handle
     * @param timeout 本次 phase-one durability 正等待上限
     */
    void prepare(XaId xid, long transactionId, SqlStorageGateway gateway,
                 SqlTransactionHandle handle, Duration timeout);

    /** 按 durable commit decision 完成共享 prepared branch。 */
    void commitPrepared(XaId xid, Duration timeout);

    /** 按 durable rollback decision 完成共享 prepared branch。 */
    void rollbackPrepared(XaId xid, Duration timeout);

    /** @return registry 中仍为 PREPARED 的不可变 XA RECOVER 列表 */
    List<XaRecoverEntry> recover();
}
