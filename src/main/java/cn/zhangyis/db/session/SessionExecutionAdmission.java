package cn.zhangyis.db.session;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

import java.time.Duration;

/**
 * Session 进入一条用户语句前使用的组合根流量门。实现必须允许不同 Session 并行执行，同时让 Engine shutdown 取得
 * 独占 quiescence；permit 生命周期覆盖 parser/binder/executor/transaction cleanup 全链，关闭前不能释放。
 */
public interface SessionExecutionAdmission {

    /**
     * 在有界时间内准入一条语句。返回后调用方拥有一次活动执行计数，必须在同线程 finally 关闭 permit。
     *
     * @param sessionId 参与 {@code enter} 的稳定领域标识 {@code SessionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @return {@code enter} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    Permit enter(SessionId sessionId, Duration timeout);

    /** 把已破坏同进程继续运行安全性的 fatal 发布给组合根；普通测试/独立 Session 可使用 no-op 实现。
     *
     * @param failure 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    void failClosed(DatabaseFatalException failure);

    /** 不受 Engine 生命周期管理的独立 Session 测试实现。 */
    static SessionExecutionAdmission unrestricted() {
        return UnrestrictedAdmission.INSTANCE;
    }

    /** 活动语句的 RAII 读许可；close 必须无异常且幂等。 */
    interface Permit extends AutoCloseable {
        /**
         * 释放本方法拥有的会话与事务边界资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
         */
        @Override
        void close();
    }

    /** 仅供不经过 DatabaseEngine 的单元测试和显式独立组装使用。
     *
     * <p>未单独声明 Javadoc 的枚举值语义：</p>
     * <ul>
     *     <li>{@code INSTANCE}：无可变状态的单例值，避免为相同空值语义重复分配对象</li>
     * </ul>
     */
    enum UnrestrictedAdmission implements SessionExecutionAdmission {
        INSTANCE;

        /**
         * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏会话与事务边界的不变量。
         */
        private static final Permit PERMIT = () -> { };

        /**
         * 推进 {@code enter} 对应的会话与事务边界阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         *
         * @param sessionId 参与 {@code enter} 的稳定领域标识 {@code SessionId}；不得为 {@code null}，并须由对应值对象构造校验产生
         * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
         * @return {@code enter} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
         */
        @Override
        public Permit enter(SessionId sessionId, Duration timeout) {
            return PERMIT;
        }

        /**
         * 释放本方法拥有的会话与事务边界资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
         *
         * @param failure 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
         */
        @Override
        public void failClosed(DatabaseFatalException failure) {
            // 独立组装没有更上层组合根；Session 自身仍会进入 FAILED。
        }
    }
}
