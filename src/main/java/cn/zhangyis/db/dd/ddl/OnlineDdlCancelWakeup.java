package cn.zhangyis.db.dd.ddl;

/**
 * durable cancel 裁决完成后的轻量唤醒端口。实现只能取消尚未授予的等待，不得释放
 * coordinator 已持有的 MDL ticket，也不得在回调中进入 catalog/control CAS。
 */
@FunctionalInterface
public interface OnlineDdlCancelWakeup {

    /** 隔离测试与旧组合构造器使用的无副作用端口。 */
    OnlineDdlCancelWakeup NO_OP = identity -> {
    };

    /**
     * 在取消 marker 已经 force 且 repository writer fence 已释放后唤醒 live coordinator。
     *
     * @param identity registry 冻结的 live operation 身份；recovery/durable-only marker 没有 live owner 时不调用
     */
    void wake(OnlineDdlOperationIdentity identity);
}
