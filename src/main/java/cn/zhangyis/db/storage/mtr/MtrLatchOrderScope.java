package cn.zhangyis.db.storage.mtr;

/**
 * MTR page latch 顺序例外作用域。默认 MTR 要求独立多页按 PageId 升序获取；B+Tree sibling/FIL 右邻等已有
 * 局部死锁证明的路径，可用该 guard 在很小范围内显式放行逆序获取。
 *
 * <p>该对象由 {@link MiniTransaction#allowOutOfOrderPageLatch(String)} 创建，必须用 try-with-resources 关闭。
 * 关闭是幂等的，便于异常路径安全收尾；作用域不拥有任何 page guard，只控制 MTR 的顺序检查深度。
 */
public final class MtrLatchOrderScope implements AutoCloseable {

    /** 拥有该作用域的 MTR。 */
    private final MiniTransaction owner;

    /** 诊断说明，描述调用方依赖的局部无环证明。 */
    private final String reason;

    /** 幂等关闭标志。 */
    private boolean closed;

    MtrLatchOrderScope(MiniTransaction owner, String reason) {
        this.owner = owner;
        this.reason = reason;
    }

    /** 本作用域的诊断说明。 */
    public String reason() {
        return reason;
    }

    /** 关闭作用域，恢复外层 MTR 的默认 page latch 顺序检查。 */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        owner.closeOutOfOrderPageLatchScope(reason);
    }
}
