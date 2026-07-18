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

    /**
     * 创建 {@code MtrLatchOrderScope}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param owner 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param reason 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     */
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
