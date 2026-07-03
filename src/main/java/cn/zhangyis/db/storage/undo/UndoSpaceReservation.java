package cn.zhangyis.db.storage.undo;

/**
 * Undo 空间预留句柄。Undo 模块只依赖本端口表达“后续 grow 最多会创建若干 UNDO 页”的生命周期，
 * 不直接暴露 DiskSpaceManager 的 reservation 类型，从而保持 undo -> api 的依赖方向不反转。
 *
 * <p>实现必须幂等关闭；调用方推荐 try-with-resources，MTR memo 也会在异常路径兜底释放底层预留。
 */
public interface UndoSpaceReservation extends AutoCloseable {

    /**
     * 释放尚未消费的 undo 空间预留。不得抛出受检异常，避免污染 undo append 的领域异常边界。
     */
    @Override
    void close();
}
