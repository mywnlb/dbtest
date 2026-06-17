package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageId;

/**
 * 页写监听（依赖倒置接缝）：PageGuard 每次物理写后回调，把 (pageId, offset, 实际写入字节) 上报给挂载方。
 * 默认 {@link #NO_OP}。签名只用 domain + 原语，**不含 redo/mtr 类型**——buf 不反向依赖上层；
 * MTR 提供实现（{@code MtrRedoCollector}）把回调译成 redo record。
 */
public interface PageWriteListener {

    /** 空实现：非 MTR 路径（直接 pool.getPage）使用，零开销、零行为变化。 */
    PageWriteListener NO_OP = (pageId, offset, newBytes) -> { };

    /**
     * 一次物理写完成后回调。
     *
     * @param pageId   被写页。
     * @param offset   页内起始偏移。
     * @param newBytes 该次写入后该区间的实际字节（PageGuard 写后读回，调用方如需保留应自行 copy）。
     */
    void onWrite(PageId pageId, int offset, byte[] newBytes);
}
