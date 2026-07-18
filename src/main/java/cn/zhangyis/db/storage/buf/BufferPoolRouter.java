package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

/**
 * Buffer Pool 分片路由（§5.2/§13.2）：把 {@link PageId} 确定性地映射到某个 {@code BufferPoolInstance} 下标。
 *
 * <p><b>不变量（分片正确性根基）</b>：同一 {@code PageId} 必恒路由到同一 instance——get/prefetch/flush/snapshot/
 * invalidate 全部依赖这一点对同一页选同一分片，否则帧会在错误分片被查/被删，破坏单页生命周期。路由<b>不加任何锁</b>
 * （§13.2「instance routing 不加锁」），是纯函数。
 *
 * <p>散列把 {@code spaceId} 与 {@code pageNo} 混合后取 {@link Math#floorMod} 到 {@code [0, instanceCount)}，使不同
 * 表空间的同页号、同表空间的连续页号都尽量打散到各分片，避免某分片独占。{@code instanceCount==1} 时恒返回 0，
 * 与单实例池字节级等价。
 */
public final class BufferPoolRouter {

    /** 分片数量（≥1）。固定于构造期，路由结果对它取模。 */
    private final int instanceCount;

    /**
     * 创建 {@code BufferPoolRouter}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param instanceCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public BufferPoolRouter(int instanceCount) {
        if (instanceCount < 1) {
            throw new DatabaseValidationException("buffer pool instance count must be >= 1: " + instanceCount);
        }
        this.instanceCount = instanceCount;
    }

    /** 分片数量。 */
    public int instanceCount() {
        return instanceCount;
    }

    /**
     * 把页路由到归属 instance 下标。
     *
     * @param pageId 目标页。
     * @return 归属 instance 下标，落在 {@code [0, instanceCount)}。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public int route(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("router pageId must not be null");
        }
        if (instanceCount == 1) {
            return 0; // 单分片快路径：恒 0，等价单实例池。
        }
        long hash = mix(pageId.spaceId().value(), pageId.pageNo().value());
        // floorMod 保证非负结果（hash 可能为负），落在 [0, instanceCount)。
        return (int) Math.floorMod(hash, (long) instanceCount);
    }

    /**
     * 混合 spaceId 与 pageNo 为一个 64 位散列（MurmurHash3 fmix64 收尾，抗低位规律）。
     * 让连续 pageNo / 同页号不同 space 都打散到各分片。
     */
    private static long mix(int spaceId, long pageNo) {
        long h = spaceId * 0x9E3779B97F4A7C15L + pageNo;
        h ^= (h >>> 33);
        h *= 0xFF51AFD7ED558CCDL;
        h ^= (h >>> 33);
        h *= 0xC4CEB9FE1A85EC53L;
        h ^= (h >>> 33);
        return h;
    }
}
