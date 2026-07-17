package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.api.index.IndexPageHandle;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

/**
 * 从稳定 root page header 刷新 {@link BTreeIndex#rootLevel()} 的短读服务。DD binding 中保存的 level 仅是
 * 创建/恢复时提示；root grow/shrink 会在不改变 root page id 的前提下更新页头，因此结构写、rollback 与 purge
 * 在计算 redo/空间预算前必须消费本服务返回的新快照。
 *
 * <p>并发边界：调用方提供短只读 MTR，本服务只取得 root S latch 并物化 level，不返回 page handle；调用方应提交
 * 该读 MTR 后再开启结构写 MTR，避免跨索引持有 root latch。刷新值是瞬时快照，真正导航仍以 root 页实际 header 为权威。</p>
 */
public final class BTreeRootSnapshotService {

    /** root 页 fix/latch 入口；生命周期归 storage engine 组合根，本服务不关闭它。 */
    private final IndexPageAccess pageAccess;

    /**
     * 创建 root 快照服务。
     *
     * @param pageAccess index page 稳定访问入口；不能为 null。
     * @throws DatabaseValidationException {@code pageAccess} 为空时抛出。
     */
    public BTreeRootSnapshotService(IndexPageAccess pageAccess) {
        if (pageAccess == null) {
            throw new DatabaseValidationException("btree root snapshot pageAccess must not be null");
        }
        this.pageAccess = pageAccess;
    }

    /**
     * 读取 root page header 并生成同 identity/schema/segment 的最新 level 快照。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在页访问前校验 MTR 与 descriptor，非法输入不产生 Buffer fix 或 latch memo。</li>
     *     <li>以 SHARED 模式打开稳定 root page；fix 与 latch 归调用方 MTR，异常时由 MTR 终止路径统一释放。</li>
     *     <li>核对页头 index id，错配表示 DD binding、页归属或恢复状态损坏，必须 fail-closed。</li>
     *     <li>复制页头 level 到不可变 descriptor；不修改页、不产生 redo/dirty 副作用。</li>
     * </ol>
     *
     * @param mtr   调用方短只读 MTR；不能为 null，方法不提交它。
     * @param index 待刷新的索引描述符；root page id 是稳定定位来源。
     * @return root level 来自当前页头的新快照；其它 descriptor 字段保持不变。
     * @throws DatabaseValidationException MTR 或 descriptor 为空时抛出。
     * @throws BTreeStructureCorruptedException root 页头 index id 与 descriptor 不一致时抛出；调用方必须终止 MTR。
     */
    public BTreeIndex refresh(MiniTransaction mtr, BTreeIndex index) {
        // 1. 先完成纯参数校验，避免错误调用把页资源挂入 MTR memo。
        if (mtr == null || index == null) {
            throw new DatabaseValidationException("btree root snapshot mtr/index must not be null");
        }

        // 2. root 仅以 S latch 打开；本方法不显式释放，统一由短 MTR commit/rollback 按 memo 逆序释放。
        IndexPageHandle root = pageAccess.openIndexPageHandle(
                mtr, index.rootPageId(), PageLatchMode.SHARED);

        // 3. index id 是物理页归属不变量；错配时不能用错误 level 继续冻结结构预算。
        long actualIndexId = root.recordPage().header().indexId();
        if (actualIndexId != index.indexId()) {
            throw new BTreeStructureCorruptedException("root snapshot index id mismatch at "
                    + index.rootPageId() + ": expected " + index.indexId() + " but was " + actualIndexId);
        }

        // 4. 只复制权威 level，不写页，所以没有 redo、pageLSN 或 dirty publish 副作用。
        return index.withRootLevel(root.recordPage().header().level());
    }
}
