package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.sql.executor.storage.SqlDataAccessPort;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeAccess;

/** comparison、复合 prefix 或 full scan cursor 的运行期访问节点。 */
public final class RangeAccessNode extends StorageAccessNode {
    /**
     * @param storage Executor 数据端口
     * @param access 已验证 range access
     */
    public RangeAccessNode(
            SqlDataAccessPort storage, PhysicalRangeAccess access) {
        super(storage, access);
    }
}
