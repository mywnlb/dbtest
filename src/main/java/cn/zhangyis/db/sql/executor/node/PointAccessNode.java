package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.sql.executor.storage.SqlDataAccessPort;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointAccess;

/** 聚簇主键或唯一二级 point cursor 的运行期访问节点。 */
public final class PointAccessNode extends StorageAccessNode {
    /**
     * @param storage Executor 数据端口
     * @param access 已验证 point access
     */
    public PointAccessNode(
            SqlDataAccessPort storage, PhysicalPointAccess access) {
        super(storage, access);
    }
}
