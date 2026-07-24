package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.sql.executor.storage.SqlDataAccessPort;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSecondaryPrefixAccess;

/** 普通二级 logical-prefix cursor 的运行期访问节点。 */
public final class SecondaryPrefixAccessNode extends StorageAccessNode {
    /**
     * @param storage Executor 数据端口
     * @param access 已验证 secondary-prefix access
     */
    public SecondaryPrefixAccessNode(
            SqlDataAccessPort storage,
            PhysicalSecondaryPrefixAccess access) {
        super(storage, access);
    }
}
