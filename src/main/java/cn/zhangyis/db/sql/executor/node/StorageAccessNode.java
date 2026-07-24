package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.ResultColumn;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.executor.runtime.ExecutionContext;
import cn.zhangyis.db.sql.executor.storage.SqlDataAccessPort;
import cn.zhangyis.db.sql.executor.storage.SqlStorageCursor;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalAccess;

import java.util.List;

/**
 * 三类 storage access node 的公共生命周期骨架；具体子类只固定 PhysicalAccess 的 sealed 种类。
 */
abstract class StorageAccessNode extends AbstractPlanNode {
    /** Executor 可见的窄数据端口。 */
    private final SqlDataAccessPort storage;
    /** Optimizer 已选择且无 residual/projection 的访问叶。 */
    private final PhysicalAccess access;
    /** 访问叶输出 exact table 的全部列，供 Filter 按 table ordinal 读取。 */
    private final List<ResultColumn> columns;
    /** open 后唯一 cursor；只能由本节点 close。 */
    private SqlStorageCursor cursor;

    /**
     * 创建访问节点。
     *
     * @param storage 组合根注入的数据端口
     * @param access 不可变物理访问叶
     * @throws DatabaseValidationException 参数缺失时抛出
     */
    protected StorageAccessNode(
            SqlDataAccessPort storage, PhysicalAccess access) {
        if (storage == null || access == null) {
            throw new DatabaseValidationException(
                    "storage access node requires port and physical access");
        }
        this.storage = storage;
        this.access = access;
        this.columns = access.table().columns().stream()
                .map(column -> new ResultColumn(
                        column.name().displayName(), column.type()))
                .toList();
    }

    /**
     * 使用当前语句上下文打开唯一 storage cursor；端口返回 Java null 属于协议错误。
     *
     * @param context 当前语句事务能力与绝对期限
     * @throws DatabaseValidationException Data Port 违反非空 cursor 契约时抛出
     */
    @Override
    protected final void openNode(ExecutionContext context) {
        cursor = context.cursorScope().isPresent()
                ? context.cursorScope().orElseThrow()
                .openCursor(access)
                : storage.openCursor(
                context.transaction(), access,
                context.deadline());
        if (cursor == null) {
            throw new DatabaseValidationException(
                    "SQL data port returned null storage cursor");
        }
    }

    /**
     * 拉取下一条完整逻辑候选，并只在 advance 成功时读取 cursor current。
     *
     * @return 下一条 cursor-owned 行视图，EOF 时为 Java {@code null}
     */
    @Override
    protected final SqlRowView advanceNode() {
        return cursor.advance() ? cursor.current() : null;
    }

    /** 幂等转移并关闭唯一 cursor，避免 close 失败后模板重复释放同一能力。 */
    @Override
    protected final void closeNode() {
        if (cursor != null) {
            SqlStorageCursor toClose = cursor;
            cursor = null;
            toClose.close();
        }
    }

    @Override
    public final List<ResultColumn> columns() {
        return columns;
    }
}
