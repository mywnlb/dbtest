package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.sql.executor.ResultColumn;
import cn.zhangyis.db.sql.executor.exception.SqlExecutionException;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.executor.runtime.ExecutionContext;
import cn.zhangyis.db.sql.executor.storage.SqlStatementDeadline;
import cn.zhangyis.db.sql.executor.storage.SqlTransactionHandle;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 验证 PlanNode 模板在部分打开失败时仍收敛资源和异常图。 */
class AbstractPlanNodeTest {

    /** openNode 主失败必须保留，closeNode 失败只能作为 suppressed，最终状态固定 CLOSED。 */
    @Test
    void closesPartialResourcesWhenOpenFails() {
        SqlExecutionException openFailure =
                new SqlExecutionException("test open failure");
        SqlExecutionException closeFailure =
                new SqlExecutionException("test close failure");
        FailingOpenNode node = new FailingOpenNode(
                openFailure, closeFailure);

        SqlExecutionException actual = assertThrows(
                SqlExecutionException.class,
                () -> node.open(new ExecutionContext(
                        new TestHandle(),
                        SqlStatementDeadline.after(
                                Duration.ofSeconds(1)))));

        assertSame(openFailure, actual);
        assertEquals(1, node.closeCalls);
        assertEquals(PlanNodeState.CLOSED, node.state());
        assertEquals(1, actual.getSuppressed().length);
        assertSame(closeFailure, actual.getSuppressed()[0]);
        node.close();
        assertEquals(1, node.closeCalls);
    }

    /** 模拟 child 已取得部分资源后在 open 阶段失败的节点。 */
    private static final class FailingOpenNode
            extends AbstractPlanNode {
        private final RuntimeException openFailure;
        private final RuntimeException closeFailure;
        private int closeCalls;

        private FailingOpenNode(
                RuntimeException openFailure,
                RuntimeException closeFailure) {
            this.openFailure = openFailure;
            this.closeFailure = closeFailure;
        }

        @Override
        protected void openNode(ExecutionContext context) {
            throw openFailure;
        }

        @Override
        protected SqlRowView advanceNode() {
            return null;
        }

        @Override
        protected void closeNode() {
            closeCalls++;
            throw closeFailure;
        }

        @Override
        public List<ResultColumn> columns() {
            return List.of();
        }
    }

    /** 测试运行上下文使用的不透明事务能力。 */
    private static final class TestHandle
            implements SqlTransactionHandle {
    }
}
