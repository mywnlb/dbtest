package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** DML workload 必须组合树结构与 undo 首写成本，且在取页前只消费索引元数据快照。 */
class DmlRedoBudgetEstimatorTest {

    /**
     * 验证 {@code insertCombinesTreeHeightAndUndoFirstWrite} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void insertCombinesTreeHeightAndUndoFirstWrite() {
        BTreeIndex index = indexAtLevel(2);

        long firstWrite = DmlRedoBudgetEstimator.insert(index, true).pageImageEquivalents();
        long existingUndo = DmlRedoBudgetEstimator.insert(index, false).pageImageEquivalents();

        assertEquals(34, firstWrite);
        assertEquals(26, existingUndo);
        assertTrue(firstWrite > existingUndo);
    }

    /**
     * 验证 {@code pointRewriteKeepsTreeCostFixedButStillAccountsForUndo} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void pointRewriteKeepsTreeCostFixedButStillAccountsForUndo() {
        BTreeIndex index = indexAtLevel(4);

        assertEquals(16, DmlRedoBudgetEstimator.pointRewrite(index, true).pageImageEquivalents());
        assertEquals(8, DmlRedoBudgetEstimator.pointRewrite(index, false).pageImageEquivalents());
    }

    private static BTreeIndex indexAtLevel(int rootLevel) {
        long indexId = 9;
        IndexKeyDef key = new IndexKeyDef(indexId,
                List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
        TableSchema schema = new TableSchema(1,
                List.of(new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0)), true);
        return new BTreeIndex(indexId, PageId.of(SpaceId.of(7), PageNo.of(3)), rootLevel,
                key, schema, true);
    }
}
