package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.PageAllocationHint;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 0.15 B+Tree split 分配 hint 判定测试。只有插入 key 落在当前 leaf 边界外且对应方向无 sibling 时，
 * 才把 leaf split 视为明显顺序增长；中间 split 或已有 sibling 的边界 split 仍保持 NO_DIRECTION。
 */
class BTreeAllocationHintPlannerTest {

    private static final SpaceId SPACE = SpaceId.of(31);
    private static final PageId LEAF = PageId.of(SPACE, PageNo.of(128));
    private static final long INDEX_ID = 7L;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final SearchKeyComparator comparator = new SearchKeyComparator(registry);
    private final BTreeIndex index = new BTreeIndex(INDEX_ID, LEAF, 0, keyDef(), schema(), true);

    @Test
    void rightEdgeInsertWithoutRightSiblingProducesUpHint() {
        PageAllocationHint hint = BTreeAllocationHintPlanner.leafSplitHint(
                LEAF, k(10), k(1), k(9), true, false, 3L, index, comparator);

        assertEquals(PageAllocationHint.Direction.UP, hint.direction());
        assertEquals(LEAF.pageNo(), hint.hintPageNo().orElseThrow());
        assertEquals(3L, hint.pagesNeeded());
    }

    @Test
    void leftEdgeInsertWithoutLeftSiblingProducesDownHint() {
        PageAllocationHint hint = BTreeAllocationHintPlanner.leafSplitHint(
                LEAF, k(0), k(1), k(9), false, true, 2L, index, comparator);

        assertEquals(PageAllocationHint.Direction.DOWN, hint.direction());
        assertEquals(LEAF.pageNo(), hint.hintPageNo().orElseThrow());
        assertEquals(2L, hint.pagesNeeded());
    }

    @Test
    void middleInsertOrExistingSiblingKeepsNoDirection() {
        assertTrue(BTreeAllocationHintPlanner.leafSplitHint(
                LEAF, k(5), k(1), k(9), false, false, 1L, index, comparator).hintPageNo().isEmpty());
        assertEquals(PageAllocationHint.Direction.NO_DIRECTION, BTreeAllocationHintPlanner.leafSplitHint(
                LEAF, k(10), k(1), k(9), true, true, 1L, index, comparator).direction());
        assertEquals(PageAllocationHint.Direction.NO_DIRECTION, BTreeAllocationHintPlanner.leafSplitHint(
                LEAF, k(0), k(1), k(9), true, false, 1L, index, comparator).direction());
    }

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0)));
    }

    private static IndexKeyDef keyDef() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static SearchKey k(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }
}
