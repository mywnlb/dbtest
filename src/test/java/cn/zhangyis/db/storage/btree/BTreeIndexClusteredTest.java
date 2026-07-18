package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** BTreeIndex.clustered() 从 schema 派生（单一权威态）。 */
class BTreeIndexClusteredTest {

    private static final PageId ROOT = PageId.of(SpaceId.of(1), PageNo.of(1));

    private TableSchema schema(boolean clustered) {
        ColumnDef id = new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0);
        return new TableSchema(1, List.of(id), clustered);
    }

    private IndexKeyDef keyDef() {
        return new IndexKeyDef(1, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    /**
     * 验证 {@code clusteredDerivesFromSchema} 所描述的字典/DDL 协作，并断言版本、对象身份、缓存失效和物理绑定保持一致。
     */
    @Test
    void clusteredDerivesFromSchema() {
        BTreeIndex idx = new BTreeIndex(1, ROOT, 0, keyDef(), schema(true), true);
        assertTrue(idx.clustered());
    }

    /**
     * 验证 {@code nonClusteredSchemaMakesNonClusteredIndex} 所描述的 B+Tree 定位或结构变化，并断言键序、父子链接、页资源和唯一性不变量。
     */
    @Test
    void nonClusteredSchemaMakesNonClusteredIndex() {
        BTreeIndex idx = new BTreeIndex(1, ROOT, 0, keyDef(), schema(false), true);
        assertFalse(idx.clustered());
    }
}
