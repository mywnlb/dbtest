package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** IndexKeyDef 复合 key part、ASC/DESC、prefix。 */
class IndexKeyDefTest {

    @Test
    void compositeKey() {
        IndexKeyDef k = new IndexKeyDef(7L, List.of(
                new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0),
                new KeyPartDef(new ColumnId(1), KeyOrder.DESC, 4)));
        assertEquals(2, k.parts().size());
        assertEquals(KeyOrder.DESC, k.parts().get(1).order());
        assertEquals(4, k.parts().get(1).prefixBytes());
    }

    @Test
    void rejectsEmptyParts() {
        assertThrows(DatabaseValidationException.class, () -> new IndexKeyDef(1L, List.of()));
    }

    @Test
    void rejectsNegativePrefix() {
        assertThrows(DatabaseValidationException.class,
                () -> new KeyPartDef(new ColumnId(0), KeyOrder.ASC, -1));
    }
}
