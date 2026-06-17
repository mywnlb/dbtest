package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** TableSchema 有序列、ordinal 连续校验、列查找。 */
class TableSchemaTest {

    private ColumnDef col(int ord, String name, ColumnType type) {
        return new ColumnDef(new ColumnId(ord), name, type, ord);
    }

    @Test
    void buildsAndLooksUpColumns() {
        TableSchema s = new TableSchema(1L, List.of(
                col(0, "id", ColumnType.bigint(true, false)),
                col(1, "name", ColumnType.varchar(64, true))));
        assertEquals(2, s.columnCount());
        assertEquals("name", s.column(1).name());
        assertEquals(TypeId.BIGINT, s.column(0).type().typeId());
    }

    @Test
    void rejectsNonContiguousOrdinals() {
        assertThrows(DatabaseValidationException.class, () -> new TableSchema(1L, List.of(
                col(0, "a", ColumnType.intType(false, false)),
                col(2, "b", ColumnType.intType(false, false)))));
    }

    @Test
    void rejectsEmptyOrNull() {
        assertThrows(DatabaseValidationException.class, () -> new TableSchema(1L, List.of()));
        assertThrows(DatabaseValidationException.class, () -> new TableSchema(1L, null));
    }
}
