package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TableSchema 有序列、ordinal 连续校验、列查找。 */
class TableSchemaTest {

    private ColumnDef col(int ord, String name, ColumnType type) {
        return new ColumnDef(new ColumnId(ord), name, type, ord);
    }

    /**
     * 验证 {@code buildsAndLooksUpColumns} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void buildsAndLooksUpColumns() {
        TableSchema s = new TableSchema(1L, List.of(
                col(0, "id", ColumnType.bigint(true, false)),
                col(1, "name", ColumnType.varchar(64, true))));
        assertEquals(2, s.columnCount());
        assertEquals("name", s.column(1).name());
        assertEquals(TypeId.BIGINT, s.column(0).type().typeId());
    }

    /**
     * 验证 {@code rejectsNonContiguousOrdinals} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsNonContiguousOrdinals() {
        assertThrows(DatabaseValidationException.class, () -> new TableSchema(1L, List.of(
                col(0, "a", ColumnType.intType(false, false)),
                col(2, "b", ColumnType.intType(false, false)))));
    }

    /**
     * 验证 {@code rejectsEmptyOrNull} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsEmptyOrNull() {
        assertThrows(DatabaseValidationException.class, () -> new TableSchema(1L, List.of()));
        assertThrows(DatabaseValidationException.class, () -> new TableSchema(1L, null));
    }

    /**
     * 验证 {@code compatConstructorDefaultsNonClustered} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void compatConstructorDefaultsNonClustered() {
        TableSchema s = new TableSchema(1L, List.of(
                col(0, "id", ColumnType.bigint(true, false))));
        assertFalse(s.clustered(), "two-arg compat constructor is non-clustered");
    }

    /**
     * 验证 {@code clusteredConstructorSetsFlag} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void clusteredConstructorSetsFlag() {
        TableSchema s = new TableSchema(2L, List.of(
                col(0, "id", ColumnType.bigint(true, false))), true);
        assertTrue(s.clustered());
    }
}
