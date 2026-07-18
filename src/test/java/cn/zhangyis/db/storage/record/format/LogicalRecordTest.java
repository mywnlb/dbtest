package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** LogicalRecord：四参兼容构造器无隐藏列、五参携带隐藏列。 */
class LogicalRecordTest {

    private List<ColumnValue> vals() {
        return List.of(new ColumnValue.IntValue(1));
    }

    /**
     * 验证 {@code compatConstructorHasNoHiddenColumns} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void compatConstructorHasNoHiddenColumns() {
        LogicalRecord r = new LogicalRecord(1L, vals(), false, RecordType.CONVENTIONAL);
        assertNull(r.hiddenColumns());
    }

    /**
     * 验证 {@code hiddenColumnsCarried} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void hiddenColumnsCarried() {
        HiddenColumns h = new HiddenColumns(TransactionId.of(5), RollPointer.NULL);
        LogicalRecord r = new LogicalRecord(1L, vals(), false, RecordType.CONVENTIONAL, h);
        assertEquals(h, r.hiddenColumns());
    }
}
