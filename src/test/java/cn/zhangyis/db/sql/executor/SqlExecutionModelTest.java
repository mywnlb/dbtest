package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** SQL 公开结果模型不变量。 */
class SqlExecutionModelTest {
    /**
     * 验证 {@code bytesAndListsAreDefensivelyCopiedAndUnsignedBigintIsExact} 对应的SQL 计划执行行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void bytesAndListsAreDefensivelyCopiedAndUnsignedBigintIsExact() {
        byte[] source = {1, 2};
        SqlValue.BytesValue bytes = new SqlValue.BytesValue(source);
        source[0] = 9;
        assertArrayEquals(new byte[]{1, 2}, bytes.value());
        byte[] returned = bytes.value();
        returned[1] = 8;
        assertArrayEquals(new byte[]{1, 2}, bytes.value());

        BigInteger unsignedMax = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
        assertEquals(unsignedMax, new SqlValue.IntegerValue(unsignedMax).value());
        ArrayList<SqlValue> mutable = new ArrayList<>(List.of(bytes));
        SqlRow row = new SqlRow(mutable);
        mutable.clear();
        assertEquals(1, row.values().size());
    }

    /**
     * 验证 {@code queryRejectsDuplicateColumnsAndWidthMismatch} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void queryRejectsDuplicateColumnsAndWidthMismatch() {
        ColumnTypeDefinition type = ColumnTypeDefinition.scalar(DictionaryTypeId.INT, false, false);
        ResultColumn id = new ResultColumn("id", type);
        TransactionStatus idle = TransactionStatus.idle(true);
        assertThrows(DatabaseValidationException.class,
                () -> new QueryResult(List.of(id, id), List.of(), idle));
        assertThrows(DatabaseValidationException.class,
                () -> new QueryResult(List.of(id), List.of(new SqlRow(List.of())), idle));
        assertThrows(DatabaseValidationException.class,
                () -> new TransactionStatus(true, false, true));
    }
}
