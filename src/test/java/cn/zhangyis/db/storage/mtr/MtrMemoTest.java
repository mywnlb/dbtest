package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MtrMemo 测试固定 LIFO 释放序、savepoint 局部释放、越界 no-op、释放异常聚合、push 校验。
 */
class MtrMemoTest {

    /** 记录关闭顺序的假资源；fail=true 时关闭抛异常但仍记录。 */
    private static final class Recorder implements AutoCloseable {
        private final String name;
        private final List<String> log;
        private final boolean fail;

        Recorder(String name, List<String> log, boolean fail) {
            this.name = name;
            this.log = log;
            this.fail = fail;
        }

        @Override
        public void close() {
            log.add(name);
            if (fail) {
                throw new RuntimeException("boom " + name);
            }
        }
    }

    @Test
    void releaseAllShouldCloseInLifoOrder() {
        List<String> log = new ArrayList<>();
        MtrMemo memo = new MtrMemo();
        memo.push(new Recorder("a", log, false));
        memo.push(new Recorder("b", log, false));
        memo.push(new Recorder("c", log, false));
        assertEquals(3, memo.depth());

        memo.releaseAll();

        assertEquals(List.of("c", "b", "a"), log);
        assertEquals(0, memo.depth());
    }

    @Test
    void releaseToShouldReleaseOnlyAboveSavepoint() {
        List<String> log = new ArrayList<>();
        MtrMemo memo = new MtrMemo();
        memo.push(new Recorder("a", log, false));
        int sp = memo.depth();
        memo.push(new Recorder("b", log, false));
        memo.push(new Recorder("c", log, false));

        memo.releaseTo(sp);

        assertEquals(List.of("c", "b"), log);
        assertEquals(1, memo.depth());
    }

    @Test
    void releaseToBeyondDepthShouldBeNoOp() {
        List<String> log = new ArrayList<>();
        MtrMemo memo = new MtrMemo();
        memo.push(new Recorder("a", log, false));

        memo.releaseTo(5);

        assertTrue(log.isEmpty());
        assertEquals(1, memo.depth());
    }

    @Test
    void releaseToNegativeShouldThrow() {
        MtrMemo memo = new MtrMemo();
        assertThrows(DatabaseValidationException.class, () -> memo.releaseTo(-1));
    }

    @Test
    void releaseShouldContinueAndAggregateOnCloseFailure() {
        List<String> log = new ArrayList<>();
        MtrMemo memo = new MtrMemo();
        memo.push(new Recorder("a", log, false));
        memo.push(new Recorder("b", log, true)); // 顶部，关闭抛异常

        assertThrows(MtrStateException.class, memo::releaseAll);

        assertEquals(List.of("b", "a"), log); // 异常后仍继续释放 a
        assertEquals(0, memo.depth());
    }

    @Test
    void pushNullShouldThrow() {
        MtrMemo memo = new MtrMemo();
        assertThrows(DatabaseValidationException.class, () -> memo.push(null));
    }
}
