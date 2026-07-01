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

    /**
     * 选择性（非 LIFO）释放：latch coupling crab 需要在持有子页后提前放掉中间父页，
     * 而它未必在栈顶。release 应按身份摘掉指定槽位、close 之，余下资源仍保持 LIFO 完整。
     */
    @Test
    void releaseShouldSelectivelyRemoveMiddleResourceKeepingLifoForRest() {
        List<String> log = new ArrayList<>();
        MtrMemo memo = new MtrMemo();
        Recorder a = new Recorder("a", log, false);
        Recorder b = new Recorder("b", log, false);
        Recorder c = new Recorder("c", log, false);
        memo.push(a);
        memo.push(b);
        memo.push(c);

        memo.release(b); // 释放中间槽（非栈顶）

        assertEquals(List.of("b"), log); // 只关了 b
        assertEquals(2, memo.depth());

        memo.releaseAll();
        assertEquals(List.of("b", "c", "a"), log); // 余下仍 LIFO：先 c 后 a
        assertEquals(0, memo.depth());
    }

    /** 释放不在本 memo 中的资源视为不变量破坏：调用方只应释放自己仍持有的 guard。 */
    @Test
    void releaseUnknownResourceShouldThrow() {
        MtrMemo memo = new MtrMemo();
        memo.push(new Recorder("a", new ArrayList<>(), false));
        assertThrows(MtrStateException.class,
                () -> memo.release(new Recorder("x", new ArrayList<>(), false)));
    }
}
