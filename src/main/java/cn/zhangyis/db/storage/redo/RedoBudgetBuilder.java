package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 操作级 redo 上界组合器。领域 estimator 用 record 权威 {@link RedoRecord#byteLength()} 或明确的页写次数构造上界；
 * 所有乘加均 checked，溢出或超过单批可恢复范围会在获取任何页资源前失败。
 */
public final class RedoBudgetBuilder {

    /** PAGE_BYTES 的 tag、pageId、offset 和 length 固定编码开销。 */
    public static final int PAGE_BYTES_HEADER = 21;
    /** PAGE_INIT 的完整编码长度。 */
    public static final int PAGE_INIT_BYTES = 17;

    private final RedoBudgetPurpose purpose;
    private long logicalBytes;

    private RedoBudgetBuilder(RedoBudgetPurpose purpose) {
        if (purpose == null || purpose == RedoBudgetPurpose.READ_ONLY) {
            throw new DatabaseValidationException("write redo budget requires a non-read-only purpose");
        }
        this.purpose = purpose;
    }

    /** 开始构造指定写操作的预算。 */
    public static RedoBudgetBuilder forPurpose(RedoBudgetPurpose purpose) {
        return new RedoBudgetBuilder(purpose);
    }

    /** 加入一条已物化 record 的精确编码长度。 */
    public RedoBudgetBuilder addRecord(RedoRecord record) {
        if (record == null) {
            throw new DatabaseValidationException("redo budget record must not be null");
        }
        return addLogicalBytes(record.byteLength());
    }

    /** 加入给定次数的 record 编码上界。 */
    public RedoBudgetBuilder addRepeatedRecord(RedoRecord record, long count) {
        if (record == null) {
            throw new DatabaseValidationException("redo budget record must not be null");
        }
        return addRepeatedLogicalBytes(record.byteLength(), count);
    }

    /** 加入一次 PAGE_BYTES（固定头加 payload）上界。 */
    public RedoBudgetBuilder addPageBytes(long payloadBytes) {
        return addRepeatedLogicalBytes(checkedAdd(PAGE_BYTES_HEADER, payloadBytes), 1);
    }

    /** 加入若干次 PAGE_BYTES 上界。 */
    public RedoBudgetBuilder addPageBytes(long payloadBytes, long count) {
        return addRepeatedLogicalBytes(checkedAdd(PAGE_BYTES_HEADER, payloadBytes), count);
    }

    /** 加入若干次 PAGE_INIT。 */
    public RedoBudgetBuilder addPageInit(long count) {
        return addRepeatedLogicalBytes(PAGE_INIT_BYTES, count);
    }

    /** 加入已经由领域算法计算出的逻辑 record 字节上界。 */
    public RedoBudgetBuilder addLogicalBytes(long bytes) {
        if (bytes < 0) {
            throw new DatabaseValidationException("redo budget bytes must not be negative: " + bytes);
        }
        logicalBytes = checkedAdd(logicalBytes, bytes);
        if (logicalBytes > RedoLogBlockSizing.MAX_LOGICAL_BATCH_BYTES) {
            throw new DatabaseValidationException("redo operation budget exceeds recoverable batch limit: "
                    + logicalBytes);
        }
        return this;
    }

    /** 构造不可变预算。 */
    public RedoAppendBudget build() {
        return RedoAppendBudget.upperBound(purpose, logicalBytes);
    }

    private RedoBudgetBuilder addRepeatedLogicalBytes(long bytes, long count) {
        if (bytes < 0 || count < 0) {
            throw new DatabaseValidationException("redo budget bytes/count must not be negative: bytes="
                    + bytes + ", count=" + count);
        }
        try {
            return addLogicalBytes(Math.multiplyExact(bytes, count));
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException("redo budget multiplication overflows: bytes="
                    + bytes + ", count=" + count, error);
        }
    }

    private static long checkedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException("redo budget addition overflows: left="
                    + left + ", right=" + right, error);
        }
    }
}
