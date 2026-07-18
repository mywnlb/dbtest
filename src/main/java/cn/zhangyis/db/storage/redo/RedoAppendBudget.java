package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * 一个 MTR 在取得 page latch、buffer fix 或 FSP lease 前声明的不可变 redo 上界。
 * logical 上界用于 checkpoint-age 反压，physical 上界用于单批 LogBlock/file-fit 校验；两者必须由同一权威公式派生。
 *
 * @param purpose 领域用途，只参与诊断。
 * @param logicalUpperBound records 编码总长度上界。
 * @param physicalBlockUpperBound LogBlock v1 完整块链上界。
 */
public record RedoAppendBudget(RedoBudgetPurpose purpose,
                               long logicalUpperBound,
                               long physicalBlockUpperBound) {

    public RedoAppendBudget {
        if (purpose == null) {
            throw new DatabaseValidationException("redo budget purpose must not be null");
        }
        if (logicalUpperBound < 0 || physicalBlockUpperBound < 0) {
            throw new DatabaseValidationException("redo budget bounds must not be negative");
        }
        long expectedPhysical = RedoLogBlockSizing.physicalBytesForLogical(logicalUpperBound);
        if (physicalBlockUpperBound != expectedPhysical) {
            throw new DatabaseValidationException("redo physical budget does not match LogBlock formula: logical="
                    + logicalUpperBound + ", physical=" + physicalBlockUpperBound
                    + ", expected=" + expectedPhysical);
        }
        if (purpose == RedoBudgetPurpose.READ_ONLY && logicalUpperBound != 0) {
            throw new DatabaseValidationException("read-only redo budget must be zero");
        }
    }

    /** 创建只读零预算。 */
    public static RedoAppendBudget readOnly() {
        return new RedoAppendBudget(RedoBudgetPurpose.READ_ONLY, 0, 0);
    }

    /** 从逻辑上界创建预算，物理值始终由 LogBlock v1 公式派生。
     *
     * @param purpose 选择 {@code upperBound} 分支的 {@code RedoBudgetPurpose} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param logicalUpperBound redo 预算计算使用的非负工作量上界 {@code logicalUpperBound}；必须保守覆盖实际写入量，且累加时不得溢出
     * @return {@code upperBound} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    public static RedoAppendBudget upperBound(RedoBudgetPurpose purpose, long logicalUpperBound) {
        return new RedoAppendBudget(purpose, logicalUpperBound,
                RedoLogBlockSizing.physicalBytesForLogical(logicalUpperBound));
    }

    /** no-op 测试 manager 的兼容预算；生产 manager 禁止无参 begin 走到这里。 */
    public static RedoAppendBudget testingUnbounded() {
        return upperBound(RedoBudgetPurpose.TEST_UNBOUNDED, RedoLogBlockSizing.MAX_LOGICAL_BATCH_BYTES);
    }

    /** 精确计算已冻结 records 的逻辑与物理尺寸。
     *
     * @param records 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return {@code measure} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static RedoAppendUsage measure(List<RedoRecord> records) {
        if (records == null) {
            throw new DatabaseValidationException("redo records must not be null");
        }
        long logical = 0;
        for (RedoRecord record : records) {
            if (record == null) {
                throw new DatabaseValidationException("redo record must not be null");
            }
            try {
                logical = Math.addExact(logical, record.byteLength());
            } catch (ArithmeticException error) {
                throw new DatabaseValidationException("redo record byte sum overflows", error);
            }
        }
        return new RedoAppendUsage(logical, RedoLogBlockSizing.physicalBytesForLogical(logical));
    }

    /**
     * 在 append 前验证实际持久 records 没有超过 admission 上界。低估属于致命实现错误，调用方不得释放页资源继续服务。
     *
     * @param records 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return {@code requireCovers} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws RedoBudgetExceededException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    public RedoAppendUsage requireCovers(List<RedoRecord> records) {
        RedoAppendUsage actual = measure(records);
        if (actual.logicalBytes() > logicalUpperBound
                || actual.physicalBlockBytes() > physicalBlockUpperBound) {
            throw new RedoBudgetExceededException("redo budget underestimated for " + purpose
                    + ": budgetLogical=" + logicalUpperBound + ", actualLogical=" + actual.logicalBytes()
                    + ", budgetPhysical=" + physicalBlockUpperBound
                    + ", actualPhysical=" + actual.physicalBlockBytes());
        }
        return actual;
    }
}
