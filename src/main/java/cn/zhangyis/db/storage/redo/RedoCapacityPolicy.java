package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

/**
 * redo capacity pressure 策略。它把 checkpoint age 映射为刷脏压力，保持 redo 模块只输出边界判断，
 * 不直接遍历 Buffer Pool 或写 data file。
 */
public final class RedoCapacityPolicy {

    /** redo 可用容量字节数；R2 中等同为 LSN 距离。 */
    private final long capacityBytes;
    /** 达到该 age 后建议后台异步 flush。 */
    private final long asyncThresholdBytes;
    /** 达到该 age 后建议前台同步等待 checkpoint 进展。 */
    private final long syncThresholdBytes;
    /** 达到该 age 后后续实现应阻止新 redo reservation。 */
    private final long hardThresholdBytes;

    /**
     * 创建 {@code RedoCapacityPolicy}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param capacityBytes 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param asyncThresholdBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param syncThresholdBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param hardThresholdBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     */
    private RedoCapacityPolicy(long capacityBytes,
                               long asyncThresholdBytes,
                               long syncThresholdBytes,
                               long hardThresholdBytes) {
        this.capacityBytes = capacityBytes;
        this.asyncThresholdBytes = asyncThresholdBytes;
        this.syncThresholdBytes = syncThresholdBytes;
        this.hardThresholdBytes = hardThresholdBytes;
    }

    /**
     * 创建固定容量策略：50% 进入异步刷脏，75% 进入同步刷脏，90% 进入 hard limit。
     *
     * @param capacityBytes redo capacity 字节数，必须为正数。
     * @return 固定阈值策略。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static RedoCapacityPolicy fixed(long capacityBytes) {
        if (capacityBytes <= 0) {
            throw new DatabaseValidationException("redo capacity must be positive: " + capacityBytes);
        }
        long async = Math.max(1, capacityBytes * 50 / 100);
        long sync = Math.max(async, capacityBytes * 75 / 100);
        long hard = Math.max(sync, capacityBytes * 90 / 100);
        return new RedoCapacityPolicy(capacityBytes, async, sync, hard);
    }

    /**
     * 评估 checkpoint age。调用方必须保证 checkpoint 不超过 current，否则说明 checkpoint label 或 redo 边界损坏。
     *
     * @param currentLsn 当前 redo LSN。
     * @param checkpointLsn 最近持久化 checkpoint LSN。
     * @return capacity pressure 判断。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RedoCapacityDecision evaluate(Lsn currentLsn, Lsn checkpointLsn) {
        if (currentLsn == null || checkpointLsn == null) {
            throw new DatabaseValidationException("redo capacity LSN inputs must not be null");
        }
        if (currentLsn.value() < checkpointLsn.value()) {
            throw new DatabaseValidationException("redo current LSN must cover checkpoint LSN: current="
                    + currentLsn + ", checkpoint=" + checkpointLsn);
        }
        long age = currentLsn.value() - checkpointLsn.value();
        RedoCapacityPressure pressure;
        if (age >= hardThresholdBytes) {
            pressure = RedoCapacityPressure.HARD_LIMIT;
        } else if (age >= syncThresholdBytes) {
            pressure = RedoCapacityPressure.SYNC_FLUSH;
        } else if (age >= asyncThresholdBytes) {
            pressure = RedoCapacityPressure.ASYNC_FLUSH;
        } else {
            pressure = RedoCapacityPressure.NONE;
        }
        long target = pressure == RedoCapacityPressure.NONE
                ? checkpointLsn.value()
                : Math.max(0, currentLsn.value() - asyncThresholdBytes);
        return new RedoCapacityDecision(pressure, age, Lsn.of(target));
    }

    /** redo capacity 字节数，用于诊断和后续配置回显。 */
    public long capacityBytes() {
        return capacityBytes;
    }
}
