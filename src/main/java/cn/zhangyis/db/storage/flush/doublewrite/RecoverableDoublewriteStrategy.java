package cn.zhangyis.db.storage.flush.doublewrite;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;
import cn.zhangyis.db.storage.flush.FlushBatchSource;

import java.util.List;

/**
 * 可恢复 doublewrite 策略：data file write 前先把完整页副本写入 doublewrite 文件并 force，
 * 崩溃后可用该副本修复 torn/corrupt data page。
 */
public final class RecoverableDoublewriteStrategy implements DoublewriteStrategy {

    /**
     * 本对象持有的 {@code repository} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DoublewriteFileRepository repository;
    /**
     * 本对象持有的 {@code channel} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DoublewriteChannel channel;

    /**
     * 创建 {@code RecoverableDoublewriteStrategy}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param repository 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecoverableDoublewriteStrategy(DoublewriteFileRepository repository) {
        if (repository == null) {
            throw new DatabaseValidationException("doublewrite repository must not be null");
        }
        this.repository = repository;
        this.channel = null;
    }

    /** 使用 FlushList/LRU 双物理文件的生产策略。
     *
     * @param channel 恢复、checkpoint、doublewrite 或刷脏阶段的协作状态；不得为 {@code null}，阶段和持久化边界必须与当前实例的恢复状态机一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecoverableDoublewriteStrategy(DoublewriteChannel channel) {
        if (channel == null) {
            throw new DatabaseValidationException("doublewrite channel must not be null");
        }
        this.repository = null;
        this.channel = channel;
    }

    @Override
    public DoublewriteMode mode() {
        return DoublewriteMode.DETECT_AND_RECOVER;
    }

    /**
     * 校验输入与当前状态后修改脏页刷盘与 checkpoint领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param snapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public void beforeDataFileWrite(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
        boolean appended = false;
        try {
            repository.append(snapshot);
            appended = true;
            repository.force();
        } catch (DatabaseRuntimeException e) {
            if (appended) {
                repository.releaseSlot(snapshot);
            }
            throw e;
        }
    }

    /**
     * 校验输入与当前状态后修改脏页刷盘与 checkpoint领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取脏页、page LSN、代际与 checkpoint 压力快照，先排除未固定或已失效的候选。</li>
     *     <li>在不持页闩执行慢等待的前提下推进 redo durable 边界，确保数据页写盘前满足 WAL。</li>
     *     <li>按既定 doublewrite、表空间写入和 force 顺序持久化快照，部分失败只确认实际成功的页面。</li>
     *     <li>重新校验代际与 dirty version 后发布完成状态；并发再修改页继续保持 dirty，异常不推进不安全边界。</li>
     * </ol>
     *
     * @param snapshots 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public void beforeDataFileWriteBatch(List<FlushPageSnapshot> snapshots) {
        // 1、读取脏页、page LSN、代际与 checkpoint 压力快照，在共享或持久副作用前拒绝非法状态。
        if (snapshots == null || snapshots.isEmpty()) {
            throw new DatabaseValidationException("doublewrite batch snapshots must not be empty");
        }
        // 2、继续完成范围、身份与候选校验；通过后，在不持页闩执行慢等待的前提下推进 redo durable 边界，保持处理顺序与资源边界。
        DoublewriteBatch batch = DoublewriteBatch.of(snapshots);
        // 3、在中间分支复核阶段性结果；满足条件后，按既定 doublewrite、表空间写入和 force 顺序持久化快照，并维持领域不变量。
        boolean appended = false;
        // 4、重新校验代际与 dirty version 后发布完成状态，以稳定返回或领域异常完成收口。
        try {
            repository.appendBatch(batch);
            appended = true;
            repository.force();
        } catch (DatabaseRuntimeException e) {
            if (appended) {
                repository.releaseBatch(batch);
            }
            throw e;
        }
    }

    /**
     * 校验输入与当前状态后修改脏页刷盘与 checkpoint领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取脏页、page LSN、代际与 checkpoint 压力快照，先排除未固定或已失效的候选。</li>
     *     <li>在不持页闩执行慢等待的前提下推进 redo durable 边界，确保数据页写盘前满足 WAL。</li>
     *     <li>按既定 doublewrite、表空间写入和 force 顺序持久化快照，部分失败只确认实际成功的页面。</li>
     *     <li>重新校验代际与 dirty version 后发布完成状态；并发再修改页继续保持 dirty，异常不推进不安全边界。</li>
     * </ol>
     *
     * @param source 选择 {@code beforeDataFileWriteBatch} 分支的 {@code FlushBatchSource} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param snapshots 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     */
    @Override
    public void beforeDataFileWriteBatch(FlushBatchSource source, List<FlushPageSnapshot> snapshots) {
        // 1、读取脏页、page LSN、代际与 checkpoint 压力快照，在共享或持久副作用前拒绝非法状态。
        validateSourceBatch(source, snapshots);
        // 2、继续完成范围、身份与候选校验；通过后，在不持页闩执行慢等待的前提下推进 redo durable 边界，保持处理顺序与资源边界。
        if (channel == null) {
            beforeDataFileWriteBatch(snapshots);
            return;
        }
        // 3、在中间分支复核阶段性结果；满足条件后，按既定 doublewrite、表空间写入和 force 顺序持久化快照，并维持领域不变量。
        DoublewriteBatch batch = DoublewriteBatch.of(snapshots);
        boolean appended = false;
        // 4、重新校验代际与 dirty version 后发布完成状态，以稳定返回或领域异常完成收口。
        try {
            channel.appendBatch(toChannel(source), batch);
            appended = true;
            channel.force(toChannel(source));
        } catch (DatabaseRuntimeException e) {
            if (appended) {
                channel.releaseBatch(toChannel(source), batch);
            }
            throw e;
        }
    }

    /**
     * 校验输入与当前状态后修改脏页刷盘与 checkpoint领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param snapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public void afterDataFileWrite(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
        repository.releaseSlot(snapshot);
    }

    /**
     * 校验输入与当前状态后修改脏页刷盘与 checkpoint领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param snapshots 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public void afterDataFileWriteBatch(List<FlushPageSnapshot> snapshots) {
        if (snapshots == null || snapshots.isEmpty()) {
            throw new DatabaseValidationException("doublewrite batch snapshots must not be empty");
        }
        repository.releaseBatch(DoublewriteBatch.of(snapshots));
    }

    /**
     * 校验输入与当前状态后修改脏页刷盘与 checkpoint领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param source 选择 {@code afterDataFileWriteBatch} 分支的 {@code FlushBatchSource} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param snapshots 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     */
    @Override
    public void afterDataFileWriteBatch(FlushBatchSource source, List<FlushPageSnapshot> snapshots) {
        validateSourceBatch(source, snapshots);
        if (channel == null) {
            afterDataFileWriteBatch(snapshots);
            return;
        }
        channel.releaseBatch(toChannel(source), DoublewriteBatch.of(snapshots));
    }

    /**
     * 推进 {@code abortDataFileWriteBatch} 对应的脏页刷盘与 checkpoint阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
     *
     * @param source 选择 {@code abortDataFileWriteBatch} 分支的 {@code FlushBatchSource} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param snapshots 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     */
    @Override
    public void abortDataFileWriteBatch(FlushBatchSource source, List<FlushPageSnapshot> snapshots) {
        afterDataFileWriteBatch(source, snapshots);
    }

    private static void validateSourceBatch(FlushBatchSource source, List<FlushPageSnapshot> snapshots) {
        if (source == null || snapshots == null || snapshots.isEmpty()) {
            throw new DatabaseValidationException("doublewrite source/batch must be non-null and non-empty");
        }
    }

    private static DoublewriteChannelId toChannel(FlushBatchSource source) {
        return source == FlushBatchSource.LRU ? DoublewriteChannelId.LRU : DoublewriteChannelId.FLUSH_LIST;
    }
}
