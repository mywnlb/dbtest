package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.buf.DirtyVictimFlusher;

/**
 * 把 Buffer Pool 的淘汰端口 {@link DirtyVictimFlusher} 适配到 {@link FlushCoordinator#singlePageFlush}。
 * 由 {@code flush} 实现（依赖方向 flush→buf），使 {@code buf} 淘汰脏帧时复用既有 WAL gate + checksum +
 * doublewrite 单页 flush 管线，而无需 import {@code flush}。
 */
public final class CoordinatedDirtyVictimFlusher implements DirtyVictimFlusher {

    /**
     * 本对象持有的 {@code coordinator} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final FlushCoordinator coordinator;

    /**
     * 创建 {@code CoordinatedDirtyVictimFlusher}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param coordinator 由组合根提供的 {@code FlushCoordinator} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public CoordinatedDirtyVictimFlusher(FlushCoordinator coordinator) {
        if (coordinator == null) {
            throw new DatabaseValidationException("flush coordinator must not be null");
        }
        this.coordinator = coordinator;
    }

    /**
     * 按 {@link FlushResultStatus} 映射淘汰端口契约：
     * <ul>
     *   <li>{@code CLEAN}→true（已落盘清脏，帧可复用）；</li>
     *   <li>{@code KEPT_DIRTY}/{@code SKIPPED_NOT_DIRTY}/{@code SKIPPED_REDO_NOT_DURABLE}→false（本轮未清成，调用方另选）；</li>
     *   <li>{@code FAILED}→抛出所携带的领域异常根因（真 IO/doublewrite/force 失败绝不能被吞成"可另选"，
     *       否则掩盖盘故障）。</li>
     * </ul>
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code flushVictim} 成功完成其命名的受控动作并发布结果时为 {@code true}；未命中、未执行或状态竞争失败时为 {@code false}
     */
    @Override
    public boolean flushVictim(PageId pageId) {
        FlushResult result = coordinator.singlePageFlush(pageId);
        return switch (result.status()) {
            case CLEAN -> true;
            case KEPT_DIRTY, SKIPPED_NOT_DIRTY, SKIPPED_REDO_NOT_DURABLE -> false;
            case FAILED -> throw result.failure();
        };
    }
}
