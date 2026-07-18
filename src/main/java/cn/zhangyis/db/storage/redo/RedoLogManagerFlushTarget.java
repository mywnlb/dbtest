package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

/**
 * {@link RedoFlushTarget} 的生产适配器：把后台 redo flusher 的驱动端口委托给 {@link RedoLogManager} 的
 * {@code currentLsn()} / {@code flushedToDiskLsn()} / {@code flush()}。只做转发，不引入刷盘策略或锁拆分。
 */
public final class RedoLogManagerFlushTarget implements RedoFlushTarget {

    /**
     * 本对象持有的 {@code redo} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final RedoLogManager redo;

    /**
     * 创建 {@code RedoLogManagerFlushTarget}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param redo 由组合根提供的 {@code RedoLogManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RedoLogManagerFlushTarget(RedoLogManager redo) {
        if (redo == null) {
            throw new DatabaseValidationException("redo log manager must not be null");
        }
        this.redo = redo;
    }

    @Override
    public Lsn currentLsn() {
        return redo.currentLsn();
    }

    @Override
    public Lsn flushedToDiskLsn() {
        return redo.flushedToDiskLsn();
    }

    @Override
    public Lsn flush() {
        return redo.flush();
    }
}
