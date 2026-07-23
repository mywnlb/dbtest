package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.RecoveryExportWriteRejectedException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.concurrent.atomic.AtomicReference;

/**
 * StorageEngine 的统一写准入闸门。启动恢复与 DatabaseEngine DDL 收敛期间允许内部写；完成钩子返回后不可逆地
 * 封为 NORMAL 或 EXPORT_READ_ONLY，使任何遗漏的上层 AST 检查仍会在首个写 MTR 前失败。
 */
public final class StorageWriteAdmission {

    /** 写闸门的单向生命周期。 */
    public enum Mode {
        /** 组合根尚未开始或已经关闭，不允许新写。 */
        CLOSED,
        /** crash/DDL recovery 单线程内部阶段；此时尚未发布用户入口。 */
        RECOVERY_INTERNAL,
        /** 普通与降级实例的生产读写模式。 */
        NORMAL,
        /** FORCE 导出实例的永久只读模式。 */
        EXPORT_READ_ONLY
    }

    /** 原子发布的权威模式；只允许 RECOVERY_INTERNAL 向一个终态单向转换。 */
    private final AtomicReference<Mode> mode;

    /** @param initial 构造时的显式初始准入状态 */
    public StorageWriteAdmission(Mode initial) {
        if (initial == null) {
            throw new DatabaseValidationException("storage write admission mode must not be null");
        }
        this.mode = new AtomicReference<>(initial);
    }

    /** @return 用于低层独立组件测试的普通写准入。 */
    public static StorageWriteAdmission normal() {
        return new StorageWriteAdmission(Mode.NORMAL);
    }

    /** @return 用于真实 StorageEngine bootstrap 的恢复内部准入。 */
    public static StorageWriteAdmission recoveryInternal() {
        return new StorageWriteAdmission(Mode.RECOVERY_INTERNAL);
    }

    /**
     * 在分配 redo 预算、取得页 latch 或文件 lease 前检查写准入。
     *
     * @throws RecoveryExportWriteRejectedException 闸门已封为导出只读时抛出，调用方不得重试为其它写入口
     * @throws EngineStateException 闸门关闭时抛出，调用方应停止当前生命周期操作
     */
    public void assertWriteAllowed() {
        Mode current = mode.get();
        if (current == Mode.EXPORT_READ_ONLY) {
            throw new RecoveryExportWriteRejectedException(
                    "storage write is rejected in recovery export read-only mode");
        }
        if (current == Mode.CLOSED) {
            throw new EngineStateException("storage write admission is closed");
        }
    }

    /**
     * recovery completion 成功后不可逆地发布普通或导出只读终态。
     *
     * @param target 只能是 NORMAL 或 EXPORT_READ_ONLY
     */
    public void seal(Mode target) {
        if (target != Mode.NORMAL && target != Mode.EXPORT_READ_ONLY) {
            throw new DatabaseValidationException("storage write admission target must be a serving mode");
        }
        if (!mode.compareAndSet(Mode.RECOVERY_INTERNAL, target)) {
            throw new EngineStateException("storage write admission cannot seal from " + mode.get());
        }
    }

    /** 关闭准入；close 幂等调用不重新开放已封实例。 */
    public void close() {
        mode.set(Mode.CLOSED);
    }

    /** @return 当前原子发布的准入模式。 */
    public Mode mode() {
        return mode.get();
    }
}
