package cn.zhangyis.db.storage.fil.state;
import cn.zhangyis.db.storage.fil.io.PageStore;


import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * page0 与运行期 registry 共享的表空间生命周期状态。
 *
 * <p>普通 {@link PageStore} 上层准入只接受 {@link #NORMAL} 和 {@link #ACTIVE}；
 * recovery open 可以装载 {@link #CORRUPTED} 或 {@link #TRUNCATING} 供诊断和续作，但不因此允许普通 IO。
 * 本枚举只表达通用状态图，不携带 {@link TablespaceType}，因此“某类型可进入哪些状态”的额外约束仍由
 * create/open/truncate 编排层校验。</p>
 *
 * <p>{@link #persistentCode} 是 page0 生命周期头的一部分，不能使用 ordinal 替代或重新编号。</p>
 */
public enum TablespaceState {
    /** 新元数据尚未完成 page0/文件初始化；不能发布给普通 IO。 */
    EMPTY(0),

    /** GENERAL 等普通表空间完成初始化后的服务状态，registry 允许普通访问。 */
    NORMAL(1),

    /** UNDO 表空间可分配且可访问的服务状态。 */
    ACTIVE(2),

    /** UNDO 文件仍存在但停止普通分配/访问，可等待重新激活或进入截断。 */
    INACTIVE(3),

    /** UNDO 已越过 durable truncate marker；启动恢复必须续作或完成状态裁决。 */
    TRUNCATING(4),

    /** 表空间已持久逻辑丢弃，普通定位按“不存在”处理，不能重新回到服务状态。 */
    DISCARDED(5),

    /** 表空间被判定为不能安全普通访问；只能由恢复/诊断读取，不能自动回到服务状态。 */
    CORRUPTED(6)

    ;

    /**
     * page0 生命周期头使用的稳定编码。该字段是磁盘协议的权威 Java 映射，禁止改为 ordinal
     * 或随声明顺序调整。
     */
    private final int persistentCode;

    /**
     * 绑定生命周期常量与稳定 page0 编码。
     *
     * @param persistentCode v1 生命周期头分配的 0..6 编码
     */
    TablespaceState(int persistentCode) {
        this.persistentCode = persistentCode;
    }

    /**
     * 返回 page-0 持久化状态码。该值是磁盘协议的一部分，不随枚举声明顺序变化。
     *
     * @return 可写入 page0 生命周期头的稳定非负状态码
     */
    public int persistentCode() {
        return persistentCode;
    }

    /**
     * 从 page-0 稳定状态码恢复生命周期状态。
     *
     * @param code 从 page0 生命周期头读取的状态码；当前合法范围为 0..6
     * @return 与稳定编码一一对应的生命周期状态
     * @throws DatabaseValidationException 状态码未知时抛出，调用方应把页头视为损坏或格式不兼容
     */
    public static TablespaceState fromPersistentCode(int code) {
        for (TablespaceState state : values()) {
            if (state.persistentCode == code) {
                return state;
            }
        }
        throw new DatabaseValidationException("unknown persistent tablespace state code: " + code);
    }

    /**
     * 判断表空间状态是否允许流转。该状态机保护普通 IO、DDL 和恢复路径共享的生命周期不变量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>空目标没有领域语义，直接拒绝；相同状态视为幂等发布，对终态也成立。</li>
     *     <li>状态变化按当前源状态白名单裁决；{@code DISCARDED/CORRUPTED} 不允许转向其它状态，
     *     {@code TRUNCATING} 只能完成为 UNDO 服务/停用状态或升级为损坏。</li>
     * </ol>
     *
     * @param next 候选目标状态；为空时返回 {@code false}
     * @return 相同状态或白名单允许的有向边返回 {@code true}，否则返回 {@code false}
     */
    public boolean canTransitTo(TablespaceState next) {
        // 1. null 永远非法；同态发布用于恢复重试和 registry 幂等替换，不改变生命周期。
        if (next == null) {
            return false;
        }
        if (this == next) {
            return true;
        }

        // 2. 只接受显式列出的有向边，防止普通调用方把终态或截断中状态任意恢复为 NORMAL。
        return switch (this) {
            case EMPTY -> next == NORMAL || next == CORRUPTED;
            case NORMAL -> next == ACTIVE || next == INACTIVE || next == DISCARDED || next == CORRUPTED;
            case ACTIVE -> next == INACTIVE || next == TRUNCATING || next == DISCARDED || next == CORRUPTED;
            case INACTIVE -> next == ACTIVE || next == TRUNCATING || next == DISCARDED || next == CORRUPTED;
            case TRUNCATING -> next == ACTIVE || next == INACTIVE || next == CORRUPTED;
            case DISCARDED, CORRUPTED -> false;
        };
    }

    /**
     * 校验状态流转是否合法；非法时抛项目异常，避免调用方绕过状态机直接构造不安全状态。
     *
     * @param next 候选目标状态；不能为空，且必须是 {@link #canTransitTo(TablespaceState)} 接受的边
     * @throws DatabaseValidationException 目标为空或状态图拒绝该迁移时抛出；源状态保持不变，
     *                                     调用方不得发布目标 registry/page0 状态
     */
    public void validateTransitTo(TablespaceState next) {
        if (!canTransitTo(next)) {
            throw new DatabaseValidationException("illegal tablespace state transition: " + this + " -> " + next);
        }
    }
}
