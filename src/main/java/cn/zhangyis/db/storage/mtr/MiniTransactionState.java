package cn.zhangyis.db.storage.mtr;

/**
 * mini-transaction 生命周期状态（设计 §9.1）。NEW 构造态、ACTIVE 活跃态、COMMITTING 提交中、
 * COMMITTED/ROLLED_BACK 终态。COMMITTING 在释放失败时为不可复用半终态（§17）。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code NEW}：表示“NEW”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code ACTIVE}：表示“ACTIVE”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code COMMITTING}：表示“COMMITTING”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code COMMITTED}：表示“COMMITTED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code ROLLED_BACK}：表示“ROLLEDBACK”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 * </ul>
 */
public enum MiniTransactionState {
    NEW,
    ACTIVE,
    COMMITTING,
    COMMITTED,
    ROLLED_BACK;

    /**
     * 判断是否允许流转到 next。保护 begin/commit/rollback 共享的状态不变量。
     *
     * @param next 目标状态。
     * @return 允许返回 true。
     */
    public boolean canTransitTo(MiniTransactionState next) {
        if (next == null) {
            return false;
        }
        return switch (this) {
            case NEW -> next == ACTIVE;
            case ACTIVE -> next == COMMITTING || next == ROLLED_BACK;
            case COMMITTING -> next == COMMITTED;
            case COMMITTED, ROLLED_BACK -> false;
        };
    }

    /**
     * 校验流转合法，非法抛 MtrStateException，避免绕过状态机构造不安全状态。
     *
     * @param next 目标状态。
     * @throws MtrStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public void validateTransitTo(MiniTransactionState next) {
        if (!canTransitTo(next)) {
            throw new MtrStateException("illegal mini transaction state transition: " + this + " -> " + next);
        }
    }
}
