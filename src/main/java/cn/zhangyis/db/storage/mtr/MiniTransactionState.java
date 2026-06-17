package cn.zhangyis.db.storage.mtr;

/**
 * mini-transaction 生命周期状态（设计 §9.1）。NEW 构造态、ACTIVE 活跃态、COMMITTING 提交中、
 * COMMITTED/ROLLED_BACK 终态。COMMITTING 在释放失败时为不可复用半终态（§17）。
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
     */
    public void validateTransitTo(MiniTransactionState next) {
        if (!canTransitTo(next)) {
            throw new MtrStateException("illegal mini transaction state transition: " + this + " -> " + next);
        }
    }
}
