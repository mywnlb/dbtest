package cn.zhangyis.db.common.exception;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * 数据库异常严重度分类器。跨 facade 包装不得把 {@link DatabaseFatalException} 降级为可重试运行时错误；本类沿 cause
 * 与 suppressed 图按对象身份查找 fatal 根因，并防止第三方异常构造环导致分类过程无限循环。
 */
public final class DatabaseFailureClassifier {

    private DatabaseFailureClassifier() {
    }

    /** 返回异常图中的首个 fatal；不存在时返回 null。
     *
     * @param failure 需要分类或包装的原始失败；允许为 {@code null} 表示没有底层 cause，存在时必须保留 cause 与 suppressed 异常图
     * @return {@code findFatal} 未找到或条件不满足时返回 {@code null}；否则返回满足构造不变量的 {@code DatabaseFatalException} 结果
     */
    public static DatabaseFatalException findFatal(Throwable failure) {
        if (failure == null) {
            return null;
        }
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        return findFatal(failure, visited);
    }

    /**
     * 若 failure 内含 fatal，则保持直接 fatal，或用新的 fatal 包住外层上下文；没有 fatal 时返回 null。
     *
     * @param message 跨层边界诊断，包装时保留原异常及其 suppressed 图。
     * @param failure 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     * @return {@code preserveFatal} 未找到或条件不满足时返回 {@code null}；否则返回满足构造不变量的 {@code DatabaseFatalException} 结果
     */
    public static DatabaseFatalException preserveFatal(String message, RuntimeException failure) {
        DatabaseFatalException fatal = findFatal(failure);
        if (fatal == null) {
            return null;
        }
        return fatal == failure ? fatal : new DatabaseFatalException(message, failure);
    }

    private static DatabaseFatalException findFatal(Throwable failure, Set<Throwable> visited) {
        if (!visited.add(failure)) {
            return null;
        }
        if (failure instanceof DatabaseFatalException fatal) {
            return fatal;
        }
        DatabaseFatalException fromCause = failure.getCause() == null
                ? null : findFatal(failure.getCause(), visited);
        if (fromCause != null) {
            return fromCause;
        }
        for (Throwable suppressed : failure.getSuppressed()) {
            DatabaseFatalException found = findFatal(suppressed, visited);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}
