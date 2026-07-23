package cn.zhangyis.db.sql.expression;

import cn.zhangyis.db.sql.parser.SourcePosition;

/**
 * Binder 输出、Optimizer 改写且由物理 residual 保留的不可变表达式根。
 *
 * <p>M3 封闭支持列引用、typed literal、comparison、AND、OR、NOT、null-test 与三值常量。新增表达式种类
 * 必须显式补齐类型校验、规则和执行求值，不能由默认分支静默接受。</p>
 */
public sealed interface BoundExpression permits BoundColumnReference, BoundLiteral,
        BoundComparison, BoundConjunction, BoundDisjunction, BoundNegation,
        BoundNullTest, BoundTruthLiteral {

    /**
     * 返回 Binder 已解析的结果类型。
     *
     * @return exact scalar 或 SQL boolean 类型；始终非 {@code null}
     */
    BoundExpressionType type();

    /**
     * 返回当前表达式可用于诊断的源起始位置。
     *
     * @return 原始 AST 的稳定起始位置；规则生成节点继承被改写表达式的位置
     */
    SourcePosition position();
}
