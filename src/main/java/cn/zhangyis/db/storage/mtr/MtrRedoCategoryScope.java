package cn.zhangyis.db.storage.mtr;

/**
 * MTR redo 分类作用域。调用方在执行一小段有明确语义来源的页写入前进入该 scope，scope 内的
 * {@code PAGE_BYTES} 会带上对应 {@link MtrRedoCategory} 诊断标签；关闭后恢复外层分类。
 *
 * <p>该对象必须用 try-with-resources 管理。关闭是幂等的，但不同 scope 必须按 LIFO 关闭，
 * 否则说明调用方把 MTR 本地语义边界打乱，collector 会抛 {@link MtrStateException}。
 */
public final class MtrRedoCategoryScope implements AutoCloseable {

    /** 拥有分类栈的 collector；scope 不直接持有 page guard 或 redo record。 */
    private final MtrRedoCollector owner;

    /** 进入 scope 前的分类，关闭时恢复。 */
    private final MtrRedoCategory previousCategory;

    /** 本 scope 设置的分类。 */
    private final MtrRedoCategory category;

    /** 诊断说明，描述该分类绑定的调用方语义。 */
    private final String reason;

    /** 幂等关闭标志。 */
    private boolean closed;

    /**
     * 创建 {@code MtrRedoCategoryScope}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param owner redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param previousCategory redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param category redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param reason 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     */
    MtrRedoCategoryScope(MtrRedoCollector owner,
                         MtrRedoCategory previousCategory,
                         MtrRedoCategory category,
                         String reason) {
        this.owner = owner;
        this.previousCategory = previousCategory;
        this.category = category;
        this.reason = reason;
    }

    /** 本 scope 设置的分类。 */
    public MtrRedoCategory category() {
        return category;
    }

    /** 进入 scope 前的分类，供 collector 做 LIFO 恢复。 */
    MtrRedoCategory previousCategory() {
        return previousCategory;
    }

    /** 调用方写入的诊断说明。 */
    public String reason() {
        return reason;
    }

    /** 关闭分类作用域并恢复外层分类。 */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        owner.closeCategoryScope(this);
        closed = true;
    }
}
