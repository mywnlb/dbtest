package cn.zhangyis.db.storage.api.ddl;

/**
 * shadow tablespace 已完成基础 CREATE、但行复制或索引重建失败。
 *
 * <p>异常携带尚未发布的完整 binding，使持有 durable DDL marker 的 DD coordinator 能在同一语句失败路径
 * 精确回收目标空间并把 marker 终结为 {@code ROLLED_BACK}。storage 层不能自行吞掉 binding 后再删除，
 * 否则调用方无法区分“已安全删除”和“删除结果不确定”，可能错误终结恢复证据。</p>
 */
public final class TableRebuildException extends TableDdlStorageException {

    /** 已完成基础物理 CREATE、但从未被 committed DD 引用的 shadow binding。 */
    private final TableStorageBinding shadowBinding;

    /**
     * 包装 rebuild 失败并转移 shadow cleanup 责任。
     *
     * @param message 包含目标 table/space 的诊断信息；不得为空白
     * @param shadowBinding 已创建且尚未发布到 DD 的目标 binding；不得为 {@code null}
     * @param cause 行投影、聚簇插入、二级索引构建或最终 force 的原始失败；不得为 {@code null}
     */
    public TableRebuildException(
            String message, TableStorageBinding shadowBinding, Throwable cause) {
        super(message, cause);
        if (shadowBinding == null) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "table rebuild failure requires shadow binding");
        }
        this.shadowBinding = shadowBinding;
    }

    /**
     * 返回必须由 durable DDL coordinator 精确回收的未发布空间。
     *
     * @return 基础 CREATE 已完成的 shadow binding；永不为空，且不能作为 ACTIVE 表发布
     */
    public TableStorageBinding shadowBinding() {
        return shadowBinding;
    }
}
