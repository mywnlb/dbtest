package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * INDEX 页用户记录不满足 schema/keyDef 声明的页内顺序或记录类型约束。
 *
 * <p>该异常与 {@link PageDirectoryCorruptedException} 分层：后者表达 next_record、heap、directory 等物理账本损坏；
 * 本异常表达物理结构仍可遍历，但字段布局、字符编码、record type 或相邻 key 顺序已经不可信。调用方必须停止普通
 * B+Tree 访问并交给恢复/损坏诊断，不能在前台静默重排页面。
 */
public class RecordKeyOrderCorruptedException extends DatabaseRuntimeException {

    /** 创建只包含页内 key 损坏诊断的异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public RecordKeyOrderCorruptedException(String message) {
        super(message);
    }

    /** 创建保留字段解析、charset 或 collation 根因的页内 key 损坏异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public RecordKeyOrderCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
