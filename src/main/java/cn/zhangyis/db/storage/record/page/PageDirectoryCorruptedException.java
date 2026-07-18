package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 页内结构损坏（innodb-record-design §15 PageDirectoryCorrupted）：目录槽越界、next_record 链成环或越出页体、
 * page header direction code 未知、系统记录类型不符等。属可恢复异常——由上层标记该 page 需 recovery 检查，不静默修复。
 */
public class PageDirectoryCorruptedException extends DatabaseRuntimeException {

    /**
     * 创建 {@code PageDirectoryCorruptedException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public PageDirectoryCorruptedException(String message) {
        super(message);
    }

    /**
     * 创建 {@code PageDirectoryCorruptedException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public PageDirectoryCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
