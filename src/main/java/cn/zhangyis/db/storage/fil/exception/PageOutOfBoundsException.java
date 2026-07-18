package cn.zhangyis.db.storage.fil.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 请求页不在数据文件当前已发布物理范围内的异常。
 *
 * <p>{@code DataFileHandle} 在 {@code pageNo >= currentSizeInPages} 时于计算 IO 偏移前抛出，
 * 因而本次页读写不会触碰 channel。普通页访问不得自行把越界解释为空页；只有具有空间管理或
 * recovery 容量协调职责的上层先通过 {@code extend/ensureCapacity} 建立物理范围后，才可重试。</p>
 */
public class PageOutOfBoundsException extends DatabaseRuntimeException {

    /**
     * 使用页范围诊断信息创建异常。
     *
     * @param message 应包含 SpaceId、请求 pageNo 和 currentSizeInPages
     */
    public PageOutOfBoundsException(String message) {
        super(message);
    }

    /**
     * 使用页范围诊断信息和底层计算原因创建异常。
     *
     * @param message 应包含 SpaceId 与请求/有效边界
     * @param cause 页偏移换算或边界取得失败的原始原因；不得丢弃
     */
    public PageOutOfBoundsException(String message, Throwable cause) {
        super(message, cause);
    }
}
