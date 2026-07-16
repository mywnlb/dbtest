package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * 新 LOB allocation 的 ownership guard 已被跨线程使用、错过 ACTIVE MTR 补偿窗口或补偿失败。此时不能谎报回收
 * 成功，否则可能发布悬空引用或泄漏已提交页链，调用方必须按 outcome-uncertain/fail-stop 处理。
 */
public class LobAllocationStateException extends DatabaseFatalException {

    public LobAllocationStateException(String message) {
        super(message);
    }

    public LobAllocationStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
