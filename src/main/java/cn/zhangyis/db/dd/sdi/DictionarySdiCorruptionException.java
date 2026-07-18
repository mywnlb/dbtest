package cn.zhangyis.db.dd.sdi;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * SDI payload 无法完整解释时的领域异常。它与 catalog 损坏不同：启动阶段若 committed DD 完整，
 * 可以丢弃该 payload 并重写；脱离 DD 单独读取时调用方必须把它视为不可用快照。
 */
public final class DictionarySdiCorruptionException extends DatabaseRuntimeException {

    /**
     * 创建不带底层根因的 SDI 格式异常。
     *
     * @param message 具体的格式、边界或聚合不变量错误
     */
    public DictionarySdiCorruptionException(String message) {
        super(message);
    }

    /**
     * 创建保留原始解码或领域构造异常的 SDI 格式异常。
     *
     * @param message 可定位到 SDI payload 阶段的诊断消息
     * @param cause   原始 IO、类型 code 或聚合校验异常
     */
    public DictionarySdiCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
