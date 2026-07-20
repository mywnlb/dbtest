package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 独立字典恢复 manifest 无法持久化或不能安全解释时抛出的领域异常。
 *
 * <p>该异常表示 catalog-loss 恢复证据不可用。普通 catalog 仍有效时，启动协调器可以保留证据并从
 * 权威 catalog 重建 manifest；catalog 已丢失时必须停止恢复，不能跳过该异常从 SDI 猜测目录。</p>
 */
public final class DictionaryRecoveryManifestException extends DatabaseRuntimeException {

    /**
     * 创建只携带诊断上下文的 manifest 异常。
     *
     * @param message 说明失败事件、序号或格式边界的非空诊断文本
     */
    public DictionaryRecoveryManifestException(String message) {
        super(message);
    }

    /**
     * 包装底层 catalog journal、编码或文件系统失败并保留根因。
     *
     * @param message 说明失败事件、序号或格式边界的非空诊断文本
     * @param cause 原始失败；调用方可据此区分持久 IO 与逻辑损坏
     */
    public DictionaryRecoveryManifestException(String message, Throwable cause) {
        super(message, cause);
    }
}
