package cn.zhangyis.db.storage.api.undotruncate;

/** 测试故障注入抛出的可识别异常；生产默认 injector 不会创建它。 */
public final class SimulatedTruncationCrashException extends UndoTablespaceTruncationException {

    /**
     * 创建 {@code SimulatedTruncationCrashException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public SimulatedTruncationCrashException(String message) {
        super(message);
    }
}
