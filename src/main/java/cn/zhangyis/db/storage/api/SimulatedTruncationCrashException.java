package cn.zhangyis.db.storage.api;

/** 测试故障注入抛出的可识别异常；生产默认 injector 不会创建它。 */
public final class SimulatedTruncationCrashException extends UndoTablespaceTruncationException {

    public SimulatedTruncationCrashException(String message) {
        super(message);
    }
}
