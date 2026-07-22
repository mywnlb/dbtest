package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 通用Online ALTER journal offset 0的不可变owner header。shadowSpaceId为0表示INPLACE，正值表示shadow。
 */
public final class OnlineAlterLogHeader {

    /** 文件名与header共同声明的capture owner；不允许通过扫描目录猜测归属。 */
    private final OnlineDdlCaptureId captureId;
    /** 本次journal唯一服务的逻辑表，防止跨表候选记录混入。 */
    private final long tableId;
    /** 初始freeze读取的source DD版本；恢复必须与marker digest交叉验证。 */
    private final long sourceDictionaryVersion;
    /** 原子发布目标DD版本；必须严格大于source版本。 */
    private final long targetDictionaryVersion;
    /** source记录布局版本；candidate解码只能使用该布局。 */
    private final long sourceRowFormatVersion;
    /** target记录布局版本；shadow写入与候选投影使用该布局。 */
    private final long targetRowFormatVersion;
    /** marker稳定protocol code；恢复不能根据文件是否存在反推协议。 */
    private final int executionProtocolCode;
    /** shadow tablespace identity；零表示INPLACE，不允许使用负数或模糊哨兵。 */
    private final int shadowSpaceId;
    /** READY force后持久化的ReadView generation；零只允许表示尚未进入READY。 */
    private final long freezeReadViewGeneration;
    /** 完整versioned manifest规范字节；其digest把header、marker与descriptor chain绑定。 */
    private final byte[] manifest;

    /**
     * 创建journal不可变owner header；header一旦写入便不得随phase推进而改写。
     *
     * @param captureId 文件名和gate登记共同使用的正capture identity
     * @param tableId journal所属正逻辑表identity
     * @param sourceDictionaryVersion initial freeze读取的正source版本
     * @param targetDictionaryVersion 严格大于source的目标版本
     * @param sourceRowFormatVersion candidate解码使用的正source布局版本
     * @param targetRowFormatVersion reconcile写入使用的正target布局版本
     * @param executionProtocolCode marker中持久化的正稳定protocol code
     * @param shadowSpaceId shadow物理space identity；INPLACE必须传零
     * @param freezeReadViewGeneration initial freeze捕获的非负ReadView generation
     * @param manifest 完整非空manifest规范字节；构造器取得独立副本
     * @throws DatabaseValidationException 版本、identity、protocol或manifest形状无法形成恢复不变量时抛出
     */
    public OnlineAlterLogHeader(OnlineDdlCaptureId captureId, long tableId,
                                long sourceDictionaryVersion, long targetDictionaryVersion,
                                long sourceRowFormatVersion, long targetRowFormatVersion,
                                int executionProtocolCode, int shadowSpaceId,
                                long freezeReadViewGeneration, byte[] manifest) {
        if (captureId == null || tableId <= 0 || sourceDictionaryVersion <= 0
                || targetDictionaryVersion <= sourceDictionaryVersion
                || sourceRowFormatVersion <= 0 || targetRowFormatVersion <= 0
                || executionProtocolCode <= 0 || shadowSpaceId < 0
                || freezeReadViewGeneration < 0 || manifest == null || manifest.length == 0) {
            throw new DatabaseValidationException("invalid online ALTER log header");
        }
        this.captureId = captureId;
        this.tableId = tableId;
        this.sourceDictionaryVersion = sourceDictionaryVersion;
        this.targetDictionaryVersion = targetDictionaryVersion;
        this.sourceRowFormatVersion = sourceRowFormatVersion;
        this.targetRowFormatVersion = targetRowFormatVersion;
        this.executionProtocolCode = executionProtocolCode;
        this.shadowSpaceId = shadowSpaceId;
        this.freezeReadViewGeneration = freezeReadViewGeneration;
        this.manifest = manifest.clone();
    }

    public OnlineDdlCaptureId captureId() { return captureId; }
    public long tableId() { return tableId; }
    public long sourceDictionaryVersion() { return sourceDictionaryVersion; }
    public long targetDictionaryVersion() { return targetDictionaryVersion; }
    public long sourceRowFormatVersion() { return sourceRowFormatVersion; }
    public long targetRowFormatVersion() { return targetRowFormatVersion; }
    public int executionProtocolCode() { return executionProtocolCode; }
    public int shadowSpaceId() { return shadowSpaceId; }
    public long freezeReadViewGeneration() { return freezeReadViewGeneration; }
    public byte[] manifest() { return manifest.clone(); }
}
