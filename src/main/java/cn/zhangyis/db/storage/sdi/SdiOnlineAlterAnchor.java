package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** page3 `ALT1/v1` 固定footer视图；descriptor root page保存自己的segment identity。 */
public final class SdiOnlineAlterAnchor {

    private final long ddlOperationId;
    private final long targetDictionaryVersion;
    private final long tableId;
    private final long generation;
    private final long descriptorRootPageNo;
    private final int descriptorCount;
    private final byte[] manifestDigest;

    public SdiOnlineAlterAnchor(long ddlOperationId, long targetDictionaryVersion, long tableId,
                                long generation, long descriptorRootPageNo, int descriptorCount,
                                byte[] manifestDigest) {
        if (ddlOperationId <= 0 || targetDictionaryVersion <= 0 || tableId <= 0 || generation <= 0
                || descriptorRootPageNo <= 0 || descriptorCount < 0
                || manifestDigest == null || manifestDigest.length != 32) {
            throw new DatabaseValidationException("invalid SDI online ALTER anchor");
        }
        this.ddlOperationId = ddlOperationId;
        this.targetDictionaryVersion = targetDictionaryVersion;
        this.tableId = tableId;
        this.generation = generation;
        this.descriptorRootPageNo = descriptorRootPageNo;
        this.descriptorCount = descriptorCount;
        this.manifestDigest = manifestDigest.clone();
    }

    public long ddlOperationId() { return ddlOperationId; }
    public long targetDictionaryVersion() { return targetDictionaryVersion; }
    public long tableId() { return tableId; }
    public long generation() { return generation; }
    public long descriptorRootPageNo() { return descriptorRootPageNo; }
    public int descriptorCount() { return descriptorCount; }
    public byte[] manifestDigest() { return manifestDigest.clone(); }
}
