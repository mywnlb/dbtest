package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * target逻辑发布后延迟回收旧物理资源的持久安全边界。
 *
 * @param tableId operation所属的正table identity
 * @param sourceDictionaryVersion 被退休metadata的正dictionary version
 * @param retireThroughTransactionNo final X下捕获的提交序号高水位；没有既有提交时允许0
 * @param sourceMetadataPinVersion cache pin必须清零的正source version
 * @param descriptorGeneration page3/sidecar owner的正generation
 * @param ownerDdlId 创建descriptor和fence的正DDL identity
 * @param resources 按kind/id严格升序且去重的非空退休资源集合
 */
public record DdlRetirementFence(long tableId,
                                 long sourceDictionaryVersion,
                                 long retireThroughTransactionNo,
                                 long sourceMetadataPinVersion,
                                 long descriptorGeneration,
                                 long ownerDdlId,
                                 List<DdlRetiredResource> resources) {
    /** v4为损坏长度分配设置的显式资源数上限。 */
    public static final int MAX_RESOURCES = 1_024;

    public DdlRetirementFence {
        if (tableId <= 0 || sourceDictionaryVersion <= 0 || retireThroughTransactionNo < 0
                || sourceMetadataPinVersion <= 0 || descriptorGeneration <= 0 || ownerDdlId <= 0
                || resources == null || resources.isEmpty() || resources.size() > MAX_RESOURCES) {
            throw new DatabaseValidationException("invalid DDL retirement fence fields");
        }
        for (DdlRetiredResource resource : resources) {
            if (resource == null) {
                throw new DatabaseValidationException("DDL retirement resources must not contain null");
            }
        }
        resources = List.copyOf(resources);
        for (int index = 1; index < resources.size(); index++) {
            if (resources.get(index - 1).compareTo(resources.get(index)) >= 0) {
                throw new DatabaseValidationException(
                        "DDL retirement resources must be strictly ordered and unique");
            }
        }
    }
}
