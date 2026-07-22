package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.format.LogicalRecord;

import java.util.Optional;

/**
 * 一次 clustered mutation 对 staged secondary tree 可能产生影响的物理 entry 对。INSERT 只有 after，DELETE
 * 只有 before，变键 UPDATE 同时包含两者；两侧不能同时缺失。
 *
 * @param beforeEntry mutation 前的完整 secondary physical entry
 * @param afterEntry mutation 后的完整 secondary physical entry
 */
public record OnlineIndexCandidate(Optional<LogicalRecord> beforeEntry,
                                   Optional<LogicalRecord> afterEntry) {

    /** 防御性固定 Optional 容器并拒绝空操作 candidate。 */
    public OnlineIndexCandidate {
        if (beforeEntry == null || afterEntry == null
                || beforeEntry.isEmpty() && afterEntry.isEmpty()
                || beforeEntry.filter(LogicalRecord::deleted).isPresent()
                || afterEntry.filter(LogicalRecord::deleted).isPresent()) {
            throw new DatabaseValidationException(
                    "online index candidate must contain at least one non-deleted physical entry");
        }
    }
}
