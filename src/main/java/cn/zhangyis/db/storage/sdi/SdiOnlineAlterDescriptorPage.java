package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.SegmentRef;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** 一张DDL_DESCRIPTOR页的完整owner、chain link和有序entry视图。 */
public record SdiOnlineAlterDescriptorPage(long ddlOperationId,
                                           long targetDictionaryVersion,
                                           long tableId,
                                           long generation,
                                           SegmentRef descriptorSegment,
                                           int pageOrdinal,
                                           long nextPageNo,
                                           List<SdiOnlineAlterDescriptorEntry> entries) {

    public SdiOnlineAlterDescriptorPage {
        if (ddlOperationId <= 0 || targetDictionaryVersion <= 0 || tableId <= 0 || generation <= 0
                || descriptorSegment == null || pageOrdinal < 0 || nextPageNo < 0 || entries == null) {
            throw new DatabaseValidationException("invalid online ALTER descriptor page");
        }
        entries = List.copyOf(entries);
        Set<Integer> actionOrdinals = new HashSet<>();
        Set<Long> indexIds = new HashSet<>();
        for (SdiOnlineAlterDescriptorEntry entry : entries) {
            if (entry == null || !actionOrdinals.add(entry.actionOrdinal())
                    || !indexIds.add(entry.indexBinding().indexId())) {
                throw new DatabaseValidationException(
                        "online ALTER descriptor page has duplicate/null entry");
            }
            entry.requireSpace(descriptorSegment.spaceId());
        }
    }
}
