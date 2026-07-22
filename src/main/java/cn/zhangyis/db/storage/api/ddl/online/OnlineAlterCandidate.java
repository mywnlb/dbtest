package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** 一个DML mutation在同一table-level capture generation内影响的有序多索引候选集合。 */
public record OnlineAlterCandidate(List<OnlineAlterCandidateEntry> entries) {

    public OnlineAlterCandidate {
        if (entries == null || entries.isEmpty()) {
            throw new DatabaseValidationException("online ALTER candidate must contain entries");
        }
        entries = List.copyOf(entries);
        int previousOrdinal = -1;
        Set<Long> indexIds = new HashSet<>();
        for (OnlineAlterCandidateEntry entry : entries) {
            if (entry == null || entry.actionOrdinal() <= previousOrdinal
                    || !indexIds.add(entry.indexId())) {
                throw new DatabaseValidationException(
                        "online ALTER candidate entries must be unique and manifest ordered");
            }
            previousOrdinal = entry.actionOrdinal();
        }
    }
}
