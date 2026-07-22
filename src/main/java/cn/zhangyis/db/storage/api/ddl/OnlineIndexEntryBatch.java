package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.page.SearchKey;

import java.util.List;
import java.util.Optional;

/**
 * staged secondary tree 的有界物理视图批次，包含 live 与 delete-marked entry。用于 final 双向验证，返回值
 * 已完全物化，不持页资源。
 *
 * @param entries 按完整 secondary physical key 排序的 entry
 * @param continuation 当前最后一个完整物理键；下一批使用 exclusive lower bound
 * @param complete 已到达不足 limit 的树尾
 */
public record OnlineIndexEntryBatch(List<LogicalRecord> entries,
                                    Optional<SearchKey> continuation,
                                    boolean complete) {

    /** 冻结列表并保护非空批次 continuation 不变量。 */
    public OnlineIndexEntryBatch {
        if (entries == null || continuation == null || !entries.isEmpty() && continuation.isEmpty()) {
            throw new DatabaseValidationException("online index entry batch fields are invalid");
        }
        entries = List.copyOf(entries);
    }
}
