package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.page.SearchKey;

import java.util.List;
import java.util.Optional;

/**
 * Online secondary build 的单个聚簇 live-row 批次。rows 已完全物化，不持有 page latch、buffer fix 或 cursor；
 * continuation 是最后一行完整聚簇物理键，下一批必须使用 exclusive lower bound。
 *
 * @param rows 当前批次按聚簇物理键排序的 live rows
 * @param continuation 当前批次最后一行的完整聚簇键；空批次可保留调用方原 continuation
 * @param complete 当前扫描已经观察到不足 limit 的尾批次，调用方无需再请求下一批
 */
public record OnlineIndexScanBatch(List<LogicalRecord> rows,
                                   Optional<SearchKey> continuation,
                                   boolean complete) {

    /** 防御性复制批次；非空批次必须给出 continuation，防止续扫从头重复。 */
    public OnlineIndexScanBatch {
        if (rows == null || continuation == null || !rows.isEmpty() && continuation.isEmpty()) {
            throw new DatabaseValidationException("online index scan batch fields are invalid");
        }
        rows = List.copyOf(rows);
    }
}
