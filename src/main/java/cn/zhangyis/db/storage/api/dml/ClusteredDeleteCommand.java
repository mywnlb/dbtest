package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.trx.Transaction;

import java.time.Duration;
import java.util.Optional;

/**
 * 单聚簇索引 DELETE 输入。DELETE 只做 delete-mark，物理删除由 purge driver 在提交后按 history list 边界执行。
 *
 * @param transaction     调用方显式持有的数据库事务；不能为 null。
 * @param index           聚簇索引快照；必须是 clustered index。
 * @param key             被删除记录的聚簇 key。
 * @param tableId         undo record 的表 id；当前无 DD，调用方显式传入且必须非负。
 * @param lobSegment      exact DD table binding 的可选 LOB segment；旧行含 external value 时必须存在。
 * @param lockWaitTimeout current-read FOR UPDATE 的等待上限，必须为正。
 */
public record ClusteredDeleteCommand(Transaction transaction, BTreeIndex index, SearchKey key,
                                     long tableId, Optional<SegmentRef> lobSegment,
                                     Duration lockWaitTimeout) {

    /**
     * 校验聚簇 DELETE 在 current-read 前的事务、索引、主键和 LOB binding 容器。
     *
     * @param transaction     执行 delete-mark 的事务。
     * @param index           exact-version 聚簇索引 descriptor。
     * @param key             完整聚簇主键。
     * @param tableId         undo target 稳定表 id；必须非负。
     * @param lobSegment      可选 authoritative LOB segment 容器；不能为 {@code null}。
     * @param lockWaitTimeout FOR_UPDATE current-read 的正等待上限。
     * @throws DatabaseValidationException 公共字段或 LOB 容器无效时抛出；失败不访问页或创建 undo。
     */
    public ClusteredDeleteCommand {
        ClusteredInsertCommand.validateCommon(transaction, index, key, tableId, lockWaitTimeout, "delete");
        if (lobSegment == null) {
            throw new DatabaseValidationException("clustered delete LOB segment container must not be null");
        }
    }

    /**
     * 兼容 LOB purge ownership 接线前的旧调用，明确映射为空 LOB binding。
     *
     * @param transaction     执行删除的事务。
     * @param index           聚簇索引 descriptor。
     * @param key             完整聚簇主键。
     * @param tableId         稳定表 id。
     * @param lockWaitTimeout current-read 等待上限。
     * @throws DatabaseValidationException 任一旧签名字段无效时由主构造器抛出。
     */
    public ClusteredDeleteCommand(Transaction transaction, BTreeIndex index, SearchKey key,
                                  long tableId, Duration lockWaitTimeout) {
        this(transaction, index, key, tableId, Optional.empty(), lockWaitTimeout);
    }
}
