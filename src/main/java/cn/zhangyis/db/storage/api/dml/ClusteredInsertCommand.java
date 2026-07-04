package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.trx.Transaction;

import java.time.Duration;

/**
 * 单聚簇索引 INSERT 输入。调用方已经完成 SQL/DD 归一化，facade 只消费显式事务、索引快照、
 * 主键 key 和用户逻辑行；隐藏列由 facade 通过事务 id 与 undo roll pointer 盖入。
 *
 * @param transaction     调用方显式持有的数据库事务；不能为 null。
 * @param index           聚簇索引快照；必须是 clustered index。
 * @param key             主键查找 key；第一阶段信任调用方保证它与 record 的聚簇 key 一致。
 * @param record          用户逻辑行；可以不带隐藏列，写入时会被重新盖戳。
 * @param tableId         undo record 的表 id；当前无 DD，调用方显式传入且必须非负。
 * @param lockWaitTimeout unique current-read / insert-intention 的等待上限，必须为正。
 */
public record ClusteredInsertCommand(Transaction transaction, BTreeIndex index, SearchKey key,
                                     LogicalRecord record, long tableId, Duration lockWaitTimeout) {

    public ClusteredInsertCommand {
        validateCommon(transaction, index, key, tableId, lockWaitTimeout, "insert");
        if (record == null) {
            throw new DatabaseValidationException("clustered insert record must not be null");
        }
    }

    /**
     * 校验 INSERT/UPDATE/DELETE 共用输入边界。该方法集中保护 facade 的第一层不变量：显式事务必须存在、
     * 目标必须是聚簇索引、key/tableId 可用于 undo 与 current-read 定位、锁等待必须有正超时。
     */
    static void validateCommon(Transaction transaction, BTreeIndex index, SearchKey key,
                               long tableId, Duration lockWaitTimeout, String operation) {
        if (transaction == null) {
            throw new DatabaseValidationException("clustered " + operation + " transaction must not be null");
        }
        if (index == null) {
            throw new DatabaseValidationException("clustered " + operation + " index must not be null");
        }
        if (!index.clustered()) {
            throw new DatabaseValidationException("clustered " + operation
                    + " requires clustered index: " + index.indexId());
        }
        if (key == null) {
            throw new DatabaseValidationException("clustered " + operation + " key must not be null");
        }
        if (tableId < 0) {
            throw new DatabaseValidationException("clustered " + operation
                    + " tableId must be non-negative: " + tableId);
        }
        if (lockWaitTimeout == null || lockWaitTimeout.isZero() || lockWaitTimeout.isNegative()) {
            throw new DatabaseValidationException("clustered " + operation
                    + " lockWaitTimeout must be positive");
        }
    }
}
