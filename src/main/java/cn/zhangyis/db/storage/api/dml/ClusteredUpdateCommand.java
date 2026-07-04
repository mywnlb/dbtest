package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.trx.Transaction;

import java.time.Duration;

/**
 * 单聚簇索引 UPDATE 输入。facade 第一阶段只支持不改变聚簇 key 的整行替换；
 * 若底层 B+Tree 检测到聚簇 key 变化，会作为不支持结构抛出领域异常。
 *
 * @param transaction     调用方显式持有的数据库事务；不能为 null。
 * @param index           聚簇索引快照；必须是 clustered index。
 * @param key             被更新记录的聚簇 key。
 * @param newRecord       新完整用户行；隐藏列由 facade 用 update undo roll pointer 盖入。
 * @param tableId         undo record 的表 id；当前无 DD，调用方显式传入且必须非负。
 * @param lockWaitTimeout current-read FOR UPDATE 的等待上限，必须为正。
 */
public record ClusteredUpdateCommand(Transaction transaction, BTreeIndex index, SearchKey key,
                                     LogicalRecord newRecord, long tableId, Duration lockWaitTimeout) {

    public ClusteredUpdateCommand {
        ClusteredInsertCommand.validateCommon(transaction, index, key, tableId, lockWaitTimeout, "update");
        if (newRecord == null) {
            throw new DatabaseValidationException("clustered update newRecord must not be null");
        }
    }
}
