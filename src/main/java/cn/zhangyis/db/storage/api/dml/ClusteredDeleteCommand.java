package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.trx.Transaction;

import java.time.Duration;

/**
 * 单聚簇索引 DELETE 输入。DELETE 只做 delete-mark，物理删除由 purge driver 在提交后按 history list 边界执行。
 *
 * @param transaction     调用方显式持有的数据库事务；不能为 null。
 * @param index           聚簇索引快照；必须是 clustered index。
 * @param key             被删除记录的聚簇 key。
 * @param tableId         undo record 的表 id；当前无 DD，调用方显式传入且必须非负。
 * @param lockWaitTimeout current-read FOR UPDATE 的等待上限，必须为正。
 */
public record ClusteredDeleteCommand(Transaction transaction, BTreeIndex index, SearchKey key,
                                     long tableId, Duration lockWaitTimeout) {

    public ClusteredDeleteCommand {
        ClusteredInsertCommand.validateCommon(transaction, index, key, tableId, lockWaitTimeout, "delete");
    }
}
