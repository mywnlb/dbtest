package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.trx.Transaction;

import java.time.Duration;
import java.util.Optional;

/**
 * 表级 DELETE 命令；聚簇记录先 delete-mark，二级 entry 同步 mark，物理回收留给 purge。
 *
 * @param transaction     执行删除的 ACTIVE 事务；事务 write id 同时写入聚簇隐藏列和 undo owner。
 * @param metadata        目标表 exact schema-version 的全部索引快照；聚簇与二级布局必须来自同一 DD binding。
 * @param clusterKey      经过聚簇类型系统物化的完整主键；用于 current-read、事务行锁和 DML/purge row guard。
 * @param lockWaitTimeout 聚簇行锁与短物理 row guard 的最大等待时长；必须为正值。
 */
public record TableDeleteCommand(Transaction transaction, TableIndexMetadata metadata,
                                 SearchKey clusterKey, Optional<SegmentRef> lobSegment,
                                 Duration lockWaitTimeout) {

    /**
     * 校验删除命令在进入任何事务锁或页访问前具备完整上下文。
     *
     * @param transaction     执行删除的事务，不能为 {@code null}。
     * @param metadata        目标表的 exact-version 索引聚合，不能为 {@code null}。
     * @param clusterKey      完整聚簇主键，不能为 {@code null}。
     * @param lockWaitTimeout 有界等待时长，不能为 {@code null}、零或负数。
     * @throws DatabaseValidationException 任一字段缺失或等待时长无效时抛出；失败不产生锁、MTR 或 redo 副作用。
     */
    public TableDeleteCommand {
        if (transaction == null || metadata == null || clusterKey == null || lobSegment == null
                || lockWaitTimeout == null || lockWaitTimeout.isZero() || lockWaitTimeout.isNegative()) {
            throw new DatabaseValidationException("table delete command fields are invalid");
        }
        if (clusterKey.size() != metadata.clusteredIndex().keyDef().parts().size()) {
            throw new DatabaseValidationException("table delete command requires a complete clustered key");
        }
    }

    public TableDeleteCommand(Transaction transaction, TableIndexMetadata metadata,
                              SearchKey clusterKey, Duration lockWaitTimeout) {
        this(transaction, metadata, clusterKey, Optional.empty(), lockWaitTimeout);
    }
}
