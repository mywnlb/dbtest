package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.trx.Transaction;

import java.time.Duration;

/**
 * 表级 UPDATE 命令；聚簇主键 v1 不允许变化，服务会用 current-read 物化旧行后再次核对主键。
 *
 * @param transaction     执行更新的 ACTIVE 事务。
 * @param metadata        目标表 exact schema-version 的全部索引快照。
 * @param clusterKey      定位旧聚簇行的完整物化主键，也是事务行锁和 row guard identity。
 * @param newRecord       更新后的完整用户行；必须与目标 schema 匹配且不能携带隐藏列。
 * @param lockWaitTimeout 聚簇行锁、二级唯一锁及短物理 row guard 的最大等待时长；必须为正值。
 */
public record TableUpdateCommand(Transaction transaction, TableIndexMetadata metadata,
                                 SearchKey clusterKey, LogicalRecord newRecord,
                                 Duration lockWaitTimeout) {

    /**
     * 校验更新命令在 current-read 前具备有效事务、metadata、主键和新行形状。
     *
     * @param transaction     执行更新的事务，不能为 {@code null}。
     * @param metadata        目标表索引聚合，不能为 {@code null}。
     * @param clusterKey      完整聚簇搜索键，不能为 {@code null}。
     * @param newRecord       新完整行，不能为 {@code null}，且不得预置聚簇隐藏列。
     * @param lockWaitTimeout 有界等待时长，不能为 {@code null}、零或负数。
     * @throws DatabaseValidationException 字段缺失、等待时长无效或新行与 exact-version schema 不匹配时抛出。
     */
    public TableUpdateCommand {
        if (transaction == null || metadata == null || clusterKey == null || newRecord == null
                || lockWaitTimeout == null || lockWaitTimeout.isZero() || lockWaitTimeout.isNegative()) {
            throw new DatabaseValidationException("table update command fields are invalid");
        }
        if (newRecord.schemaVersion() != metadata.schemaVersion()
                || newRecord.columnValues().size() != metadata.clusteredIndex().schema().columnCount()
                || newRecord.hiddenColumns() != null) {
            throw new DatabaseValidationException("table update record does not match table metadata");
        }
    }
}
