package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.trx.Transaction;

import java.time.Duration;
import java.util.Optional;

/**
 * 表级 INSERT 命令；完整行由 exact-version layout 投影到聚簇与全部二级 entry。
 *
 * @param transaction     执行插入的 ACTIVE 事务；首次写入时由事务系统分配稳定 write id。
 * @param metadata        目标表 exact schema-version 的全部索引快照。
 * @param record          无隐藏列的完整用户行；列顺序和 schema version 必须与聚簇表 schema 一致。
 * @param lobSegment      可选 LOB segment；仅在完整行含溢出列时由聚簇写入路径消费。
 * @param lockWaitTimeout 聚簇/二级唯一锁及短物理 row guard 的最大等待时长；必须为正值。
 */
public record TableInsertCommand(Transaction transaction, TableIndexMetadata metadata,
                                 LogicalRecord record, Optional<SegmentRef> lobSegment,
                                 Duration lockWaitTimeout) {

    /**
     * 校验插入命令与目标表 schema 的基本一致性。
     *
     * @param transaction     执行插入的事务，不能为 {@code null}。
     * @param metadata        目标表索引聚合，不能为 {@code null}。
     * @param record          待插入完整行，不能为 {@code null}，且不得预置 DB_TRX_ID/DB_ROLL_PTR。
     * @param lobSegment      LOB segment 容器本身不能为 {@code null}。
     * @param lockWaitTimeout 有界等待时长，不能为 {@code null}、零或负数。
     * @throws DatabaseValidationException 字段缺失、schema version/列数错配或调用方伪造隐藏列时抛出。
     */
    public TableInsertCommand {
        if (transaction == null || metadata == null || record == null || lobSegment == null
                || lockWaitTimeout == null || lockWaitTimeout.isZero() || lockWaitTimeout.isNegative()) {
            throw new DatabaseValidationException("table insert command fields are invalid");
        }
        if (record.schemaVersion() != metadata.schemaVersion()
                || record.columnValues().size() != metadata.clusteredIndex().schema().columnCount()
                || record.hiddenColumns() != null) {
            throw new DatabaseValidationException("table insert record does not match table metadata");
        }
    }
}
