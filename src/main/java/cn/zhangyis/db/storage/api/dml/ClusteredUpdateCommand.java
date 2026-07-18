package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.trx.Transaction;

import java.time.Duration;
import java.util.Optional;

/**
 * 单聚簇索引 UPDATE 输入。facade 第一阶段只支持不改变聚簇 key 的整行替换；
 * 若底层 B+Tree 检测到聚簇 key 变化，会作为不支持结构抛出领域异常。
 *
 * @param transaction     调用方显式持有的数据库事务；不能为 null。
 * @param index           聚簇索引快照；必须是 clustered index。
 * @param key             被更新记录的聚簇 key。
 * @param newRecord       新完整用户行；隐藏列由 facade 用 update undo roll pointer 盖入。
 * @param tableId         undo record 的表 id；当前无 DD，调用方显式传入且必须非负。
 * @param lobSegment      exact DD table binding 的可选 LOB segment；仅在 replacement ownership 存在时消费。
 * @param lockWaitTimeout current-read FOR UPDATE 的等待上限，必须为正。
 */
public record ClusteredUpdateCommand(Transaction transaction, BTreeIndex index, SearchKey key,
                                     LogicalRecord newRecord, long tableId, Optional<SegmentRef> lobSegment,
                                     Duration lockWaitTimeout) {

    /**
     * 校验聚簇 UPDATE 在 current-read 前具备完整事务、索引、目标行和 LOB binding 容器。
     *
     * @param transaction     执行更新的 ACTIVE 事务；状态由 DML facade 在进入锁等待前复核。
     * @param index           exact-version 聚簇索引 descriptor。
     * @param key             定位旧记录的完整聚簇主键。
     * @param newRecord       目标完整用户行；不能预置隐藏列，列形状由 record/B+Tree 继续校验。
     * @param tableId         undo target 的稳定表 id；必须非负。
     * @param lobSegment      可选 authoritative LOB segment 容器；容器本身不能为 {@code null}。
     * @param lockWaitTimeout FOR_UPDATE current-read 的正等待上限。
     * @throws DatabaseValidationException 公共命令字段、目标行或 LOB 容器无效时抛出；失败不产生锁或 MTR。
     */
    public ClusteredUpdateCommand {
        ClusteredInsertCommand.validateCommon(transaction, index, key, tableId, lockWaitTimeout, "update");
        if (newRecord == null || lobSegment == null) {
            throw new DatabaseValidationException("clustered update newRecord/LOB segment must not be null");
        }
    }

    /**
     * 兼容 LOB replacement 接线前的单聚簇调用；明确映射为空 LOB binding，仅允许页内值或未变化 external 引用。
     *
     * @param transaction     执行更新的事务。
     * @param index           聚簇索引 descriptor。
     * @param key             完整聚簇主键。
     * @param newRecord       目标完整用户行。
     * @param tableId         稳定表 id。
     * @param lockWaitTimeout current-read 等待上限。
     * @throws DatabaseValidationException 任一旧签名字段无效时由主构造器抛出。
     */
    public ClusteredUpdateCommand(Transaction transaction, BTreeIndex index, SearchKey key,
                                  LogicalRecord newRecord, long tableId, Duration lockWaitTimeout) {
        this(transaction, index, key, newRecord, tableId, Optional.empty(), lockWaitTimeout);
    }
}
