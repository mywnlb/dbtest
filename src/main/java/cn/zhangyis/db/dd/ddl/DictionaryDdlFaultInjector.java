package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.storage.api.ddl.SecondaryIndexDropDescriptor;

import java.util.List;

/**
 * 只用于确定性崩溃点测试；生产恒用 NO_OP，不参与正常 DDL 决策。
 *
 * <p>保留单个抽象 DROP_PENDING 接缝以兼容既有 lambda，新增阶段均为 default no-op。</p>
 */
@FunctionalInterface
public interface DictionaryDdlFaultInjector {
    /** 生产默认接缝；所有阶段都保持无副作用。 */
    DictionaryDdlFaultInjector NO_OP = pending -> { };

    /**
     * DROP_PENDING 已 durable、尚未撤销 cache/开始物理删除的崩溃点。
     *
     * @param pending 已提交且携带物理 binding 的 DROP_PENDING 表版本。
     */
    void afterDropPendingPublished(TableDefinition pending);

    /**
     * CREATE PREPARED marker 已 durable、尚未创建物理表空间。
     *
     * @param prepared 当前 CREATE 的稳定 identity/path marker。
     */
    default void afterCreatePrepared(DdlLogRecord prepared) {
    }

    /**
     * CREATE 物理结构与 ENGINE_DONE marker 已 durable、尚未发布 ACTIVE DD。
     *
     * @param engineDone 已证明物理 CREATE 完成的 marker。
     */
    default void afterCreateEngineDone(DdlLogRecord engineDone) {
    }

    /**
     * CREATE ACTIVE DD 与 DICTIONARY_COMMITTED marker 已 durable、尚未发布 cache/terminal marker。
     *
     * @param active 已提交、可供恢复核对 binding 的 ACTIVE 表版本。
     */
    default void afterCreateDictionaryCommitted(TableDefinition active) {
    }

    /**
     * CREATE INDEX PREPARED marker 已 durable、尚未 staged segment/root/footer。
     *
     * @param prepared 同时携带 table 与 secondary index identity 的 marker
     */
    default void afterCreateIndexPrepared(DdlLogRecord prepared) {
    }

    /**
     * Online CREATE INDEX descriptor、GENERATION_STARTED/CAPTURING 与 runtime target 已发布，base scan 尚未开始。
     *
     * @param prepared phase 仍为 PREPARED、但 auxiliary row-log 与 page3 descriptor 都已 durable 的 marker
     */
    default void afterCreateIndexCaptureDurable(DdlLogRecord prepared) {
    }

    /**
     * 二级 B+Tree backfill 与 ENGINE_DONE marker 已 durable、尚未写 SDI/发布新 DD。
     *
     * @param engineDone 可由恢复回滚 staged segments 的 marker
     */
    default void afterCreateIndexEngineDone(DdlLogRecord engineDone) {
    }

    /**
     * 新 ACTIVE table aggregate 与 DICTIONARY_COMMITTED marker 已 durable、footer 尚未清理。
     *
     * @param active 已包含新 index definition/binding 的 committed 表版本
     */
    default void afterCreateIndexDictionaryCommitted(TableDefinition active) {
    }

    /**
     * DROP INDEX PREPARED marker 已 durable、尚未写 page3 DROP descriptor。
     *
     * @param prepared 同时携带 table 与待删除 index identity 的 marker
     */
    default void afterDropIndexPrepared(DdlLogRecord prepared) {
    }

    /**
     * DROP descriptor 已 durable、旧 DD 仍引用目标索引的崩溃点。
     *
     * @param descriptor 可由恢复 exact-CAS 回滚、但此时禁止释放 segment 的物理所有权
     */
    default void afterDropIndexStaged(SecondaryIndexDropDescriptor descriptor) {
    }

    /**
     * Online DROP的retirement fence与FORWARD_ONLY均已durable，但source DD尚未替换的崩溃点。
     *
     * @param forwardFenced 携带不可变fence且control为FORWARD_ONLY的PREPARED marker
     */
    default void afterDropIndexForwardFenced(DdlLogRecord forwardFenced) {
    }

    /**
     * 不含目标索引的新 DD 已 durable，但 marker 仍可能停在 PREPARED 的崩溃点。
     *
     * @param active 新字典版本的 ACTIVE table aggregate
     */
    default void afterDropIndexDictionaryPublished(TableDefinition active) {
    }

    /**
     * DROP INDEX DICTIONARY_COMMITTED marker 已 durable、尚未回收 segment。
     *
     * @param active 不再包含目标索引的 committed table aggregate
     */
    default void afterDropIndexDictionaryCommitted(TableDefinition active) {
    }

    /**
     * 两个索引 segment 与 descriptor 已共同回收、ENGINE_DONE marker 已 durable。
     *
     * @param engineDone 等待 terminal COMMITTED 的 DROP_INDEX marker
     */
    default void afterDropIndexEngineDone(DdlLogRecord engineDone) {
    }

    /**
     * DROP PREPARED marker 已 durable、尚未建立 cache barrier/发布 DROP_PENDING。
     *
     * @param prepared 当前 DROP 的稳定 identity/binding marker。
     */
    default void afterDropPrepared(DdlLogRecord prepared) {
    }

    /**
     * DROP 物理文件与 ENGINE_DONE marker 已收敛、尚未发布 DROPPED DD。
     *
     * @param engineDone 已证明物理 DROP 完成的 marker。
     */
    default void afterDropEngineDone(DdlLogRecord engineDone) {
    }

    /**
     * DROP 的 DROPPED DD 已 durable、尚未写入 COMMITTED marker。
     *
     * @param dropped 已提交、普通 lookup 永久不可见的 DROPPED 表版本。
     */
    default void afterDropDictionaryCommitted(TableDefinition dropped) {
    }

    /**
     * v5 批量 DROP 的完整 PREPARED marker 已 durable、任何目标仍为 ACTIVE 的崩溃点。
     *
     * @param prepared 携带完整排序 manifest 的批量 marker
     */
    default void afterBatchDropPrepared(DdlLogRecord prepared) {
    }

    /**
     * 全体目标已在同一 DD transaction 中进入 DROP_PENDING、物理删除尚未开始的崩溃点。
     *
     * @param pending 与 marker manifest 一一对应的完整 pending 集合；DROP 空 schema 时为空
     */
    default void afterBatchDropPending(List<TableDefinition> pending) {
    }

    /**
     * 全部物理文件已删除且 ENGINE_DONE durable、最终 tombstone transaction 尚未提交的崩溃点。
     *
     * @param engineDone 保留原 manifest 的 ENGINE_DONE marker
     */
    default void afterBatchDropEngineDone(DdlLogRecord engineDone) {
    }

    /**
     * schema/table tombstone 原子 transaction 已 durable、terminal marker 尚未写入的崩溃点。
     *
     * @param dropped 与 manifest 对应的完整 table tombstone 集合；DROP 空 schema 时为空
     */
    default void afterBatchDropDictionaryCommitted(
            List<TableDefinition> dropped) {
    }

    /**
     * metadata-only ALTER的target SDI已durable、control仍为OPEN，可由source DD确定性回滚。
     *
     * @param prepared source/target digest已冻结的PREPARED marker
     */
    default void afterInplaceAlterTargetSdi(DdlLogRecord prepared) {
    }

    /**
     * metadata-only ALTER已进入FORWARD_ONLY但target DD尚未发布；恢复必须从target SDI前滚。
     *
     * @param forwardFenced control为FORWARD_ONLY的PREPARED marker
     */
    default void afterInplaceAlterForwardFenced(DdlLogRecord forwardFenced) {
    }

    /**
     * metadata-only target DD与DICTIONARY_COMMITTED均已durable、cache/terminal尚可缺失。
     *
     * @param active target dictionary version的完整ACTIVE aggregate
     */
    default void afterInplaceAlterDictionaryCommitted(TableDefinition active) {
    }

    /**
     * REBUILD PREPARED marker 已 durable、shadow 文件尚未创建。
     *
     * @param prepared 同时携带旧 binding 与新 space/path identity 的 marker
     */
    default void afterAlterPrepared(DdlLogRecord prepared) {
    }

    /**
     * shadow rows/indexes/SDI 与 ENGINE_DONE marker 已 durable、committed DD 仍引用旧 binding。
     *
     * @param engineDone 恢复时可按旧 DD 精确删除 shadow 的 marker
     */
    default void afterAlterEngineDone(DdlLogRecord engineDone) {
    }

    /**
     * 新 table aggregate 与 DICTIONARY_COMMITTED marker 已 durable、旧空间尚未删除。
     *
     * @param active committed DD 已引用 shadow binding 的 ACTIVE 表版本
     */
    default void afterAlterDictionaryCommitted(TableDefinition active) {
    }

    /**
     * 通用 Online ALTER 的 manifest/journal 与 PREPARED marker 已 durable，descriptor 或 shadow 尚未创建。
     *
     * @param prepared 携带通用 auxiliary journal 路径与 source/target digest 的 marker
     */
    default void afterGeneralAlterPrepared(DdlLogRecord prepared) {
    }

    /**
     * 通用 Online ALTER target SDI、物理 target 与 READY frame 已 durable，但 control 仍为 OPEN。
     *
     * @param prepared 仍可由 committed source 确定性回滚的 PREPARED marker
     */
    default void afterGeneralAlterReady(DdlLogRecord prepared) {
    }

    /**
     * 通用 Online ALTER 已赢得 FORWARD_ONLY，但 RECONCILED 尚未 force；恢复必须保留现场并 fail-closed。
     *
     * @param forwardFenced control 为 FORWARD_ONLY 的 PREPARED marker
     */
    default void afterGeneralAlterForwardFenced(DdlLogRecord forwardFenced) {
    }

    /**
     * 通用 Online ALTER 的 RECONCILED 与 ENGINE_DONE 已 durable，committed DD 仍可能是 source。
     *
     * @param engineDone 只允许恢复前滚的 marker
     */
    default void afterGeneralAlterEngineDone(DdlLogRecord engineDone) {
    }

    /**
     * 通用 Online ALTER target DD 与 DICTIONARY_COMMITTED 已 durable，旧资源尚未退休。
     *
     * @param active 已发布完整 target binding 的 ACTIVE aggregate
     */
    default void afterGeneralAlterDictionaryCommitted(TableDefinition active) {
    }
}
