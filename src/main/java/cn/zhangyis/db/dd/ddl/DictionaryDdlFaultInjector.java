package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.dd.domain.TableDefinition;

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
}
