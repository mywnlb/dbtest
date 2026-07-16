package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.dd.domain.TableDefinition;

/** 只用于确定性崩溃点测试；生产恒用 NO_OP，不参与正常 DDL 决策。 */
@FunctionalInterface
public interface DictionaryDdlFaultInjector {
    DictionaryDdlFaultInjector NO_OP = pending -> { };

    /** DROP_PENDING 已 durable、尚未撤销 cache/开始物理删除的崩溃点。 */
    void afterDropPendingPublished(TableDefinition pending);
}
