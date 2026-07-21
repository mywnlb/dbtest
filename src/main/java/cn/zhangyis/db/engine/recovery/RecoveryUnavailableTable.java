package cn.zhangyis.db.engine.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;

/**
 * 对外暴露的恢复隔离对象快照；只含诊断与控制面所需身份，不提供页、索引游标或绕过隔离的物理访问器。
 *
 * @param qualifiedName catalog/schema/table 的规范名称
 * @param tableId 不因隔离、discard 或可信替换而改变的 DD identity
 * @param state 当前 committed 恢复隔离状态
 * @param version 发布该状态的字典版本
 * @param storageBinding 原对象的稳定空间、路径与索引 binding 证据
 */
public record RecoveryUnavailableTable(QualifiedTableName qualifiedName, TableId tableId,
                                       TableState state, DictionaryVersion version,
                                       TableStorageBinding storageBinding) {

    /** 构造时拒绝普通 DDL 状态，避免诊断 API 把健康或半完成对象误报成恢复隔离对象。 */
    public RecoveryUnavailableTable {
        if (qualifiedName == null || tableId == null || state == null || version == null
                || storageBinding == null) {
            throw new DatabaseValidationException("recovery unavailable table fields must not be null");
        }
        if (state != TableState.RECOVERY_UNAVAILABLE && state != TableState.RECOVERY_DISCARDED) {
            throw new DatabaseValidationException("table is not in a recovery isolation state: " + state);
        }
        if (storageBinding.tableId() != tableId.value()) {
            throw new DatabaseValidationException("recovery unavailable table/binding identity mismatch");
        }
    }
}
