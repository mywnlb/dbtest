package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexBuildId;

/**
 * Row-log immutable header 内的 DD 恢复定义。manifest 保存命令本身不会出现在普通 DDL marker 中的索引名称、
 * UNIQUE 属性和 key parts，令 PREPARED crash 可在没有原 SQL 的情况下重建同一 target aggregate。
 *
 * @param buildId 与 DDL marker id 相同的 storage identity
 * @param tableId 旧 committed table aggregate identity
 * @param sourceVersion build 开始时重读的 committed DD version
 * @param targetVersion 本次已预留且最终发布的 DD version
 * @param index 尚未发布的完整 secondary index definition
 */
public record OnlineIndexBuildManifest(OnlineIndexBuildId buildId,
                                       TableId tableId,
                                       DictionaryVersion sourceVersion,
                                       DictionaryVersion targetVersion,
                                       IndexDefinition index) {

    /** 构造时拒绝版本回退、identity 错配和 clustered target。 */
    public OnlineIndexBuildManifest {
        if (buildId == null || tableId == null || sourceVersion == null || targetVersion == null
                || index == null || index.clustered()
                || targetVersion.value() <= sourceVersion.value()) {
            throw new DatabaseValidationException("online index manifest fields are invalid");
        }
    }
}
