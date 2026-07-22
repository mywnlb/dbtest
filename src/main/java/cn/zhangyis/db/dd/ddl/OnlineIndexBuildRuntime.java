package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTableGate;
import cn.zhangyis.db.storage.engine.OnlineDdlConfig;
import cn.zhangyis.db.storage.fil.online.OnlineIndexChangeLogFiles;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

/**
 * DatabaseEngine 组合根注入 Dictionary DDL 的 Online ADD INDEX 运行期依赖。存在本对象即表示 CREATE INDEX
 * 必须走 online 生产链；低层孤立测试未注入时保留 legacy blocking 路径以验证旧恢复兼容。
 *
 * @param gate DML/COMMIT/XA/DDL 共用的唯一 table gate
 * @param config row-log 容量、scan batch 与 abort reserve
 * @param logFiles 由 EngineConfig base directory 约束的文件工厂
 * @param typeRegistry 与 record/B+Tree 使用同一类型、prefix 和 collation 语义
 */
public record OnlineIndexBuildRuntime(OnlineDdlTableGate gate,
                                      OnlineDdlConfig config,
                                      OnlineIndexChangeLogFiles logFiles,
                                      TypeCodecRegistry typeRegistry) {

    /** 组合根依赖缺失时拒绝发布半在线服务。 */
    public OnlineIndexBuildRuntime {
        if (gate == null || config == null || logFiles == null || typeRegistry == null) {
            throw new DatabaseValidationException("online index runtime collaborators must not be null");
        }
    }
}
