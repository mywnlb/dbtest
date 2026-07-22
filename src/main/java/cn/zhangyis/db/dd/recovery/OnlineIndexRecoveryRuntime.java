package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.engine.OnlineDdlConfig;
import cn.zhangyis.db.storage.fil.online.OnlineIndexChangeLogFiles;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

/**
 * 启动期 Online ADD INDEX 同步恢复依赖。恢复发生在用户流量开放前，不需要 live table gate，但必须复用相同
 * scan 配置、受控日志目录和 record/collation 语义。
 *
 * @param config row-log/scan 配置
 * @param logFiles 受 EngineConfig baseDir 约束的文件工厂
 * @param typeRegistry StorageEngine 本次 open 已初始化的类型 registry
 */
public record OnlineIndexRecoveryRuntime(OnlineDdlConfig config,
                                         OnlineIndexChangeLogFiles logFiles,
                                         TypeCodecRegistry typeRegistry) {

    public OnlineIndexRecoveryRuntime {
        if (config == null || logFiles == null || typeRegistry == null) {
            throw new DatabaseValidationException("online index recovery runtime must be complete");
        }
    }
}
