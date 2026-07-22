package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ReadViewRetentionBarrier;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTableGate;
import cn.zhangyis.db.storage.engine.OnlineDdlConfig;
import cn.zhangyis.db.storage.fil.online.OnlineAlterChangeLogFiles;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

/**
 * DatabaseEngine注入通用Online ALTER coordinator的运行期组合。存在本对象表示multi-index sidecar与
 * shadow identity capture均可用；低层兼容构造不注入时selector必须继续阻塞降级。
 *
 * @param gate DML/COMMIT/XA/DDL共享的唯一table gate
 * @param config journal容量、scan/reconciliation批次与terminal reserve
 * @param logFiles 通用manifest+journal受控文件工厂
 * @param typeRegistry 与source record/B+Tree共享的稳定类型codec
 * @param readViewBarrier shadow final quiescence使用的live ReadView generation屏障
 */
public record OnlineAlterRuntime(OnlineDdlTableGate gate,
                                 OnlineDdlConfig config,
                                 OnlineAlterChangeLogFiles logFiles,
                                 TypeCodecRegistry typeRegistry,
                                 ReadViewRetentionBarrier readViewBarrier) {

    /** 冻结组合根依赖，拒绝只启用部分协议的运行期对象。 */
    public OnlineAlterRuntime {
        if (gate == null || config == null || logFiles == null
                || typeRegistry == null || readViewBarrier == null) {
            throw new DatabaseValidationException(
                    "online ALTER runtime collaborators must not be null");
        }
    }
}
