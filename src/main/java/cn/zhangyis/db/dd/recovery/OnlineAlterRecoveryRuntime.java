package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.fil.online.OnlineAlterChangeLogFiles;

/**
 * 通用 Online ALTER 启动恢复所需的最小运行期。恢复不重新开放 gate 或执行 base scan，只解释
 * operation-owned journal，并依据 durable control/READY/RECONCILED 证据回滚或前滚。
 *
 * @param logFiles 从 capture id 推导 exact journal 路径并负责打开、删除的受控文件工厂
 */
public record OnlineAlterRecoveryRuntime(OnlineAlterChangeLogFiles logFiles) {

    /**
     * 绑定恢复期 journal owner；不打开文件，也不创建后台资源。
     *
     * @param logFiles 与 live coordinator 使用相同目录和容量配置的文件工厂
     * @throws DatabaseValidationException 协作者缺失时抛出且不发布半初始化 runtime
     */
    public OnlineAlterRecoveryRuntime {
        if (logFiles == null) {
            throw new DatabaseValidationException(
                    "online ALTER recovery log files must not be null");
        }
    }
}
