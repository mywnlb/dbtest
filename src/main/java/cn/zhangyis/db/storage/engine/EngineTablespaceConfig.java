package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;

import java.nio.file.Path;

/**
 * 引擎启动恢复期显式打开的数据表空间配置。
 *
 * <p>当前项目尚无 data dictionary / tablespace discovery，恢复服务不能自行发现 file-per-table 或 general
 * tablespace。因此 E2 只接受调用方显式列出的空间：open(existing) 会先按 recovery 准入打开这些文件，再允许
 * {@code CrashRecoveryService} 对其中的物理页执行 redo replay 和 SPACE_FILE_RECONCILE。后续接入 DD 后，
 * 该配置应退化为 bootstrap/discovery 的输入或被字典绑定替代。
 *
 * @param spaceId 表空间编号；必须与 page0 自描述一致，否则恢复期会 fail closed。
 * @param path 表空间数据文件路径；启动恢复期由 {@link StorageEngine} 打开，成功后保持为普通运行句柄。
 */
public record EngineTablespaceConfig(SpaceId spaceId, Path path) {

    public EngineTablespaceConfig {
        if (spaceId == null || path == null) {
            throw new DatabaseValidationException("engine recovery tablespace space/path must not be null");
        }
    }
}
