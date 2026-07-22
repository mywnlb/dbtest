package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;

import java.nio.file.Path;

/** Online shadow rebuild 在manifest中冻结的新表空间identity和受控规范路径。 */
public record OnlineAlterShadowTarget(SpaceId spaceId, Path path) {

    public OnlineAlterShadowTarget {
        if (spaceId == null || spaceId.value() <= 0 || path == null) {
            throw new DatabaseValidationException("online ALTER shadow target is invalid");
        }
        path = path.toAbsolutePath().normalize();
    }
}
