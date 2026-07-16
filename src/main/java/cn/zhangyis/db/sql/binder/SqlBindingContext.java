package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ObjectName;

import java.time.ZoneId;
import java.util.Optional;

/** 单次绑定上下文；metadata scope 拥有 deadline/MDL owner，Binder 只消费当前 schema 和时区语义。 */
public record SqlBindingContext(Optional<ObjectName> currentSchema, ZoneId zoneId,
                                StatementBindingScope metadataScope) {
    public SqlBindingContext {
        if (currentSchema == null || zoneId == null || metadataScope == null) {
            throw new DatabaseValidationException("binding schema optional, zone and metadata scope are required");
        }
    }
}
