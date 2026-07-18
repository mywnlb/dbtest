package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ObjectName;

import java.time.ZoneId;
import java.util.Optional;

/** 单次绑定上下文；metadata scope 拥有 deadline/MDL owner，Binder 只消费当前 schema 和时区语义。
 *
 * @param currentSchema 可选的 {@code currentSchema}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
 * @param zoneId 参与 {@code 构造} 的稳定领域标识 {@code ZoneId}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param metadataScope SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
 */
public record SqlBindingContext(Optional<ObjectName> currentSchema, ZoneId zoneId,
                                StatementBindingScope metadataScope) {
    public SqlBindingContext {
        if (currentSchema == null || zoneId == null || metadataScope == null) {
            throw new DatabaseValidationException("binding schema optional, zone and metadata scope are required");
        }
    }
}
