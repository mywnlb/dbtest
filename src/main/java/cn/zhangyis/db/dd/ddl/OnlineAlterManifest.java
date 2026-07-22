package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.TableId;

import java.util.List;
import java.util.Optional;

/**
 * 通用 Online ALTER 的不可变恢复命令。marker只保存固定identity/digest/control，本对象保存有序动作、
 * row-format和可选shadow目标；storage journal把编码结果当作opaque bytes并用SHA-256绑定。
 */
public record OnlineAlterManifest(long ddlOperationId, TableId tableId,
                                  DictionaryVersion sourceVersion,
                                  DictionaryVersion targetVersion,
                                  DdlExecutionProtocol executionProtocol,
                                  DdlSchemaDigest sourceSchemaDigest,
                                  DdlSchemaDigest targetSchemaDigest,
                                  long sourceRowFormatVersion,
                                  long targetRowFormatVersion,
                                  long freezeReadViewGeneration,
                                  List<OnlineAlterActionDescriptor> actions,
                                  Optional<OnlineAlterShadowTarget> shadowTarget) {

    /** v1对单条ALTER声明动作数的防内存放大上限。 */
    public static final int MAX_ACTIONS = 1_024;

    public OnlineAlterManifest {
        if (ddlOperationId <= 0 || tableId == null || sourceVersion == null || targetVersion == null
                || targetVersion.value() <= sourceVersion.value() || executionProtocol == null
                || sourceSchemaDigest == null || targetSchemaDigest == null
                || sourceRowFormatVersion <= 0 || targetRowFormatVersion <= 0
                || freezeReadViewGeneration < 0 || actions == null || actions.isEmpty()
                || actions.size() > MAX_ACTIONS || shadowTarget == null) {
            throw new DatabaseValidationException("invalid online ALTER manifest fields");
        }
        if (executionProtocol != DdlExecutionProtocol.ONLINE_ALTER_INPLACE_V1
                && executionProtocol != DdlExecutionProtocol.ONLINE_ALTER_SHADOW_V1) {
            throw new DatabaseValidationException("online ALTER manifest protocol is unsupported");
        }
        if ((executionProtocol == DdlExecutionProtocol.ONLINE_ALTER_SHADOW_V1)
                != shadowTarget.isPresent()) {
            throw new DatabaseValidationException(
                    "online ALTER shadow target does not match execution protocol");
        }
        actions = List.copyOf(actions);
        for (int ordinal = 0; ordinal < actions.size(); ordinal++) {
            if (actions.get(ordinal) == null || actions.get(ordinal).ordinal() != ordinal) {
                throw new DatabaseValidationException(
                        "online ALTER actions must have continuous declaration ordinals");
            }
        }
    }
}
