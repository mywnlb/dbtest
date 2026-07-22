package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;
import java.util.Objects;

/**
 * manifest 中一条有序动作的恢复视图。对象 identity 使用显式字段，动作专属定义使用受控 codec 产生的
 * opaque payload；本值对象拥有数组，避免调用方修改已经参与 digest 的命令字节。
 */
public final class OnlineAlterActionDescriptor {

    /** 单动作 payload 的防内存放大上限。 */
    public static final int MAX_PAYLOAD_BYTES = 256 * 1024;

    /** 跨版本稳定的动作类别；恢复通过stable code解码，不依赖Java枚举顺序。 */
    private final OnlineAlterActionType type;
    /** 用户SQL中的零基声明顺序；manifest要求从零连续，保证多action发布次序确定。 */
    private final int ordinal;
    /** 动作主对象的稳定identity；列/索引动作分别指向目标或源对象，metadata动作可为零。 */
    private final long primaryObjectId;
    /** 为后续双对象动作保留的稳定identity；零明确表示当前动作没有辅助对象。 */
    private final long secondaryObjectId;
    /** 动作专属规范字节；由本对象独占，参与manifest CRC与SHA-256绑定。 */
    private final byte[] payload;

    /**
     * 构造冻结动作。
     *
     * @param type 动作稳定类别；不得为空
     * @param ordinal 用户声明顺序中的零基位置；必须非负
     * @param primaryObjectId 动作主对象identity；零表示该动作无已分配对象
     * @param secondaryObjectId 动作辅助identity；零表示不存在
     * @param payload 由动作专属稳定codec产生的完整参数；允许空数组但不得为空引用
     * @throws DatabaseValidationException identity、顺序或payload边界非法时抛出
     */
    public OnlineAlterActionDescriptor(OnlineAlterActionType type, int ordinal,
                                       long primaryObjectId, long secondaryObjectId,
                                       byte[] payload) {
        if (type == null || ordinal < 0 || primaryObjectId < 0 || secondaryObjectId < 0
                || payload == null || payload.length > MAX_PAYLOAD_BYTES) {
            throw new DatabaseValidationException("invalid online ALTER action descriptor");
        }
        this.type = type;
        this.ordinal = ordinal;
        this.primaryObjectId = primaryObjectId;
        this.secondaryObjectId = secondaryObjectId;
        this.payload = payload.clone();
    }

    public OnlineAlterActionType type() {
        return type;
    }

    public int ordinal() {
        return ordinal;
    }

    public long primaryObjectId() {
        return primaryObjectId;
    }

    public long secondaryObjectId() {
        return secondaryObjectId;
    }

    /** @return 独立payload副本。 */
    public byte[] payload() {
        return payload.clone();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof OnlineAlterActionDescriptor that
                && ordinal == that.ordinal
                && primaryObjectId == that.primaryObjectId
                && secondaryObjectId == that.secondaryObjectId
                && type == that.type
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, ordinal, primaryObjectId, secondaryObjectId,
                Arrays.hashCode(payload));
    }
}
