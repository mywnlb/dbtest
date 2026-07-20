package cn.zhangyis.db.storage.sdi;

import java.util.Arrays;

/**
 * page3 索引 DDL footer 的动作类型。稳定码属于持久格式，禁止依赖枚举 ordinal。
 */
public enum SdiIndexDdlAction {
    /** 新建二级索引；v1 footer 在没有动作字段时也按 BUILD 解释。 */
    BUILD(1),
    /** 删除已由旧 DD 引用的二级索引；DD 提交后据此回收两个 segment。 */
    DROP(2);

    /** footer v2 使用且跨版本不可重排的稳定码。 */
    private final int stableCode;

    SdiIndexDdlAction(int stableCode) {
        this.stableCode = stableCode;
    }

    /** @return footer 中使用的正稳定码。 */
    public int stableCode() {
        return stableCode;
    }

    /**
     * 解码 footer 动作。
     *
     * @param code footer v2 中的稳定码
     * @return 对应索引 DDL 动作
     * @throws SdiPageCorruptionException 未知动作无法安全决定回收方向时抛出
     */
    public static SdiIndexDdlAction fromStableCode(int code) {
        return Arrays.stream(values())
                .filter(action -> action.stableCode == code)
                .findFirst()
                .orElseThrow(() -> new SdiPageCorruptionException(
                        "unknown SDI index DDL action code: " + code));
    }
}
