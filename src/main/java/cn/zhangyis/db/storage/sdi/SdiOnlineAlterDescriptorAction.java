package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/** descriptor entry的物理动作；稳定码独立于manifest action和Java ordinal。 */
public enum SdiOnlineAlterDescriptorAction {
    ADD(1), DROP(2);

    private final int stableCode;

    SdiOnlineAlterDescriptorAction(int stableCode) {
        this.stableCode = stableCode;
    }

    public int stableCode() {
        return stableCode;
    }

    public static SdiOnlineAlterDescriptorAction fromStableCode(int code) {
        return Arrays.stream(values()).filter(value -> value.stableCode == code).findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "unknown online ALTER descriptor action: " + code));
    }
}
