package cn.zhangyis.db.storage.fsp.extent;

import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;

/** 独立 XDES 页在管理区中的稳定角色；code 进入磁盘 header，禁止依赖 enum ordinal。 */
public enum XdesPageRole {
    /** 管理区首张 descriptor 页，固定在 groupBase。 */
    PRIMARY(1),
    /** primary 容量不足时的第二张 descriptor 页，固定在 groupBase+5。 */
    OVERFLOW(2);

    /** 持久 code；追加新角色时不得修改既有值。 */
    private final int persistentCode;

    XdesPageRole(int persistentCode) {
        this.persistentCode = persistentCode;
    }

    /** @return 写入 XDES header 的稳定正整数 code */
    public int persistentCode() {
        return persistentCode;
    }

    /**
     * 从磁盘 code 恢复角色。
     *
     * @param code XDES header 中的稳定 code
     * @return 对应的已知角色
     * @throws FspMetadataException 未知 code 表示页面格式损坏时抛出
     */
    public static XdesPageRole fromPersistentCode(int code) {
        for (XdesPageRole role : values()) {
            if (role.persistentCode == code) {
                return role;
            }
        }
        throw new FspMetadataException("unknown XDES page role code: " + code);
    }
}
