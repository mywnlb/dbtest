package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 物理页类型（设计 §5.3 FilePageHeader.pageType）。code 落盘（4 字节），取值不可改（PageTypeTest 钉死）。
 * ALLOCATED=0 使零初始化页天然解码为"未用"。毕业到 storage.page：redo（PAGE_INIT）与 mtr（newPage 参数）需引用它，
 * 二者不能依赖 fsp，故下沉到此纯层（不 import PageGuard）。
 */
public enum PageType {
    /** 已分配但未用（空闲数据页）；零初始化页解码为它。 */
    ALLOCATED(0),
    /** page 0：表空间头 + 首批 XDES。 */
    FSP_HDR(1),
    /** page 1：change buffer bitmap（保留）。 */
    IBUF_BITMAP(2),
    /** page 2：segment inode array。 */
    INODE(3),
    /** page 3：序列化数据字典（保留）。 */
    SDI(4),
    /** B+Tree 索引页。 */
    INDEX(5),
    /** Undo 日志页：承载 undo page header + undo record（T1.3a 起）。 */
    UNDO(6);

    private final int code;

    PageType(int code) {
        this.code = code;
    }

    /** 落盘 code。 */
    public int code() {
        return code;
    }

    /** 由落盘 code 还原；未知 code 视为页上类型损坏（脱离 fsp 异常，用通用领域异常）。 */
    public static PageType fromCode(int code) {
        for (PageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new DatabaseValidationException("unknown page type code on disk: " + code);
    }
}
