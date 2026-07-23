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
    /** GENERAL 表空间 page 3：序列化数据字典 v1；UNDO 表空间 page3 另用 RSEG_HEADER。 */
    SDI(4),
    /** B+Tree 索引页。 */
    INDEX(5),
    /** Undo 日志页：承载 undo page header + undo record（T1.3a 起）。 */
    UNDO(6),
    /** Rollback segment header 页：undo 表空间 page3，承载 slot 目录（slot -> insert-undo 首页），0.3 起。 */
    RSEG_HEADER(7),
    /** Off-page TEXT/BLOB/JSON payload chain 页（0.21h）。 */
    BLOB(8),
    /** 超出单张 UNDO 页容量的完整 UndoRecord 编码 payload 页链（1.6）。 */
    UNDO_PAYLOAD(9),
    /** 通用Online ALTER的版本化物理descriptor chain页；只由DDL专属segment拥有。 */
    DDL_DESCRIPTOR(10),
    /** system.ibd page 3：Change Buffer 持久 header；追加编码，既有页类型 code 不变。 */
    IBUF_HEADER(11),
    /** system.ibd 全局 Change Buffer B+Tree 页；与普通用户 INDEX 页按 envelope 类型隔离。 */
    IBUF_INDEX(12),
    /** page0 容量之后承载连续 extent descriptor 的独立管理页。 */
    XDES(13);

    /**
     * 记录 {@code code} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final int code;

    /**
     * 创建 {@code PageType}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param code 参与 {@code 构造} 的稳定编码 {@code code}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     */
    PageType(int code) {
        this.code = code;
    }

    /** 落盘 code。 */
    public int code() {
        return code;
    }

    /** 由落盘 code 还原；未知 code 视为页上类型损坏（脱离 fsp 异常，用通用领域异常）。
     *
     * @param code 参与 {@code fromCode} 的稳定编码 {@code code}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     * @return {@code fromCode} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static PageType fromCode(int code) {
        for (PageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new DatabaseValidationException("unknown page type code on disk: " + code);
    }
}
