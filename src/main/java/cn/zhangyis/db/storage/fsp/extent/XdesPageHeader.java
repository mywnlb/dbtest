package cn.zhangyis.db.storage.fsp.extent;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 独立 XDES 页 body header 的不可变视图。
 *
 * @param role 页在管理区中的 PRIMARY/OVERFLOW 角色
 * @param groupBasePageNo 所属管理区首物理页号；用于拒绝搬错位置的完整页镜像
 * @param firstExtentNo 本页 slot0 描述的全局 extentNo
 * @param entryCount 本页已声明的连续 descriptor 数；允许 primary 为 0
 */
public record XdesPageHeader(
        XdesPageRole role,
        long groupBasePageNo,
        long firstExtentNo,
        int entryCount) {

    /** 构造时固定所有范围不变量，避免损坏 header 进入 slot 算术。 */
    public XdesPageHeader {
        if (role == null) {
            throw new DatabaseValidationException("XDES page role must not be null");
        }
        if (groupBasePageNo < 0 || firstExtentNo < 0 || entryCount < 0) {
            throw new DatabaseValidationException("XDES header values must not be negative");
        }
    }
}
