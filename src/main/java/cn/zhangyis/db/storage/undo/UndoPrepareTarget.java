package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

/**
 * phase-one first-page 批处理的稳定目标。对象只携带 undo 层能够验证的物理身份，不反向依赖事务
 * {@code UndoLogBinding}；slot/page3 owner 由上层事务生命周期协调器在进入写 MTR 前核对。
 *
 * @param firstPageId 待从 ACTIVE 转成 PREPARED 的 undo segment 首页
 * @param kind 首页必须携带的普通 INSERT/UPDATE undo kind
 */
public record UndoPrepareTarget(PageId firstPageId, UndoLogKind kind) {

    public UndoPrepareTarget {
        if (firstPageId == null || kind == null) {
            throw new DatabaseValidationException("undo prepare target fields must not be null");
        }
        if (kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("temporary undo cannot enter ordinary XA prepare");
        }
    }
}
