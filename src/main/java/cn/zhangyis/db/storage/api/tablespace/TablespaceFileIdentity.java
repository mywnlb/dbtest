package cn.zhangyis.db.storage.api.tablespace;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.state.TablespaceType;

/**
 * 外部表空间文件重新接入前必须匹配的 page0 身份快照。
 *
 * <p>该值对象只表达文件身份，不代表文件当前是否已挂载。IMPORT 必须同时匹配
 * SpaceId、页大小、类型和 page0 版本，防止把另一张表或旧代文件接到当前字典 binding。</p>
 *
 * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
 * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
 * @param type 选择 {@code 构造} 分支的 {@code TablespaceType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
 * @param serverVersion 参与 {@code 构造} 的单调版本值 {@code serverVersion}；必须非负，回退或与权威快照冲突时拒绝
 * @param spaceVersion 参与 {@code 构造} 的单调版本值 {@code spaceVersion}；必须非负，回退或与权威快照冲突时拒绝
 */
public record TablespaceFileIdentity(SpaceId spaceId, PageSize pageSize, TablespaceType type,
                                     int serverVersion, long spaceVersion) {
    public TablespaceFileIdentity {
        if (spaceId == null || pageSize == null || type == null) {
            throw new DatabaseValidationException("tablespace file identity fields must not be null");
        }
        if (serverVersion <= 0 || spaceVersion <= 0) {
            throw new DatabaseValidationException("tablespace file identity versions must be positive");
        }
    }
}
