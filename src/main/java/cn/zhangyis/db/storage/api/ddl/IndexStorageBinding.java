package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.api.SegmentRef;

/** catalog 持久化的索引物理定位：稳定 root page 加 leaf/non-leaf segment 身份。
 *
 * @param indexId 参与 {@code 构造} 的零基位置 {@code indexId}；必须非负且小于所属页面、集合或持久结构的容量
 * @param rootPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
 * @param rootLevel 参与 {@code 构造} 的树层级或递归深度 {@code rootLevel}；必须非负且不得超过当前页结构、MTR memo 或解析器声明的最大深度
 * @param leafSegment 参与 {@code 构造} 的稳定领域标识 {@code SegmentRef}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param nonLeafSegment 参与 {@code 构造} 的稳定领域标识 {@code SegmentRef}；不得为 {@code null}，并须由对应值对象构造校验产生
 */
public record IndexStorageBinding(long indexId, PageId rootPageId, int rootLevel,
                                  SegmentRef leafSegment, SegmentRef nonLeafSegment) {
    public IndexStorageBinding {
        if (indexId <= 0 || rootPageId == null || rootLevel < 0 || leafSegment == null || nonLeafSegment == null) {
            throw new DatabaseValidationException("invalid index storage binding");
        }
    }
}
