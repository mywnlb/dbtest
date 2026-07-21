package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;

import java.util.Optional;

/**
 * 一条 undo identity 对应的权威表级物理目标。索引聚合用于聚簇/全部二级 inverse 与 exact-version codec；
 * LOB segment 来自同一精确 table binding，rollback 只能用它校验并释放 INSERT ownership，不能信任 undo reference
 * 自带的 segment identity 作为授权。
 *
 * @param tableIndexes 同一 DD table/schema version 派生的聚簇与全部二级运行期 metadata。
 * @param lobSegment   同一 table binding 的可选 LOB segment；不存在时 INSERT undo 不得携带 external ownership。
 * @param disposition 目标对象是否允许物理访问；隔离对象只把 metadata 用于 undo 完整解码与链校验
 */
public record UndoTargetMetadata(TableIndexMetadata tableIndexes, Optional<SegmentRef> lobSegment,
                                 UndoTargetDisposition disposition) {

    /**
     * 校验表级索引聚合和 LOB segment 属于同一物理 tablespace。
     *
     * @param tableIndexes exact-version 表级索引聚合，不能为 {@code null}。
     * @param lobSegment   可选 LOB segment 容器，不能为 {@code null}。
     * @throws DatabaseValidationException 字段缺失或 LOB segment 与聚簇 root 不属于同一 space 时抛出。
     */
    public UndoTargetMetadata {
        if (tableIndexes == null || lobSegment == null || disposition == null) {
            throw new DatabaseValidationException("undo target requires table indexes and optional LOB segment");
        }
        if (lobSegment.isPresent()
                && !lobSegment.orElseThrow().spaceId().equals(
                        tableIndexes.clusteredIndex().rootPageId().spaceId())) {
            throw new DatabaseValidationException("undo target index/LOB segment must belong to the same space");
        }
    }

    /** 兼容既有调用点；显式构造的传统目标均视为可物理访问。 */
    public UndoTargetMetadata(TableIndexMetadata tableIndexes, Optional<SegmentRef> lobSegment) {
        this(tableIndexes, lobSegment, UndoTargetDisposition.AVAILABLE);
    }

    /** @return {@code true} 表示 recovery 必须跳过用户 B+Tree/LOB inverse，仅推进系统 undo 进度。 */
    public boolean recoveryUnavailable() {
        return disposition == UndoTargetDisposition.RECOVERY_UNAVAILABLE;
    }

    /**
     * 返回 undo 主体编码和聚簇 inverse 使用的唯一聚簇 descriptor。
     *
     * @return {@link #tableIndexes()} 中的聚簇索引。
     */
    public BTreeIndex clusteredIndex() {
        return tableIndexes.clusteredIndex();
    }
}
