package cn.zhangyis.db.storage.flush.doublewrite;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;

import java.util.List;

/**
 * 一次 doublewrite 批量写入请求。Batch 只描述已经完成 WAL gate 和 checksum stamp 的稳定页镜像，
 * 不持有 Buffer Pool frame、page latch 或 data file 资源；仓储可据此在单个 doublewrite 文件临界区内
 * 分配连续 slot 并顺序写入，减少多页 flush 时的锁切换和随机定位。
 *
 * @param snapshots 本批要写入 doublewrite 文件的页镜像，顺序即 slot 写入顺序；不能为空且不能包含 null。
 */
public record DoublewriteBatch(List<FlushPageSnapshot> snapshots) {

    /**
     * 创建不可变批次并校验输入。空批次没有恢复语义，也容易掩盖上游 flush selector 的状态错误，因此明确拒绝。
     */
    public DoublewriteBatch {
        if (snapshots == null) {
            throw new DatabaseValidationException("doublewrite batch snapshots must not be null");
        }
        if (snapshots.isEmpty()) {
            throw new DatabaseValidationException("doublewrite batch snapshots must not be empty");
        }
        for (FlushPageSnapshot snapshot : snapshots) {
            if (snapshot == null) {
                throw new DatabaseValidationException("doublewrite batch snapshot must not be null");
            }
        }
        snapshots = List.copyOf(snapshots);
    }

    /**
     * 创建多页批次。
     *
     * @param snapshots 页镜像列表，按写入顺序排列。
     * @return 不可变 doublewrite 批次。
     */
    public static DoublewriteBatch of(List<FlushPageSnapshot> snapshots) {
        return new DoublewriteBatch(snapshots);
    }

    /**
     * 创建单页批次，供旧的逐页写路径复用 batch 写入实现，避免单页和多页 slot 语义分叉。
     *
     * @param snapshot 单个页镜像。
     * @return 只包含一个页的 doublewrite 批次。
     */
    public static DoublewriteBatch single(FlushPageSnapshot snapshot) {
        return new DoublewriteBatch(List.of(snapshot));
    }

    /**
     * 本批页数，也等于需要连续分配的 slot 数。
     *
     * @return 批次页数。
     */
    public int size() {
        return snapshots.size();
    }
}
