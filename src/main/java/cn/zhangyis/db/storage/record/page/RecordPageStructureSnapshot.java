package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * INDEX 页结构区的不可变只读快照。它只暴露 header、已使用 heap 与 Page Directory 三段 after-image，
 * 不暴露 PageGuard/BufferFrame，也刻意排除未使用 free-space，使 B+Tree redo 可以记录最终结构而不复制整页空洞。
 *
 * @param level 页层级；0 为 leaf，大于 0 为 internal。
 * @param userRecordCount 页内用户记录数；internal 页即 node pointer 数。
 * @param headerOffset INDEX header 起始偏移。
 * @param headerImage INDEX header after-image。
 * @param heapOffset infimum 起始偏移。
 * @param heapImage infimum、supremum 与已分配 record heap after-image。
 * @param directoryOffset Page Directory 起始偏移。
 * @param directoryImage Page Directory after-image。
 */
public record RecordPageStructureSnapshot(
        int level,
        int userRecordCount,
        int headerOffset,
        byte[] headerImage,
        int heapOffset,
        byte[] heapImage,
        int directoryOffset,
        byte[] directoryImage) {

    public RecordPageStructureSnapshot {
        if (level < 0 || userRecordCount < 0 || headerOffset < 0 || heapOffset < 0 || directoryOffset < 0) {
            throw new DatabaseValidationException("record page structure snapshot values must not be negative");
        }
        if (headerImage == null || headerImage.length == 0
                || heapImage == null || heapImage.length == 0
                || directoryImage == null || directoryImage.length == 0) {
            throw new DatabaseValidationException("record page structure snapshot images must not be empty");
        }
        headerImage = headerImage.clone();
        heapImage = heapImage.clone();
        directoryImage = directoryImage.clone();
    }

    @Override
    public byte[] headerImage() {
        return headerImage.clone();
    }

    @Override
    public byte[] heapImage() {
        return heapImage.clone();
    }

    @Override
    public byte[] directoryImage() {
        return directoryImage.clone();
    }
}
