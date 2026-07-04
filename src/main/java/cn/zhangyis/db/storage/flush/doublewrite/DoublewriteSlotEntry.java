package cn.zhangyis.db.storage.flush.doublewrite;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

/**
 * doublewrite 文件中一个通过 magic/format/CRC 校验的 slot 摘要。它不暴露 payload，避免上层误把
 * detect-only metadata 当作完整页副本使用。
 *
 * @param pageId slot 覆盖的 data page。
 * @param pageLsn slot 写入时页头记录的 LSN。
 * @param kind slot 持久化内容类型。
 * @param checksum slot payload CRC，用于诊断 doublewrite 文件自身是否损坏。
 */
public record DoublewriteSlotEntry(PageId pageId, long pageLsn, DoublewriteSlotKind kind, int checksum) {

    public DoublewriteSlotEntry {
        if (pageId == null || kind == null) {
            throw new DatabaseValidationException("doublewrite slot entry page/kind must not be null");
        }
    }

    /**
     * @return true 表示该 slot 保存完整页镜像，恢复期可由 {@link DoublewriteFileRepository#latestCopy(PageId)} 使用。
     */
    public boolean hasFullCopy() {
        return kind == DoublewriteSlotKind.FULL_COPY;
    }
}
