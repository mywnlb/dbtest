package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;

import java.util.Arrays;

/**
 * Flush 期间写 doublewrite 和 data file 的稳定页镜像。创建 snapshot 后，Flush 模块只操作该副本，
 * 不跨 redo wait、doublewrite fsync 或 data file IO 持有 Buffer Pool 的 poolLock/page latch。
 *
 * @param pageId 页定位键。
 * @param pageLsn snapshot 时页头中的 pageLSN。
 * @param dirtyVersion BufferFrame 的脏版本，completeFlush 用它判断 snapshot 后页面是否再次变脏。
 * @param pageImage 整页副本；构造和访问都防御性复制，避免外部修改 Buffer Pool 状态。
 */
public record FlushPageSnapshot(PageId pageId, Lsn pageLsn, long dirtyVersion, byte[] pageImage) {

    public FlushPageSnapshot {
        if (pageId == null || pageLsn == null || pageImage == null) {
            throw new DatabaseValidationException("flush page snapshot fields must not be null");
        }
        if (dirtyVersion < 0) {
            throw new DatabaseValidationException("dirty version must not be negative: " + dirtyVersion);
        }
        pageImage = Arrays.copyOf(pageImage, pageImage.length);
    }

    @Override
    public byte[] pageImage() {
        return Arrays.copyOf(pageImage, pageImage.length);
    }
}
