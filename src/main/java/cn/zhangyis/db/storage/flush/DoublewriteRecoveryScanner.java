package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.page.PageImageChecksum;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Doublewrite recovery helper。启动恢复阶段先检测 data page checksum；若目标页损坏且 doublewrite 中有有效 full copy，
 * 则通过 PageStore 修复 data file 并 force。修复后 redo replay 仍按 pageLSN 幂等补齐后续修改。
 */
public final class DoublewriteRecoveryScanner {

    private final DoublewriteFileRepository repository;
    private final PageStore pageStore;
    private final PageSize pageSize;

    public DoublewriteRecoveryScanner(DoublewriteFileRepository repository, PageStore pageStore, PageSize pageSize) {
        if (repository == null || pageStore == null || pageSize == null) {
            throw new DatabaseValidationException("doublewrite repository/page store/page size must not be null");
        }
        this.repository = repository;
        this.pageStore = pageStore;
        this.pageSize = pageSize;
    }

    /**
     * 若 data file 中的页 checksum 无效，则尝试用 doublewrite 最新有效副本修复。
     *
     * @param pageId 目标页。
     * @return true 表示执行了修复；false 表示页原本有效或没有可用副本。
     */
    public boolean repairPageIfNeeded(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        // 越当前物理文件尾的页物理上不存在，谈不上 torn page；崩溃可能把 autoExtend 出的尾部截掉，此处不应
        // 越界读取或用 doublewrite 复活它（doublewrite 在 redo 之前执行，文件可能仍短）。交给 redo replay 的
        // extend-on-demand 与 SPACE_FILE_RECONCILE 重建，故跳过。
        if (pageId.pageNo().value() >= pageStore.currentSizeInPages(pageId.spaceId()).value()) {
            return false;
        }
        byte[] current = new byte[pageSize.bytes()];
        pageStore.readPage(pageId, ByteBuffer.wrap(current));
        if (PageImageChecksum.verify(current, pageSize)) {
            return false;
        }
        Optional<byte[]> copy = repository.latestCopy(pageId);
        if (copy.isEmpty()) {
            return false;
        }
        pageStore.writePage(pageId, ByteBuffer.wrap(copy.get()));
        pageStore.force(pageId.spaceId());
        return true;
    }
}
