package cn.zhangyis.db.storage.flush.doublewrite;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.fil.io.PageStore;
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
        return scanPageIfNeeded(pageId).repaired();
    }

    /**
     * 检查单页并返回结构化结果。full-copy 命中时执行修复；detect-only metadata 命中时只报告，绝不写回 data file。
     *
     * @param pageId 目标页。
     * @return 单页检查结果。
     */
    public DoublewriteRecoveryResult scanPageIfNeeded(PageId pageId) {
        return scanPage(pageId, true);
    }

    /**
     * 只读校验单页并返回结构化结果。该路径与普通 recovery 使用同样的 checksum/full-copy/detect-only 判定，
     * 但即使命中 full-copy 也只报告 {@link DoublewriteRecoveryOutcome#REPAIRABLE_FROM_COPY}，绝不写回 data file。
     *
     * @param pageId 目标页。
     * @return 单页只读校验结果。
     */
    public DoublewriteRecoveryResult scanPageForValidation(PageId pageId) {
        return scanPage(pageId, false);
    }

    /**
     * doublewrite 单页判定的共享数据流：先拒绝 null 页号，再跳过物理文件尾外页，随后读取当前 data page
     * 并校验 checksum；只有 checksum 无效时才查 doublewrite full-copy / detect-only metadata。`repairAllowed`
     * 是唯一写盘开关，保证 READ_ONLY_VALIDATE 和 NORMAL 走同一判定但不会误用同一写回副作用。
     */
    private DoublewriteRecoveryResult scanPage(PageId pageId, boolean repairAllowed) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        // 越当前物理文件尾的页物理上不存在，谈不上 torn page；崩溃可能把 autoExtend 出的尾部截掉，此处不应
        // 越界读取或用 doublewrite 复活它（doublewrite 在 redo 之前执行，文件可能仍短）。交给 redo replay 的
        // extend-on-demand 与 SPACE_FILE_RECONCILE 重建，故跳过。
        if (pageId.pageNo().value() >= pageStore.currentSizeInPages(pageId.spaceId()).value()) {
            return new DoublewriteRecoveryResult(pageId, DoublewriteRecoveryOutcome.CLEAN_OR_NOT_COVERED);
        }
        byte[] current = new byte[pageSize.bytes()];
        pageStore.readPage(pageId, ByteBuffer.wrap(current));
        if (PageImageChecksum.verify(current, pageSize)) {
            return new DoublewriteRecoveryResult(pageId, DoublewriteRecoveryOutcome.CLEAN_OR_NOT_COVERED);
        }
        Optional<byte[]> copy = repository.latestCopy(pageId);
        if (copy.isPresent()) {
            if (!repairAllowed) {
                return new DoublewriteRecoveryResult(pageId, DoublewriteRecoveryOutcome.REPAIRABLE_FROM_COPY);
            }
            pageStore.writePage(pageId, ByteBuffer.wrap(copy.get()));
            pageStore.force(pageId.spaceId());
            return new DoublewriteRecoveryResult(pageId, DoublewriteRecoveryOutcome.REPAIRED_FROM_COPY);
        }
        boolean coveredByDetectOnly = repository.scanEntries().stream()
                .anyMatch(entry -> pageId.equals(entry.pageId()) && !entry.hasFullCopy());
        if (coveredByDetectOnly) {
            return new DoublewriteRecoveryResult(pageId, DoublewriteRecoveryOutcome.DETECTED_ONLY);
        }
        return new DoublewriteRecoveryResult(pageId, DoublewriteRecoveryOutcome.CLEAN_OR_NOT_COVERED);
    }
}
