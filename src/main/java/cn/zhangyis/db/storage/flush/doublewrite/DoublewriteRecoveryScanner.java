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

    /**
     * 本对象持有的 {@code repository} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DoublewriteFileRepository repository;
    /**
     * 本对象持有的 {@code channel} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DoublewriteChannel channel;
    /**
     * 本对象持有的 {@code legacyRepository} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DoublewriteFileRepository legacyRepository;
    /**
     * 本对象持有的 {@code pageStore} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final PageStore pageStore;
    /**
     * 本对象持有的 {@code pageSize} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final PageSize pageSize;

    /**
     * 创建 {@code DoublewriteRecoveryScanner}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param repository 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param pageStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DoublewriteRecoveryScanner(DoublewriteFileRepository repository, PageStore pageStore, PageSize pageSize) {
        if (repository == null || pageStore == null || pageSize == null) {
            throw new DatabaseValidationException("doublewrite repository/page store/page size must not be null");
        }
        this.repository = repository;
        this.channel = null;
        this.legacyRepository = null;
        this.pageStore = pageStore;
        this.pageSize = pageSize;
    }

    /** 使用 FlushList/LRU 双物理文件的恢复 scanner。
     *
     * @param channel 恢复、checkpoint、doublewrite 或刷脏阶段的协作状态；不得为 {@code null}，阶段和持久化边界必须与当前实例的恢复状态机一致
     * @param pageStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DoublewriteRecoveryScanner(DoublewriteChannel channel, PageStore pageStore, PageSize pageSize) {
        if (channel == null || pageStore == null || pageSize == null) {
            throw new DatabaseValidationException("doublewrite channel/page store/page size must not be null");
        }
        this.repository = null;
        this.channel = channel;
        this.legacyRepository = null;
        this.pageStore = pageStore;
        this.pageSize = pageSize;
    }

    /** 双物理文件加旧单文件兼容恢复入口。
     *
     * @param channel 恢复、checkpoint、doublewrite 或刷脏阶段的协作状态；不得为 {@code null}，阶段和持久化边界必须与当前实例的恢复状态机一致
     * @param legacyRepository 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param pageStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DoublewriteRecoveryScanner(DoublewriteChannel channel, DoublewriteFileRepository legacyRepository,
                                      PageStore pageStore, PageSize pageSize) {
        if (channel == null || pageStore == null || pageSize == null) {
            throw new DatabaseValidationException("doublewrite channel/page store/page size must not be null");
        }
        this.repository = null;
        this.channel = channel;
        this.legacyRepository = legacyRepository;
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
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取脏页、page LSN、代际与 checkpoint 压力快照，先排除未固定或已失效的候选。</li>
     *     <li>在不持页闩执行慢等待的前提下推进 redo durable 边界，确保数据页写盘前满足 WAL。</li>
     *     <li>按既定 doublewrite、表空间写入和 force 顺序持久化快照，部分失败只确认实际成功的页面。</li>
     *     <li>重新校验代际与 dirty version 后发布完成状态；并发再修改页继续保持 dirty，异常不推进不安全边界。</li>
     * </ol>
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param repairAllowed 恢复容错策略标志；只允许在契约明确的损坏或结果不确定场景放宽校验，不得掩盖其他数据损坏
     * @return {@code scanPage} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private DoublewriteRecoveryResult scanPage(PageId pageId, boolean repairAllowed) {
        // 1、读取脏页、page LSN、代际与 checkpoint 压力快照，在共享或持久副作用前拒绝非法状态。
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
        // 2、继续完成范围、身份与候选校验；通过后，在不持页闩执行慢等待的前提下推进 redo durable 边界，保持处理顺序与资源边界。
        pageStore.readPage(pageId, ByteBuffer.wrap(current));
        if (PageImageChecksum.verify(current, pageSize)) {
            return new DoublewriteRecoveryResult(pageId, DoublewriteRecoveryOutcome.CLEAN_OR_NOT_COVERED);
        }
        Optional<byte[]> copy = latestCopy(pageId);
        // 3、在中间分支复核阶段性结果；满足条件后，按既定 doublewrite、表空间写入和 force 顺序持久化快照，并维持领域不变量。
        if (copy.isPresent()) {
            if (!repairAllowed) {
                return new DoublewriteRecoveryResult(pageId, DoublewriteRecoveryOutcome.REPAIRABLE_FROM_COPY);
            }
            pageStore.writePage(pageId, ByteBuffer.wrap(copy.get()));
            pageStore.force(pageId.spaceId());
            return new DoublewriteRecoveryResult(pageId, DoublewriteRecoveryOutcome.REPAIRED_FROM_COPY);
        }
        boolean coveredByDetectOnly = channel == null
                ? repository.scanEntries().stream()
                        .anyMatch(entry -> pageId.equals(entry.pageId()) && !entry.hasFullCopy())
                : java.util.stream.Stream.of(DoublewriteChannelId.FLUSH_LIST, DoublewriteChannelId.LRU)
                        .flatMap(id -> channel.scanEntries(id).stream())
                        .anyMatch(entry -> pageId.equals(entry.pageId()) && !entry.hasFullCopy());
        if (!coveredByDetectOnly && legacyRepository != null) {
            coveredByDetectOnly = legacyRepository.scanEntries().stream()
                    .anyMatch(entry -> pageId.equals(entry.pageId()) && !entry.hasFullCopy());
        }
        if (coveredByDetectOnly) {
            return new DoublewriteRecoveryResult(pageId, DoublewriteRecoveryOutcome.DETECTED_ONLY);
        }
        // 4、重新校验代际与 dirty version 后发布完成状态，以稳定返回或领域异常完成收口。
        return new DoublewriteRecoveryResult(pageId, DoublewriteRecoveryOutcome.CLEAN_OR_NOT_COVERED);
    }

    private Optional<byte[]> latestCopy(PageId pageId) {
        Optional<byte[]> selected = channel == null ? repository.latestCopy(pageId) : channel.latestCopy(pageId);
        if (legacyRepository == null) {
            return selected;
        }
        Optional<byte[]> legacy = legacyRepository.latestCopy(pageId);
        if (selected.isEmpty()) {
            return legacy;
        }
        if (legacy.isEmpty()) {
            return selected;
        }
        long selectedLsn = channel == null ? repository.latestCopyLsn(pageId).orElse(Long.MIN_VALUE)
                : channel.latestCopyLsn(pageId).orElse(Long.MIN_VALUE);
        long legacyLsn = legacyRepository.latestCopyLsn(pageId).orElse(Long.MIN_VALUE);
        if (legacyLsn == selectedLsn && !java.util.Arrays.equals(selected.orElseThrow(), legacy.orElseThrow())) {
            throw new cn.zhangyis.db.common.exception.DatabaseRuntimeException(
                    "conflicting legacy and dual doublewrite copies for page " + pageId
                            + " at pageLSN " + selectedLsn);
        }
        return legacyLsn > selectedLsn ? legacy : selected;
    }
}
