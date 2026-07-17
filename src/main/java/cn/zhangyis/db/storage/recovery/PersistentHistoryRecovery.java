package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.trx.HistoryEntry;
import cn.zhangyis.db.storage.undo.RollbackSegmentHistoryBase;
import cn.zhangyis.db.storage.undo.UndoHistoryNodeSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * page3 history base、occupied slot 证据与 undo first-page 双向链的恢复闭包校验器。
 *
 * <p>本类只编排不可变快照；每个节点如何在短 MTR 中读取由 {@link NodeReader} 注入。成功结果严格保持物理链顺序，
 * 不按 TransactionNo 排序。任何 cycle、断链、orphan COMMITTED、linked ACTIVE、重复 identity 或提交号高水位回退
 * 都是恢复安全性损坏，必须在开放流量前 fail-closed。
 */
public final class PersistentHistoryRecovery {

    /** 读取单个 first page 的无 latch 快照；生产实现保证一节点一短 MTR。 */
    @FunctionalInterface
    public interface NodeReader {
        /**
         * 读取指定 undo first page 的 history/header 快照，返回前必须释放所有页资源。
         *
         * @param firstPageId page3/history 链当前节点声明的稳定 first-page identity。
         * @return 与请求 page id 精确一致、可在恢复编排中跨 MTR 使用的不可变节点快照。
         * @throws RuntimeException 页读取、格式校验或 MTR 释放失败时抛出，恢复总控必须保持流量关闭。
         */
        UndoHistoryNodeSnapshot read(PageId firstPageId);
    }

    /** 从一条已校验 history node 的 persistent logical chain 投影去重表集合；实现必须逐 pointer 短读并释放 latch。 */
    @FunctionalInterface
    public interface AffectedTableReader {
        /**
         * 从 committed UPDATE undo 的 logical head 遍历当前有效链，投影其中 UPDATE/DELETE record 的表集合。
         *
         * @param firstPageId 当前 history entry 的 undo 段首页，用于打开 exact persistent owner。
         * @param logicalHead first-page header 中已恢复并校验的 logical chain 入口。
         * @return 去重稳定 table id 集合；无业务 record 时返回空集合，不能返回 {@code null}。
         * @throws RuntimeException undo 读取、格式/identity 校验或资源释放失败时抛出，恢复必须 fail-closed。
         */
        Set<Long> read(PageId firstPageId, cn.zhangyis.db.storage.undo.UndoLogicalHead logicalHead);
    }

    /**
     * 兼容不需要 affected-table barrier 的低层恢复，交叉校验并按 head→next 物理顺序重建 history 条目。
     *
     * @param base           page3 持久 history base，声明 head/tail/length/last transaction no。
     * @param occupiedSlots page3 occupied slot 到 first-page 的权威 owner 映射。
     * @param recoveredSlots 逐 first-page 恢复出的 slot/header 状态证据。
     * @param nodeReader     每个物理 history 节点的一短 MTR 快照读取器。
     * @return 保持物理链顺序、affectedTableIds 为空的不可变 history 条目列表。
     * @throws DatabaseValidationException 输入缺失时抛出。
     * @throws TransactionRecoveryException 链、slot、owner、状态或高水位证据不闭合时抛出。
     */
    public List<HistoryEntry> rebuild(RollbackSegmentHistoryBase base,
                                      Map<UndoSlotId, PageId> occupiedSlots,
                                      List<RecoveredUndoSlotEvidence> recoveredSlots,
                                      NodeReader nodeReader) {
        return rebuild(base, occupiedSlots, recoveredSlots, nodeReader, (pageId, logicalHead) -> Set.of());
    }

    /**
     * 交叉校验 page3、occupied slots 与 undo first-page 物理 history，并从每个 logical head 重建表引用投影。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验输入 cardinality，并把 recovered slot evidence 建成 first-page 唯一索引。</li>
     *     <li>严格按 base.head→next 遍历声明长度，逐节点核对 slot、COMMITTED UPDATE、双向链接、creator/commit identity，
     *         再读取 logical chain 的 affected-table 集合。</li>
     *     <li>核对 tail/length 闭包，并扫描全部 occupied evidence：COMMITTED 不得 orphan，ACTIVE 必须未链接且 header 状态一致。</li>
     *     <li>确认 page3 lastTransactionNo 不低于 live history 最大提交号，最后发布保持物理顺序的不可变结果。</li>
     * </ol>
     *
     * @param base                page3 持久 history base 与提交号高水位。
     * @param occupiedSlots       page3 occupied slot→first-page 的一对一 owner 映射。
     * @param recoveredSlots      从 occupied first pages 读取的 slot/header 恢复证据。
     * @param nodeReader          每个 first page 的无 latch 快照读取器。
     * @param affectedTableReader 从节点 logical head 投影 committed UPDATE 链表集合的读取器。
     * @return head→tail 物理顺序的不可变 {@link HistoryEntry} 列表，可直接交给 {@code HistoryList.restore}。
     * @throws DatabaseValidationException 任一输入/读取器缺失或 cardinality 基础字段非法时抛出。
     * @throws TransactionRecoveryException 链中断/成环、owner/状态/链接/identity 错配、orphan COMMITTED、linked ACTIVE，
     *                                      affected-table 读取返回 null 或提交号高水位回退时抛出。
     */
    public List<HistoryEntry> rebuild(RollbackSegmentHistoryBase base,
                                      Map<UndoSlotId, PageId> occupiedSlots,
                                      List<RecoveredUndoSlotEvidence> recoveredSlots,
                                      NodeReader nodeReader,
                                      AffectedTableReader affectedTableReader) {
        // 1. 输入集合和一对一 evidence 索引先于物理链遍历构造，防止中途才发现 slot 映射歧义。
        if (base == null || occupiedSlots == null || recoveredSlots == null || nodeReader == null) {
            throw new DatabaseValidationException("persistent history recovery inputs must not be null");
        }
        if (affectedTableReader == null) {
            throw new DatabaseValidationException("persistent history affected-table reader must not be null");
        }
        if (base.length() > occupiedSlots.size() || recoveredSlots.size() != occupiedSlots.size()) {
            throw new TransactionRecoveryException("history/slot evidence cardinality mismatch: history="
                    + base.length() + ", occupied=" + occupiedSlots.size()
                    + ", recovered=" + recoveredSlots.size());
        }
        Map<PageId, RecoveredUndoSlotEvidence> evidenceByPage = new HashMap<>();
        for (RecoveredUndoSlotEvidence evidence : recoveredSlots) {
            if (!evidence.firstPageId().equals(occupiedSlots.get(evidence.slotId()))
                    || evidenceByPage.put(evidence.firstPageId(), evidence) != null) {
                throw new TransactionRecoveryException("recovered undo slot/page mapping is not one-to-one: "
                        + evidence.firstPageId());
            }
        }
        // 2. 严格按持久 next 链读取；不按 transaction no 排序，否则会掩盖真实物理断链。
        Set<PageId> seenPages = new HashSet<>();
        Set<Long> seenCreators = new HashSet<>();
        Set<Long> seenCommitNos = new HashSet<>();
        List<HistoryEntry> physicalOrder = new ArrayList<>();
        Optional<PageId> current = base.headPageId();
        Optional<PageId> previous = Optional.empty();
        long maxCommitNo = 0L;
        for (long index = 0; index < base.length(); index++) {
            if (current.isEmpty()) {
                throw new TransactionRecoveryException("persistent history ended before declared length at " + index);
            }
            PageId pageId = current.orElseThrow();
            if (!seenPages.add(pageId)) {
                throw new TransactionRecoveryException("persistent history contains a cycle/duplicate node: " + pageId);
            }
            RecoveredUndoSlotEvidence evidence = evidenceByPage.get(pageId);
            if (evidence == null || evidence.state() != RecoveredUndoState.COMMITTED
                    || evidence.kind() != UndoLogKind.UPDATE) {
                throw new TransactionRecoveryException("history node is not a COMMITTED UPDATE occupied slot: "
                        + pageId);
            }
            UndoHistoryNodeSnapshot node = requireNode(nodeReader.read(pageId), pageId);
            if (!node.isCommitted() || node.kind() != UndoLogKind.UPDATE
                    || !node.creatorTransactionId().equals(evidence.creatorTransactionId())
                    || !node.committedTransactionNo().equals(evidence.transactionNo())
                    || !node.previousHistoryPageId().equals(previous)) {
                throw new TransactionRecoveryException("persistent history node/header/link mismatch: " + pageId);
            }
            if (!seenCreators.add(node.creatorTransactionId().value())
                    || !seenCommitNos.add(node.committedTransactionNo().value())) {
                throw new TransactionRecoveryException("persistent history creator/commit number is duplicated: "
                        + pageId);
            }
            maxCommitNo = Math.max(maxCommitNo, node.committedTransactionNo().value());
            Set<Long> affectedTables = affectedTableReader.read(pageId, node.logicalHead());
            if (affectedTables == null) {
                throw new TransactionRecoveryException(
                        "history affected-table reader returned null: " + pageId);
            }
            physicalOrder.add(new HistoryEntry(node.committedTransactionNo(), node.creatorTransactionId(),
                    pageId.spaceId(), pageId, evidence.slotId(), affectedTables));
            previous = Optional.of(pageId);
            current = node.nextHistoryPageId();
        }
        // 3. 声明长度结束后必须精确落在 tail；随后核对所有 slot，禁止 committed orphan 或 active linked node。
        if (current.isPresent() || !previous.equals(base.tailPageId())) {
            throw new TransactionRecoveryException("persistent history tail/declared length mismatch: base=" + base);
        }
        for (RecoveredUndoSlotEvidence evidence : recoveredSlots) {
            boolean linked = seenPages.contains(evidence.firstPageId());
            if (evidence.state() == RecoveredUndoState.COMMITTED && !linked) {
                throw new TransactionRecoveryException("COMMITTED occupied slot is omitted from history: "
                        + evidence.firstPageId());
            }
            if (evidence.state() == RecoveredUndoState.ACTIVE) {
                UndoHistoryNodeSnapshot active = requireNode(nodeReader.read(evidence.firstPageId()),
                        evidence.firstPageId());
                if (linked || !active.isActive() || active.kind() != evidence.kind()
                        || !active.creatorTransactionId().equals(evidence.creatorTransactionId())
                        || !active.committedTransactionNo().isNone()
                        || active.previousHistoryPageId().isPresent() || active.nextHistoryPageId().isPresent()) {
                    throw new TransactionRecoveryException("ACTIVE undo slot header/state must match evidence and "
                            + "remain unlinked: " + evidence.firstPageId());
                }
            }
        }
        // 4. 即使 history 之后会被 purge，page3 持久提交号高水位也不能低于当前 live 最大值。
        if (base.lastTransactionNo().value() < maxCommitNo) {
            throw new TransactionRecoveryException("rseg lastTransactionNo is below live history maximum: last="
                    + base.lastTransactionNo().value() + ", max=" + maxCommitNo);
        }
        return List.copyOf(physicalOrder);
    }

    /**
     * 合并 transaction sidecar/redo 恢复出的 nextNo 与 page3 持久高水位。即使 history 已全部 purge，后者也不能
     * 丢失，否则重启会复用旧提交号。
     *
     * @param base                     已完成格式校验的 page3 history base，提供 lastTransactionNo。
     * @param reconciledNextTransactionNo transaction redo/sidecar 已恢复出的正 next transaction number。
     * @return 两个权威来源中较大的安全 next number。
     * @throws DatabaseValidationException base 缺失或 reconciled next number 小于 1 时抛出。
     * @throws TransactionRecoveryException lastTransactionNo 已达长整型上限、无法安全加一时抛出。
     */
    public long nextTransactionNo(RollbackSegmentHistoryBase base, long reconciledNextTransactionNo) {
        if (base == null || reconciledNextTransactionNo < 1L) {
            throw new DatabaseValidationException("history counter recovery inputs are invalid");
        }
        final long historyNext;
        try {
            historyNext = Math.addExact(base.lastTransactionNo().value(), 1L);
        } catch (ArithmeticException overflow) {
            throw new TransactionRecoveryException("rseg lastTransactionNo cannot advance after recovery", overflow);
        }
        return Math.max(reconciledNextTransactionNo, historyNext);
    }

    /**
     * 校验节点读取器没有返回 null 或其它 first page 的快照。
     *
     * @param node           读取器返回的无 latch 节点快照。
     * @param expectedPageId 当前物理链位置要求的稳定 first-page identity。
     * @return identity 精确匹配的原节点快照。
     * @throws TransactionRecoveryException 节点缺失或 page id 错配时抛出。
     */
    private static UndoHistoryNodeSnapshot requireNode(UndoHistoryNodeSnapshot node, PageId expectedPageId) {
        if (node == null || !node.firstPageId().equals(expectedPageId)) {
            throw new TransactionRecoveryException("history node reader returned the wrong page: expected="
                    + expectedPageId + ", current=" + (node == null ? null : node.firstPageId()));
        }
        return node;
    }
}
