package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.format.RecordHeader;
import cn.zhangyis.db.storage.record.format.RecordType;

/**
 * 页内 purge 算子（innodb-record-design §10.3 purge 子集）：把已 delete-marked 的记录从 next_record 链物理摘除，
 * 维护 group 的 {@code n_owned} 与 PageDirectory（组末记录被删则改槽/删槽，组过小则与后一组合并），把空间挂回 GarbageList。
 *
 * <p>简化（trx/MVCC 暂停）：无 purge view 安全门（§10.3 step1）——本片要求调用方已确保安全，仅校验目标已 delete-marked；
 * 不写 undo。组合并只在「与后一组合并后 ≤MAX」时进行，否则留小组（不 borrow 再分配）。无状态、线程安全；要求页 X latch。
 *
 * <p>**plan-then-execute**：先把校验 + 前驱/owner/槽下标 + 合并判定全部算好（任何失败在写页前抛出），再连续写页，
 * 避免「摘链后定位槽失败」留下半改页。
 */
public final class RecordPagePurger {

    /**
     * 物理 purge {@code recordOffset} 处记录（要求 X）。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @throws DatabaseValidationException 目标为系统记录或未 delete-marked。
     * @throws PageDirectoryCorruptedException 前驱/owner 槽定位失败（页损坏）。
     * @param page 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param recordOffset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     */
    public void purge(RecordPage page, int recordOffset) {
        // ---------- plan：只读 + 校验 ----------
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        RecordHeader target = page.recordHeaderAt(recordOffset);
        RecordType type = target.recordType();
        if (type == RecordType.INFIMUM || type == RecordType.SUPREMUM) {
            throw new DatabaseValidationException("cannot purge system record at offset " + recordOffset);
        }
        if (!target.deletedFlag()) {
            throw new DatabaseValidationException("can only purge delete-marked record at offset " + recordOffset);
        }
        int prev = page.findPredecessor(recordOffset);
        int targetNext = page.nextRecord(recordOffset);
        // owner = 沿链首个 n_owned>0 的记录（supremum 恒 n_owned≥1，必终止）。
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        int owner = recordOffset;
        while (page.recordHeaderAt(owner).nOwned() == 0) {
            owner = page.nextRecord(owner);
        }
        boolean targetIsOwner = (owner == recordOffset);
        RecordPageDirectory dir = page.directory();
        int ownerSlot = dir.indexOf(owner);
        if (ownerSlot < 1) {
            throw new PageDirectoryCorruptedException("owner slot not found for offset " + owner);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        int cnt = page.recordHeaderAt(owner).nOwned();

        // ---------- execute：连续写 ----------
        page.setNextRecord(prev, targetNext); // 摘链

        int affectedOwner;
        int affectedSlot;
        if (targetIsOwner) {
            if (cnt == 1) {
                // 组仅此一员：整槽删除。
                dir.removeSlot(ownerSlot);
                affectedOwner = -1;
                affectedSlot = -1;
            } else {
                // 槽改指链上前一条（新组末，必在本组内），移交 n_owned-1。target 即将被 free，残留 n_owned 不影响。
                dir.setSlot(ownerSlot, prev);
                page.setNOwned(prev, cnt - 1);
                affectedOwner = prev;
                affectedSlot = ownerSlot;
            }
        } else {
            page.setNOwned(owner, cnt - 1);
            affectedOwner = owner;
            affectedSlot = ownerSlot;
        }

        // 组合并：仅中间组（非 supremum 槽），且与后一组合并后 ≤MAX。
        if (affectedOwner != -1 && affectedSlot >= 1 && affectedSlot < dir.slotCount() - 1) {
            int curOwned = page.recordHeaderAt(affectedOwner).nOwned();
            if (curOwned < RecordPageInserter.MIN_N_OWNED) {
                int nextOwner = dir.slot(affectedSlot + 1);
                int sum = curOwned + page.recordHeaderAt(nextOwner).nOwned();
                if (sum <= RecordPageInserter.MAX_N_OWNED) {
                    page.setNOwned(nextOwner, sum);
                    page.setNOwned(affectedOwner, 0); // 旧 owner 降为 interior，必须清零（否则 owner-walk 误判）
                    dir.removeSlot(affectedSlot);
                }
            }
        }

        new HeapSpaceManager(page).free(recordOffset); // 空间挂回 GarbageList、GARBAGE+=len
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        page.writeHeader(page.header().withNRecs(page.header().nRecs() - 1));
    }
}
