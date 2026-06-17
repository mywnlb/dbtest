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
     * @throws DatabaseValidationException 目标为系统记录或未 delete-marked。
     * @throws PageDirectoryCorruptedException 前驱/owner 槽定位失败（页损坏）。
     */
    public void purge(RecordPage page, int recordOffset) {
        // ---------- plan：只读 + 校验 ----------
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
        page.writeHeader(page.header().withNRecs(page.header().nRecs() - 1));
    }
}
