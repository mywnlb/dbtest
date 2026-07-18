package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.util.ArrayList;
import java.util.List;

/**
 * INDEX 页内 key 有序插入（innodb-record-design §10.1）。把一条逻辑记录编码后按 key 顺序串入 next_record 链，
 * 并维护 PageDirectory 的 owner {@code n_owned} 与组划分。全程在调用方持有的单页 X latch 内完成
 * （PageGuard 写原语自校验 EXCLUSIVE）；本片不跨页、不写 undo/redo、不等行锁——split/merge、跨页导航归 B+Tree。
 *
 * <p>无状态、线程安全：仅持只读 {@link RecordEncoder}/{@link RecordPageSearch}（registry 只读）。
 *
 * <p>ownership 关键约束（§7、spec §6.3）：每条记录的 {@code n_owned} 只在 group 末记录（被某 slot 指向者）非 0；
 * 新插入的用户记录 {@code n_owned=0}（编码默认）。插入后沿链找到所属 group 的末记录（owner）将其 {@code n_owned}++；
 * 超过 {@link #MAX_N_OWNED} 时把该组从中部切成两组，使两组各约半数，维持目录二分效率。
 */
public final class RecordPageInserter {

    /** 单 group 最小成员数（对齐 InnoDB PAGE_DIR_SLOT_MIN_N_OWNED）。split 后新组取此值。 */
    static final int MIN_N_OWNED = 4;
    /** 单 group 最大成员数（对齐 InnoDB PAGE_DIR_SLOT_MAX_N_OWNED）。owner 超此值触发 split。 */
    static final int MAX_N_OWNED = 8;

    /** 逻辑记录 → 物理字节（含 schemaVersion/列数/长度校验）。 */
    private final RecordEncoder encoder;
    /** key 有序前驱定位（findInsertPosition）。 */
    private final RecordPageSearch search;

    /**
     * 创建 {@code RecordPageInserter}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param registry 由组合根提供的 {@code TypeCodecRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecordPageInserter(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("type codec registry must not be null");
        }
        this.encoder = new RecordEncoder(registry);
        this.search = new RecordPageSearch(registry);
    }

    /**
     * 按 key 顺序插入一条记录（要求 X），返回其短期定位 {@link RecordRef}。
     *
     * <p>数据流（§10.1）：编码 → 取 key 列值 → 二分+线扫定位前驱 prev → 从 free space 切 heap 空间（页满在任何
     * 修改前抛 {@link RecordPageOverflowException}，是干净的整体失败）→ 落字节、回填 heapNo、链入
     * {@code prev -> new -> next(prev)} → 更新 header 计数器（nRecs/lastInsert/direction）→ 维护 owner n_owned
     * 并按需 split。允许重复 key（插到相等记录之后，稳定；唯一性由上层保证）。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param page   目标 INDEX 页（须已 format，X latch 有效）。
     * @param pageId 该页页号，用于产出 RecordRef。
     * @param rec    待插逻辑记录（列序同 schema；recordType 通常 CONVENTIONAL）。
     * @param keyDef 索引 key 定义（决定取哪些列、ASC/DESC）。
     * @param schema 表结构（编码与比较的权威类型来源）。
     * @return {@code insert} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecordRef insert(RecordPage page, PageId pageId, LogicalRecord rec, IndexKeyDef keyDef, TableSchema schema) {
        if (page == null || pageId == null || rec == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("inserter args must not be null");
        }
        // 1. 编码：含 schemaVersion/列数/长度校验；编码头的 n_owned 默认 0（新记录非组末，关键不变量）。
        byte[] bytes = encoder.encode(rec, schema);
        // 2. 取 key 列值，组 SearchKey。
        SearchKey key = keyOf(rec, keyDef);
        // 3. 定位前驱：key ≤ newKey 的最后一条（至少 infimum）。
        int prev = search.findInsertPosition(page, key, keyDef, schema);
        // 4. 分配 heap 空间：优先复用 GarbageList 碎片（first-fit），否则 FreeSpace；页满在此抛 overflow，
        //    此前未做任何修改，失败是整体干净的（无部分链入）。复用碎片沿用其 heapNo，物理落点与链序解耦（链=key 序）。
        HeapSpaceManager.Allocation alloc = new HeapSpaceManager(page).allocate(bytes.length);
        int heapNo = alloc.heapNo();
        int off = alloc.offset();
        // 5. 落记录字节 + 回填真实 heapNo + 链入 prev -> off -> next。
        page.writeRecordBytes(off, bytes);
        page.setHeapNo(off, heapNo);
        int next = page.nextRecord(prev);
        page.setNextRecord(off, next);
        page.setNextRecord(prev, off);
        // 6. 更新 header 计数器。
        updateInsertStats(page, off);
        // 7. 维护 PageDirectory owner 的 n_owned（按链定位 owner），超 MAX 则 split。
        maintainOwnership(page, off);
        // 8. 产出短期定位值（reorganize/split 后须重新校验，§5.1）。
        return new RecordRef(pageId, heapNo, off, schema.schemaVersion(), keyDef.indexId());
    }

    /** 按 keyDef 的 part 顺序取 rec 的列值组 SearchKey（NULL 列以 NullValue 表达，由列值本身携带）。 */
    private SearchKey keyOf(LogicalRecord rec, IndexKeyDef keyDef) {
        List<ColumnValue> vals = new ArrayList<>(keyDef.parts().size());
        for (KeyPartDef part : keyDef.parts()) {
            vals.add(rec.columnValues().get(part.columnId().value()));
        }
        return new SearchKey(vals);
    }

    /**
     * insert 后更新 header 计数器（read-modify-write，单 X latch 内）：nRecs++、lastInsert=新记录偏移、
     * 方向简化为 RIGHT 且 nDirection++（不实现方向驱动 split 优化，归 B+Tree；简化点）。
     * heapTop/nHeap 已由 {@link RecordPage#allocateFromFreeSpace} 推进，此处读到的即最新值，回写为同值。
     */
    private void updateInsertStats(RecordPage page, int off) {
        IndexPageHeader h = page.header();
        IndexPageHeader updated = new IndexPageHeader(
                h.nDirSlots(), h.heapTop(), h.nHeap(), h.free(), h.garbage(),
                off, IndexPageDirection.RIGHT, h.nDirection() + 1, h.nRecs() + 1, h.level(), h.indexId());
        page.writeHeader(updated);
    }

    /**
     * 维护新记录所属 group 的 owner（组末记录）n_owned：沿 next_record 从新记录走到首个 {@code n_owned>0}
     * 的记录即 owner（supremum 恒 {@code n_owned≥1}，循环必终止；前向遍历不回头，owner 永不为 infimum）。
     * owner.n_owned++ 后若超 {@link #MAX_N_OWNED} 则 {@link #splitGroup} 切分。
     */
    private void maintainOwnership(RecordPage page, int off) {
        int ownerOff = off;
        while (page.recordHeaderAt(ownerOff).nOwned() == 0) {
            ownerOff = page.nextRecord(ownerOff);
        }
        int newOwned = page.recordHeaderAt(ownerOff).nOwned() + 1;
        page.setNOwned(ownerOff, newOwned);
        if (newOwned > MAX_N_OWNED) {
            splitGroup(page, ownerOff, newOwned);
        }
    }

    /**
     * 把 owner 所在的超限 group 切成两组（§6.3）：新组取前 {@link #MIN_N_OWNED} 条，owner 组留其余。
     *
     * <p>数据流：线扫目录定位 owner 的槽下标 h（owner 必被某槽指向，h≥1）；{@code base = slot(h-1)} 记录为前一组末
     * （h-1=0 时为 infimum）；**从 base 起沿 next 走 MIN_N_OWNED 步**得新组末 {@code mid}（即 owner 组前 4 条的第 4 条，
     * 非 base.next 走 4 步以免多算一条）。在槽 h 插入指向 mid 的新槽（mid.key &lt; owner.key，故 mid 槽在 owner 槽之前），
     * 再置 {@code n_owned(mid)=MIN_N_OWNED}、{@code n_owned(owner)=newOwned-MIN_N_OWNED}，两组各 ∈ [MIN..MAX]。
     *
     * <p>尽力而为（spec §6.3）：若 {@code insertSlot} 因页尾无空间抛 {@link RecordPageOverflowException}，吞掉并跳过
     * split——记录已成功链入、owner 计数已是真实值（暂超 MAX），只是该组线扫变长，查找仍正确，待 reorganize 整理。
     * 用记录 offset（base/mid/ownerOff）持有而非 slot index：insertSlot 后下标漂移，offset 稳定。
     */
    private void splitGroup(RecordPage page, int ownerOff, int newOwned) {
        RecordPageDirectory dir = page.directory();
        int h = slotIndexOf(dir, ownerOff);
        if (h <= 0) {
            // 防御：owner 应为某槽记录且非 infimum 槽（h≥1）。定位异常则不拆，留超限组（不破坏查找正确性）。
            return;
        }
        int base = dir.slot(h - 1);
        int mid = base;
        for (int i = 0; i < MIN_N_OWNED; i++) {
            mid = page.nextRecord(mid);
        }
        try {
            dir.insertSlot(h, mid);
        } catch (RecordPageOverflowException overflow) {
            return;
        }
        page.setNOwned(mid, MIN_N_OWNED);
        page.setNOwned(ownerOff, newOwned - MIN_N_OWNED);
    }

    /** 线扫目录找指向 recordOffset 的槽下标；未找到返回 -1。 */
    private int slotIndexOf(RecordPageDirectory dir, int recordOffset) {
        int n = dir.slotCount();
        for (int i = 0; i < n; i++) {
            if (dir.slot(i) == recordOffset) {
                return i;
            }
        }
        return -1;
    }
}
