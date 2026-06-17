package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.util.OptionalInt;

/**
 * INDEX 页内 key 查找（innodb-record-design §7）。两段式定位：先用 PageDirectory 二分把范围收敛到一个 group，
 * 再沿该 group 的 next_record 链线扫做精确比较。无状态、线程安全（仅持只读 {@link RecordComparator}）。
 *
 * <p>查找入口：{@link #findEqual} 返回相等记录的页内偏移（供读路径）；{@link #findInsertPosition} 返回
 * 「key ≤ 目标 key 的最后一条记录」偏移（= 新记录前驱，供 insert 链入，§10.1）；{@link #findEqualCursor}
 * 命中返回字段级游标、未命中抛 {@link RecordNotFoundException}。
 *
 * <p>哨兵安全（spec §5.3 再检查）：二分循环不变式为 {@code high-low>1}，故 {@code mid} 严格落在 (low,high)，
 * 初始 low=0、high=slotCount-1，于是 mid 命中的恒是真实用户记录，永不取 slot(0)=infimum 或 slot(n-1)=supremum，
 * 二分阶段不会对系统记录做字段比较。线扫阶段以 {@code cur!=supremum}、{@code next(prev)!=supremum} 守卫，
 * 同样避免对 supremum 建字段游标。infimum 只作为 {@code slot(low)} 出现，扫描从其 {@code nextRecord} 起步。
 *
 * <p>简化（后续片）：本片不处理跨页导航/范围扫描游标；只在单页内定位。PageId 在本类方法中不需要——游标定位
 * 仅依赖页内偏移，产出 {@link RecordRef} 才需 PageId，故签名不带 PageId（spec §5.3 的 PageId 参数在此省略）。
 */
public final class RecordPageSearch {

    /** 记录 vs key 的保序比较器（无状态）。 */
    private final RecordComparator comparator;
    /** 类型 codec 注册表；构造页内字段游标时透传给 {@link RecordCursor}。 */
    private final TypeCodecRegistry registry;

    public RecordPageSearch(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("type codec registry must not be null");
        }
        this.registry = registry;
        this.comparator = new RecordComparator(registry);
    }

    /**
     * 二分定位起始 group 的 slot 下标 {@code low}：使 {@code slot(low)} 记录 key &lt; 目标 key（或为 infimum），
     * 且 {@code slot(low+1)} 记录 key ≥ 目标 key。目标记录若存在，必在 {@code (slot(low), slot(low+1)]} 这一组内。
     *
     * <p>收敛：infimum 哨兵恒 cmp&lt;0、supremum 哨兵恒 cmp≥0，保证 low/high 夹逼到 {@code high=low+1}。
     */
    private int startGroupSlotLow(RecordPage page, SearchKey key, IndexKeyDef keyDef, TableSchema schema) {
        RecordPageDirectory dir = page.directory();
        int low = 0;
        int high = dir.slotCount() - 1;
        while (high - low > 1) {
            int mid = (low + high) >>> 1;
            if (compareAt(page, dir.slot(mid), key, keyDef, schema) < 0) {
                low = mid;
            } else {
                high = mid;
            }
        }
        return low;
    }

    /**
     * 查找与 key 相等的记录页内偏移；未命中返回 {@link OptionalInt#empty()}。
     *
     * <p>数据流：二分得起始 group 的 {@code slot(low)} → 从其 {@code nextRecord} 起沿链比较；遇 cmp==0 命中，
     * 遇 cmp&gt;0（记录已大于 key，因链按 key 升序）提前止损，走到 supremum 则未命中。允许重复 key 时返回**首个**相等记录。
     */
    public OptionalInt findEqual(RecordPage page, SearchKey key, IndexKeyDef keyDef, TableSchema schema) {
        RecordPageDirectory dir = page.directory();
        int low = startGroupSlotLow(page, key, keyDef, schema);
        int supremum = page.supremumOffset();
        int cur = page.nextRecord(dir.slot(low));
        while (cur != supremum) {
            int c = compareAt(page, cur, key, keyDef, schema);
            if (c == 0) {
                return OptionalInt.of(cur);
            }
            if (c > 0) {
                return OptionalInt.empty();
            }
            cur = page.nextRecord(cur);
        }
        return OptionalInt.empty();
    }

    /**
     * 返回新记录的**前驱**记录偏移：key ≤ 目标 key 的最后一条记录（至少为 infimum）。insert 据此把新记录链入
     * {@code prev -> new -> next(prev)}（§10.1）。重复 key 时插到所有相等记录之后（稳定，允许重复）。
     *
     * <p>数据流：二分得 {@code slot(low)} 作初始 prev；只要其 next 不是 supremum 且 next 记录 key ≤ 目标 key，
     * 就把 prev 前移到 next。守卫 {@code next(prev)!=supremum} 保证不对 supremum 建字段游标。
     */
    public int findInsertPosition(RecordPage page, SearchKey key, IndexKeyDef keyDef, TableSchema schema) {
        RecordPageDirectory dir = page.directory();
        int low = startGroupSlotLow(page, key, keyDef, schema);
        int supremum = page.supremumOffset();
        int prev = dir.slot(low);
        int next = page.nextRecord(prev);
        while (next != supremum && compareAt(page, next, key, keyDef, schema) <= 0) {
            prev = next;
            next = page.nextRecord(prev);
        }
        return prev;
    }

    /**
     * 命中则返回相等记录的字段级游标，否则抛 {@link RecordNotFoundException}（供读路径「按 key 取行」）。
     *
     * @param pageId 记录所在页，用于游标产出 {@link RecordRef} 时的稳定定位（游标本身不依赖它做字段读）。
     */
    public RecordCursor findEqualCursor(RecordPage page, SearchKey key, IndexKeyDef keyDef, TableSchema schema) {
        OptionalInt found = findEqual(page, key, keyDef, schema);
        if (found.isEmpty()) {
            throw new RecordNotFoundException("no record equal to search key in page");
        }
        return new RecordCursor(page, found.getAsInt(), schema, registry);
    }

    /** 对某偏移记录建临时游标并与 key 比较；对系统记录安全（比较器走 recordType 哨兵分支，游标延迟解析字段）。 */
    private int compareAt(RecordPage page, int recordOffset, SearchKey key, IndexKeyDef keyDef, TableSchema schema) {
        RecordCursor cursor = new RecordCursor(page, recordOffset, schema, registry);
        return comparator.compare(cursor, key, keyDef, schema);
    }
}
