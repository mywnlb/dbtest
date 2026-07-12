package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.format.RecordHeader;
import cn.zhangyis.db.storage.record.format.RecordType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 已格式化 INDEX 页的完整物理结构校验器。调用方必须已持有该页 S/X latch；本类只读取 {@link RecordPage}
 * 暴露的页内原语，不拥有 latch/fix、不访问文件，也不解析 schema、key 或事务可见性。
 *
 * <p>校验按 header/system records → next-record/user headers → directory/nOwned 的顺序执行。任何损坏都统一为
 * {@link PageDirectoryCorruptedException}，使 B+Tree 在字段解析或写页前 fail-closed；低层格式/边界异常保留为 cause。
 */
public final class RecordPageStructureValidator {

    private RecordPageStructureValidator() {
    }

    /**
     * 校验一个已格式化 INDEX 页的全部已建模结构不变量。
     *
     * @param page 调用方在有效 S/X latch 内持有的页视图。
     * @throws DatabaseValidationException page 为 null，属于调用方错误。
     * @throws PageDirectoryCorruptedException header、系统记录、用户链或目录账本不一致。
     */
    public static void validate(RecordPage page) {
        if (page == null) {
            throw new DatabaseValidationException("record page to validate must not be null");
        }
        try {
            validateStructure(page);
        } catch (PageDirectoryCorruptedException error) {
            throw error;
        } catch (DatabaseRuntimeException error) {
            throw new PageDirectoryCorruptedException(
                    "record page structure could not be decoded safely", error);
        }
    }

    /** 串联三段纯读校验；每段只消费前一段已经验证并冻结在局部值对象中的结果。 */
    private static void validateStructure(RecordPage page) {
        IndexPageHeader header = page.header();
        validateHeaderGeometry(page, header);
        SystemRecords system = validateSystemRecords(page);
        UserChain chain = validateUserChain(page, header, system);
        validateDirectory(page, header, system, chain);
    }

    /** 校验 header 计数与 heap/directory/trailer 几何，防止后续偏移读取越出已使用区域。 */
    private static void validateHeaderGeometry(RecordPage page, IndexPageHeader header) {
        int directoryStart = page.dirStart();
        if (directoryStart < IndexPageLayout.USER_RECORDS_START) {
            throw corrupted("page directory overlaps page header/record heap: dirStart=" + directoryStart);
        }
        if (header.heapTop() < IndexPageLayout.USER_RECORDS_START || header.heapTop() > directoryStart) {
            throw corrupted("heapTop outside record heap/free-space geometry: heapTop=" + header.heapTop()
                    + ", dirStart=" + directoryStart);
        }
        if (header.nRecs() > header.nHeap() - 2) {
            throw corrupted("user record count exceeds allocated heap numbers: nRecs=" + header.nRecs()
                    + ", nHeap=" + header.nHeap());
        }
    }

    /** 校验固定系统记录 identity/标签；supremum 的 nOwned 可随尾 group 变化，不能固定为 1。 */
    private static SystemRecords validateSystemRecords(RecordPage page) {
        RecordHeader infimum = page.recordHeaderAt(page.infimumOffset());
        requireSystemHeader("infimum", infimum, RecordType.INFIMUM, 0);
        if (infimum.nOwned() != 1) {
            throw corrupted("infimum must own exactly itself: nOwned=" + infimum.nOwned());
        }
        requireLabel("infimum", IndexPageLayout.INFIMUM_LABEL, page.systemLabelAt(page.infimumOffset()));

        RecordHeader supremum = page.recordHeaderAt(page.supremumOffset());
        requireSystemHeader("supremum", supremum, RecordType.SUPREMUM, 1);
        if (supremum.nextRecordOffset() != 0) {
            throw corrupted("supremum must terminate next_record chain: next="
                    + supremum.nextRecordOffset());
        }
        requireLabel("supremum", IndexPageLayout.SUPREMUM_LABEL, page.systemLabelAt(page.supremumOffset()));
        return new SystemRecords(infimum, supremum);
    }

    /** 固定系统记录不能伪装成用户记录、delete-mark，也不能改变 heap identity 或物理长度。 */
    private static void requireSystemHeader(String name, RecordHeader header, RecordType type, int heapNo) {
        if (header.recordType() != type || header.heapNo() != heapNo
                || header.recordLength() != IndexPageLayout.SYS_REC_BYTES || header.deletedFlag()) {
            throw corrupted(name + " system record header mismatch: type=" + header.recordType()
                    + ", heapNo=" + header.heapNo() + ", length=" + header.recordLength()
                    + ", deleted=" + header.deletedFlag());
        }
    }

    private static void requireLabel(String name, byte[] expected, byte[] actual) {
        if (!java.util.Arrays.equals(expected, actual)) {
            throw corrupted(name + " system record label mismatch");
        }
    }

    /**
     * 从 infimum 唯一遍历至 supremum，同时验证用户 header、heapNo、recordLength 与物理区间不重叠。
     * 先验证至少可读完整 header，再解析 recordLength，避免损坏 offset 借 free-space 字节伪造记录。
     */
    private static UserChain validateUserChain(RecordPage page, IndexPageHeader pageHeader,
                                               SystemRecords system) {
        Map<Integer, RecordHeader> headers = new HashMap<>();
        Map<Integer, Integer> positions = new HashMap<>();
        Set<Integer> heapNumbers = new HashSet<>();
        List<RecordRange> ranges = new ArrayList<>();
        int current = system.infimum().nextRecordOffset();
        int position = 1;
        while (current != page.supremumOffset()) {
            if (current == 0) {
                throw corrupted("next_record chain reached zero before supremum");
            }
            if (positions.containsKey(current)) {
                throw corrupted("next_record chain repeats/cycles at offset " + current);
            }
            if (current < IndexPageLayout.USER_RECORDS_START
                    || (long) current + IndexPageLayout.REC_HEADER_BYTES > pageHeader.heapTop()) {
                throw corrupted("user record header outside used heap: offset=" + current
                        + ", heapTop=" + pageHeader.heapTop());
            }
            RecordHeader header = page.recordHeaderAt(current);
            if (header.recordType() != RecordType.CONVENTIONAL
                    && header.recordType() != RecordType.NODE_POINTER) {
                throw corrupted("user chain contains system record type at offset " + current
                        + ": " + header.recordType());
            }
            if (header.recordLength() < IndexPageLayout.REC_HEADER_BYTES
                    || (long) current + header.recordLength() > pageHeader.heapTop()) {
                throw corrupted("user record length exceeds used heap: offset=" + current
                        + ", length=" + header.recordLength() + ", heapTop=" + pageHeader.heapTop());
            }
            if (header.heapNo() < 2 || header.heapNo() >= pageHeader.nHeap()
                    || !heapNumbers.add(header.heapNo())) {
                throw corrupted("user heapNo is out of range or duplicated: offset=" + current
                        + ", heapNo=" + header.heapNo() + ", nHeap=" + pageHeader.nHeap());
            }
            positions.put(current, position++);
            headers.put(current, header);
            ranges.add(new RecordRange(current, current + header.recordLength()));
            current = header.nextRecordOffset();
        }
        if (headers.size() != pageHeader.nRecs()) {
            throw corrupted("next_record user count differs from nRecs: chain=" + headers.size()
                    + ", nRecs=" + pageHeader.nRecs());
        }
        validateNonOverlappingRanges(ranges);
        positions.put(page.infimumOffset(), 0);
        positions.put(page.supremumOffset(), headers.size() + 1);
        return new UserChain(headers, positions);
    }

    /** key 顺序与物理 offset 无关，因此按 offset 排序后单独验证 live record 区间不得重叠。 */
    private static void validateNonOverlappingRanges(List<RecordRange> ranges) {
        ranges.sort(Comparator.comparingInt(RecordRange::start));
        for (int i = 1; i < ranges.size(); i++) {
            RecordRange previous = ranges.get(i - 1);
            RecordRange current = ranges.get(i);
            if (current.start() < previous.endExclusive()) {
                throw corrupted("live user record ranges overlap: previous=" + previous
                        + ", current=" + current);
            }
        }
    }

    /**
     * 目录槽必须按逻辑链严格前进。相邻 owner 的链位置差就是后一个 owner 的真实 nOwned，因而无需 schema/key
     * 即可交叉验证 group 账本；interior 用户记录必须保持 nOwned=0。
     */
    private static void validateDirectory(RecordPage page, IndexPageHeader pageHeader,
                                          SystemRecords system, UserChain chain) {
        RecordPageDirectory directory = page.directory();
        int slotCount = directory.slotCount();
        if (slotCount != pageHeader.nDirSlots()) {
            throw corrupted("directory slot count differs from header: directory=" + slotCount
                    + ", header=" + pageHeader.nDirSlots());
        }
        if (directory.slot(0) != page.infimumOffset()) {
            throw corrupted("directory slot 0 must point to infimum");
        }
        if (directory.slot(slotCount - 1) != page.supremumOffset()) {
            throw corrupted("last directory slot must point to supremum");
        }

        Set<Integer> owners = new HashSet<>();
        long ownedTotal = 0;
        int previousPosition = -1;
        for (int slot = 0; slot < slotCount; slot++) {
            int ownerOffset = directory.slot(slot);
            Integer ownerPosition = chain.positions().get(ownerOffset);
            if (ownerPosition == null) {
                throw corrupted("directory owner is absent from next_record chain: slot=" + slot
                        + ", offset=" + ownerOffset);
            }
            if (slot > 0 && ownerPosition <= previousPosition) {
                throw corrupted("directory owners do not advance along next_record chain: slot=" + slot
                        + ", position=" + ownerPosition + ", previous=" + previousPosition);
            }
            if (slot > 0 && slot < slotCount - 1 && !chain.headers().containsKey(ownerOffset)) {
                throw corrupted("internal directory slot must point to a user record: slot=" + slot);
            }
            int expectedOwned = slot == 0 ? 1 : ownerPosition - previousPosition;
            RecordHeader ownerHeader = headerAt(ownerOffset, page, system, chain);
            if (ownerHeader.nOwned() != expectedOwned) {
                throw corrupted("directory owner nOwned mismatch: slot=" + slot + ", offset=" + ownerOffset
                        + ", expected=" + expectedOwned + ", actual=" + ownerHeader.nOwned());
            }
            owners.add(ownerOffset);
            ownedTotal += ownerHeader.nOwned();
            previousPosition = ownerPosition;
        }
        for (Map.Entry<Integer, RecordHeader> entry : chain.headers().entrySet()) {
            if (!owners.contains(entry.getKey()) && entry.getValue().nOwned() != 0) {
                throw corrupted("non-owner user record has non-zero nOwned: offset=" + entry.getKey()
                        + ", nOwned=" + entry.getValue().nOwned());
            }
        }
        if (ownedTotal != (long) pageHeader.nRecs() + 2L) {
            throw corrupted("directory nOwned total differs from records plus sentinels: owned="
                    + ownedTotal + ", expected=" + ((long) pageHeader.nRecs() + 2L));
        }
    }

    private static RecordHeader headerAt(int offset, RecordPage page, SystemRecords system, UserChain chain) {
        if (offset == page.infimumOffset()) {
            return system.infimum();
        }
        if (offset == page.supremumOffset()) {
            return system.supremum();
        }
        return chain.headers().get(offset);
    }

    private static PageDirectoryCorruptedException corrupted(String message) {
        return new PageDirectoryCorruptedException(message);
    }

    /** 已校验的固定系统记录头。 */
    private record SystemRecords(RecordHeader infimum, RecordHeader supremum) {
    }

    /** 已校验用户记录头与包含两端哨兵的逻辑链位置。 */
    private record UserChain(Map<Integer, RecordHeader> headers, Map<Integer, Integer> positions) {
    }

    /** 一条 live 用户记录在物理 heap 中的半开区间。 */
    private record RecordRange(int start, int endExclusive) {
    }
}
