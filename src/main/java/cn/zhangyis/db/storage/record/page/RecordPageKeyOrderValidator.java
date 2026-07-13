package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.schema.TypeId;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.util.List;

/**
 * INDEX 页 schema-aware 用户链校验器。物理 {@link RecordPageStructureValidator} 只知道 page header、heap、FREE 链和
 * PageDirectory，本类由持有 B+Tree 元数据的上层传入 schema/keyDef，补充记录类型、字符编码与 key 非降序不变量。
 *
 * <p>并发边界：调用方必须在目标页 S/X latch 内调用。本类只读页快照，不获取新 latch/事务锁，不写页、不收集 redo、
 * 不尝试修复。每次既有页打开执行 O(n) 相邻扫描，优先保证教学实现的 fail-closed 正确性。
 */
public final class RecordPageKeyOrderValidator {

    /** 共享只读类型入口；字符严格解码与 record cursor 布局解析均复用它。 */
    private final TypeCodecRegistry registry;

    /** 两条记录间的权威复合 key 比较器。 */
    private final RecordComparator comparator;

    /** 创建绑定统一类型/字符语义的页内 key 校验器。 */
    public RecordPageKeyOrderValidator(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("key order validator registry must not be null");
        }
        this.registry = registry;
        this.comparator = new RecordComparator(registry);
    }

    /**
     * 校验一张已通过物理结构校验的 INDEX 页。
     *
     * <p>数据流：先沿 {@link RecordPage#recordOffsetsInOrder()} 取得物理用户链；该步骤的异常保留为
     * {@link PageDirectoryCorruptedException}。随后逐条校验 expectedType、按 schema 解析 key 字段、对非 NULL 字符 key
     * 严格解码完整 slice，最后比较相邻记录并要求 previous&lt;=current。delete-mark 不改变 key 且仍在链中，因此不会跳过。
     * 任一字段/charset/collation 错误包装为 {@link RecordKeyOrderCorruptedException} 并保留 cause。
     *
     * @param pageId 物理页定位，仅用于损坏诊断。
     * @param page 已持 S/X latch 的 INDEX 页视图。
     * @param schema 当前页用户记录的权威 schema；internal 页应传派生 node-pointer schema。
     * @param keyDef 当前页内顺序的权威 key 定义。
     * @param expectedType leaf 为 CONVENTIONAL，internal 为 NODE_POINTER。
     */
    public void validate(PageId pageId, RecordPage page, TableSchema schema,
                         IndexKeyDef keyDef, RecordType expectedType) {
        if (pageId == null || page == null || schema == null || keyDef == null || expectedType == null) {
            throw new DatabaseValidationException("key order validation inputs must not be null");
        }
        if (expectedType != RecordType.CONVENTIONAL && expectedType != RecordType.NODE_POINTER) {
            throw new DatabaseValidationException("key order expected type must be CONVENTIONAL or NODE_POINTER: "
                    + expectedType);
        }

        // 物理链错误必须维持 PageDirectoryCorruptedException，不能包装成较高层 key 语义损坏。
        List<Integer> offsets = page.recordOffsetsInOrder();
        RecordCursor previous = null;
        int previousOffset = -1;
        for (int currentOffset : offsets) {
            RecordCursor current;
            try {
                current = new RecordCursor(page, currentOffset, schema, registry);
                if (current.recordType() != expectedType) {
                    throw typeMismatch(pageId, previousOffset, previous, currentOffset, current, expectedType);
                }
                validateKeyFields(current, schema, keyDef);
                if (previous != null && comparator.compare(previous, current, keyDef, schema) > 0) {
                    throw new RecordKeyOrderCorruptedException("index page key order corrupted: pageId=" + pageId
                            + " previousOffset=" + previousOffset + " previousType=" + previous.recordType()
                            + " currentOffset=" + currentOffset + " currentType=" + current.recordType());
                }
            } catch (RecordKeyOrderCorruptedException e) {
                throw e;
            } catch (DatabaseRuntimeException e) {
                throw new RecordKeyOrderCorruptedException("index page key field corrupted: pageId=" + pageId
                        + " previousOffset=" + diagnosticOffset(previousOffset)
                        + " currentOffset=" + currentOffset + " expectedType=" + expectedType, e);
            }
            previous = current;
            previousOffset = currentOffset;
        }
    }

    /**
     * 触发 key 字段布局解析，并在 prefix 截断前严格解码完整 CHAR/VARCHAR slice。
     * ASCII_CI/BINARY 比较策略本身按字节工作，不会发现 malformed UTF-8，故该步骤不能省略。
     */
    private void validateKeyFields(RecordCursor cursor, TableSchema schema, IndexKeyDef keyDef) {
        for (KeyPartDef part : keyDef.parts()) {
            ColumnType type = schema.column(part.columnId().value()).type();
            boolean isNull = cursor.isNull(part.columnId());
            if (!isNull && (type.typeId() == TypeId.CHAR || type.typeId() == TypeId.VARCHAR)) {
                cursor.readColumn(part.columnId());
            }
        }
    }

    /** record type 不符合页层级语义时生成包含相邻定位的直接损坏异常。 */
    private static RecordKeyOrderCorruptedException typeMismatch(
            PageId pageId, int previousOffset, RecordCursor previous,
            int currentOffset, RecordCursor current, RecordType expectedType) {
        return new RecordKeyOrderCorruptedException("index page record type corrupted: pageId=" + pageId
                + " previousOffset=" + diagnosticOffset(previousOffset)
                + " previousType=" + (previous == null ? "none" : previous.recordType())
                + " currentOffset=" + currentOffset + " currentType=" + current.recordType()
                + " expectedType=" + expectedType);
    }

    /** 首条记录没有 previous offset，用文本 none 避免与真实页内偏移混淆。 */
    private static String diagnosticOffset(int offset) {
        return offset < 0 ? "none" : Integer.toString(offset);
    }
}
