package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.schema.CharsetId;
import cn.zhangyis.db.storage.record.schema.CollationId;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.InvalidCharacterEncodingException;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * schema-aware 页内 key 顺序校验。测试刻意先构造通过物理结构校验的页，再只破坏字段值或 record type，
 * 证明该校验补的是物理 validator 无法识别的索引语义损坏。
 */
class RecordPageKeyOrderValidatorTest {

    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE_ID = SpaceId.of(17);
    private static final PageId PAGE_ID = PageId.of(SPACE_ID, PageNo.of(3));
    private static final long INDEX_ID = 7L;

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordPageInserter inserter = new RecordPageInserter(registry);
    private final RecordPageKeyOrderValidator validator = new RecordPageKeyOrderValidator(registry);

    /** 空页、单记录、重复 key 与 delete-mark 都不破坏页内非降序不变量。 */
    @Test
    void acceptsEmptySingleDuplicateAndDeleteMarkedRecords() {
        onPage(intSchema(), (page, guard) -> {
            assertDoesNotThrow(() -> validate(page, intSchema(), intKey(KeyOrder.ASC), RecordType.CONVENTIONAL));
            insert(page, intSchema(), intKey(KeyOrder.ASC), intRow(1, RecordType.CONVENTIONAL));
            assertDoesNotThrow(() -> validate(page, intSchema(), intKey(KeyOrder.ASC), RecordType.CONVENTIONAL));
            insert(page, intSchema(), intKey(KeyOrder.ASC), intRow(1, RecordType.CONVENTIONAL));
            page.setDeleted(page.recordOffsetsInOrder().get(0), true);
            assertDoesNotThrow(() -> validate(page, intSchema(), intKey(KeyOrder.ASC), RecordType.CONVENTIONAL));
        });
    }

    /** ASC/DESC 都按声明索引序校验，而不是固定使用字段自然序。 */
    @Test
    void acceptsAscendingAndDescendingPageOrder() {
        onPage(intSchema(), (page, guard) -> {
            insert(page, intSchema(), intKey(KeyOrder.ASC), intRow(3, RecordType.CONVENTIONAL));
            insert(page, intSchema(), intKey(KeyOrder.ASC), intRow(1, RecordType.CONVENTIONAL));
            insert(page, intSchema(), intKey(KeyOrder.ASC), intRow(2, RecordType.CONVENTIONAL));
            assertDoesNotThrow(() -> validate(page, intSchema(), intKey(KeyOrder.ASC), RecordType.CONVENTIONAL));

            page.format(INDEX_ID, 0);
            insert(page, intSchema(), intKey(KeyOrder.DESC), intRow(1, RecordType.CONVENTIONAL));
            insert(page, intSchema(), intKey(KeyOrder.DESC), intRow(3, RecordType.CONVENTIONAL));
            insert(page, intSchema(), intKey(KeyOrder.DESC), intRow(2, RecordType.CONVENTIONAL));
            assertDoesNotThrow(() -> validate(page, intSchema(), intKey(KeyOrder.DESC), RecordType.CONVENTIONAL));
        });
    }

    /** NULL、ASCII_CI 与 byte-prefix 等价允许相邻记录比较为 0。 */
    @Test
    void acceptsNullCollationAndBytePrefixEquivalence() {
        TableSchema schema = stringSchema();
        IndexKeyDef key = stringKey(5);
        onPage(schema, (page, guard) -> {
            insert(page, schema, key, stringRow(ColumnValue.NullValue.INSTANCE));
            insert(page, schema, key, stringRow(new ColumnValue.StringValue("Apple-x")));
            insert(page, schema, key, stringRow(new ColumnValue.StringValue("apple-y")));
            assertDoesNotThrow(() -> validate(page, schema, key, RecordType.CONVENTIONAL));
        });
    }

    /** 等长改写只改变字段值，物理链/heap/directory 仍合法，但 schema-aware 校验必须拒绝逆序。 */
    @Test
    void rejectsPhysicallyValidButDescendingKeyInAscendingPage() {
        onPage(intSchema(), (page, guard) -> {
            insert(page, intSchema(), intKey(KeyOrder.ASC), intRow(1, RecordType.CONVENTIONAL));
            insert(page, intSchema(), intKey(KeyOrder.ASC), intRow(2, RecordType.CONVENTIONAL));
            RecordPageStructureValidator.validate(page);

            int first = page.recordOffsetsInOrder().get(0);
            replaceFieldBytes(page, first, intRow(3, RecordType.CONVENTIONAL), intSchema());
            RecordPageStructureValidator.validate(page);

            RecordKeyOrderCorruptedException error = assertThrows(RecordKeyOrderCorruptedException.class,
                    () -> validate(page, intSchema(), intKey(KeyOrder.ASC), RecordType.CONVENTIONAL));
            assertTrue(error.getMessage().contains(PAGE_ID.toString()));
            assertTrue(error.getMessage().contains("previousOffset=" + first));
        });
    }

    /** leaf 与 internal 页不能混用 CONVENTIONAL/NODE_POINTER 用户记录。 */
    @Test
    void rejectsUnexpectedUserRecordType() {
        onPage(intSchema(), (page, guard) -> {
            insert(page, intSchema(), intKey(KeyOrder.ASC), intRow(1, RecordType.NODE_POINTER));
            assertThrows(RecordKeyOrderCorruptedException.class,
                    () -> validate(page, intSchema(), intKey(KeyOrder.ASC), RecordType.CONVENTIONAL));
            assertDoesNotThrow(() -> validate(page, intSchema(), intKey(KeyOrder.ASC), RecordType.NODE_POINTER));
            assertThrows(DatabaseValidationException.class,
                    () -> validate(page, intSchema(), intKey(KeyOrder.ASC), RecordType.INFIMUM));
        });
    }

    /** 单记录页没有相邻比较，仍必须严格解码完整 UTF-8 字段并保留字符异常根因。 */
    @Test
    void rejectsMalformedUtf8EvenOnSingleRecordAndKeepsCause() {
        TableSchema schema = stringSchema();
        IndexKeyDef key = stringKey(0);
        onPage(schema, (page, guard) -> {
            insert(page, schema, key, stringRow(new ColumnValue.StringValue("aa")));
            int offset = page.recordOffsetsInOrder().get(0);
            RecordCursor cursor = new RecordCursor(page, offset, schema, registry);
            int fieldOffset = cursor.columnSlice(new ColumnId(0)).offset();
            byte[] bytes = page.readRecordBytes(offset);
            bytes[fieldOffset] = (byte) 0xC3;
            page.writeRecordBytes(offset, bytes);
            RecordPageStructureValidator.validate(page);

            RecordKeyOrderCorruptedException error = assertThrows(RecordKeyOrderCorruptedException.class,
                    () -> validate(page, schema, key, RecordType.CONVENTIONAL));
            assertInstanceOf(InvalidCharacterEncodingException.class, error.getCause());
        });
    }

    /** 物理 next_record 损坏继续保留 PageDirectoryCorruptedException，不伪装成 key 顺序损坏。 */
    @Test
    void preservesPhysicalChainCorruptionException() {
        onPage(intSchema(), (page, guard) -> {
            insert(page, intSchema(), intKey(KeyOrder.ASC), intRow(1, RecordType.CONVENTIONAL));
            page.setNextRecord(page.infimumOffset(), 0);
            assertThrows(PageDirectoryCorruptedException.class,
                    () -> validate(page, intSchema(), intKey(KeyOrder.ASC), RecordType.CONVENTIONAL));
        });
    }

    private void validate(RecordPage page, TableSchema schema, IndexKeyDef keyDef, RecordType expectedType) {
        validator.validate(PAGE_ID, page, schema, keyDef, expectedType);
    }

    private void insert(RecordPage page, TableSchema schema, IndexKeyDef keyDef, LogicalRecord record) {
        inserter.insert(page, PAGE_ID, record, keyDef, schema);
    }

    /** 保留原记录头，仅用等长目标记录覆盖字段区，确保测试破坏点只有 key 值。 */
    private void replaceFieldBytes(RecordPage page, int offset, LogicalRecord replacement, TableSchema schema) {
        byte[] original = page.readRecordBytes(offset);
        byte[] encoded = new RecordEncoder(registry).encode(replacement, schema);
        assertEquals(original.length, encoded.length);
        System.arraycopy(encoded, IndexPageLayout.REC_HEADER_BYTES, original, IndexPageLayout.REC_HEADER_BYTES,
                original.length - IndexPageLayout.REC_HEADER_BYTES);
        page.writeRecordBytes(offset, original);
    }

    private static TableSchema intSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0)));
    }

    private static IndexKeyDef intKey(KeyOrder order) {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), order, 0)));
    }

    private static LogicalRecord intRow(long id, RecordType type) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id)), false, type);
    }

    private static TableSchema stringSchema() {
        return new TableSchema(1, List.of(new ColumnDef(new ColumnId(0), "name",
                ColumnType.varchar(20, true, CharsetId.UTF8, CollationId.UTF8_ASCII_CI), 0)));
    }

    private static IndexKeyDef stringKey(int prefixBytes) {
        return new IndexKeyDef(INDEX_ID,
                List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, prefixBytes)));
    }

    private static LogicalRecord stringRow(ColumnValue value) {
        return new LogicalRecord(1, List.of(value), false, RecordType.CONVENTIONAL);
    }

    private interface PageBody {
        void run(RecordPage page, PageGuard guard);
    }

    /** 建立一张已 format 的 INDEX 页，回调期间持有 X latch，所有损坏均限制在临时文件。 */
    private void onPage(TableSchema schema, PageBody body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE_ID, dir.resolve("key-order-" + System.nanoTime() + ".ibd"), PAGE_SIZE, PageNo.of(4));
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PAGE_SIZE, 4);
             PageGuard guard = pool.getPage(PAGE_ID, PageLatchMode.EXCLUSIVE)) {
            RecordPage page = new RecordPage(guard, PAGE_SIZE);
            page.format(INDEX_ID, 0);
            body.run(page, guard);
        }
    }
}
