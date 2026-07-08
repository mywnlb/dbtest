package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.page.PageType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 0.19a：RedoApplyDispatcher 多 handler 框架。测试只验证分发纪律，不新增持久 redo tag。
 */
class RedoApplyDispatcherHandlerTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final PageId PAGE = PageId.of(SpaceId.of(1), PageNo.of(3));

    @Test
    void dispatcherInvokesHandlersInOriginalRecordOrder() {
        List<String> calls = new ArrayList<>();
        RedoApplyDispatcher dispatcher = RedoApplyDispatcher.withHandlers(List.of(
                new RecordingHandler("init", PageInitRecord.class, calls),
                new RecordingHandler("bytes", PageBytesRecord.class, calls)));
        RedoLogBatch batch = batchOf(List.of(
                new PageInitRecord(PAGE, PageType.INDEX),
                new PageBytesRecord(PAGE, 100, new byte[]{1}),
                new PageBytesRecord(PAGE, 101, new byte[]{2})));

        RedoApplySummary summary = dispatcher.apply(batch, context());

        assertEquals(List.of("init:apply:PageInitRecord",
                "bytes:apply:PageBytesRecord",
                "bytes:apply:PageBytesRecord",
                "init:finish",
                "bytes:finish"), calls);
        assertEquals(1, summary.scannedBatchCount());
        assertEquals(1, summary.appliedBatchCount());
        assertEquals(0, summary.skippedRecordCount());
    }

    @Test
    void dispatcherRejectsAmbiguousHandlerConfiguration() {
        List<String> calls = new ArrayList<>();
        RedoApplyDispatcher dispatcher = RedoApplyDispatcher.withHandlers(List.of(
                new RecordingHandler("a", PageInitRecord.class, calls),
                new RecordingHandler("b", PageInitRecord.class, calls)));
        RedoLogBatch batch = batchOf(List.of(new PageInitRecord(PAGE, PageType.INDEX)));

        assertThrows(DatabaseValidationException.class, () -> dispatcher.apply(batch, context()));
    }

    @Test
    void dispatcherRejectsRecordWithoutHandler() {
        RedoApplyDispatcher dispatcher = RedoApplyDispatcher.withHandlers(List.of());
        RedoLogBatch batch = batchOf(List.of(new PageInitRecord(PAGE, PageType.INDEX)));

        assertThrows(DatabaseValidationException.class, () -> dispatcher.apply(batch, context()));
    }

    @Test
    void dispatcherSkipsRecordBeforeOpeningHandlerWhenAffectedPageIsSkipped() {
        List<String> calls = new ArrayList<>();
        RedoApplyDispatcher dispatcher = RedoApplyDispatcher.withHandlers(List.of(
                new RecordingHandler("page", RedoRecord.class, calls)));
        RedoLogBatch batch = batchOf(List.of(
                new PageInitRecord(PAGE, PageType.INDEX),
                new PageBytesRecord(PageId.of(SpaceId.of(9), PageNo.of(4)), 100, new byte[]{1})));

        RedoApplySummary summary = dispatcher.apply(batch, context(), pageId -> pageId.spaceId().value() == 9);

        assertEquals(List.of("page:apply:PageInitRecord", "page:finish"), calls);
        assertEquals(1, summary.scannedBatchCount());
        assertEquals(1, summary.appliedBatchCount());
        assertEquals(1, summary.skippedRecordCount());
    }

    private static RedoApplyContext context() {
        PageStore store = new FileChannelPageStore();
        return new RedoApplyContext(store, PS);
    }

    private static RedoLogBatch batchOf(List<RedoRecord> records) {
        long end = 100;
        for (RedoRecord record : records) {
            end += record.byteLength();
        }
        return new RedoLogBatch(new LogRange(Lsn.of(100), Lsn.of(end)), records);
    }

    private static final class RecordingHandler implements RedoApplyHandler {

        private final String name;
        private final Class<?> supportedClass;
        private final List<String> calls;

        private RecordingHandler(String name, Class<?> supportedClass, List<String> calls) {
            this.name = name;
            this.supportedClass = supportedClass;
            this.calls = calls;
        }

        @Override
        public boolean supports(RedoRecord record) {
            return supportedClass.isInstance(record);
        }

        @Override
        public List<PageId> affectedPages(RedoRecord record) {
            if (record instanceof PageInitRecord pir) {
                return List.of(pir.pageId());
            }
            if (record instanceof PageBytesRecord pbr) {
                return List.of(pbr.pageId());
            }
            return List.of();
        }

        @Override
        public RedoApplyBatchHandler openBatch(LogRange range, RedoApplyContext context) {
            return new RedoApplyBatchHandler() {
                @Override
                public void apply(RedoRecord record) {
                    calls.add(name + ":apply:" + record.getClass().getSimpleName());
                }

                @Override
                public void finish() {
                    calls.add(name + ":finish");
                }
            };
        }
    }
}
