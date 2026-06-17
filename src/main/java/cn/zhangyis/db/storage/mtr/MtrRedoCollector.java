package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.buf.PageWriteListener;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import cn.zhangyis.db.storage.redo.RedoRecord;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 本 MTR 的 redo 收集器：实现 buf 的 {@link PageWriteListener}，把页写译成 {@link PageBytesRecord} 累积；
 * {@code newPage} 经 {@link #recordInit} 产 {@link PageInitRecord}。维护 {@code touchedPages}（收到任一记录即标记该页），
 * commit 据此决定给哪些页盖 pageLSN（不读 PageGuard.wrote 私有状态）。{@code enabled} 开关：commit 盖戳前关闭，使 pageLSN 写不入 redo。
 * 单线程拥有（随 MiniTransaction），无需加锁。
 */
final class MtrRedoCollector implements PageWriteListener {

    private final List<RedoRecord> records = new ArrayList<>();
    private final Set<PageId> touchedPages = new LinkedHashSet<>();
    private boolean enabled = true;

    @Override
    public void onWrite(PageId pageId, int offset, byte[] newBytes) {
        if (!enabled) {
            return;
        }
        records.add(new PageBytesRecord(pageId, offset, newBytes));
        touchedPages.add(pageId);
    }

    /** 记录一次页初始化（newPage）。 */
    void recordInit(PageId pageId, PageType pageType) {
        if (!enabled) {
            return;
        }
        records.add(new PageInitRecord(pageId, pageType));
        touchedPages.add(pageId);
    }

    /** 关闭收集（commit 盖 pageLSN 前调用，排除 LSN 写进 redo）。 */
    void disable() {
        enabled = false;
    }

    List<RedoRecord> records() {
        return records;
    }

    Set<PageId> touchedPages() {
        return touchedPages;
    }
}
