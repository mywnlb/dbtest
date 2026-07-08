package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.buf.PageWriteListener;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import cn.zhangyis.db.storage.redo.RedoRecord;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 本 MTR 的 redo 收集器：实现 buf 的 {@link PageWriteListener}，把页写译成 {@link PageBytesRecord} 累积；
 * {@code newPage} 经 {@link #recordInit} 产 {@link PageInitRecord}。维护 {@code touchedPages}（收到任一记录即标记该页），
 * commit 据此决定给哪些页盖 pageLSN（不读 PageGuard.wrote 私有状态）。
 *
 * <p>0.23b 起额外维护 {@link MtrRedoEntry} 诊断列表：record 仍是实际提交给 redo manager 的物理命令，
 * category 只表达本 MTR 内这次字节写来自 record、btree、fsp 还是 undo。它不改变 redo 编码和恢复行为。
 *
 * <p>{@code enabled} 开关：commit 盖戳前关闭，使 pageLSN 写不入 redo。单线程拥有（随 MiniTransaction），无需加锁。
 */
final class MtrRedoCollector implements PageWriteListener {

    /** 实际提交给 redo manager 的物理 redo record，顺序即本 MTR 内页写入被捕获的顺序。 */
    private final List<RedoRecord> records = new ArrayList<>();

    /** 与 {@link #records} 一一对应的本地诊断条目；分类不进入持久 redo 文件，只用于审计语义来源。 */
    private final List<MtrRedoEntry> entries = new ArrayList<>();

    /** 已被任一 redo record 触碰的页；commit 依赖它定位需要盖同一 batch end LSN 的 page guard。 */
    private final Set<PageId> touchedPages = new LinkedHashSet<>();

    /** 当前打开的 redo 分类作用域栈；只由 MTR owner 线程访问，关闭时必须按 LIFO 恢复分类。 */
    private final Deque<MtrRedoCategoryScope> categoryScopes = new ArrayDeque<>();

    /** 当前普通 {@code PAGE_BYTES} 写入使用的分类；没有 scope 时保持 generic。 */
    private MtrRedoCategory currentCategory = MtrRedoCategory.PAGE_BYTES_GENERIC;

    /** commit 盖 pageLSN 前关闭 collector，避免 pageLSN stamp 自身被回收成 redo。 */
    private boolean enabled = true;

    @Override
    public void onWrite(PageId pageId, int offset, byte[] newBytes) {
        if (!enabled) {
            return;
        }
        PageBytesRecord record = new PageBytesRecord(pageId, offset, newBytes);
        records.add(record);
        entries.add(new MtrRedoEntry(record, currentCategory));
        touchedPages.add(pageId);
    }

    /** 记录一次页初始化（newPage）。 */
    void recordInit(PageId pageId, PageType pageType) {
        if (!enabled) {
            return;
        }
        PageInitRecord record = new PageInitRecord(pageId, pageType);
        records.add(record);
        entries.add(new MtrRedoEntry(record, MtrRedoCategory.PAGE_INIT));
        touchedPages.add(pageId);
    }

    /**
     * 进入一个 redo 分类作用域。普通 {@code PAGE_BYTES} 写入使用当前分类；{@code PAGE_INIT} 永远保持自己的分类，
     * 以便恢复语义上继续作为唯一建页记录被审计。
     */
    MtrRedoCategoryScope enterCategory(MtrRedoCategory category, String reason) {
        if (category == null) {
            throw new DatabaseValidationException("MTR redo category must not be null");
        }
        if (category == MtrRedoCategory.PAGE_INIT) {
            throw new DatabaseValidationException("PAGE_INIT category is reserved for MiniTransaction.newPage");
        }
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("MTR redo category reason must not be blank");
        }
        MtrRedoCategoryScope scope = new MtrRedoCategoryScope(this, currentCategory, category, reason);
        currentCategory = category;
        categoryScopes.push(scope);
        return scope;
    }

    /** 按 LIFO 关闭分类作用域，防止嵌套分类被乱序恢复。 */
    void closeCategoryScope(MtrRedoCategoryScope scope) {
        if (categoryScopes.peek() != scope) {
            throw new MtrStateException("MTR redo category scope closed out of order: " + scope.reason());
        }
        categoryScopes.pop();
        currentCategory = scope.previousCategory();
    }

    /** 关闭收集（commit 盖 pageLSN 前调用，排除 LSN 写进 redo）。 */
    void disable() {
        enabled = false;
    }

    List<RedoRecord> records() {
        return records;
    }

    List<MtrRedoEntry> entries() {
        return List.copyOf(entries);
    }

    Set<PageId> touchedPages() {
        return touchedPages;
    }
}
