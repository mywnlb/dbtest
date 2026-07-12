package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.api.index.IndexPageHandle;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MtrRedoCategory;
import cn.zhangyis.db.storage.mtr.MtrRedoCategoryScope;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.record.page.RecordPage;
import cn.zhangyis.db.storage.record.page.RecordPageStructureSnapshot;
import cn.zhangyis.db.storage.redo.BTreePageDeltaKind;
import cn.zhangyis.db.storage.redo.BTreePageDeltaRecord;

import java.nio.ByteBuffer;

/**
 * B+Tree 结构页逻辑 redo 收集小工具。B+Tree 模块仍通过 {@link IndexPageHandle} 改真实页内容，
 * 本类把可稳定表达的结构字段 after-image 同步登记为 {@link BTreePageDeltaRecord}，并把对应物理字节写标为
 * {@link MtrRedoCategory#BTREE_STRUCTURE_BYTES}。
 */
final class BTreeRedoDeltas {

    private BTreeRedoDeltas() {
    }

    /**
     * 写 FIL sibling 链并追加 B+Tree structure delta。数据流：构造 PREV/NEXT 8 字节 after-image →
     * 在 BTREE_STRUCTURE_BYTES 分类下写真实页 → 追加 {@link BTreePageDeltaRecord}。恢复期只 patch 页内
     * FIL prev/next 字段，不重新执行 split/merge/root shrink。
     *
     * @param mtr        当前 MTR，负责收集 page bytes 与 logical redo。
     * @param handle     已持 X latch 的 INDEX 页句柄。
     * @param indexId    索引 id，用于 redo 诊断。
     * @param prevPageNo 写入 FIL prev 的页号或 FIL_NULL。
     * @param nextPageNo 写入 FIL next 的页号或 FIL_NULL。
     * @param reason     结构变化原因，需说明调用方语义。
     */
    static void writeSiblingLinks(MiniTransaction mtr, IndexPageHandle handle, long indexId,
                                  long prevPageNo, long nextPageNo, String reason) {
        if (mtr == null || handle == null) {
            throw new DatabaseValidationException("B+Tree sibling redo mtr/handle must not be null");
        }
        if (indexId < 0 || prevPageNo < 0 || nextPageNo < 0) {
            throw new DatabaseValidationException("B+Tree sibling redo indexId/prev/next must be non-negative");
        }
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("B+Tree sibling redo reason must not be blank");
        }
        byte[] image = ByteBuffer.allocate(Integer.BYTES * 2)
                .putInt((int) prevPageNo)
                .putInt((int) nextPageNo)
                .array();
        try (MtrRedoCategoryScope ignored =
                     mtr.enterRedoCategory(MtrRedoCategory.BTREE_STRUCTURE_BYTES, reason)) {
            handle.writeSiblingLinks(prevPageNo, nextPageNo);
        }
        mtr.appendLogicalRedo(new BTreePageDeltaRecord(
                        handle.pageId(), indexId, BTreePageDeltaKind.SIBLING_LINKS,
                        nextPageNo, PageEnvelopeLayout.PREV_PAGE_NO, image),
                MtrRedoCategory.BTREE_STRUCTURE_BYTES, reason);
    }

    /**
     * 捕获一个完成结构动作后的 internal 页最终 after-image。header、已使用 heap 与 directory 分段记录，排除大块
     * free-space；root header 使用专用 kind，普通 internal header 使用 PAGE_FORMAT_IMAGE。恢复只按 offset patch。
     */
    static void captureNodePointerPage(MiniTransaction mtr, RecordPage page, PageId pageId,
                                       long indexId, boolean root, String reason) {
        requireCaptureArgs(mtr, page, pageId, indexId, reason);
        RecordPageStructureSnapshot snapshot = page.structureSnapshot();
        if (snapshot.level() <= 0) {
            throw new DatabaseValidationException("node pointer redo requires an internal page: " + pageId);
        }
        append(mtr, pageId, indexId,
                root ? BTreePageDeltaKind.ROOT_LEVEL_OR_HEADER : BTreePageDeltaKind.PAGE_FORMAT_IMAGE,
                snapshot.level(), snapshot.headerOffset(), snapshot.headerImage(), reason + " header");
        append(mtr, pageId, indexId, BTreePageDeltaKind.NODE_POINTER_AREA,
                snapshot.userRecordCount(), snapshot.heapOffset(), snapshot.heapImage(), reason + " heap");
        append(mtr, pageId, indexId, BTreePageDeltaKind.NODE_POINTER_AREA,
                snapshot.userRecordCount(), snapshot.directoryOffset(), snapshot.directoryImage(),
                reason + " directory");
    }

    /** root shrink 到 leaf 后只登记 PAGE_LEVEL + INDEX_ID；leaf row/目录仍由物理 PAGE_BYTES 保护。 */
    static void captureLeafRootIdentity(MiniTransaction mtr, RecordPage root,
                                        PageId rootPageId,
                                        long indexId, String reason) {
        requireCaptureArgs(mtr, root, rootPageId, indexId, reason);
        RecordPageStructureSnapshot snapshot = root.structureSnapshot();
        if (snapshot.level() != 0) {
            throw new DatabaseValidationException("leaf root identity redo requires level 0: " + rootPageId);
        }
        int relative = BTreePageDeltaRecord.ROOT_IDENTITY_OFFSET - snapshot.headerOffset();
        byte[] header = snapshot.headerImage();
        byte[] identity = new byte[BTreePageDeltaRecord.ROOT_IDENTITY_BYTES];
        System.arraycopy(header, relative, identity, 0, identity.length);
        append(mtr, rootPageId, indexId, BTreePageDeltaKind.ROOT_LEVEL_OR_HEADER,
                0, BTreePageDeltaRecord.ROOT_IDENTITY_OFFSET, identity, reason);
    }

    private static void requireCaptureArgs(MiniTransaction mtr, RecordPage page,
                                           PageId pageId,
                                           long indexId, String reason) {
        if (mtr == null || page == null || pageId == null) {
            throw new DatabaseValidationException("B+Tree structure snapshot arguments must not be null");
        }
        if (indexId < 0 || reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("B+Tree structure snapshot index/reason is invalid");
        }
    }

    private static void append(MiniTransaction mtr, PageId pageId, long indexId,
                               BTreePageDeltaKind kind, long subjectId, int offset, byte[] image, String reason) {
        mtr.appendLogicalRedo(new BTreePageDeltaRecord(pageId, indexId, kind, subjectId, offset, image),
                MtrRedoCategory.BTREE_STRUCTURE_BYTES, reason);
    }
}
