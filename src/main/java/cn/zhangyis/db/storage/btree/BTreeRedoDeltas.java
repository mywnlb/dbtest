package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.index.IndexPageHandle;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MtrRedoCategory;
import cn.zhangyis.db.storage.mtr.MtrRedoCategoryScope;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
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
}
