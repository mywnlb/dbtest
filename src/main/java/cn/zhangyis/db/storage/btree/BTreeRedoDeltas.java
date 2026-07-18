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
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param mtr        当前 MTR，负责收集 page bytes 与 logical redo。
     * @param handle     已持 X latch 的 INDEX 页句柄。
     * @param indexId    索引 id，用于 redo 诊断。
     * @param prevPageNo 写入 FIL prev 的页号或 FIL_NULL。
     * @param nextPageNo 写入 FIL next 的页号或 FIL_NULL。
     * @param reason     结构变化原因，需说明调用方语义。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static void writeSiblingLinks(MiniTransaction mtr, IndexPageHandle handle, long indexId,
                                  long prevPageNo, long nextPageNo, String reason) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || handle == null) {
            throw new DatabaseValidationException("B+Tree sibling redo mtr/handle must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        if (indexId < 0 || prevPageNo < 0 || nextPageNo < 0) {
            throw new DatabaseValidationException("B+Tree sibling redo indexId/prev/next must be non-negative");
        }
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("B+Tree sibling redo reason must not be blank");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        byte[] image = ByteBuffer.allocate(Integer.BYTES * 2)
                .putInt((int) prevPageNo)
                .putInt((int) nextPageNo)
                .array();
        try (MtrRedoCategoryScope ignored =
                     mtr.enterRedoCategory(MtrRedoCategory.BTREE_STRUCTURE_BYTES, reason)) {
            handle.writeSiblingLinks(prevPageNo, nextPageNo);
        }
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        mtr.appendLogicalRedo(new BTreePageDeltaRecord(
                        handle.pageId(), indexId, BTreePageDeltaKind.SIBLING_LINKS,
                        nextPageNo, PageEnvelopeLayout.PREV_PAGE_NO, image),
                MtrRedoCategory.BTREE_STRUCTURE_BYTES, reason);
    }

    /**
     * 捕获一个完成结构动作后的 internal 页最终 after-image。header、已使用 heap 与 directory 分段记录，排除大块
     * free-space；root header 使用专用 kind，普通 internal header 使用 PAGE_FORMAT_IMAGE。恢复只按 offset patch。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param page 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param indexId 参与 {@code captureNodePointerPage} 的零基位置 {@code indexId}；必须非负且小于所属页面、集合或持久结构的容量
     * @param root 当前算法是否纳入终态增量、同步压力、磁盘来源、根节点或周期上界校验；用于选择对应的不变量检查分支
     * @param reason 传给 {@code captureNodePointerPage} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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

    /** root shrink 到 leaf 后只登记 PAGE_LEVEL + INDEX_ID；leaf row/目录仍由物理 PAGE_BYTES 保护。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param root 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param rootPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param indexId 参与 {@code captureLeafRootIdentity} 的零基位置 {@code indexId}；必须非负且小于所属页面、集合或持久结构的容量
     * @param reason 传给 {@code captureLeafRootIdentity} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
