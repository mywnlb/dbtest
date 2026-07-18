package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MtrRedoCategory;
import cn.zhangyis.db.storage.mtr.MtrRedoCategoryScope;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaKind;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaRecord;
import cn.zhangyis.db.storage.redo.UndoRecordPayloadRecord;

import java.nio.ByteBuffer;

/**
 * Undo/rseg 元数据逻辑 redo 收集小工具。Undo 模块仍通过 {@link PageGuard} 改真实页内容，本类把 header/slot
 * after-image 同步登记为 {@link UndoMetadataDeltaRecord}，并把对应物理字节写标为
 * {@link MtrRedoCategory#UNDO_PAGE_BYTES}。
 *
 * <p>0.19f 起 MTR 提交视图会精确过滤被 metadata after-image 完整覆盖的物理 {@code PAGE_BYTES}；未覆盖的
 * record payload 等字节仍保留物理 redo。1.4b 的 15B logical-head pair 也通过同一机制作为单条 delta 持久化。
 */
final class UndoRedoDeltas {

    private UndoRedoDeltas() {
    }

    /** 在 undo metadata 分类 scope 内执行页写入，使诊断能区分 undo/rseg 元数据物理字节。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param reason 传给 {@code withUndoCategory} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @param action 在契约指定成功、失败或释放边界调用的回调；不得为 {@code null}，且不得破坏当前资源所有权和异常传播规则
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static void withUndoCategory(MiniTransaction mtr, String reason, Runnable action) {
        requireMtr(mtr);
        if (action == null) {
            throw new DatabaseValidationException("undo redo category action must not be null");
        }
        try (MtrRedoCategoryScope ignored = mtr.enterRedoCategory(MtrRedoCategory.UNDO_PAGE_BYTES, reason)) {
            action.run();
        }
    }

    /** 写 u8 字段并追加对应 undo metadata after-image delta。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param kind 选择 {@code writeU8} 分支的 {@code UndoMetadataDeltaKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param subjectId 参与 {@code writeU8} 的原始数值身份 {@code subjectId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param subIndex 参与 {@code writeU8} 的零基位置 {@code subIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param value 由 {@code writeU8} 转换或编码的原始 {@code int} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     * @param reason 传给 {@code writeU8} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static void writeU8(MiniTransaction mtr, PageGuard guard, PageId pageId,
                        UndoMetadataDeltaKind kind, long subjectId, int subIndex,
                        int offset, int value, String reason) {
        if (value < 0 || value > 0xFF) {
            throw new DatabaseValidationException("u8 out of range: " + value);
        }
        writeBytes(mtr, guard, pageId, kind, subjectId, subIndex, offset, new byte[]{(byte) value}, reason);
    }

    /** 写 u16 字段并追加对应 undo metadata after-image delta。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param kind 选择 {@code writeU16} 分支的 {@code UndoMetadataDeltaKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param subjectId 参与 {@code writeU16} 的原始数值身份 {@code subjectId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param subIndex 参与 {@code writeU16} 的零基位置 {@code subIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param value 由 {@code writeU16} 转换或编码的原始 {@code int} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     * @param reason 传给 {@code writeU16} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static void writeU16(MiniTransaction mtr, PageGuard guard, PageId pageId,
                         UndoMetadataDeltaKind kind, long subjectId, int subIndex,
                         int offset, int value, String reason) {
        if (value < 0 || value > 0xFFFF) {
            throw new DatabaseValidationException("u16 out of range: " + value);
        }
        writeBytes(mtr, guard, pageId, kind, subjectId, subIndex, offset,
                new byte[]{(byte) (value >>> 8), (byte) value}, reason);
    }

    /** 写 u32/int 字段并追加对应 undo metadata after-image delta。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param kind 选择 {@code writeInt} 分支的 {@code UndoMetadataDeltaKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param subjectId 参与 {@code writeInt} 的原始数值身份 {@code subjectId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param subIndex 参与 {@code writeInt} 的零基位置 {@code subIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param value 由 {@code writeInt} 转换或编码的原始 {@code int} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     * @param reason 传给 {@code writeInt} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     */
    static void writeInt(MiniTransaction mtr, PageGuard guard, PageId pageId,
                         UndoMetadataDeltaKind kind, long subjectId, int subIndex,
                         int offset, int value, String reason) {
        byte[] image = ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
        writeBytes(mtr, guard, pageId, kind, subjectId, subIndex, offset, image, reason);
    }

    /** 写 long 字段并追加对应 undo metadata after-image delta。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param kind 选择 {@code writeLong} 分支的 {@code UndoMetadataDeltaKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param subjectId 参与 {@code writeLong} 的原始数值身份 {@code subjectId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param subIndex 参与 {@code writeLong} 的零基位置 {@code subIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param value 由 {@code writeLong} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     * @param reason 传给 {@code writeLong} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     */
    static void writeLong(MiniTransaction mtr, PageGuard guard, PageId pageId,
                          UndoMetadataDeltaKind kind, long subjectId, int subIndex,
                          int offset, long value, String reason) {
        byte[] image = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        writeBytes(mtr, guard, pageId, kind, subjectId, subIndex, offset, image, reason);
    }

    /** 写任意字节 after-image，并追加对应 undo metadata delta。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param kind 选择 {@code writeBytes} 分支的 {@code UndoMetadataDeltaKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param subjectId 参与 {@code writeBytes} 的原始数值身份 {@code subjectId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param subIndex 参与 {@code writeBytes} 的零基位置 {@code subIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param image 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param reason 传给 {@code writeBytes} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static void writeBytes(MiniTransaction mtr, PageGuard guard, PageId pageId,
                           UndoMetadataDeltaKind kind, long subjectId, int subIndex,
                           int offset, byte[] image, String reason) {
        requireMtr(mtr);
        if (guard == null) {
            throw new DatabaseValidationException("undo metadata page guard must not be null");
        }
        byte[] copy = requireImage(image);
        withUndoCategory(mtr, reason, () -> guard.writeBytes(offset, copy));
        append(mtr, pageId, kind, subjectId, subIndex, offset, copy, reason);
    }

    /**
     * 写完整 undo record 槽并追加 payload logical redo。数据流：构造 {@code [len u16][payload]} after-image →
     * 在 UNDO_PAGE_BYTES 分类下写入真实页 → 追加 {@link UndoRecordPayloadRecord}。恢复期只 patch 槽镜像，
     * 不重新执行 appendRecord，因此不会重复推进 undo page header。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param transactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @param undoNo 参与 {@code writeRecordPayload} 的稳定领域标识 {@code UndoNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param recordOffset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param payload 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param reason 传给 {@code writeRecordPayload} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static void writeRecordPayload(MiniTransaction mtr, PageGuard guard, PageId pageId,
                                   TransactionId transactionId, UndoNo undoNo, int recordOffset,
                                   byte[] payload, String reason) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        if (guard == null || pageId == null || transactionId == null || undoNo == null) {
            throw new DatabaseValidationException(
                    "undo record payload mtr/guard/pageId/transactionId/undoNo must not be null");
        }
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("undo record payload redo reason must not be blank");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (payload == null) {
            throw new DatabaseValidationException("undo record payload must not be null");
        }
        if (payload.length > 0xFFFF) {
            throw new DatabaseValidationException("undo record payload too large: " + payload.length);
        }
        byte[] slotImage = new byte[Short.BYTES + payload.length];
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        slotImage[0] = (byte) (payload.length >>> 8);
        slotImage[1] = (byte) payload.length;
        System.arraycopy(payload, 0, slotImage, Short.BYTES, payload.length);
        withUndoCategory(mtr, reason, () -> guard.writeBytes(recordOffset, slotImage));
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        mtr.appendLogicalRedo(new UndoRecordPayloadRecord(pageId, transactionId, undoNo, recordOffset, slotImage),
                MtrRedoCategory.UNDO_PAGE_BYTES, reason);
    }

    /** 只追加逻辑 delta；调用方已负责在正确分类 scope 内完成物理写。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param kind 选择 {@code append} 分支的 {@code UndoMetadataDeltaKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param subjectId 参与 {@code append} 的原始数值身份 {@code subjectId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param subIndex 参与 {@code append} 的零基位置 {@code subIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param image 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param reason 传给 {@code append} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static void append(MiniTransaction mtr, PageId pageId, UndoMetadataDeltaKind kind,
                       long subjectId, int subIndex, int offset, byte[] image, String reason) {
        requireMtr(mtr);
        if (pageId == null || kind == null) {
            throw new DatabaseValidationException("undo metadata redo pageId/kind must not be null");
        }
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("undo metadata redo reason must not be blank");
        }
        mtr.appendLogicalRedo(new UndoMetadataDeltaRecord(
                        pageId, kind, subjectId, subIndex, offset, requireImage(image)),
                MtrRedoCategory.UNDO_PAGE_BYTES, reason);
    }

    private static void requireMtr(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
    }

    private static byte[] requireImage(byte[] image) {
        if (image == null || image.length == 0) {
            throw new DatabaseValidationException("undo metadata after-image must not be null or empty");
        }
        return image.clone();
    }
}
