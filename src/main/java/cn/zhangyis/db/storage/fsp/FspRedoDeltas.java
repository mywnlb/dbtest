package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MtrRedoCategory;
import cn.zhangyis.db.storage.mtr.MtrRedoCategoryScope;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaKind;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaRecord;

import java.nio.ByteBuffer;

/**
 * FSP 元数据逻辑 redo 收集小工具。FSP 仓储仍通过 {@link PageGuard} 改真实页内容，本类把同一次写入登记为
 * {@link FspMetadataDeltaRecord}，并在写入期间把物理字节监听标成 {@link MtrRedoCategory#FSP_METADATA_BYTES}。
 *
 * <p>0.19d 起提交边界会删除被 metadata delta after-image 精确覆盖的 FSP {@code PAGE_BYTES}，从而让逻辑 delta
 * 成为这些账本字段的持久 redo 来源；未被 delta 覆盖的页信封、生命周期 marker 等物理字节仍保留 {@code PAGE_BYTES}。
 */
public final class FspRedoDeltas {

    private FspRedoDeltas() {
    }

    /** 在 FSP metadata 分类 scope 内执行页写入，使提交过滤器能区分可被逻辑 delta 替代的物理字节。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param reason 传给 {@code withFspCategory} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @param action 在契约指定成功、失败或释放边界调用的回调；不得为 {@code null}，且不得破坏当前资源所有权和异常传播规则
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static void withFspCategory(MiniTransaction mtr, String reason, Runnable action) {
        requireMtr(mtr);
        if (action == null) {
            throw new DatabaseValidationException("FSP redo category action must not be null");
        }
        try (MtrRedoCategoryScope ignored = mtr.enterRedoCategory(MtrRedoCategory.FSP_METADATA_BYTES, reason)) {
            action.run();
        }
    }

    /** 写 int 字段并追加对应 FSP metadata after-image delta。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param kind 选择 {@code writeInt} 分支的 {@code FspMetadataDeltaKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param subjectId 参与 {@code writeInt} 的原始数值身份 {@code subjectId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param subIndex 参与 {@code writeInt} 的零基位置 {@code subIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param value 由 {@code writeInt} 转换或编码的原始 {@code int} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     * @param reason 传给 {@code writeInt} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     */
    public static void writeInt(MiniTransaction mtr, PageGuard guard, PageId pageId,
                                FspMetadataDeltaKind kind, long subjectId, int subIndex,
                                int offset, int value, String reason) {
        byte[] image = ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
        writeBytes(mtr, guard, pageId, kind, subjectId, subIndex, offset, image, reason);
    }

    /** 写 long 字段并追加对应 FSP metadata after-image delta。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param kind 选择 {@code writeLong} 分支的 {@code FspMetadataDeltaKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param subjectId 参与 {@code writeLong} 的原始数值身份 {@code subjectId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param subIndex 参与 {@code writeLong} 的零基位置 {@code subIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param value 由 {@code writeLong} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     * @param reason 传给 {@code writeLong} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     */
    public static void writeLong(MiniTransaction mtr, PageGuard guard, PageId pageId,
                                 FspMetadataDeltaKind kind, long subjectId, int subIndex,
                                 int offset, long value, String reason) {
        byte[] image = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        writeBytes(mtr, guard, pageId, kind, subjectId, subIndex, offset, image, reason);
    }

    /** 写 fil_addr_t 字段并追加对应 FSP metadata after-image delta。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param kind 选择 {@code writeAddress} 分支的 {@code FspMetadataDeltaKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param subjectId 参与 {@code writeAddress} 的原始数值身份 {@code subjectId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param subIndex 参与 {@code writeAddress} 的零基位置 {@code subIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param address 参与 {@code writeAddress} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param reason 传给 {@code writeAddress} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static void writeAddress(MiniTransaction mtr, PageGuard guard, PageId pageId,
                                    FspMetadataDeltaKind kind, long subjectId, int subIndex,
                                    int offset, FileAddress address, String reason) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        if (address == null) {
            throw new DatabaseValidationException("FSP metadata file address must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        byte[] image = new byte[Long.BYTES + Integer.BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(image);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        if (address.isNull()) {
            buffer.putLong(0L).putInt(0);
        } else {
            buffer.putLong(address.pageNo().value()).putInt(address.offset());
        }
        withFspCategory(mtr, reason, () -> address.writeTo(guard, offset));
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        append(mtr, pageId, kind, subjectId, subIndex, offset, image, reason);
    }

    /** 写任意字节 after image，并追加对应 FSP metadata delta。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param kind 选择 {@code writeBytes} 分支的 {@code FspMetadataDeltaKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param subjectId 参与 {@code writeBytes} 的原始数值身份 {@code subjectId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param subIndex 参与 {@code writeBytes} 的零基位置 {@code subIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param image 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param reason 传给 {@code writeBytes} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static void writeBytes(MiniTransaction mtr, PageGuard guard, PageId pageId,
                                  FspMetadataDeltaKind kind, long subjectId, int subIndex,
                                  int offset, byte[] image, String reason) {
        requireMtr(mtr);
        if (guard == null) {
            throw new DatabaseValidationException("FSP metadata page guard must not be null");
        }
        byte[] copy = requireImage(image);
        withFspCategory(mtr, reason, () -> guard.writeBytes(offset, copy));
        append(mtr, pageId, kind, subjectId, subIndex, offset, copy, reason);
    }

    /** 从已写入的 guard 读取 after image，并追加 FSP metadata delta。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param kind 选择 {@code recordAfterImage} 分支的 {@code FspMetadataDeltaKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param subjectId 参与 {@code recordAfterImage} 的原始数值身份 {@code subjectId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param subIndex 参与 {@code recordAfterImage} 的零基位置 {@code subIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param length 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param reason 传给 {@code recordAfterImage} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static void recordAfterImage(MiniTransaction mtr, PageGuard guard, PageId pageId,
                                        FspMetadataDeltaKind kind, long subjectId, int subIndex,
                                        int offset, int length, String reason) {
        requireMtr(mtr);
        if (guard == null) {
            throw new DatabaseValidationException("FSP metadata page guard must not be null");
        }
        if (length <= 0) {
            throw new DatabaseValidationException("FSP metadata after-image length must be positive: " + length);
        }
        append(mtr, pageId, kind, subjectId, subIndex, offset, guard.readBytes(offset, length), reason);
    }

    /** 只追加逻辑 delta；调用方已负责在正确分类 scope 内完成物理写。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param kind 选择 {@code append} 分支的 {@code FspMetadataDeltaKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param subjectId 参与 {@code append} 的原始数值身份 {@code subjectId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param subIndex 参与 {@code append} 的零基位置 {@code subIndex}；必须非负且小于所属页面、集合或持久结构的容量
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param image 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param reason 传给 {@code append} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static void append(MiniTransaction mtr, PageId pageId, FspMetadataDeltaKind kind,
                              long subjectId, int subIndex, int offset, byte[] image, String reason) {
        requireMtr(mtr);
        if (pageId == null || kind == null) {
            throw new DatabaseValidationException("FSP metadata redo pageId/kind must not be null");
        }
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("FSP metadata redo reason must not be blank");
        }
        mtr.appendLogicalRedo(new FspMetadataDeltaRecord(
                        pageId, kind, subjectId, subIndex, offset, requireImage(image)),
                MtrRedoCategory.FSP_METADATA_BYTES, reason);
    }

    private static void requireMtr(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
    }

    private static byte[] requireImage(byte[] image) {
        if (image == null || image.length == 0) {
            throw new DatabaseValidationException("FSP metadata after-image must not be null or empty");
        }
        return image.clone();
    }
}
