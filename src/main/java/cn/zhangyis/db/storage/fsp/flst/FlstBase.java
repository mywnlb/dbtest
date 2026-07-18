package cn.zhangyis.db.storage.fsp.flst;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.buf.PageGuard;

/**
 * FLST 链表 base（表头，InnoDB FLST base = len + first + last）。空链 = {@link #EMPTY} = (0, NULL, NULL)。
 *
 * <p>校验分两层，避免把磁盘损坏当成程序错误：record 构造只校验 length>=0（{@link DatabaseValidationException}）；
 * 磁盘解码（{@link #readFrom}）额外校验空链一致性 length==0 ⇔ first/last 均 NULL，不一致抛 {@link FspMetadataException}。
 *
 * @param length 链长（节点数，非负）。
 * @param first  首节点地址；空链为 NULL。
 * @param last   尾节点地址；空链为 NULL。
 */
public record FlstBase(long length, FileAddress first, FileAddress last) {

    /** 空链常量：(0, NULL, NULL)。零初始化页天然解码为它。 */
    public static final FlstBase EMPTY = new FlstBase(0L, FileAddress.NULL, FileAddress.NULL);

    public FlstBase {
        if (first == null || last == null) {
            throw new DatabaseValidationException("flst base first/last must not be null (use FileAddress.NULL)");
        }
        if (length < 0) {
            throw new DatabaseValidationException("flst base length must be non-negative: " + length);
        }
    }

    /** 编码 32 字节写入 guard（要求 X latch）：len(8)+first(12)+last(12)。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param at 参与 {@code writeTo} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     */
    public void writeTo(PageGuard guard, int at) {
        guard.writeLong(at + FlstBaseLayout.LEN, length);
        first.writeTo(guard, at + FlstBaseLayout.FIRST);
        last.writeTo(guard, at + FlstBaseLayout.LAST);
    }

    /**
     * 从 guard 解码 32 字节，并校验空链一致性：length==0 ⇔ first/last 均 NULL；length>0 ⇔ first/last 均非 NULL。
     * 不一致视为页上链账本损坏，抛 {@link FspMetadataException}（与构造期 {@link DatabaseValidationException} 区分）。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param at 参与 {@code readFrom} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code readFrom} 准备或解码出的中间领域对象；成功时不为 {@code null}，其边界、资源归属和后续发布阶段已明确
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    public static FlstBase readFrom(PageGuard guard, int at) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        long length = guard.readLong(at + FlstBaseLayout.LEN);
        FileAddress first = FileAddress.readFrom(guard, at + FlstBaseLayout.FIRST);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        FileAddress last = FileAddress.readFrom(guard, at + FlstBaseLayout.LAST);
        if (length < 0) {
            throw new FspMetadataException("invalid flst base length on disk: " + length);
        }
        boolean bothNull = first.isNull() && last.isNull();
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        boolean bothSet = !first.isNull() && !last.isNull();
        boolean consistent = length == 0 ? bothNull : bothSet;
        if (!consistent) {
            throw new FspMetadataException("flst base length/endpoints inconsistency on disk: length=" + length
                    + " first.null=" + first.isNull() + " last.null=" + last.isNull());
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return new FlstBase(length, first, last);
    }
}
