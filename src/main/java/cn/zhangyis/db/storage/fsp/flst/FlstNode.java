package cn.zhangyis.db.storage.fsp.flst;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.buf.PageGuard;

/**
 * FLST 链表节点（InnoDB FLST node = 两个 fil_addr）：prev/next 指针对。链外/链端位置用 {@link FileAddress#NULL}。
 *
 * @param prev 前驱节点地址；链头节点为 NULL。
 * @param next 后继节点地址；链尾节点为 NULL。
 */
public record FlstNode(FileAddress prev, FileAddress next) {

    public FlstNode {
        if (prev == null || next == null) {
            throw new DatabaseValidationException("flst node prev/next must not be null (use FileAddress.NULL)");
        }
    }

    /** 编码 24 字节写入 guard（要求 X latch）：prev(12)+next(12)。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param at 参与 {@code writeTo} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     */
    public void writeTo(PageGuard guard, int at) {
        prev.writeTo(guard, at + FlstNodeLayout.PREV);
        next.writeTo(guard, at + FlstNodeLayout.NEXT);
    }

    /** 从 guard 解码 24 字节。全零→(NULL,NULL)，即不在任何链中。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param at 参与 {@code readFrom} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code readFrom} 准备或解码出的中间领域对象；成功时不为 {@code null}，其边界、资源归属和后续发布阶段已明确
     */
    public static FlstNode readFrom(PageGuard guard, int at) {
        return new FlstNode(
                FileAddress.readFrom(guard, at + FlstNodeLayout.PREV),
                FileAddress.readFrom(guard, at + FlstNodeLayout.NEXT));
    }
}
