package cn.zhangyis.db.storage.fsp;

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

    /** 编码 24 字节写入 guard（要求 X latch）：prev(12)+next(12)。 */
    public void writeTo(PageGuard guard, int at) {
        prev.writeTo(guard, at + FlstNodeLayout.PREV);
        next.writeTo(guard, at + FlstNodeLayout.NEXT);
    }

    /** 从 guard 解码 24 字节。全零→(NULL,NULL)，即不在任何链中。 */
    public static FlstNode readFrom(PageGuard guard, int at) {
        return new FlstNode(
                FileAddress.readFrom(guard, at + FlstNodeLayout.PREV),
                FileAddress.readFrom(guard, at + FlstNodeLayout.NEXT));
    }
}
