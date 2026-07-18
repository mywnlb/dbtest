package cn.zhangyis.db.storage.fsp.flst;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.storage.buf.PageGuard;

import java.util.Objects;

/**
 * 页内地址（InnoDB fil_addr_t 最小版）：PageNo + 页内 offset，或 NULL 哨兵。只存取、不走链（链表算法属 fsp 分配切片）。
 *
 * <p>编码 12 字节：pageNo(long 8) + offset(int 4)。NULL = 全零编码（pageNoRaw==0 且 offset==0），
 * 使刚 create 的全零页的 list 指针天然解码为 NULL；真实节点偏移恒 ≥ FIL_PAGE_DATA，绝不落在 (page0,offset0)，无碰撞。
 */
public final class FileAddress {

    /** NULL 哨兵：全零编码，表示空 free-list 头或链尾。 */
    public static final FileAddress NULL = new FileAddress(null, 0, true);

    /**
     * 构造时冻结的 {@code pageNo} 稳定领域标识；必须属于本对象的表空间、事务或日志上下文，下游定位与恢复均依赖其身份不变。
     */
    private final PageNo pageNo;
    /**
     * 记录 {@code offset} 的非负位置、容量或计数；写入前必须校验所属页/集合上界，溢出会破坏布局或资源记账。
     */
    private final int offset;
    /**
     * 记录 {@code nil} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
     */
    private final boolean nil;

    private FileAddress(PageNo pageNo, int offset, boolean nil) {
        this.pageNo = pageNo;
        this.offset = offset;
        this.nil = nil;
    }

    /**
     * 创建非空页内地址。
     *
     * @param pageNo 所在页。
     * @param offset 页内偏移（≥0）；(page0,offset0) 保留作 NULL，拒绝。
     * @return 页内地址。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static FileAddress of(PageNo pageNo, int offset) {
        Objects.requireNonNull(pageNo, "pageNo");
        if (offset < 0) {
            throw new DatabaseValidationException("file address offset must be non-negative: " + offset);
        }
        if (pageNo.value() == 0 && offset == 0) {
            throw new DatabaseValidationException("(page0, offset0) is reserved as NULL; use FileAddress.NULL");
        }
        return new FileAddress(pageNo, offset, false);
    }

    /** 是否为 NULL 哨兵。 */
    public boolean isNull() {
        return nil;
    }

    /** 所在页；NULL 调用抛异常。
     *
     * @return {@code pageNo} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public PageNo pageNo() {
        if (nil) {
            throw new DatabaseValidationException("NULL file address has no pageNo");
        }
        return pageNo;
    }

    /** 页内偏移；NULL 调用抛异常。
     *
     * @return {@code offset} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public int offset() {
        if (nil) {
            throw new DatabaseValidationException("NULL file address has no offset");
        }
        return offset;
    }

    /** 编码 12 字节写入 guard（要求 X latch）：pageNoRaw(long)+offset(int)。NULL→全零。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param at 参与 {@code writeTo} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     */
    public void writeTo(PageGuard guard, int at) {
        if (nil) {
            guard.writeLong(at, 0L);
            guard.writeInt(at + Long.BYTES, 0);
        } else {
            guard.writeLong(at, pageNo.value());
            guard.writeInt(at + Long.BYTES, offset);
        }
    }

    /** 从 guard 解码 12 字节。全零→NULL。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param at 参与 {@code readFrom} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code readFrom} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public static FileAddress readFrom(PageGuard guard, int at) {
        long pageNoRaw = guard.readLong(at);
        int off = guard.readInt(at + Long.BYTES);
        if (pageNoRaw == 0 && off == 0) {
            return NULL;
        }
        return of(PageNo.of(pageNoRaw), off);
    }

    /**
     * 实现 {@code equals} 的稳定值语义；比较只读取输入与本对象，不改变表空间、区与段分配状态。
     *
     * @param o 待比较对象；允许为 {@code null} 或其他类型，此时按 {@code equals} 契约返回 {@code false}
     * @return 比较对象类型与全部值语义相等时为 {@code true}；对象为 {@code null}、类型不同或任一组件不等时为 {@code false}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FileAddress that)) {
            return false;
        }
        if (nil || that.nil) {
            return nil == that.nil;
        }
        return offset == that.offset && pageNo.equals(that.pageNo);
    }

    @Override
    public int hashCode() {
        return nil ? 0 : Objects.hash(pageNo, offset);
    }

    @Override
    public String toString() {
        return nil ? "FileAddress.NULL" : "FileAddress(" + pageNo.value() + "," + offset + ")";
    }
}
