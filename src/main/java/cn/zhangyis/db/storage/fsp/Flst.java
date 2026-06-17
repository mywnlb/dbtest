package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

/**
 * FLST 跨页双向链表原语（InnoDB flst0flst 风格，设计 §5.4 extent list）。按 {@link FileAddress} 寻址 base 与 node，
 * 全局自由链与 segment extent 链复用同一实现。mutator 取 X、读取取 S。
 *
 * <p>锁序：mutator 涉及 base 页与 node 页时，按 pageNo 升序取 X（page0 先于 page2，符合 design §18 / slice-1 §9）。
 * 依赖「同链 node 同页」不变量——邻居节点（oldFirst/oldLast/prev/next）与目标 node 同页、已被覆盖，不引入逆序取闩。
 * 调用方若已持高页号 latch 再触发更低页号的 Flst 操作会构成逆序，须由调用方按页号升序编排（2b/测试遵守）。
 *
 * <p>简化点：本片 no-redo（写页只标脏、不产 redo，§15 推迟满足）；不做 O(n) 成员/双向一致性校验（信任调用方，
 * 与 InnoDB 一致）。节点是否属于该链由调用方经 extent 状态机保证。
 */
public final class Flst {

    /** 受控页来源；经 MTR.getPage 读写 base/node 字段，不直接碰裸文件。 */
    private final BufferPool pool;

    public Flst(BufferPool pool) {
        if (pool == null) {
            throw new DatabaseValidationException("buffer pool must not be null");
        }
        this.pool = pool;
    }

    /** 尾插：node 接到 base.last 之后，更新 base.last 与 length。 */
    public void addLast(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        requireArgs(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard[] g = latchAscending(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard baseG = g[0];
        PageGuard nodeG = g[1];
        FlstBase base = FlstBase.readFrom(baseG, baseAddr.offset());
        if (base.last().isNull()) {
            new FlstNode(FileAddress.NULL, FileAddress.NULL).writeTo(nodeG, nodeAddr.offset());
            new FlstBase(1L, nodeAddr, nodeAddr).writeTo(baseG, baseAddr.offset());
            return;
        }
        FileAddress oldLast = base.last();
        PageGuard oldLastG = mtr.getPage(pool, pageId(spaceId, oldLast), PageLatchMode.EXCLUSIVE);
        FlstNode oln = FlstNode.readFrom(oldLastG, oldLast.offset());
        new FlstNode(oldLast, FileAddress.NULL).writeTo(nodeG, nodeAddr.offset());
        new FlstNode(oln.prev(), nodeAddr).writeTo(oldLastG, oldLast.offset());
        new FlstBase(base.length() + 1L, base.first(), nodeAddr).writeTo(baseG, baseAddr.offset());
    }

    /** 头插：node 接到 base.first 之前，更新 base.first 与 length。 */
    public void addFirst(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        requireArgs(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard[] g = latchAscending(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard baseG = g[0];
        PageGuard nodeG = g[1];
        FlstBase base = FlstBase.readFrom(baseG, baseAddr.offset());
        if (base.first().isNull()) {
            new FlstNode(FileAddress.NULL, FileAddress.NULL).writeTo(nodeG, nodeAddr.offset());
            new FlstBase(1L, nodeAddr, nodeAddr).writeTo(baseG, baseAddr.offset());
            return;
        }
        FileAddress oldFirst = base.first();
        PageGuard oldFirstG = mtr.getPage(pool, pageId(spaceId, oldFirst), PageLatchMode.EXCLUSIVE);
        FlstNode ofn = FlstNode.readFrom(oldFirstG, oldFirst.offset());
        new FlstNode(FileAddress.NULL, oldFirst).writeTo(nodeG, nodeAddr.offset());
        new FlstNode(nodeAddr, ofn.next()).writeTo(oldFirstG, oldFirst.offset());
        new FlstBase(base.length() + 1L, nodeAddr, base.last()).writeTo(baseG, baseAddr.offset());
    }

    /** 解链：从 base 所属链移除 node，修复 prev/next 与 base.first/last/length；移除后 node 指针置空。 */
    public void remove(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        requireArgs(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard[] g = latchAscending(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard baseG = g[0];
        PageGuard nodeG = g[1];
        FlstBase base = FlstBase.readFrom(baseG, baseAddr.offset());
        if (base.length() <= 0) {
            throw new FspMetadataException("remove from empty flst base: " + baseAddr);
        }
        FlstNode node = FlstNode.readFrom(nodeG, nodeAddr.offset());
        FileAddress newFirst = base.first();
        FileAddress newLast = base.last();
        if (node.prev().isNull()) {
            newFirst = node.next();
        } else {
            PageGuard prevG = mtr.getPage(pool, pageId(spaceId, node.prev()), PageLatchMode.EXCLUSIVE);
            FlstNode prev = FlstNode.readFrom(prevG, node.prev().offset());
            new FlstNode(prev.prev(), node.next()).writeTo(prevG, node.prev().offset());
        }
        if (node.next().isNull()) {
            newLast = node.prev();
        } else {
            PageGuard nextG = mtr.getPage(pool, pageId(spaceId, node.next()), PageLatchMode.EXCLUSIVE);
            FlstNode next = FlstNode.readFrom(nextG, node.next().offset());
            new FlstNode(node.prev(), next.next()).writeTo(nextG, node.next().offset());
        }
        new FlstNode(FileAddress.NULL, FileAddress.NULL).writeTo(nodeG, nodeAddr.offset());
        long newLen = base.length() - 1L;
        if (newLen == 0L) {
            new FlstBase(0L, FileAddress.NULL, FileAddress.NULL).writeTo(baseG, baseAddr.offset());
        } else {
            new FlstBase(newLen, newFirst, newLast).writeTo(baseG, baseAddr.offset());
        }
    }

    /** 读链头地址（S）。空链返回 NULL。 */
    public FileAddress getFirst(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr) {
        requireRead(mtr, spaceId, baseAddr);
        PageGuard baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.SHARED);
        return FlstBase.readFrom(baseG, baseAddr.offset()).first();
    }

    /** 读链尾地址（S）。空链返回 NULL。 */
    public FileAddress getLast(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr) {
        requireRead(mtr, spaceId, baseAddr);
        PageGuard baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.SHARED);
        return FlstBase.readFrom(baseG, baseAddr.offset()).last();
    }

    /** 读链长（S）。 */
    public long length(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr) {
        requireRead(mtr, spaceId, baseAddr);
        PageGuard baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.SHARED);
        return FlstBase.readFrom(baseG, baseAddr.offset()).length();
    }

    /** 读 node 后继地址（S）。链尾返回 NULL。 */
    public FileAddress getNext(MiniTransaction mtr, SpaceId spaceId, FileAddress nodeAddr) {
        requireRead(mtr, spaceId, nodeAddr);
        PageGuard nodeG = mtr.getPage(pool, pageId(spaceId, nodeAddr), PageLatchMode.SHARED);
        return FlstNode.readFrom(nodeG, nodeAddr.offset()).next();
    }

    /** 读 node 前驱地址（S）。链头返回 NULL。 */
    public FileAddress getPrev(MiniTransaction mtr, SpaceId spaceId, FileAddress nodeAddr) {
        requireRead(mtr, spaceId, nodeAddr);
        PageGuard nodeG = mtr.getPage(pool, pageId(spaceId, nodeAddr), PageLatchMode.SHARED);
        return FlstNode.readFrom(nodeG, nodeAddr.offset()).prev();
    }

    /**
     * 按 pageNo 升序对 base/node 两页取 X（去重），返回 [baseGuard, nodeGuard]（同页时为同一 guard）。
     * 保证 page0 先于 page2，符合 design §18 锁序。
     */
    private PageGuard[] latchAscending(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        long bp = baseAddr.pageNo().value();
        long np = nodeAddr.pageNo().value();
        PageGuard baseG;
        PageGuard nodeG;
        if (bp == np) {
            baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.EXCLUSIVE);
            nodeG = baseG;
        } else if (bp < np) {
            baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.EXCLUSIVE);
            nodeG = mtr.getPage(pool, pageId(spaceId, nodeAddr), PageLatchMode.EXCLUSIVE);
        } else {
            nodeG = mtr.getPage(pool, pageId(spaceId, nodeAddr), PageLatchMode.EXCLUSIVE);
            baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.EXCLUSIVE);
        }
        return new PageGuard[] {baseG, nodeG};
    }

    private static PageId pageId(SpaceId spaceId, FileAddress addr) {
        return PageId.of(spaceId, addr.pageNo());
    }

    private static void requireArgs(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        requireRead(mtr, spaceId, baseAddr);
        requireConcrete(nodeAddr, "node address");
    }

    private static void requireRead(MiniTransaction mtr, SpaceId spaceId, FileAddress addr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        requireConcrete(addr, "address");
    }

    private static void requireConcrete(FileAddress addr, String what) {
        if (addr == null) {
            throw new DatabaseValidationException(what + " must not be null");
        }
        if (addr.isNull()) {
            throw new DatabaseValidationException(what + " must not be FileAddress.NULL");
        }
    }
}
