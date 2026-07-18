package cn.zhangyis.db.storage.fsp.flst;
import cn.zhangyis.db.storage.fsp.FspRedoDeltas;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaKind;


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

    /**
     * 创建 {@code Flst}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public Flst(BufferPool pool) {
        if (pool == null) {
            throw new DatabaseValidationException("buffer pool must not be null");
        }
        this.pool = pool;
    }

    /** 尾插：node 接到 base.last 之后，更新 base.last 与 length。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param baseAddr 参与 {@code addLast} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param nodeAddr 参与 {@code addLast} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    public void addLast(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        requireArgs(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard[] g = latchAscending(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard baseG = g[0];
        PageGuard nodeG = g[1];
        FlstBase base = FlstBase.readFrom(baseG, baseAddr.offset());
        if (base.last().isNull()) {
            writeNode(mtr, spaceId, nodeG, nodeAddr, new FlstNode(FileAddress.NULL, FileAddress.NULL),
                    "FLST addLast initialize node");
            writeBase(mtr, spaceId, baseG, baseAddr, new FlstBase(1L, nodeAddr, nodeAddr),
                    "FLST addLast initialize base");
            return;
        }
        FileAddress oldLast = base.last();
        PageGuard oldLastG = mtr.getPage(pool, pageId(spaceId, oldLast), PageLatchMode.EXCLUSIVE);
        FlstNode oln = FlstNode.readFrom(oldLastG, oldLast.offset());
        writeNode(mtr, spaceId, nodeG, nodeAddr, new FlstNode(oldLast, FileAddress.NULL),
                "FLST addLast write new node");
        writeNode(mtr, spaceId, oldLastG, oldLast, new FlstNode(oln.prev(), nodeAddr),
                "FLST addLast relink old last");
        writeBase(mtr, spaceId, baseG, baseAddr, new FlstBase(base.length() + 1L, base.first(), nodeAddr),
                "FLST addLast update base");
    }

    /** 头插：node 接到 base.first 之前，更新 base.first 与 length。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param baseAddr 参与 {@code addFirst} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param nodeAddr 参与 {@code addFirst} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    public void addFirst(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        requireArgs(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard[] g = latchAscending(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard baseG = g[0];
        PageGuard nodeG = g[1];
        FlstBase base = FlstBase.readFrom(baseG, baseAddr.offset());
        if (base.first().isNull()) {
            writeNode(mtr, spaceId, nodeG, nodeAddr, new FlstNode(FileAddress.NULL, FileAddress.NULL),
                    "FLST addFirst initialize node");
            writeBase(mtr, spaceId, baseG, baseAddr, new FlstBase(1L, nodeAddr, nodeAddr),
                    "FLST addFirst initialize base");
            return;
        }
        FileAddress oldFirst = base.first();
        PageGuard oldFirstG = mtr.getPage(pool, pageId(spaceId, oldFirst), PageLatchMode.EXCLUSIVE);
        FlstNode ofn = FlstNode.readFrom(oldFirstG, oldFirst.offset());
        writeNode(mtr, spaceId, nodeG, nodeAddr, new FlstNode(FileAddress.NULL, oldFirst),
                "FLST addFirst write new node");
        writeNode(mtr, spaceId, oldFirstG, oldFirst, new FlstNode(nodeAddr, ofn.next()),
                "FLST addFirst relink old first");
        writeBase(mtr, spaceId, baseG, baseAddr, new FlstBase(base.length() + 1L, nodeAddr, base.last()),
                "FLST addFirst update base");
    }

    /** 解链：从 base 所属链移除 node，修复 prev/next 与 base.first/last/length；移除后 node 指针置空。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param baseAddr 参与 {@code remove} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param nodeAddr 参与 {@code remove} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
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
            writeNode(mtr, spaceId, prevG, node.prev(), new FlstNode(prev.prev(), node.next()),
                    "FLST remove relink prev");
        }
        if (node.next().isNull()) {
            newLast = node.prev();
        } else {
            PageGuard nextG = mtr.getPage(pool, pageId(spaceId, node.next()), PageLatchMode.EXCLUSIVE);
            FlstNode next = FlstNode.readFrom(nextG, node.next().offset());
            writeNode(mtr, spaceId, nextG, node.next(), new FlstNode(node.prev(), next.next()),
                    "FLST remove relink next");
        }
        writeNode(mtr, spaceId, nodeG, nodeAddr, new FlstNode(FileAddress.NULL, FileAddress.NULL),
                "FLST remove clear node");
        long newLen = base.length() - 1L;
        if (newLen == 0L) {
            writeBase(mtr, spaceId, baseG, baseAddr, new FlstBase(0L, FileAddress.NULL, FileAddress.NULL),
                    "FLST remove empty base");
        } else {
            writeBase(mtr, spaceId, baseG, baseAddr, new FlstBase(newLen, newFirst, newLast),
                    "FLST remove update base");
        }
    }

    /** 读链头地址（S）。空链返回 NULL。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param baseAddr 参与 {@code getFirst} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code getFirst} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public FileAddress getFirst(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr) {
        requireRead(mtr, spaceId, baseAddr);
        PageGuard baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.SHARED);
        return FlstBase.readFrom(baseG, baseAddr.offset()).first();
    }

    /** 读链尾地址（S）。空链返回 NULL。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param baseAddr 参与 {@code getLast} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code getLast} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public FileAddress getLast(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr) {
        requireRead(mtr, spaceId, baseAddr);
        PageGuard baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.SHARED);
        return FlstBase.readFrom(baseG, baseAddr.offset()).last();
    }

    /** 读链长（S）。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param baseAddr 参与 {@code length} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code length} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    public long length(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr) {
        requireRead(mtr, spaceId, baseAddr);
        PageGuard baseG = mtr.getPage(pool, pageId(spaceId, baseAddr), PageLatchMode.SHARED);
        return FlstBase.readFrom(baseG, baseAddr.offset()).length();
    }

    /** 读 node 后继地址（S）。链尾返回 NULL。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param nodeAddr 参与 {@code getNext} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code getNext} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public FileAddress getNext(MiniTransaction mtr, SpaceId spaceId, FileAddress nodeAddr) {
        requireRead(mtr, spaceId, nodeAddr);
        PageGuard nodeG = mtr.getPage(pool, pageId(spaceId, nodeAddr), PageLatchMode.SHARED);
        return FlstNode.readFrom(nodeG, nodeAddr.offset()).next();
    }

    /** 读 node 前驱地址（S）。链头返回 NULL。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param nodeAddr 参与 {@code getPrev} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code getPrev} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public FileAddress getPrev(MiniTransaction mtr, SpaceId spaceId, FileAddress nodeAddr) {
        requireRead(mtr, spaceId, nodeAddr);
        PageGuard nodeG = mtr.getPage(pool, pageId(spaceId, nodeAddr), PageLatchMode.SHARED);
        return FlstNode.readFrom(nodeG, nodeAddr.offset()).prev();
    }

    private static void writeBase(MiniTransaction mtr, SpaceId spaceId, PageGuard guard,
                                  FileAddress baseAddr, FlstBase base, String reason) {
        FspRedoDeltas.withFspCategory(mtr, reason, () -> base.writeTo(guard, baseAddr.offset()));
        FspRedoDeltas.recordAfterImage(mtr, guard, pageId(spaceId, baseAddr),
                FspMetadataDeltaKind.FLST_BASE_FIELD, baseAddr.pageNo().value(), baseAddr.offset(),
                baseAddr.offset(), FlstBaseLayout.SIZE, reason);
    }

    private static void writeNode(MiniTransaction mtr, SpaceId spaceId, PageGuard guard,
                                  FileAddress nodeAddr, FlstNode node, String reason) {
        FspRedoDeltas.withFspCategory(mtr, reason, () -> node.writeTo(guard, nodeAddr.offset()));
        FspRedoDeltas.recordAfterImage(mtr, guard, pageId(spaceId, nodeAddr),
                FspMetadataDeltaKind.FLST_NODE_FIELD, nodeAddr.pageNo().value(), nodeAddr.offset(),
                nodeAddr.offset(), FlstNodeLayout.SIZE, reason);
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

    /**
     * 校验 {@code requireRead} 涉及的表空间、区与段分配结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param addr 参与 {@code requireRead} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
