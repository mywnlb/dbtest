package cn.zhangyis.db.storage.fsp.flst;
import cn.zhangyis.db.storage.fsp.FspRedoDeltas;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaKind;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

/**
 * FLST 跨页双向链表原语（InnoDB flst0flst 风格，设计 §5.4 extent list）。按 {@link FileAddress} 寻址 base 与 node，
 * 全局自由链与 segment extent 链复用同一实现。mutator 取 X、读取取 S。
 *
 * <p>锁序：每次访问先取得所属表空间 page0 S/X gate；writer 在 gate X 下修改 base、node 与跨 XDES 页邻居，
 * reader 在 gate S 下读取链指针。base/node 首次获取仍按 PageId 升序；跨页链已经持有较高节点后回访较低邻居时，
 * 只在 page0 gate 已排除 writer 环的前提下使用 MTR 的窄越序作用域。该协议替代旧的“同链节点必在同页”假设。
 *
 * <p>简化点：在线 mutator 不做 O(n) 成员/整链一致性扫描；base/node after-image 进入 FSP metadata redo，
 * 节点是否属于该链由调用方状态机保证，离线 scrub 负责完整双向地址核对。
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
     * <p>数据流：</p>
     * <ol>
     *     <li>校验地址后先取得 page0 X gate，使跨页链修改在整个 MTR 内对 reader/writer 原子可见。</li>
     *     <li>按物理页号升序固定 base 与新 node，读取持久 base；空链直接发布唯一节点与 length=1。</li>
     *     <li>非空链在 gate 下固定旧尾，写新节点的 prev 并把旧尾 next 反向链接到新节点。</li>
     *     <li>最后更新 base.last/length；所有节点和 base after-image 都进入同一 FSP redo 批次。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param baseAddr 参与 {@code addLast} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param nodeAddr 参与 {@code addLast} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    public void addLast(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        // 1. page0 X gate 先于任意独立 XDES 页，排除跨页 writer 环和读到半条链的窗口。
        requireArgs(mtr, spaceId, baseAddr, nodeAddr);
        latchGate(mtr, spaceId, PageLatchMode.EXCLUSIVE);
        // 2. base 与待插 node 首次获取仍遵守 PageId 全序；空链在同一批次直接发布完整形状。
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
        // 3. 非空链的旧尾可能低于已持页面；page0 gate 证明成立时才使用窄越序 scope 回访。
        FileAddress oldLast = base.last();
        PageGuard oldLastG = latchNeighborUnderGate(mtr, spaceId, oldLast, PageLatchMode.EXCLUSIVE,
                "page0 X gate serializes FLST addLast while revisiting a cross-page tail");
        FlstNode oln = FlstNode.readFrom(oldLastG, oldLast.offset());
        writeNode(mtr, spaceId, nodeG, nodeAddr, new FlstNode(oldLast, FileAddress.NULL),
                "FLST addLast write new node");
        writeNode(mtr, spaceId, oldLastG, oldLast, new FlstNode(oln.prev(), nodeAddr),
                "FLST addLast relink old last");
        // 4. 邻居双向关系建立后再推进 base，恢复重放会按同一 redo batch 收敛到该 after-image。
        writeBase(mtr, spaceId, baseG, baseAddr, new FlstBase(base.length() + 1L, base.first(), nodeAddr),
                "FLST addLast update base");
    }

    /** 头插：node 接到 base.first 之前，更新 base.first 与 length。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验地址并先取得 page0 X gate，建立本表空间 FLST writer 的共同锁序起点。</li>
     *     <li>升序固定 base 与新 node，读取持久 base；空链直接写唯一节点及完整 base。</li>
     *     <li>非空链在 gate 下固定旧头，写新节点 next 并把旧头 prev 反向链接到新节点。</li>
     *     <li>最后更新 base.first/length，使已发布 base 永远对应完成双向链接的节点集合。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param baseAddr 参与 {@code addFirst} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param nodeAddr 参与 {@code addFirst} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    public void addFirst(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        // 1. gate 在所有 base/node 页面之前获取，禁止从独立 XDES 页反向等待 page0。
        requireArgs(mtr, spaceId, baseAddr, nodeAddr);
        latchGate(mtr, spaceId, PageLatchMode.EXCLUSIVE);
        // 2. 首次固定遵守全序；空链不需要访问任何动态邻居。
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
        // 3. gate 覆盖旧头读取与双向改写，越序 scope 只放宽 MTR 的页号守卫。
        FileAddress oldFirst = base.first();
        PageGuard oldFirstG = latchNeighborUnderGate(mtr, spaceId, oldFirst, PageLatchMode.EXCLUSIVE,
                "page0 X gate serializes FLST addFirst while revisiting a cross-page head");
        FlstNode ofn = FlstNode.readFrom(oldFirstG, oldFirst.offset());
        writeNode(mtr, spaceId, nodeG, nodeAddr, new FlstNode(FileAddress.NULL, oldFirst),
                "FLST addFirst write new node");
        writeNode(mtr, spaceId, oldFirstG, oldFirst, new FlstNode(nodeAddr, ofn.next()),
                "FLST addFirst relink old first");
        // 4. 节点链接完成后发布新的 first/length，避免 reader 看到尚未反链的头节点。
        writeBase(mtr, spaceId, baseG, baseAddr, new FlstBase(base.length() + 1L, nodeAddr, base.last()),
                "FLST addFirst update base");
    }

    /** 解链：从 base 所属链移除 node，修复 prev/next 与 base.first/last/length；移除后 node 指针置空。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验地址、取得 page0 X gate，并按页号升序固定 base 与目标 node。</li>
     *     <li>读取 base/node，空链删除立即作为元数据损坏拒绝，尚未产生任何 after-image。</li>
     *     <li>在 gate 下分别修复存在的前驱和后继；跨页回访只使用带证明的窄越序 scope。</li>
     *     <li>清空目标节点，再按剩余长度写空 base 或新的 first/last；全部变更属于同一 MTR redo 批次。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param baseAddr 参与 {@code remove} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param nodeAddr 参与 {@code remove} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    public void remove(MiniTransaction mtr, SpaceId spaceId, FileAddress baseAddr, FileAddress nodeAddr) {
        // 1. gate 是任意跨页邻居回访的前置证明；base/node 首次固定仍按 PageId 全序。
        requireArgs(mtr, spaceId, baseAddr, nodeAddr);
        latchGate(mtr, spaceId, PageLatchMode.EXCLUSIVE);
        PageGuard[] g = latchAscending(mtr, spaceId, baseAddr, nodeAddr);
        PageGuard baseG = g[0];
        PageGuard nodeG = g[1];
        // 2. 在写第一个 after-image 前拒绝空 base，保留原始损坏证据供 scrub/恢复诊断。
        FlstBase base = FlstBase.readFrom(baseG, baseAddr.offset());
        if (base.length() <= 0) {
            throw new FspMetadataException("remove from empty flst base: " + baseAddr);
        }
        FlstNode node = FlstNode.readFrom(nodeG, nodeAddr.offset());
        FileAddress newFirst = base.first();
        FileAddress newLast = base.last();
        // 3. 先修复两侧邻居或计算新的边界；gate 使 prev/next 在整个过程内保持 writer 独占。
        if (node.prev().isNull()) {
            newFirst = node.next();
        } else {
            PageGuard prevG = latchNeighborUnderGate(mtr, spaceId, node.prev(), PageLatchMode.EXCLUSIVE,
                    "page0 X gate serializes FLST remove while relinking a cross-page predecessor");
            FlstNode prev = FlstNode.readFrom(prevG, node.prev().offset());
            writeNode(mtr, spaceId, prevG, node.prev(), new FlstNode(prev.prev(), node.next()),
                    "FLST remove relink prev");
        }
        if (node.next().isNull()) {
            newLast = node.prev();
        } else {
            PageGuard nextG = latchNeighborUnderGate(mtr, spaceId, node.next(), PageLatchMode.EXCLUSIVE,
                    "page0 X gate serializes FLST remove while relinking a cross-page successor");
            FlstNode next = FlstNode.readFrom(nextG, node.next().offset());
            writeNode(mtr, spaceId, nextG, node.next(), new FlstNode(node.prev(), next.next()),
                    "FLST remove relink next");
        }
        // 4. 邻居稳定后清空目标节点并发布最终 base；失败由 redo 原子批次决定是否可见。
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
        latchGate(mtr, spaceId, PageLatchMode.SHARED);
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
        latchGate(mtr, spaceId, PageLatchMode.SHARED);
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
        latchGate(mtr, spaceId, PageLatchMode.SHARED);
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
        latchGate(mtr, spaceId, PageLatchMode.SHARED);
        PageGuard nodeG = latchNeighborUnderGate(mtr, spaceId, nodeAddr, PageLatchMode.SHARED,
                "page0 S gate excludes FLST writers during cross-page forward traversal");
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
        latchGate(mtr, spaceId, PageLatchMode.SHARED);
        PageGuard nodeG = latchNeighborUnderGate(mtr, spaceId, nodeAddr, PageLatchMode.SHARED,
                "page0 S gate excludes FLST writers during cross-page backward traversal");
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

    /** 取得 per-space FLST gate；调用方若已持高页必须预先持有 page0，禁止用越序 scope 掩盖入口锁序错误。 */
    private void latchGate(MiniTransaction mtr, SpaceId spaceId, PageLatchMode mode) {
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(0)), mode);
    }

    /**
     * 在 page0 gate 已稳定链表写集合后访问任意邻居。scope 只放宽 MTR 的 PageId 守卫，不释放 gate，
     * 返回 guard 继续由 MTR memo 持有，因此 writer 不可能在指针读取与修改之间插入。
     */
    private PageGuard latchNeighborUnderGate(MiniTransaction mtr, SpaceId spaceId, FileAddress address,
                                             PageLatchMode mode, String proof) {
        try (var ignored = mtr.allowOutOfOrderPageLatch(proof)) {
            return mtr.getPage(pool, pageId(spaceId, address), mode);
        }
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
