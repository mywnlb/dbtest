package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.buf.PageGuard;

/**
 * PageDirectory 槽数组视图（innodb-record-design §7，R3）。绑定 {@link PageGuard}，在页尾 trailer 之前向低地址增长的
 * 稀疏槽数组上读写：每槽 2 字节 u16，存所属 group **最后一条记录**的页内偏移。槽数权威来源为 page header 的 N_DIR_SLOTS。
 *
 * <p>槽序：slot[0] 紧贴 trailer（最高地址），index 越大地址越低；逻辑上 slot[0]=infimum 组、slot[n-1]=supremum 组。
 * 每组成员数 {@code n_owned} 记在组末记录的 RecordHeader 上，由调用方（R4 insert/purge）维护，本类只管槽数组本身。
 *
 * <p>并发：写原语要求 PageGuard 为 EXCLUSIVE（由 PageGuard 自校验）。本类不拥有 latch/buffer fix 生命周期。
 */
public final class RecordPageDirectory {

    /**
     * 本对象持有的 {@code guard} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final PageGuard guard;
    /**
     * 本对象持有的 {@code pageSize} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final PageSize pageSize;

    /**
     * 创建 {@code RecordPageDirectory}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecordPageDirectory(PageGuard guard, PageSize pageSize) {
        if (guard == null || pageSize == null) {
            throw new DatabaseValidationException("directory guard/pageSize must not be null");
        }
        this.pageSize = pageSize;
        this.guard = guard;
    }

    /** 槽数（读 page header N_DIR_SLOTS）。 */
    public int slotCount() {
        return PageU16.get(guard, IndexPageHeaderLayout.N_DIR_SLOTS);
    }

    /** 第 i 槽存的记录偏移；i 越界视为目录损坏。
     *
     * @param i 参与 {@code slot} 的零基位置 {@code i}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code slot} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    public int slot(int i) {
        checkIndex(i, slotCount());
        return PageU16.get(guard, slotAddr(i));
    }

    /** 改写第 i 槽（要求 X）；i 越界视为目录损坏。
     *
     * @param i 参与 {@code setSlot} 的零基位置 {@code i}；必须非负且小于所属页面、集合或持久结构的容量
     * @param recordOffset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     */
    public void setSlot(int i, int recordOffset) {
        checkIndex(i, slotCount());
        PageU16.put(guard, slotAddr(i), recordOffset);
    }

    /** 线扫返回指向 {@code recordOffset} 的槽下标；未找到返回 -1。供 purge/update 定位 owner 的槽。
     * @param recordOffset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @return {@code indexOf} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    public int indexOf(int recordOffset) {
        int n = slotCount();
        for (int i = 0; i < n; i++) {
            if (slot(i) == recordOffset) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 在第 {@code at} 槽位置插入新槽（要求 X）：原 [at, n) 槽逻辑右移一格（向更低地址多占 2 字节），N_DIR_SLOTS+1。
     *
     * <p>容量校验：新增一槽使 dirStart 下移 2 字节，若撞上 heapTop（free space 不足）则抛 {@link RecordPageOverflowException}。
     * 数据流：从高 index 向 at 复制（先把 old slot n-1..at 搬到 n..at+1，避免覆盖），再写新槽、再更新槽数。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param at           插入位置，{@code 0..n}。
     * @param recordOffset 新槽指向的记录偏移。
     * @throws PageDirectoryCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     * @throws RecordPageOverflowException 输入值或资源需求超出编码、页面或容量上限时抛出；调用方应缩小请求、回滚或等待资源释放
     */
    public void insertSlot(int at, int recordOffset) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        int n = slotCount();
        if (at < 0 || at > n) {
            throw new PageDirectoryCorruptedException("insert slot index out of range: " + at + " (n=" + n + ")");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        int newDirStart = dirEnd() - (n + 1) * IndexPageLayout.DIR_SLOT_BYTES;
        int heapTop = PageU16.get(guard, IndexPageHeaderLayout.HEAP_TOP);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        if (newDirStart < heapTop) {
            throw new RecordPageOverflowException("no room for new directory slot; heapTop=" + heapTop
                    + " newDirStart=" + newDirStart);
        }
        for (int i = n; i > at; i--) {
            PageU16.put(guard, slotAddr(i), PageU16.get(guard, slotAddr(i - 1)));
        }
        PageU16.put(guard, slotAddr(at), recordOffset);
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        PageU16.put(guard, IndexPageHeaderLayout.N_DIR_SLOTS, n + 1);
    }

    /**
     * 删除第 {@code at} 槽（要求 X）：原 (at, n) 槽逻辑左移一格，N_DIR_SLOTS-1。
     * 不允许降到 2 槽以下（infimum/supremum 必须各保留一槽），否则视为目录损坏。
     * @param at 参与 {@code removeSlot} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     * @throws PageDirectoryCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    public void removeSlot(int at) {
        int n = slotCount();
        if (n <= 2) {
            throw new PageDirectoryCorruptedException("cannot remove slot below minimum 2: n=" + n);
        }
        checkIndex(at, n);
        for (int i = at; i < n - 1; i++) {
            PageU16.put(guard, slotAddr(i), PageU16.get(guard, slotAddr(i + 1)));
        }
        PageU16.put(guard, IndexPageHeaderLayout.N_DIR_SLOTS, n - 1);
    }

    private int dirEnd() {
        return pageSize.bytes() - IndexPageLayout.FIL_PAGE_TRAILER_BYTES;
    }

    private int slotAddr(int i) {
        return dirEnd() - (i + 1) * IndexPageLayout.DIR_SLOT_BYTES;
    }

    private static void checkIndex(int i, int n) {
        if (i < 0 || i >= n) {
            throw new PageDirectoryCorruptedException("slot index out of range: " + i + " (n=" + n + ")");
        }
    }
}
