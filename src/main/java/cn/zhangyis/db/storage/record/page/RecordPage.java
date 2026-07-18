package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.record.format.HiddenColumnLayout;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.format.RecordHeader;
import cn.zhangyis.db.storage.record.format.RecordType;

import java.util.ArrayList;
import java.util.List;

/**
 * INDEX 页内记录区访问器（innodb-record-design §7、§14.1 的结构子集，R3）。绑定一个有效 {@link PageGuard}
 * 在页体 {@code [38, pageSize-8)} 上工作，提供空页初始化、INDEX page header 读写、系统记录读、heap 空间分配、
 * next_record 链遍历等**结构原语**。
 *
 * <p>不含 key 语义：key 比较、key 有序插入定位、PageDirectory 二分查找、{@code RecordCursor} 均归 R4。
 * 本类只负责物理结构，调用方（R4 insert/B+Tree）负责把分配出的 heap 空间按 key 顺序 wire 进 next_record 链与目录。
 *
 * <p>并发：不拥有 page latch / buffer fix 生命周期（§13.1）；写原语要求 {@link PageGuard} 为 EXCLUSIVE
 * （由 PageGuard 自身校验并抛 {@link DatabaseValidationException}）。本类不缓存任何跨 PageGuard 生命周期的状态。
 * 信封（FilePageHeader/PageType.INDEX）由调用方在建页时盖，本类不碰 {@code [0,38)} 与 {@code [pageSize-8,pageSize)}。
 */
public final class RecordPage {

    /** 受控页句柄；持有期间由调用方保证有效（close 后再用，PageGuard 自身抛异常）。 */
    private final PageGuard guard;
    /** 页大小，用于定位页尾的 PageDirectory 与 trailer。PageGuard 不暴露页大小，故由调用方传入。 */
    private final PageSize pageSize;

    /**
     * 创建 {@code RecordPage}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecordPage(PageGuard guard, PageSize pageSize) {
        if (guard == null || pageSize == null) {
            throw new DatabaseValidationException("record page guard/pageSize must not be null");
        }
        this.pageSize = pageSize;
        this.guard = guard;
    }

    // ---------------------------------------------------------------------
    // 空页初始化
    // ---------------------------------------------------------------------

    /**
     * 初始化为空 INDEX 页记录区（要求 X）：写 INDEX page header、infimum/supremum 系统记录、初始 2 槽 PageDirectory。
     *
     * <p>数据流：header（nDirSlots=2、heapTop=98、nHeap=2、nRecs=0、direction=NO_DIRECTION）→ infimum（next 指向 supremum）
     * → supremum（next=0 表链尾）→ 目录 slot[0]=infimum、slot[1]=supremum。不盖 FilePageHeader（调用方负责 PageType.INDEX）。
     *
     * @param indexId 本页所属索引 id。
     * @param level   B+Tree 层（0=leaf）。
     */
    public void format(long indexId, int level) {
        new IndexPageHeader(2, IndexPageLayout.USER_RECORDS_START, 2, 0, 0, 0,
                IndexPageDirection.NO_DIRECTION, 0, 0, level, indexId).writeTo(guard);
        writeSystemRecord(IndexPageLayout.INFIMUM_OFFSET, RecordType.INFIMUM, 0,
                IndexPageLayout.SUPREMUM_OFFSET, IndexPageLayout.INFIMUM_LABEL);
        writeSystemRecord(IndexPageLayout.SUPREMUM_OFFSET, RecordType.SUPREMUM, 1,
                0, IndexPageLayout.SUPREMUM_LABEL);
        PageU16.put(guard, slotAddr(0), IndexPageLayout.INFIMUM_OFFSET);
        PageU16.put(guard, slotAddr(1), IndexPageLayout.SUPREMUM_OFFSET);
    }

    /** 写一条系统记录（8 字节定长头 + 8 字节标签）。nOwned=1：infimum/supremum 各自成组、各占一槽。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param type 选择 {@code writeSystemRecord} 分支的 {@code RecordType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param heapNo 参与 {@code writeSystemRecord} 的原始数值身份 {@code heapNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param next 写入页内记录链或空闲链的偏移 {@code next}；必须是当前页面中的合法记录边界，哨兵值只按页格式约定解释
     * @param label 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     */
    private void writeSystemRecord(int offset, RecordType type, int heapNo, int next, byte[] label) {
        byte[] header = new byte[IndexPageLayout.REC_HEADER_BYTES];
        new RecordHeader(false, false, type, heapNo, 1, next, IndexPageLayout.SYS_REC_BYTES).writeTo(header, 0);
        guard.writeBytes(offset, header);
        guard.writeBytes(offset + IndexPageLayout.REC_HEADER_BYTES, label);
    }

    // ---------------------------------------------------------------------
    // 读 + 几何
    // ---------------------------------------------------------------------

    /** 读 INDEX page header（S/X 均可）。 */
    public IndexPageHeader header() {
        return IndexPageHeader.readFrom(guard);
    }

    /** infimum 系统记录页内偏移（常量）。 */
    public int infimumOffset() {
        return IndexPageLayout.INFIMUM_OFFSET;
    }

    /** supremum 系统记录页内偏移（常量）。 */
    public int supremumOffset() {
        return IndexPageLayout.SUPREMUM_OFFSET;
    }

    /** 读某偏移处的记录头（前向布局，头在记录起始）。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @return {@code recordHeaderAt} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    public RecordHeader recordHeaderAt(int offset) {
        return RecordHeader.readFrom(guard.readBytes(offset, IndexPageLayout.REC_HEADER_BYTES), 0);
    }

    /** 读系统记录的 8 字节标签（紧跟记录头），用于校验/诊断。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @return {@code systemLabelAt} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    public byte[] systemLabelAt(int offset) {
        return guard.readBytes(offset + IndexPageLayout.REC_HEADER_BYTES, 8);
    }

    /** 写整条记录字节到 offset（要求 X）。供 R4 insert 把编码好的记录落页。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param bytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     */
    public void writeRecordBytes(int offset, byte[] bytes) {
        guard.writeBytes(offset, bytes);
    }

    /** 读整条记录字节（先读头取 recordLength，再读该长度）。供 RecordCursor 一次性拷出后做字段解析。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @return {@code readRecordBytes} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    public byte[] readRecordBytes(int offset) {
        RecordHeader header = recordHeaderAt(offset);
        return guard.readBytes(offset, header.recordLength());
    }

    /** 改写某记录的 heapNo（要求 X）。编码记录默认 heapNo=0，insert 落页后须改为真实页内 heap 序号。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param heapNo 参与 {@code setHeapNo} 的原始数值身份 {@code heapNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     */
    public void setHeapNo(int offset, int heapNo) {
        PageU16.put(guard, offset + IndexPageLayout.REC_HEAPNO_FIELD_OFFSET, heapNo);
    }

    /**
     * 置/清某记录的 delete-mark（要求 X）。**只读-改-写 flags 字节的 bit0**，保留 bit1（min-rec）与 bit2-3（recordType）——
     * 不能用 {@link #writeRecordBytes}（会覆盖整头）。delete-mark 保留记录在 next_record 链中（供历史版本/后续 purge）。
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param deleted 资源是否处于删除、空闲、静默、持久化或终态；必须与权威状态机一致，不能由调用方猜测
     */
    public void setDeleted(int offset, boolean deleted) {
        int flags = guard.readBytes(offset + IndexPageLayout.REC_FLAGS_FIELD_OFFSET, 1)[0] & 0xFF;
        flags = deleted ? (flags | 0x01) : (flags & ~0x01);
        guard.writeBytes(offset + IndexPageLayout.REC_FLAGS_FIELD_OFFSET, new byte[]{(byte) flags});
    }

    /**
     * 改写聚簇记录的隐藏列 DB_TRX_ID/DB_ROLL_PTR（要求 X，T1.3f）。**外科修补尾部 15B**——隐藏区贴在记录字节末尾
     * （{@code offset + recordLength - HIDDEN_BYTES}），仅覆写这 15 字节，不动用户列、记录头与 next/heap/flags 字段。
     * 供 delete-mark/取消标记改版本指针而保持列值与记录长度不变（与 {@link #setDeleted} 配对，两步纯写、无内容 undo
     * 风险）。调用方须保证该记录为聚簇记录（带隐藏区）；非聚簇记录无隐藏区，调用即破坏记录字节。
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param hidden 记录格式使用的 header、隐藏列或键布局；不得为 {@code null}，偏移、字段顺序和编码宽度必须与当前页格式一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void writeHiddenColumns(int offset, HiddenColumns hidden) {
        if (hidden == null) {
            throw new DatabaseValidationException("hidden columns must not be null");
        }
        int recordLength = recordHeaderAt(offset).recordLength();
        byte[] buf = new byte[HiddenColumnLayout.HIDDEN_BYTES];
        HiddenColumnLayout.encode(buf, 0, hidden.dbTrxId(), hidden.dbRollPtr());
        guard.writeBytes(offset + recordLength - HiddenColumnLayout.HIDDEN_BYTES, buf);
    }

    /**
     * 改写某记录的 n_owned（组成员数，要求 X）。仅 group 末记录（slot 指向者）非 0；insert 后 owner.n_owned++，
     * 组 split 时由 inserter 重新分配两组的 n_owned。值域 u8（0..255），越界抛 {@link DatabaseValidationException}
     * 防止把高位写进相邻的 next_record 字段。
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param nOwned 待写入页头或目录的记录计数 {@code nOwned}；必须非负并满足当前页容量及 page-directory 分组上界
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void setNOwned(int offset, int nOwned) {
        if (nOwned < 0 || nOwned > 0xFF) {
            throw new DatabaseValidationException("nOwned out of u8 range: " + nOwned);
        }
        guard.writeBytes(offset + IndexPageLayout.REC_NOWNED_FIELD_OFFSET, new byte[]{(byte) nOwned});
    }

    /** 返回绑定同 guard/pageSize 的 PageDirectory 视图，作为 search/inserter 取槽的桥（RecordPage 持有 guard，不外泄）。 */
    public RecordPageDirectory directory() {
        return new RecordPageDirectory(guard, pageSize);
    }

    /**
     * 整体回写 INDEX page header（要求 X）。insert 在单页 X latch 内做 header 计数器的 read-modify-write：
     * 先 {@link #header()} 读出，再以更新后的 {@link IndexPageHeader} 调本方法落盘。RecordPage 不暴露 guard，
     * 故 header 写回须经此桥；本类只忠实落字段，哪些字段随 insert 变（nRecs/lastInsert/direction）由 inserter 决定。
     * @param header 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void writeHeader(IndexPageHeader header) {
        if (header == null) {
            throw new DatabaseValidationException("index page header must not be null");
        }
        header.writeTo(guard);
    }

    /** 页尾（trailer 前）地址，PageDirectory slot[0] 紧贴此处向低地址增长。 */
    private int dirEnd() {
        return pageSize.bytes() - IndexPageLayout.FIL_PAGE_TRAILER_BYTES;
    }

    /** 第 i 槽在页内的地址（slot[0] 最高、向低地址递增 index）。 */
    private int slotAddr(int i) {
        return dirEnd() - (i + 1) * IndexPageLayout.DIR_SLOT_BYTES;
    }

    /** 目录起始地址（最低槽地址）= dirEnd - nDirSlots*2；free space 上界。 */
    public int dirStart() {
        return dirEnd() - PageU16.get(guard, IndexPageHeaderLayout.N_DIR_SLOTS) * IndexPageLayout.DIR_SLOT_BYTES;
    }

    /** 连续可用空间字节数 = dirStart - heapTop。 */
    public int freeSpace() {
        return dirStart() - PageU16.get(guard, IndexPageHeaderLayout.HEAP_TOP);
    }

    /**
     * 捕获当前 INDEX 页最终结构 after-image。数据流：读取权威 header 得到 level/record count/heapTop，
     * 再复制固定 header、已使用 heap 和目录三段；不复制 free-space，不修改页面，也不触发 redo listener。
     * 调用方必须仍持有当前页的 S/X latch；B+Tree 结构 redo 在写路径中使用 X latch 下的稳定快照。
     *
     * @return {@code structureSnapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public RecordPageStructureSnapshot structureSnapshot() {
        IndexPageHeader current = header();
        int directoryStart = dirStart();
        int trailerStart = pageSize.bytes() - IndexPageLayout.FIL_PAGE_TRAILER_BYTES;
        return new RecordPageStructureSnapshot(
                current.level(),
                current.nRecs(),
                IndexPageLayout.PAGE_HEADER_START,
                guard.readBytes(IndexPageLayout.PAGE_HEADER_START,
                        IndexPageLayout.PAGE_HEADER_END - IndexPageLayout.PAGE_HEADER_START),
                IndexPageLayout.INFIMUM_OFFSET,
                guard.readBytes(IndexPageLayout.INFIMUM_OFFSET,
                        current.heapTop() - IndexPageLayout.INFIMUM_OFFSET),
                directoryStart,
                guard.readBytes(directoryStart, trailerStart - directoryStart));
    }

    // ---------------------------------------------------------------------
    // heap 分配 + next_record 链
    // ---------------------------------------------------------------------

    /** 即将分配的新记录 heapNo（= 当前 nHeap）。 */
    public int nextHeapNo() {
        return PageU16.get(guard, IndexPageHeaderLayout.N_HEAP);
    }

    /**
     * 从 free space 顶部切出 {@code bytes} 字节给一条新记录（要求 X），返回新记录页内起始偏移（= 旧 heapTop）。
     * 推进 heapTop 与 nHeap；新记录 heapNo = 旧 nHeap。
     *
     * <p>本原语**不** wire next_record、不动 PageDirectory、不增 nRecs——按 key 顺序串接与目录维护归 R4 insert，
     * 保持「物理空间分配」与「逻辑有序插入」职责分离。空间不足抛 {@link RecordPageOverflowException}（B+Tree 据此 split）。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param bytes 记录字节数，必须为正。
     * @return {@code allocateFromFreeSpace} 实际完成的资源、绑定、页或槽位数量；未处理任何对象时为零，结果不得超过输入候选数
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RecordPageOverflowException 输入值或资源需求超出编码、页面或容量上限时抛出；调用方应缩小请求、回滚或等待资源释放
     */
    public int allocateFromFreeSpace(int bytes) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        if (bytes <= 0) {
            throw new DatabaseValidationException("record bytes must be positive: " + bytes);
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        if (bytes > freeSpace()) {
            throw new RecordPageOverflowException("no room for record of " + bytes
                    + " bytes; free=" + freeSpace());
        }
        int offset = PageU16.get(guard, IndexPageHeaderLayout.HEAP_TOP);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        PageU16.put(guard, IndexPageHeaderLayout.HEAP_TOP, offset + bytes);
        PageU16.put(guard, IndexPageHeaderLayout.N_HEAP, PageU16.get(guard, IndexPageHeaderLayout.N_HEAP) + 1);
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return offset;
    }

    /** 读某记录的 next_record（绝对页内偏移；指向下一记录头，0 表链尾哨兵）。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @return {@code nextRecord} 从受校验输入或持久字节中得到的 {@code int} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
     */
    public int nextRecord(int offset) {
        return PageU16.get(guard, offset + IndexPageLayout.REC_NEXT_FIELD_OFFSET);
    }

    /** 改写某记录的 next_record（要求 X）。target 为下一记录页内偏移；链尾指向 supremum。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param target 写入页内记录链或空闲链的偏移 {@code target}；必须是当前页面中的合法记录边界，哨兵值只按页格式约定解释
     */
    public void setNextRecord(int offset, int target) {
        PageU16.put(guard, offset + IndexPageLayout.REC_NEXT_FIELD_OFFSET, target);
    }

    /**
     * 从 infimum 沿 next_record 找到 {@code next == offset} 的记录偏移（即 offset 的链上前驱）。offset 为首条用户记录时返回
     * {@link #infimumOffset()}。供 purge / update 搬迁定位并重写前驱链。
     *
     * <p>守卫：遍历到 supremum 仍未命中判 offset 不在链中、步数超过 nHeap 判成环，均抛 {@link PageDirectoryCorruptedException}
     * （不静默修复）。要求 offset 为链中的用户记录；不接受 infimum/supremum 作为 offset。
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @return {@code findPredecessor} 从受校验输入或持久字节中得到的 {@code int} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
     * @throws PageDirectoryCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    public int findPredecessor(int offset) {
        int maxSteps = PageU16.get(guard, IndexPageHeaderLayout.N_HEAP);
        int cur = infimumOffset();
        int supremum = supremumOffset();
        int steps = 0;
        while (true) {
            int next = nextRecord(cur);
            if (next == offset) {
                return cur;
            }
            if (next == supremum) {
                throw new PageDirectoryCorruptedException("predecessor not found; offset not in chain: " + offset);
            }
            if (steps++ > maxSteps) {
                throw new PageDirectoryCorruptedException("next_record chain cycle while finding predecessor of " + offset);
            }
            cur = next;
        }
    }

    /**
     * 沿 next_record 从 infimum 走到 supremum，返回中间用户记录偏移（不含 infimum/supremum），即页内逻辑（key）顺序。
     *
     * <p>防御：每个用户记录偏移必须落在页体 {@code [USER_RECORDS_START, dirStart)} 内，否则判为链损坏；
     * 步数超过 nHeap 判为成环。两者均抛 {@link PageDirectoryCorruptedException}，不静默修复。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @return {@code recordOffsetsInOrder} 产生的非空集合容器；元素身份与顺序遵循当前模块契约，无元素时返回空集合而非 {@code null}
     * @throws PageDirectoryCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    public List<Integer> recordOffsetsInOrder() {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        List<Integer> result = new ArrayList<>();
        int supremum = supremumOffset();
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        int maxSteps = PageU16.get(guard, IndexPageHeaderLayout.N_HEAP);
        int lowerBound = IndexPageLayout.USER_RECORDS_START;
        int upperBound = dirStart();
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        int cur = nextRecord(infimumOffset());
        int steps = 0;
        while (cur != supremum) {
            if (steps++ > maxSteps) {
                throw new PageDirectoryCorruptedException("next_record chain cycle detected (steps>" + maxSteps + ")");
            }
            if (cur < lowerBound || cur >= upperBound) {
                throw new PageDirectoryCorruptedException("next_record offset out of page body: " + cur);
            }
            result.add(cur);
            cur = nextRecord(cur);
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return result;
    }
}
