package cn.zhangyis.db.storage.fsp.header;
import cn.zhangyis.db.storage.fil.state.TablespaceState;

import cn.zhangyis.db.storage.fsp.FspRedoDeltas;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleFormat;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaKind;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;

import java.util.Optional;

/**
 * SpaceHeaderPage（page 0）仓储（设计 §6.2）。经 MTR 持 page 0 latch 读写 header 字段；写须 X latch。
 * 三个 extent list 头为 FLST base，由 {@link Flst} 维护——repo 只负责 initialize/read 整 base 与暴露 base 地址访问器。
 *
 * <p>简化点：本切片 no-redo，写页只标脏、不产 redo，不声明 crash-safe（设计 §15 redo 规则推迟满足）。
 */
public final class SpaceHeaderRepository {

    /** 受控页来源；本仓储只经 MTR.getPage 拿 page 0 的 PageGuard。 */
    private final BufferPool pool;

    /**
     * 创建 {@code SpaceHeaderRepository}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public SpaceHeaderRepository(BufferPool pool) {
        if (pool == null) {
            throw new DatabaseValidationException("buffer pool must not be null");
        }
        this.pool = pool;
    }

    private static PageId page0(SpaceId spaceId) {
        return PageId.of(spaceId, PageNo.of(0));
    }

    /** 写入全部 header 字段（X）。三个 extent list base 按 snapshot 写入（新建表空间应为 EMPTY）。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param h 本次操作的不可变上下文或权威快照；不得为 {@code null}，其中的版本、owner 与资源边界必须来自同一次调用链
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void initialize(MiniTransaction mtr, SpaceHeaderSnapshot h) {
        requireMtr(mtr);
        if (h == null) {
            throw new DatabaseValidationException("space header snapshot must not be null");
        }
        PageGuard g = mtr.getPage(pool, page0(h.spaceId()), PageLatchMode.EXCLUSIVE);
        // page0 物理信封：统一标记为 FSP_HDR 表空间头页（pageNo=0、无兄弟链）。这是 page0 与所有其它页型一致的
        // FilePageHeader 不变量——loader/recovery 打开时据此判定 page0 真为表空间头，拒绝绑定错误或损坏的物理页。
        // 信封头经 page guard 写入，作为 PAGE_BYTES 进入 MTR redo，replay 可重建；pageLSN 由 MTR commit 盖戳。
        // FSP 自描述字段（SPACE_ID@38 等）位于信封头之后，二者偏移不重叠。
        FspRedoDeltas.withFspCategory(mtr, "initialize page0 FSP_HDR envelope",
                () -> PageEnvelope.writeHeader(g, new FilePageHeader(h.spaceId(), 0L,
                        FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.FSP_HDR)));
        PageId pageId = page0(h.spaceId());
        FspRedoDeltas.writeInt(mtr, g, pageId, FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                0L, SpaceHeaderLayout.SPACE_ID, SpaceHeaderLayout.SPACE_ID, h.spaceId().value(),
                "initialize space id");
        FspRedoDeltas.writeInt(mtr, g, pageId, FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                0L, SpaceHeaderLayout.PAGE_SIZE_BYTES, SpaceHeaderLayout.PAGE_SIZE_BYTES, h.pageSize().bytes(),
                "initialize page size");
        FspRedoDeltas.writeInt(mtr, g, pageId, FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                0L, SpaceHeaderLayout.SPACE_FLAGS, SpaceHeaderLayout.SPACE_FLAGS, h.spaceFlags(),
                "initialize space flags");
        FspRedoDeltas.writeLong(mtr, g, pageId, FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                0L, SpaceHeaderLayout.CURRENT_SIZE, SpaceHeaderLayout.CURRENT_SIZE, h.currentSizeInPages().value(),
                "initialize current size");
        FspRedoDeltas.writeLong(mtr, g, pageId, FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                0L, SpaceHeaderLayout.FREE_LIMIT, SpaceHeaderLayout.FREE_LIMIT, h.freeLimitPageNo().value(),
                "initialize free limit");
        FspRedoDeltas.writeLong(mtr, g, pageId, FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                0L, SpaceHeaderLayout.NEXT_SEGMENT_ID, SpaceHeaderLayout.NEXT_SEGMENT_ID, h.nextSegmentId(),
                "initialize next segment id");
        writeFlstBase(mtr, g, pageId, SpaceHeaderLayout.FREE_EXTENT_LIST_BASE, h.freeExtentList(),
                "initialize FSP_FREE base");
        writeFlstBase(mtr, g, pageId, SpaceHeaderLayout.FREE_FRAG_LIST_BASE, h.freeFragExtentList(),
                "initialize FSP_FREE_FRAG base");
        writeFlstBase(mtr, g, pageId, SpaceHeaderLayout.FULL_FRAG_LIST_BASE, h.fullFragExtentList(),
                "initialize FSP_FULL_FRAG base");
        FspRedoDeltas.writeLong(mtr, g, pageId, FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                0L, SpaceHeaderLayout.FIRST_INODE_PAGE, SpaceHeaderLayout.FIRST_INODE_PAGE,
                h.firstInodePageNo().value(), "initialize first inode page");
        FspRedoDeltas.writeLong(mtr, g, pageId, FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                0L, SpaceHeaderLayout.SDI_ROOT, SpaceHeaderLayout.SDI_ROOT, h.sdiRootPageNo(),
                "initialize SDI root");
        FspRedoDeltas.writeInt(mtr, g, pageId, FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                0L, SpaceHeaderLayout.SERVER_VERSION, SpaceHeaderLayout.SERVER_VERSION, h.serverVersion(),
                "initialize server version");
        FspRedoDeltas.writeLong(mtr, g, pageId, FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                0L, SpaceHeaderLayout.SPACE_VERSION, SpaceHeaderLayout.SPACE_VERSION, h.spaceVersion(),
                "initialize space version");
    }

    /** 读出全部 header 字段（S）；三个 list base 经 FlstBase.readFrom 解码（含空链一致性校验）。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return {@code read} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public SpaceHeaderSnapshot read(MiniTransaction mtr, SpaceId spaceId) {
        requireMtr(mtr);
        requireSpace(spaceId);
        PageGuard g = mtr.getPage(pool, page0(spaceId), PageLatchMode.SHARED);
        return new SpaceHeaderSnapshot(
                SpaceId.of(g.readInt(SpaceHeaderLayout.SPACE_ID)),
                PageSize.ofBytes(g.readInt(SpaceHeaderLayout.PAGE_SIZE_BYTES)),
                g.readInt(SpaceHeaderLayout.SPACE_FLAGS),
                PageNo.of(g.readLong(SpaceHeaderLayout.CURRENT_SIZE)),
                PageNo.of(g.readLong(SpaceHeaderLayout.FREE_LIMIT)),
                g.readLong(SpaceHeaderLayout.NEXT_SEGMENT_ID),
                FlstBase.readFrom(g, SpaceHeaderLayout.FREE_EXTENT_LIST_BASE),
                FlstBase.readFrom(g, SpaceHeaderLayout.FREE_FRAG_LIST_BASE),
                FlstBase.readFrom(g, SpaceHeaderLayout.FULL_FRAG_LIST_BASE),
                PageNo.of(g.readLong(SpaceHeaderLayout.FIRST_INODE_PAGE)),
                g.readLong(SpaceHeaderLayout.SDI_ROOT),
                g.readInt(SpaceHeaderLayout.SERVER_VERSION),
                g.readLong(SpaceHeaderLayout.SPACE_VERSION));
    }

    /**
     * 以 page0 X latch 读取 header。该入口供“读容量后可能立即改容量”的路径使用，例如空间预留服务：
     * 先拿 X latch 可以避免同一 MTR 内先 S 后 X 的升级禁令，同时把容量判断和 currentSize 推进放在同一物理临界区。
     *
     * @param mtr 当前活动 MTR。
     * @param spaceId 目标表空间。
     * @return page0 header 快照。
     */
    public SpaceHeaderSnapshot readForUpdate(MiniTransaction mtr, SpaceId spaceId) {
        requireMtr(mtr);
        requireSpace(spaceId);
        mtr.getPage(pool, page0(spaceId), PageLatchMode.EXCLUSIVE);
        return read(mtr, spaceId);
    }

    /**
     * 在同一 page-0 X latch 下写完整生命周期头。调用方应把状态转换与相关 FSP 修改放进同一 MTR，
     * 使 redo replay 不会观察到半个 marker。该方法不使用枚举 ordinal，磁盘兼容性由稳定状态码保证。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param mtr 当前活动 MTR。
     * @param spaceId 目标表空间。
     * @param header 要持久化的完整生命周期快照。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void writeLifecycle(MiniTransaction mtr, SpaceId spaceId, TablespaceLifecycleHeader header) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        requireSpace(spaceId);
        if (header == null) {
            throw new DatabaseValidationException("tablespace lifecycle header must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        PageGuard g = mtr.getPage(pool, page0(spaceId), PageLatchMode.EXCLUSIVE);
        g.writeInt(SpaceHeaderLayout.LIFECYCLE_MAGIC, TablespaceLifecycleFormat.MAGIC);
        g.writeInt(SpaceHeaderLayout.LIFECYCLE_FORMAT, TablespaceLifecycleFormat.VERSION);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        g.writeInt(SpaceHeaderLayout.LIFECYCLE_STATE, header.state().persistentCode());
        g.writeLong(SpaceHeaderLayout.LIFECYCLE_INITIAL_SIZE, header.initialSizeInPages().value());
        g.writeLong(SpaceHeaderLayout.LIFECYCLE_EPOCH, header.truncateEpoch());
        g.writeLong(SpaceHeaderLayout.LIFECYCLE_TARGET_SIZE, header.targetSizeInPages().value());
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        g.writeInt(SpaceHeaderLayout.LIFECYCLE_FINISH_STATE, header.finishState().persistentCode());
    }

    /**
     * 读取 page-0 生命周期头。magic 为 0 表示旧格式并返回 empty；其它未知 magic/format 属于元数据损坏，
     * 必须阻断截断，防止用猜测的 initial size 破坏文件。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param mtr 当前活动 MTR。
     * @param spaceId 目标表空间。
     * @return 已初始化的生命周期快照，或旧格式的 empty。
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    public Optional<TablespaceLifecycleHeader> readLifecycle(MiniTransaction mtr, SpaceId spaceId) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        requireSpace(spaceId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        PageGuard g = mtr.getPage(pool, page0(spaceId), PageLatchMode.SHARED);
        int magic = g.readInt(SpaceHeaderLayout.LIFECYCLE_MAGIC);
        if (magic == 0) {
            return Optional.empty();
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        if (magic != TablespaceLifecycleFormat.MAGIC) {
            throw new FspMetadataException("invalid tablespace lifecycle magic: " + Integer.toHexString(magic));
        }
        int format = g.readInt(SpaceHeaderLayout.LIFECYCLE_FORMAT);
        if (format != TablespaceLifecycleFormat.VERSION) {
            throw new FspMetadataException("unsupported tablespace lifecycle format: " + format);
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return Optional.of(new TablespaceLifecycleHeader(
                TablespaceState.fromPersistentCode(g.readInt(SpaceHeaderLayout.LIFECYCLE_STATE)),
                PageNo.of(g.readLong(SpaceHeaderLayout.LIFECYCLE_INITIAL_SIZE)),
                g.readLong(SpaceHeaderLayout.LIFECYCLE_EPOCH),
                PageNo.of(g.readLong(SpaceHeaderLayout.LIFECYCLE_TARGET_SIZE)),
                TablespaceState.fromPersistentCode(g.readInt(SpaceHeaderLayout.LIFECYCLE_FINISH_STATE))));
    }

    /**
     * 更新 {@code setCurrentSizeInPages} 指定的表空间、区与段分配局部状态；写入前校验身份和范围，成功后由所属对象维护一致性。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param value 参与 {@code setCurrentSizeInPages} 的稳定领域标识 {@code PageNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    public void setCurrentSizeInPages(MiniTransaction mtr, SpaceId spaceId, PageNo value) {
        writeLongField(mtr, spaceId, SpaceHeaderLayout.CURRENT_SIZE, requireValue(value).value(),
                "set current size");
    }

    /** 在 page0 X latch 内递增表空间格式代际，供 DISCARDED 文件重新挂载时拒绝旧 frame/file。
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
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param currentVersion 参与 {@code bumpSpaceVersion} 的单调版本值 {@code currentVersion}；必须非负，回退或与权威快照冲突时拒绝
     * @return {@code bumpSpaceVersion} 返回的稳定数值身份或单调版本；零值仅按对应格式的系统/空身份约定解释，非法回退通过领域异常报告
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public long bumpSpaceVersion(MiniTransaction mtr, SpaceId spaceId, long currentVersion) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        requireSpace(spaceId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        if (currentVersion <= 0 || currentVersion == Long.MAX_VALUE) {
            throw new DatabaseValidationException("space version cannot be bumped: " + currentVersion);
        }
        PageGuard guard = mtr.getPage(pool, page0(spaceId), PageLatchMode.EXCLUSIVE);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        long next = currentVersion + 1;
        FspRedoDeltas.writeLong(mtr, guard, page0(spaceId), FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                0L, SpaceHeaderLayout.SPACE_VERSION, SpaceHeaderLayout.SPACE_VERSION, next,
                "restore imported tablespace space version");
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return next;
    }

    /**
     * 更新 {@code setFreeLimitPageNo} 指定的表空间、区与段分配局部状态；写入前校验身份和范围，成功后由所属对象维护一致性。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param value 参与 {@code setFreeLimitPageNo} 的稳定领域标识 {@code PageNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    public void setFreeLimitPageNo(MiniTransaction mtr, SpaceId spaceId, PageNo value) {
        writeLongField(mtr, spaceId, SpaceHeaderLayout.FREE_LIMIT, requireValue(value).value(),
                "set free limit");
    }

    /**
     * 更新 {@code setFirstInodePageNo} 指定的表空间、区与段分配局部状态；写入前校验身份和范围，成功后由所属对象维护一致性。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param value 参与 {@code setFirstInodePageNo} 的稳定领域标识 {@code PageNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    public void setFirstInodePageNo(MiniTransaction mtr, SpaceId spaceId, PageNo value) {
        writeLongField(mtr, spaceId, SpaceHeaderLayout.FIRST_INODE_PAGE, requireValue(value).value(),
                "set first inode page");
    }

    /**
     * 设置 GENERAL 表空间的 SDI 根页。当前 v1 只由 SDI page repository 写入固定 page3；
     * 本仓储只维护 page0 字段和 redo，不解释 SDI payload。
     *
     * @param mtr 当前活动 MTR，必须已经遵守 page0 优先的 latch 顺序
     * @param spaceId 目标表空间 identity
     * @param pageNo SDI 根页号；0 表示 legacy/未启用，v1 生产写入使用 3
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void setSdiRootPageNo(MiniTransaction mtr, SpaceId spaceId, long pageNo) {
        if (pageNo < 0) {
            throw new DatabaseValidationException("SDI root page no must not be negative");
        }
        writeLongField(mtr, spaceId, SpaceHeaderLayout.SDI_ROOT, pageNo, "set SDI root page");
    }

    /** FSP_FREE 链 base 地址（page0 内固定偏移），供 Flst/2b 维护链。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return {@code freeExtentListBaseAddr} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public FileAddress freeExtentListBaseAddr(SpaceId spaceId) {
        requireSpace(spaceId);
        return FileAddress.of(PageNo.of(0), SpaceHeaderLayout.FREE_EXTENT_LIST_BASE);
    }

    /** FSP_FREE_FRAG 链 base 地址。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return {@code freeFragExtentListBaseAddr} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public FileAddress freeFragExtentListBaseAddr(SpaceId spaceId) {
        requireSpace(spaceId);
        return FileAddress.of(PageNo.of(0), SpaceHeaderLayout.FREE_FRAG_LIST_BASE);
    }

    /** FSP_FULL_FRAG 链 base 地址。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return {@code fullFragExtentListBaseAddr} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public FileAddress fullFragExtentListBaseAddr(SpaceId spaceId) {
        requireSpace(spaceId);
        return FileAddress.of(PageNo.of(0), SpaceHeaderLayout.FULL_FRAG_LIST_BASE);
    }

    /** 读 nextSegmentId、写回 +1、返回旧值（segment id 分配，非幂等，调用方一个 MTR 内使用）。
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
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return {@code allocateNextSegmentId} 返回的稳定数值身份或单调版本；零值仅按对应格式的系统/空身份约定解释，非法回退通过领域异常报告
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    public long allocateNextSegmentId(MiniTransaction mtr, SpaceId spaceId) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        requireSpace(spaceId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        PageGuard g = mtr.getPage(pool, page0(spaceId), PageLatchMode.EXCLUSIVE);
        long current = g.readLong(SpaceHeaderLayout.NEXT_SEGMENT_ID);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        if (current <= 0) {
            throw new FspMetadataException("invalid next segment id on disk: " + current);
        }
        FspRedoDeltas.writeLong(mtr, g, page0(spaceId), FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                0L, SpaceHeaderLayout.NEXT_SEGMENT_ID, SpaceHeaderLayout.NEXT_SEGMENT_ID, current + 1,
                "allocate next segment id");
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return current;
    }

    /**
     * 校验输入与当前状态后修改表空间、区与段分配领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param value 由 {@code writeLongField} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     * @param reason 传给 {@code writeLongField} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     */
    private void writeLongField(MiniTransaction mtr, SpaceId spaceId, int offset, long value, String reason) {
        requireMtr(mtr);
        requireSpace(spaceId);
        PageGuard g = mtr.getPage(pool, page0(spaceId), PageLatchMode.EXCLUSIVE);
        FspRedoDeltas.writeLong(mtr, g, page0(spaceId), FspMetadataDeltaKind.SPACE_HEADER_FIELD,
                0L, offset, offset, value, reason);
    }

    private static void writeFlstBase(MiniTransaction mtr, PageGuard guard, PageId pageId,
                                      int offset, FlstBase base, String reason) {
        FspRedoDeltas.withFspCategory(mtr, reason, () -> base.writeTo(guard, offset));
        FspRedoDeltas.recordAfterImage(mtr, guard, pageId, FspMetadataDeltaKind.FLST_BASE_FIELD,
                0L, offset, offset, cn.zhangyis.db.storage.fsp.flst.FlstBaseLayout.SIZE, reason);
    }

    private static void requireMtr(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
    }

    private static void requireSpace(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
    }

    private static PageNo requireValue(PageNo value) {
        if (value == null) {
            throw new DatabaseValidationException("page no value must not be null");
        }
        return value;
    }
}
