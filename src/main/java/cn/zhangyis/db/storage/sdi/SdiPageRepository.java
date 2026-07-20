package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.zip.CRC32C;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;

/**
 * 固定 page3 SDI 仓储。只维护物理 envelope、page0 root、identity/version、长度和 CRC；
 * payload 对本层完全 opaque，因而不会形成 storage→DD 反向依赖。
 */
public final class SdiPageRepository {

    /**
     * 本对象持有的 {@code pool} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final BufferPool pool;
    /**
     * 本对象持有的 {@code pageSize} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final PageSize pageSize;
    /**
     * 本对象持有的 {@code spaceHeaders} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final SpaceHeaderRepository spaceHeaders;

    /**
     * 创建 {@code SdiPageRepository}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public SdiPageRepository(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("SDI page repository pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
        this.spaceHeaders = new SpaceHeaderRepository(pool);
    }

    /**
     * 为尚未发布的 GENERAL 表空间建立空 SDI 页。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>把 page0 root 写为固定 page3，使后续读取不依赖目录猜测。</li>
     *     <li>以 PAGE_INIT(SDI) 创建零 frame 并写稳定 FIL envelope。</li>
     *     <li>写入 magic/version 与全零空快照；同一 MTR commit 后 root 和 page3 才共同可恢复。</li>
     * </ol>
     *
     * @param mtr 当前物理 CREATE 的活动 MTR；调用方负责未发布空间的 latch-order 例外证明
     * @param spaceId 新建 GENERAL 表空间 identity
     * @throws SdiPageCorruptionException page0 已指向非 v1 root 时抛出
     */
    public void initialize(MiniTransaction mtr, SpaceId spaceId) {
        require(mtr, spaceId);
        // 1. page0 是 root 的权威入口；重复初始化只接受 0/3，未知指针不能被静默覆盖。
        SpaceHeaderSnapshot header = spaceHeaders.readForUpdate(mtr, spaceId);
        if (header.sdiRootPageNo() != 0 && header.sdiRootPageNo() != SdiPageLayout.PAGE_NO) {
            throw new SdiPageCorruptionException("unsupported SDI root page: " + header.sdiRootPageNo());
        }
        spaceHeaders.setSdiRootPageNo(mtr, spaceId, SdiPageLayout.PAGE_NO);

        // 2. extent0 已预留 page3，不走普通 segment allocator；PAGE_INIT 只建立页类型和恢复 identity。
        PageGuard page = mtr.newPage(pool, pageId(spaceId), PageLatchMode.EXCLUSIVE, PageType.SDI);
        PageEnvelope.writeHeader(page, new FilePageHeader(spaceId, SdiPageLayout.PAGE_NO,
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.SDI));

        // 3. 空页明确用全零 identity/length/CRC 表示“已格式化、尚未发布 DD 快照”。
        page.writeBytes(SdiPageLayout.MAGIC_OFFSET, body(0L, 0L, new byte[0]));
    }

    /**
     * 读取并校验 SDI。legacy root=0 与已格式化空页都返回 empty，任何部分 identity 或 CRC 错配都拒绝。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param mtr 只读或活动 MTR，负责 page0→page3 的 latch/fix 生命周期
     * @param spaceId 已打开 GENERAL 表空间
     * @return 有效快照，或 legacy/空 SDI 的 empty
     * @throws SdiPageCorruptionException root、envelope、format、长度、identity 或 CRC 不合法时抛出
     */
    public Optional<SdiPageSnapshot> read(MiniTransaction mtr, SpaceId spaceId) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        require(mtr, spaceId);
        SpaceHeaderSnapshot header = spaceHeaders.read(mtr, spaceId);
        if (header.sdiRootPageNo() == 0) {
            return Optional.empty();
        }
        requireV1Root(header.sdiRootPageNo());
        PageGuard page = mtr.getPage(pool, pageId(spaceId), PageLatchMode.SHARED);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        validateEnvelope(page, spaceId);
        validateFormat(page);
        long tableId = page.readLong(SdiPageLayout.TABLE_ID_OFFSET);
        long version = page.readLong(SdiPageLayout.DICTIONARY_VERSION_OFFSET);
        int length = page.readInt(SdiPageLayout.PAYLOAD_LENGTH_OFFSET);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        int expectedCrc = page.readInt(SdiPageLayout.PAYLOAD_CRC32C_OFFSET);
        if (tableId == 0 && version == 0 && length == 0 && expectedCrc == 0) {
            return Optional.empty();
        }
        if (tableId <= 0 || version <= 0 || length <= 0
                || length > SdiPageLayout.legacyPayloadCapacity(pageSize)) {
            throw new SdiPageCorruptionException("invalid SDI identity/version/payload length: table="
                    + tableId + " version=" + version + " length=" + length);
        }
        byte[] payload = page.readBytes(SdiPageLayout.PAYLOAD_OFFSET, length);
        if (crc32c(payload) != expectedCrc) {
            throw new SdiPageCorruptionException("SDI payload CRC32C mismatch: table=" + tableId);
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return Optional.of(new SdiPageSnapshot(tableId, version, payload));
    }

    /**
     * 覆盖或为 legacy 表空间补写 SDI。逻辑 header 损坏允许由 committed DD 覆盖，但物理 envelope/未知 root
     * 不允许猜测修复。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>以 page0 X latch 读取 root；0 时原地建立 page3，3 时打开现有 SDI，未知 root 立即失败。</li>
     *     <li>校验 table/version/payload 和单页容量；现有页只复核物理 envelope，不依赖待修复逻辑 header。</li>
     *     <li>一次覆盖全部 body，先生成 payload/CRC 后写 identity header；旧 payload 尾部被清零且修改进入同一 redo 批次。</li>
     * </ol>
     *
     * @param mtr 当前 SDI durable write MTR
     * @param spaceId binding 指向的已打开 GENERAL space
     * @param tableId committed table identity，必须为正
     * @param dictionaryVersion committed dictionary version，必须为正
     * @param payload DD codec 产生的非空 opaque payload
     * @throws DatabaseValidationException identity 或 payload 无效时抛出
     * @throws SdiPageCorruptionException payload 超过单页、root 或 envelope 不安全时抛出
     */
    public void write(MiniTransaction mtr, SpaceId spaceId, long tableId,
                      long dictionaryVersion, byte[] payload) {
        require(mtr, spaceId);
        if (tableId <= 0 || dictionaryVersion <= 0 || payload == null || payload.length == 0) {
            throw new DatabaseValidationException("SDI write identity/version/payload is invalid");
        }
        if (payload.length > SdiPageLayout.payloadCapacity(pageSize)) {
            throw new SdiPageCorruptionException("SDI payload exceeds single-page capacity: payload="
                    + payload.length + " capacity=" + SdiPageLayout.payloadCapacity(pageSize));
        }

        // 1. page0→page3 顺序固定；legacy root=0 只在 committed binding 指定的 page3 原地补格式。
        SpaceHeaderSnapshot header = spaceHeaders.readForUpdate(mtr, spaceId);
        PageGuard page;
        if (header.sdiRootPageNo() == 0) {
            spaceHeaders.setSdiRootPageNo(mtr, spaceId, SdiPageLayout.PAGE_NO);
            page = mtr.newPage(pool, pageId(spaceId), PageLatchMode.EXCLUSIVE, PageType.SDI);
            PageEnvelope.writeHeader(page, new FilePageHeader(spaceId, SdiPageLayout.PAGE_NO,
                    FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.SDI));
        } else {
            requireV1Root(header.sdiRootPageNo());
            page = mtr.getPage(pool, pageId(spaceId), PageLatchMode.EXCLUSIVE);
            validateEnvelope(page, spaceId);
        }

        // 2. 参数和容量已在取 page3 前完成；到这里不会因 DD 内容形状留下半个逻辑 header。
        byte[] copied = java.util.Arrays.copyOf(payload, payload.length);

        // 3. body 缓冲包含零填充尾部，单次 page write 令旧快照不可见；MTR commit 再盖 LSN/发布 dirty。
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        page.writeBytes(SdiPageLayout.MAGIC_OFFSET, body(tableId, dictionaryVersion, copied));
    }

    /**
     * 读取 page3 footer 中未决 CREATE INDEX 资源；全零 footer 返回 empty，未知 magic/version/CRC 拒绝恢复。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param mtr 负责 page0→page3 S latch/fix 的短只读 MTR
     * @param spaceId 已打开的目标表空间
     * @return 完整 build descriptor，或没有未决构建时为空
     * @throws SdiPageCorruptionException footer 与 SDI payload 重叠、损坏或物理 identity 跨 space 时抛出
     */
    public Optional<SdiIndexBuildDescriptor> readIndexBuild(MiniTransaction mtr, SpaceId spaceId) {
        return readIndexDdl(mtr, spaceId, SdiIndexDdlAction.BUILD)
                .map(descriptor -> new SdiIndexBuildDescriptor(
                        descriptor.ddlOperationId(), descriptor.dictionaryVersion(),
                        descriptor.tableId(), descriptor.indexBinding()));
    }

    /**
     * 读取 page3 中未决 DROP INDEX 物理所有权；遇到 BUILD descriptor 必须 fail-closed。
     *
     * @param mtr 负责 page0→page3 S latch/fix 的短只读 MTR
     * @param spaceId 已打开的目标表空间
     * @return 完整 DROP descriptor，或 footer 全零时为空
     * @throws SdiPageCorruptionException footer 损坏或动作与 DROP 不一致时抛出
     */
    public Optional<SdiIndexDdlDescriptor> readIndexDrop(MiniTransaction mtr, SpaceId spaceId) {
        return readIndexDdl(mtr, spaceId, SdiIndexDdlAction.DROP);
    }

    /**
     * 读取并校验通用索引 DDL footer；非空 descriptor 的动作必须与调用方预期完全一致。
     */
    private Optional<SdiIndexDdlDescriptor> readIndexDdl(
            MiniTransaction mtr, SpaceId spaceId, SdiIndexDdlAction expectedAction) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        require(mtr, spaceId);
        if (expectedAction == null) {
            throw new DatabaseValidationException("expected SDI index DDL action must not be null");
        }
        SpaceHeaderSnapshot header = spaceHeaders.read(mtr, spaceId);
        requireV1Root(header.sdiRootPageNo());
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        PageGuard page = mtr.getPage(pool, pageId(spaceId), PageLatchMode.SHARED);
        validateEnvelope(page, spaceId);
        validateFormat(page);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        requireFooterAvailable(page);
        byte[] footer = page.readBytes(SdiPageLayout.indexBuildFooterOffset(pageSize),
                SdiPageLayout.INDEX_BUILD_FOOTER_BYTES);
        if (java.util.Arrays.equals(footer, new byte[footer.length])) {
            return Optional.empty();
        }
        SdiIndexDdlDescriptor descriptor = decodeIndexDdl(spaceId, footer);
        if (descriptor.action() != expectedAction) {
            throw new SdiPageCorruptionException(
                    "SDI index DDL action mismatch: expected=" + expectedAction
                            + " actual=" + descriptor.action());
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return Optional.of(descriptor);
    }

    /**
     * 把 staged segment/root 与 DDL identity 写入 page3。调用方必须与 segment/root 创建放在同一 MTR。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param mtr 正在创建新 index 物理资源的活动 MTR
     * @param spaceId 既有表空间
     * @param descriptor 待持久化的未决资源所有权
     * @throws SdiPageCorruptionException footer 已占用、旧 payload 占用保留区或页损坏时抛出
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void writeIndexBuild(MiniTransaction mtr, SpaceId spaceId, SdiIndexBuildDescriptor descriptor) {
        if (descriptor == null) {
            throw new DatabaseValidationException("SDI index build descriptor must not be null");
        }
        writeIndexDdl(mtr, spaceId, new SdiIndexDdlDescriptor(
                SdiIndexDdlAction.BUILD, descriptor.ddlOperationId(), descriptor.dictionaryVersion(),
                descriptor.tableId(), descriptor.indexBinding()));
    }

    /**
     * 在 page3 写入未决 DROP INDEX descriptor。调用方必须在 table MDL X 下保证同表只有一条索引 DDL。
     *
     * @param mtr 写 descriptor 的短物理事务
     * @param spaceId 既有表空间 identity
     * @param descriptor 与 marker、旧 DD binding 精确对应的 DROP 所有权
     * @throws SdiPageCorruptionException footer 已占用或页损坏时抛出
     * @throws DatabaseValidationException descriptor identity 与 space 不一致时抛出
     */
    public void writeIndexDrop(MiniTransaction mtr, SpaceId spaceId,
                               SdiIndexDdlDescriptor descriptor) {
        if (descriptor == null || descriptor.action() != SdiIndexDdlAction.DROP) {
            throw new DatabaseValidationException("SDI DROP INDEX descriptor/action is invalid");
        }
        writeIndexDdl(mtr, spaceId, descriptor);
    }

    /** 写入通用索引 DDL descriptor；新写入统一使用带 action 的 footer v2。 */
    private void writeIndexDdl(MiniTransaction mtr, SpaceId spaceId,
                               SdiIndexDdlDescriptor descriptor) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        require(mtr, spaceId);
        if (descriptor == null || !descriptor.indexBinding().rootPageId().spaceId().equals(spaceId)
                || !descriptor.indexBinding().leafSegment().spaceId().equals(spaceId)
                || !descriptor.indexBinding().nonLeafSegment().spaceId().equals(spaceId)) {
            throw new DatabaseValidationException("SDI index build descriptor does not belong to target space");
        }
        SpaceHeaderSnapshot header = spaceHeaders.readForUpdate(mtr, spaceId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        requireV1Root(header.sdiRootPageNo());
        PageGuard page = mtr.getPage(pool, pageId(spaceId), PageLatchMode.EXCLUSIVE);
        validateEnvelope(page, spaceId);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        validateFormat(page);
        requireFooterAvailable(page);
        byte[] current = page.readBytes(SdiPageLayout.indexBuildFooterOffset(pageSize),
                SdiPageLayout.INDEX_BUILD_FOOTER_BYTES);
        if (!java.util.Arrays.equals(current, new byte[current.length])) {
            throw new SdiPageCorruptionException("SDI index DDL footer is already occupied");
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        page.writeBytes(SdiPageLayout.indexBuildFooterOffset(pageSize), encodeIndexDdl(descriptor));
    }

    /**
     * 只在 footer 与 expected 完全相等时清空 build descriptor，避免旧恢复任务擦除另一条 DDL 的所有权证据。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param expected 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws SdiPageCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    public void clearIndexBuild(MiniTransaction mtr, SpaceId spaceId, SdiIndexBuildDescriptor expected) {
        if (expected == null) {
            throw new DatabaseValidationException("expected SDI index build descriptor must not be null");
        }
        clearIndexDdl(mtr, spaceId, new SdiIndexDdlDescriptor(
                SdiIndexDdlAction.BUILD, expected.ddlOperationId(), expected.dictionaryVersion(),
                expected.tableId(), expected.indexBinding()));
    }

    /**
     * 只在 footer 与 expected DROP identity 完全相等时清空 descriptor。
     *
     * @param mtr 清理 descriptor 的写 MTR
     * @param spaceId 目标表空间
     * @param expected 已与 marker/DD 交叉校验的 DROP descriptor
     * @throws SdiPageCorruptionException footer 动作或 identity 已变化时抛出
     */
    public void clearIndexDrop(MiniTransaction mtr, SpaceId spaceId,
                               SdiIndexDdlDescriptor expected) {
        if (expected == null || expected.action() != SdiIndexDdlAction.DROP) {
            throw new DatabaseValidationException("expected SDI DROP INDEX descriptor/action is invalid");
        }
        clearIndexDdl(mtr, spaceId, expected);
    }

    /** 对通用索引 DDL footer 执行 exact identity CAS clear。 */
    private void clearIndexDdl(MiniTransaction mtr, SpaceId spaceId,
                               SdiIndexDdlDescriptor expected) {
        require(mtr, spaceId);
        if (expected == null) {
            throw new DatabaseValidationException("expected SDI index DDL descriptor must not be null");
        }
        SpaceHeaderSnapshot header = spaceHeaders.readForUpdate(mtr, spaceId);
        requireV1Root(header.sdiRootPageNo());
        PageGuard page = mtr.getPage(pool, pageId(spaceId), PageLatchMode.EXCLUSIVE);
        validateEnvelope(page, spaceId);
        validateFormat(page);
        requireFooterAvailable(page);
        byte[] current = page.readBytes(SdiPageLayout.indexBuildFooterOffset(pageSize),
                SdiPageLayout.INDEX_BUILD_FOOTER_BYTES);
        if (!decodeIndexDdl(spaceId, current).equals(expected)) {
            throw new SdiPageCorruptionException("SDI index DDL descriptor identity changed before clear");
        }
        page.writeBytes(SdiPageLayout.indexBuildFooterOffset(pageSize),
                new byte[SdiPageLayout.INDEX_BUILD_FOOTER_BYTES]);
    }

    private byte[] body(long tableId, long dictionaryVersion, byte[] payload) {
        int length = SdiPageLayout.indexBuildFooterOffset(pageSize) - SdiPageLayout.MAGIC_OFFSET;
        ByteBuffer body = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN);
        body.putInt(SdiPageLayout.MAGIC);
        body.putInt(SdiPageLayout.FORMAT_VERSION);
        body.putLong(tableId);
        body.putLong(dictionaryVersion);
        body.putInt(payload.length);
        body.putInt(payload.length == 0 ? 0 : crc32c(payload));
        body.put(payload);
        return body.array();
    }

    /** 新 footer 只能覆盖未被旧 SDI payload 使用的页尾；旧大 payload 可读但必须先迁移才能 CREATE INDEX。 */
    private void requireFooterAvailable(PageGuard page) {
        int payloadLength = page.readInt(SdiPageLayout.PAYLOAD_LENGTH_OFFSET);
        if (payloadLength < 0 || payloadLength > SdiPageLayout.payloadCapacity(pageSize)) {
            throw new SdiPageCorruptionException(
                    "existing SDI payload occupies index DDL footer reservation: " + payloadLength);
        }
    }

    /**
     * 把调用方领域值编码为数据库内核的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * @param descriptor 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @return {@code encodeIndexBuild} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    static byte[] encodeIndexDdl(SdiIndexDdlDescriptor descriptor) {
        IndexStorageBinding index = descriptor.indexBinding();
        ByteBuffer footer = ByteBuffer.allocate(SdiPageLayout.INDEX_BUILD_FOOTER_BYTES)
                .order(ByteOrder.BIG_ENDIAN);
        footer.putInt(SdiPageLayout.INDEX_BUILD_MAGIC)
                .putInt(SdiPageLayout.INDEX_DDL_FORMAT_VERSION)
                .putInt(descriptor.action().stableCode())
                .putLong(descriptor.ddlOperationId())
                .putLong(descriptor.dictionaryVersion())
                .putLong(descriptor.tableId())
                .putLong(index.indexId())
                .putLong(index.rootPageId().pageNo().value())
                .putInt(index.rootLevel())
                .putInt(index.leafSegment().inodeSlot())
                .putLong(index.leafSegment().segmentId().value())
                .putInt(index.nonLeafSegment().inodeSlot())
                .putLong(index.nonLeafSegment().segmentId().value());
        int crcOffset = footer.position();
        footer.putInt(crc32c(java.util.Arrays.copyOf(footer.array(), crcOffset)));
        return footer.array();
    }

    /**
     * 从稳定表示解码数据库内核领域值；先校验边界、标识与长度，损坏输入以领域异常拒绝。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param footer 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return {@code decodeIndexBuild} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     * @throws SdiPageCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    static SdiIndexDdlDescriptor decodeIndexDdl(SpaceId spaceId, byte[] footer) {
        try {
            // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
            ByteBuffer buffer = ByteBuffer.wrap(footer).order(ByteOrder.BIG_ENDIAN);
            if (buffer.getInt() != SdiPageLayout.INDEX_BUILD_MAGIC) {
                throw new SdiPageCorruptionException("invalid SDI index DDL footer magic");
            }
            int formatVersion = buffer.getInt();
            SdiIndexDdlAction action;
            if (formatVersion == SdiPageLayout.LEGACY_INDEX_BUILD_FORMAT_VERSION) {
                action = SdiIndexDdlAction.BUILD;
            } else if (formatVersion == SdiPageLayout.INDEX_DDL_FORMAT_VERSION) {
                action = SdiIndexDdlAction.fromStableCode(buffer.getInt());
            } else {
                throw new SdiPageCorruptionException(
                        "invalid SDI index DDL footer version: " + formatVersion);
            }
            long ddlId = buffer.getLong();
            long version = buffer.getLong();
            long tableId = buffer.getLong();
            // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
            long indexId = buffer.getLong();
            long rootPageNo = buffer.getLong();
            int rootLevel = buffer.getInt();
            SegmentRef leaf = new SegmentRef(spaceId, buffer.getInt(), SegmentId.of(buffer.getLong()));
            SegmentRef nonLeaf = new SegmentRef(spaceId, buffer.getInt(), SegmentId.of(buffer.getLong()));
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            int crcOffset = buffer.position();
            int expectedCrc = buffer.getInt();
            if (expectedCrc != crc32c(java.util.Arrays.copyOf(footer, crcOffset))) {
                throw new SdiPageCorruptionException("SDI index DDL footer CRC32C mismatch");
            }
            for (int offset = buffer.position(); offset < footer.length; offset++) {
                if (footer[offset] != 0) {
                    throw new SdiPageCorruptionException("SDI index DDL footer reserved bytes are non-zero");
                }
            }
            IndexStorageBinding binding = new IndexStorageBinding(indexId,
                    PageId.of(spaceId, PageNo.of(rootPageNo)), rootLevel, leaf, nonLeaf);
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            return new SdiIndexDdlDescriptor(action, ddlId, version, tableId, binding);
        } catch (SdiPageCorruptionException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new SdiPageCorruptionException("decode SDI index DDL footer failed", failure);
        }
    }

    private static void validateEnvelope(PageGuard page, SpaceId spaceId) {
        var envelope = PageEnvelope.readHeader(page);
        if (!envelope.spaceId().equals(spaceId)
                || envelope.pageNo() != SdiPageLayout.PAGE_NO
                || envelope.prevPageNo() != FilePageHeader.FIL_NULL
                || envelope.nextPageNo() != FilePageHeader.FIL_NULL
                || envelope.pageType() != PageType.SDI) {
            throw new SdiPageCorruptionException("invalid SDI page envelope: " + page.pageId());
        }
    }

    private static void validateFormat(PageGuard page) {
        if (page.readInt(SdiPageLayout.MAGIC_OFFSET) != SdiPageLayout.MAGIC) {
            throw new SdiPageCorruptionException("invalid SDI page magic");
        }
        int version = page.readInt(SdiPageLayout.FORMAT_OFFSET);
        if (version != SdiPageLayout.FORMAT_VERSION) {
            throw new SdiPageCorruptionException("unsupported SDI page format: " + version);
        }
    }

    private static void requireV1Root(long root) {
        if (root != SdiPageLayout.PAGE_NO) {
            throw new SdiPageCorruptionException("unsupported SDI root page: " + root);
        }
    }

    private static int crc32c(byte[] payload) {
        CRC32C crc = new CRC32C();
        crc.update(payload, 0, payload.length);
        return (int) crc.getValue();
    }

    private static PageId pageId(SpaceId spaceId) {
        return PageId.of(spaceId, PageNo.of(SdiPageLayout.PAGE_NO));
    }

    private static void require(MiniTransaction mtr, SpaceId spaceId) {
        if (mtr == null || spaceId == null) {
            throw new DatabaseValidationException("SDI page MTR/space must not be null");
        }
    }
}
