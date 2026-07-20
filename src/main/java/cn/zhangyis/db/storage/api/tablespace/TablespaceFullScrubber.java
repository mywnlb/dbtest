package cn.zhangyis.db.storage.api.tablespace;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.ddl.SerializedDictionaryInfo;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorLayout;
import cn.zhangyis.db.storage.fsp.extent.ExtentState;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderPhysical;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRawCodec;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleRawCodec;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.sdi.SdiPageLayout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * catalog-loss recovery 使用的 file-per-table 全页只读 scrubber。
 *
 * <p>scanner 不挂载 {@code PageStore}、不占用 SpaceId registry，也不做 doublewrite/redo 修复。page0、
 * XDES allocation bitmap、每个非零页信封/checksum、page3 SDI/footer 与扫描前后文件属性共同构成
 * 成功证明；任一不确定性都以异常交给上层 conflict 分类。</p>
 */
public final class TablespaceFullScrubber {

    /**
     * channel 已以 {@link LinkOption#NOFOLLOW_LINKS} 打开后的故障注入接缝。生产实例固定为空动作；
     * package 测试用它在扫描期间改变目录项属性，验证结束复核不会签发混合快照。
     */
    private final Runnable afterReadChannelOpen;

    /** 创建不注入扫描故障的生产 full-page scrubber。 */
    public TablespaceFullScrubber() {
        this(() -> {
        });
    }

    /**
     * 创建带 channel-open 故障接缝的 scrubber；仅供同 package 并发/TOCTOU 回归测试使用。
     *
     * @param afterReadChannelOpen channel 成功打开后、读取 page0 前执行的非空动作；不得接管或关闭 channel
     * @throws DatabaseValidationException 故障动作为空时抛出
     */
    TablespaceFullScrubber(Runnable afterReadChannelOpen) {
        if (afterReadChannelOpen == null) {
            throw new DatabaseValidationException("tablespace scrub channel-open hook must not be null");
        }
        this.afterReadChannelOpen = afterReadChannelOpen;
    }

    /**
     * 顺序扫描一个候选并生成可绑定 complete-scan token 的 fingerprint。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>NOFOLLOW 读取初始属性，校验 regular/page-aligned/至少四页，并建立单一总 deadline。</li>
     *     <li>严格校验 page0 checksum、信封、FSP identity/size/type/lifecycle/SDI root 与 XDES 覆盖范围。</li>
     *     <li>复用一个 page buffer 顺序扫描全部页：XDES 决定零页是否可接受，已分配非零页必须通过
     *     checksum、spaceId/pageNo/type；page3 额外解码 SDI 并要求 DDL footer 全零。</li>
     *     <li>扫描结束重新读取 size/mtime/fileKey；属性不变才返回全文件 SHA-256，否则拒绝 TOCTOU 候选。</li>
     * </ol>
     *
     * @param request 路径、期望 identity/page size 与共享正超时
     * @return 经过完整物理校验的不可变候选结果
     * @throws DatabaseValidationException request 为空时抛出
     * @throws TablespaceScrubException 文件损坏、身份冲突、超时、中断、属性变化或只读 IO 失败时抛出
     */
    public TablespaceFullScrubResult scrub(TablespaceFullScrubRequest request) {
        // 1. 所有 IO 使用同一 deadline；软链接和非常规文件在打开 channel 前拒绝。
        if (request == null) {
            throw new DatabaseValidationException("tablespace full scrub request must not be null");
        }
        Path path = request.path();
        long deadline = deadline(request.timeout());
        try {
            BasicFileAttributes before = attributes(path);
            int pageBytes = request.expectedPageSize().bytes();
            if (!before.isRegularFile() || before.isSymbolicLink()
                    || before.size() < (long) pageBytes * 4 || before.size() % pageBytes != 0) {
                throw new TablespaceScrubException(
                        "tablespace candidate is not a page-aligned NOFOLLOW regular file: " + path);
            }
            int pageCount = Math.toIntExact(before.size() / pageBytes);
            ByteBuffer page = ByteBuffer.allocate(pageBytes).order(ByteOrder.BIG_ENDIAN);
            MessageDigest fileDigest = sha256();
            SerializedDictionaryInfo sdi;
            SpaceHeaderPhysical physical;

            try (FileChannel channel = FileChannel.open(
                    path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                afterReadChannelOpen.run();
                checkDeadline(deadline, path, -1);
                readPage(channel, page, 0L, pageBytes, deadline, path, 0);
                byte[] page0 = page.array().clone();

                // 2. rebuild 模式不接受 legacy-zero checksum；page0 是后续 XDES/size 判断的唯一物理根。
                validatePageEnvelope(page0, request.expectedPageSize(), request.expectedSpaceId(), 0, PageType.FSP_HDR);
                physical = SpaceHeaderRawCodec.readPhysical(ByteBuffer.wrap(page0).order(ByteOrder.BIG_ENDIAN));
                validatePageZero(request, physical, pageCount, page0);

                long extentCapacity = ExtentDescriptorLayout.maxEntriesInPage0(request.expectedPageSize())
                        * request.expectedPageSize().pagesPerExtent();
                if (pageCount > extentCapacity) {
                    throw new TablespaceScrubException(
                            "tablespace exceeds page0 XDES coverage: pages=" + pageCount
                                    + " capacity=" + extentCapacity);
                }
                validateXdes(page0, request.expectedPageSize(), pageCount, path);

                sdi = null;
                for (int pageNo = 0; pageNo < pageCount; pageNo++) {
                    // 3. 每页边界响应 timeout/interrupt，不在一个损坏或超大文件上无界等待。
                    checkDeadline(deadline, path, pageNo);
                    if (pageNo == 0) {
                        System.arraycopy(page0, 0, page.array(), 0, pageBytes);
                    } else {
                        readPage(channel, page, (long) pageNo * pageBytes, pageBytes,
                                deadline, path, pageNo);
                    }
                    byte[] bytes = page.array();
                    fileDigest.update(bytes);
                    boolean allocated = isAllocated(page0, request.expectedPageSize(), pageNo);
                    if (isAllZero(bytes)) {
                        /*
                         * page 1 在旧版教学格式中只是尚未初始化的 change-buffer bitmap 占位页：extent0 会把它标为
                         * system allocated。当前创建路径已写入 IBUF_BITMAP 信封，只对历史文件的这个固定身份保留兼容；
                         * 其它 allocated zero page 仍表示丢失初始化内容并 fail-closed。
                         */
                        boolean reservedIbufPlaceholder = pageNo == 1 && allocated;
                        if (allocated && !reservedIbufPlaceholder) {
                            throw new TablespaceScrubException(
                                    "allocated tablespace page is all-zero: " + path + " page=" + pageNo);
                        }
                        continue;
                    }
                    if (!allocated) {
                        throw new TablespaceScrubException(
                                "unallocated tablespace page contains data: " + path + " page=" + pageNo);
                    }
                    validatePageEnvelope(bytes, request.expectedPageSize(), request.expectedSpaceId(), pageNo, null);
                    if (pageNo == 1
                            && PageType.fromCode(ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
                            .getInt(PageEnvelopeLayout.PAGE_TYPE)) != PageType.IBUF_BITMAP) {
                        throw new TablespaceScrubException(
                                "tablespace page1 is not IBUF_BITMAP: " + request.path());
                    }
                    if (pageNo == 2
                            && PageType.fromCode(ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
                            .getInt(PageEnvelopeLayout.PAGE_TYPE)) != PageType.INODE) {
                        throw new TablespaceScrubException(
                                "tablespace page2 is not INODE: " + request.path());
                    }
                    if (pageNo == SdiPageLayout.PAGE_NO) {
                        sdi = decodeSdi(bytes, request);
                    }
                }
                if (sdi == null) {
                    throw new TablespaceScrubException("tablespace candidate has no valid page3 SDI: " + path);
                }
            }

            // 4. fileKey 不可用时仍比较 size/mtime；可用时再绑定底层文件对象身份。
            checkDeadline(deadline, path, pageCount);
            BasicFileAttributes after = attributes(path);
            if (!after.isRegularFile() || after.isSymbolicLink()
                    || before.size() != after.size()
                    || !before.lastModifiedTime().equals(after.lastModifiedTime())
                    || !Objects.equals(fileKey(before), fileKey(after))) {
                throw new TablespaceScrubException("tablespace candidate changed during full scrub: " + path);
            }
            return new TablespaceFullScrubResult(path, after.size(), after.lastModifiedTime().toMillis(),
                    fileKey(after), pageCount, physical.spaceId(), physical.pageSize(),
                    physical.spaceVersion(), sdi, fileDigest.digest());
        } catch (TablespaceScrubException failure) {
            throw failure;
        } catch (IOException | RuntimeException failure) {
            if (failure instanceof TablespaceScrubException scrubFailure) {
                throw scrubFailure;
            }
            throw new TablespaceScrubException("full-page scrub failed: " + path, failure);
        }
    }

    /**
     * 校验当前文件覆盖范围内的 XDES 状态、owner 和 FLST 节点地址，再允许 allocation bitmap
     * 参与页面裁决。该检查只解释 page0 原始字节，不挂载 Buffer Pool，也不修复损坏账本。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按文件页数计算必须存在的 extent entry 数，逐项拒绝未知持久 state ordinal。</li>
     *     <li>校验 owner sentinel：无主状态必须为零，普通 FSEG/FSEG_FRAG 必须携带正 segment id；
     *     extent0 的系统 FSEG_FRAG 是明确例外。</li>
     *     <li>校验 prev/next 要么是全零 NULL，要么指向同状态/同 owner entry 并由其反向字段精确指回。</li>
     *     <li>FREE extent 不得声明 allocated bit，FULL_FRAG 必须覆盖完整 extent，EOF 后不得有幽灵分配位。</li>
     * </ol>
     *
     * @param page0 已通过严格 checksum、信封和 Space Header identity 校验的页镜像
     * @param pageSize 当前实例页大小，决定每个 extent 的页数
     * @param pageCount 文件完整页数，决定必须校验的 XDES entry 范围
     * @param path 当前候选规范路径，仅用于损坏诊断
     * @throws TablespaceScrubException XDES 任一状态、owner、链地址或 bitmap 语义不一致时抛出
     */
    private static void validateXdes(byte[] page0, PageSize pageSize, int pageCount, Path path) {
        // 1. ordinal 是持久格式的一部分；未知值不能退化成仅信任 bitmap 的候选。
        int pagesPerExtent = pageSize.pagesPerExtent();
        int extentCount = Math.ceilDiv(pageCount, pagesPerExtent);
        ExtentState[] states = ExtentState.values();
        ByteBuffer image = ByteBuffer.wrap(page0).order(ByteOrder.BIG_ENDIAN);
        for (int extentNo = 0; extentNo < extentCount; extentNo++) {
            int base = ExtentDescriptorLayout.entryOffset(extentNo);
            int ordinal = image.getInt(base + ExtentDescriptorLayout.STATE);
            if (ordinal < 0 || ordinal >= states.length) {
                throw new TablespaceScrubException(
                        "invalid XDES state ordinal: path=" + path + " extent=" + extentNo
                                + " ordinal=" + ordinal);
            }
            ExtentState state = states[ordinal];
            if (extentNo == 0 && state != ExtentState.FSEG_FRAG) {
                throw new TablespaceScrubException(
                        "invalid system XDES state: path=" + path + " state=" + state);
            }

            // 2. owner=0 是无主哨兵；extent0 由系统保留路径以 FSEG_FRAG/owner0 初始化。
            long owner = image.getLong(base + ExtentDescriptorLayout.OWNER_SEGMENT);
            boolean systemExtent = extentNo == 0;
            boolean segmentOwned = state == ExtentState.FSEG || state == ExtentState.FSEG_FRAG;
            if (owner < 0 || (!segmentOwned && owner != 0)
                    || (segmentOwned && !systemExtent && owner == 0)) {
                throw new TablespaceScrubException(
                        "invalid XDES owner semantics: path=" + path + " extent=" + extentNo
                                + " state=" + state + " owner=" + owner);
            }

            // 3. 当前 v1 的 extent FLST node 全部内嵌 page0，地址必须落到受检 entry 的 PREV 字段。
            int previousExtent = validateXdesAddress(
                    image, base + ExtentDescriptorLayout.PREV, extentCount, path, extentNo, "prev");
            int nextExtent = validateXdesAddress(
                    image, base + ExtentDescriptorLayout.NEXT, extentCount, path, extentNo, "next");
            int currentNodeOffset = base + ExtentDescriptorLayout.PREV;
            validateXdesReciprocalAddress(
                    image, previousExtent, ExtentDescriptorLayout.NEXT,
                    currentNodeOffset, state, owner, states,
                    path, extentNo, "previous.next");
            validateXdesReciprocalAddress(
                    image, nextExtent, ExtentDescriptorLayout.PREV,
                    currentNodeOffset, state, owner, states,
                    path, extentNo, "next.prev");

            // 4. FREE 不允许任何已分配页；FULL_FRAG 必须完整，且文件 EOF 之后不得出现幽灵分配位。
            int pagesInFile = Math.min(pagesPerExtent, pageCount - extentNo * pagesPerExtent);
            if (state == ExtentState.FULL_FRAG && pagesInFile != pagesPerExtent) {
                throw new TablespaceScrubException(
                        "FULL_FRAG XDES extent is only partially backed by file: path=" + path
                                + " extent=" + extentNo + " pages=" + pagesInFile);
            }
            for (int pageInExtent = 0; pageInExtent < pagesPerExtent; pageInExtent++) {
                int bitmapOffset = base + ExtentDescriptorLayout.BITMAP + pageInExtent / 8;
                boolean allocated = (page0[bitmapOffset] & (1 << (pageInExtent % 8))) != 0;
                if (pageInExtent >= pagesInFile && allocated) {
                    throw new TablespaceScrubException(
                            "XDES bitmap allocates page beyond file size: path=" + path
                                    + " extent=" + extentNo + " page=" + pageInExtent);
                }
                if (state == ExtentState.FREE && allocated) {
                    throw new TablespaceScrubException(
                            "FREE XDES extent contains allocated page: path=" + path
                                    + " extent=" + extentNo + " page=" + pageInExtent);
                }
                if (state == ExtentState.FULL_FRAG && pageInExtent < pagesInFile && !allocated) {
                    throw new TablespaceScrubException(
                            "FULL_FRAG XDES extent contains free page: path=" + path
                                    + " extent=" + extentNo + " page=" + pageInExtent);
                }
            }
        }
    }

    /**
     * 校验一个原始 XDES prev/next 地址满足当前 page0 内嵌 FLST node 布局。
     *
     * @param image page0 大端视图
     * @param offset FileAddress 的 12 字节起点
     * @param extentCount 当前文件覆盖、允许被链指针引用的 entry 数
     * @param path 候选诊断路径
     * @param ownerExtent 持有该指针的 extent 号
     * @param field 指针字段名，仅用于诊断
     * @return NULL 地址返回 -1；非空地址返回被引用的 extent 号
     * @throws TablespaceScrubException 非 NULL 地址越页、未对齐、自引用或引用文件范围外 entry 时抛出
     */
    private static int validateXdesAddress(
            ByteBuffer image,
            int offset,
            int extentCount,
            Path path,
            int ownerExtent,
            String field) {
        long pageNo = image.getLong(offset);
        int nodeOffset = image.getInt(offset + Long.BYTES);
        if (pageNo == 0 && nodeOffset == 0) {
            return -1;
        }
        int firstNodeOffset = ExtentDescriptorLayout.entryOffset(0) + ExtentDescriptorLayout.PREV;
        int relative = nodeOffset - firstNodeOffset;
        int referencedExtent = relative < 0 ? -1 : relative / ExtentDescriptorLayout.ENTRY_SIZE;
        if (pageNo != 0 || relative < 0 || relative % ExtentDescriptorLayout.ENTRY_SIZE != 0
                || referencedExtent >= extentCount || referencedExtent == ownerExtent) {
            throw new TablespaceScrubException(
                    "invalid XDES " + field + " address: path=" + path + " extent=" + ownerExtent
                            + " page=" + pageNo + " offset=" + nodeOffset);
        }
        return referencedExtent;
    }

    /**
     * 校验 XDES 双向链节点的反向地址精确指回当前 entry，拒绝只有单边看似对齐的损坏指针。
     *
     * @param image page0 大端视图
     * @param referencedExtent 前向字段引用的 extent；-1 表示 NULL，无需反向检查
     * @param reciprocalField 被引用 entry 中应当反指当前节点的 PREV 或 NEXT 字段偏移
     * @param expectedNodeOffset 当前 entry 的 PREV 节点地址
     * @param expectedState 当前 entry 的 XDES 状态；同一物理链中的邻居必须一致
     * @param expectedOwner 当前 entry 的 segment owner；同一 segment 链中的邻居必须一致
     * @param states 持久 ordinal 到领域状态的固定映射
     * @param path 候选诊断路径
     * @param ownerExtent 当前 entry 号
     * @param relation 反向关系诊断名
     * @throws TablespaceScrubException 反向指针不是 page0 当前节点地址时抛出
     */
    private static void validateXdesReciprocalAddress(
            ByteBuffer image,
            int referencedExtent,
            int reciprocalField,
            int expectedNodeOffset,
            ExtentState expectedState,
            long expectedOwner,
            ExtentState[] states,
            Path path,
            int ownerExtent,
            String relation) {
        if (referencedExtent < 0) {
            return;
        }
        int reciprocal = ExtentDescriptorLayout.entryOffset(referencedExtent) + reciprocalField;
        long reciprocalPage = image.getLong(reciprocal);
        int reciprocalOffset = image.getInt(reciprocal + Long.BYTES);
        int targetBase = ExtentDescriptorLayout.entryOffset(referencedExtent);
        int targetOrdinal = image.getInt(targetBase + ExtentDescriptorLayout.STATE);
        long targetOwner = image.getLong(targetBase + ExtentDescriptorLayout.OWNER_SEGMENT);
        if (reciprocalPage != 0 || reciprocalOffset != expectedNodeOffset) {
            throw new TablespaceScrubException(
                    "invalid XDES reciprocal " + relation + " address: path=" + path
                            + " extent=" + ownerExtent + " referencedExtent=" + referencedExtent);
        }
        if (targetOrdinal < 0 || targetOrdinal >= states.length
                || states[targetOrdinal] != expectedState || targetOwner != expectedOwner) {
            throw new TablespaceScrubException(
                    "XDES list crosses state or owner boundary: path=" + path
                            + " extent=" + ownerExtent + " referencedExtent=" + referencedExtent);
        }
    }

    /** 校验 page0 自描述、文件长度、GENERAL lifecycle 与固定 SDI root。 */
    private static void validatePageZero(TablespaceFullScrubRequest request, SpaceHeaderPhysical physical,
                                         int pageCount, byte[] page0) {
        if (!physical.spaceId().equals(request.expectedSpaceId())
                || !physical.pageSize().equals(request.expectedPageSize())
                || physical.currentSizeInPages().value() != pageCount
                || physical.freeLimitPageNo().value() > pageCount
                || physical.spaceVersion() <= 0) {
            throw new TablespaceScrubException("tablespace page0 identity/size is inconsistent: " + request.path());
        }
        TablespaceType type = TablespaceTypeFlags.decode(physical.spaceFlags());
        if (type != TablespaceType.GENERAL && type != TablespaceType.FILE_PER_TABLE) {
            throw new TablespaceScrubException("catalog recovery candidate is not a user tablespace: " + type);
        }
        TablespaceLifecycleHeader lifecycle = TablespaceLifecycleRawCodec
                .read(ByteBuffer.wrap(page0).order(ByteOrder.BIG_ENDIAN))
                .orElseThrow(() -> new TablespaceScrubException(
                        "catalog recovery candidate lacks lifecycle marker: " + request.path()));
        if (lifecycle.state() != TablespaceState.NORMAL
                || lifecycle.initialSizeInPages().value() > pageCount) {
            throw new TablespaceScrubException(
                    "catalog recovery candidate is not stable NORMAL: " + lifecycle.state());
        }
        long root = ByteBuffer.wrap(page0).order(ByteOrder.BIG_ENDIAN).getLong(SpaceHeaderLayout.SDI_ROOT);
        if (root != SdiPageLayout.PAGE_NO) {
            throw new TablespaceScrubException("catalog recovery candidate has unsupported SDI root: " + root);
        }
    }

    /** 校验严格 checksum 和物理信封；expectedType 为空时仍拒绝未知 page type。 */
    private static void validatePageEnvelope(byte[] bytes, PageSize pageSize, SpaceId spaceId,
                                             int pageNo, PageType expectedType) {
        if (!PageImageChecksum.verify(bytes, pageSize)) {
            throw new TablespaceScrubException("tablespace page checksum/trailer mismatch: page=" + pageNo);
        }
        ByteBuffer page = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (page.getInt(PageEnvelopeLayout.SPACE_ID) != spaceId.value()
                || page.getInt(PageEnvelopeLayout.PAGE_NO) != pageNo) {
            throw new TablespaceScrubException("tablespace page envelope identity mismatch: page=" + pageNo);
        }
        PageType actual = PageType.fromCode(page.getInt(PageEnvelopeLayout.PAGE_TYPE));
        if (expectedType != null && actual != expectedType) {
            throw new TablespaceScrubException(
                    "tablespace page type mismatch: page=" + pageNo + " actual=" + actual);
        }
    }

    /** 解码 page3 opaque SDI、payload CRC 与全零 index-DDL footer。 */
    private static SerializedDictionaryInfo decodeSdi(byte[] bytes, TablespaceFullScrubRequest request) {
        ByteBuffer page = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (PageType.fromCode(page.getInt(PageEnvelopeLayout.PAGE_TYPE)) != PageType.SDI
                || page.getInt(SdiPageLayout.MAGIC_OFFSET) != SdiPageLayout.MAGIC
                || page.getInt(SdiPageLayout.FORMAT_OFFSET) != SdiPageLayout.FORMAT_VERSION) {
            throw new TablespaceScrubException("tablespace page3 SDI envelope/format is invalid: " + request.path());
        }
        long tableId = page.getLong(SdiPageLayout.TABLE_ID_OFFSET);
        long version = page.getLong(SdiPageLayout.DICTIONARY_VERSION_OFFSET);
        int length = page.getInt(SdiPageLayout.PAYLOAD_LENGTH_OFFSET);
        int expectedCrc = page.getInt(SdiPageLayout.PAYLOAD_CRC32C_OFFSET);
        if (tableId != request.expectedTableId() || version <= 0 || length <= 0
                || length > SdiPageLayout.payloadCapacity(request.expectedPageSize())) {
            throw new TablespaceScrubException(
                    "tablespace page3 SDI identity/length is invalid: " + request.path());
        }
        byte[] payload = Arrays.copyOfRange(bytes, SdiPageLayout.PAYLOAD_OFFSET,
                SdiPageLayout.PAYLOAD_OFFSET + length);
        CRC32C crc = new CRC32C();
        crc.update(payload, 0, payload.length);
        if ((int) crc.getValue() != expectedCrc) {
            throw new TablespaceScrubException("tablespace page3 SDI payload CRC32C mismatch: " + request.path());
        }
        int footer = SdiPageLayout.indexBuildFooterOffset(request.expectedPageSize());
        for (int offset = footer; offset < footer + SdiPageLayout.INDEX_BUILD_FOOTER_BYTES; offset++) {
            if (bytes[offset] != 0) {
                throw new TablespaceScrubException(
                        "tablespace page3 contains unresolved index DDL footer: " + request.path());
            }
        }
        return new SerializedDictionaryInfo(tableId, version, payload);
    }

    /** 从 page0 内嵌 XDES bitmap 判断物理页是否已经分配。 */
    private static boolean isAllocated(byte[] page0, PageSize pageSize, int pageNo) {
        int pagesPerExtent = pageSize.pagesPerExtent();
        long extentNo = pageNo / pagesPerExtent;
        int pageInExtent = pageNo % pagesPerExtent;
        int offset = ExtentDescriptorLayout.entryOffset(extentNo)
                + ExtentDescriptorLayout.BITMAP + pageInExtent / 8;
        int mask = 1 << (pageInExtent % 8);
        return (page0[offset] & mask) != 0;
    }

    /** 判断整页是否仍为文件扩展留下的零填充未分配页。 */
    private static boolean isAllZero(byte[] bytes) {
        for (byte value : bytes) {
            if (value != 0) {
                return false;
            }
        }
        return true;
    }

    /** 使用显式位置完整读取一个物理页。 */
    private static void readPage(FileChannel channel, ByteBuffer page, long position, int pageBytes,
                                 long deadline, Path path, int pageNo)
            throws IOException {
        page.clear();
        int read = 0;
        while (page.hasRemaining()) {
            checkDeadline(deadline, path, pageNo);
            int count = channel.read(page, position + read);
            if (count < 0) {
                throw new IOException("unexpected EOF while reading tablespace page");
            }
            if (count == 0) {
                Thread.onSpinWait();
                continue;
            }
            read += count;
        }
        page.position(0);
        page.limit(pageBytes);
    }

    /** NOFOLLOW 获取文件属性。 */
    private static BasicFileAttributes attributes(Path path) throws IOException {
        return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    }

    /** 把可选 fileKey 转为稳定诊断/token 字段。 */
    private static String fileKey(BasicFileAttributes attributes) {
        return attributes.fileKey() == null ? "" : attributes.fileKey().toString();
    }

    /** 建立单一 monotonic deadline，溢出时饱和为 Long.MAX_VALUE。 */
    private static long deadline(Duration timeout) {
        try {
            long nanos = timeout.toNanos();
            long now = System.nanoTime();
            long deadline = now + nanos;
            return deadline < 0 && now > 0 ? Long.MAX_VALUE : deadline;
        } catch (ArithmeticException failure) {
            return Long.MAX_VALUE;
        }
    }

    /** 在页边界检查中断和 deadline；中断状态保持给上层观察。 */
    private static void checkDeadline(long deadline, Path path, int pageNo) {
        if (Thread.currentThread().isInterrupted()) {
            throw new TablespaceScrubException(
                    "tablespace full scrub interrupted: " + path + " page=" + pageNo);
        }
        if (deadline != Long.MAX_VALUE && System.nanoTime() - deadline >= 0) {
            throw new TablespaceScrubException(
                    "tablespace full scrub timed out: " + path + " page=" + pageNo);
        }
    }

    /** 获取标准 SHA-256 实例。 */
    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new TablespaceScrubException("SHA-256 is unavailable", failure);
        }
    }
}
