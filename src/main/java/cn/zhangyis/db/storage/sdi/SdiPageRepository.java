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

    private final BufferPool pool;
    private final PageSize pageSize;
    private final SpaceHeaderRepository spaceHeaders;

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
     * @param mtr 只读或活动 MTR，负责 page0→page3 的 latch/fix 生命周期
     * @param spaceId 已打开 GENERAL 表空间
     * @return 有效快照，或 legacy/空 SDI 的 empty
     * @throws SdiPageCorruptionException root、envelope、format、长度、identity 或 CRC 不合法时抛出
     */
    public Optional<SdiPageSnapshot> read(MiniTransaction mtr, SpaceId spaceId) {
        require(mtr, spaceId);
        SpaceHeaderSnapshot header = spaceHeaders.read(mtr, spaceId);
        if (header.sdiRootPageNo() == 0) {
            return Optional.empty();
        }
        requireV1Root(header.sdiRootPageNo());
        PageGuard page = mtr.getPage(pool, pageId(spaceId), PageLatchMode.SHARED);
        validateEnvelope(page, spaceId);
        validateFormat(page);
        long tableId = page.readLong(SdiPageLayout.TABLE_ID_OFFSET);
        long version = page.readLong(SdiPageLayout.DICTIONARY_VERSION_OFFSET);
        int length = page.readInt(SdiPageLayout.PAYLOAD_LENGTH_OFFSET);
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
        page.writeBytes(SdiPageLayout.MAGIC_OFFSET, body(tableId, dictionaryVersion, copied));
    }

    /**
     * 读取 page3 footer 中未决 CREATE INDEX 资源；全零 footer 返回 empty，未知 magic/version/CRC 拒绝恢复。
     *
     * @param mtr 负责 page0→page3 S latch/fix 的短只读 MTR
     * @param spaceId 已打开的目标表空间
     * @return 完整 build descriptor，或没有未决构建时为空
     * @throws SdiPageCorruptionException footer 与 SDI payload 重叠、损坏或物理 identity 跨 space 时抛出
     */
    public Optional<SdiIndexBuildDescriptor> readIndexBuild(MiniTransaction mtr, SpaceId spaceId) {
        require(mtr, spaceId);
        SpaceHeaderSnapshot header = spaceHeaders.read(mtr, spaceId);
        requireV1Root(header.sdiRootPageNo());
        PageGuard page = mtr.getPage(pool, pageId(spaceId), PageLatchMode.SHARED);
        validateEnvelope(page, spaceId);
        validateFormat(page);
        requireFooterAvailable(page);
        byte[] footer = page.readBytes(SdiPageLayout.indexBuildFooterOffset(pageSize),
                SdiPageLayout.INDEX_BUILD_FOOTER_BYTES);
        if (java.util.Arrays.equals(footer, new byte[footer.length])) {
            return Optional.empty();
        }
        return Optional.of(decodeIndexBuild(spaceId, footer));
    }

    /**
     * 把 staged segment/root 与 DDL identity 写入 page3。调用方必须与 segment/root 创建放在同一 MTR。
     *
     * @param mtr 正在创建新 index 物理资源的活动 MTR
     * @param spaceId 既有表空间
     * @param descriptor 待持久化的未决资源所有权
     * @throws SdiPageCorruptionException footer 已占用、旧 payload 占用保留区或页损坏时抛出
     */
    public void writeIndexBuild(MiniTransaction mtr, SpaceId spaceId, SdiIndexBuildDescriptor descriptor) {
        require(mtr, spaceId);
        if (descriptor == null || !descriptor.indexBinding().rootPageId().spaceId().equals(spaceId)
                || !descriptor.indexBinding().leafSegment().spaceId().equals(spaceId)
                || !descriptor.indexBinding().nonLeafSegment().spaceId().equals(spaceId)) {
            throw new DatabaseValidationException("SDI index build descriptor does not belong to target space");
        }
        SpaceHeaderSnapshot header = spaceHeaders.readForUpdate(mtr, spaceId);
        requireV1Root(header.sdiRootPageNo());
        PageGuard page = mtr.getPage(pool, pageId(spaceId), PageLatchMode.EXCLUSIVE);
        validateEnvelope(page, spaceId);
        validateFormat(page);
        requireFooterAvailable(page);
        byte[] current = page.readBytes(SdiPageLayout.indexBuildFooterOffset(pageSize),
                SdiPageLayout.INDEX_BUILD_FOOTER_BYTES);
        if (!java.util.Arrays.equals(current, new byte[current.length])) {
            throw new SdiPageCorruptionException("SDI index build footer is already occupied");
        }
        page.writeBytes(SdiPageLayout.indexBuildFooterOffset(pageSize), encodeIndexBuild(descriptor));
    }

    /**
     * 只在 footer 与 expected 完全相等时清空 build descriptor，避免旧恢复任务擦除另一条 DDL 的所有权证据。
     */
    public void clearIndexBuild(MiniTransaction mtr, SpaceId spaceId, SdiIndexBuildDescriptor expected) {
        require(mtr, spaceId);
        if (expected == null) {
            throw new DatabaseValidationException("expected SDI index build descriptor must not be null");
        }
        SpaceHeaderSnapshot header = spaceHeaders.readForUpdate(mtr, spaceId);
        requireV1Root(header.sdiRootPageNo());
        PageGuard page = mtr.getPage(pool, pageId(spaceId), PageLatchMode.EXCLUSIVE);
        validateEnvelope(page, spaceId);
        validateFormat(page);
        requireFooterAvailable(page);
        byte[] current = page.readBytes(SdiPageLayout.indexBuildFooterOffset(pageSize),
                SdiPageLayout.INDEX_BUILD_FOOTER_BYTES);
        if (!decodeIndexBuild(spaceId, current).equals(expected)) {
            throw new SdiPageCorruptionException("SDI index build descriptor identity changed before clear");
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
                    "existing SDI payload occupies CREATE INDEX footer reservation: " + payloadLength);
        }
    }

    private static byte[] encodeIndexBuild(SdiIndexBuildDescriptor descriptor) {
        IndexStorageBinding index = descriptor.indexBinding();
        ByteBuffer footer = ByteBuffer.allocate(SdiPageLayout.INDEX_BUILD_FOOTER_BYTES)
                .order(ByteOrder.BIG_ENDIAN);
        footer.putInt(SdiPageLayout.INDEX_BUILD_MAGIC)
                .putInt(SdiPageLayout.INDEX_BUILD_FORMAT_VERSION)
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

    private static SdiIndexBuildDescriptor decodeIndexBuild(SpaceId spaceId, byte[] footer) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(footer).order(ByteOrder.BIG_ENDIAN);
            if (buffer.getInt() != SdiPageLayout.INDEX_BUILD_MAGIC
                    || buffer.getInt() != SdiPageLayout.INDEX_BUILD_FORMAT_VERSION) {
                throw new SdiPageCorruptionException("invalid SDI index build footer magic/version");
            }
            long ddlId = buffer.getLong();
            long version = buffer.getLong();
            long tableId = buffer.getLong();
            long indexId = buffer.getLong();
            long rootPageNo = buffer.getLong();
            int rootLevel = buffer.getInt();
            SegmentRef leaf = new SegmentRef(spaceId, buffer.getInt(), SegmentId.of(buffer.getLong()));
            SegmentRef nonLeaf = new SegmentRef(spaceId, buffer.getInt(), SegmentId.of(buffer.getLong()));
            int crcOffset = buffer.position();
            int expectedCrc = buffer.getInt();
            if (expectedCrc != crc32c(java.util.Arrays.copyOf(footer, crcOffset))) {
                throw new SdiPageCorruptionException("SDI index build footer CRC32C mismatch");
            }
            for (int offset = buffer.position(); offset < footer.length; offset++) {
                if (footer[offset] != 0) {
                    throw new SdiPageCorruptionException("SDI index build footer reserved bytes are non-zero");
                }
            }
            IndexStorageBinding binding = new IndexStorageBinding(indexId,
                    PageId.of(spaceId, PageNo.of(rootPageNo)), rootLevel, leaf, nonLeaf);
            return new SdiIndexBuildDescriptor(ddlId, version, tableId, binding);
        } catch (SdiPageCorruptionException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new SdiPageCorruptionException("decode SDI index build footer failed", failure);
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
