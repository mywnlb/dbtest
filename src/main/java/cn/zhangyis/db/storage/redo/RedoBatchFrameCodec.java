package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.page.PageType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Redo 批次帧编解码。
 *
 * <p>从 {@link RedoLogFileRepository} 抽出，使「单文件仓储」与「文件环仓储」(0.18) 共用同一套帧格式，避免两套序列化逻辑
 * 在 crash recovery 跨实现读取时漂移。本类只负责 byte 级序列化，不持有 channel、不决定文件布局/轮转/回收。
 *
 * <p>帧布局：{@code magic(4) + payloadLength(4) + crc32(4) + payload}；
 * payload 布局：{@code startLsn(8) + endLsn(8) + recordCount(4) + records...}。
 * 完整但 magic/length/crc/record 不合法的帧视为致命损坏；不完整尾部（torn tail）由扫描方按 crash 截断点处理。
 */
final class RedoBatchFrameCodec {

    /** 帧外层 magic：ASCII "RLG1"。 */
    static final int MAGIC = 0x524C4731;
    /** 帧外层头：magic(4) + payloadLength(4) + crc32(4)。 */
    static final int FRAME_HEADER_BYTES = 12;
    /** 防止损坏 length 触发大内存分配；教学实现单批 32MiB 足够。 */
    static final int MAX_PAYLOAD_BYTES = 32 * 1024 * 1024;
    /** payload 内层头：startLsn(8) + endLsn(8) + recordCount(4)。 */
    private static final int PAYLOAD_HEADER_BYTES = 20;
    /** 最小 record：tag(1) + pageId(space 4 + pageNo 8) + type(4)。 */
    private static final int MIN_RECORD_BYTES = 17;

    private RedoBatchFrameCodec() {
    }

    /**
     * 把一个批次编码为完整帧（含外层 header）。返回已 flip、可直接写入 channel 的缓冲；其 {@code remaining()} 即帧字节数，
     * 文件环据此判断当前文件剩余容量是否放得下整批。
     */
    static ByteBuffer encodeFrame(RedoLogBatch batch) {
        if (batch == null) {
            throw new DatabaseValidationException("redo log batch must not be null");
        }
        byte[] payload = encodePayload(batch);
        ByteBuffer frame = ByteBuffer.allocate(FRAME_HEADER_BYTES + payload.length);
        frame.putInt(MAGIC);
        frame.putInt(payload.length);
        frame.putInt(crc32(payload));
        frame.put(payload);
        frame.flip();
        return frame;
    }

    /**
     * 从内容缓冲顺序解析连续帧。
     *
     * <p>数据流：从当前 position 起，反复读外层 header→校验 magic/length→读 payload→校验 crc→解码批次。遇到不足一个完整
     * header 或 payload 的尾部即停止（视为 crash torn tail，position 停在该帧起点）；遇到「字节足够但结构非法」的完整帧则抛
     * {@link RedoLogCorruptedException}，不静默跳过。返回的批次顺序与文件内物理顺序一致。
     *
     * @param content 待扫描内容（position 在数据起点，limit 在数据末尾）。
     * @return 完整且校验通过的批次列表。
     */
    static List<RedoLogBatch> decodeFrames(ByteBuffer content) {
        if (content == null) {
            throw new DatabaseValidationException("redo frame content must not be null");
        }
        List<RedoLogBatch> out = new ArrayList<>();
        while (content.remaining() >= FRAME_HEADER_BYTES) {
            int frameStart = content.position();
            int magic = content.getInt();
            int payloadLength = content.getInt();
            int expectedCrc = content.getInt();
            if (magic != MAGIC) {
                throw new RedoLogCorruptedException("redo frame magic mismatch: " + magic);
            }
            if (payloadLength <= 0 || payloadLength > MAX_PAYLOAD_BYTES) {
                throw new RedoLogCorruptedException("redo frame payload length invalid: " + payloadLength);
            }
            if (content.remaining() < payloadLength) {
                // payload 尾部不完整：crash 截断点，回退 position 让调用方据此定位续写偏移。
                content.position(frameStart);
                break;
            }
            byte[] bytes = new byte[payloadLength];
            content.get(bytes);
            if (crc32(bytes) != expectedCrc) {
                throw new RedoLogCorruptedException("redo frame checksum mismatch");
            }
            out.add(decodePayload(bytes));
        }
        return out;
    }

    /** 解码单个 payload（不含外层 header）。供单文件仓储的增量 channel 扫描复用。 */
    static RedoLogBatch decodePayload(byte[] bytes) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            LogRange range = new LogRange(Lsn.of(in.readLong()), Lsn.of(in.readLong()));
            int count = in.readInt();
            int remaining = bytes.length - PAYLOAD_HEADER_BYTES;
            if (count < 0 || count > remaining / MIN_RECORD_BYTES) {
                throw new RedoLogCorruptedException("redo record count invalid: " + count);
            }
            List<RedoRecord> records = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                records.add(readRecord(in));
            }
            if (in.available() != 0) {
                throw new RedoLogCorruptedException("redo payload has trailing bytes: " + in.available());
            }
            return new RedoLogBatch(range, records);
        } catch (IOException e) {
            throw new RedoLogCorruptedException("failed to decode redo payload", e);
        } catch (DatabaseValidationException e) {
            throw new RedoLogCorruptedException("redo payload contains invalid domain value", e);
        }
    }

    /** CRC32 校验和（int 截断），帧 payload 完整性校验。 */
    static int crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }

    private static byte[] encodePayload(RedoLogBatch batch) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeLong(batch.range().start().value());
            out.writeLong(batch.range().end().value());
            out.writeInt(batch.records().size());
            for (RedoRecord r : batch.records()) {
                writeRecord(out, r);
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new RedoLogIoException("failed to encode redo payload", e);
        }
    }

    private static void writeRecord(DataOutputStream out, RedoRecord r) throws IOException {
        PageId pageId = pageIdOf(r);
        if (r instanceof PageInitRecord pir) {
            out.writeByte(RedoRecordType.PAGE_INIT.tag());
            writePageId(out, pageId);
            out.writeInt(pir.pageType().code());
        } else if (r instanceof PageBytesRecord pbr) {
            out.writeByte(RedoRecordType.PAGE_BYTES.tag());
            writePageId(out, pageId);
            out.writeInt(pbr.offset());
            byte[] bytes = pbr.bytes();
            out.writeInt(bytes.length);
            out.write(bytes);
        } else if (r instanceof FspPageAllocationRecord far) {
            out.writeByte(RedoRecordType.FSP_PAGE_ALLOC.tag());
            writePageId(out, pageId);
            out.writeInt(far.inodeSlot());
            out.writeLong(far.segmentId().value());
            out.writeBoolean(far.autoExtendRetry());
        } else if (r instanceof FspMetadataDeltaRecord fmd) {
            out.writeByte(RedoRecordType.FSP_METADATA_DELTA.tag());
            writePageId(out, pageId);
            out.writeByte(fmd.kind().code());
            out.writeLong(fmd.subjectId());
            out.writeInt(fmd.subIndex());
            out.writeInt(fmd.offset());
            byte[] payload = fmd.afterImage();
            out.writeInt(payload.length);
            out.write(payload);
        } else if (r instanceof FspPageFreeRecord fpf) {
            out.writeByte(RedoRecordType.FSP_PAGE_FREE.tag());
            writePageId(out, pageId);
            out.writeInt(fpf.inodeSlot());
            out.writeLong(fpf.segmentId().value());
        } else {
            throw new RedoLogCorruptedException("unsupported redo record type: " + r.getClass().getName());
        }
    }

    private static RedoRecord readRecord(DataInputStream in) throws IOException {
        RedoRecordType type = RedoRecordType.fromTag(in.readByte());
        PageId pageId = readPageId(in);
        return switch (type) {
            case PAGE_INIT -> new PageInitRecord(pageId, PageType.fromCode(in.readInt()));
            case PAGE_BYTES -> {
                int offset = in.readInt();
                int len = in.readInt();
                if (len < 0 || len > MAX_PAYLOAD_BYTES) {
                    throw new RedoLogCorruptedException("redo bytes length invalid: " + len);
                }
                byte[] payload = in.readNBytes(len);
                if (payload.length != len) {
                    throw new RedoLogCorruptedException("redo bytes payload truncated");
                }
                yield new PageBytesRecord(pageId, offset, payload);
            }
            case FSP_PAGE_ALLOC -> new FspPageAllocationRecord(
                    pageId, in.readInt(), SegmentId.of(in.readLong()), in.readBoolean());
            case FSP_METADATA_DELTA -> {
                FspMetadataDeltaKind kind = FspMetadataDeltaKind.fromCode(in.readByte());
                long subjectId = in.readLong();
                int subIndex = in.readInt();
                int offset = in.readInt();
                int len = in.readInt();
                if (len < 0 || len > MAX_PAYLOAD_BYTES) {
                    throw new RedoLogCorruptedException("FSP metadata delta payload length invalid: " + len);
                }
                byte[] payload = in.readNBytes(len);
                if (payload.length != len) {
                    throw new RedoLogCorruptedException("FSP metadata delta payload truncated");
                }
                yield new FspMetadataDeltaRecord(pageId, kind, subjectId, subIndex, offset, payload);
            }
            case FSP_PAGE_FREE -> new FspPageFreeRecord(pageId, in.readInt(), SegmentId.of(in.readLong()));
        };
    }

    private static void writePageId(DataOutputStream out, PageId pageId) throws IOException {
        out.writeInt(pageId.spaceId().value());
        out.writeLong(pageId.pageNo().value());
    }

    private static PageId readPageId(DataInputStream in) throws IOException {
        return PageId.of(SpaceId.of(in.readInt()), PageNo.of(in.readLong()));
    }

    private static PageId pageIdOf(RedoRecord r) {
        if (r instanceof PageInitRecord pir) {
            return pir.pageId();
        }
        if (r instanceof PageBytesRecord pbr) {
            return pbr.pageId();
        }
        if (r instanceof FspPageAllocationRecord far) {
            return far.allocatedPageId();
        }
        if (r instanceof FspMetadataDeltaRecord fmd) {
            return fmd.pageId();
        }
        if (r instanceof FspPageFreeRecord fpf) {
            return fpf.freedPageId();
        }
        throw new RedoLogCorruptedException("unsupported redo record type: " + r.getClass().getName());
    }
}
