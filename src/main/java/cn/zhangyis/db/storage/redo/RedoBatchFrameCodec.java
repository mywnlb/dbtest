package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoNo;
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
    static final int PAYLOAD_HEADER_BYTES = 20;
    /** 最小 record：PAGE_INIT，tag(1) + pageId(space 4 + pageNo 8) + type(4)。 */
    private static final int MIN_RECORD_BYTES = 17;

    private RedoBatchFrameCodec() {
    }

    /**
     * 把一个批次编码为完整帧（含外层 header）。返回已 flip、可直接写入 channel 的缓冲；其 {@code remaining()} 即帧字节数，
     * 文件环据此判断当前文件剩余容量是否放得下整批。
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param batch redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @return {@code encodeFrame} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static ByteBuffer encodeFrame(RedoLogBatch batch) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (batch == null) {
            throw new DatabaseValidationException("redo log batch must not be null");
        }
        byte[] payload = encodePayload(batch);
        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw new DatabaseValidationException("redo batch payload exceeds recoverable frame limit: "
                    + payload.length + " > " + MAX_PAYLOAD_BYTES);
        }
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        ByteBuffer frame = ByteBuffer.allocate(FRAME_HEADER_BYTES + payload.length);
        frame.putInt(MAGIC);
        frame.putInt(payload.length);
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        frame.putInt(crc32(payload));
        frame.put(payload);
        frame.flip();
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return frame;
    }

    /**
     * 从内容缓冲顺序解析连续帧。
     *
     * <p>数据流：从当前 position 起，反复读外层 header→校验 magic/length→读 payload→校验 crc→解码批次。遇到不足一个完整
     * header 或 payload 的尾部即停止（视为 crash torn tail，position 停在该帧起点）；遇到「字节足够但结构非法」的完整帧则抛
     * {@link RedoLogCorruptedException}，不静默跳过。返回的批次顺序与文件内物理顺序一致。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param content 待扫描内容（position 在数据起点，limit 在数据末尾）。
     * @return 完整且校验通过的批次列表。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    static List<RedoLogBatch> decodeFrames(ByteBuffer content) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (content == null) {
            throw new DatabaseValidationException("redo frame content must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        List<RedoLogBatch> out = new ArrayList<>();
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
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
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return out;
    }

    /** 解码单个 payload（不含外层 header）。供单文件仓储的增量 channel 扫描复用。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param bytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return {@code decodePayload} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    static RedoLogBatch decodePayload(byte[] bytes) {
        try {
            // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            LogRange range = new LogRange(Lsn.of(in.readLong()), Lsn.of(in.readLong()));
            // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
            int count = in.readInt();
            int remaining = bytes.length - PAYLOAD_HEADER_BYTES;
            if (count < 0 || count > remaining / MIN_RECORD_BYTES) {
                throw new RedoLogCorruptedException("redo record count invalid: " + count);
            }
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            List<RedoRecord> records = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                records.add(readRecord(in));
            }
            if (in.available() != 0) {
                throw new RedoLogCorruptedException("redo payload has trailing bytes: " + in.available());
            }
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            return new RedoLogBatch(range, records);
        } catch (IOException e) {
            throw new RedoLogCorruptedException("failed to decode redo payload", e);
        } catch (DatabaseValidationException e) {
            throw new RedoLogCorruptedException("redo payload contains invalid domain value", e);
        }
    }

    /** CRC32 校验和（int 截断），帧 payload 完整性校验。
     *
     * @param bytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return 按当前持久格式计算的校验位模式；使用 Java 数值类型承载无符号结果，不代表有符号业务量
     */
    static int crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }

    /**
     * 把调用方领域值编码为Redo/WAL的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param batch redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @return {@code encodePayload} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     * @throws RedoLogIoException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private static byte[] encodePayload(RedoLogBatch batch) {
        try {
            // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
            out.writeLong(batch.range().start().value());
            out.writeLong(batch.range().end().value());
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            out.writeInt(batch.records().size());
            for (RedoRecord r : batch.records()) {
                writeRecord(out, r);
            }
            out.flush();
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new RedoLogIoException("failed to encode redo payload", e);
        }
    }

    /**
     * 校验输入与当前状态后修改Redo/WAL领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param out 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param r redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private static void writeRecord(DataOutputStream out, RedoRecord r) throws IOException {
        if (r instanceof PageInitRecord pir) {
            out.writeByte(RedoRecordType.PAGE_INIT.tag());
            writePageId(out, pir.pageId());
            out.writeInt(pir.pageType().code());
        } else if (r instanceof PageBytesRecord pbr) {
            out.writeByte(RedoRecordType.PAGE_BYTES.tag());
            writePageId(out, pbr.pageId());
            out.writeInt(pbr.offset());
            byte[] bytes = pbr.bytes();
            out.writeInt(bytes.length);
            out.write(bytes);
        } else if (r instanceof FspPageAllocationRecord far) {
            out.writeByte(RedoRecordType.FSP_PAGE_ALLOC.tag());
            writePageId(out, far.allocatedPageId());
            out.writeInt(far.inodeSlot());
            out.writeLong(far.segmentId().value());
            out.writeBoolean(far.autoExtendRetry());
        } else if (r instanceof FspMetadataDeltaRecord fmd) {
            out.writeByte(RedoRecordType.FSP_METADATA_DELTA.tag());
            writePageId(out, fmd.pageId());
            out.writeByte(fmd.kind().code());
            out.writeLong(fmd.subjectId());
            out.writeInt(fmd.subIndex());
            out.writeInt(fmd.offset());
            byte[] payload = fmd.afterImage();
            out.writeInt(payload.length);
            out.write(payload);
        } else if (r instanceof FspPageFreeRecord fpf) {
            out.writeByte(RedoRecordType.FSP_PAGE_FREE.tag());
            writePageId(out, fpf.freedPageId());
            out.writeInt(fpf.inodeSlot());
            out.writeLong(fpf.segmentId().value());
        } else if (r instanceof UndoMetadataDeltaRecord umd) {
            out.writeByte(RedoRecordType.UNDO_METADATA_DELTA.tag());
            writePageId(out, umd.pageId());
            out.writeByte(umd.kind().code());
            out.writeLong(umd.subjectId());
            out.writeInt(umd.subIndex());
            out.writeInt(umd.offset());
            byte[] payload = umd.afterImage();
            out.writeInt(payload.length);
            out.write(payload);
        } else if (r instanceof UndoRecordPayloadRecord urp) {
            out.writeByte(RedoRecordType.UNDO_RECORD_PAYLOAD.tag());
            writePageId(out, urp.pageId());
            out.writeLong(urp.transactionId().value());
            out.writeLong(urp.undoNo().value());
            out.writeInt(urp.recordOffset());
            byte[] payload = urp.slotImage();
            out.writeInt(payload.length);
            out.write(payload);
        } else if (r instanceof BTreePageDeltaRecord btd) {
            out.writeByte(RedoRecordType.BTREE_PAGE_DELTA.tag());
            writePageId(out, btd.pageId());
            out.writeLong(btd.indexId());
            out.writeByte(btd.kind().code());
            out.writeLong(btd.subjectId());
            out.writeInt(btd.offset());
            byte[] payload = btd.afterImage();
            out.writeInt(payload.length);
            out.write(payload);
        } else if (r instanceof TransactionStateDeltaRecord tsd) {
            out.writeByte(RedoRecordType.TRX_STATE_DELTA.tag());
            out.writeLong(tsd.transactionId().value());
            out.writeByte(tsd.fromState().code());
            out.writeByte(tsd.toState().code());
            out.writeLong(tsd.transactionNo().value());
            out.writeByte(tsd.reason().code());
        } else {
            throw new RedoLogCorruptedException("unsupported redo record type: " + r.getClass().getName());
        }
    }

    /**
     * 根据调用参数创建或转换 {@code readRecord} 返回的 {@code RedoRecord}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param in 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @return {@code readRecord} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private static RedoRecord readRecord(DataInputStream in) throws IOException {
        RedoRecordType type = RedoRecordType.fromTag(in.readByte());
        return switch (type) {
            case PAGE_INIT -> new PageInitRecord(readPageId(in), PageType.fromCode(in.readInt()));
            case PAGE_BYTES -> {
                PageId pageId = readPageId(in);
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
                    readPageId(in), in.readInt(), SegmentId.of(in.readLong()), in.readBoolean());
            case FSP_METADATA_DELTA -> {
                PageId pageId = readPageId(in);
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
            case FSP_PAGE_FREE -> new FspPageFreeRecord(readPageId(in), in.readInt(), SegmentId.of(in.readLong()));
            case UNDO_METADATA_DELTA -> {
                PageId pageId = readPageId(in);
                UndoMetadataDeltaKind kind = UndoMetadataDeltaKind.fromCode(in.readByte());
                long subjectId = in.readLong();
                int subIndex = in.readInt();
                int offset = in.readInt();
                int len = in.readInt();
                if (len < 0 || len > MAX_PAYLOAD_BYTES) {
                    throw new RedoLogCorruptedException("undo metadata delta payload length invalid: " + len);
                }
                byte[] payload = in.readNBytes(len);
                if (payload.length != len) {
                    throw new RedoLogCorruptedException("undo metadata delta payload truncated");
                }
                yield new UndoMetadataDeltaRecord(pageId, kind, subjectId, subIndex, offset, payload);
            }
            case UNDO_RECORD_PAYLOAD -> {
                PageId pageId = readPageId(in);
                TransactionId transactionId = TransactionId.of(in.readLong());
                UndoNo undoNo = UndoNo.of(in.readLong());
                int offset = in.readInt();
                int len = in.readInt();
                if (len < 0 || len > MAX_PAYLOAD_BYTES) {
                    throw new RedoLogCorruptedException("undo record payload length invalid: " + len);
                }
                byte[] payload = in.readNBytes(len);
                if (payload.length != len) {
                    throw new RedoLogCorruptedException("undo record payload truncated");
                }
                yield new UndoRecordPayloadRecord(pageId, transactionId, undoNo, offset, payload);
            }
            case BTREE_PAGE_DELTA -> {
                PageId pageId = readPageId(in);
                long indexId = in.readLong();
                BTreePageDeltaKind kind = BTreePageDeltaKind.fromCode(in.readByte());
                long subjectId = in.readLong();
                int offset = in.readInt();
                int len = in.readInt();
                if (len < 0 || len > MAX_PAYLOAD_BYTES) {
                    throw new RedoLogCorruptedException("B+Tree page delta payload length invalid: " + len);
                }
                byte[] payload = in.readNBytes(len);
                if (payload.length != len) {
                    throw new RedoLogCorruptedException("B+Tree page delta payload truncated");
                }
                yield new BTreePageDeltaRecord(pageId, indexId, kind, subjectId, offset, payload);
            }
            case TRX_STATE_DELTA -> new TransactionStateDeltaRecord(
                    TransactionId.of(in.readLong()),
                    TransactionStateDeltaState.fromCode(in.readByte()),
                    TransactionStateDeltaState.fromCode(in.readByte()),
                    TransactionNo.of(in.readLong()),
                    TransactionStateDeltaReason.fromCode(in.readByte()));
        };
    }

    private static void writePageId(DataOutputStream out, PageId pageId) throws IOException {
        out.writeInt(pageId.spaceId().value());
        out.writeLong(pageId.pageNo().value());
    }

    private static PageId readPageId(DataInputStream in) throws IOException {
        return PageId.of(SpaceId.of(in.readInt()), PageNo.of(in.readLong()));
    }

    /**
     * 根据调用参数创建或转换 {@code pageIdOf} 返回的 {@code PageId}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param r redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @return {@code pageIdOf} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private static PageId pageIdOf(RedoRecord r) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (r instanceof PageInitRecord pir) {
            return pir.pageId();
        }
        if (r instanceof PageBytesRecord pbr) {
            return pbr.pageId();
        }
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        if (r instanceof FspPageAllocationRecord far) {
            return far.allocatedPageId();
        }
        if (r instanceof FspMetadataDeltaRecord fmd) {
            return fmd.pageId();
        }
        if (r instanceof FspPageFreeRecord fpf) {
            return fpf.freedPageId();
        }
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        if (r instanceof UndoMetadataDeltaRecord umd) {
            return umd.pageId();
        }
        if (r instanceof UndoRecordPayloadRecord urp) {
            return urp.pageId();
        }
        if (r instanceof BTreePageDeltaRecord btd) {
            return btd.pageId();
        }
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        throw new RedoLogCorruptedException("unsupported redo record type: " + r.getClass().getName());
    }
}
