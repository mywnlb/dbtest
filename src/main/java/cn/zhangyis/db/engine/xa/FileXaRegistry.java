package cn.zhangyis.db.engine.xa;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.XaId;
import cn.zhangyis.db.storage.recovery.PreparedTransactionDecision;
import cn.zhangyis.db.storage.recovery.PreparedTransactionDecisionProvider;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32C;

/**
 * 实例级 append-only XA registry。调用方必须在持有 {@code mysql.instance.lock} 时打开；
 * 本类的公平显式锁只串行同一 JVM 内 scan/append/close，不替代跨进程实例锁。
 *
 * <p>每帧固定 40 字节 header 加 XID payload。CRC32C 覆盖除 CRC 字段外的全部 header 语义和 payload。
 * 扫描只截断最后一个不完整 frame；完整 frame 的 magic/version/CRC/sequence/状态转换错误均是恢复致命错误。</p>
 */
public final class FileXaRegistry implements AutoCloseable, PreparedTransactionDecisionProvider {

    /** ASCII "XA11"。 */
    private static final int MAGIC = 0x58413131;
    /** 持久格式版本。 */
    private static final short VERSION = 1;
    /** magic/version/state/reserved/payload/crc/sequence/trx/format/glen/blen。 */
    private static final int HEADER_BYTES = 40;
    /** CRC 字段在 header 内偏移。 */
    private static final int CRC_OFFSET = 12;

    /** 同进程 registry 操作 owner；不允许持有该锁调用 storage。 */
    private final ReentrantLock lock = new ReentrantLock(true);
    /** registry 文件 channel；append 成功必须 force(true)。 */
    private final FileChannel channel;
    /** 诊断路径。 */
    private final Path path;
    /** XID 最新 frame；包含 COMPLETED 以裁决 XID 是否可复用。 */
    private final Map<XaId, XaRegistryEntry> latestByXid = new HashMap<>();
    /** 尚未 COMPLETED 的 transactionId 索引；供 storage recovery 只读决议。 */
    private final Map<TransactionId, XaRegistryEntry> activeByTransaction = new HashMap<>();
    /** 下一 append sequence；只在持锁下读取和推进。 */
    private long nextSequence = 1;
    /** close 后拒绝任何 scan/append。 */
    private boolean closed;
    /** append/force 一旦失败即进入 fail-stop；后续调用不得跨越未知文件尾继续读写。 */
    private boolean failed;
    /** 首个 registry I/O 失败根因；由 lock 保护并用于后续拒绝时保留诊断链。 */
    private IOException failureCause;

    private FileXaRegistry(Path path, FileChannel channel) {
        this.path = path;
        this.channel = channel;
        scanAndRepairTail();
    }

    /**
     * 打开或创建 `${baseDir}/mysql.xa`。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证实例目录并以 NOFOLLOW 语义拒绝 symlink。</li>
     *     <li>打开单一读写 channel，文件不存在时创建。</li>
     *     <li>扫描全部完整 frame，重建 XID 与 transaction 双索引。</li>
     *     <li>只对不完整尾部执行 truncate+force；其它损坏关闭 channel 后 fail-closed。</li>
     * </ol>
     *
     * @param baseDir 已由 DatabaseEngine 或离线 maintenance 取得 instance lock 的实例根目录
     * @return 已完成恢复扫描的 registry
     * @throws DatabaseFatalException 文件身份、格式、CRC 或状态链损坏时抛出
     */
    public static FileXaRegistry openOrCreate(Path baseDir) {
        if (baseDir == null) {
            throw new DatabaseValidationException("XA registry base directory must not be null");
        }
        Path normalized = baseDir.toAbsolutePath().normalize();
        Path path = normalized.resolve("mysql.xa");
        FileChannel channel = null;
        try {
            Files.createDirectories(normalized);
            if (Files.isSymbolicLink(path)) {
                throw new DatabaseFatalException("XA registry must not be a symbolic link: " + path);
            }
            channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            return new FileXaRegistry(path, channel);
        } catch (IOException failure) {
            closeAfterOpenFailure(channel, failure);
            throw new DatabaseFatalException("open XA registry failed: " + path, failure);
        } catch (RuntimeException failure) {
            closeAfterOpenFailure(channel, failure);
            throw failure;
        }
    }

    /**
     * 持久发布 PREPARING。相同 XID 只有上一轮已 COMPLETED 时才可复用，transactionId 不能同时归属另一分支。
     *
     * @param xid 新 XA 分支身份
     * @param transactionId 已发生首写的 storage transaction id
     * @return fsync 后最新 entry
     */
    public XaRegistryEntry preparing(XaId xid, TransactionId transactionId) {
        return append(xid, transactionId, XaRegistryState.PREPARING);
    }

    /** @return 持久发布 PREPARED 后的最新 entry */
    public XaRegistryEntry prepared(XaId xid, TransactionId transactionId) {
        return append(xid, transactionId, XaRegistryState.PREPARED);
    }

    /** @return 持久发布不可逆 COMMIT_DECIDED 后的最新 entry */
    public XaRegistryEntry decideCommit(XaId xid, TransactionId transactionId) {
        return append(xid, transactionId, XaRegistryState.COMMIT_DECIDED);
    }

    /** @return 持久发布不可逆 ROLLBACK_DECIDED 后的最新 entry */
    public XaRegistryEntry decideRollback(XaId xid, TransactionId transactionId) {
        return append(xid, transactionId, XaRegistryState.ROLLBACK_DECIDED);
    }

    /** @return 持久发布 COMPLETED 后的最新 entry */
    public XaRegistryEntry completed(XaId xid, TransactionId transactionId) {
        return append(xid, transactionId, XaRegistryState.COMPLETED);
    }

    /**
     * 读取 XID 最新持久快照。
     *
     * @param xid 待查询分支
     * @return 未出现过时为空；返回对象不可变
     */
    public Optional<XaRegistryEntry> find(XaId xid) {
        if (xid == null) {
            throw new DatabaseValidationException("XA registry lookup xid must not be null");
        }
        lock.lock();
        try {
            requireOpen();
            return Optional.ofNullable(latestByXid.get(xid));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 返回按 sequence 排序的未完成 branch 快照，供 RECOVER、启动收尾和离线 inspect 使用。
     *
     * @return 不含 COMPLETED 的不可变列表
     */
    public List<XaRegistryEntry> pendingEntries() {
        lock.lock();
        try {
            requireOpen();
            return activeByTransaction.values().stream()
                    .sorted(Comparator.comparingLong(XaRegistryEntry::sequence))
                    .toList();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 判断 registry 是否对 storage transaction 持有权威映射。该检查用于组合外部兼容 provider，
     * 不能用 {@link #decisionFor(TransactionId)} 的 UNRESOLVED 区分“缺失”和“真实未决”。
     *
     * @param transactionId storage 恢复出的 PREPARED id
     * @return 存在未完成 registry entry 时为 true
     */
    public boolean containsTransaction(TransactionId transactionId) {
        if (transactionId == null || transactionId.isNone()) {
            throw new DatabaseValidationException("XA registry transaction lookup requires positive id");
        }
        lock.lock();
        try {
            requireOpen();
            return activeByTransaction.containsKey(transactionId);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 为 storage crash recovery 映射权威决议。PREPARING 只能回滚；PREPARED 必须等待外部裁决；
     * durable decision 原样返回，COMPLETED/缺失不应再对应恢复 participant。
     *
     * @param transactionId storage 恢复出的 PREPARED id
     * @return COMMIT、ROLLBACK 或 UNRESOLVED
     */
    @Override
    public PreparedTransactionDecision decisionFor(TransactionId transactionId) {
        if (transactionId == null || transactionId.isNone()) {
            throw new DatabaseValidationException("prepared recovery requires positive transaction id");
        }
        lock.lock();
        try {
            requireOpen();
            XaRegistryEntry entry = activeByTransaction.get(transactionId);
            if (entry == null) {
                return PreparedTransactionDecision.UNRESOLVED;
            }
            return switch (entry.state()) {
                case PREPARING, ROLLBACK_DECIDED -> PreparedTransactionDecision.ROLLBACK;
                case COMMIT_DECIDED -> PreparedTransactionDecision.COMMIT;
                case PREPARED, COMPLETED -> PreparedTransactionDecision.UNRESOLVED;
            };
        } finally {
            lock.unlock();
        }
    }

    /**
     * DatabaseEngine storage recovery 成功后收尾已被恢复消费的 PREPARING/DECIDED。
     * PREPARED 不会出现在成功启动路径，因为 decision provider 会返回 UNRESOLVED。
     *
     * @return 新写入 COMPLETED 的数量
     */
    public int completeRecoveryDecisions() {
        List<XaRegistryEntry> candidates = pendingEntries().stream()
                .filter(entry -> entry.state() == XaRegistryState.PREPARING
                        || entry.state() == XaRegistryState.COMMIT_DECIDED
                        || entry.state() == XaRegistryState.ROLLBACK_DECIDED)
                .toList();
        int completed = 0;
        for (XaRegistryEntry entry : candidates) {
            FileXaRegistry.this.completed(entry.xid(), entry.transactionId());
            completed++;
        }
        return completed;
    }

    /**
     * 关闭 registry channel。调用方应在 storage 关闭后、instance lock 释放前调用。
     */
    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            try {
                channel.close();
            } catch (IOException failure) {
                throw new XaException("close XA registry failed: " + path, failure);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 校验状态链、编码并 append+force 一个 frame。方法不调用 storage，避免 registry lock
     * 与 transaction/page/redo 锁形成反向依赖。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 XID、storage transaction 与目标状态，非法输入不进入 registry 锁。</li>
     *     <li>持 registry 短锁复核当前状态链、XID/transaction 唯一绑定与 sequence 容量。</li>
     *     <li>在当前物理尾部写完整 frame 并 force；任一 I/O 失败把 registry 永久标记 fail-stop。</li>
     *     <li>force 成功后才发布双内存索引并推进 sequence；返回值因此只表达 durable frame。</li>
     * </ol>
     *
     * @param xid 待推进的外部 XA 分支身份；不得为 {@code null}
     * @param transactionId 已与该分支绑定的正 storage transaction id
     * @param target 状态机允许的下一持久状态，或当前状态的幂等重试
     * @return 已 force 并发布的最新 entry；同状态重试返回原 entry
     * @throws DatabaseFatalException append/force 失败或持久状态链损坏时抛出；实例不得继续服务
     */
    private XaRegistryEntry append(XaId xid, TransactionId transactionId, XaRegistryState target) {
        // 1、输入错误不创建 frame、锁请求或任何持久副作用。
        if (xid == null || transactionId == null || transactionId.isNone() || target == null) {
            throw new DatabaseValidationException("XA registry append fields are invalid");
        }
        lock.lock();
        try {
            // 2、同锁内核对状态与双索引，防止并发 XID/transaction 重绑或 sequence 重用。
            requireOpen();
            XaRegistryEntry previous = latestByXid.get(xid);
            validateTransition(previous, xid, transactionId, target);
            if (previous != null && previous.transactionId().equals(transactionId)
                    && previous.state() == target) {
                return previous;
            }
            XaRegistryEntry other = activeByTransaction.get(transactionId);
            if (other != null && !other.xid().equals(xid)) {
                throw new XaException("storage transaction is already bound to another XID: " + transactionId);
            }
            long sequence = nextSequence;
            if (sequence == Long.MAX_VALUE) {
                throw new DatabaseFatalException("XA registry sequence exhausted");
            }
            // 3、frame 必须完整写入并 force 后才能成为内存真相；失败把未知尾部隔离到下次 reopen 扫描。
            ByteBuffer frame = encode(sequence, xid, transactionId, target);
            long offset = channel.size();
            writeFully(channel, frame, offset);
            channel.force(true);
            // 4、只发布 durable 证据；COMPLETED 同时移除 active transaction recovery 索引。
            XaRegistryEntry entry = new XaRegistryEntry(sequence, xid, transactionId, target);
            publish(entry);
            nextSequence++;
            return entry;
        } catch (IOException failure) {
            // write 可能只完成部分 frame，force 失败也可能具有未知持久结果；禁止在未知尾部后继续 append。
            failed = true;
            failureCause = failure;
            throw new DatabaseFatalException(
                    "append/fsync XA registry failed; registry entered fail-stop: xid="
                            + xid + ", state=" + target,
                    failure);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 扫描文件、交叉校验完整 frame，并只修复 torn tail。
     */
    private void scanAndRepairTail() {
        lock.lock();
        try {
            long size = channel.size();
            long position = 0;
            long lastValid = 0;
            long previousSequence = 0;
            while (position < size) {
                long remaining = size - position;
                if (remaining < HEADER_BYTES) {
                    truncateTail(lastValid);
                    return;
                }
                ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
                readFully(channel, header, position);
                header.flip();
                int magic = header.getInt();
                short version = header.getShort();
                int stateCode = Byte.toUnsignedInt(header.get());
                int reserved = Byte.toUnsignedInt(header.get());
                int payloadBytes = header.getInt();
                int expectedCrc = header.getInt();
                long sequence = header.getLong();
                long transactionValue = header.getLong();
                int formatId = header.getInt();
                int gtridLength = Short.toUnsignedInt(header.getShort());
                int bqualLength = Short.toUnsignedInt(header.getShort());
                if (magic != MAGIC || version != VERSION || reserved != 0
                        || payloadBytes != gtridLength + bqualLength
                        || gtridLength == 0 || gtridLength > XaId.MAX_GTRID_BYTES
                        || bqualLength > XaId.MAX_BQUAL_BYTES
                        || payloadBytes > XaId.MAX_TOTAL_BYTES
                        || sequence <= previousSequence || transactionValue <= 0) {
                    throw new DatabaseFatalException("invalid XA registry frame header at offset " + position);
                }
                long frameBytes = HEADER_BYTES + (long) payloadBytes;
                if (remaining < frameBytes) {
                    truncateTail(lastValid);
                    return;
                }
                ByteBuffer payload = ByteBuffer.allocate(payloadBytes);
                readFully(channel, payload, position + HEADER_BYTES);
                payload.flip();
                byte[] xidBytes = new byte[payloadBytes];
                payload.get(xidBytes);
                int actualCrc = crc(header.array(), xidBytes);
                if (actualCrc != expectedCrc) {
                    throw new DatabaseFatalException("XA registry CRC32C mismatch at offset " + position);
                }
                byte[] gtrid = java.util.Arrays.copyOfRange(xidBytes, 0, gtridLength);
                byte[] bqual = java.util.Arrays.copyOfRange(xidBytes, gtridLength, xidBytes.length);
                XaRegistryEntry entry = new XaRegistryEntry(sequence,
                        new XaId(formatId, gtrid, bqual), TransactionId.of(transactionValue),
                        XaRegistryState.fromCode(stateCode));
                validateTransition(latestByXid.get(entry.xid()), entry.xid(),
                        entry.transactionId(), entry.state());
                XaRegistryEntry other = activeByTransaction.get(entry.transactionId());
                if (other != null && !other.xid().equals(entry.xid())) {
                    throw new DatabaseFatalException(
                            "XA registry transaction id is bound to multiple XIDs: " + entry.transactionId());
                }
                publish(entry);
                previousSequence = sequence;
                nextSequence = sequence == Long.MAX_VALUE ? Long.MAX_VALUE : sequence + 1;
                position += frameBytes;
                lastValid = position;
            }
        } catch (IOException failure) {
            throw new DatabaseFatalException("scan XA registry failed: " + path, failure);
        } finally {
            lock.unlock();
        }
    }

    /** 把最新 entry 同步发布到双索引；COMPLETED 移除 transaction recovery 索引。 */
    private void publish(XaRegistryEntry entry) {
        latestByXid.put(entry.xid(), entry);
        if (entry.state() == XaRegistryState.COMPLETED) {
            activeByTransaction.remove(entry.transactionId());
        } else {
            activeByTransaction.put(entry.transactionId(), entry);
        }
    }

    /** 验证单 XID 状态机，防止 crash recovery 从损坏或相反决议继续。 */
    private static void validateTransition(XaRegistryEntry previous, XaId xid,
                                           TransactionId transactionId, XaRegistryState target) {
        if (previous == null) {
            if (target != XaRegistryState.PREPARING) {
                throw new DatabaseFatalException("XA registry branch must start at PREPARING: " + xid);
            }
            return;
        }
        if (previous.state() == XaRegistryState.COMPLETED) {
            if (target != XaRegistryState.PREPARING
                    || previous.transactionId().equals(transactionId)) {
                throw new XaException("completed XID reuse requires a new transaction and PREPARING: " + xid);
            }
            return;
        }
        if (!previous.transactionId().equals(transactionId)) {
            throw new DatabaseFatalException("XA registry transaction identity changed before completion: " + xid);
        }
        if (previous.state() == target) {
            return;
        }
        boolean legal = switch (previous.state()) {
            case PREPARING -> target == XaRegistryState.PREPARED
                    || target == XaRegistryState.ROLLBACK_DECIDED
                    || target == XaRegistryState.COMPLETED;
            case PREPARED -> target == XaRegistryState.COMMIT_DECIDED
                    || target == XaRegistryState.ROLLBACK_DECIDED;
            case COMMIT_DECIDED, ROLLBACK_DECIDED -> target == XaRegistryState.COMPLETED;
            case COMPLETED -> false;
        };
        if (!legal) {
            throw new DatabaseFatalException("illegal XA registry transition "
                    + previous.state() + " -> " + target + " for " + xid);
        }
    }

    /** 编码 frame 并写入 CRC 字段。 */
    private static ByteBuffer encode(long sequence, XaId xid, TransactionId transactionId,
                                     XaRegistryState state) {
        byte[] gtrid = xid.gtrid();
        byte[] bqual = xid.bqual();
        int payloadBytes = gtrid.length + bqual.length;
        ByteBuffer frame = ByteBuffer.allocate(HEADER_BYTES + payloadBytes).order(ByteOrder.BIG_ENDIAN);
        frame.putInt(MAGIC);
        frame.putShort(VERSION);
        frame.put((byte) state.code());
        frame.put((byte) 0);
        frame.putInt(payloadBytes);
        frame.putInt(0);
        frame.putLong(sequence);
        frame.putLong(transactionId.value());
        frame.putInt(xid.formatId());
        frame.putShort((short) gtrid.length);
        frame.putShort((short) bqual.length);
        frame.put(gtrid);
        frame.put(bqual);
        int crc = crc(java.util.Arrays.copyOf(frame.array(), HEADER_BYTES),
                java.util.Arrays.copyOfRange(frame.array(), HEADER_BYTES, frame.capacity()));
        frame.putInt(CRC_OFFSET, crc);
        frame.flip();
        return frame;
    }

    /** CRC 计算把 header 中 CRC 四字节视为零。 */
    private static int crc(byte[] header, byte[] payload) {
        ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN).putInt(CRC_OFFSET, 0);
        CRC32C crc = new CRC32C();
        crc.update(header, 0, header.length);
        crc.update(payload, 0, payload.length);
        return (int) crc.getValue();
    }

    /** torn tail 只有在 instance lock 下才允许物理收紧。 */
    private void truncateTail(long lastValid) throws IOException {
        channel.truncate(lastValid);
        channel.force(true);
    }

    private void requireOpen() {
        if (closed) {
            throw new XaException("XA registry is closed: " + path);
        }
        if (failed) {
            throw new DatabaseFatalException(
                    "XA registry is fail-stopped after an earlier I/O failure: " + path,
                    failureCause);
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer source, long offset)
            throws IOException {
        long position = offset;
        while (source.hasRemaining()) {
            int written = channel.write(source, position);
            if (written <= 0) {
                throw new IOException("XA registry write made no progress");
            }
            position += written;
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer target, long offset)
            throws IOException {
        long position = offset;
        while (target.hasRemaining()) {
            int read = channel.read(target, position);
            if (read < 0) {
                throw new IOException("unexpected EOF in XA registry frame");
            }
            if (read == 0) {
                throw new IOException("XA registry read made no progress");
            }
            position += read;
        }
    }

    private static void closeAfterOpenFailure(FileChannel channel, Throwable primary) {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException closeFailure) {
            primary.addSuppressed(closeFailure);
        }
    }
}
