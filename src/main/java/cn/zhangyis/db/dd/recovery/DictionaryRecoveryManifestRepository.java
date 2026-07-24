package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaState;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.repo.DictionaryCatalogArchiveCodec;
import cn.zhangyis.db.dd.repo.DictionaryControlSnapshot;
import cn.zhangyis.db.dd.repo.DictionaryDurabilityWitness;
import cn.zhangyis.db.dd.repo.DictionarySnapshot;
import cn.zhangyis.db.dd.sdi.DictionarySdiCodec;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogStore;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@code mysql.dd.manifest} 的事件仓储，同时作为 control/catalog 写前 durability witness。
 *
 * <p>每个逻辑事件独占一个底层 durable batch。事件 body 先整体盖 SHA-256，再按不超过 1024 字节切片；
 * 底层 journal 继续提供 frame CRC、batch SHA 与双 header committed-length。恢复时只有 body 摘要、
 * chunk 顺序和领域交叉校验全部通过的事件才会进入视图。</p>
 *
 * <p>{@link #eventLock} 只串行化本 manifest 的 append 和内存视图发布，不获取 MDL、page latch、
 * repository writer lock 或文件生命周期锁。control/repository 可以在各自短锁内调用 witness，但
 * manifest 从不反向回调它们，因而不会形成锁环。</p>
 */
public final class DictionaryRecoveryManifestRepository implements DictionaryDurabilityWitness, AutoCloseable {

    /** 逻辑 event body 魔数，ASCII {@code DMR1}。 */
    private static final int BODY_MAGIC = 0x444D5231;
    /** chunk key 魔数，ASCII {@code DMC1}。 */
    private static final int CHUNK_MAGIC = 0x444D4331;
    /** manifest 逻辑格式版本。 */
    private static final int FORMAT_VERSION = 1;
    /** 底层 catalog store 单 payload 上限。 */
    private static final int CHUNK_BYTES = 1024;
    /** 防止损坏 count 造成无界内存分配。 */
    private static final int MAX_RECORDS = 1_000_000;
    /** 单个 manifest event 的教学实现上限。 */
    private static final int MAX_EVENT_BYTES = 64 * 1024 * 1024;
    /** 字符串上限，足以容纳受控 tables 目录内路径。 */
    private static final int MAX_STRING_BYTES = 16 * 1024;
    /** SHA-256 固定长度。 */
    private static final int SHA256_BYTES = 32;

    /** manifest 物理 journal；生命周期由本对象拥有。 */
    private final InternalCatalogStore store;
    /** baseline records 与 DD snapshot 之间的稳定 codec。 */
    private final DictionaryCatalogArchiveCodec archiveCodec = new DictionaryCatalogArchiveCodec();
    /** ACTIVE 表聚合到 SDI payload 的确定性 codec。 */
    private final DictionarySdiCodec sdiCodec = new DictionarySdiCodec();
    /** 保护 event append、重建和 volatile view 发布的短显式锁。 */
    private final ReentrantLock eventLock = new ReentrantLock();
    /** 最近一次从全部 committed event 推导出的不可变视图。 */
    private volatile DictionaryRecoveryManifestView view;

    /**
     * 打开已由独立 magic 验证的 manifest store 并重建事件状态。
     *
     * @param store manifest 专用 durable batch store；不能与 {@code mysql.ibd} 共用实例
     * @throws DatabaseValidationException store 为空时抛出
     * @throws DictionaryRecoveryManifestException committed event 损坏、乱序或快照不自洽时抛出
     */
    public DictionaryRecoveryManifestRepository(InternalCatalogStore store) {
        if (store == null) {
            throw new DatabaseValidationException("dictionary recovery manifest store must not be null");
        }
        this.store = store;
        this.view = rebuild(store.readCommittedBatches());
    }

    /**
     * 返回最近 committed event 推导出的只读视图。
     *
     * @return 可并发读取的不可变 manifest 状态
     */
    public DictionaryRecoveryManifestView view() {
        return view;
    }

    /**
     * 在 control 新 generation 写盘前发布 reservation witness。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证目标快照及其正数 counters，防止无效高水位污染永久恢复证据。</li>
     *     <li>在 manifest 独占锁内编码完整 control 目标并原子 append 一个 event batch。</li>
     *     <li>从 committed batches 重建 safe-control 逐分量最大值，确认 event 可被重新读取。</li>
     *     <li>发布新只读视图；任一失败都发生在 control 文件修改前，由调用方中止 reservation。</li>
     * </ol>
     *
     * @param target 即将写入 control 非当前槽的完整目标；next 值必须保持正数
     * @throws DictionaryRecoveryManifestException manifest append 或回读校验失败时抛出
     */
    @Override
    public void beforeControlReservation(DictionaryControlSnapshot target) {
        // 1. witness 不能接受会让重建复用零/负 identity 的 control 状态。
        validateControl(target);
        // 2. append 在独立锁内串行，底层 committed-length 在返回前已经 durable。
        appendEvent(EventType.CONTROL_RESERVATION, encodeControlEvent(target));
        // 3、4. appendEvent 回读全部 committed event 后才发布 view；异常时调用方尚未写 control。
    }

    /**
     * 在 catalog mutation append 前发布不可撤销 intent。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证目标版本与实际 records，并对随后原样写入 catalog 的 key/payload 计算确定性摘要。</li>
     *     <li>在 manifest 独占锁内 append intent；该 durable 边界先于 catalog 真相提交点。</li>
     *     <li>回读后视图标记为 unresolved，直到后续 clean snapshot 的 resolvedThrough 覆盖本序号。</li>
     *     <li>失败时不触碰 catalog；成功后即使进程崩溃，离线 rebuild 也不会使用过时 clean snapshot。</li>
     * </ol>
     *
     * @param version 即将提交的严格递增字典版本
     * @param records 随后会原样 append 到 catalog 的已编码 mutation records
     * @throws DictionaryRecoveryManifestException manifest append 或回读校验失败时抛出
     */
    @Override
    public void beforeCatalogMutation(DictionaryVersion version, List<CatalogRecord> records) {
        // 1. 摘要绑定实际 key/payload，而不是只绑定易碰撞的目标版本。
        if (version == null || records == null || records.isEmpty()) {
            throw new DatabaseValidationException("catalog mutation witness version/records are invalid");
        }
        byte[] body = write(out -> {
            writeBodyPrefix(out, EventType.CATALOG_MUTATION_INTENT);
            out.writeLong(version.value());
            out.write(digestRecords(records));
        });
        // 2、3、4. append 成功即使未发生 catalog write，也只会造成保守阻塞，不会错误授权重建。
        appendEvent(EventType.CATALOG_MUTATION_INTENT, body);
    }

    /**
     * 在 catalog、DDL recovery、SDI 与目录均收敛后发布完整 clean snapshot。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>过滤 DROPPED tombstone，拒绝任一临时表状态，并编码确定性 baseline records。</li>
     *     <li>为每张 ACTIVE 表校验 binding 位于 tables 目录内，生成相对路径和 SDI SHA-256。</li>
     *     <li>锁内读取当前 last sequence 作为 resolvedThrough，编码 control、archive 与目录见证并 append。</li>
     *     <li>回读、解码并交叉校验新 clean event后发布视图；失败保留旧 view 和所有 intent。</li>
     * </ol>
     *
     * @param snapshot 当前权威 catalog 的稳定不可变快照
     * @param control 当前有效 control 高水位
     * @param tablesDirectory file-per-table 根目录；ACTIVE binding 必须位于该目录下
     * @return 新发布、可作为离线 rebuild 输入的 clean snapshot
     * @throws DictionaryRecoveryManifestException 临时状态、路径逃逸、编码或持久化失败时抛出
     */
    public DictionaryRecoveryCleanSnapshot publishCleanSnapshot(
            DictionarySnapshot snapshot,
            DictionaryControlSnapshot control,
            Path tablesDirectory) {
        // 1. baseline 不保存 tombstone，也绝不把未完成 DDL 状态伪装为 clean。
        validateControl(control);
        DictionarySnapshot stable = stableSnapshot(snapshot);
        validateControlCoversSnapshot(control, stable);
        List<CatalogRecord> archive = archiveCodec.encode(stable);

        // 2. 目录 witness 与 SDI 使用同一确定性 table aggregate，identity/path/digest 必须一一对应。
        List<DictionaryRecoveryPathEntry> entries = pathEntries(stable, tablesDirectory);

        eventLock.lock();
        try {
            // 3. resolvedThrough 只覆盖本 clean append 前已经 committed 的 event。
            long resolvedThrough = view.lastSequence();
            byte[] body = encodeCleanEvent(resolvedThrough, control, archive, entries);
            appendLocked(EventType.CLEAN_SNAPSHOT, body);

            // 4. 必须从底层 committed bytes 重建后再返回，不能用待写对象直接伪造发布成功。
            DictionaryRecoveryCleanSnapshot clean = view.latestClean().orElseThrow(() ->
                    new DictionaryRecoveryManifestException("clean snapshot append was not observable"));
            if (clean.resolvedThroughSequence() != resolvedThrough
                    || !clean.dictionarySnapshot().equals(stable)) {
                throw new DictionaryRecoveryManifestException("published clean snapshot differs from requested state");
            }
            return clean;
        } finally {
            eventLock.unlock();
        }
    }

    /**
     * 关闭 manifest journal。调用方必须先停止可能触发 witness 的 DDL/control 写入。
     */
    @Override
    public void close() {
        store.close();
    }

    /** 在非重入调用路径取得 event lock 并执行 append+回读。 */
    private void appendEvent(EventType type, byte[] body) {
        eventLock.lock();
        try {
            appendLocked(type, body);
        } finally {
            eventLock.unlock();
        }
    }

    /**
     * 把一个带自身摘要的逻辑 body 分片后 durable append，并从磁盘可见批次重建 view。
     * 调用方必须持有 {@link #eventLock}。
     */
    private void appendLocked(EventType type, byte[] bodyWithoutDigest) {
        byte[] digest = sha256(bodyWithoutDigest);
        byte[] body = Arrays.copyOf(bodyWithoutDigest, bodyWithoutDigest.length + digest.length);
        System.arraycopy(digest, 0, body, bodyWithoutDigest.length, digest.length);
        if (body.length > MAX_EVENT_BYTES) {
            throw new DictionaryRecoveryManifestException("manifest event exceeds size limit: " + body.length);
        }
        int total = Math.max(1, (body.length + CHUNK_BYTES - 1) / CHUNK_BYTES);
        List<CatalogRecord> chunks = new ArrayList<>(total);
        for (int ordinal = 0; ordinal < total; ordinal++) {
            int from = ordinal * CHUNK_BYTES;
            int to = Math.min(body.length, from + CHUNK_BYTES);
            chunks.add(new CatalogRecord(chunkKey(type, ordinal, total), Arrays.copyOfRange(body, from, to)));
        }
        try {
            store.append(chunks);
            view = rebuild(store.readCommittedBatches());
        } catch (RuntimeException failure) {
            throw failure instanceof DictionaryRecoveryManifestException manifestFailure
                    ? manifestFailure
                    : new DictionaryRecoveryManifestException("append dictionary recovery manifest event failed", failure);
        }
    }

    /** 从全部 committed event 重建 clean、safe-control 和 unresolved mutation 状态。 */
    private DictionaryRecoveryManifestView rebuild(List<CatalogBatch> batches) {
        DictionaryRecoveryCleanSnapshot latestClean = null;
        DictionaryControlSnapshot safeControl = null;
        long lastMutationSequence = 0;
        long lastSequence = 0;
        for (CatalogBatch batch : batches) {
            if (batch.sequence() <= lastSequence) {
                throw new DictionaryRecoveryManifestException("manifest batch sequence is not strictly increasing");
            }
            Event event = decodeEvent(batch);
            lastSequence = batch.sequence();
            switch (event.type()) {
                case CONTROL_RESERVATION -> safeControl = componentMaximum(safeControl, event.control());
                case CATALOG_MUTATION_INTENT -> lastMutationSequence = batch.sequence();
                case CLEAN_SNAPSHOT -> {
                    latestClean = event.clean();
                    safeControl = componentMaximum(safeControl, latestClean.controlSnapshot());
                }
            }
        }
        boolean unresolved = latestClean == null
                ? lastMutationSequence > 0
                : lastMutationSequence > latestClean.resolvedThroughSequence();
        return new DictionaryRecoveryManifestView(lastSequence, Optional.ofNullable(latestClean),
                Optional.ofNullable(safeControl), unresolved);
    }

    /** 解码单个底层 batch，并验证 chunk、body digest 与事件领域字段。 */
    private Event decodeEvent(CatalogBatch batch) {
        List<CatalogRecord> records = batch.records();
        ByteArrayOutputStream joined = new ByteArrayOutputStream();
        EventType expectedType = null;
        int expectedTotal = records.size();
        if (expectedTotal <= 0) {
            throw new DictionaryRecoveryManifestException("manifest event has no chunks");
        }
        for (int ordinal = 0; ordinal < records.size(); ordinal++) {
            ChunkKey key = decodeChunkKey(records.get(ordinal).key());
            if (key.ordinal() != ordinal || key.total() != expectedTotal
                    || expectedType != null && key.type() != expectedType) {
                throw new DictionaryRecoveryManifestException("manifest event chunk order/type mismatch: "
                        + batch.sequence());
            }
            expectedType = key.type();
            joined.writeBytes(records.get(ordinal).payload());
        }
        byte[] bytes = joined.toByteArray();
        if (bytes.length <= SHA256_BYTES || bytes.length > MAX_EVENT_BYTES) {
            throw new DictionaryRecoveryManifestException("manifest event body length is invalid: " + bytes.length);
        }
        byte[] content = Arrays.copyOf(bytes, bytes.length - SHA256_BYTES);
        byte[] expectedDigest = Arrays.copyOfRange(bytes, content.length, bytes.length);
        if (!MessageDigest.isEqual(expectedDigest, sha256(content))) {
            throw new DictionaryRecoveryManifestException("manifest event body SHA-256 mismatch: " + batch.sequence());
        }
        return decodeContent(batch.sequence(), expectedType, content, expectedDigest);
    }

    /** 按事件类型解码逻辑 body，并对 clean archive/path 做交叉校验。 */
    private Event decodeContent(long sequence, EventType keyType, byte[] content, byte[] digest) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(content))) {
            if (in.readInt() != BODY_MAGIC || in.readInt() != FORMAT_VERSION) {
                throw new DictionaryRecoveryManifestException("manifest event body magic/version mismatch");
            }
            EventType bodyType = EventType.fromCode(in.readUnsignedByte());
            if (bodyType != keyType) {
                throw new DictionaryRecoveryManifestException("manifest chunk/body event type mismatch");
            }
            Event event = switch (bodyType) {
                case CONTROL_RESERVATION -> Event.control(readControl(in));
                case CATALOG_MUTATION_INTENT -> {
                    long version = in.readLong();
                    byte[] mutationDigest = in.readNBytes(SHA256_BYTES);
                    if (version <= 0 || mutationDigest.length != SHA256_BYTES) {
                        throw new DictionaryRecoveryManifestException("catalog mutation intent is truncated");
                    }
                    yield Event.mutation();
                }
                case CLEAN_SNAPSHOT -> Event.clean(readClean(sequence, in, digest));
            };
            if (in.available() != 0) {
                throw new DictionaryRecoveryManifestException("manifest event has trailing bytes");
            }
            return event;
        } catch (DictionaryRecoveryManifestException failure) {
            throw failure;
        } catch (IOException | RuntimeException failure) {
            throw new DictionaryRecoveryManifestException("decode dictionary recovery manifest event failed", failure);
        }
    }

    /** 解码 clean body 并证明 archive、paths、control 三者互相一致。 */
    private DictionaryRecoveryCleanSnapshot readClean(long sequence, DataInputStream in, byte[] digest)
            throws IOException {
        long resolvedThrough = in.readLong();
        if (resolvedThrough < 0 || resolvedThrough >= sequence) {
            throw new DictionaryRecoveryManifestException("clean resolved-through sequence is invalid");
        }
        DictionaryControlSnapshot control = readControl(in);
        int archiveCount = boundedCount(in.readInt(), "archive");
        List<CatalogRecord> archive = new ArrayList<>(archiveCount);
        for (int index = 0; index < archiveCount; index++) {
            archive.add(new CatalogRecord(readBytes(in, 256, "archive key"),
                    readBytes(in, 1024, "archive payload")));
        }
        DictionarySnapshot snapshot = archiveCodec.decode(archive);
        validateControlCoversSnapshot(control, snapshot);
        int pathCount = boundedCount(in.readInt(), "path");
        List<DictionaryRecoveryPathEntry> paths = new ArrayList<>(pathCount);
        for (int index = 0; index < pathCount; index++) {
            paths.add(new DictionaryRecoveryPathEntry(in.readLong(), in.readInt(), readString(in),
                    readFixed(in, SHA256_BYTES, "SDI digest")));
        }
        validatePaths(snapshot, paths);
        return new DictionaryRecoveryCleanSnapshot(sequence, resolvedThrough, snapshot, control, paths, digest);
    }

    /** 编码 clean snapshot 的无摘要 body；摘要由统一 append 层追加。 */
    private byte[] encodeCleanEvent(long resolvedThrough, DictionaryControlSnapshot control,
                                    List<CatalogRecord> archive,
                                    List<DictionaryRecoveryPathEntry> entries) {
        return write(out -> {
            writeBodyPrefix(out, EventType.CLEAN_SNAPSHOT);
            out.writeLong(resolvedThrough);
            writeControl(out, control);
            out.writeInt(archive.size());
            for (CatalogRecord record : archive) {
                writeBytes(out, record.key());
                writeBytes(out, record.payload());
            }
            out.writeInt(entries.size());
            for (DictionaryRecoveryPathEntry entry : entries) {
                out.writeLong(entry.tableId());
                out.writeInt(entry.spaceId());
                writeString(out, entry.relativePath());
                out.write(entry.sdiDigest());
            }
        });
    }

    /** 编码 control reservation 的无摘要 body。 */
    private static byte[] encodeControlEvent(DictionaryControlSnapshot control) {
        return write(out -> {
            writeBodyPrefix(out, EventType.CONTROL_RESERVATION);
            writeControl(out, control);
        });
    }

    /**
     * 构造只包含稳定服务/隔离状态且没有 DROPPED tombstone 的 rebuild baseline。
     * RECOVERY_UNAVAILABLE/RECOVERY_DISCARDED 保留逻辑 catalog 与 binding 供对象级恢复，但不会进入下方 ACTIVE path witness。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>从 append-only catalog 投影 ACTIVE schema，剔除已删除 identity，避免同名重建被误判为 baseline 重名。</li>
     *     <li>剔除 DROPPED table，并验证其余表均为允许灾难恢复重建的稳定状态且具有物理 binding。</li>
     *     <li>由保留表重新派生全局 index map，并构造与原 published version 一致的不可变快照。</li>
     * </ol>
     *
     * @param source repository writer fence 内取得的权威不可变快照；不得为 {@code null}
     * @return 不含 schema/table tombstone、可由 archive codec 严格校验和重建的稳定快照
     * @throws DatabaseValidationException source 为空时抛出，调用方不得发布 manifest
     * @throws DictionaryRecoveryManifestException 表处于临时状态、缺失 binding 或引用已删除 schema 时抛出，
     *         调用方必须保留旧 clean manifest 并等待 DDL 恢复收敛
     */
    private static DictionarySnapshot stableSnapshot(DictionarySnapshot source) {
        // 1. schema tombstone 仅服务普通 catalog 审计；clean baseline 只保存可见 identity。
        if (source == null) {
            throw new DatabaseValidationException("dictionary snapshot must not be null");
        }
        Map<cn.zhangyis.db.dd.domain.SchemaId, SchemaDefinition> schemas = new LinkedHashMap<>();
        source.schemas().values().stream()
                .filter(schema -> schema.state() == SchemaState.ACTIVE)
                .sorted(Comparator.comparingLong(schema -> schema.id().value()))
                .forEach(schema -> schemas.put(schema.id(), schema));

        // 2. 表必须处于稳定终态；保留表不得指向已经由 schema tombstone 隐藏的父对象。
        Map<cn.zhangyis.db.dd.domain.TableId, TableDefinition> tables = new LinkedHashMap<>();
        Map<cn.zhangyis.db.dd.domain.IndexId, IndexDefinition> indexes = new LinkedHashMap<>();
        source.tables().values().stream()
                .sorted(Comparator.comparingLong(table -> table.id().value()))
                .forEach(table -> {
                    if (table.state() == TableState.DROPPED) {
                        return;
                    }
                    if (table.state() != TableState.ACTIVE
                            && table.state() != TableState.DISCARDED
                            && table.state() != TableState.RECOVERY_UNAVAILABLE
                            && table.state() != TableState.RECOVERY_DISCARDED) {
                        throw new DictionaryRecoveryManifestException(
                                "cannot publish clean snapshot with transient table state: "
                                        + table.id().value() + "/" + table.state());
                    }
                    if (table.storageBinding().isEmpty()) {
                        throw new DictionaryRecoveryManifestException(
                                "stable recovery table lacks storage binding: " + table.id().value());
                    }
                    if (!schemas.containsKey(table.schemaId())) {
                        throw new DictionaryRecoveryManifestException(
                                "stable recovery table references dropped schema: " + table.id().value());
                    }
                    tables.put(table.id(), table);
                    for (IndexDefinition index : table.indexes()) {
                        indexes.put(index.id(), index);
                    }
                });
        // 3. 全局 index map 从保留表派生，不能把已删除表的索引身份带入灾难恢复 baseline。
        return new DictionarySnapshot(source.publishedVersion(), schemas, tables, indexes);
    }

    /**
     * 证明 clean control 的所有 next counters 严格越过 archive 已使用 identity/version。
     *
     * <p>manifest 是 catalog/control 同时丢失时的唯一高水位证据，因此“等于最大已用值”也不安全；
     * 该校验在发布和解码两端执行，防止格式完整但领域不一致的 clean 授权 identity 复用。</p>
     */
    private static void validateControlCoversSnapshot(
            DictionaryControlSnapshot control,
            DictionarySnapshot snapshot) {
        long maxSchema = snapshot.schemas().keySet().stream()
                .mapToLong(id -> id.value()).max().orElse(0);
        long maxTable = snapshot.tables().keySet().stream()
                .mapToLong(id -> id.value()).max().orElse(0);
        long maxIndex = snapshot.indexes().keySet().stream()
                .mapToLong(id -> id.value()).max().orElse(0);
        long maxSpace = snapshot.tables().values().stream()
                .map(TableDefinition::storageBinding)
                .flatMap(Optional::stream)
                .mapToLong(binding -> binding.spaceId().value())
                .max().orElse(0);
        if (control.nextSchemaId() <= maxSchema
                || control.nextTableId() <= maxTable
                || control.nextIndexId() <= maxIndex
                || control.nextSpaceId() <= maxSpace
                || control.nextDictionaryVersion() <= snapshot.publishedVersion().value()) {
            throw new DictionaryRecoveryManifestException(
                    "clean manifest control high-water does not cover dictionary archive");
        }
    }

    /** 为 ACTIVE 表建立按 tableId 排序、无重复 identity/path 的目录见证。 */
    private List<DictionaryRecoveryPathEntry> pathEntries(DictionarySnapshot snapshot, Path tablesDirectory) {
        if (tablesDirectory == null) {
            throw new DatabaseValidationException("tables directory must not be null");
        }
        Path root = tablesDirectory.toAbsolutePath().normalize();
        List<DictionaryRecoveryPathEntry> entries = new ArrayList<>();
        snapshot.tables().values().stream()
                .filter(table -> table.state() == TableState.ACTIVE)
                .sorted(Comparator.comparingLong(table -> table.id().value()))
                .forEach(table -> {
                    TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                            new DictionaryRecoveryManifestException(
                                    "ACTIVE table lacks storage binding: " + table.id().value()));
                    Path path = binding.path().toAbsolutePath().normalize();
                    if (!path.startsWith(root) || path.equals(root)) {
                        throw new DictionaryRecoveryManifestException(
                                "ACTIVE table path escapes tables directory: " + path);
                    }
                    String relative = root.relativize(path).toString();
                    entries.add(new DictionaryRecoveryPathEntry(table.id().value(),
                            binding.spaceId().value(), relative, sha256(sdiCodec.encode(table))));
                });
        validatePaths(snapshot, entries);
        return List.copyOf(entries);
    }

    /** 校验 clean path entries 与 ACTIVE snapshot 一一对应且 identity/path 无重复。 */
    private static void validatePaths(DictionarySnapshot snapshot, List<DictionaryRecoveryPathEntry> paths) {
        Map<Long, DictionaryRecoveryPathEntry> byTable = new LinkedHashMap<>();
        Set<Integer> spaces = new HashSet<>();
        Set<String> relativePaths = new HashSet<>();
        for (DictionaryRecoveryPathEntry entry : paths) {
            if (byTable.putIfAbsent(entry.tableId(), entry) != null
                    || !spaces.add(entry.spaceId()) || !relativePaths.add(entry.relativePath())) {
                throw new DictionaryRecoveryManifestException("duplicate identity/path in clean manifest");
            }
        }
        long activeCount = snapshot.tables().values().stream()
                .filter(table -> table.state() == TableState.ACTIVE).count();
        if (activeCount != paths.size()) {
            throw new DictionaryRecoveryManifestException("clean manifest ACTIVE/path count mismatch");
        }
        for (TableDefinition table : snapshot.tables().values()) {
            if (table.state() != TableState.ACTIVE) {
                continue;
            }
            DictionaryRecoveryPathEntry entry = byTable.get(table.id().value());
            TableStorageBinding binding = table.storageBinding().orElseThrow();
            if (entry == null || entry.spaceId() != binding.spaceId().value()
                    || !entry.relativePath().equals(binding.path().getFileName().toString())) {
                throw new DictionaryRecoveryManifestException(
                        "clean manifest path identity mismatch: " + table.id().value());
            }
        }
    }

    /** 所有 control 见证按分量取最大，宁可跳号也不复用可能暴露的 identity。 */
    public static DictionaryControlSnapshot componentMaximum(
            DictionaryControlSnapshot left,
            DictionaryControlSnapshot right) {
        if (left == null) {
            validateControl(right);
            return right;
        }
        validateControl(left);
        validateControl(right);
        if (!left.dictionarySpaceId().equals(right.dictionarySpaceId())) {
            throw new DictionaryRecoveryManifestException("control witnesses disagree on dictionary space id");
        }
        return new DictionaryControlSnapshot(
                Math.max(left.generation(), right.generation()),
                left.dictionarySpaceId(),
                Math.max(left.nextSchemaId(), right.nextSchemaId()),
                Math.max(left.nextTableId(), right.nextTableId()),
                Math.max(left.nextIndexId(), right.nextIndexId()),
                Math.max(left.nextSpaceId(), right.nextSpaceId()),
                Math.max(left.nextDdlId(), right.nextDdlId()),
                Math.max(left.nextDictionaryVersion(), right.nextDictionaryVersion()));
    }

    /** 验证 control snapshot 可作为 identity 分配高水位。 */
    private static void validateControl(DictionaryControlSnapshot control) {
        if (control == null || control.generation() <= 0 || control.dictionarySpaceId() == null
                || control.nextSchemaId() <= 0 || control.nextTableId() <= 0 || control.nextIndexId() <= 0
                || control.nextSpaceId() <= 0 || control.nextDdlId() <= 0
                || control.nextDictionaryVersion() <= 0) {
            throw new DatabaseValidationException("dictionary recovery control snapshot is invalid");
        }
    }

    /** 写 control 的固定字段，不持久化 Java 类型名或 enum ordinal。 */
    private static void writeControl(DataOutputStream out, DictionaryControlSnapshot control) throws IOException {
        validateControl(control);
        out.writeLong(control.generation());
        out.writeInt(control.dictionarySpaceId().value());
        out.writeLong(control.nextSchemaId());
        out.writeLong(control.nextTableId());
        out.writeLong(control.nextIndexId());
        out.writeLong(control.nextSpaceId());
        out.writeLong(control.nextDdlId());
        out.writeLong(control.nextDictionaryVersion());
    }

    /** 读取并验证 control 固定字段。 */
    private static DictionaryControlSnapshot readControl(DataInputStream in) throws IOException {
        DictionaryControlSnapshot control = new DictionaryControlSnapshot(
                in.readLong(), SpaceId.of(in.readInt()), in.readLong(), in.readLong(), in.readLong(),
                in.readLong(), in.readLong(), in.readLong());
        validateControl(control);
        return control;
    }

    /** 写 event 通用 prefix。 */
    private static void writeBodyPrefix(DataOutputStream out, EventType type) throws IOException {
        out.writeInt(BODY_MAGIC);
        out.writeInt(FORMAT_VERSION);
        out.writeByte(type.code);
    }

    /** 生成稳定 chunk key：magic/type/ordinal/total。 */
    private static byte[] chunkKey(EventType type, int ordinal, int total) {
        return write(out -> {
            out.writeInt(CHUNK_MAGIC);
            out.writeByte(type.code);
            out.writeInt(ordinal);
            out.writeInt(total);
        });
    }

    /** 解码并验证稳定 chunk key。 */
    private static ChunkKey decodeChunkKey(byte[] key) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(key))) {
            if (in.readInt() != CHUNK_MAGIC) {
                throw new DictionaryRecoveryManifestException("manifest chunk key magic mismatch");
            }
            EventType type = EventType.fromCode(in.readUnsignedByte());
            int ordinal = in.readInt();
            int total = in.readInt();
            if (ordinal < 0 || total <= 0 || ordinal >= total || in.available() != 0) {
                throw new DictionaryRecoveryManifestException("manifest chunk key range is invalid");
            }
            return new ChunkKey(type, ordinal, total);
        } catch (DictionaryRecoveryManifestException failure) {
            throw failure;
        } catch (IOException | RuntimeException failure) {
            throw new DictionaryRecoveryManifestException("decode manifest chunk key failed", failure);
        }
    }

    /** 对 catalog records 的长度前缀 key/payload 序列计算摘要。 */
    private static byte[] digestRecords(List<CatalogRecord> records) {
        return sha256(write(out -> {
            out.writeInt(records.size());
            for (CatalogRecord record : records) {
                if (record == null) {
                    throw new DatabaseValidationException("catalog mutation record must not be null");
                }
                writeBytes(out, record.key());
                writeBytes(out, record.payload());
            }
        }));
    }

    /** 写长度前缀字节。 */
    private static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    /** 读取有上界的长度前缀字节。 */
    private static byte[] readBytes(DataInputStream in, int max, String field) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > max) {
            throw new DictionaryRecoveryManifestException(field + " length is invalid: " + length);
        }
        return readFixed(in, length, field);
    }

    /** 写 UTF-8 路径。 */
    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > MAX_STRING_BYTES) {
            throw new DictionaryRecoveryManifestException("manifest string length is invalid: " + bytes.length);
        }
        writeBytes(out, bytes);
    }

    /** 读取 UTF-8 路径。 */
    private static String readString(DataInputStream in) throws IOException {
        return new String(readBytes(in, MAX_STRING_BYTES, "manifest string"), StandardCharsets.UTF_8);
    }

    /** 精确读取固定长度字段，拒绝静默短读。 */
    private static byte[] readFixed(DataInputStream in, int length, String field) throws IOException {
        byte[] bytes = in.readNBytes(length);
        if (bytes.length != length) {
            throw new DictionaryRecoveryManifestException(field + " is truncated");
        }
        return bytes;
    }

    /** 验证通用 count 上界。 */
    private static int boundedCount(int count, String field) {
        if (count < 0 || count > MAX_RECORDS) {
            throw new DictionaryRecoveryManifestException(field + " count is invalid: " + count);
        }
        return count;
    }

    /** 执行内存编码并把 IOException 归类为 manifest 异常。 */
    private static byte[] write(IoWriter writer) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(bytes)) {
            writer.write(out);
            out.flush();
            return bytes.toByteArray();
        } catch (DictionaryRecoveryManifestException | DatabaseValidationException failure) {
            throw failure;
        } catch (IOException failure) {
            throw new DictionaryRecoveryManifestException("encode dictionary recovery manifest failed", failure);
        }
    }

    /** 计算 SHA-256；JVM 缺失标准算法属于无法继续解释持久格式的致命环境错误。 */
    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException failure) {
            throw new DictionaryRecoveryManifestException("SHA-256 is unavailable", failure);
        }
    }

    /** 支持抛 IOException 的内存编码动作。 */
    @FunctionalInterface
    private interface IoWriter {
        void write(DataOutputStream out) throws IOException;
    }

    /** manifest event 稳定类型码。 */
    private enum EventType {
        /** control 写前高水位见证。 */
        CONTROL_RESERVATION(1),
        /** catalog mutation 写前意图。 */
        CATALOG_MUTATION_INTENT(2),
        /** 解析此前 intent 的完整稳定快照。 */
        CLEAN_SNAPSHOT(3);

        /** 持久稳定码，不能改用 ordinal。 */
        private final int code;

        EventType(int code) {
            this.code = code;
        }

        /** 从稳定码恢复事件类型，未知码 fail-closed。 */
        private static EventType fromCode(int code) {
            for (EventType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new DictionaryRecoveryManifestException("unknown manifest event type: " + code);
        }
    }

    /** 已验证的 chunk key。 */
    private record ChunkKey(EventType type, int ordinal, int total) {
    }

    /** 解码阶段的内部事件联合类型。 */
    private record Event(
            EventType type,
            DictionaryControlSnapshot control,
            DictionaryRecoveryCleanSnapshot clean) {

        private static Event control(DictionaryControlSnapshot control) {
            return new Event(EventType.CONTROL_RESERVATION, control, null);
        }

        private static Event mutation() {
            return new Event(EventType.CATALOG_MUTATION_INTENT, null, null);
        }

        private static Event clean(DictionaryRecoveryCleanSnapshot clean) {
            return new Event(EventType.CLEAN_SNAPSHOT, null, clean);
        }
    }
}
