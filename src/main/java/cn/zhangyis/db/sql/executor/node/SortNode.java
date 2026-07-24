package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.sql.executor.ResultColumn;
import cn.zhangyis.db.sql.executor.exception.SqlExecutionException;
import cn.zhangyis.db.sql.executor.row.MaterializedSqlRowView;
import cn.zhangyis.db.sql.executor.row.SqlRowView;
import cn.zhangyis.db.sql.executor.row.SqlValueOrder;
import cn.zhangyis.db.sql.executor.runtime.ExecutionContext;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalLimit;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSortKey;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSortStrategy;
import cn.zhangyis.db.sql.type.SqlValue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.zip.CRC32C;

/**
 * 投影前的稳定阻塞排序算子。Top-N 用最大堆仅保留 offset+count 个最佳候选；
 * 普通排序把固定行数分堆为临时 run，再以最小堆归并，避免把全量结果同时驻留 Java heap。
 *
 * <p>每行在 child 下一次 advance 前完成 LOB hydrate 和值复制。临时文件只包含 SQL 值与稳定输入序号，
 * 不写 record/page 引用；close 按 reader→file→statement-dir 顺序释放。</p>
 */
public final class SortNode extends AbstractPlanNode {
    /** run header magic "MSRT"。 */
    private static final int RUN_MAGIC = 0x4D535254;
    /** 当前临时 run 格式版本。 */
    private static final int RUN_VERSION = 1;
    /** run trailer magic "SEND"。 */
    private static final int RUN_TRAILER_MAGIC = 0x53454E44;
    /** SHA-256 schema fingerprint 固定长度。 */
    private static final int SCHEMA_FINGERPRINT_BYTES = 32;
    /** 单行 frame 的绝对防损坏上限；实际还受 statement memory budget 约束。 */
    private static final int MAX_ROW_FRAME_BYTES = 128 * 1024 * 1024;
    /** 已完成最终 WHERE truth 的完整 table-row child。 */
    private final PlanNode input;
    /** 按用户优先级排列的 table ordinal 和方向。 */
    private final List<PhysicalSortKey> keys;
    /** 只允许 TOP_N_HEAP 或 PARTITIONED_HEAP_MERGE。 */
    private final PhysicalSortStrategy strategy;
    /** Top-N 容量来源；分堆排序可以为空。 */
    private final Optional<PhysicalLimit> limit;
    /** 临时根和单 run 内存行数上界。 */
    private final SortExecutionConfig config;
    /** 与 child 等宽的 exact 类型，用于 typed/collation compare 和 run 恢复。 */
    private final List<ResultColumn> columns;
    /** 包含输入序号 tie-breaker 的稳定比较器。 */
    private final Comparator<SortRow> comparator;
    /** exact 输出 schema 的稳定 SHA-256 指纹，防止 run 被错误 reader 解释。 */
    private final byte[] schemaFingerprint;
    /** Top-N 完成后的有序行；分堆模式保持为空。 */
    private List<SortRow> readyRows = List.of();
    /** readyRows 下一发布位置。 */
    private int readyPosition;
    /** 本节点独占的临时目录；未形成 run 时为空。 */
    private Path temporaryDirectory;
    /** 只记录本节点创建的确定文件，清理不做递归或 glob。 */
    private final List<Path> runFiles = new ArrayList<>();
    /** 已打开的 run reader，由 close 逆序释放。 */
    private final List<RunReader> readers = new ArrayList<>();
    /** 分堆模式当前每个 run 头部的最小堆。 */
    private PriorityQueue<RunHead> mergeHeap;
    /** child 是否已打开且尚未成功关闭。 */
    private boolean inputOpen;
    /** Top-N 实际宽度超预算后已在输出前转为外部归并。 */
    private boolean spilled;
    /** 当前仍存在的 run 文件总字节数，包含多轮归并的峰值。 */
    private long temporaryBytes;
    /** statement 内 run 文件的单调序号，避免多轮归并名称复用。 */
    private long runOrdinal;
    /** 当前语句绝对 deadline；只在 open 成功/失败清理期间由 statement 线程访问。 */
    private cn.zhangyis.db.sql.executor.storage.SqlStatementDeadline deadline;

    /**
     * 创建一个未打开的排序节点。
     *
     * @param input 已完成 residual 的完整行输入
     * @param keys 非空 table-column 排序键
     * @param strategy Top-N 或分堆归并，INDEX/NONE 不应创建本节点
     * @param limit Top-N 必须提供的 offset/count
     * @param config 每个 run 的资源上界与受控临时根
     * @throws DatabaseValidationException 参数、键 ordinal 或策略组合无效时抛出
     */
    public SortNode(
            PlanNode input, List<PhysicalSortKey> keys,
            PhysicalSortStrategy strategy, Optional<PhysicalLimit> limit,
            SortExecutionConfig config) {
        if (input == null || keys == null || keys.isEmpty()
                || strategy == null || limit == null || config == null
                || (strategy != PhysicalSortStrategy.TOP_N_HEAP
                && strategy != PhysicalSortStrategy.PARTITIONED_HEAP_MERGE)
                || (strategy == PhysicalSortStrategy.TOP_N_HEAP
                && limit.isEmpty())) {
            throw new DatabaseValidationException(
                    "sort node input/keys/strategy/limit/config are invalid");
        }
        this.columns = List.copyOf(input.columns());
        for (PhysicalSortKey key : keys) {
            if (key == null || key.columnOrdinal() >= columns.size()) {
                throw new DatabaseValidationException(
                        "sort key ordinal is outside child schema");
            }
        }
        this.input = input;
        this.keys = List.copyOf(keys);
        this.strategy = strategy;
        this.limit = limit;
        this.config = config;
        this.comparator = this::compareRows;
        this.schemaFingerprint = schemaFingerprint(columns);
    }

    /**
     * 打开 child 并完整形成可拉取的排序结果。
     *
     * <ol>
     *     <li>打开 child；任一行在再次 advance 前复制为不含 storage 引用的 MaterializedSqlRowView。</li>
     *     <li>Top-N 以反向比较器维护最大堆，只保留 offset+count 个最佳候选。</li>
     *     <li>分堆模式按 rowsPerRun 排序并写入独占临时文件，随后关闭 child，慢 IO 不持有 cursor 行。</li>
     *     <li>为每个 run 读取一个头部进入最小堆；失败由模板调用 closeNode 删除全部已创建文件。</li>
     * </ol>
     *
     * @param context 当前语句事务能力与绝对 deadline
     * @throws SqlExecutionException child、临时 IO、值编码或容量计算失败时抛出
     */
    @Override
    protected void openNode(ExecutionContext context) {
        // 1、先打开 child，再按 cursor view 生命周期逐行完成稳定复制。
        deadline = context.deadline();
        checkDeadline("opening SQL sort");
        input.open(context);
        inputOpen = true;
        if (strategy == PhysicalSortStrategy.TOP_N_HEAP) {
            // 2、最大堆堆顶始终是当前保留集合中最差的一行。
            buildTopN();
        } else {
            // 3、每个 run 的内存大小固定，不受最终匹配行数影响。
            buildRuns();
        }
        closeInput();
        if (strategy == PhysicalSortStrategy.PARTITIONED_HEAP_MERGE
                || spilled) {
            // 4、先把 run 收敛到 fan-in 上限，再只为每个最终 run 保存一个头部。
            reduceRunsToFanIn();
            openMergeReaders();
        }
    }

    /**
     * 从已完成的 Top-N 数组或 run 最小堆返回下一行。
     *
     * @return 排序后的独立行；全部消费时返回 Java null
     */
    @Override
    protected SqlRowView advanceNode() {
        checkDeadline("reading SQL sort result");
        if (strategy == PhysicalSortStrategy.TOP_N_HEAP
                && !spilled) {
            if (readyPosition >= readyRows.size()) {
                return null;
            }
            return readyRows.get(readyPosition++).row();
        }
        if (mergeHeap == null || mergeHeap.isEmpty()) {
            return null;
        }
        RunHead head = mergeHeap.remove();
        SortRow current = head.row();
        SortRow next = head.reader().next();
        if (next != null) {
            mergeHeap.add(new RunHead(head.reader(), next));
        }
        return current.row();
    }

    /**
     * 逆序关闭 child/readers，并只删除本节点显式记录的文件和独占目录。
     */
    @Override
    protected void closeNode() {
        RuntimeException failure = null;
        try {
            closeInput();
        } catch (RuntimeException closeFailure) {
            failure = closeFailure;
        }
        for (int index = readers.size() - 1; index >= 0; index--) {
            try {
                readers.get(index).close();
            } catch (RuntimeException closeFailure) {
                failure = append(failure, closeFailure);
            }
        }
        readers.clear();
        for (int index = runFiles.size() - 1; index >= 0; index--) {
            try {
                deleteOwnedRun(runFiles.get(index));
            } catch (RuntimeException deleteFailure) {
                failure = append(failure, deleteFailure);
            }
        }
        runFiles.clear();
        if (temporaryDirectory != null) {
            SortExecutionConfig.TEMP_ROOT_LOCK.lock();
            try {
                requireOwnedDirectory(temporaryDirectory);
                Files.deleteIfExists(temporaryDirectory);
            } catch (IOException deleteFailure) {
                failure = append(failure, new SqlExecutionException(
                        "delete SQL sort statement directory failed: "
                                + temporaryDirectory, deleteFailure));
            } catch (RuntimeException deleteFailure) {
                failure = append(failure, deleteFailure);
            } finally {
                SortExecutionConfig.TEMP_ROOT_LOCK.unlock();
            }
            temporaryDirectory = null;
        }
        readyRows = List.of();
        mergeHeap = null;
        deadline = null;
        spilled = false;
        temporaryBytes = 0L;
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public List<ResultColumn> columns() {
        return columns;
    }

    /**
     * 拉取全部输入但只保留最终 Limit 可能观察到的最佳候选。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>从绑定 limit 计算 {@code offset+count}，在创建堆前拒绝 Java 容器无法表示的容量。</li>
     *     <li>逐行复制 child view，以反向比较器让堆顶保持当前最差候选；被淘汰行之后不可能重新进入 Top-N。</li>
     *     <li>若实际 retained bytes 超预算，在发布任何结果前把当前堆与尚未消费输入交给外部排序；
     *     否则按统一比较器冻结最终有序列表。</li>
     * </ol>
     *
     * @throws SqlExecutionException limit 容量不可表示、行超过内存上限、deadline 到期或降级写 run 失败时抛出
     */
    private void buildTopN() {
        // 1、先证明 retained row 数能由 Java heap 容器精确表示，避免截断 offset+count。
        long retained = limit.orElseThrow().retainedRows();
        if (retained > Integer.MAX_VALUE) {
            throw new SqlExecutionException(
                    "Top-N retained row count exceeds Java heap index range");
        }
        int capacity = Math.toIntExact(retained);
        if (capacity == 0) {
            readyRows = List.of();
            return;
        }
        PriorityQueue<SortRow> heap =
                new PriorityQueue<>(capacity, comparator.reversed());
        long sequence = 0L;
        long retainedBytes = 0L;
        // 2、child row view 只在本次 advance 有效；入堆前复制全部值和稳定序号。
        while (input.advance()) {
            checkDeadline("building SQL Top-N heap");
            SortRow candidate = snapshot(input.current(), sequence++);
            requireRowFitsMemory(candidate);
            if (heap.size() < capacity) {
                heap.add(candidate);
                retainedBytes = addExact(
                        retainedBytes, candidate.retainedBytes(),
                        "Top-N retained bytes");
            } else if (comparator.compare(candidate, heap.element()) < 0) {
                SortRow removed = heap.remove();
                retainedBytes -= removed.retainedBytes();
                heap.add(candidate);
                retainedBytes = addExact(
                        retainedBytes, candidate.retainedBytes(),
                        "Top-N retained bytes");
            }
            if (retainedBytes > config.memoryBudgetBytes()) {
                // 3、已被堆淘汰的行不可能在未来重新进入最终 Top-N；保留当前候选集并消费剩余输入即可。
                spilled = true;
                buildRunsFrom(
                        new ArrayList<>(heap), sequence);
                readyRows = List.of();
                return;
            }
        }
        // 3、未 spill 时只对至多 offset+count 个候选做最终正向排序。
        ArrayList<SortRow> sorted = new ArrayList<>(heap);
        sorted.sort(comparator);
        readyRows = List.copyOf(sorted);
    }

    /**
     * 把输入划为固定行数的有序 run 并持久到独占临时目录。
     */
    private void buildRuns() {
        buildRunsFrom(List.of(), 0L);
    }

    /**
     * 把 seed 与尚未消费的 child 输入按字节/行双预算分区；seed 用于 Top-N 运行期降级。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先接管 Top-N 降级时仍可能进入结果的 seed，并按双预算切出完整 run。</li>
     *     <li>继续拉取尚未消费的 child，每行在 view 失效前复制，达到行数或字节边界即写 run。</li>
     *     <li>输入耗尽后写出尾部分区；任一 run 失败由 SortNode close 路径清理此前已登记文件。</li>
     * </ol>
     *
     * @param seed 已物化且仍可能进入最终结果的候选；普通外部排序传空列表
     * @param nextSequence 尚未读取 child 行应使用的下一个稳定序号
     * @throws SqlExecutionException 行或预算非法、deadline 到期、编码或临时 IO 失败时抛出
     */
    private void buildRunsFrom(
            List<SortRow> seed, long nextSequence) {
        ArrayList<SortRow> run = new ArrayList<>(config.rowsPerRun());
        long retainedBytes = 0L;
        // 1、先写入 Top-N 已保留集合；已淘汰行有单调支配证明，无需重扫。
        for (SortRow row : seed) {
            requireRowFitsMemory(row);
            if (!run.isEmpty()
                    && wouldOverflowRun(
                    retainedBytes, row.retainedBytes(), run.size())) {
                writeRun(run);
                run.clear();
                retainedBytes = 0L;
            }
            run.add(row);
            retainedBytes = addExact(
                    retainedBytes, row.retainedBytes(),
                    "sort run retained bytes");
        }
        long sequence = nextSequence;
        // 2、每次 advance 后立即复制，不让临时 IO 保存 storage-owned row view。
        while (input.advance()) {
            checkDeadline("partitioning SQL sort input");
            SortRow row = snapshot(input.current(), sequence++);
            requireRowFitsMemory(row);
            if (!run.isEmpty()
                    && wouldOverflowRun(
                    retainedBytes, row.retainedBytes(), run.size())) {
                writeRun(run);
                run.clear();
                retainedBytes = 0L;
            }
            run.add(row);
            retainedBytes = addExact(
                    retainedBytes, row.retainedBytes(),
                    "sort run retained bytes");
        }
        // 3、尾分区即使未达到预算也必须形成带完整 trailer 的可验证 run。
        if (!run.isEmpty()) {
            writeRun(run);
        }
    }

    /**
     * 判断下一行是否会越过单 run 的行数或 retained-byte 上界。
     *
     * @param retainedBytes 当前分区已占用的估算字节数
     * @param nextBytes 下一行已验证为正且不超过单语句内存预算的估算字节数
     * @param rowCount 当前分区行数
     * @return 加入下一行前是否必须先封闭当前非空分区
     */
    private boolean wouldOverflowRun(
            long retainedBytes,
            long nextBytes,
            int rowCount) {
        return rowCount >= config.rowsPerRun()
                || retainedBytes > config.memoryBudgetBytes()
                - nextBytes;
    }

    /**
     * 用最小堆排空当前分区并写出单个有序 run。
     *
     * <p>输入行先进入以完整排序键和稳定序号比较的最小堆，随后逐个弹出写盘，因此这里实现的是
     * “分区建堆 + 多路堆归并”，而不是借用 JVM 全量数组排序。原列表会在堆接管行引用后立即清空，
     * 避免同一批行被两个容器长期持有；header、逐帧 CRC 和 trailer 全部成功 close 后文件才进入
     * {@link #runFiles}，异常路径只删除当前半成品。</p>
     *
     * @param rows 已按行数和估算字节双预算形成的非空分区；调用后列表内容被消费
     * @throws SqlExecutionException 临时空间超额、deadline 到期或 run 编码/写盘失败时抛出；
     *                               失败不会把半成品登记为可归并 run
     */
    private void writeRun(List<SortRow> rows) {
        int rowCount = rows.size();
        PriorityQueue<SortRow> heap =
                new PriorityQueue<>(Math.max(1, rowCount), comparator);
        heap.addAll(rows);
        rows.clear();
        writeOrderedRun(rowCount, heap::poll);
    }

    /**
     * 把已经有序的行写成完整 run，并在任何异常时删除半成品、归还临时字节预算。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在受控 statement 目录中以 CREATE_NEW 创建唯一 run，并先预留 header 字节预算。</li>
     *     <li>逐行编码 typed payload，预留 frame 字节后写长度、payload 和 CRC32C。</li>
     *     <li>核对 supplier 行数并写 trailer；仅在流成功关闭后登记 run，失败删除半成品并归还预算。</li>
     * </ol>
     *
     * @param expectedRowCount supplier 必须恰好产生的非负行数
     * @param rows 已按统一比较器有序、以 Java null 表示耗尽的单次 supplier
     * @return 已完整关闭并登记、位于当前 statement 目录的 run 路径
     * @throws SqlExecutionException 路径、预算、deadline、编码或文件 IO 失败时抛出
     */
    private Path writeOrderedRun(
            long expectedRowCount,
            SortRowSupplier rows) {
        Path run = null;
        long reservedBytes = 0L;
        try {
            // 1、创建动作与启动遗留清理共用根锁；CREATE_NEW 防止名称意外复用。
            ensureTemporaryDirectory();
            run = temporaryDirectory.resolve(
                    "run-" + runOrdinal++ + ".bin")
                    .toAbsolutePath().normalize();
            requireOwnedPath(run);
            try (DataOutputStream output = new DataOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(
                            run, StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.WRITE)))) {
                reservedBytes = reserveTemporaryBytes(
                        Integer.BYTES * 2L
                                + SCHEMA_FINGERPRINT_BYTES
                                + Long.BYTES);
                output.writeInt(RUN_MAGIC);
                output.writeInt(RUN_VERSION);
                output.write(schemaFingerprint);
                output.writeLong(expectedRowCount);
                long actualRowCount = 0L;
                SortRow row;
                // 2、每个 frame 独立计费和校验，预算失败发生在对应字节写盘之前。
                while ((row = rows.next()) != null) {
                    checkDeadline("encoding SQL sort run");
                    byte[] payload = encodeRow(row);
                    if (payload.length > MAX_ROW_FRAME_BYTES
                            || payload.length
                            > config.memoryBudgetBytes()) {
                        throw new SqlExecutionException(
                                "SQL sort row frame exceeds memory bound");
                    }
                    long frameBytes = Integer.BYTES * 2L
                            + payload.length;
                    reservedBytes = addExact(
                            reservedBytes,
                            reserveTemporaryBytes(frameBytes),
                            "sort run reserved bytes");
                    output.writeInt(payload.length);
                    output.write(payload);
                    output.writeInt(crc32c(payload));
                    actualRowCount++;
                }
                if (actualRowCount != expectedRowCount) {
                    throw new SqlExecutionException(
                            "SQL sort run supplier row count changed");
                }
                // 3、trailer 固定记录实际行数，reader 在发布第一行前会全文件预校验。
                reservedBytes = addExact(
                        reservedBytes,
                        reserveTemporaryBytes(
                                Integer.BYTES + Long.BYTES),
                        "sort run trailer bytes");
                output.writeInt(RUN_TRAILER_MAGIC);
                output.writeLong(actualRowCount);
            }
            // 3、只有 close 成功的 run 才进入 owner 列表，异常清理不会误认半成品。
            runFiles.add(run);
            return run;
        } catch (RuntimeException failure) {
            releaseFailedRun(run, reservedBytes, failure);
            throw failure;
        } catch (IOException failure) {
            SqlExecutionException wrapped = new SqlExecutionException(
                    "write SQL sort run failed", failure);
            releaseFailedRun(run, reservedBytes, wrapped);
            throw wrapped;
        }
    }

    /**
     * 在实例受控根下创建本节点独占目录；根和目录创建与启动清理互斥。
     *
     * @throws IOException 创建或解析目录失败时抛出，由写 run 路径保留 cause 后包装
     * @throws SqlExecutionException 临时根经真实路径解析发生逃逸时抛出
     */
    private void ensureTemporaryDirectory() throws IOException {
        if (temporaryDirectory != null) {
            return;
        }
        SortExecutionConfig.TEMP_ROOT_LOCK.lock();
        try {
            config.ensureControlledRoot();
            temporaryDirectory = Files.createTempDirectory(
                    config.temporaryRoot(), "statement-")
                    .toAbsolutePath().normalize();
            if (!temporaryDirectory.getParent()
                    .equals(config.temporaryRoot())) {
                throw new SqlExecutionException(
                        "SQL sort statement directory escaped configured root");
            }
        } finally {
            SortExecutionConfig.TEMP_ROOT_LOCK.unlock();
        }
    }

    /**
     * 当初始 run 超过 fan-in 时执行多轮归并；每个输出完整校验关闭后才删除对应输入。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>冻结当前层 run 列表，并按 fan-in 切分互不重叠的归并组。</li>
     *     <li>每组流式形成一个完整新 run；新 run 成功关闭后才删除该组输入并归还临时预算。</li>
     *     <li>重复到最终 run 数不超过 fan-in，再原子替换本节点拥有的最终列表。</li>
     * </ol>
     *
     * @throws SqlExecutionException deadline 到期、run 损坏、预算不足或归并 IO 失败时抛出
     */
    private void reduceRunsToFanIn() {
        // 1、当前层只读；输出使用新的单调 run ordinal，不覆盖输入。
        List<Path> current = List.copyOf(runFiles);
        while (current.size() > config.mergeFanIn()) {
            checkDeadline("merging SQL sort run level");
            ArrayList<Path> next = new ArrayList<>(
                    (current.size() + config.mergeFanIn() - 1)
                            / config.mergeFanIn());
            for (int start = 0; start < current.size();
                 start += config.mergeFanIn()) {
                int end = Math.min(
                        current.size(),
                        start + config.mergeFanIn());
                List<Path> group = current.subList(start, end);
                if (group.size() == 1) {
                    next.add(group.getFirst());
                    continue;
                }
                // 2、输出完整且 reader 全部关闭后，才允许删除本组输入。
                Path merged = mergeRunGroup(group);
                next.add(merged);
                for (Path consumed : group) {
                    deleteOwnedRun(consumed);
                }
            }
            current = List.copyOf(next);
        }
        // 3、最终 reader 只需同时打开不超过 fan-in 个文件。
        runFiles.clear();
        runFiles.addAll(current);
    }

    /**
     * 用最小堆归并一组不超过 fan-in 的 run，输出行仍保留原 statement sequence。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>逐个打开并全文件预校验输入 run，把各自首行加入最小堆并汇总预期行数。</li>
     *     <li>每次弹出全局最小行后只从同一 reader 补一个头部，直接流式写入新 run。</li>
     *     <li>无论成功失败都逆序关闭所有输入 reader；失败不删除输入，交上层统一清理。</li>
     * </ol>
     *
     * @param group 同一归并层中非空且不超过 fan-in 的受控 run 路径
     * @return 完整关闭并登记的新有序 run
     * @throws SqlExecutionException 输入损坏、deadline、预算或 IO 失败时抛出
     */
    private Path mergeRunGroup(List<Path> group) {
        ArrayList<RunReader> inputs =
                new ArrayList<>(group.size());
        PriorityQueue<RunHead> heap = new PriorityQueue<>(
                (left, right) -> comparator.compare(
                        left.row(), right.row()));
        RuntimeException failure = null;
        try {
            // 1、RunReader 构造在发布首行前验证 magic/version/schema/frame/trailer/count。
            long rowCount = 0L;
            for (Path path : group) {
                RunReader reader = new RunReader(
                        path, columns, schemaFingerprint);
                inputs.add(reader);
                rowCount = addExact(
                        rowCount, reader.rowCount(),
                        "merged SQL sort row count");
                SortRow first = reader.next();
                if (first != null) {
                    heap.add(new RunHead(reader, first));
                }
            }
            // 2、heap 中始终最多保留每个输入 run 的一个头部。
            return writeOrderedRun(rowCount, () -> {
                if (heap.isEmpty()) {
                    return null;
                }
                checkDeadline("merging SQL sort run");
                RunHead head = heap.remove();
                SortRow result = head.row();
                SortRow next = head.reader().next();
                if (next != null) {
                    heap.add(new RunHead(
                            head.reader(), next));
                }
                return result;
            });
        } catch (RuntimeException mergeFailure) {
            failure = mergeFailure;
            throw mergeFailure;
        } finally {
            // 3、reader close 失败附加到主异常；无主异常时作为本次归并失败向上传播。
            for (int index = inputs.size() - 1;
                 index >= 0; index--) {
                try {
                    inputs.get(index).close();
                } catch (RuntimeException closeFailure) {
                    if (failure != null) {
                        failure.addSuppressed(closeFailure);
                    } else {
                        throw closeFailure;
                    }
                }
            }
        }
    }

    /**
     * 打开全部最终 run，并把各自第一行放入归并堆。
     *
     * @throws SqlExecutionException 任一最终 run 损坏或打开失败时抛出；已打开 reader 由节点 close 逆序释放
     */
    private void openMergeReaders() {
        mergeHeap = new PriorityQueue<>(
                (left, right) -> comparator.compare(left.row(), right.row()));
        for (Path run : runFiles) {
            RunReader reader = new RunReader(
                    run, columns, schemaFingerprint);
            readers.add(reader);
            SortRow first = reader.next();
            if (first != null) {
                mergeHeap.add(new RunHead(reader, first));
            }
        }
    }

    /**
     * 在 child view 失效前复制全部列和稳定输入序号。
     *
     * @param source child 当前有效且与节点 schema 等宽的行视图
     * @param sequence 当前 statement 内唯一、单调递增的稳定次序
     * @return 不含 storage 引用并携带 retained-byte 估算的排序行
     * @throws SqlExecutionException child 宽度在执行期改变或字段读取失败时抛出
     */
    private SortRow snapshot(SqlRowView source, long sequence) {
        if (source.width() != columns.size()) {
            throw new SqlExecutionException(
                    "sort child row width changed during execution");
        }
        ArrayList<SqlValue> values = new ArrayList<>(source.width());
        for (int ordinal = 0; ordinal < source.width(); ordinal++) {
            values.add(source.valueAt(ordinal));
        }
        MaterializedSqlRowView materialized =
                new MaterializedSqlRowView(values, columns);
        return new SortRow(
                materialized, sequence,
                estimateRetainedBytes(materialized));
    }

    /**
     * 按 SQL NULL/方向/稳定序号比较两行。
     */
    private int compareRows(SortRow left, SortRow right) {
        for (PhysicalSortKey key : keys) {
            int ordinal = key.columnOrdinal();
            boolean leftNull = left.row().isNullAt(ordinal);
            boolean rightNull = right.row().isNullAt(ordinal);
            int compared;
            if (leftNull || rightNull) {
                if (leftNull && rightNull) {
                    continue;
                }
                compared = leftNull ? -1 : 1;
            } else {
                compared = SqlValueOrder.compare(
                        left.row().valueAt(ordinal),
                        right.row().valueAt(ordinal),
                        columns.get(ordinal).type());
            }
            if (compared != 0) {
                return key.direction() == IndexOrder.ASC
                        ? compared : -compared;
            }
        }
        return Long.compare(left.sequence(), right.sequence());
    }

    private void closeInput() {
        if (!inputOpen) {
            return;
        }
        inputOpen = false;
        input.close();
    }

    /** deadline 是 statement cancel/timeout 的统一检查点，不能为排序阶段重新创建相对超时。 */
    private void checkDeadline(String operation) {
        if (deadline != null) {
            deadline.remaining(operation);
        }
    }

    /** 单行不能突破整个 statement 内存配额，否则任何分堆策略都无法有界物化它。 */
    private void requireRowFitsMemory(SortRow row) {
        if (row.retainedBytes() > config.memoryBudgetBytes()) {
            throw new SqlExecutionException(
                    "single SQL sort row exceeds statement memory budget");
        }
    }

    /** 估算 executor-owned snapshot 与 typed values 的 retained bytes；用于运行期 Top-N 降级与 run 分区。 */
    private static long estimateRetainedBytes(
            MaterializedSqlRowView row) {
        long bytes = 64L + row.width() * 16L;
        for (SqlValue value : row.values()) {
            long valueBytes = switch (value) {
                case SqlValue.NullValue ignored -> 1L;
                case SqlValue.IntegerValue integer ->
                        integer.value().toByteArray().length + 24L;
                case SqlValue.FloatingValue ignored -> 16L;
                case SqlValue.DecimalValue decimal ->
                        decimal.value().unscaledValue()
                                .toByteArray().length + 32L;
                case SqlValue.StringValue string ->
                        string.value().getBytes(
                                StandardCharsets.UTF_8).length + 32L;
                case SqlValue.BytesValue binary ->
                        binary.value().length + 24L;
                case SqlValue.TemporalValue ignored -> 24L;
                case SqlValue.BitValue bit ->
                        bit.bytes().length + 24L;
                case SqlValue.EnumValue enumeration ->
                        enumeration.symbol().getBytes(
                                StandardCharsets.UTF_8).length + 24L;
                case SqlValue.SetValue set -> {
                    long symbols = 32L;
                    for (String symbol : set.symbols()) {
                        symbols = addExact(
                                symbols,
                                symbol.getBytes(StandardCharsets.UTF_8)
                                        .length + 16L,
                                "SET sort retained bytes");
                    }
                    yield symbols;
                }
            };
            bytes = addExact(
                    bytes, valueBytes,
                    "SQL sort retained row bytes");
        }
        return bytes;
    }

    /** 以溢出检测维护资源计数，JDK arithmetic 异常不能泄漏为生产领域错误。 */
    private static long addExact(
            long left, long right, String field) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new SqlExecutionException(
                    field + " overflowed", overflow);
        }
    }

    /** 在写盘前预留临时文件字节；超预算不写当前 frame。 */
    private long reserveTemporaryBytes(long bytes) {
        long next = addExact(
                temporaryBytes, bytes,
                "SQL sort temporary bytes");
        if (next > config.temporaryFileBudgetBytes()) {
            throw new SqlExecutionException(
                    "SQL sort temporary file budget exceeded");
        }
        temporaryBytes = next;
        return bytes;
    }

    /** 半成品 run 失败时只删除已验证属于当前 statement 的 exact 文件，并归还本次预留。 */
    private void releaseFailedRun(
            Path run, long reservedBytes,
            RuntimeException failure) {
        temporaryBytes = Math.max(
                0L, temporaryBytes - reservedBytes);
        if (run == null) {
            return;
        }
        try {
            requireOwnedPath(run);
            Files.deleteIfExists(run);
        } catch (IOException | RuntimeException cleanupFailure) {
            failure.addSuppressed(
                    cleanupFailure instanceof RuntimeException runtime
                            ? runtime
                            : new SqlExecutionException(
                            "delete failed SQL sort run failed: "
                                    + run, cleanupFailure));
        }
    }

    /** 删除一个已完成 run 并按删除前实际文件大小归还临时预算。 */
    private void deleteOwnedRun(Path run) {
        requireOwnedPath(run);
        try {
            long bytes = Files.exists(run)
                    ? Files.size(run) : 0L;
            Files.deleteIfExists(run);
            temporaryBytes = Math.max(
                    0L, temporaryBytes - bytes);
        } catch (IOException failure) {
            throw new SqlExecutionException(
                    "delete SQL sort run failed: " + run,
                    failure);
        }
    }

    /** statement 文件必须是独占目录的直接子项，禁止通过路径漂移扩大清理范围。 */
    private void requireOwnedPath(Path path) {
        if (temporaryDirectory == null || path == null
                || !path.toAbsolutePath().normalize().getParent()
                .equals(temporaryDirectory)) {
            throw new SqlExecutionException(
                    "SQL sort run is outside statement directory");
        }
    }

    /** statement 目录必须是配置根的直接子项且使用受控前缀。 */
    private void requireOwnedDirectory(Path directory) {
        Path normalized = directory.toAbsolutePath().normalize();
        if (!normalized.getParent().equals(config.temporaryRoot())
                || normalized.getFileName() == null
                || !normalized.getFileName().toString()
                .startsWith("statement-")) {
            throw new SqlExecutionException(
                    "SQL sort statement directory is outside configured root");
        }
    }

    /** 对 exact result schema 计算 run header 指纹；JRE 缺少 SHA-256 时执行环境不可继续排序。 */
    private static byte[] schemaFingerprint(
            List<ResultColumn> columns) {
        try {
            MessageDigest digest =
                    MessageDigest.getInstance("SHA-256");
            for (ResultColumn column : columns) {
                byte[] name = column.name().getBytes(
                        StandardCharsets.UTF_8);
                byte[] type = column.type().toString().getBytes(
                        StandardCharsets.UTF_8);
                digest.update(java.nio.ByteBuffer.allocate(
                                Integer.BYTES)
                        .putInt(name.length).array());
                digest.update(name);
                digest.update(java.nio.ByteBuffer.allocate(
                                Integer.BYTES)
                        .putInt(type.length).array());
                digest.update(type);
            }
            return digest.digest();
        } catch (NoSuchAlgorithmException unavailable) {
            throw new SqlExecutionException(
                    "SHA-256 is unavailable for SQL sort schema",
                    unavailable);
        }
    }

    /** CRC32C 覆盖每个独立行 payload，避免一个 frame 损坏污染后续长度解释。 */
    private static int crc32c(byte[] payload) {
        CRC32C crc = new CRC32C();
        crc.update(payload, 0, payload.length);
        return (int) crc.getValue();
    }

    private static RuntimeException append(
            RuntimeException primary, RuntimeException additional) {
        if (primary == null) {
            return additional;
        }
        primary.addSuppressed(additional);
        return primary;
    }

    /** 把一行编码为独立 frame payload；长度与 CRC 由外层 run grammar 负责。 */
    private static byte[] encodeRow(SortRow row) {
        try {
            ByteArrayOutputStream bytes =
                    new ByteArrayOutputStream();
            try (DataOutputStream output =
                         new DataOutputStream(bytes)) {
                writeRowPayload(output, row);
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new SqlExecutionException(
                    "encode in-memory SQL sort row failed",
                    impossible);
        }
    }

    private static void writeRowPayload(
            DataOutputStream output, SortRow row)
            throws IOException {
        output.writeLong(row.sequence());
        output.writeInt(row.row().width());
        for (SqlValue value : row.row().values()) {
            writeValue(output, value);
        }
    }

    private static void writeValue(DataOutputStream output, SqlValue value)
            throws IOException {
        switch (value) {
            case SqlValue.NullValue ignored -> output.writeByte(0);
            case SqlValue.IntegerValue integer -> {
                output.writeByte(1);
                writeBytes(output, integer.value().toByteArray());
            }
            case SqlValue.FloatingValue floating -> {
                output.writeByte(2);
                output.writeDouble(floating.value());
            }
            case SqlValue.DecimalValue decimal -> {
                output.writeByte(3);
                output.writeInt(decimal.value().scale());
                writeBytes(output, decimal.value().unscaledValue().toByteArray());
            }
            case SqlValue.StringValue string -> {
                output.writeByte(4);
                writeBytes(output, string.value().getBytes(StandardCharsets.UTF_8));
            }
            case SqlValue.BytesValue bytes -> {
                output.writeByte(5);
                writeBytes(output, bytes.value());
            }
            case SqlValue.TemporalValue temporal -> {
                output.writeByte(6);
                output.writeByte(temporal.kind().ordinal());
                output.writeLong(temporal.value());
            }
            case SqlValue.BitValue bit -> {
                output.writeByte(7);
                output.writeInt(bit.bitWidth());
                writeBytes(output, bit.bytes());
            }
            case SqlValue.EnumValue enumeration -> {
                output.writeByte(8);
                output.writeInt(enumeration.ordinal());
                writeBytes(output, enumeration.symbol().getBytes(StandardCharsets.UTF_8));
            }
            case SqlValue.SetValue set -> {
                output.writeByte(9);
                output.writeLong(set.bitmap());
                output.writeInt(set.symbols().size());
                for (String symbol : set.symbols()) {
                    writeBytes(output, symbol.getBytes(StandardCharsets.UTF_8));
                }
            }
        }
    }

    private static void writeBytes(DataOutputStream output, byte[] value)
            throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }

    private record SortRow(
            MaterializedSqlRowView row,
            long sequence,
            long retainedBytes) {
    }

    private record RunHead(RunReader reader, SortRow row) {
    }

    /** 允许初始 run 与 heap merge 共用同一个流式 writer，Java null 表示 supplier EOF。 */
    @FunctionalInterface
    private interface SortRowSupplier {
        SortRow next();
    }

    /**
     * 单个 run 的顺序 reader。构造时先完整预校验 header、全部 frame CRC/typed payload 和 trailer，
     * 因而上层即使 LIMIT 提前停止，也不会在已经发布部分结果后才发现尾部损坏。
     */
    private static final class RunReader implements AutoCloseable {
        private static final int MAX_VALUE_BYTES = 64 * 1024 * 1024;
        private final Path path;
        private final List<ResultColumn> columns;
        private final DataInputStream input;
        private final long rowCount;
        private long remaining;
        private boolean closed;

        private RunReader(
                Path path,
                List<ResultColumn> columns,
                byte[] expectedFingerprint) {
            this.path = path;
            this.columns = columns;
            validateRun(path, columns, expectedFingerprint);
            DataInputStream opened = null;
            try {
                opened = new DataInputStream(new BufferedInputStream(
                        Files.newInputStream(path, StandardOpenOption.READ)));
                this.rowCount = readHeader(
                        opened, expectedFingerprint, path);
                this.remaining = rowCount;
                this.input = opened;
            } catch (IOException | RuntimeException failure) {
                if (opened != null) {
                    try {
                        opened.close();
                    } catch (IOException closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
                if (failure instanceof SqlExecutionException execution) {
                    throw execution;
                }
                throw new SqlExecutionException(
                        "open SQL sort run failed: " + path, failure);
            }
        }

        private long rowCount() {
            return rowCount;
        }

        private SortRow next() {
            if (remaining == 0) {
                return null;
            }
            try {
                byte[] payload = readFrame(input, path);
                remaining--;
                return decodeRow(payload, columns);
            } catch (EOFException truncated) {
                throw new SqlExecutionException(
                        "SQL sort run ended before declared row count: " + path,
                        truncated);
            } catch (IOException failure) {
                throw new SqlExecutionException(
                        "read SQL sort run failed: " + path, failure);
            }
        }

        /**
         * 在 reader 发布前完成全文件校验，保证 header/schema、frame CRC、row count、trailer 和 EOF 一致。
         */
        private static void validateRun(
                Path path,
                List<ResultColumn> columns,
                byte[] expectedFingerprint) {
            try (DataInputStream validation = new DataInputStream(
                    new BufferedInputStream(Files.newInputStream(
                            path, StandardOpenOption.READ)))) {
                long rows = readHeader(
                        validation, expectedFingerprint, path);
                for (long index = 0; index < rows; index++) {
                    decodeRow(readFrame(validation, path), columns);
                }
                if (validation.readInt() != RUN_TRAILER_MAGIC
                        || validation.readLong() != rows
                        || validation.read() != -1) {
                    throw new SqlExecutionException(
                            "SQL sort run trailer/count/EOF is invalid: "
                                    + path);
                }
            } catch (IOException failure) {
                throw new SqlExecutionException(
                        "validate SQL sort run failed: " + path,
                        failure);
            }
        }

        /** 读取固定 header 并返回声明行数。 */
        private static long readHeader(
                DataInputStream input,
                byte[] expectedFingerprint,
                Path path) throws IOException {
            if (input.readInt() != RUN_MAGIC
                    || input.readInt() != RUN_VERSION) {
                throw new SqlExecutionException(
                        "SQL sort run header/version is invalid: "
                                + path);
            }
            byte[] fingerprint = input.readNBytes(
                    SCHEMA_FINGERPRINT_BYTES);
            if (fingerprint.length != SCHEMA_FINGERPRINT_BYTES
                    || !MessageDigest.isEqual(
                    fingerprint, expectedFingerprint)) {
                throw new SqlExecutionException(
                        "SQL sort run schema fingerprint mismatch: "
                                + path);
            }
            long rows = input.readLong();
            if (rows < 0) {
                throw new SqlExecutionException(
                        "SQL sort run has negative row count: "
                                + path);
            }
            return rows;
        }

        /** 读取并校验一个有界 length/payload/CRC frame。 */
        private static byte[] readFrame(
                DataInputStream input,
                Path path) throws IOException {
            int length = input.readInt();
            if (length <= 0 || length > MAX_ROW_FRAME_BYTES) {
                throw new SqlExecutionException(
                        "SQL sort run frame length is invalid: "
                                + path);
            }
            byte[] payload = input.readNBytes(length);
            if (payload.length != length) {
                throw new EOFException(
                        "SQL sort run frame ended before declared length");
            }
            int expectedCrc = input.readInt();
            if (crc32c(payload) != expectedCrc) {
                throw new SqlExecutionException(
                        "SQL sort run frame CRC mismatch: " + path);
            }
            return payload;
        }

        /** 从一个完整 frame 恢复 typed executor row，并要求 payload 精确 EOF。 */
        private static SortRow decodeRow(
                byte[] payload,
                List<ResultColumn> columns) {
            try (DataInputStream rowInput =
                         new DataInputStream(
                                 new ByteArrayInputStream(payload))) {
                long sequence = rowInput.readLong();
                int width = rowInput.readInt();
                if (width != columns.size()) {
                    throw new SqlExecutionException(
                            "SQL sort run row width does not match exact schema");
                }
                ArrayList<SqlValue> values =
                        new ArrayList<>(width);
                for (int ordinal = 0; ordinal < width;
                     ordinal++) {
                    values.add(readValue(rowInput));
                }
                if (rowInput.available() != 0) {
                    throw new SqlExecutionException(
                            "SQL sort run row payload has trailing bytes");
                }
                MaterializedSqlRowView row =
                        new MaterializedSqlRowView(values, columns);
                return new SortRow(
                        row, sequence,
                        estimateRetainedBytes(row));
            } catch (IOException failure) {
                throw new SqlExecutionException(
                        "decode SQL sort row failed", failure);
            }
        }

        private static SqlValue readValue(DataInputStream input)
                throws IOException {
            int tag = input.readUnsignedByte();
            return switch (tag) {
                case 0 -> SqlValue.NullValue.INSTANCE;
                case 1 -> new SqlValue.IntegerValue(
                        new BigInteger(readBytes(input)));
                case 2 -> new SqlValue.FloatingValue(input.readDouble());
                case 3 -> {
                    int scale = input.readInt();
                    yield new SqlValue.DecimalValue(new BigDecimal(
                            new BigInteger(readBytes(input)), scale));
                }
                case 4 -> new SqlValue.StringValue(
                        new String(readBytes(input), StandardCharsets.UTF_8));
                case 5 -> new SqlValue.BytesValue(readBytes(input));
                case 6 -> {
                    int ordinal = input.readUnsignedByte();
                    SqlValue.TemporalKind[] kinds =
                            SqlValue.TemporalKind.values();
                    if (ordinal >= kinds.length) {
                        throw new SqlExecutionException(
                                "SQL sort run has unknown temporal kind");
                    }
                    yield new SqlValue.TemporalValue(
                            kinds[ordinal], input.readLong());
                }
                case 7 -> {
                    int bitWidth = input.readInt();
                    if (bitWidth <= 0) {
                        throw new SqlExecutionException(
                                "SQL sort run has invalid BIT width");
                    }
                    yield new SqlValue.BitValue(
                            readBytes(input), bitWidth);
                }
                case 8 -> {
                    int ordinal = input.readInt();
                    yield new SqlValue.EnumValue(
                            new String(readBytes(input), StandardCharsets.UTF_8),
                            ordinal);
                }
                case 9 -> {
                    long bitmap = input.readLong();
                    int count = input.readInt();
                    if (count < 0 || count > 64) {
                        throw new SqlExecutionException(
                                "SQL sort run has invalid SET symbol count");
                    }
                    ArrayList<String> symbols = new ArrayList<>(count);
                    for (int index = 0; index < count; index++) {
                        symbols.add(new String(
                                readBytes(input), StandardCharsets.UTF_8));
                    }
                    yield new SqlValue.SetValue(symbols, bitmap);
                }
                default -> throw new SqlExecutionException(
                        "SQL sort run has unknown value tag: " + tag);
            };
        }

        private static byte[] readBytes(DataInputStream input)
                throws IOException {
            int length = input.readInt();
            if (length < 0 || length > MAX_VALUE_BYTES) {
                throw new SqlExecutionException(
                        "SQL sort run value length is outside safety bound");
            }
            byte[] result = input.readNBytes(length);
            if (result.length != length) {
                throw new EOFException(
                        "SQL sort run value ended before declared length");
            }
            return result;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                input.close();
            } catch (IOException failure) {
                throw new SqlExecutionException(
                        "close SQL sort run failed: " + path, failure);
            }
        }
    }
}
