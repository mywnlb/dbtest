package cn.zhangyis.db.engine.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.recovery.DictionaryRecoveryCleanSnapshot;
import cn.zhangyis.db.dd.recovery.DictionaryRecoveryManifestRepository;
import cn.zhangyis.db.dd.recovery.DictionaryRecoveryManifestView;
import cn.zhangyis.db.dd.recovery.DictionaryRecoveryPathEntry;
import cn.zhangyis.db.dd.repo.DictionaryCatalogArchiveCodec;
import cn.zhangyis.db.dd.repo.DictionaryControlSnapshot;
import cn.zhangyis.db.dd.repo.DictionaryControlStore;
import cn.zhangyis.db.dd.repo.DictionarySnapshot;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.sdi.DictionarySdiCodec;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFullScrubRequest;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFullScrubResult;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFullScrubber;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.fil.catalog.FileDictionaryRecoveryManifestStore;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * catalog 丢失后的显式离线 {@code inspect -> quarantine -> inspect -> rebuild} 门面。
 *
 * <p>普通 {@code DatabaseEngine.open()} 不调用本服务，也不会自动采用 SDI。每个公开操作先取得与普通
 * 引擎共享的 instance file lock；quarantine/rebuild 再在锁内重跑完整 inspection 并精确比较 token，
 * 防止管理员依据过期目录快照移动或发布文件。</p>
 */
public final class CatalogRecoveryService {

    /** 公共数据库组合根使用的固定 dictionary space identity。 */
    private static final SpaceId DICTIONARY_SPACE_ID = SpaceId.of(1);
    /** 与 DDL/orphan cleanup 一致的受控候选命名。 */
    private static final Pattern TABLESPACE_NAME =
            Pattern.compile("table_([1-9][0-9]*)_space_([1-9][0-9]*)\\.ibd");
    /** manifest 固定路径名。 */
    private static final String MANIFEST_FILE = "mysql.dd.manifest";
    /** catalog 固定路径名。 */
    private static final String CATALOG_FILE = "mysql.ibd";
    /** control 固定路径名。 */
    private static final String CONTROL_FILE = "mysql.dd.ctrl";

    /** 冻结的实例配置；只使用 baseDir/pageSize/flush timeout 等稳定值。 */
    private final EngineConfig config;
    /** storage 层全页只读 scanner。 */
    private final TablespaceFullScrubber scrubber = new TablespaceFullScrubber();
    /** SDI opaque payload 的 DD 领域 decoder。 */
    private final DictionarySdiCodec sdiCodec = new DictionarySdiCodec();
    /** rebuild baseline records codec。 */
    private final DictionaryCatalogArchiveCodec archiveCodec = new DictionaryCatalogArchiveCodec();
    /** 所有恢复文件发布/隔离共用的无覆盖原子移动端口；生产固定为同盘 {@code ATOMIC_MOVE}。 */
    private final AtomicMover atomicMover;

    /**
     * 为一个未运行的数据库实例创建离线 recovery 门面。
     *
     * @param config 与普通 {@code DatabaseEngine} 相同的实例配置；页大小和 baseDir 必须匹配原实例
     * @throws DatabaseValidationException config 为空时抛出
     */
    public CatalogRecoveryService(EngineConfig config) {
        this(config, (source, destination) ->
                Files.move(source, destination, StandardCopyOption.ATOMIC_MOVE));
    }

    /**
     * 注入原子移动故障接缝；仅供同 package 恢复测试验证 crash/retry，不构成普通上层 API。
     *
     * @param config 与普通引擎一致的冻结配置
     * @param atomicMover 必须保持无覆盖、同盘原子移动语义的实现或故障注入器
     */
    CatalogRecoveryService(EngineConfig config, AtomicMover atomicMover) {
        if (config == null) {
            throw new DatabaseValidationException("catalog recovery engine config must not be null");
        }
        if (atomicMover == null) {
            throw new DatabaseValidationException("catalog recovery atomic mover must not be null");
        }
        this.config = config;
        this.atomicMover = atomicMover;
    }

    /**
     * 在实例锁内完整枚举目录、逐页扫描候选并生成恢复裁决。
     *
     * @param timeout instance lock 获取和本次完整扫描共享的正总时限
     * @return NO_RECOVERY_NEEDED、BLOCKED 或 REBUILDABLE inspection
     * @throws CatalogRecoveryBusyException 实例正在运行或 lock 等待超时/中断时抛出
     * @throws CatalogRecoveryException 无法形成保守 inspection 结果的实例级 IO 失败时抛出
     */
    public CatalogRecoveryInspection inspect(Duration timeout) {
        long deadline = deadline(timeout);
        try (DatabaseInstanceFileLock ignored =
                     DatabaseInstanceFileLock.acquire(config.baseDir(), remaining(deadline))) {
            return inspectLocked(deadline);
        }
    }

    /**
     * 根据仍然有效的 token 显式隔离管理员选择的可隔离冲突。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>取得实例独占锁并重跑完整 inspection；token 任一字段变化都在文件移动前拒绝。</li>
     *     <li>把 selected ids 映射到当前冲突，要求每项同时携带路径且类别明确允许 quarantine。</li>
     *     <li>预检全部源存在、目标不存在后，以同盘 {@code ATOMIC_MOVE} 逐项移动到 scanId 目录，不覆盖。</li>
     *     <li>移动完成后再次完整 inspection 并返回新 token；中途 crash 由下一次目录重扫裁决。</li>
     * </ol>
     *
     * @param token 上一次 complete inspection 返回的精确 token
     * @param selected 管理员显式选择的冲突 identities；不得为空
     * @param timeout instance lock、两次扫描和移动共享的正总时限
     * @return 移动后的新 inspection
     * @throws CatalogRecoveryStaleTokenException token 与锁内重扫不一致时抛出
     * @throws CatalogRecoveryException 选择不可隔离冲突、源/目标状态或 atomic move 失败时抛出
     */
    public CatalogRecoveryInspection quarantine(
            CatalogRecoveryToken token,
            Set<CatalogRecoveryConflictId> selected,
            Duration timeout) {
        // 1. 获取锁后必须从物理文件重新建立 token，不能信任调用方缓存的 conflicts。
        requireTokenAndSelection(token, selected, timeout);
        long deadline = deadline(timeout);
        try (DatabaseInstanceFileLock ignored =
                     DatabaseInstanceFileLock.acquire(config.baseDir(), remaining(deadline))) {
            CatalogRecoveryInspection current = inspectLocked(deadline);
            requireCurrentToken(token, current);

            // 2. 先解析并验证全部选择；任何非法项都不能形成部分 move。
            Map<CatalogRecoveryConflictId, CatalogRecoveryConflict> byId = new LinkedHashMap<>();
            for (CatalogRecoveryConflict conflict : current.conflicts()) {
                byId.put(conflict.id(), conflict);
            }
            List<CatalogRecoveryConflict> moves = new ArrayList<>(selected.size());
            Set<Path> uniqueSources = new HashSet<>();
            for (CatalogRecoveryConflictId id : selected) {
                CatalogRecoveryConflict conflict = byId.get(id);
                if (conflict == null || !conflict.quarantinable()) {
                    throw new CatalogRecoveryException(
                            "selected catalog recovery conflict is missing or not quarantinable: " + id.value());
                }
                Path source = conflict.path().orElseThrow();
                if (uniqueSources.add(source)) {
                    moves.add(conflict);
                }
            }
            moves.sort(Comparator.comparing(conflict -> conflict.path().orElseThrow().toString()));

            // 3. 目标与源全部预检后才开始移动；目标命名保留原文件名并禁止覆盖。
            Path destinationDirectory = config.baseDir().toAbsolutePath().normalize()
                    .resolve("catalog-recovery").resolve("quarantine").resolve(token.scanId());
            List<Move> prepared = new ArrayList<>(moves.size());
            for (CatalogRecoveryConflict conflict : moves) {
                Path source = conflict.path().orElseThrow();
                if (!existsNoFollow(source)) {
                    throw new CatalogRecoveryStaleTokenException(
                            "quarantine source disappeared after inspection: " + source);
                }
                Path destination = destinationDirectory.resolve(source.getFileName().toString());
                if (existsNoFollow(destination)) {
                    throw new CatalogRecoveryException(
                            "quarantine destination already exists; refusing overwrite: " + destination);
                }
                prepared.add(new Move(source, destination));
            }
            try {
                Files.createDirectories(destinationDirectory);
                for (Move move : prepared) {
                    checkDeadline(deadline, "quarantine");
                    atomicMover.move(move.source(), move.destination());
                }
            } catch (AtomicMoveNotSupportedException failure) {
                throw new CatalogRecoveryException(
                        "catalog recovery quarantine requires same-volume atomic move", failure);
            } catch (IOException failure) {
                throw new CatalogRecoveryException("catalog recovery quarantine move failed", failure);
            }

            // 4. 返回当前物理现场的新裁决；旧 token 自此必然过期。
            return inspectLocked(deadline);
        }
    }

    /**
     * 以 clean manifest baseline 重建 control/catalog，并只在完整验证后原子发布 catalog。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>取得实例锁、完整重扫并要求 token 精确一致、状态为 REBUILDABLE、catalog 路径不存在。</li>
     *     <li>从 manifest clean/control reservations 与有效 control 取逐分量最大，先 durable 创建或推进 control。</li>
     *     <li>在同目录临时 catalog 写单个 baseline，关闭后严格重开 repository，要求快照逐值等于 manifest。</li>
     *     <li>重新 full scrub 全部 expected 候选并比较 fingerprint，再确认目标不存在，以 ATOMIC_MOVE 发布 catalog。</li>
     * </ol>
     *
     * @param token 最近一次 REBUILDABLE complete inspection 的精确 token
     * @param timeout instance lock、重扫、control、baseline 校验和候选复核共享的正总时限
     * @return 已发布 catalog 和安全 control 摘要
     * @throws CatalogRecoveryStaleTokenException token 已过期时抛出
     * @throws CatalogRecoveryException 任一持久边界、复核或 atomic move 失败时抛出
     */
    public CatalogRebuildResult rebuild(CatalogRecoveryToken token, Duration timeout) {
        // 1. 只有锁内 complete scan 可授权写入，不接受手工构造或 BLOCKED token。
        if (token == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("catalog rebuild token/timeout is invalid");
        }
        long deadline = deadline(timeout);
        Path base = config.baseDir().toAbsolutePath().normalize();
        Path catalogPath = base.resolve(CATALOG_FILE);
        Path controlPath = base.resolve(CONTROL_FILE);
        try (DatabaseInstanceFileLock ignored =
                     DatabaseInstanceFileLock.acquire(base, remaining(deadline))) {
            CatalogRecoveryInspection inspection = inspectLocked(deadline);
            requireCurrentToken(token, inspection);
            if (inspection.status() != CatalogRecoveryStatus.REBUILDABLE) {
                throw new CatalogRecoveryException(
                        "catalog rebuild requires REBUILDABLE inspection: " + inspection.status());
            }
            if (existsNoFollow(catalogPath)) {
                throw new CatalogRecoveryException(
                        "catalog target must be absent before rebuild: " + catalogPath);
            }
            DictionaryRecoveryCleanSnapshot clean = inspection.manifest().orElseThrow();
            DictionaryRecoveryManifestView manifestView = loadManifestView().orElseThrow(() ->
                    new CatalogRecoveryStaleTokenException(
                            "manifest disappeared between inspection and rebuild"));
            if (!manifestView.rebuildable()
                    || manifestView.latestClean().orElseThrow().sequence() != clean.sequence()) {
                throw new CatalogRecoveryStaleTokenException(
                        "manifest changed between inspection and rebuild");
            }

            // 2. control 先于 catalog 发布；若此后 crash，普通 open 仍因 catalog missing 而 fail-closed。
            DictionaryControlSnapshot safeControl = DictionaryRecoveryManifestRepository.componentMaximum(
                    clean.controlSnapshot(), manifestView.safeControl().orElse(clean.controlSnapshot()));
            DictionaryControlSnapshot publishedControl;
            if (existsNoFollow(controlPath)) {
                try (DictionaryControlStore control =
                             DictionaryControlStore.openExisting(controlPath, DICTIONARY_SPACE_ID)) {
                    safeControl = DictionaryRecoveryManifestRepository.componentMaximum(
                            safeControl, control.snapshot());
                    publishedControl = control.advanceTo(safeControl);
                }
            } else {
                try (DictionaryControlStore control =
                             DictionaryControlStore.createRebuilt(controlPath, safeControl)) {
                    publishedControl = control.snapshot();
                }
            }

            // 3. baseline temp 的物理 batch 和逻辑 repository 都必须能从文件重建，才进入发布阶段。
            checkDeadline(deadline, "baseline write");
            Path temporary = base.resolve(
                    CATALOG_FILE + ".rebuild." + hex(clean.digest()).substring(0, 24));
            DictionarySnapshot expected = clean.dictionarySnapshot();
            prepareBaseline(temporary, expected, deadline);

            // 4. temp 验证期间用户文件也不得变化；复扫摘要后再做最后 target-absent 检查。
            Map<Path, TablespaceFullScrubResult> before = new HashMap<>();
            for (TablespaceFullScrubResult candidate : inspection.candidates()) {
                before.put(candidate.path(), candidate);
            }
            for (DictionaryRecoveryPathEntry entry : clean.paths()) {
                Path path = base.resolve("tables").resolve(entry.relativePath()).toAbsolutePath().normalize();
                TablespaceFullScrubResult previous = before.get(path);
                TablespaceFullScrubResult current = scrubber.scrub(new TablespaceFullScrubRequest(
                        path, config.pageSize(), entry.tableId(), SpaceId.of(entry.spaceId()),
                        remaining(deadline)));
                if (previous == null || !sameFingerprint(previous, current)) {
                    throw new CatalogRecoveryStaleTokenException(
                            "tablespace candidate changed before catalog publish: " + path);
                }
            }
            if (existsNoFollow(catalogPath)) {
                throw new CatalogRecoveryStaleTokenException(
                        "catalog target appeared before atomic publish: " + catalogPath);
            }
            try {
                atomicMover.move(temporary, catalogPath);
            } catch (AtomicMoveNotSupportedException failure) {
                throw new CatalogRecoveryException(
                        "catalog rebuild requires same-directory atomic move", failure);
            } catch (IOException failure) {
                throw new CatalogRecoveryException("publish rebuilt catalog failed: " + catalogPath, failure);
            }
            return new CatalogRebuildResult(catalogPath, expected.publishedVersion(),
                    expected.schemas().size(), expected.tables().size(), publishedControl,
                    hex(clean.digest()));
        }
    }

    /**
     * 创建或复用同一 clean manifest digest 的已验证 baseline 临时文件。
     *
     * <p>重建可能在临时 catalog durable 后、最终 rename 前崩溃。再次执行时，严格重开且快照一致的临时文件
     * 可以复用；截断、损坏或内容不一致的受控临时文件先原子移入 evidence，再从零创建，绝不删除证据。</p>
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>检查总 deadline；已存在临时文件先严格重开并逐值比较 baseline。</li>
     *     <li>不一致或损坏的受控临时文件原子移入 evidence，保留 crash 现场且释放固定名称。</li>
     *     <li>缺失时写入完整 baseline，关闭后再次严格重开；不一致则禁止进入最终 publish。</li>
     * </ol>
     *
     * @param temporary 当前 clean manifest digest 唯一确定的同目录临时 catalog 路径
     * @param expected clean manifest 解码出的完整稳定字典快照
     * @param deadline 本次 rebuild 共享的 monotonic deadline
     * @throws CatalogRecoveryException 临时证据无法保留、baseline 写入或回读验证失败时抛出
     */
    private void prepareBaseline(Path temporary, DictionarySnapshot expected, long deadline) {
        // 1. 只有逐值相等且物理格式可严格重开的 durable 临时文件才可作为幂等重试输入。
        checkDeadline(deadline, "baseline preparation");
        if (existsNoFollow(temporary)) {
            try {
                if (baselineMatches(temporary, expected)) {
                    return;
                }
            } catch (RuntimeException invalidTemporary) {
                // 下方统一保存无效临时文件；原始异常不代表 manifest baseline 本身不可重建。
            }
            // 2. 无效临时文件属于恢复证据，必须移动保留，不能 truncate/delete 后静默重用名称。
            preserveInvalidTemporary(temporary);
        }
        // 3. 新 baseline 经 append force、close 和独立 reopen 后才允许返回给最终 rename 阶段。
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(temporary)) {
            store.append(archiveCodec.encode(expected));
        }
        if (!baselineMatches(temporary, expected)) {
            throw new CatalogRecoveryException(
                    "reopened rebuilt catalog snapshot differs from clean manifest");
        }
    }

    /** 严格打开候选 baseline，只有完整 repository 快照逐值相等才允许复用或发布。 */
    private static boolean baselineMatches(Path temporary, DictionarySnapshot expected) {
        BasicFileAttributes attributes = attributesOrNull(temporary);
        if (attributes == null || !attributes.isRegularFile() || attributes.isSymbolicLink()) {
            return false;
        }
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openExisting(temporary)) {
            return new PersistentDictionaryRepository(store).snapshot().equals(expected);
        }
    }

    /** 把本工具命名空间内的无效重建临时文件原子保存为 evidence，随后才允许重新创建。 */
    private void preserveInvalidTemporary(Path temporary) {
        Path evidenceDirectory = config.baseDir().toAbsolutePath().normalize()
                .resolve("catalog-recovery").resolve("evidence");
        Path destination = evidenceDirectory.resolve(
                temporary.getFileName() + ".invalid." + Long.toUnsignedString(System.nanoTime()));
        try {
            Files.createDirectories(evidenceDirectory);
            atomicMover.move(temporary, destination);
        } catch (AtomicMoveNotSupportedException failure) {
            throw new CatalogRecoveryException(
                    "preserving invalid rebuild temporary requires same-volume atomic move", failure);
        } catch (IOException failure) {
            throw new CatalogRecoveryException(
                    "preserve invalid catalog rebuild temporary failed: " + temporary, failure);
        }
    }

    /**
     * 在已持 instance lock 条件下建立一次完整裁决。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>严格判定 catalog；有效 catalog 直接返回 NO_RECOVERY_NEEDED，绝不旁路权威真相。</li>
     *     <li>恢复 manifest clean/intent/control，并把缺失、损坏或 dirty 状态转换为显式冲突。</li>
     *     <li>完整枚举受控命名目录项，建立 manifest expected path 与实际 identity 集合。</li>
     *     <li>对每个候选计算 raw fingerprint、full-page scrub 并比较 SDI/archive，汇总全部冲突。</li>
     *     <li>再次绑定 catalog/control/raw/scrub/conflicts 生成 token；仅无冲突且 catalog missing 时可重建。</li>
     * </ol>
     *
     * @param deadline inspect/quarantine/rebuild 上层传入并共享的 monotonic deadline
     * @return 完整离线裁决；无法可靠枚举或解释 manifest 时不携带 token
     */
    private CatalogRecoveryInspection inspectLocked(long deadline) {
        Path base = config.baseDir().toAbsolutePath().normalize();
        Path catalogPath = base.resolve(CATALOG_FILE);
        Path controlPath = base.resolve(CONTROL_FILE);
        Path manifestPath = base.resolve(MANIFEST_FILE);
        Path tablesDirectory = base.resolve("tables");
        List<CatalogRecoveryConflict> conflicts = new ArrayList<>();

        // 1. catalog 有效即仍是权威真相，离线服务不旁路它。
        CatalogState catalog = catalogState(catalogPath);
        if (catalog == CatalogState.VALID) {
            return new CatalogRecoveryInspection(CatalogRecoveryStatus.NO_RECOVERY_NEEDED,
                    Optional.empty(), List.of(), List.of(), Optional.empty());
        }
        if (catalog == CatalogState.EMPTY) {
            conflicts.add(conflict(CatalogRecoveryConflictKind.CATALOG_EMPTY, catalogPath,
                    "dictionary catalog is a zero-length file"));
        } else if (catalog == CatalogState.CORRUPT) {
            conflicts.add(conflict(CatalogRecoveryConflictKind.CATALOG_CORRUPT, catalogPath,
                    "dictionary catalog cannot be strictly opened"));
        }

        // 2. manifest 必须能完整恢复；否则没有 schema/目录完整性证明，也不签发 token。
        Optional<DictionaryRecoveryManifestView> loadedManifest;
        try {
            loadedManifest = loadManifestView();
        } catch (RuntimeException failure) {
            conflicts.add(conflict(CatalogRecoveryConflictKind.MANIFEST_CORRUPT, manifestPath,
                    "dictionary recovery manifest is corrupt: " + diagnostic(failure)));
            return blockedWithoutToken(conflicts);
        }
        if (loadedManifest.isEmpty()) {
            conflicts.add(conflict(CatalogRecoveryConflictKind.MANIFEST_MISSING, null,
                    "dictionary recovery manifest does not exist"));
            return blockedWithoutToken(conflicts);
        }
        DictionaryRecoveryManifestView manifestView = loadedManifest.orElseThrow();
        DictionaryRecoveryCleanSnapshot clean = manifestView.latestClean().orElse(null);
        if (clean == null) {
            conflicts.add(conflict(CatalogRecoveryConflictKind.MANIFEST_MISSING, manifestPath,
                    "dictionary recovery manifest has no clean snapshot"));
            return blockedWithoutToken(conflicts);
        }
        if (manifestView.unresolvedCatalogMutation()) {
            conflicts.add(conflict(CatalogRecoveryConflictKind.MANIFEST_DIRTY, manifestPath,
                    "clean snapshot is followed by unresolved catalog mutation intent"));
        }

        // control missing 可由 manifest 重建；empty/corrupt 必须先显式隔离，valid 会在 rebuild 单调推进。
        ControlState control = controlState(controlPath);
        if (control == ControlState.EMPTY) {
            conflicts.add(conflict(CatalogRecoveryConflictKind.CONTROL_EMPTY, controlPath,
                    "dictionary control is a zero-length file"));
        } else if (control == ControlState.CORRUPT) {
            conflicts.add(conflict(CatalogRecoveryConflictKind.CONTROL_CORRUPT, controlPath,
                    "dictionary control cannot recover either valid slot"));
        }

        // 3. 枚举失败表示 complete scan 不成立，此时不能签发任何写操作 token。
        List<CandidateName> names;
        try {
            names = enumerateCandidates(tablesDirectory);
        } catch (RuntimeException failure) {
            conflicts.add(conflict(CatalogRecoveryConflictKind.DIRECTORY_SCAN_FAILED, tablesDirectory,
                    "cannot completely enumerate tables directory: " + diagnostic(failure)));
            return blockedWithoutToken(conflicts);
        }

        Map<Path, DictionaryRecoveryPathEntry> expectedByPath = new LinkedHashMap<>();
        for (DictionaryRecoveryPathEntry entry : clean.paths()) {
            Path expected = tablesDirectory.resolve(entry.relativePath()).toAbsolutePath().normalize();
            if (!expected.startsWith(tablesDirectory.toAbsolutePath().normalize())) {
                conflicts.add(conflict(CatalogRecoveryConflictKind.MANIFEST_CORRUPT, manifestPath,
                        "manifest path escapes tables directory: " + entry.relativePath()));
                return blockedWithoutToken(conflicts);
            }
            expectedByPath.put(expected, entry);
        }
        Map<Long, TableDefinition> tableById = new HashMap<>();
        clean.dictionarySnapshot().tables().values()
                .forEach(table -> tableById.put(table.id().value(), table));
        Set<Path> observed = new HashSet<>();
        List<TablespaceFullScrubResult> candidates = new ArrayList<>();
        List<CandidateFingerprint> candidateFingerprints = new ArrayList<>();
        Map<Long, List<CandidateName>> byTable = new HashMap<>();
        Map<Integer, List<CandidateName>> bySpace = new HashMap<>();

        // 4. raw 与 scrub 双指纹覆盖每个目录项；expected 的逻辑 SDI 还必须逐值等于 clean archive。
        for (CandidateName name : names) {
            checkDeadline(deadline, "candidate scrub");
            CandidateFingerprint rawFingerprint;
            try {
                rawFingerprint = fingerprint(name.path(), deadline);
            } catch (CatalogRecoveryBusyException timeoutOrInterrupt) {
                throw timeoutOrInterrupt;
            } catch (RuntimeException failure) {
                conflicts.add(conflict(CatalogRecoveryConflictKind.DIRECTORY_SCAN_FAILED, name.path(),
                        "cannot bind candidate raw fingerprint: " + diagnostic(failure)));
                return blockedWithoutToken(conflicts);
            }
            candidateFingerprints.add(rawFingerprint);
            observed.add(name.path());
            byTable.computeIfAbsent(name.tableId(), ignored -> new ArrayList<>()).add(name);
            bySpace.computeIfAbsent(name.spaceId(), ignored -> new ArrayList<>()).add(name);
            DictionaryRecoveryPathEntry expected = expectedByPath.get(name.path());
            try {
                TablespaceFullScrubResult result = scrubber.scrub(new TablespaceFullScrubRequest(
                        name.path(), config.pageSize(), name.tableId(), SpaceId.of(name.spaceId()),
                        remaining(deadline)));
                if (!rawFingerprint.matches(result)) {
                    throw new CatalogRecoveryStaleTokenException(
                            "candidate changed between raw fingerprint and full scrub: " + name.path());
                }
                candidates.add(result);
                if (expected == null) {
                    conflicts.add(conflict(CatalogRecoveryConflictKind.EXTRA_CANDIDATE, name.path(),
                            "candidate path is not declared by clean manifest"));
                    continue;
                }
                if (expected.tableId() != name.tableId() || expected.spaceId() != name.spaceId()) {
                    conflicts.add(conflict(CatalogRecoveryConflictKind.EXPECTED_SDI_MISMATCH, name.path(),
                            "candidate filename identity differs from manifest path entry"));
                    continue;
                }
                TableDefinition decoded = sdiCodec.decode(result.sdi().payload());
                TableDefinition manifestTable = tableById.get(expected.tableId());
                if (manifestTable == null || !decoded.equals(manifestTable)
                        || !MessageDigest.isEqual(expected.sdiDigest(), sha256(result.sdi().payload()))) {
                    conflicts.add(conflict(CatalogRecoveryConflictKind.EXPECTED_SDI_MISMATCH, name.path(),
                            "candidate SDI aggregate/digest differs from clean manifest"));
                }
            } catch (CatalogRecoveryStaleTokenException changedDuringScan) {
                throw changedDuringScan;
            } catch (RuntimeException failure) {
                // timeout/interrupt 表示 complete scan 未完成，不能伪装成稳定的文件损坏并签发 token。
                checkDeadline(deadline, "candidate scrub");
                CatalogRecoveryConflictKind kind = expected == null
                        ? CatalogRecoveryConflictKind.EXTRA_CANDIDATE_INVALID
                        : CatalogRecoveryConflictKind.EXPECTED_CANDIDATE_INVALID;
                conflicts.add(conflict(kind, name.path(),
                        "candidate full-page scrub failed: " + diagnostic(failure)));
            }
        }
        for (Map.Entry<Path, DictionaryRecoveryPathEntry> expected : expectedByPath.entrySet()) {
            if (!observed.contains(expected.getKey())) {
                conflicts.add(conflict(CatalogRecoveryConflictKind.EXPECTED_CANDIDATE_MISSING, expected.getKey(),
                        "clean manifest expected table " + expected.getValue().tableId() + " at this path"));
            }
        }
        addDuplicateConflicts(byTable, expectedByPath, CatalogRecoveryConflictKind.DUPLICATE_TABLE_ID, conflicts);
        addDuplicateConflicts(bySpace, expectedByPath, CatalogRecoveryConflictKind.DUPLICATE_SPACE_ID, conflicts);

        // 5. token canonical stream 使用确定顺序并绑定所有可观察证据，状态变化后必须重新 inspect。
        candidates.sort(Comparator.comparing(result -> result.path().toString()));
        conflicts.sort(Comparator.comparing(conflict -> conflict.id().value()));
        CatalogRecoveryToken token = token(clean, manifestView.lastSequence(), catalog, control,
                fingerprint(catalogPath, deadline), fingerprint(controlPath, deadline),
                candidateFingerprints, candidates, conflicts);
        CatalogRecoveryStatus status = conflicts.isEmpty() && manifestView.rebuildable()
                && catalog == CatalogState.MISSING
                ? CatalogRecoveryStatus.REBUILDABLE
                : CatalogRecoveryStatus.BLOCKED;
        return new CatalogRecoveryInspection(status, Optional.of(clean), candidates, conflicts,
                Optional.of(token));
    }

    /**
     * 严格打开 manifest 并返回 committed event 推导视图。
     *
     * @return 文件缺失时为空；存在时返回经过物理 journal 与逻辑事件完整校验的视图
     */
    private Optional<DictionaryRecoveryManifestView> loadManifestView() {
        Path path = config.baseDir().toAbsolutePath().normalize().resolve(MANIFEST_FILE);
        if (!existsNoFollow(path)) {
            return Optional.empty();
        }
        try (FileDictionaryRecoveryManifestStore store =
                     FileDictionaryRecoveryManifestStore.openExisting(path)) {
            return Optional.of(new DictionaryRecoveryManifestRepository(store).view());
        }
    }

    /** 严格区分 missing/empty/valid/corrupt catalog。 */
    private static CatalogState catalogState(Path path) {
        BasicFileAttributes attributes = attributesOrNull(path);
        if (attributes == null) {
            return CatalogState.MISSING;
        }
        if (!attributes.isRegularFile() || attributes.isSymbolicLink()) {
            return CatalogState.CORRUPT;
        }
        if (attributes.size() == 0) {
            return CatalogState.EMPTY;
        }
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openExisting(path)) {
            new PersistentDictionaryRepository(store);
            return CatalogState.VALID;
        } catch (RuntimeException failure) {
            return CatalogState.CORRUPT;
        }
    }

    /** 严格区分 missing/empty/valid/corrupt control。 */
    private static ControlState controlState(Path path) {
        BasicFileAttributes attributes = attributesOrNull(path);
        if (attributes == null) {
            return ControlState.MISSING;
        }
        if (!attributes.isRegularFile() || attributes.isSymbolicLink()) {
            return ControlState.CORRUPT;
        }
        if (attributes.size() == 0) {
            return ControlState.EMPTY;
        }
        try (DictionaryControlStore ignored =
                     DictionaryControlStore.openExisting(path, DICTIONARY_SPACE_ID)) {
            return ControlState.VALID;
        } catch (RuntimeException failure) {
            return ControlState.CORRUPT;
        }
    }

    /** 完整枚举受控文件名并解析 identity；非法数字/类型作为目录失败保守阻断。 */
    private static List<CandidateName> enumerateCandidates(Path tablesDirectory) {
        BasicFileAttributes directory = attributesOrNull(tablesDirectory);
        if (directory == null) {
            return List.of();
        }
        if (!directory.isDirectory() || directory.isSymbolicLink()) {
            throw new CatalogRecoveryException("tables path is not a NOFOLLOW directory: " + tablesDirectory);
        }
        List<CandidateName> result = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(tablesDirectory, "table_*_space_*.ibd")) {
            for (Path entry : stream) {
                Path path = entry.toAbsolutePath().normalize();
                Matcher matcher = TABLESPACE_NAME.matcher(path.getFileName().toString());
                if (!matcher.matches()) {
                    throw new CatalogRecoveryException("controlled tablespace name is malformed: " + path);
                }
                long tableId = Long.parseLong(matcher.group(1));
                int spaceId = Integer.parseInt(matcher.group(2));
                result.add(new CandidateName(path, tableId, spaceId));
            }
        } catch (IOException | NumberFormatException failure) {
            throw new CatalogRecoveryException("enumerate controlled tablespaces failed: " + tablesDirectory,
                    failure);
        }
        result.sort(Comparator.comparing(candidate -> candidate.path().toString()));
        return List.copyOf(result);
    }

    /** 对重复 identity 的非 expected 副本添加可隔离冲突。 */
    private static <K> void addDuplicateConflicts(
            Map<K, List<CandidateName>> groups,
            Map<Path, DictionaryRecoveryPathEntry> expectedByPath,
            CatalogRecoveryConflictKind kind,
            List<CatalogRecoveryConflict> conflicts) {
        for (Map.Entry<K, List<CandidateName>> group : groups.entrySet()) {
            if (group.getValue().size() < 2) {
                continue;
            }
            for (CandidateName candidate : group.getValue()) {
                if (!expectedByPath.containsKey(candidate.path())) {
                    conflicts.add(conflict(kind, candidate.path(),
                            "duplicate candidate identity " + group.getKey()));
                }
            }
        }
    }

    /** 生成绑定所有可观察状态的确定性 token。 */
    private static CatalogRecoveryToken token(
            DictionaryRecoveryCleanSnapshot clean,
            long manifestCommittedSequence,
            CatalogState catalog,
            ControlState control,
            CandidateFingerprint catalogFingerprint,
            CandidateFingerprint controlFingerprint,
            List<CandidateFingerprint> candidateFingerprints,
            List<TablespaceFullScrubResult> candidates,
            List<CatalogRecoveryConflict> conflicts) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        addLine(bytes, "manifest=" + manifestCommittedSequence + ":" + hex(clean.digest()));
        addLine(bytes, "catalog=" + catalog + ";" + catalogFingerprint.canonical());
        addLine(bytes, "control=" + control + ";" + controlFingerprint.canonical());
        for (CandidateFingerprint fingerprint : candidateFingerprints) {
            addLine(bytes, "entry=" + fingerprint.canonical());
        }
        for (TablespaceFullScrubResult candidate : candidates) {
            addLine(bytes, "candidate=" + candidate.path() + "|" + candidate.fileSize() + "|"
                    + candidate.lastModifiedMillis() + "|" + candidate.fileKey() + "|"
                    + hex(candidate.fileDigest()) + "|" + hex(sha256(candidate.sdi().payload())));
        }
        for (CatalogRecoveryConflict conflict : conflicts) {
            addLine(bytes, "conflict=" + conflict.id().value() + "|" + conflict.kind() + "|"
                    + conflict.path().map(Path::toString).orElse("") + "|" + conflict.detail());
        }
        String digest = hex(sha256(bytes.toByteArray()));
        return new CatalogRecoveryToken(digest.substring(0, 24), manifestCommittedSequence,
                hex(clean.digest()), digest);
    }

    /**
     * 对 token 范围内每个目录项计算原始 fingerprint。regular file 读取全部字节并在前后属性间复核；
     * missing 使用显式哨兵，symlink/其它类型只绑定 NOFOLLOW entry 属性，因为 scanner 永远不会跟随它。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>NOFOLLOW 读取目录项类型与初始 size/mtime/fileKey；missing 生成确定性哨兵。</li>
     *     <li>普通文件使用只读 channel 在共享 deadline 内覆盖全部字节计算 SHA-256。</li>
     *     <li>读取结束再次比较属性；变化表示本次 complete scan 不成立并抛 stale token 异常。</li>
     *     <li>返回规范路径、类型、属性与摘要组成的 immutable token 片段。</li>
     * </ol>
     *
     * @param path catalog、control 或完整枚举得到的候选路径
     * @param deadline 当前恢复操作共享的 monotonic deadline
     * @return 覆盖该目录项当前形状与内容的 token 片段
     * @throws CatalogRecoveryException 属性或全文件读取失败时抛出
     * @throws CatalogRecoveryStaleTokenException 扫描中目录项变化时抛出
     */
    private static CandidateFingerprint fingerprint(Path path, long deadline) {
        // 1. missing 与非常规文件也进入 token，不能因缺少内容摘要从目录快照消失。
        BasicFileAttributes before = attributesOrNull(path);
        if (before == null) {
            return new CandidateFingerprint(path.toAbsolutePath().normalize(), "missing",
                    0, 0, "", "");
        }
        String type = before.isRegularFile() && !before.isSymbolicLink()
                ? "regular" : before.isSymbolicLink() ? "symlink"
                : before.isDirectory() ? "directory" : "other";
        String digest = "";
        if ("regular".equals(type)) {
            // 2. 摘要覆盖文件全部字节，不以 header 或 committed length 替代原始证据。
            MessageDigest hash;
            try {
                hash = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException failure) {
                throw new CatalogRecoveryException("SHA-256 is unavailable", failure);
            }
            ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
            try (FileChannel channel = FileChannel.open(
                    path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                long position = 0;
                while (position < before.size()) {
                    checkDeadline(deadline, "candidate fingerprint");
                    buffer.clear();
                    int expected = (int) Math.min(buffer.capacity(), before.size() - position);
                    buffer.limit(expected);
                    int read = channel.read(buffer, position);
                    if (read <= 0) {
                        throw new IOException("unexpected EOF/no progress while fingerprinting recovery evidence");
                    }
                    hash.update(buffer.array(), 0, read);
                    position += read;
                }
            } catch (IOException failure) {
                throw new CatalogRecoveryException(
                        "fingerprint catalog recovery evidence failed: " + path, failure);
            }
            // 3. 摘要只能与同一 file identity/属性快照配对；变化时整次操作必须重试。
            BasicFileAttributes after = attributesOrNull(path);
            if (after == null || !after.isRegularFile() || after.isSymbolicLink()
                    || before.size() != after.size()
                    || !before.lastModifiedTime().equals(after.lastModifiedTime())
                    || !fileKey(before).equals(fileKey(after))) {
                throw new CatalogRecoveryStaleTokenException(
                        "catalog recovery evidence changed during fingerprint: " + path);
            }
            digest = hex(hash.digest());
        }
        // 4. 非普通目录项摘要为空，但 type/NOFOLLOW 属性仍使 token 能检测替换。
        return new CandidateFingerprint(path.toAbsolutePath().normalize(), type, before.size(),
                before.lastModifiedTime().toMillis(), fileKey(before), digest);
    }

    /** 构造 deterministic conflict id。 */
    private static CatalogRecoveryConflict conflict(
            CatalogRecoveryConflictKind kind, Path path, String detail) {
        Path normalized = path == null ? null : path.toAbsolutePath().normalize();
        String identity = kind + "|" + (normalized == null ? "" : normalized) + "|" + detail;
        CatalogRecoveryConflictId id = new CatalogRecoveryConflictId(
                hex(sha256(identity.getBytes(StandardCharsets.UTF_8))).substring(0, 24));
        return new CatalogRecoveryConflict(id, kind, Optional.ofNullable(normalized), detail);
    }

    /** 没有可靠 clean/目录边界时返回不带 token 的 BLOCKED 结果。 */
    private static CatalogRecoveryInspection blockedWithoutToken(List<CatalogRecoveryConflict> conflicts) {
        conflicts.sort(Comparator.comparing(conflict -> conflict.id().value()));
        return new CatalogRecoveryInspection(CatalogRecoveryStatus.BLOCKED, Optional.empty(),
                List.of(), conflicts, Optional.empty());
    }

    /** 验证 quarantine 公共输入。 */
    private static void requireTokenAndSelection(
            CatalogRecoveryToken token,
            Set<CatalogRecoveryConflictId> selected,
            Duration timeout) {
        if (token == null || selected == null || selected.isEmpty()
                || selected.stream().anyMatch(java.util.Objects::isNull)
                || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("catalog quarantine token/selection/timeout is invalid");
        }
    }

    /** 要求调用 token 与锁内 complete scan 逐字段相同。 */
    private static void requireCurrentToken(
            CatalogRecoveryToken requested,
            CatalogRecoveryInspection current) {
        CatalogRecoveryToken observed = current.token().orElseThrow(() ->
                new CatalogRecoveryStaleTokenException(
                        "current catalog recovery inspection cannot issue a token"));
        if (!observed.equals(requested)) {
            throw new CatalogRecoveryStaleTokenException(
                    "catalog recovery token is stale; run inspect again");
        }
    }

    /** 比较两次 full scrub 的稳定文件与 SDI fingerprint。 */
    private static boolean sameFingerprint(
            TablespaceFullScrubResult left,
            TablespaceFullScrubResult right) {
        return left.path().equals(right.path())
                && left.fileSize() == right.fileSize()
                && left.lastModifiedMillis() == right.lastModifiedMillis()
                && left.fileKey().equals(right.fileKey())
                && left.pageCount() == right.pageCount()
                && left.spaceId().equals(right.spaceId())
                && left.pageSize().equals(right.pageSize())
                && left.spaceVersion() == right.spaceVersion()
                && left.sdi().equals(right.sdi())
                && MessageDigest.isEqual(left.fileDigest(), right.fileDigest());
    }

    /** NOFOLLOW 读取属性；明确 missing 返回 null，其它失败保守抛出。 */
    private static BasicFileAttributes attributesOrNull(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException missing) {
            return null;
        } catch (IOException failure) {
            throw new CatalogRecoveryException("read catalog recovery file attributes failed: " + path, failure);
        }
    }

    /** NOFOLLOW 判断路径目录项是否存在。 */
    private static boolean existsNoFollow(Path path) {
        return attributesOrNull(path) != null;
    }

    /** 建立正 timeout 的 monotonic deadline。 */
    private static long deadline(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("catalog recovery timeout must be positive");
        }
        try {
            long now = System.nanoTime();
            long target = now + timeout.toNanos();
            return target < 0 && now > 0 ? Long.MAX_VALUE : target;
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    /** 返回剩余正 timeout；耗尽时直接抛出，不把 zero 传给下游等待。 */
    private static Duration remaining(long deadline) {
        long nanos = deadline == Long.MAX_VALUE ? Long.MAX_VALUE : deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new CatalogRecoveryBusyException("catalog recovery operation timed out");
        }
        return Duration.ofNanos(nanos);
    }

    /** 在非阻塞阶段响应中断与 deadline。 */
    private static void checkDeadline(long deadline, String stage) {
        if (Thread.currentThread().isInterrupted()) {
            throw new CatalogRecoveryBusyException("catalog recovery interrupted during " + stage);
        }
        if (deadline != Long.MAX_VALUE && System.nanoTime() - deadline >= 0) {
            throw new CatalogRecoveryBusyException("catalog recovery timed out during " + stage);
        }
    }

    /** 计算 SHA-256。 */
    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException failure) {
            throw new CatalogRecoveryException("SHA-256 is unavailable", failure);
        }
    }

    /** 使用小写十六进制编码摘要。 */
    private static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }

    /** 向 token canonical stream 添加 UTF-8 行。 */
    private static void addLine(ByteArrayOutputStream output, String line) {
        output.writeBytes(line.getBytes(StandardCharsets.UTF_8));
        output.write('\n');
    }

    /** 限制异常诊断进入 token 的长度，避免底层路径堆栈或超长消息放大结果。 */
    private static String diagnostic(Throwable failure) {
        String message = failure.getMessage();
        String value = failure.getClass().getSimpleName() + ": " + (message == null ? "" : message);
        return value.length() <= 1024 ? value : value.substring(0, 1024);
    }

    /** 把可选文件系统 fileKey 归一成 token 字符串。 */
    private static String fileKey(BasicFileAttributes attributes) {
        return attributes.fileKey() == null ? "" : attributes.fileKey().toString();
    }

    /** catalog 路径状态。 */
    private enum CatalogState {
        MISSING,
        EMPTY,
        VALID,
        CORRUPT
    }

    /** control 路径状态。 */
    private enum ControlState {
        MISSING,
        EMPTY,
        VALID,
        CORRUPT
    }

    /** 文件名解析结果。 */
    private record CandidateName(Path path, long tableId, int spaceId) {
    }

    /** 已预检的 atomic move。 */
    private record Move(Path source, Path destination) {
    }

    /** complete-scan token 中每个原始目录项的 NOFOLLOW 属性与可用时的全文件摘要。 */
    private record CandidateFingerprint(
            Path path, String type, long size, long lastModifiedMillis, String fileKey, String digest) {

        private String canonical() {
            return path + "|" + type + "|" + size + "|" + lastModifiedMillis + "|" + fileKey + "|" + digest;
        }

        /** 比较 raw directory evidence 与 full scrub 结束时的稳定文件 fingerprint。 */
        private boolean matches(TablespaceFullScrubResult result) {
            return type.equals("regular")
                    && path.equals(result.path())
                    && size == result.fileSize()
                    && lastModifiedMillis == result.lastModifiedMillis()
                    && fileKey.equals(result.fileKey())
                    && digest.equals(hex(result.fileDigest()));
        }
    }

    /**
     * catalog recovery 的同盘无覆盖 rename 接缝。
     *
     * <p>生产实现必须请求 {@link StandardCopyOption#ATOMIC_MOVE} 且不得携带 REPLACE_EXISTING；测试实现只用于
     * 在 move 前抛出 {@link IOException}，不能伪造非原子成功。</p>
     */
    @FunctionalInterface
    interface AtomicMover {

        /**
         * 原子移动一个已预检源到不存在目标。
         *
         * @param source 当前 instance lock 下验证过的源
         * @param destination 同一文件系统内且必须不存在的目标
         * @throws IOException 文件系统拒绝或故障注入中断移动时抛出
         */
        void move(Path source, Path destination) throws IOException;
    }
}
