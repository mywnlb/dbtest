package cn.zhangyis.db.engine;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.exception.DictionaryCatalogAdmissionException;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;
import cn.zhangyis.db.storage.redo.RotatingRedoLogRepository;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.regex.Pattern;

/**
 * 公共数据库组合根的 catalog 启动准入 Guard。
 *
 * <p>{@code mysql.ibd} 非空时，本类只允许按 existing catalog 打开；缺失或零长度时，则先检查
 * DD control/recovery manifest、redo、undo、doublewrite 和受控用户表空间。只有完全没有权威持久痕迹时才允许初始化
 * 空 catalog，避免空字典驱动 orphan cleanup 删除仍有价值的表空间。</p>
 *
 * <p>该 Guard 只执行元数据级文件存在性扫描，不打开数据页、不修改文件，也不持有任何句柄进入后续
 * storage recovery。调用方 {@link DatabaseEngine} 在进入本 Guard 前已持有跨进程 instance file lock，
 * 因此状态判定到 catalog 打开之间不会与另一个合规引擎或离线 recovery 并发。</p>
 */
final class CatalogBootstrapAdmission {

    /** 与系统 undo 文件稳定布局一致；扫描所有历史配置的 space id，而非只看当前配置。 */
    private static final Pattern UNDO_FILE_NAME = Pattern.compile("undo_[0-9]+\\.ibu");
    /** 必须与 {@code DictionaryDdlRecoveryService.cleanupDroppedAndOrphans} 的删除候选范围一致。 */
    private static final String CONTROLLED_TABLESPACE_GLOB = "table_*_space_*.ibd";

    /** 工具类不发布实例；所有状态都局限在一次 {@link #openCatalog} 调用内。 */
    private CatalogBootstrapAdmission() {
    }

    /**
     * 根据 catalog 状态和实例持久证据选择 existing open 或 fresh initialization。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验组合根传入的配置与固定路径，并只读取得 catalog 的 missing/empty/present 状态；
     *     状态读取失败时不创建任何文件。</li>
     *     <li>非空 catalog 直接交给 {@link FileInternalCatalogStore#openExisting(Path)} 做双 header、
     *     committed 边界和 frame 校验，任何损坏都不得降级为 fresh。</li>
     *     <li>missing/empty catalog 扫描全部权威持久痕迹；任一证据或扫描异常都致命拒绝，
     *     从而保证 storage、DDL recovery 和 orphan cleanup 尚未启动。</li>
     *     <li>仅在证据集合为空时调用 {@link FileInternalCatalogStore#openOrCreate(Path)}，
     *     durable 初始化双 header 后才把 store ownership 返回组合根。</li>
     * </ol>
     *
     * @param config 公共组合根冻结的引擎配置；用于定位所有 redo、undo 和 doublewrite 稳定路径
     * @param catalogPath 固定的 {@code mysql.ibd} 路径；必须属于当前实例根目录
     * @param controlPath 固定的 {@code mysql.dd.ctrl} 路径；存在即证明实例不是 fresh
     * @param tablesDirectory 受控 file-per-table 目录；扫描范围必须与 orphan cleanup 完全一致
     * @return existing 校验成功或 fresh 双 header 已 durable 的 catalog store；调用方接管并负责关闭
     * @throws DatabaseValidationException 任一配置或路径为 {@code null} 时抛出，文件系统保持不变
     * @throws DictionaryCatalogAdmissionException catalog 不是普通文件、existing 证据存在或证据无法检查时抛出；
     * 调用方必须保持普通流量关闭，并且不得运行 orphan cleanup
     */
    static FileInternalCatalogStore openCatalog(
            EngineConfig config, Path catalogPath, Path controlPath, Path tablesDirectory) {
        // 1、在任何 catalog 创建或 storage 打开前冻结路径并判定 catalog 状态。
        if (config == null || catalogPath == null || controlPath == null || tablesDirectory == null) {
            throw new DatabaseValidationException("catalog admission config and paths must not be null");
        }
        Path normalizedCatalog = catalogPath.toAbsolutePath().normalize();
        CatalogState catalogState = inspectCatalogState(normalizedCatalog);

        // 2、非空 catalog 永远沿 existing 校验路径恢复，不允许把格式损坏解释为新实例。
        if (catalogState == CatalogState.PRESENT) {
            return FileInternalCatalogStore.openExisting(normalizedCatalog);
        }

        // 3、空或缺失 catalog 必须证明整个实例没有权威持久痕迹；未知状态与任一证据均 fail-closed。
        EnumSet<PersistentArtifact> evidence = inspectPersistentArtifacts(
                config, controlPath.toAbsolutePath().normalize(),
                tablesDirectory.toAbsolutePath().normalize(), normalizedCatalog, catalogState);
        if (!evidence.isEmpty()) {
            throw new DictionaryCatalogAdmissionException(
                    "refuse to initialize " + catalogState + " dictionary catalog: catalog="
                            + normalizedCatalog + " existingArtifacts=" + evidence
                            + "; ordinary startup never rebuilds catalog; use the explicit offline recovery service");
        }

        // 4、只有完全无证据的 missing/empty 状态才是可初始化的 fresh catalog。
        return FileInternalCatalogStore.openOrCreate(normalizedCatalog);
    }

    /**
     * 读取 catalog 文件形状；非普通文件无法承载 catalog 双 header，必须在创建路径前拒绝。
     *
     * @param catalogPath 已规范化的固定 catalog 路径
     * @return {@link CatalogState#MISSING}、{@link CatalogState#EMPTY} 或 {@link CatalogState#PRESENT}
     * @throws DictionaryCatalogAdmissionException 文件类型非法或属性读取失败时抛出，原始 IO cause 被保留
     */
    private static CatalogState inspectCatalogState(Path catalogPath) {
        try {
            BasicFileAttributes attributes = Files.readAttributes(
                    catalogPath, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (attributes.isSymbolicLink() || !attributes.isRegularFile()) {
                throw new DictionaryCatalogAdmissionException(
                        "dictionary catalog path is not a regular file: " + catalogPath);
            }
            return attributes.size() == 0 ? CatalogState.EMPTY : CatalogState.PRESENT;
        } catch (NoSuchFileException missing) {
            return CatalogState.MISSING;
        } catch (IOException failure) {
            throw new DictionaryCatalogAdmissionException(
                    "cannot inspect dictionary catalog state without risking fresh initialization: "
                            + catalogPath, failure);
        }
    }

    /**
     * 收集有界的持久证据类别，而不是用户文件清单；类别足以诊断拒绝原因，也不会随表数量无限占用内存。
     *
     * @param config 当前实例配置；同时检查 ring 与 single-file redo，避免配置切换隐藏旧证据
     * @param controlPath 固定 DD control 路径
     * @param tablesDirectory 与 orphan cleanup 共用的受控目录
     * @param catalogPath 仅用于失败诊断，绝不作为 evidence 重复计数
     * @param catalogState 已判定的 missing 或 empty 状态
     * @return 按稳定枚举顺序排列的不可空证据集合；空集合表示可继续 fresh catalog 初始化
     * @throws DictionaryCatalogAdmissionException 任一目录或路径无法检查时抛出并保留原始 cause
     */
    private static EnumSet<PersistentArtifact> inspectPersistentArtifacts(
            EngineConfig config, Path controlPath, Path tablesDirectory,
            Path catalogPath, CatalogState catalogState) {
        EnumSet<PersistentArtifact> evidence = EnumSet.noneOf(PersistentArtifact.class);
        try {
            if (pathExists(controlPath)) {
                evidence.add(PersistentArtifact.DICTIONARY_CONTROL);
            }
            if (pathExists(config.baseDir().resolve("mysql.dd.manifest"))) {
                evidence.add(PersistentArtifact.DICTIONARY_RECOVERY_MANIFEST);
            }
            if (pathExists(config.redoFile())) {
                evidence.add(PersistentArtifact.SINGLE_FILE_REDO);
            }
            if (RotatingRedoLogRepository.hasAnyRingFiles(config.redoDir())) {
                evidence.add(PersistentArtifact.REDO_RING);
            }
            if (pathExists(config.redoControlFile())) {
                evidence.add(PersistentArtifact.REDO_CONTROL);
            }
            if (pathExists(config.transactionRecoveryCheckpointFile())) {
                evidence.add(PersistentArtifact.TRANSACTION_RECOVERY_CONTROL);
            }
            if (hasStableUndoFile(config.baseDir())) {
                evidence.add(PersistentArtifact.UNDO_TABLESPACE);
            }
            if (pathExists(config.doublewriteFile())
                    || pathExists(config.flushListDoublewriteFile())
                    || pathExists(config.lruDoublewriteFile())) {
                evidence.add(PersistentArtifact.DOUBLEWRITE);
            }
            if (hasMatchingEntry(tablesDirectory, CONTROLLED_TABLESPACE_GLOB)) {
                evidence.add(PersistentArtifact.CONTROLLED_TABLESPACE);
            }
            return evidence;
        } catch (IOException | DatabaseRuntimeException failure) {
            throw new DictionaryCatalogAdmissionException(
                    "cannot prove instance is fresh while catalog is " + catalogState + ": catalog="
                            + catalogPath + "; persistent evidence inspection failed before recovery or cleanup",
                    failure);
        }
    }

    /**
     * 检查稳定命名的任意系统 undo 文件；数字 id 可来自旧配置，不能只认当前 {@code undoSpaceId}。
     *
     * @param baseDir 已存在的实例根目录
     * @return 至少发现一个 {@code undo_<digits>.ibu} 目录项时为 {@code true}
     * @throws IOException 根目录无法扫描时抛出，由上层转为 fail-closed catalog admission
     */
    private static boolean hasStableUndoFile(Path baseDir) throws IOException {
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(baseDir, "undo_*.ibu")) {
            for (Path entry : entries) {
                if (UNDO_FILE_NAME.matcher(entry.getFileName().toString()).matches()) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * 判断目录中是否存在与 cleanup 相同 glob 匹配的任意项；目录项类型不影响保护，因为旧 cleanup
     * 同样会尝试处理该名字。
     *
     * @param directory 候选目录；不存在表示当前类别无证据
     * @param glob 与下游清理范围一致的稳定文件名表达式
     * @return 至少存在一个匹配目录项时为 {@code true}
     * @throws IOException 目录存在但无法扫描或实际不是目录时抛出
     */
    private static boolean hasMatchingEntry(Path directory, String glob) throws IOException {
        if (!pathExists(directory)) {
            return false;
        }
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory, glob)) {
            return entries.iterator().hasNext();
        }
    }

    /**
     * 使用 NOFOLLOW_LINKS 检查目录项本身是否存在，确保零长度文件和 broken link 也不能被当作无证据。
     *
     * @param path 待检查的固定持久路径
     * @return 路径目录项存在时为 {@code true}，明确不存在时为 {@code false}
     * @throws IOException 无法区分存在与不存在时抛出，调用方必须 fail-closed
     */
    private static boolean pathExists(Path path) throws IOException {
        try {
            Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            return true;
        } catch (NoSuchFileException missing) {
            return false;
        }
    }

    /** catalog 文件在准入时可观察到的三态；只有 MISSING/EMPTY 可以进入持久证据扫描。 */
    private enum CatalogState {
        /** 路径目录项不存在，但仍需排除其它 existing 证据。 */
        MISSING,
        /** 普通文件存在但长度为零，可能是首次建库中断，也可能是 catalog 丢失。 */
        EMPTY,
        /** 非空普通文件，必须按 existing catalog 严格校验。 */
        PRESENT
    }

    /** 有界诊断类别；每类只记录一次，避免表数量影响启动探测内存。 */
    private enum PersistentArtifact {
        /** DD ID/version high-water control，存在即表明公共字典曾启动。 */
        DICTIONARY_CONTROL,
        /** 独立 clean snapshot/witness journal；catalog 缺失时本身就是 existing 实例证据。 */
        DICTIONARY_RECOVERY_MANIFEST,
        /** 兼容配置留下的单文件 redo；即使当前启用 ring 也不能忽略。 */
        SINGLE_FILE_REDO,
        /** 任一稳定命名 ring 文件，包括 partial ring。 */
        REDO_RING,
        /** checkpoint label control；单独存在也属于未完成或既有实例证据。 */
        REDO_CONTROL,
        /** 事务恢复高水位 sidecar；丢弃它可能导致事务身份复用。 */
        TRANSACTION_RECOVERY_CONTROL,
        /** 任意稳定命名系统 undo 表空间。 */
        UNDO_TABLESPACE,
        /** legacy 或双通道 doublewrite 持久文件。 */
        DOUBLEWRITE,
        /** 可能被 catalog-driven orphan cleanup 删除的受控命名表空间。 */
        CONTROLLED_TABLESPACE
    }
}
