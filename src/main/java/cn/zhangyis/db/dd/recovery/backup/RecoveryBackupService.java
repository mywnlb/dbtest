package cn.zhangyis.db.dd.recovery.backup;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.storage.api.ddl.RecoveryBackupFile;
import cn.zhangyis.db.storage.api.ddl.TableDdlStorageService;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.UUID;

/**
 * DD 与 storage 之间的可信恢复备份协调器。它懒加载实例签名身份，固定 archive/incoming 路径，且只在
 * 数据文件已稳定发布后原子写 manifest；普通 DatabaseEngine startup 构造本对象不会触发 identity IO。
 */
public final class RecoveryBackupService {

    /** 实例根，用于派生 recovery-backups 与 tablespace-transfer/recovery-incoming。 */
    private final Path baseDirectory;
    /** 负责 drain、稳定复制、离线 page0 标记和只读 page0 inspection 的 storage facade。 */
    private final TableDdlStorageService physical;
    /** 懒读取/创建且带 CRC 的实例 UUID/HMAC key 仓储。 */
    private final RecoveryIdentityStore identityStore;
    /** 稳定 manifest codec 与 HMAC 协作者。 */
    private final RecoveryBackupManifestCodec manifestCodec =
            new RecoveryBackupManifestCodec();

    /**
     * @param baseDirectory 当前数据库实例根目录
     * @param physical 与当前 StorageEngine 共享的物理 DDL facade
     */
    public RecoveryBackupService(
            Path baseDirectory, TableDdlStorageService physical) {
        if (baseDirectory == null || physical == null) {
            throw new DatabaseValidationException(
                    "recovery backup base directory/physical facade must not be null");
        }
        this.baseDirectory = baseDirectory.toAbsolutePath().normalize();
        this.physical = physical;
        this.identityStore = new RecoveryIdentityStore(this.baseDirectory);
    }

    /**
     * 从锁内 ACTIVE aggregate 创建 archive pair。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证 ACTIVE/binding 后懒创建或读取实例 recovery identity，生成单次 backup UUID。</li>
     *     <li>把固定 archive 路径交给 storage，在 SpaceId X lease 内 drain、copy、写 DISCARDED page0 并 force。</li>
     *     <li>对不含 state/version 的 table definition 计算 SHA-256，与物理 clean LSN/hash 共同组成 manifest。</li>
     *     <li>使用实例 HMAC key 签名并最后原子发布 manifest，返回不可变 pair；无 manifest 的残留数据不能导入。</li>
     * </ol>
     *
     * @param table MDL X 下重读且必须处于 ACTIVE 的完整表聚合
     * @param timeout drain 与物理 lease 共用的正上界
     * @return archive 数据与签名 manifest 路径
     * @throws RecoveryBackupException identity、文件或 manifest 无法稳定发布时抛出
     */
    public RecoveryBackupArtifact createBackup(
            TableDefinition table, Duration timeout) {
        // 1. 只允许在线健康对象产生 clean backup，隔离/待处理状态不得被签名为可信源。
        validateTableTimeout(table, timeout);
        if (table.state() != TableState.ACTIVE) {
            throw new RecoveryBackupException(
                    "recovery backup source table is not ACTIVE: " + table.id().value());
        }
        TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                new RecoveryBackupException(
                        "ACTIVE recovery backup source has no binding: " + table.id().value()));
        RecoveryIdentity identity = identityStore.openOrCreate();
        UUID backupId = UUID.randomUUID();
        Path archiveDirectory = archiveDirectory(table, binding);
        Path dataPath = archiveDirectory.resolve(backupId + ".ibd");
        Path manifestPath = archiveDirectory.resolve(backupId + ".manifest");

        // 2. storage 返回前数据文件已经 page0 DISCARDED、force、原子发布并完成整文件 hash。
        RecoveryBackupFile file = physical.createRecoveryBackupFile(
                binding, dataPath, timeout);

        // 3. manifest 绑定源版本用于审计，但定义 hash 刻意排除 lifecycle/version，允许隔离后精确比较。
        RecoveryBackupManifest unsigned = new RecoveryBackupManifest(
                RecoveryBackupManifest.CURRENT_FORMAT, backupId, identity.instanceId(),
                table.id(), binding.spaceId(), table.version(), file.identity(),
                tableDefinitionSha256(table), file.cleanLsn(), file.lengthBytes(),
                file.sha256(), "");

        // 4. manifest 是备份完成标志，必须晚于数据文件；任何残留无签名数据均不被 validateIncoming 接受。
        RecoveryBackupManifest signed = manifestCodec.sign(unsigned, identity);
        manifestCodec.writeAtomic(manifestPath, signed);
        return new RecoveryBackupArtifact(dataPath, manifestPath, signed);
    }

    /**
     * 验证管理员放入固定 recovery-incoming 目录的 pair；成功前不创建 DDL marker、不复制 canonical 文件。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>要求目标为 RECOVERY_DISCARDED，并从固定 table/space 文件名派生 data/manifest 路径。</li>
     *     <li>读取已有 recovery identity，解码 manifest 并验证 source UUID/HMAC、table/space/定义摘要。</li>
     *     <li>校验数据文件无符号链接、长度与整文件 SHA-256 精确匹配 manifest。</li>
     *     <li>由 storage inspector 重读 DISCARDED page0，匹配完整 file identity 后返回可用于 op=10 marker 的证据。</li>
     * </ol>
     *
     * @param table MDL X 下重读的 RECOVERY_DISCARDED aggregate
     * @return 已完成全部密码学与物理身份校验的固定 incoming pair
     * @throws RecoveryBackupException 任一证据缺失、损坏、错配或来源不可验证时抛出
     */
    public ValidatedRecoveryBackup validateIncoming(TableDefinition table) {
        // 1. ordinary DISCARDED 不允许绕过旧 IMPORT 规则进入本可信恢复入口。
        if (table == null || table.state() != TableState.RECOVERY_DISCARDED) {
            throw new RecoveryBackupException(
                    "trusted recovery import requires RECOVERY_DISCARDED table");
        }
        TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                new RecoveryBackupException(
                        "RECOVERY_DISCARDED table has no binding: " + table.id().value()));
        Path dataPath = incomingDataPath(table, binding);
        Path manifestPath = incomingManifestPath(table, binding);
        requireRegularNoLink(dataPath, "recovery backup data");
        requireRegularNoLink(manifestPath, "recovery backup manifest");

        // 2. key 丢失绝不能生成新 identity；旧签名必须继续 fail-closed。
        RecoveryIdentity identity = identityStore.openExisting();
        RecoveryBackupManifest manifest = manifestCodec.read(manifestPath);
        manifestCodec.verify(manifest, identity);
        if (!manifest.tableId().equals(table.id())
                || !manifest.spaceId().equals(binding.spaceId())
                || manifest.sourceDictionaryVersion().value() > table.version().value()
                || !manifest.tableDefinitionSha256().equals(tableDefinitionSha256(table))) {
            throw new RecoveryBackupException(
                    "recovery backup manifest does not match current table definition/identity");
        }

        // 3. 签名只证明 manifest 来源，仍须把实际 incoming bytes 与签名 hash/length 绑定。
        try {
            if (Files.size(dataPath) != manifest.fileLengthBytes()
                    || !sha256(dataPath).equals(manifest.fileSha256())) {
                throw new RecoveryBackupException(
                        "recovery backup data length/SHA-256 mismatch: " + dataPath);
            }
        } catch (IOException failure) {
            throw new RecoveryBackupException(
                    "read recovery backup data evidence failed: " + dataPath, failure);
        }

        // 4. page0 必须是本引擎备份写出的 DISCARDED GENERAL 文件，不能只靠文件 hash 猜测挂载语义。
        TablespaceFileIdentity actual = physical.inspectTablespaceFile(
                dataPath, binding.spaceId());
        if (!actual.equals(manifest.fileIdentity())) {
            throw new RecoveryBackupException(
                    "recovery backup page0 identity does not match manifest");
        }
        return new ValidatedRecoveryBackup(
                dataPath, manifestPath, actual, manifest);
    }

    /** @return 当前表固定 incoming 数据文件路径，供管理员 staging 与测试定位。 */
    public Path incomingDataPath(TableDefinition table) {
        if (table == null || table.storageBinding().isEmpty()) {
            throw new DatabaseValidationException(
                    "incoming recovery backup path requires table binding");
        }
        return incomingDataPath(table, table.storageBinding().orElseThrow());
    }

    /** @return 当前表固定 incoming manifest 路径，供管理员 staging 与测试定位。 */
    public Path incomingManifestPath(TableDefinition table) {
        if (table == null || table.storageBinding().isEmpty()) {
            throw new DatabaseValidationException(
                    "incoming recovery manifest path requires table binding");
        }
        return incomingManifestPath(table, table.storageBinding().orElseThrow());
    }

    /** archive 子目录同时带 table/space identity，避免跨对象误操作。 */
    private Path archiveDirectory(
            TableDefinition table, TableStorageBinding binding) {
        return baseDirectory.resolve("recovery-backups").resolve(
                "table_" + table.id().value() + "_space_" + binding.spaceId().value());
    }

    /** 固定 incoming data 路径不包含管理员输入片段。 */
    private Path incomingDataPath(
            TableDefinition table, TableStorageBinding binding) {
        return incomingRoot().resolve(fileStem(table, binding) + ".ibd");
    }

    /** 固定 incoming manifest 与 data 仅扩展名不同。 */
    private Path incomingManifestPath(
            TableDefinition table, TableStorageBinding binding) {
        return incomingRoot().resolve(fileStem(table, binding) + ".manifest");
    }

    /** @return 实例唯一 recovery-incoming 目录。 */
    private Path incomingRoot() {
        return baseDirectory.resolve("tablespace-transfer").resolve("recovery-incoming");
    }

    /** 把 stable ids 编入文件名，禁止表 A 的 pair 被表 B 入口消费。 */
    private static String fileStem(
            TableDefinition table, TableStorageBinding binding) {
        return "table_" + table.id().value() + "_space_" + binding.spaceId().value();
    }

    /** 基础 ACTIVE/timeout 校验必须早于 recovery identity 或文件创建。 */
    private static void validateTableTimeout(
            TableDefinition table, Duration timeout) {
        if (table == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException(
                    "recovery backup requires table and positive timeout");
        }
    }

    /** incoming 证据必须是普通文件且最终路径不是符号链接。 */
    private static void requireRegularNoLink(Path path, String role) {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                || Files.isSymbolicLink(path)) {
            throw new RecoveryBackupException(role + " is missing or unsafe: " + path);
        }
    }

    /**
     * 形成跨 lifecycle/version 稳定的表定义摘要；路径和 DD 状态不属于数据格式，table/space/index/segment
     * identity、列/索引/options 与 row-format 才是 replacement 必须精确匹配的契约。
     */
    private static String tableDefinitionSha256(TableDefinition table) {
        TableStorageBinding binding = table.storageBinding().orElseThrow();
        String canonical = table.id().value() + "|" + table.schemaId().value()
                + "|" + table.name().canonicalName() + "|" + table.columns()
                + "|" + table.indexes() + "|" + table.options()
                + "|" + binding.tableId() + "|" + binding.spaceId().value()
                + "|" + binding.rowFormatVersion() + "|" + binding.indexes()
                + "|" + binding.lobSegment();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(
                    digest.digest(canonical.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException failure) {
            throw new RecoveryBackupException(
                    "SHA-256 is unavailable for table definition digest", failure);
        }
    }

    /** 流式计算 incoming 数据文件摘要，避免大型表空间一次性载入堆。 */
    private static String sha256(Path path) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(path)) {
                byte[] buffer = new byte[64 * 1024];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException | IOException failure) {
            throw new RecoveryBackupException(
                    "compute recovery backup SHA-256 failed: " + path, failure);
        }
    }
}
