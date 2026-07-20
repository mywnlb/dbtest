package cn.zhangyis.db.storage.fil.catalog;

import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogStore;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogCorruptionException;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogPersistenceException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

/**
 * 字典灾难恢复 manifest 的独立物理 journal 适配器。
 *
 * <p>它复用 catalog 已验证的双 header、frame CRC、batch SHA 和 committed-length 协议，但使用
 * ASCII {@code MINIDDRM} 的独立文件魔数。这样两个文件即使拥有相同批次形状，也不能被错误地
 * 作为对方打开；DD 事件语义仍由上层 manifest codec 解释。</p>
 */
public final class FileDictionaryRecoveryManifestStore implements InternalCatalogStore {

    /** ASCII {@code MINIDDRM}，与 {@code mysql.ibd} 的 {@code MINIDDIB} 明确区分。 */
    private static final long MANIFEST_MAGIC = 0x4D_49_4E_49_44_44_52_4DL;

    /** 物理批次存储委托；所有资源所有权由本适配器接管。 */
    private final FileInternalCatalogStore delegate;

    private FileDictionaryRecoveryManifestStore(FileInternalCatalogStore delegate) {
        this.delegate = delegate;
    }

    /**
     * 仅在路径明确缺失时新建空 manifest；任何已存在目录项都严格按 existing journal 恢复。
     * 因而零长度文件不会被原地初始化覆盖，而会作为损坏证据交给上层保留流程。
     *
     * @param path 固定的 {@code mysql.dd.manifest} 路径；不得为 {@code null}
     * @return 已持有独立 manifest channel 的 store
     */
    public static FileDictionaryRecoveryManifestStore openOrCreate(Path path) {
        boolean exists = requireRegularOrMissing(path, true);
        return new FileDictionaryRecoveryManifestStore(
                exists
                        ? FileInternalCatalogStore.openExisting(path, MANIFEST_MAGIC)
                        : FileInternalCatalogStore.openOrCreate(path, MANIFEST_MAGIC));
    }

    /**
     * 严格打开已有 manifest；缺失、魔数错误、双头损坏或 committed frame 损坏都 fail-closed。
     *
     * @param path 已存在的 manifest 路径
     * @return 已恢复的 manifest store
     */
    public static FileDictionaryRecoveryManifestStore openExisting(Path path) {
        requireRegularOrMissing(path, false);
        return new FileDictionaryRecoveryManifestStore(
                FileInternalCatalogStore.openExisting(path, MANIFEST_MAGIC));
    }

    /**
     * 使用 NOFOLLOW 属性拒绝 symlink/目录/device，并显式区分 regular existing 与 missing。
     *
     * @param path 固定 manifest 路径
     * @param allowMissing 是否允许明确缺失，供 create 分支使用
     * @return 目录项存在且为 regular file 时为 {@code true}；允许且明确缺失时为 {@code false}
     */
    private static boolean requireRegularOrMissing(Path path, boolean allowMissing) {
        if (path == null) {
            return false;
        }
        try {
            BasicFileAttributes attributes =
                    Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (attributes.isSymbolicLink() || !attributes.isRegularFile()) {
                throw new InternalCatalogCorruptionException(
                        "dictionary recovery manifest is not a regular file: " + path);
            }
            return true;
        } catch (NoSuchFileException missing) {
            if (!allowMissing) {
                throw new InternalCatalogPersistenceException(
                        "dictionary recovery manifest does not exist: " + path, missing);
            }
            return false;
        } catch (IOException failure) {
            throw new InternalCatalogPersistenceException(
                    "inspect dictionary recovery manifest path failed: " + path, failure);
        }
    }

    /**
     * 将一个完整 manifest event 的分片作为单个 durable batch 追加。
     *
     * @param records 已由 DD manifest codec 排序并编码的非空分片
     * @return 底层 journal 分配的严格递增 committed sequence
     */
    @Override
    public long append(List<CatalogRecord> records) {
        return delegate.append(records);
    }

    /**
     * 返回 committed-length 边界内已经通过 frame CRC 与 batch SHA 校验的全部事件批次。
     *
     * @return 不包含崩溃尾部的不可变 committed batch 快照
     */
    @Override
    public List<CatalogBatch> readCommittedBatches() {
        return delegate.readCommittedBatches();
    }

    /**
     * 返回当前双 header 已发布的物理 committed 边界。
     *
     * @return 从文件起点计数的非负 committed 字节位置
     */
    @Override
    public long committedLength() {
        return delegate.committedLength();
    }

    /** 关闭本适配器拥有的 manifest channel；关闭后不能再读写。 */
    @Override
    public void close() {
        delegate.close();
    }
}
