package cn.zhangyis.db.storage.flush.doublewrite;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * FlushList/LRU 双物理 doublewrite 文件的组合仓储。
 *
 * <p>每个通道拥有独立 slot 游标和锁；批次只在所属文件内 append/force/release，恢复扫描则合并两个文件的
 * 有效证据。该类不持有 Buffer Pool frame，也不在文件锁内回调上层对象。
 */
public final class DoublewriteChannel implements AutoCloseable {

    /**
     * 本对象拥有的 {@code repositories} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
     */
    private final EnumMap<DoublewriteChannelId, DoublewriteFileRepository> repositories;

    private DoublewriteChannel(EnumMap<DoublewriteChannelId, DoublewriteFileRepository> repositories) {
        this.repositories = repositories;
    }

    /**
     * 打开两个可写双写文件。
     *
     * @param flushListPath FlushList 文件路径
     * @param lruPath LRU 文件路径
     * @param pageSize 实例页大小
     * @return 双通道仓储
     */
    public static DoublewriteChannel open(Path flushListPath, Path lruPath, PageSize pageSize) {
        return open(flushListPath, lruPath, pageSize, 1024);
    }

    /** 测试用可配置 slot 数量的双通道打开入口。
     *
     * @param flushListPath 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param lruPath 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param slotCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code open} 产生的恢复或持久化阶段对象；成功时不为 {@code null}，其中的 durable 边界不超过已安全完成的工作
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static DoublewriteChannel open(Path flushListPath, Path lruPath, PageSize pageSize, int slotCount) {
        if (flushListPath == null || lruPath == null || pageSize == null) {
            throw new DatabaseValidationException("doublewrite channel paths/page size must not be null");
        }
        EnumMap<DoublewriteChannelId, DoublewriteFileRepository> opened =
                new EnumMap<>(DoublewriteChannelId.class);
        try {
            opened.put(DoublewriteChannelId.FLUSH_LIST,
                    DoublewriteFileRepository.open(flushListPath, pageSize, slotCount));
            opened.put(DoublewriteChannelId.LRU,
                    DoublewriteFileRepository.open(lruPath, pageSize, slotCount));
            return new DoublewriteChannel(opened);
        } catch (RuntimeException failure) {
            opened.values().forEach(repository -> {
                try {
                    repository.close();
                } catch (RuntimeException ignored) {
                    failure.addSuppressed(ignored);
                }
            });
            throw failure;
        }
    }

    /** 只读恢复扫描入口；缺失的物理文件保持为空，不创建新文件。
     *
     * @param flushListPath 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param lruPath 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @return {@code openReadOnly} 产生的恢复或持久化阶段对象；成功时不为 {@code null}，其中的 durable 边界不超过已安全完成的工作
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static DoublewriteChannel openReadOnly(Path flushListPath, Path lruPath, PageSize pageSize) {
        if (flushListPath == null || lruPath == null || pageSize == null) {
            throw new DatabaseValidationException("doublewrite channel paths/page size must not be null");
        }
        EnumMap<DoublewriteChannelId, DoublewriteFileRepository> opened =
                new EnumMap<>(DoublewriteChannelId.class);
        opened.put(DoublewriteChannelId.FLUSH_LIST,
                DoublewriteFileRepository.openReadOnlyIfExists(flushListPath, pageSize).orElse(null));
        opened.put(DoublewriteChannelId.LRU,
                DoublewriteFileRepository.openReadOnlyIfExists(lruPath, pageSize).orElse(null));
        return new DoublewriteChannel(opened);
    }

    /** 返回指定物理通道的仓储；仅供 flush strategy 和恢复 scanner 使用。
     *
     * @param channel 参与 {@code repository} 的稳定领域标识 {@code DoublewriteChannelId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code repository} 产生的恢复或持久化阶段对象；成功时不为 {@code null}，其中的 durable 边界不超过已安全完成的工作
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws cn.zhangyis.db.common.exception.DatabaseRuntimeException 可恢复的数据库运行期协作失败时抛出；调用方应依据当前事务状态选择回滚、重试或关闭资源
     */
    public DoublewriteFileRepository repository(DoublewriteChannelId channel) {
        if (channel == null) {
            throw new DatabaseValidationException("doublewrite channel id must not be null");
        }
        DoublewriteFileRepository repository = repositories.get(channel);
        if (repository == null) {
            throw new cn.zhangyis.db.common.exception.DatabaseRuntimeException(
                    "doublewrite channel file is not open: " + channel);
        }
        return repository;
    }

    /** 在指定文件内追加 full-copy 批次。
     *
     * @param channel 参与 {@code appendBatch} 的稳定领域标识 {@code DoublewriteChannelId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param batch 恢复、checkpoint、doublewrite 或刷脏阶段的协作状态；不得为 {@code null}，阶段和持久化边界必须与当前实例的恢复状态机一致
     */
    public void appendBatch(DoublewriteChannelId channel, DoublewriteBatch batch) {
        repository(channel).appendBatch(batch);
    }

    /** 在指定文件内追加 detect-only metadata 批次。
     *
     * @param channel 参与 {@code appendDetectOnlyBatch} 的稳定领域标识 {@code DoublewriteChannelId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param batch 恢复、checkpoint、doublewrite 或刷脏阶段的协作状态；不得为 {@code null}，阶段和持久化边界必须与当前实例的恢复状态机一致
     */
    public void appendDetectOnlyBatch(DoublewriteChannelId channel, DoublewriteBatch batch) {
        repository(channel).appendDetectOnlyBatch(batch);
    }

    /** force 指定物理文件，不能跨两个文件合并 force 语义。
     *
     * @param channel 参与 {@code force} 的稳定领域标识 {@code DoublewriteChannelId}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    public void force(DoublewriteChannelId channel) {
        repository(channel).force();
    }

    /** 释放指定文件内的批次 reservation，但不擦除磁盘副本。
     *
     * @param channel 参与 {@code releaseBatch} 的稳定领域标识 {@code DoublewriteChannelId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param batch 恢复、checkpoint、doublewrite 或刷脏阶段的协作状态；不得为 {@code null}，阶段和持久化边界必须与当前实例的恢复状态机一致
     */
    public void releaseBatch(DoublewriteChannelId channel, DoublewriteBatch batch) {
        repository(channel).releaseBatch(batch);
    }

    /** 合并两个物理文件中的有效页号，保持通道枚举顺序稳定。
     *
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     */
    public List<PageId> pageIds() {
        Set<PageId> ids = new LinkedHashSet<>();
        for (DoublewriteChannelId channel : DoublewriteChannelId.values()) {
            DoublewriteFileRepository repository = repositories.get(channel);
            if (repository != null) {
                ids.addAll(repository.pageIds());
            }
        }
        return List.copyOf(ids);
    }

    /** 返回指定通道的有效 slot 摘要；缺失的只读文件视为空。
     *
     * @param channel 参与 {@code scanEntries} 的稳定领域标识 {@code DoublewriteChannelId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     */
    public List<DoublewriteSlotEntry> scanEntries(DoublewriteChannelId channel) {
        DoublewriteFileRepository repository = repositories.get(channel);
        return repository == null ? List.of() : repository.scanEntries();
    }

    /**
     * 从两个文件中选择目标页最新有效 full-copy；同 LSN 的副本必须字节相同，否则拒绝恢复。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return 当前可见的最近快照或持久边界；尚未产生对应状态时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws cn.zhangyis.db.common.exception.DatabaseRuntimeException 可恢复的数据库运行期协作失败时抛出；调用方应依据当前事务状态选择回滚、重试或关闭资源
     */
    public Optional<byte[]> latestCopy(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        byte[] selected = null;
        long selectedLsn = Long.MIN_VALUE;
        for (DoublewriteChannelId channel : DoublewriteChannelId.values()) {
            DoublewriteFileRepository repository = repositories.get(channel);
            if (repository == null) {
                continue;
            }
            Optional<byte[]> candidate = repository.latestCopy(pageId);
            if (candidate.isEmpty()) {
                continue;
            }
            byte[] bytes = candidate.orElseThrow();
            long lsn = repository.latestCopyLsn(pageId).orElse(Long.MIN_VALUE);
            if (selected == null || lsn > selectedLsn) {
                selected = bytes;
                selectedLsn = lsn;
            } else if (lsn == selectedLsn && !java.util.Arrays.equals(selected, bytes)) {
                throw new cn.zhangyis.db.common.exception.DatabaseRuntimeException(
                        "conflicting doublewrite copies for page " + pageId + " at pageLSN " + lsn);
            }
        }
        return selected == null ? Optional.empty() : Optional.of(selected.clone());
    }

    /** 返回两个物理文件中目标页最新 full-copy 的 pageLSN。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return 当前可见的最近快照或持久边界；尚未产生对应状态时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public OptionalLong latestCopyLsn(PageId pageId) {
        long selected = Long.MIN_VALUE;
        for (DoublewriteChannelId channel : DoublewriteChannelId.values()) {
            DoublewriteFileRepository repository = repositories.get(channel);
            if (repository != null) {
                selected = Math.max(selected, repository.latestCopyLsn(pageId).orElse(Long.MIN_VALUE));
            }
        }
        return selected == Long.MIN_VALUE ? OptionalLong.empty() : OptionalLong.of(selected);
    }

    /**
     * 释放本方法拥有的脏页刷盘与 checkpoint资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     */
    @Override
    public void close() {
        List<RuntimeException> failures = new ArrayList<>();
        repositories.values().forEach(repository -> {
            if (repository == null) {
                return;
            }
            try {
                repository.close();
            } catch (RuntimeException failure) {
                failures.add(failure);
            }
        });
        if (!failures.isEmpty()) {
            RuntimeException first = failures.remove(0);
            failures.forEach(first::addSuppressed);
            throw first;
        }
    }
}
