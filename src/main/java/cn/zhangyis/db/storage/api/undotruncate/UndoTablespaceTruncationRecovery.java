package cn.zhangyis.db.storage.api.undotruncate;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteRecoveryScanner;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderPhysical;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRawCodec;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleRawCodec;
import cn.zhangyis.db.storage.recovery.UndoTablespaceRecoveryParticipant;
import cn.zhangyis.db.storage.redo.RedoLogManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link UndoTablespaceRecoveryParticipant} 的生产实现。
 *
 * <p>构造时必须显式给出配置的 undo SpaceId 集合，不做文件 discovery。doublewrite 前逐个修复 page0 并读取
 * TRUNCATING target；已准备的 page0 不重复扫描，且 target 之后尾页禁止恢复。redo replay 后再次 raw 扫描全部配置空间，
 * 以发现“marker redo durable 但 page0 尚未刷出”的崩溃窗口；随后安装 recovered LSN 并调用截断服务续作。
 */
public final class UndoTablespaceTruncationRecovery implements UndoTablespaceRecoveryParticipant {

    private final List<SpaceId> configuredUndoSpaces;
    private final PageStore pageStore;
    private final PageSize pageSize;
    private final TablespaceRegistry registry;
    private final RedoLogManager redo;
    private final UndoTablespaceTruncationService truncationService;

    /** prepare/rescan 生成的 durable TRUNCATING marker；RecoveryService 单线程调用，无需额外锁。 */
    private final Map<SpaceId, TablespaceLifecycleHeader> truncating = new HashMap<>();
    /** doublewrite prepare 已处理的 page0，普通扫描必须跳过，避免修复计数重复和 marker 读取后再次改页。 */
    private final Set<PageId> preparedPageZeros = new HashSet<>();

    public UndoTablespaceTruncationRecovery(
            Set<SpaceId> configuredUndoSpaces,
            PageStore pageStore,
            PageSize pageSize,
            TablespaceRegistry registry,
            RedoLogManager redo,
            UndoTablespaceTruncationService truncationService) {
        if (configuredUndoSpaces == null || pageStore == null || pageSize == null || registry == null
                || redo == null || truncationService == null) {
            throw new DatabaseValidationException("undo truncation recovery dependencies must not be null");
        }
        if (configuredUndoSpaces.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("configured undo space id must not be null");
        }
        this.configuredUndoSpaces = configuredUndoSpaces.stream()
                .sorted(Comparator.comparingInt(SpaceId::value))
                .toList();
        this.pageStore = pageStore;
        this.pageSize = pageSize;
        this.registry = registry;
        this.redo = redo;
        this.truncationService = truncationService;
    }

    /**
     * 先逐个处理配置空间 page0，再建立 tail filter。任何配置空间未打开、类型不是 UNDO 或页头损坏都会 fail closed。
     */
    @Override
    public int prepareDoublewrite(DoublewriteRecoveryScanner scanner) {
        truncating.clear();
        preparedPageZeros.clear();
        int repaired = 0;
        for (SpaceId spaceId : configuredUndoSpaces) {
            // pathOf 是显式“已打开”校验；不允许静默跳过配置空间，否则可能带未续作 marker 开放流量。
            pageStore.pathOf(spaceId);
            PageId page0 = PageId.of(spaceId, PageNo.of(0));
            if (scanner != null && scanner.repairPageIfNeeded(page0)) {
                repaired++;
            }
            preparedPageZeros.add(page0);
            readAndRegister(spaceId);
        }
        return repaired;
    }

    /** page0 已准备则跳过；TRUNCATING 空间的 target 之后页属于已声明丢弃尾部，不允许 doublewrite 复活。 */
    @Override
    public boolean shouldRepairDoublewritePage(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("doublewrite candidate page id must not be null");
        }
        if (preparedPageZeros.contains(pageId)) {
            return false;
        }
        TablespaceLifecycleHeader marker = truncating.get(pageId.spaceId());
        return marker == null || pageId.pageNo().value() < marker.targetSizeInPages().value();
    }

    /**
     * 安装 redo 完整边界后重新扫描 page0，并按 SpaceId 顺序续作。重扫可发现 prepare 时尚未刷到 page0、
     * 但刚由 redo replay 写出的 TRUNCATING marker。
     */
    @Override
    public void resumeAfterRedo(Lsn recoveredToLsn) {
        redo.restoreRecoveredBoundary(recoveredToLsn);
        truncating.clear();
        for (SpaceId spaceId : configuredUndoSpaces) {
            readAndRegister(spaceId);
        }
        List<Map.Entry<SpaceId, TablespaceLifecycleHeader>> work = new ArrayList<>(truncating.entrySet());
        work.sort(Map.Entry.comparingByKey(Comparator.comparingInt(SpaceId::value)));
        for (Map.Entry<SpaceId, TablespaceLifecycleHeader> entry : work) {
            truncationService.truncate(entry.getKey(), entry.getValue().finishState());
        }
    }

    private void readAndRegister(SpaceId spaceId) {
        ByteBuffer page0 = ByteBuffer.allocate(pageSize.bytes());
        pageStore.readPage(PageId.of(spaceId, PageNo.of(0)), page0);
        SpaceHeaderPhysical physical = SpaceHeaderRawCodec.readPhysical(page0);
        if (!physical.spaceId().equals(spaceId)) {
            throw new UndoTablespaceTruncationException("configured UNDO page0 space id mismatch: expected="
                    + spaceId.value() + ", actual=" + physical.spaceId().value());
        }
        TablespaceType type = TablespaceTypeFlags.decode(physical.spaceFlags());
        if (type != TablespaceType.UNDO) {
            throw new UndoTablespaceTruncationException("configured undo SpaceId is not UNDO on disk: "
                    + spaceId.value() + " type=" + type);
        }
        TablespaceLifecycleRawCodec.read(page0).ifPresent(lifecycle -> {
            if (lifecycle.state() == TablespaceState.TRUNCATING) {
                truncating.put(spaceId, lifecycle);
            }
        });
        // raw page0 已验证后刷新 Registry，使截断服务和最终普通准入观察同一磁盘状态。
        registry.refresh(spaceId);
    }
}
