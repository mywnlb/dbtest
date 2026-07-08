package cn.zhangyis.db.storage.fil.io;
import cn.zhangyis.db.storage.fil.exception.DataFilePhysicalException;
import cn.zhangyis.db.storage.fil.exception.TablespaceNotOpenException;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link PageStore} 的 FileChannel 实现。维护 SpaceId 到 {@link DataFileHandle} 的映射，按 PageId 路由 positional IO。
 * registry-无关、state-无关：元数据由 create/open 注入，逻辑准入由上层负责（设计取舍 B′）。
 *
 * <p>并发：handles 用 ConcurrentHashMap 保护映射；单个表空间的物理生命周期与 IO 互斥由其 DataFileHandle 内的
 * 生命周期闩/文件大小锁负责。
 *
 * <p>简化点：mmap/预分配 adapter 未实现；单文件表空间，多文件跨文件路由属编排层。
 */
@Slf4j
public final class FileChannelPageStore implements PageStore {

    /**
     * 自动扩展策略；默认 MySQL 8.0 file-per-table 边界。
     */
    private final AutoExtendPolicy autoExtendPolicy;

    /**
     * data-file 物理范围初始化网关。默认零填充；测试可注入 recording/failing gateway 验证发布边界。
     */
    private final DataFileGateway dataFileGateway;

    /**
     * 已登记物理句柄。key 为表空间编号。
     */
    private final ConcurrentMap<SpaceId, DataFileHandle> handles = new ConcurrentHashMap<>();

    public FileChannelPageStore() {
        this(new DefaultIbdAutoExtendPolicy(), new ZeroFillDataFileGateway());
    }

    public FileChannelPageStore(AutoExtendPolicy autoExtendPolicy) {
        this(autoExtendPolicy, new ZeroFillDataFileGateway());
    }

    FileChannelPageStore(AutoExtendPolicy autoExtendPolicy, DataFileGateway dataFileGateway) {
        if (autoExtendPolicy == null || dataFileGateway == null) {
            throw new DatabaseValidationException("page store dependencies must not be null");
        }
        this.autoExtendPolicy = autoExtendPolicy;
        this.dataFileGateway = dataFileGateway;
    }

    @Override
    public void create(SpaceId spaceId, Path path, PageSize pageSize, PageNo initialSizeInPages) {
        validateSpaceId(spaceId);
        // 双重检查：containsKey 是无竞争快路径（避免无谓建文件），putIfAbsent 才是并发下的原子登记守卫。
        if (handles.containsKey(spaceId)) {
            throw new DatabaseValidationException("tablespace already registered: " + spaceId.value());
        }
        DataFileHandle handle = DataFileHandle.create(spaceId, path, pageSize, initialSizeInPages, dataFileGateway);
        if (handles.putIfAbsent(spaceId, handle) != null) {
            // 输给并发 create 的一方：关闭句柄并删除自己刚 CREATE_NEW 出来的孤儿文件，避免磁盘残留无人登记的 .ibd。
            // 删除失败不掩盖“重复登记”根因，仅告警。
            handle.close();
            try {
                Files.deleteIfExists(path);
            } catch (IOException deleteError) {
                log.warn("failed to delete orphan data file after lost create race: {}", path, deleteError);
            }
            throw new DatabaseValidationException("tablespace already registered: " + spaceId.value());
        }
        log.info("created tablespace data file: space={} path={}", spaceId.value(), path);
    }

    @Override
    public void open(SpaceId spaceId, Path path, PageSize pageSize) {
        validateSpaceId(spaceId);
        // 同 create 的双重检查；open 的输方只关闭已存在文件的句柄，文件本就该在，无需删除。
        if (handles.containsKey(spaceId)) {
            throw new DatabaseValidationException("tablespace already registered: " + spaceId.value());
        }
        DataFileHandle handle = DataFileHandle.open(spaceId, path, pageSize, dataFileGateway);
        if (handles.putIfAbsent(spaceId, handle) != null) {
            handle.close();
            throw new DatabaseValidationException("tablespace already registered: " + spaceId.value());
        }
        log.info("opened tablespace data file: space={} path={}", spaceId.value(), path);
    }

    @Override
    public void readPage(PageId pageId, ByteBuffer dst) {
        validatePageId(pageId);
        require(pageId.spaceId()).readPage(pageId.pageNo(), dst);
    }

    @Override
    public void writePage(PageId pageId, ByteBuffer src) {
        validatePageId(pageId);
        require(pageId.spaceId()).writePage(pageId.pageNo(), src);
    }

    @Override
    public PageNo extend(SpaceId spaceId) {
        validateSpaceId(spaceId);
        return PageNo.of(require(spaceId).autoExtend(autoExtendPolicy));
    }

    @Override
    public PageNo currentSizeInPages(SpaceId spaceId) {
        validateSpaceId(spaceId);
        return PageNo.of(require(spaceId).currentSizeInPages());
    }

    @Override
    public Path pathOf(SpaceId spaceId) {
        validateSpaceId(spaceId);
        return require(spaceId).path();
    }

    @Override
    public void force(SpaceId spaceId) {
        validateSpaceId(spaceId);
        require(spaceId).force();
    }

    /**
     * force 全部已打开句柄。即使某个 force 失败也继续 force 其余，最后汇总抛出，避免部分句柄漏刷。
     * 先对 values 做快照再遍历，使语义确定、不受并发 create/open 影响。
     */
    @Override
    public void forceAll() {
        List<RuntimeException> errors = new ArrayList<>();
        for (DataFileHandle handle : new ArrayList<>(handles.values())) {
            try {
                handle.force();
            } catch (RuntimeException e) {
                errors.add(e);
            }
        }
        if (!errors.isEmpty()) {
            DataFilePhysicalException aggregate = new DataFilePhysicalException(
                    "failed to force " + errors.size() + " tablespace handle(s)", errors.get(0));
            errors.subList(1, errors.size()).forEach(aggregate::addSuppressed);
            throw aggregate;
        }
    }

    /** 将经过上层校验的单文件截断请求路由到目标物理句柄。 */
    @Override
    public void truncate(SpaceId spaceId, PageNo targetSizeInPages) {
        validateSpaceId(spaceId);
        require(spaceId).truncateTo(targetSizeInPages);
    }

    /** 将经过上层校验的“扩到至少 N”请求路由到目标物理句柄（幂等，只增不减）。 */
    @Override
    public void ensureCapacity(SpaceId spaceId, PageNo minSizeInPages) {
        validateSpaceId(spaceId);
        require(spaceId).ensureCapacity(minSizeInPages);
    }

    @Override
    public void close(SpaceId spaceId) {
        validateSpaceId(spaceId);
        DataFileHandle handle = handles.remove(spaceId);
        if (handle != null) {
            handle.close();
        }
    }

    /**
     * 关闭全部句柄。即使某个句柄关闭失败也继续关闭其余，最后汇总抛出，避免半关闭泄漏其它 FileChannel。
     * 先对 key 做快照再遍历，使关闭语义确定、不受并发 create/open 影响；单个 close(spaceId) 内 remove 后 close
     * 避免并发重复关闭同一句柄。
     */
    @Override
    public void close() {
        List<RuntimeException> errors = new ArrayList<>();
        for (SpaceId spaceId : new ArrayList<>(handles.keySet())) {
            try {
                close(spaceId);
            } catch (RuntimeException e) {
                errors.add(e);
            }
        }
        if (!errors.isEmpty()) {
            DataFilePhysicalException aggregate = new DataFilePhysicalException(
                    "failed to close " + errors.size() + " tablespace handle(s)", errors.get(0));
            errors.subList(1, errors.size()).forEach(aggregate::addSuppressed);
            throw aggregate;
        }
    }

    private DataFileHandle require(SpaceId spaceId) {
        DataFileHandle handle = handles.get(spaceId);
        if (handle == null) {
            throw new TablespaceNotOpenException("tablespace not open: " + spaceId.value());
        }
        return handle;
    }

    private void validatePageId(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
    }

    private void validateSpaceId(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
    }
}
