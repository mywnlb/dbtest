package cn.zhangyis.db.storage.fsp.lifecycle;
import cn.zhangyis.db.storage.fil.state.TablespaceState;

import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * 从 raw page-0 读取 undo 生命周期头。启动期 loader 不经 Buffer Pool/MTR，故与仓储共享同一布局协议，
 * 但只做无副作用解码；magic 为零代表旧格式，其它未知值一律按元数据损坏处理。
 */
public final class TablespaceLifecycleRawCodec {

    private TablespaceLifecycleRawCodec() {
    }

    /**
     * 解码生命周期头。
     *
     * @param page 完整 page-0 字节。
     * @return 新格式头，旧格式返回 empty。
     */
    public static Optional<TablespaceLifecycleHeader> read(ByteBuffer page) {
        if (page == null) {
            throw new DatabaseValidationException("tablespace lifecycle page must not be null");
        }
        if (page.capacity() < SpaceHeaderLayout.LIFECYCLE_FINISH_STATE + Integer.BYTES) {
            throw new DatabaseValidationException("tablespace lifecycle page buffer too small: " + page.capacity());
        }
        int magic = page.getInt(SpaceHeaderLayout.LIFECYCLE_MAGIC);
        if (magic == 0) {
            return Optional.empty();
        }
        if (magic != TablespaceLifecycleFormat.MAGIC) {
            throw new FspMetadataException("invalid tablespace lifecycle magic: " + Integer.toHexString(magic));
        }
        int format = page.getInt(SpaceHeaderLayout.LIFECYCLE_FORMAT);
        if (format != TablespaceLifecycleFormat.VERSION) {
            throw new FspMetadataException("unsupported tablespace lifecycle format: " + format);
        }
        return Optional.of(new TablespaceLifecycleHeader(
                TablespaceState.fromPersistentCode(page.getInt(SpaceHeaderLayout.LIFECYCLE_STATE)),
                PageNo.of(page.getLong(SpaceHeaderLayout.LIFECYCLE_INITIAL_SIZE)),
                page.getLong(SpaceHeaderLayout.LIFECYCLE_EPOCH),
                PageNo.of(page.getLong(SpaceHeaderLayout.LIFECYCLE_TARGET_SIZE)),
                TablespaceState.fromPersistentCode(page.getInt(SpaceHeaderLayout.LIFECYCLE_FINISH_STATE))));
    }
}
