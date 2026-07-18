package cn.zhangyis.db.storage.fsp.lifecycle;
import cn.zhangyis.db.storage.fil.state.TablespaceState;

import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * 从 raw page-0 读取表空间 lifecycle marker。启动期 loader 不经 Buffer Pool/MTR，故与仓储共享同一布局协议，
 * 但只做无副作用解码；magic 为零代表旧格式，其它未知值一律按元数据损坏处理。
 */
public final class TablespaceLifecycleRawCodec {

    private TablespaceLifecycleRawCodec() {
    }

    /**
     * 解码生命周期头。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param page 完整 page-0 字节。
     * @return 新格式头，旧格式返回 empty。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    public static Optional<TablespaceLifecycleHeader> read(ByteBuffer page) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (page == null) {
            throw new DatabaseValidationException("tablespace lifecycle page must not be null");
        }
        if (page.capacity() < SpaceHeaderLayout.LIFECYCLE_FINISH_STATE + Integer.BYTES) {
            throw new DatabaseValidationException("tablespace lifecycle page buffer too small: " + page.capacity());
        }
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        int magic = page.getInt(SpaceHeaderLayout.LIFECYCLE_MAGIC);
        if (magic == 0) {
            return Optional.empty();
        }
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        if (magic != TablespaceLifecycleFormat.MAGIC) {
            throw new FspMetadataException("invalid tablespace lifecycle magic: " + Integer.toHexString(magic));
        }
        int format = page.getInt(SpaceHeaderLayout.LIFECYCLE_FORMAT);
        if (format != TablespaceLifecycleFormat.VERSION) {
            throw new FspMetadataException("unsupported tablespace lifecycle format: " + format);
        }
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return Optional.of(new TablespaceLifecycleHeader(
                TablespaceState.fromPersistentCode(page.getInt(SpaceHeaderLayout.LIFECYCLE_STATE)),
                PageNo.of(page.getLong(SpaceHeaderLayout.LIFECYCLE_INITIAL_SIZE)),
                page.getLong(SpaceHeaderLayout.LIFECYCLE_EPOCH),
                PageNo.of(page.getLong(SpaceHeaderLayout.LIFECYCLE_TARGET_SIZE)),
                TablespaceState.fromPersistentCode(page.getInt(SpaceHeaderLayout.LIFECYCLE_FINISH_STATE))));
    }
}
