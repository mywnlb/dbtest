package cn.zhangyis.db.storage.fil.meta;
import cn.zhangyis.db.storage.fil.io.AutoExtendPolicy;
import cn.zhangyis.db.storage.fil.io.DataFileDescriptor;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.state.SpaceFlags;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.util.List;

/**
 * 运行时表空间元数据快照。它描述 SpaceId、页大小、文件范围、状态和版本，不直接持有文件句柄或解析 FSP_HDR。
 *
 * <p>简化点：设计文档 §5.2 把 {@code autoExtendPolicy} 列为表空间聚合根字段。本首版快照暂不纳入
 * autoExtendPolicy（autoextend 尚未实现），仅用 currentSizeInPages/freeLimitPageNo 表达当前文件边界；
 * 后续实现 AutoExtendPolicy 时再补入聚合，或交由 fsp 扩展服务按 TablespaceType 持有对应扩展策略。
 *
 * @param spaceId 表空间编号；运行时 Registry 使用它作为 cache key，PageStore 使用它选择文件句柄。
 * @param name 表空间逻辑名称；用于日志、诊断和未来数据字典映射，不参与物理页定位。
 * @param type 表空间类型；影响 autoextend、恢复和临时表空间 redo 简化策略。
 * @param pageSize 表空间页大小；用于 pageNo 到文件偏移、extent 页数和页镜像长度校验。
 * @param state 当前运行时生命周期状态；普通 IO 必须拒绝 CORRUPTED/DISCARDED 状态。
 * @param dataFiles 数据文件覆盖范围快照；当前不持有 FileChannel，后续 DataFileHandle 依据它打开文件。
 * @param spaceFlags 原始表空间标志位；作为后续功能开关的权威输入。
 * @param currentSizeInPages 当前可读写文件大小页数；PageStore 越界检查依赖该值。
 * @param freeLimitPageNo 当前允许 FSP 分配的页上界；autoextend 未发布前新页不能越过该值。
 * @param spaceVersion 运行时快照版本；状态、大小或 metadata 替换后递增，用于识别旧句柄。
 */
public record Tablespace(
        SpaceId spaceId,
        String name,
        TablespaceType type,
        PageSize pageSize,
        TablespaceState state,
        List<DataFileDescriptor> dataFiles,
        SpaceFlags spaceFlags,
        PageNo currentSizeInPages,
        PageNo freeLimitPageNo,
        long spaceVersion) {

    public Tablespace {
        // 字段级合法性校验与 TablespaceMetadata 完全一致，集中到 TablespaceFieldValidation 复用，
        // 返回不可变 dataFiles 回填 record 组件，避免两处校验各自漂移。
        dataFiles = TablespaceFieldValidation.validate(spaceId, name, type, pageSize, state, dataFiles,
                spaceFlags, currentSizeInPages, freeLimitPageNo, spaceVersion);
    }

    /**
     * 发布新的文件大小快照。数据从 autoextend 或 metadata reload 进入，只允许保持或增大，防止普通 IO 看到倒退的文件边界。
     *
     * @param newCurrentSizeInPages 新的当前文件页数。
     * @param newFreeLimitPageNo 新的可分配上界页号。
     * @return 更新大小和版本后的新 Tablespace 快照。
     */
    public Tablespace publishSize(PageNo newCurrentSizeInPages, PageNo newFreeLimitPageNo) {
        if (newCurrentSizeInPages == null) {
            throw new DatabaseValidationException("new tablespace current size must not be null");
        }
        if (newFreeLimitPageNo == null) {
            throw new DatabaseValidationException("new tablespace free limit must not be null");
        }
        if (newCurrentSizeInPages.value() < currentSizeInPages.value()) {
            throw new DatabaseValidationException("tablespace current size must not shrink");
        }
        if (newFreeLimitPageNo.value() < freeLimitPageNo.value()) {
            throw new DatabaseValidationException("tablespace free limit must not shrink");
        }
        return new Tablespace(spaceId, name, type, pageSize, state, dataFiles, spaceFlags,
                newCurrentSizeInPages, newFreeLimitPageNo, spaceVersion + 1);
    }

    /**
     * 按表空间状态机切换生命周期状态。数据流为当前快照进入，先由 TablespaceState 校验合法流转，
     * 通过后创建新快照并推进 spaceVersion；该方法不关闭文件、不刷脏、不修改磁盘，真实副作用由 Registry/DDL 后续编排。
     *
     * @param nextState 目标状态。
     * @return 状态更新后的新 Tablespace 快照。
     */
    public Tablespace transitTo(TablespaceState nextState) {
        if (nextState == null) {
            throw new DatabaseValidationException("next tablespace state must not be null");
        }
        state.validateTransitTo(nextState);
        return new Tablespace(spaceId, name, type, pageSize, nextState, dataFiles, spaceFlags,
                currentSizeInPages, freeLimitPageNo, spaceVersion + 1);
    }
}
