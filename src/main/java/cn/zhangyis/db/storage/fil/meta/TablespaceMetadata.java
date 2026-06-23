package cn.zhangyis.db.storage.fil.meta;
import cn.zhangyis.db.storage.fil.io.DataFileDescriptor;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.state.SpaceFlags;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;


import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.util.List;

/**
 * 从磁盘、数据字典或启动配置加载出的表空间权威元数据快照。
 *
 * @param spaceId 表空间编号；必须与 FSP_HDR 或数据字典中记录的 SpaceId 一致。
 * @param name 表空间逻辑名称；用于诊断、DDL 映射和未来数据字典关联。
 * @param type 表空间类型；决定 loader 来源、autoextend 策略和恢复处理分支。
 * @param pageSize 表空间页大小；MiniMySQL 首版按实例统一页大小处理，但元数据仍保留该字段。
 * @param state 从权威来源读取的生命周期状态；Registry 会据此阻断普通 IO 或允许恢复路径访问。
 * @param dataFiles 表空间数据文件范围；首版通常只有一个文件，结构上保留 general tablespace 扩展能力。
 * @param spaceFlags 表空间原始标志位；后续压缩、加密或 atomic DDL 语义由专门解析器解释。
 * @param currentSizeInPages 当前文件大小页数；PageStore 普通读写必须小于该边界。
 * @param freeLimitPageNo FSP 可分配页上界；autoextend 发布新大小前不得让新页对普通分配可见。
 * @param spaceVersion 表空间元数据版本；DDL、discard、recovery 更新快照时递增，帮助调用方识别 stale handle。
 */
public record TablespaceMetadata(
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

    public TablespaceMetadata {
        // 与运行时 Tablespace 共用同一套字段级校验，集中在 TablespaceFieldValidation，
        // 返回不可变 dataFiles 回填 record 组件，保证权威元数据快照构造完成即结构有效。
        dataFiles = TablespaceFieldValidation.validate(spaceId, name, type, pageSize, state, dataFiles,
                spaceFlags, currentSizeInPages, freeLimitPageNo, spaceVersion);
    }

    /**
     * 把权威元数据转换成运行时 Tablespace 快照。数据从 loader 进入，经基础合法性校验后交给 Registry 缓存。
     *
     * @return 不持有文件句柄的 Tablespace 快照。
     */
    public Tablespace toTablespace() {
        return new Tablespace(spaceId, name, type, pageSize, state, dataFiles, spaceFlags,
                currentSizeInPages, freeLimitPageNo, spaceVersion);
    }
}
