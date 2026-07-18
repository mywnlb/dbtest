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
 * 交给 {@link TablespaceRegistry} 发布的表空间元数据传输快照。
 *
 * <p>已有空间由 {@link TablespaceMetadataLoader} 从已校验 page0 与已打开文件路径构造；新建空间在
 * page0 初始化的同一编排中由已校验创建参数构造。record 本身不读取磁盘，也不是 page0 的替代权威，
 * 只把一次来源一致的结果传给 registry。紧凑构造器只保证字段结构合法，type/state、文件长度与
 * page0 的跨源一致性必须在 loader/创建流程中完成。</p>
 *
 * @param spaceId registry 键及 page0 物理身份；loader 必须已确认二者一致
 * @param name 非空白逻辑名称，用于运行期诊断和上层元数据映射，不用于定位文件
 * @param type 从 page0 flags 或创建请求取得的类型；当前完整生产生命周期只支持 GENERAL/UNDO
 * @param pageSize 解释 page0 和所有数据文件页号的固定页大小
 * @param state 从 page0 lifecycle 或创建协议确定的状态；registry 据此执行普通/恢复准入
 * @param dataFiles 有序、非空的物理文件范围快照；当前生产实现使用起始页为 0 的单文件
 * @param spaceFlags page0 {@code SPACE_FLAGS} 完整原始位，未知高位保持不变
 * @param currentSizeInPages page0 声明的逻辑当前页数；crash recovery 可能在之后协调物理文件长度
 * @param freeLimitPageNo FSP 已初始化并允许参与分配的 exclusive 上界，不等同于物理文件末尾
 * @param spaceVersion page0/创建流程提供的非负元数据版本，用于区分快照代次；record 不自动检测 stale handle
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

    /**
     * 对传输快照执行共享结构校验，并防御性复制数据文件列表。
     *
     * <p>成功只建立不可变 Java 值，不发布 registry、不打开文件，也不修改 page0。</p>
     *
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 必填结构字段非法时抛出；
     *         具体约束由 {@link TablespaceFieldValidation} 定义
     */
    public TablespaceMetadata {
        // 与运行时 Tablespace 共用结构校验；返回的不可变列表回填 record 组件，隔离调用方后续修改。
        dataFiles = TablespaceFieldValidation.validate(spaceId, name, type, pageSize, state, dataFiles,
                spaceFlags, currentSizeInPages, freeLimitPageNo, spaceVersion);
    }

    /**
     * 把传输快照转换为 registry 持有的运行时 {@link Tablespace} 值。
     *
     * <p>所有组件原样传递，并由运行时 record 再次执行相同结构校验；该转换不打开
     * {@link PageStore} 句柄、不检查生命周期准入，也不发布到 registry。</p>
     *
     * @return 与本 metadata 字段一致、且不持有物理文件句柄的不可变运行时快照
     */
    public Tablespace toTablespace() {
        return new Tablespace(spaceId, name, type, pageSize, state, dataFiles, spaceFlags,
                currentSizeInPages, freeLimitPageNo, spaceVersion);
    }
}
