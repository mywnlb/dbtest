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
 * registry 持有的不可变运行时表空间快照。
 *
 * <p>它承载普通/恢复准入所需的 identity、类型、状态和 page0 尺寸快照，但不持有
 * {@code FileChannel}、不解析 FSP_HDR，也不自行执行 IO。{@link PageStore} 以自己的
 * {@code DataFileHandle.currentSizeInPages} 做物理越界判断；page0 是逻辑 size/free-limit 的持久权威。
 * 因此 autoextend 后本 record 的 size 可以暂时是旧快照，不能替代 FSP/page-store 的实时容量判断。</p>
 *
 * <p>与目标设计的简化差异：{@link AutoExtendPolicy} 由 {@code FileChannelPageStore} 组合持有，
 * 没有放入每个 tablespace 聚合；当前生产为单文件模型，record 的列表结构只保留多文件表达能力。</p>
 *
 * @param spaceId registry 的稳定 cache key，也是 PageId 选择已打开 handle 的空间部分
 * @param name 非空白逻辑名称，仅用于诊断和上层映射，不参与物理偏移计算
 * @param type 从 page0 flags 恢复的表空间类型，供生命周期准入与 UNDO/GENERAL 编排判断
 * @param pageSize loader 已校验的固定页大小；物理 handle 使用同一值计算 page offset
 * @param state 当前运行期生命周期状态；普通 require 只接受 NORMAL/ACTIVE
 * @param dataFiles loader/create 发布的不可变文件范围快照；当前生产只有一个起始页为 0 的文件
 * @param spaceFlags page0 原始 flags 快照；已定义低位可解析类型，未知高位保持原样
 * @param currentSizeInPages 本 metadata generation 观察到的逻辑大小，不是 PageStore 的实时物理计数器
 * @param freeLimitPageNo 本 metadata generation 观察到的 FSP 可分配 exclusive 上界
 * @param spaceVersion 非负快照代次；状态 helper 和 metadata 替换会推进它，但 handle 不自动做 stale 检测
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

    /**
     * 校验运行时快照的共享结构不变量，并防御性复制文件列表。
     *
     * <p>构造不会登记 registry、打开 handle 或校验 page0；跨源不变量必须已由创建/加载流程建立。</p>
     *
     * @throws DatabaseValidationException 必填结构字段非法时抛出；具体约束由
     *                                     {@link TablespaceFieldValidation} 定义
     */
    public Tablespace {
        // 与 TablespaceMetadata 共用结构校验，并用不可变列表隔离外部 mutation。
        dataFiles = TablespaceFieldValidation.validate(spaceId, name, type, pageSize, state, dataFiles,
                spaceFlags, currentSizeInPages, freeLimitPageNo, spaceVersion);
    }

    /**
     * 派生一个 size/free-limit 不倒退的新运行时快照。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>拒绝空 size/free-limit，避免丢失 page0 容量语义。</li>
     *     <li>分别验证两个值不小于当前快照；本 helper 只支持增长/同值发布，不用于 UNDO 物理截断，
     *     也不额外验证 free-limit 与 size 的交叉关系。</li>
     *     <li>保留其它组件、推进 version 并返回新值；不写 page0、不扩展文件，也不替换 registry。</li>
     * </ol>
     *
     * <p>当前生产 autoextend 直接更新 PageStore/page0，尚无该 helper 的生产调用方；它不能被解释为
     * 已完成持久容量发布。</p>
     *
     * @param newCurrentSizeInPages 候选逻辑大小；非空且不得小于当前快照值
     * @param newFreeLimitPageNo 候选 FSP exclusive 上界；非空且不得小于当前快照值
     * @return 保留 identity/type/state/file flags，仅替换容量并把 version 加一的新快照
     * @throws DatabaseValidationException 任一参数为空或容量发生倒退时抛出；当前对象保持不变
     */
    public Tablespace publishSize(PageNo newCurrentSizeInPages, PageNo newFreeLimitPageNo) {
        // 1. 两个容量标记都必须显式提供，禁止用 null 表达“沿用旧值”。
        if (newCurrentSizeInPages == null) {
            throw new DatabaseValidationException("new tablespace current size must not be null");
        }
        if (newFreeLimitPageNo == null) {
            throw new DatabaseValidationException("new tablespace free limit must not be null");
        }

        // 2. 通用 size helper 只允许单调增长；truncate 由专用流程构造 metadata 快照。
        if (newCurrentSizeInPages.value() < currentSizeInPages.value()) {
            throw new DatabaseValidationException("tablespace current size must not shrink");
        }
        if (newFreeLimitPageNo.value() < freeLimitPageNo.value()) {
            throw new DatabaseValidationException("tablespace free limit must not shrink");
        }

        // 3. 仅派生内存值并推进代次；持久化和 registry replace 必须由上层显式完成。
        return new Tablespace(spaceId, name, type, pageSize, state, dataFiles, spaceFlags,
                newCurrentSizeInPages, newFreeLimitPageNo, spaceVersion + 1);
    }

    /**
     * 按通用状态图派生生命周期状态更新后的快照。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>拒绝空目标，避免构造无法执行准入判断的 registry 值。</li>
     *     <li>委托 {@link TablespaceState#validateTransitTo(TablespaceState)} 校验有向迁移；类型专属规则
     *     仍须由调用方保证。</li>
     *     <li>替换状态并把 version 加一；即使同态幂等迁移也产生新代次。方法不关闭文件、不 flush、
     *     不写 page0，也不自动发布 registry。</li>
     * </ol>
     *
     * @param nextState 非空目标状态，必须满足当前状态图
     * @return 仅更新 state/version 的新不可变快照
     * @throws DatabaseValidationException 目标为空或状态迁移非法时抛出；当前对象保持不变
     */
    public Tablespace transitTo(TablespaceState nextState) {
        // 1. null 状态无法供 registry 做普通/恢复准入。
        if (nextState == null) {
            throw new DatabaseValidationException("next tablespace state must not be null");
        }

        // 2. 通用状态图先 fail-closed；调用方还负责 GENERAL/UNDO 类型专属约束。
        state.validateTransitTo(nextState);

        // 3. 只派生新值并推进版本，物理文件与 page0 没有副作用。
        return new Tablespace(spaceId, name, type, pageSize, nextState, dataFiles, spaceFlags,
                currentSizeInPages, freeLimitPageNo, spaceVersion + 1);
    }
}
