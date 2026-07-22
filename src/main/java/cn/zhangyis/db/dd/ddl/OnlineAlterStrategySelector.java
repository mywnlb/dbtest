package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * 通用ALTER纯决策器。只读取已经bound的有序action与编译期能力，不查询repository、不预留identity、
 * 不写marker；因此失败或显式降级都发生在DDL副作用之前。
 */
public final class OnlineAlterStrategySelector {

    /** 未注入通用runtime的兼容能力矩阵；只允许既有单索引在线协议，其余显式降级。 */
    private static final OnlineAlterStrategySelector PRODUCTION_V1 =
            new OnlineAlterStrategySelector(true, false, false);

    /** 当前完整生产能力矩阵；调用方只有在通用coordinator runtime可用时才能选择本实例。 */
    private static final OnlineAlterStrategySelector PRODUCTION_COMPLETE =
            new OnlineAlterStrategySelector(true, true, true);

    /** 单index ADD/DROP是否已有完整live/recovery链。 */
    private final boolean singleIndexOnline;
    /** multi-index manifest/descriptor/capture是否以一个owner原子接线。 */
    private final boolean multiIndexOnline;
    /** shadow change-log、MVCC barrier与old-space retirement是否完整接线。 */
    private final boolean shadowOnline;

    private OnlineAlterStrategySelector(boolean singleIndexOnline,
                                        boolean multiIndexOnline,
                                        boolean shadowOnline) {
        this.singleIndexOnline = singleIndexOnline;
        this.multiIndexOnline = multiIndexOnline;
        this.shadowOnline = shadowOnline;
    }

    /** @return 未注入通用runtime时使用的单索引兼容selector。 */
    public static OnlineAlterStrategySelector productionV1() {
        return PRODUCTION_V1;
    }

    /**
     * 返回通用INPLACE与SHADOW均已接线时使用的生产selector。
     *
     * @return 支持单索引、多索引manifest与row-layout shadow rebuild的共享实例
     */
    public static OnlineAlterStrategySelector productionComplete() {
        return PRODUCTION_COMPLETE;
    }

    /**
     * 按action全集冻结唯一策略，绝不在执行中途从online静默切到blocking。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证command后分类metadata、index与row-layout action，不改变原声明顺序。</li>
     *     <li>纯metadata选择短X；单index在能力启用时选择现有online protocol。</li>
     *     <li>多index、混合action或row-layout能力不完整时返回带稳定缺口的BLOCKING裁决。</li>
     * </ol>
     *
     * @param command binder产生的非空有序ALTER命令
     * @return immutable策略、原因与拒绝能力；永不返回{@code null}
     * @throws DatabaseValidationException command缺失时抛出，且没有DDL副作用
     */
    public OnlineAlterDecision select(AlterTableCommand command) {
        // 1. action类型是唯一输入；selector不绑定会变化的table/index identity。
        if (command == null) {
            throw new DatabaseValidationException("online ALTER selector requires a command");
        }
        long indexActions = command.actions().stream().filter(OnlineAlterStrategySelector::indexAction).count();
        long rowLayoutActions = command.actions().stream().filter(OnlineAlterStrategySelector::rowLayoutAction).count();
        long metadataActions = command.actions().size() - indexActions - rowLayoutActions;

        // 2. 当前单index路径复用已实现的ONLINE_INDEX/ONLINE_DROP marker，不创造第二套恢复协议。
        if (metadataActions == command.actions().size()) {
            return new OnlineAlterDecision(OnlineAlterStrategy.INSTANT_METADATA,
                    OnlineAlterReason.METADATA_ONLY_ACTIONS, List.of());
        }
        if (indexActions == 1L && metadataActions == 0L && rowLayoutActions == 0L
                && singleIndexOnline) {
            return new OnlineAlterDecision(OnlineAlterStrategy.INPLACE_INDEX,
                    OnlineAlterReason.SINGLE_INDEX_ONLINE_PROTOCOL, List.of());
        }

        if (rowLayoutActions > 0L && shadowOnline) {
            return new OnlineAlterDecision(OnlineAlterStrategy.SHADOW_REBUILD_V1,
                    OnlineAlterReason.SHADOW_REBUILD_PROTOCOL, List.of());
        }
        if (rowLayoutActions == 0L && indexActions > 0L && multiIndexOnline) {
            return new OnlineAlterDecision(OnlineAlterStrategy.INPLACE_INDEX,
                    OnlineAlterReason.GENERAL_INPLACE_MANIFEST, List.of());
        }

        // 3. 降级原因精确指向未接能力；禁止逐action调用现有单slot协议制造中间可见版本。
        if (rowLayoutActions > 0L && !shadowOnline) {
            return new OnlineAlterDecision(OnlineAlterStrategy.BLOCKING,
                    OnlineAlterReason.SHADOW_REBUILD_PROTOCOL_PENDING,
                    List.of(OnlineAlterCapability.SHADOW_CHANGE_LOG,
                            OnlineAlterCapability.MVCC_SCHEMA_RETENTION_BARRIER));
        }
        if (indexActions > 1L && metadataActions == 0L && !multiIndexOnline) {
            return new OnlineAlterDecision(OnlineAlterStrategy.BLOCKING,
                    OnlineAlterReason.MULTI_INDEX_SIDECAR_PENDING,
                    List.of(OnlineAlterCapability.VERSIONED_MULTI_INDEX_SIDECAR));
        }
        return new OnlineAlterDecision(OnlineAlterStrategy.BLOCKING,
                OnlineAlterReason.MIXED_ACTION_MANIFEST_PENDING,
                List.of(OnlineAlterCapability.VERSIONED_MULTI_INDEX_SIDECAR));
    }

    private static boolean indexAction(AlterTableAction action) {
        return action instanceof AlterTableAction.AddIndex
                || action instanceof AlterTableAction.DropIndex;
    }

    private static boolean rowLayoutAction(AlterTableAction action) {
        return action instanceof AlterTableAction.AddColumn
                || action instanceof AlterTableAction.DropColumn
                || action instanceof AlterTableAction.ConvertCharset;
    }
}
