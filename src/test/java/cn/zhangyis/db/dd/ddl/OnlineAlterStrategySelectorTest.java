package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证通用ALTER在任何持久副作用前给出稳定策略、原因与缺失能力。 */
class OnlineAlterStrategySelectorTest {

    /** comment/default charset/rename只改变DD image，应选择短X的metadata策略。 */
    @Test
    void selectsInstantMetadataForMetadataOnlyActions() {
        OnlineAlterDecision decision = OnlineAlterStrategySelector.productionV1().select(
                command(List.of(new AlterTableAction.Comment("archive"),
                        new AlterTableAction.DefaultCharset(45, 255))));

        assertEquals(OnlineAlterStrategy.INSTANT_METADATA, decision.strategy());
        assertEquals(OnlineAlterReason.METADATA_ONLY_ACTIONS, decision.reason());
        assertTrue(decision.rejectedCapabilities().isEmpty());
    }

    /** 单个二级索引action可以直接复用现有online ADD/DROP的完整marker、gate、取消与恢复协议。 */
    @Test
    void selectsInplaceForSingleSecondaryIndexAction() {
        AlterTableAction.DropIndex drop = new AlterTableAction.DropIndex(ObjectName.of("idx_status"));

        OnlineAlterDecision decision = OnlineAlterStrategySelector.productionV1().select(
                command(List.of(drop)));

        assertEquals(OnlineAlterStrategy.INPLACE_INDEX, decision.strategy());
        assertEquals(OnlineAlterReason.SINGLE_INDEX_ONLINE_PROTOCOL, decision.reason());
        assertTrue(decision.rejectedCapabilities().isEmpty());
    }

    /** 多ADD/DROP不得循环复用单slot descriptor，否则会暴露中间DD；能力未接线时必须显式blocking。 */
    @Test
    void reportsMissingMultiDescriptorForMultiIndexAlter() {
        OnlineAlterDecision decision = OnlineAlterStrategySelector.productionV1().select(command(List.of(
                new AlterTableAction.DropIndex(ObjectName.of("idx_status")),
                new AlterTableAction.DropIndex(ObjectName.of("idx_created")))));

        assertEquals(OnlineAlterStrategy.BLOCKING, decision.strategy());
        assertEquals(OnlineAlterReason.MULTI_INDEX_SIDECAR_PENDING, decision.reason());
        assertEquals(List.of(OnlineAlterCapability.VERSIONED_MULTI_INDEX_SIDECAR),
                decision.rejectedCapabilities());
    }

    /** row-layout action在shadow change-log与MVCC barrier接线前保持明确blocking，不能静默宣称online。 */
    @Test
    void reportsMissingShadowCapabilitiesForColumnAlter() {
        OnlineAlterDecision decision = OnlineAlterStrategySelector.productionV1().select(command(List.of(
                new AlterTableAction.DropColumn(ObjectName.of("status")))));

        assertEquals(OnlineAlterStrategy.BLOCKING, decision.strategy());
        assertEquals(OnlineAlterReason.SHADOW_REBUILD_PROTOCOL_PENDING, decision.reason());
        assertEquals(List.of(OnlineAlterCapability.SHADOW_CHANGE_LOG,
                        OnlineAlterCapability.MVCC_SCHEMA_RETENTION_BARRIER),
                decision.rejectedCapabilities());
    }

    /** 完整能力矩阵必须把多个索引与metadata混合动作归入一个manifest拥有的INPLACE协议。 */
    @Test
    void selectsGeneralInplaceForMultipleIndexAndMetadataActionsWhenComplete() {
        OnlineAlterDecision decision = OnlineAlterStrategySelector.productionComplete().select(command(List.of(
                new AlterTableAction.DropIndex(ObjectName.of("idx_status")),
                new AlterTableAction.DropIndex(ObjectName.of("idx_created")),
                new AlterTableAction.Comment("compacted"))));

        assertEquals(OnlineAlterStrategy.INPLACE_INDEX, decision.strategy());
        assertEquals(OnlineAlterReason.GENERAL_INPLACE_MANIFEST, decision.reason());
        assertTrue(decision.rejectedCapabilities().isEmpty());
    }

    /** 任一改变record layout的动作必须选择shadow协议，不能由多索引INPLACE能力错误吞掉。 */
    @Test
    void selectsShadowForRowLayoutActionWhenComplete() {
        OnlineAlterDecision decision = OnlineAlterStrategySelector.productionComplete().select(command(List.of(
                new AlterTableAction.DropColumn(ObjectName.of("status")),
                new AlterTableAction.DropIndex(ObjectName.of("idx_status")),
                new AlterTableAction.Comment("rewritten"))));

        assertEquals(OnlineAlterStrategy.SHADOW_REBUILD_V1, decision.strategy());
        assertEquals(OnlineAlterReason.SHADOW_REBUILD_PROTOCOL, decision.reason());
        assertTrue(decision.rejectedCapabilities().isEmpty());
    }

    private static AlterTableCommand command(List<AlterTableAction> actions) {
        return new AlterTableCommand(QualifiedTableName.of("app", "orders"), actions);
    }
}
