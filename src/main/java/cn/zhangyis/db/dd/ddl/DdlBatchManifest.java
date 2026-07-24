package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * 一个批量 DROP operation 的完整、确定性恢复清单。列表按 table id 排序，任何阶段 marker 都携带同一值。
 *
 * @param schema DROP SCHEMA CASCADE 的 schema 证据；普通多表 DROP 为空
 * @param tables 冻结的表集合；DROP SCHEMA 允许为空，普通多表 DROP 必须由 operation 校验为非空
 */
public record DdlBatchManifest(
        Optional<DdlBatchSchemaEntry> schema,
        List<DdlBatchTableEntry> tables) {

    /** 单个 durable marker 可冻结的最大表数；与 DDL log 有界解码分配保持一致。 */
    public static final int MAX_TABLES = 4_096;

    /**
     * 规范化表顺序并交叉校验 table/space/path 唯一性。
     *
     * @throws DatabaseValidationException optional/list 缺失或任一持久 identity 重复时抛出
     */
    public DdlBatchManifest {
        if (schema == null || tables == null
                || tables.size() > MAX_TABLES
                || tables.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "DDL batch manifest schema/tables are null or exceed bound");
        }
        tables = tables.stream()
                .sorted(Comparator.comparingLong(entry -> entry.tableId().value()))
                .toList();
        Set<Long> tableIds = new HashSet<>();
        Set<Integer> spaceIds = new HashSet<>();
        Set<String> paths = new HashSet<>();
        for (DdlBatchTableEntry table : tables) {
            if (!tableIds.add(table.tableId().value())
                    || !spaceIds.add(table.spaceId().value())
                    || !paths.add(table.relativePath())) {
                throw new DatabaseValidationException(
                        "DDL batch manifest contains duplicate table/space/path identity");
            }
        }
    }
}
