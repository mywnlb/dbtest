package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * crash recovery 的对象级空间排除策略。管理员集合表达本次 FORCE 请求，DD 集合表达此前已经持久隔离的对象；
 * 所有物理恢复阶段只消费二者并集，诊断接口仍保留来源，避免把一次性配置误当作长期真相。
 *
 * @param administrativeSpaces 本次启动由管理员明确声明的损坏空间；NORMAL 启动为空
 * @param dictionarySpaces committed DD 中处于 RECOVERY_UNAVAILABLE 的空间；所有模式都必须加载
 */
public record RecoverySpaceExclusionPolicy(Set<SpaceId> administrativeSpaces,
                                           Set<SpaceId> dictionarySpaces) {

    /** 校验两类证据并冻结不可变副本；重复 SpaceId 合法，最终排除集合按集合并集解释。 */
    public RecoverySpaceExclusionPolicy {
        administrativeSpaces = immutableSpaces(administrativeSpaces, "administrative");
        dictionarySpaces = immutableSpaces(dictionarySpaces, "dictionary");
    }

    /**
     * 创建不排除任何表空间的普通恢复策略。
     *
     * @return 两类证据均为空的不可变策略
     */
    public static RecoverySpaceExclusionPolicy none() {
        return new RecoverySpaceExclusionPolicy(Set.of(), Set.of());
    }

    /**
     * 合并本次管理员声明与 DD durable 隔离证据。
     *
     * @param administrativeSpaces 本次 FORCE 配置；可为空集合但不可为 {@code null}
     * @param dictionarySpaces committed DD 隔离集合；可为空集合但不可为 {@code null}
     * @return 保留来源且按并集执行判断的不可变策略
     */
    public static RecoverySpaceExclusionPolicy of(Set<SpaceId> administrativeSpaces,
                                                   Set<SpaceId> dictionarySpaces) {
        return new RecoverySpaceExclusionPolicy(administrativeSpaces, dictionarySpaces);
    }

    /** @return 管理员和 DD 证据的不可变集合并集。 */
    public Set<SpaceId> excludedSpaces() {
        Set<SpaceId> result = new LinkedHashSet<>(administrativeSpaces);
        result.addAll(dictionarySpaces);
        return Set.copyOf(result);
    }

    /**
     * 兼容旧恢复报告与诊断调用点的名称；语义已从“本次管理员 skip”扩展为“管理员与 DD 的排除并集”。
     *
     * @return 与 {@link #excludedSpaces()} 相同的不可变集合
     */
    public Set<SpaceId> skippedSpaces() {
        return excludedSpaces();
    }

    /** @return {@code true} 表示当前没有任何物理恢复排除项。 */
    public boolean isEmpty() {
        return administrativeSpaces.isEmpty() && dictionarySpaces.isEmpty();
    }

    /**
     * 判断物理空间是否在任一权威排除集合中。
     *
     * @param spaceId 待进入 doublewrite、redo、undo 或 reconcile 的空间标识
     * @return {@code true} 表示该空间不得发生物理 IO
     */
    public boolean shouldSkip(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id for recovery exclusion check must not be null");
        }
        return administrativeSpaces.contains(spaceId) || dictionarySpaces.contains(spaceId);
    }

    /**
     * 判断物理页所属空间是否被隔离。
     *
     * @param pageId 待恢复页的稳定物理标识
     * @return {@code true} 表示页所属表空间不得发生物理 IO
     */
    public boolean shouldSkip(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id for recovery exclusion check must not be null");
        }
        return shouldSkip(pageId.spaceId());
    }

    /** @return 按 SpaceId 升序输出并集，供稳定日志和测试断言使用。 */
    public String describeExcludedSpaces() {
        return excludedSpaces().stream().sorted(Comparator.comparingInt(SpaceId::value))
                .map(spaceId -> Integer.toString(spaceId.value()))
                .collect(Collectors.joining(", ", "[", "]"));
    }

    /** @return 与 {@link #describeExcludedSpaces()} 相同的稳定诊断文本。 */
    public String describeSkippedSpaces() {
        return describeExcludedSpaces();
    }

    private static Set<SpaceId> immutableSpaces(Set<SpaceId> spaces, String source) {
        if (spaces == null || spaces.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("recovery " + source + " spaces must not contain null");
        }
        return Set.copyOf(spaces);
    }
}
