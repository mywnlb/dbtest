package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;

import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * force-skip 恢复策略。它只表达管理员显式声明的损坏表空间集合，
 * recovery 各阶段必须在进入物理 IO 前用该谓词过滤 page/space。
 *
 * @param skippedSpaces 不可变表空间集合；为空表示普通恢复路径不跳过任何空间。
 */
public record RecoverySkipPolicy(Set<SpaceId> skippedSpaces) {

    public RecoverySkipPolicy {
        if (skippedSpaces == null) {
            throw new DatabaseValidationException("recovery skipped spaces must not be null");
        }
        for (SpaceId spaceId : skippedSpaces) {
            if (spaceId == null) {
                throw new DatabaseValidationException("recovery skipped space must not be null");
            }
        }
        skippedSpaces = Set.copyOf(skippedSpaces);
    }

    /**
     * 创建不跳过任何空间的策略，供 NORMAL 和 READ_ONLY_VALIDATE 使用。
     *
     * @return 空 skip policy。
     */
    public static RecoverySkipPolicy none() {
        return new RecoverySkipPolicy(Set.of());
    }

    /**
     * 创建显式跳过策略；调用方负责根据 recovery mode 判断是否允许非空集合。
     *
     * @param skippedSpaces 管理员显式配置的跳过空间集合。
     * @return 不可变 skip policy。
     */
    public static RecoverySkipPolicy of(Set<SpaceId> skippedSpaces) {
        return new RecoverySkipPolicy(skippedSpaces);
    }

    /**
     * 策略是否为空，便于普通恢复路径走无过滤分支。
     *
     * @return true 表示不跳过任何表空间。
     */
    public boolean isEmpty() {
        return skippedSpaces.isEmpty();
    }

    /**
     * 判断一个表空间是否应被跳过；空值说明调用方恢复输入错误，直接拒绝。
     *
     * @param spaceId 待判断表空间。
     * @return true 表示该空间不应进入物理 IO。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public boolean shouldSkip(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id for recovery skip check must not be null");
        }
        return skippedSpaces.contains(spaceId);
    }

    /**
     * 判断物理页所属空间是否应被跳过。
     *
     * @param pageId 待判断物理页。
     * @return true 表示该页不应进入 doublewrite/redo/reconcile 物理 IO。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public boolean shouldSkip(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id for recovery skip check must not be null");
        }
        return shouldSkip(pageId.spaceId());
    }

    /**
     * 生成稳定诊断文本，避免 journal 中 Set 迭代顺序导致测试和排障不稳定。
     *
     * @return 按 SpaceId 升序排列的诊断文本。
     */
    public String describeSkippedSpaces() {
        return skippedSpaces.stream()
                .sorted(Comparator.comparingInt(SpaceId::value))
                .map(spaceId -> Integer.toString(spaceId.value()))
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
