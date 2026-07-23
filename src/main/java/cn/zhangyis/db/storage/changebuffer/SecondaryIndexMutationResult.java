package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.btree.BTreeInsertResult;
import cn.zhangyis.db.storage.btree.BTreeSecondaryDeleteMarkResult;
import cn.zhangyis.db.storage.btree.BTreeSecondaryRemovalResult;

import java.util.Optional;

/**
 * 一次二级物理 mutation 的稳定结果。BUFFERED 表示全局记录与 bitmap 已提交并接管后续工作；
 * DIRECT 表示对应 B+Tree 结果已经在真实 leaf 上提交。结果不携带页 guard 或 cursor。
 *
 * @param buffered 是否由 Change Buffer durable 接管
 * @param operation 已完成接管或直写的物理动作
 * @param targetPageId eligibility/直写定位到的 leaf identity
 * @param insertResult 直写新 INSERT 的结果；其它分支为空
 * @param markResult 直写 revive/delete-mark 的结果；其它分支为空
 * @param removalResult 直写物理 DELETE 的结果；其它分支为空
 */
public record SecondaryIndexMutationResult(boolean buffered, ChangeBufferOperation operation,
                                           PageId targetPageId,
                                           Optional<BTreeInsertResult> insertResult,
                                           Optional<BTreeSecondaryDeleteMarkResult> markResult,
                                           Optional<BTreeSecondaryRemovalResult> removalResult) {

    public SecondaryIndexMutationResult {
        if (operation == null || targetPageId == null || insertResult == null
                || markResult == null || removalResult == null) {
            throw new DatabaseValidationException("secondary mutation result fields must not be null");
        }
        int directResults = (insertResult.isPresent() ? 1 : 0)
                + (markResult.isPresent() ? 1 : 0) + (removalResult.isPresent() ? 1 : 0);
        if (buffered ? directResults != 0 : directResults != 1) {
            throw new DatabaseValidationException("secondary mutation result has an invalid completion shape");
        }
    }

    /**
     * 构造全局记录已经接管、且没有直接 B+Tree 结果的完成态。
     *
     * @param operation 已 durable 追加的物理动作
     * @param target 被全局记录引用的目标 leaf identity
     * @return buffered=true 且三个 direct result 均为空的完成态
     */
    public static SecondaryIndexMutationResult buffered(ChangeBufferOperation operation, PageId target) {
        return new SecondaryIndexMutationResult(true, operation, target,
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * 构造已在真实 leaf 插入新 entry 的完成态。
     *
     * @param target B+Tree 结果中的实际 leaf identity
     * @param result 已提交的插入/结构后置结果
     * @return 只携带 insertResult 的 direct 完成态
     * @throws DatabaseValidationException result 为空时抛出
     */
    public static SecondaryIndexMutationResult inserted(PageId target, BTreeInsertResult result) {
        if (result == null) {
            throw new DatabaseValidationException("secondary insert result must not be null");
        }
        return new SecondaryIndexMutationResult(false, ChangeBufferOperation.INSERT, target,
                Optional.of(result), Optional.empty(), Optional.empty());
    }

    /**
     * 构造已在真实 leaf 翻转 delete 位的完成态。
     *
     * @param operation INSERT 表示 revive，DELETE_MARK 表示标记删除
     * @param target 被点改 leaf 的稳定 identity
     * @param result 已提交的状态分类结果
     * @return 只携带 markResult 的 direct 完成态
     * @throws DatabaseValidationException result 为空时抛出
     */
    public static SecondaryIndexMutationResult marked(ChangeBufferOperation operation, PageId target,
                                                       BTreeSecondaryDeleteMarkResult result) {
        if (result == null) {
            throw new DatabaseValidationException("secondary mark result must not be null");
        }
        return new SecondaryIndexMutationResult(false, operation, target,
                Optional.empty(), Optional.of(result), Optional.empty());
    }

    /**
     * 构造已在真实树执行物理删除的完成态。
     *
     * @param target 删除前首次定位的 leaf identity，仅用于诊断
     * @param result 已提交的删除状态、后置 descriptor 与 freed pages
     * @return 只携带 removalResult 的 direct 完成态
     * @throws DatabaseValidationException result 为空时抛出
     */
    public static SecondaryIndexMutationResult removed(PageId target, BTreeSecondaryRemovalResult result) {
        if (result == null) {
            throw new DatabaseValidationException("secondary removal result must not be null");
        }
        return new SecondaryIndexMutationResult(false, ChangeBufferOperation.DELETE, target,
                Optional.empty(), Optional.empty(), Optional.of(result));
    }
}
