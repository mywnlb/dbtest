package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Optional;

/**
 * unique insert current-read 检查结果。本片只判断物理重复或目标 gap 可插入，不执行实际 insert；
 * delete-marked 同 key 仍按物理重复处理，MVCC 逻辑唯一留后续切片。
 *
 * @param duplicate       true 表示已有同 key 记录。
 * @param duplicateRecord duplicate=true 时携带该物理记录的当前版本。
 */
public record BTreeUniqueCheckResult(boolean duplicate, Optional<BTreeLookupResult> duplicateRecord) {

    public BTreeUniqueCheckResult {
        if (duplicateRecord == null) {
            throw new DatabaseValidationException("unique check duplicateRecord must not be null");
        }
        if (duplicate && duplicateRecord.isEmpty()) {
            throw new DatabaseValidationException("duplicate unique check must carry duplicate record");
        }
        if (!duplicate && duplicateRecord.isPresent()) {
            throw new DatabaseValidationException("available unique check must not carry duplicate record");
        }
    }

    /** 构造可插入结果。 */
    public static BTreeUniqueCheckResult available() {
        return new BTreeUniqueCheckResult(false, Optional.empty());
    }

    /** 构造物理重复结果。 */
    public static BTreeUniqueCheckResult duplicate(BTreeLookupResult duplicateRecord) {
        if (duplicateRecord == null) {
            throw new DatabaseValidationException("duplicate record must not be null");
        }
        return new BTreeUniqueCheckResult(true, Optional.of(duplicateRecord));
    }
}
