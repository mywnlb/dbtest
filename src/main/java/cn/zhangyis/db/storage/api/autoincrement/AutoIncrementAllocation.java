package cn.zhangyis.db.storage.api.autoincrement;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

/**
 * 一次 statement 顺序分配结果。
 *
 * @param values 与请求等长的正自增值；显式正值原样保留
 * @param firstGeneratedKey 第一个由空请求实际生成的值
 */
public record AutoIncrementAllocation(
        List<BigInteger> values, Optional<BigInteger> firstGeneratedKey) {
    public AutoIncrementAllocation {
        if (values == null || values.isEmpty() || firstGeneratedKey == null
                || values.stream().anyMatch(value -> value == null
                || value.signum() <= 0)) {
            throw new DatabaseValidationException(
                    "auto-increment allocation values are invalid");
        }
        values = List.copyOf(values);
    }
}
