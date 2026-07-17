package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 二级索引 delete-mark/revive 的物化结果；结果不携带页句柄，调用方不得跨 MTR 使用页内定位。
 *
 * @param status 本次状态转换结果；不能为 null。
 */
public record BTreeSecondaryDeleteMarkResult(SecondaryDeleteMarkStatus status) {

    /**
     * 校验 delete-mark/revive 的状态结果。
     *
     * @param status 本次物理状态转换的结果，不能为 {@code null}。
     * @throws DatabaseValidationException 状态缺失时抛出，避免编排层把未知结果误判为幂等成功。
     */
    public BTreeSecondaryDeleteMarkResult {
        if (status == null) {
            throw new DatabaseValidationException("secondary delete-mark result status must not be null");
        }
    }
}
