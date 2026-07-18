package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 索引键列引用；columnId 在映射物理 ordinal 前必须能解析到本表列。
 *
 * @param columnId 参与 {@code 构造} 的原始数值身份 {@code columnId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
 * @param order 选择 {@code 构造} 分支的 {@code StorageIndexOrder} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
 * @param prefixBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
 */
public record StorageIndexKeyPart(long columnId, StorageIndexOrder order, int prefixBytes) {
    public StorageIndexKeyPart {
        if (columnId <= 0 || order == null || prefixBytes < 0) {
            throw new DatabaseValidationException("invalid storage index key part");
        }
    }
}
