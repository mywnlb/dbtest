package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * secondary purge 无法从当前聚簇版本严格到达目标 undo pointer，或沿途 owner/table/index/key 不一致。该状态下不能
 * 猜测 entry 已安全；history head 必须保留并由恢复/诊断处理。
 */
public final class SecondaryPurgeVersionChainException extends DatabaseRuntimeException {

    /**
     * 创建没有底层 cause 的版本链证明失败异常。
     *
     * @param message 包含 table、cluster key、target roll pointer 或身份错配位置的诊断信息。
     */
    public SecondaryPurgeVersionChainException(String message) {
        super(message);
    }

    /**
     * 包装 undo 读取、解码或版本重建过程中导致证明失败的底层原因。
     *
     * @param message 说明无法安全物理删除 secondary identity 的领域上下文。
     * @param cause   原始异常；purge 必须保留它并让 history head 留在队首。
     */
    public SecondaryPurgeVersionChainException(String message, Throwable cause) {
        super(message, cause);
    }
}
