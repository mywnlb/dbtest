package cn.zhangyis.db.storage.record.page;

/**
 * update 结果分类（innodb-record-design §10.2）。
 */
public enum UpdateOutcome {
    /** 原地更新：新 encoded 长度 ≤ 旧长度、key 未变，直接覆盖 payload（保留 heapNo/next/n_owned）。 */
    IN_PLACE,
    /** 页内搬迁：新记录变长但页内有空间，迁到新位置并修正前驱链与目录。 */
    MOVED,
    /** key 变化：record 层不跨页重定位，交调用方按 deleteMark→purge→insert 处理（无 B+Tree）。 */
    REQUIRES_REINSERT
}
