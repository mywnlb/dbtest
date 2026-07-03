package cn.zhangyis.db.storage.btree;

/**
 * current-read 在授予事务锁后多次重新定位仍发现 record/gap 变化。调用方可重启语句或回滚事务；
 * 本异常表示 B+Tree 已释放本轮 stale lock，不会携带 page latch 或 buffer fix。
 */
public class BTreeCurrentReadRelocationException extends BTreeException {

    public BTreeCurrentReadRelocationException(String message) {
        super(message);
    }

    public BTreeCurrentReadRelocationException(String message, Throwable cause) {
        super(message, cause);
    }
}
