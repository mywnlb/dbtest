/**
 * B+Tree 索引导航模块。当前落地的第一片只支持 root 即 leaf 的 point lookup、页内 bounded scan
 * 与 insert without split；非 leaf、split、merge、事务锁等待和 MVCC 可见性按后续切片接入。
 *
 * <p>包边界：B+Tree 只通过 storage.api/record/mtr 门面访问页，不解析 record 字节布局、不直接访问
 * BufferFrame/PageStore，也不决定事务可见性或死锁 victim。
 */
package cn.zhangyis.db.storage.btree;
import cn.zhangyis.db.storage.fil.io.PageStore;

