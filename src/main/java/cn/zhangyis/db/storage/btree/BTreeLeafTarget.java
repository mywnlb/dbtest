package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

/**
 * 只读取 root/internal 层得到的目标 leaf identity。
 *
 * @param pageId key 当前应路由到的 leaf 物理页
 * @param rootLeaf true 表示 root 本身就是 leaf，定位过程不可避免地已把目标页载入，Change Buffer 必须回退直写
 */
public record BTreeLeafTarget(PageId pageId, boolean rootLeaf) {
    public BTreeLeafTarget {
        if (pageId == null) {
            throw new DatabaseValidationException("btree leaf target page id must not be null");
        }
    }
}
