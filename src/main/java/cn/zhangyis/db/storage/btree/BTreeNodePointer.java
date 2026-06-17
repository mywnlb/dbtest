package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.record.page.SearchKey;

/**
 * 非叶页中的 node pointer 逻辑模型。lowKey 表示 child 覆盖范围的最小 key；
 * root 查找选择 greatest lowKey <= searchKey 的指针进入下一层。
 *
 * @param lowKey      子页范围下界，必须使用索引 keyDef 的 key part 顺序。
 * @param childPageId 子页物理页号；B3 只指向 leaf 页。
 */
public record BTreeNodePointer(SearchKey lowKey, PageId childPageId) {

    public BTreeNodePointer {
        if (lowKey == null || childPageId == null) {
            throw new DatabaseValidationException("btree node pointer lowKey/childPageId must not be null");
        }
    }
}
