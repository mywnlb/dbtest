package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.fil.PageStore;

/**
 * Redo 回放上下文。Recovery 把物理页 store 和实例页大小传给 page handler；handler 不依赖 BufferPool/MTR。
 *
 * @param pageStore 恢复期直接读写物理页的 PageStore。
 * @param pageSize  实例页大小。
 */
public record RedoApplyContext(PageStore pageStore, PageSize pageSize) {

    public RedoApplyContext {
        if (pageStore == null || pageSize == null) {
            throw new DatabaseValidationException("redo apply pageStore/pageSize must not be null");
        }
    }
}
