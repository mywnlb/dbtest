package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;

/**
 * descriptor chain中的完整index物理ownership。entry只有在root和双segment已经能由同一MTR证明时才可追加，
 * 因而不需要模糊的“部分完成”状态。
 */
public record SdiOnlineAlterDescriptorEntry(SdiOnlineAlterDescriptorAction action,
                                            int actionOrdinal,
                                            IndexStorageBinding indexBinding) {

    public SdiOnlineAlterDescriptorEntry {
        if (action == null || actionOrdinal < 0 || indexBinding == null) {
            throw new DatabaseValidationException("invalid online ALTER descriptor entry");
        }
    }

    /**
     * 证明root与双segment都属于anchor所在表空间。
     *
     * @param expectedSpace page3和descriptor segment所属空间
     * @return 当前不可变entry，便于构造器链式校验
     * @throws DatabaseValidationException 任一物理identity跨space时抛出
     */
    public SdiOnlineAlterDescriptorEntry requireSpace(SpaceId expectedSpace) {
        if (expectedSpace == null
                || !indexBinding.rootPageId().spaceId().equals(expectedSpace)
                || !indexBinding.leafSegment().spaceId().equals(expectedSpace)
                || !indexBinding.nonLeafSegment().spaceId().equals(expectedSpace)) {
            throw new DatabaseValidationException(
                    "online ALTER descriptor index binding crosses tablespace");
        }
        return this;
    }
}
