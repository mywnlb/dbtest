package cn.zhangyis.db.storage.fsp.header;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorRepository;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

/**
 * SpaceHeaderPage 字段快照（§6.2）。新建表空间应取 nextSegmentId=1（0 留作 XDES 无主哨兵）、firstInodePageNo=2、
 * 三个 extent list base 为 {@link FlstBase#EMPTY}。初始化表空间时还须在同一 MTR 内调用
 * ExtentDescriptorRepository.reserveSystemExtent，避免 extent 0 被当作普通 FREE。
 *
 * @param spaceId            表空间编号。
 * @param pageSize           实例级页大小。
 * @param spaceFlags         表空间标志位（0 表无特殊标志）。
 * @param currentSizeInPages 表空间当前总页数。
 * @param freeLimitPageNo    已纳入空间管理的页号上界。
 * @param nextSegmentId      下一个待分配 segment id；必须 > 0。
 * @param freeExtentList     全局 FSP_FREE extent 链 base。
 * @param freeFragExtentList 全局 FSP_FREE_FRAG extent 链 base。
 * @param fullFragExtentList 全局 FSP_FULL_FRAG extent 链 base。
 * @param firstInodePageNo   首个 INODE 页页号（首版固定 page 2）。
 * @param sdiRootPageNo      GENERAL 表空间的 SDI 根页号；0 表示 legacy/未启用，v1 使用固定 page 3。
 * @param serverVersion      写入该表空间的 server 版本号。
 * @param spaceVersion       表空间格式版本号。
 */
public record SpaceHeaderSnapshot(
        SpaceId spaceId,
        PageSize pageSize,
        int spaceFlags,
        PageNo currentSizeInPages,
        PageNo freeLimitPageNo,
        long nextSegmentId,
        FlstBase freeExtentList,
        FlstBase freeFragExtentList,
        FlstBase fullFragExtentList,
        PageNo firstInodePageNo,
        long sdiRootPageNo,
        int serverVersion,
        long spaceVersion) {

    public SpaceHeaderSnapshot {
        if (spaceId == null || pageSize == null || currentSizeInPages == null || freeLimitPageNo == null
                || freeExtentList == null || freeFragExtentList == null || fullFragExtentList == null
                || firstInodePageNo == null) {
            throw new DatabaseValidationException("space header snapshot fields must not be null");
        }
        if (nextSegmentId <= 0) {
            throw new DatabaseValidationException("next segment id must be positive; 0 is reserved as owner sentinel");
        }
    }
}
