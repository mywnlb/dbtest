package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.domain.PageSize;

/**
 * 表空间自动扩展策略（Strategy）。根据当前文件大小和页大小决定一次扩展多少页。
 * 实现必须返回 >=1 的页数，且边界值用单元测试钉死，避免误写成模糊闭区间（设计 §8、§15）。
 */
public interface AutoExtendPolicy {

    /**
     * 计算本次扩展页数。
     *
     * @param currentSizeInPages 当前文件大小页数（非负）。
     * @param pageSize 实例级页大小，用于推导 extent 页数。
     * @return 本次应扩展的页数，>=1。
     */
    long nextIncrementPages(long currentSizeInPages, PageSize pageSize);
}
