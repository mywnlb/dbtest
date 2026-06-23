package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;

import java.nio.file.Path;

/**
 * 表空间数据文件描述。它只表达文件覆盖的页号范围，不持有 FileChannel，也不解释页内 FSP_HDR 内容。
 *
 * @param path 数据文件路径；后续 DataFileHandle 用它打开真实文件，领域模型本身不持有句柄。
 * @param startPageNo 当前文件覆盖的表空间起始页号；多文件表空间依赖它判断 page 落在哪个文件。
 * @param sizeInPages 当前文件覆盖的页数；PageStore 用它做越界检查和 truncate/drop 边界判断。
 */
public record DataFileDescriptor(Path path, PageNo startPageNo, PageNo sizeInPages) {

    public DataFileDescriptor {
        if (path == null) {
            throw new DatabaseValidationException("data file path must not be null");
        }
        if (startPageNo == null) {
            throw new DatabaseValidationException("data file start page must not be null");
        }
        if (sizeInPages == null) {
            throw new DatabaseValidationException("data file size must not be null");
        }
        if (sizeInPages.value() <= 0) {
            throw new DatabaseValidationException("data file size must be positive pages");
        }
    }

    /**
     * 创建单文件表空间的数据文件描述；首版只支持一个文件，但用描述对象保留多文件扩展点。
     *
     * @param path 数据文件路径。
     * @param startPageNo 该文件覆盖的起始页号。
     * @param sizeInPages 文件覆盖的页数。
     * @return 数据文件描述。
     */
    public static DataFileDescriptor single(Path path, PageNo startPageNo, PageNo sizeInPages) {
        return new DataFileDescriptor(path, startPageNo, sizeInPages);
    }

    /**
     * 计算文件覆盖范围的排他结束页号。PageStore 后续会用该范围判断 pageNo 是否属于当前文件。
     *
     * @return 文件覆盖范围的排他结束页号。
     */
    public PageNo endExclusivePageNo() {
        return PageNo.of(Math.addExact(startPageNo.value(), sizeInPages.value()));
    }
}
