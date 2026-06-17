package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 实例级页大小。extent 页数必须由页大小推导，不能在空间分配代码里写死 64。
 *
 * @param bytes 实例统一页大小字节数；一旦创建 Buffer Pool、FSP 和 PageStore 都必须遵循同一值。
 */
public record PageSize(int bytes) {

    /**
     * 二进制 KiB 单位，用于表达 InnoDB 支持的 page size 和 extent size 换算规则。
     */
    private static final int KIB = 1024;

    /**
     * 二进制 MiB 单位；16KB 及以下页大小的 extent 固定为 1MiB。
     */
    private static final int MIB = 1024 * 1024;

    public PageSize {
        if (!isSupported(bytes)) {
            throw new DatabaseValidationException("unsupported page size: " + bytes);
        }
    }

    /**
     * 创建实例级页大小；只接受 InnoDB 支持的 4KB、8KB、16KB、32KB、64KB。
     *
     * @param bytes 页大小字节数。
     * @return 通过校验的页大小。
     */
    public static PageSize ofBytes(int bytes) {
        return new PageSize(bytes);
    }

    /**
     * 计算 extent 物理大小。该规则来自 MySQL 8.0：页大小不超过 16KB 时 extent 为 1MiB，32KB 为 2MiB，64KB 为 4MiB。
     *
     * @return extent 字节数。
     */
    public int extentSizeBytes() {
        return switch (bytes) {
            case 4 * KIB, 8 * KIB, 16 * KIB -> MIB;
            case 32 * KIB -> 2 * MIB;
            case 64 * KIB -> 4 * MIB;
            default -> throw new DatabaseValidationException("validated page size became unsupported: " + bytes);
        };
    }

    /**
     * 计算每个 extent 包含的页数；FSP 分配代码必须使用该方法，不能写死 64。
     *
     * @return 每个 extent 的页数。
     */
    public int pagesPerExtent() {
        return extentSizeBytes() / bytes;
    }

    private static boolean isSupported(int bytes) {
        return bytes == 4 * KIB
                || bytes == 8 * KIB
                || bytes == 16 * KIB
                || bytes == 32 * KIB
                || bytes == 64 * KIB;
    }
}
