package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.page.SearchKey;

import java.util.Optional;

/**
 * B+Tree 扫描边界。空 Optional 分别表示负无穷/正无穷；边界 key 可以是完整 physical key，
 * 也可以是 logical prefix。短 key 比较相等时，inclusive 覆盖整个 suffix 域。
 */
public final class BTreeScanRange {
    /** 可选下界；empty 时扫描必须从最左 leaf 开始。 */
    private final Optional<SearchKey> lowerBound;
    /** 下界相等 key 是否包含。 */
    private final boolean lowerInclusive;
    /** 可选上界；empty 时扫描延伸到最右 leaf。 */
    private final Optional<SearchKey> upperBound;
    /** 上界相等 key 是否包含。 */
    private final boolean upperInclusive;
    /** 本次最多物化的记录数；零表示只做参数校验。 */
    private final int limit;

    /**
     * 保留既有有界构造入口。
     *
     * @param lowerKey 非空下界
     * @param lowerInclusive 是否包含下界
     * @param upperKey 非空上界
     * @param upperInclusive 是否包含上界
     * @param limit 最大物化数；必须非负
     */
    public BTreeScanRange(SearchKey lowerKey, boolean lowerInclusive,
                          SearchKey upperKey, boolean upperInclusive, int limit) {
        this(Optional.ofNullable(lowerKey), lowerInclusive,
                Optional.ofNullable(upperKey), upperInclusive, limit);
        if (lowerKey == null || upperKey == null) {
            throw new DatabaseValidationException(
                    "bounded btree scan lower/upper keys must not be null");
        }
    }

    private BTreeScanRange(Optional<SearchKey> lowerBound, boolean lowerInclusive,
                           Optional<SearchKey> upperBound, boolean upperInclusive, int limit) {
        if (lowerBound == null || upperBound == null || limit < 0) {
            throw new DatabaseValidationException("invalid btree scan bounds/limit");
        }
        this.lowerBound = lowerBound;
        this.lowerInclusive = lowerInclusive;
        this.upperBound = upperBound;
        this.upperInclusive = upperInclusive;
        this.limit = limit;
    }

    /** @return 覆盖整棵索引的无界范围。 */
    public static BTreeScanRange unbounded(int limit) {
        return new BTreeScanRange(Optional.empty(), true, Optional.empty(), true, limit);
    }

    /**
     * 创建 continuation 范围；上一批最后一个完整 physical key 必须排除，防止重复行。
     *
     * @param lastPhysicalKey 上一批最后一个物理 key
     * @param limit 下一批最大物化数
     * @return 无上界、exclusive lower 的 continuation
     */
    public static BTreeScanRange after(SearchKey lastPhysicalKey, int limit) {
        if (lastPhysicalKey == null) {
            throw new DatabaseValidationException("scan continuation key must not be null");
        }
        return new BTreeScanRange(Optional.of(lastPhysicalKey), false,
                Optional.empty(), true, limit);
    }

    /**
     * 创建任意 optional 边界范围。
     *
     * @param lowerBound 可选下界
     * @param lowerInclusive 下界开闭性
     * @param upperBound 可选上界
     * @param upperInclusive 上界开闭性
     * @param limit 最大物化数
     * @return 已冻结的扫描范围
     */
    public static BTreeScanRange of(Optional<SearchKey> lowerBound, boolean lowerInclusive,
                                    Optional<SearchKey> upperBound, boolean upperInclusive,
                                    int limit) {
        return new BTreeScanRange(lowerBound, lowerInclusive, upperBound, upperInclusive, limit);
    }

    public Optional<SearchKey> lowerBound() {
        return lowerBound;
    }

    public boolean lowerInclusive() {
        return lowerInclusive;
    }

    public Optional<SearchKey> upperBound() {
        return upperBound;
    }

    public boolean upperInclusive() {
        return upperInclusive;
    }

    public int limit() {
        return limit;
    }
}
