package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;

/**
 * MySQL 风格 4-bit/page IBUF bitmap 的纯寻址与位运算。页面 envelope/body 读写由 repository 负责，本类不访问 IO。
 */
public final class ChangeBufferBitmapLayout {

    /** nibble 低两位：连续可用空间等级。 */
    private static final int FREE_CLASS_MASK = 0b0011;
    /** nibble bit2：目标页存在 buffered mutation。 */
    private static final int BUFFERED_MASK = 0b0100;
    /** nibble bit3：目标页是 Change Buffer 内部页。 */
    private static final int INTERNAL_MASK = 0b1000;

    private ChangeBufferBitmapLayout() {
    }

    /**
     * 计算目标页所属重复 bitmap 页：{@code 1 + (pageNo & ~(physicalPageBytes-1))}。
     *
     * @param pageSize 实例物理页大小，必须为受支持的二次幂
     * @param targetPageNo 目标页号，必须非负
     * @return 承载目标 nibble 的 bitmap 页号
     */
    public static PageNo bitmapPageNo(PageSize pageSize, PageNo targetPageNo) {
        requireInputs(pageSize, targetPageNo);
        long mask = ~((long) pageSize.bytes() - 1L);
        return PageNo.of(1L + (targetPageNo.value() & mask));
    }

    /**
     * 计算 nibble 所在 bitmap body 的 byte 相对偏移；一个 byte 容纳偶数页和奇数页两个 entry。
     *
     * @param pageSize 实例物理页大小
     * @param targetPageNo 目标页号
     * @return 从 bitmap body 起点计算的非负 byte offset
     */
    public static int byteOffsetInBody(PageSize pageSize, PageNo targetPageNo) {
        long relative = relativeTargetPage(pageSize, targetPageNo);
        return Math.toIntExact(relative >>> 1);
    }

    /**
     * 返回目标 nibble 在 byte 内的右移位数。
     *
     * @param pageSize 实例物理页大小
     * @param targetPageNo 目标页号
     * @return 偶数相对页为 0，奇数相对页为 4
     */
    public static int nibbleShift(PageSize pageSize, PageNo targetPageNo) {
        return (relativeTargetPage(pageSize, targetPageNo) & 1L) == 0L ? 0 : 4;
    }

    /**
     * 判断某页号是否是重复 Change Buffer bitmap 固定页，供 Buffer Pool 发布拦截器避免递归读取自身。
     *
     * @param pageSize 实例固定页大小
     * @param pageNo 待分类页号
     * @return 页号满足 {@code 1 + k * pageSize.bytes()} 时为 {@code true}
     */
    public static boolean isBitmapPage(PageSize pageSize, PageNo pageNo) {
        requireInputs(pageSize, pageNo);
        long value = pageNo.value();
        return value >= 1L && ((value - 1L) & ((long) pageSize.bytes() - 1L)) == 0L;
    }

    /**
     * 判断目标页是否落在 bootstrap 的首张 bitmap 覆盖区间。
     *
     * <p>该方法只保留给格式边界测试和 legacy 诊断；生产 eligibility 已使用重复管理区公式，不能再据此把
     * 后续已格式化 bitmap 区间错误降级为直写。</p>
     *
     * @param pageSize 实例固定物理页大小；不得为 {@code null}
     * @param targetPageNo 待判断目标页号；不得为 {@code null}
     * @return 目标页由 bootstrap page 1 bitmap 覆盖时为 {@code true}
     */
    public static boolean isBootstrapBitmapCovered(PageSize pageSize, PageNo targetPageNo) {
        return bitmapPageNo(pageSize, targetPageNo).value() == 1L;
    }

    /**
     * 把三个逻辑字段压成低四位。
     *
     * @param freeSpaceClass 0..3 的保守等级
     * @param buffered 是否存在 pending record
     * @param internal 是否为 ibuf 内部页
     * @return 0..15 的 nibble
     */
    public static int encodeNibble(int freeSpaceClass, boolean buffered, boolean internal) {
        ChangeBufferBitmapState state = new ChangeBufferBitmapState(freeSpaceClass, buffered, internal);
        int result = state.freeSpaceClass();
        if (state.buffered()) {
            result |= BUFFERED_MASK;
        }
        if (state.changeBufferInternal()) {
            result |= INTERNAL_MASK;
        }
        return result;
    }

    /**
     * 从已读取 byte 的指定半字节解码状态。
     *
     * @param packed bitmap body 中的原始 byte
     * @param shift 只能是 0 或 4
     * @return 独立不可变状态
     * @throws DatabaseValidationException shift 非法时抛出
     */
    public static ChangeBufferBitmapState decodeNibble(byte packed, int shift) {
        if (shift != 0 && shift != 4) {
            throw new DatabaseValidationException("change buffer bitmap nibble shift must be 0 or 4: " + shift);
        }
        int nibble = (Byte.toUnsignedInt(packed) >>> shift) & 0xF;
        return new ChangeBufferBitmapState(nibble & FREE_CLASS_MASK,
                (nibble & BUFFERED_MASK) != 0, (nibble & INTERNAL_MASK) != 0);
    }

    /**
     * 按 {@code pageSize/32} 单位把连续空闲字节转成保守两位等级；精确三个单位降为 2。
     *
     * @param pageSize 实例页大小
     * @param freeBytes 页内当前连续可用字节，必须非负且不超过页大小
     * @return 0..3 的持久等级
     */
    public static int freeSpaceClass(PageSize pageSize, int freeBytes) {
        if (pageSize == null) {
            throw new DatabaseValidationException("change buffer bitmap page size must not be null");
        }
        if (freeBytes < 0 || freeBytes > pageSize.bytes()) {
            throw new DatabaseValidationException("change buffer free bytes out of page range: " + freeBytes);
        }
        int units = freeBytes / (pageSize.bytes() / 32);
        if (units <= 2) {
            return units;
        }
        return units == 3 ? 2 : 3;
    }

    /**
     * 返回一个等级可安全承诺的空闲字节下界。
     *
     * @param pageSize 实例页大小
     * @param freeSpaceClass bitmap 中的 0..3 等级
     * @return eligibility 可使用的保守下界；等级 3 从四个单位起算
     */
    public static int freeSpaceLowerBound(PageSize pageSize, int freeSpaceClass) {
        ChangeBufferBitmapState state = new ChangeBufferBitmapState(freeSpaceClass, false, false);
        if (pageSize == null) {
            throw new DatabaseValidationException("change buffer bitmap page size must not be null");
        }
        int units = state.freeSpaceClass() == 3 ? 4 : state.freeSpaceClass();
        return units * (pageSize.bytes() / 32);
    }

    private static long relativeTargetPage(PageSize pageSize, PageNo targetPageNo) {
        requireInputs(pageSize, targetPageNo);
        long bitmap = bitmapPageNo(pageSize, targetPageNo).value();
        long base = bitmap - 1L;
        long relative = targetPageNo.value() - base;
        if (relative < 0 || relative >= pageSize.bytes()) {
            throw new DatabaseValidationException("target page is outside computed change buffer bitmap range: "
                    + targetPageNo.value());
        }
        return relative;
    }

    private static void requireInputs(PageSize pageSize, PageNo targetPageNo) {
        if (pageSize == null || targetPageNo == null) {
            throw new DatabaseValidationException("change buffer bitmap page size/page no must not be null");
        }
    }
}
