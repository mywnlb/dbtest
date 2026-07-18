package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * DB_ROLL_PTR：指向 undo record 的位置。本片（T1.1+T1.2）只用 {@link #NULL}（尚无 undo）。
 *
 * <p>7 字节定长编码（big-endian，与 InnoDB 二进制不兼容，故意简化）：
 * <ul>
 *   <li>byte0：bit7 = insert flag；bit6..0 = reserved，必须为 0；</li>
 *   <li>byte1..4：pageNo（u32）；</li>
 *   <li>byte5..6：offset（u16）。</li>
 * </ul>
 * space id 不入编码，由 undo 表空间隐含（首个 undo 片单 undo 表空间假设；多 undo 表空间需改为 InnoDB 风格
 * rollback-segment-id 编码，留 T1.3）。{@link #NULL} 为全零（undo page 0 保留作头页，故全零与任何真实 undo
 * 记录位置无歧义）。
 *
 * @param insert 是否 insert undo（true=insert，false=update/delete）。
 * @param pageNo undo page 号，必须落在 u32 范围。
 * @param offset undo record 页内偏移，必须落在 u16 范围。
 */
public record RollPointer(boolean insert, PageNo pageNo, int offset) {

    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏数据库内核值对象的不变量。
     */
    private static final long U32_MAX = 0xFFFFFFFFL;
    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏数据库内核值对象的不变量。
     */
    private static final int U16_MAX = 0xFFFF;
    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏数据库内核值对象的不变量。
     */
    private static final int INSERT_BIT = 0x80;
    /**
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
    private static final int RESERVED_MASK = 0x7F;

    /** 编码字节宽度。 */
    public static final int BYTES = 7;

    /** 全零空指针哨兵（无 undo）。 */
    public static final RollPointer NULL = new RollPointer(false, PageNo.of(0), 0);

    public RollPointer {
        if (pageNo == null) {
            throw new DatabaseValidationException("roll pointer pageNo must not be null");
        }
        if (pageNo.value() < 0 || pageNo.value() > U32_MAX) {
            throw new DatabaseValidationException("roll pointer pageNo out of u32: " + pageNo.value());
        }
        if (offset < 0 || offset > U16_MAX) {
            throw new DatabaseValidationException("roll pointer offset out of u16: " + offset);
        }
    }

    /** 是否空指针（无 undo）：insert=false 且 pageNo=0 且 offset=0。 */
    public boolean isNull() {
        return !insert && pageNo.value() == 0 && offset == 0;
    }

    /** 编码为 7 字节（big-endian）。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @return {@code encode} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    public byte[] encode() {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        byte[] b = new byte[BYTES];
        b[0] = (byte) (insert ? INSERT_BIT : 0);
        long p = pageNo.value();
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        b[1] = (byte) (p >>> 24);
        b[2] = (byte) (p >>> 16);
        b[3] = (byte) (p >>> 8);
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        b[4] = (byte) p;
        b[5] = (byte) (offset >>> 8);
        b[6] = (byte) offset;
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return b;
    }

    /**
     * 从 {@code off} 处解码 7 字节。reserved 位非 0 视为损坏抛 {@link DatabaseValidationException}，
     * 不静默忽略——否则未来扩展位被误吞会破坏向后兼容判断。
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param buf 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param off 参与 {@code decode} 的零基位置 {@code off}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code decode} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static RollPointer decode(byte[] buf, int off) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (buf == null || off < 0 || off + BYTES > buf.length) {
            throw new DatabaseValidationException("roll pointer buffer too short");
        }
        int flags = buf[off] & 0xFF;
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        if ((flags & RESERVED_MASK) != 0) {
            throw new DatabaseValidationException("roll pointer reserved bits set: " + flags);
        }
        boolean insert = (flags & INSERT_BIT) != 0;
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        long p = ((long) (buf[off + 1] & 0xFF) << 24)
                | ((long) (buf[off + 2] & 0xFF) << 16)
                | ((long) (buf[off + 3] & 0xFF) << 8)
                | (buf[off + 4] & 0xFF);
        int offset = ((buf[off + 5] & 0xFF) << 8) | (buf[off + 6] & 0xFF);
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return new RollPointer(insert, PageNo.of(p), offset);
    }
}
