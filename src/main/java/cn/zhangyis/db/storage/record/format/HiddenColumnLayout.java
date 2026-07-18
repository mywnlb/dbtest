package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;

/**
 * 聚簇记录尾部隐藏区编解码（15 字节，innodb-record-design §6）：
 * DB_TRX_ID（8 字节无符号 big-endian）+ DB_ROLL_PTR（7 字节，见 {@link RollPointer}）。
 *
 * <p>仅聚簇 leaf CONVENTIONAL 记录写此区，贴在用户字段区之后、作为记录字节的尾部 15 字节；
 * node-pointer/非聚簇/系统记录无隐藏区。隐藏区是否存在由 schema 的 clustered 标志决定，由 encoder/decoder 控制。
 */
public final class HiddenColumnLayout {

    /** 隐藏区字节宽度 = 8(trx id) + 7(roll ptr)。 */
    public static final int HIDDEN_BYTES = 15;
    /**
     * 持久结构布局常量；它定义 {@code HiddenColumnLayout} 中 {@code DB_TRX_ID_OFFSET} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    private static final int DB_TRX_ID_OFFSET = 0;
    /**
     * 持久结构布局常量；它定义 {@code HiddenColumnLayout} 中 {@code DB_ROLL_PTR_OFFSET} 的固定偏移、槽位或宽度，读写两端必须使用同一数值。
     */
    private static final int DB_ROLL_PTR_OFFSET = 8;

    private HiddenColumnLayout() {
    }

    /** 在 {@code off} 处写 8 字节 trxId + 7 字节 rollPtr。
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param buf 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param off 参与 {@code encode} 的零基位置 {@code off}；必须非负且小于所属页面、集合或持久结构的容量
     * @param trxId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @param rollPtr 参与 {@code encode} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static void encode(byte[] buf, int off, TransactionId trxId, RollPointer rollPtr) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (buf == null || off < 0 || off + HIDDEN_BYTES > buf.length) {
            throw new DatabaseValidationException("hidden area buffer too short");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        long v = trxId.value();
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        for (int i = 0; i < 8; i++) {
            buf[off + DB_TRX_ID_OFFSET + i] = (byte) (v >>> (56 - 8 * i));
        }
        byte[] rp = rollPtr.encode();
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        System.arraycopy(rp, 0, buf, off + DB_ROLL_PTR_OFFSET, RollPointer.BYTES);
    }

    /** 从 {@code off} 处解码 DB_TRX_ID（8 字节无符号）。
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param buf 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param off 参与 {@code decodeTrxId} 的零基位置 {@code off}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code decodeTrxId} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static TransactionId decodeTrxId(byte[] buf, int off) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (buf == null || off < 0 || off + HIDDEN_BYTES > buf.length) {
            throw new DatabaseValidationException("hidden area buffer too short");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        long v = 0;
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (buf[off + DB_TRX_ID_OFFSET + i] & 0xFFL);
        }
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return TransactionId.of(v);
    }

    /** 从 {@code off} 处解码 DB_ROLL_PTR（7 字节）。
     * @param buf 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param off 参与 {@code decodeRollPtr} 的零基位置 {@code off}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code decodeRollPtr} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public static RollPointer decodeRollPtr(byte[] buf, int off) {
        return RollPointer.decode(buf, off + DB_ROLL_PTR_OFFSET);
    }
}
