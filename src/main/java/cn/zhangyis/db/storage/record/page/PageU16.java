package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.buf.PageGuard;

/**
 * 页内无符号 16 位大端字段读写助手。INDEX page header 与 PageDirectory 槽、next_record 字段均为 u16
 * （16KB 页内一切偏移 ≤ 65535），但 {@link PageGuard} 只提供 int(4)/long(8) 访问，故此处用 2 字节 readBytes/writeBytes 拼装，
 * 避免写 u16 时用 writeInt 覆盖相邻 2 字节。
 */
final class PageU16 {

    private PageU16() {
    }

    /** 读 at 处 2 字节大端 u16。S/X 均可（委托 PageGuard.readBytes）。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param at 参与 {@code get} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code get} 从受校验输入或持久字节中得到的 {@code int} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
     */
    static int get(PageGuard guard, int at) {
        byte[] b = guard.readBytes(at, 2);
        return ((b[0] & 0xFF) << 8) | (b[1] & 0xFF);
    }

    /** 写 at 处 2 字节大端 u16；value 须在 0..65535，否则 {@link DatabaseValidationException}。要求 X（writeBytes 自校验并置脏）。
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param at 参与 {@code put} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     * @param value 由 {@code put} 转换或编码的原始 {@code int} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static void put(PageGuard guard, int at, int value) {
        if (value < 0 || value > 0xFFFF) {
            throw new DatabaseValidationException("u16 page field out of range: " + value);
        }
        guard.writeBytes(at, new byte[]{(byte) (value >>> 8), (byte) value});
    }
}
