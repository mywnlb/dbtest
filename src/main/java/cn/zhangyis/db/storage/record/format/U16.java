package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 记录格式内部的 2 字节无符号大端整数读写工具（0..65535）。 */
final class U16 {

    private U16() {
    }

    /**
     * 更新 {@code put} 指定的记录格式与页内组织局部状态；写入前校验身份和范围，成功后由所属对象维护一致性。
     *
     * @param buf 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param at 参与 {@code put} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     * @param value 由 {@code put} 转换或编码的原始 {@code int} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static void put(byte[] buf, int at, int value) {
        if (value < 0 || value > 0xFFFF) {
            throw new DatabaseValidationException("u16 out of range: " + value);
        }
        buf[at] = (byte) ((value >>> 8) & 0xFF);
        buf[at + 1] = (byte) (value & 0xFF);
    }

    /**
     * 返回 {@code get} 对应的记录格式与页内组织受控对象；调用方获得使用权但不接管组合根或 owner 的生命周期。
     *
     * @param buf 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param at 参与 {@code get} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code get} 从受校验输入或持久字节中得到的 {@code int} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
     */
    static int get(byte[] buf, int at) {
        return ((buf[at] & 0xFF) << 8) | (buf[at + 1] & 0xFF);
    }
}
