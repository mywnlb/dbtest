package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * undo record 类型。{@code code} 作为 undo record 首字节落盘（稳定，{@code UndoRecordCodecTest} 钉死）；
 * code 从 1 起（0 留作「非法/零页」可检测）。三类 payload 均已实现，并由 v2 {@link UndoLogKind} 同步约束。
 */
public enum UndoRecordType {
    /** 插入未提交行的撤销：rollback 时按 cluster key 物理删除该插入。 */
    INSERT_ROW(1),
    /** 更新前镜像（T1.3d）。 */
    UPDATE_ROW(2),
    /** delete-mark 前镜像（T1.3d）。 */
    DELETE_MARK(3);

    /**
     * 记录 {@code code} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final int code;

    /**
     * 创建 {@code UndoRecordType}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param code 参与 {@code 构造} 的稳定编码 {@code code}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     */
    UndoRecordType(int code) {
        this.code = code;
    }

    /** 落盘 code（undo record 首字节）。 */
    public int code() {
        return code;
    }

    /** 由落盘 code 还原；未知 code 视为 undo record 类型损坏。
     *
     * @param code 参与 {@code fromCode} 的稳定编码 {@code code}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     * @return {@code fromCode} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static UndoRecordType fromCode(int code) {
        for (UndoRecordType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new DatabaseValidationException("unknown undo record type code: " + code);
    }
}
