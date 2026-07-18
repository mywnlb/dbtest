package cn.zhangyis.db.dd.mdl;

/** MDL 层级顺序；数值越大越接近物理对象，批量获取不得反向。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code GLOBAL}：表示“GLOBAL”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code SCHEMA}：表示“SCHEMA”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code TABLE}：表示“表”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code TABLESPACE}：表示“TABLESPACE”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 * </ul>
 */
public enum MdlNamespace {
    GLOBAL(0),
    SCHEMA(1),
    TABLE(2),
    TABLESPACE(3);

    /**
     * 记录 {@code rank} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final int rank;

    /**
     * 创建 {@code MdlNamespace}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param rank 参与 {@code 构造} 的稳定编码 {@code rank}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     */
    MdlNamespace(int rank) {
        this.rank = rank;
    }

    public int rank() {
        return rank;
    }
}
