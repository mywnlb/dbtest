package cn.zhangyis.db.dd.mdl;

/** MDL 层级顺序；数值越大越接近物理对象，批量获取不得反向。 */
public enum MdlNamespace {
    GLOBAL(0),
    SCHEMA(1),
    TABLE(2),
    TABLESPACE(3);

    private final int rank;

    MdlNamespace(int rank) {
        this.rank = rank;
    }

    public int rank() {
        return rank;
    }
}
