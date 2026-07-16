package cn.zhangyis.db.dd.service;

import cn.zhangyis.db.dd.mdl.MdlMode;

/**
 * 表元数据访问意图。它把上层“只读绑定/可能写表”的语义显式映射为 table 级 MDL，避免调用方遗漏参数时
 * 静默降级为 READ。schema 名称身份始终由 {@link MdlMode#SHARED_READ} 保护，不由本枚举改变。
 */
public enum TableAccessIntent {

    /** 只读取表定义和数据；table 名称取得 SHARED_READ。 */
    READ(MdlMode.SHARED_READ),

    /** 可能修改表数据；table 名称取得 SHARED_WRITE，阻止 DDL X 越过活动事务。 */
    WRITE(MdlMode.SHARED_WRITE);

    /** 本访问意图对应的 table 级 MDL；只供 DD service 组装请求，不向 SQL 层暴露 ticket。 */
    private final MdlMode tableMode;

    TableAccessIntent(MdlMode tableMode) {
        this.tableMode = tableMode;
    }

    /** 返回 table 名称需要取得的 MDL 模式。 */
    MdlMode tableMode() {
        return tableMode;
    }
}
