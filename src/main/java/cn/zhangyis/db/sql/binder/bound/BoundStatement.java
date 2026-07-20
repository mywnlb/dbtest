package cn.zhangyis.db.sql.binder.bound;

/** Binder 输出的封闭执行意图；v1 不创建伪 optimizer 或可变 plan cache 对象。 */
public sealed interface BoundStatement permits BoundClusteredInsert, BoundPointSelect, BoundSecondaryRangeSelect,
        BoundUpdate, BoundDelete, BoundCreateIndex, BoundDropIndex {
}
