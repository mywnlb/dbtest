package cn.zhangyis.db.dd.ddl;

/** 通用ALTER协调器在产生identity、marker或物理副作用前冻结的执行策略。 */
public enum OnlineAlterStrategy {
    /** 只改变schema image，以短table X一次发布，不扫描基础行。 */
    INSTANT_METADATA,
    /** 只改变普通二级索引，使用gate/candidate/retirement协议。 */
    INPLACE_INDEX,
    /** 通过shadow space与全行change-log重写record layout。 */
    SHADOW_REBUILD_V1,
    /** 已有安全阻塞实现，但当前online能力不足；调用方必须明确记录选择原因。 */
    BLOCKING,
    /** 当前binder/storage均无法正确实现，必须在副作用前拒绝。 */
    UNSUPPORTED
}
