/**
 * SQL 各阶段共享的不可变值与真值类型。该包不拥有解析、计划或执行生命周期，
 * 防止 Binder/Optimizer 为使用 typed value 反向依赖 Executor。
 */
package cn.zhangyis.db.sql.type;
