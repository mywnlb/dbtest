/**
 * SQL 执行包，以算子生命周期驱动物理计划并生成结果集，只通过 storage API 访问数据。
 *
 * <p>本包只通过显式接口与相邻模块协作；调用方不得绕过稳定 API 读取内部可变状态，
 * 包内实现也不得反向依赖 SQL、会话或其他上层语义。
 */
package cn.zhangyis.db.sql.executor;
