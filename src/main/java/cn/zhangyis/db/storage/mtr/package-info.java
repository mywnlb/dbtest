/**
 * Mini Transaction 包，以短物理临界区管理 latch、fix memo 与 redo 收集，不承担数据库事务提交语义。
 *
 * <p>本包只通过显式接口与相邻模块协作；调用方不得绕过稳定 API 读取内部可变状态，
 * 包内实现也不得反向依赖 SQL、会话或其他上层语义。
 */
package cn.zhangyis.db.storage.mtr;
