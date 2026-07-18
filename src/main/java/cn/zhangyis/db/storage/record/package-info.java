/**
 * 记录格式包，负责页内字段编码、隐藏列与 Page Directory，不分配物理页或持有长期事务锁。
 *
 * <p>本包只通过显式接口与相邻模块协作；调用方不得绕过稳定 API 读取内部可变状态，
 * 包内实现也不得反向依赖 SQL、会话或其他上层语义。
 */
package cn.zhangyis.db.storage.record;
