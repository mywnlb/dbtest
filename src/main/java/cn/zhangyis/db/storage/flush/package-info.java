/**
 * 刷脏、doublewrite 与 checkpoint 协作包，确保任何数据页写盘前其 page LSN 已满足 WAL。
 *
 * <p>本包只通过显式接口与相邻模块协作；调用方不得绕过稳定 API 读取内部可变状态，
 * 包内实现也不得反向依赖 SQL、会话或其他上层语义。
 */
package cn.zhangyis.db.storage.flush;
