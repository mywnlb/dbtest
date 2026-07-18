/**
 * Redo/WAL 包，维护 LSN、日志缓冲、writer/flusher 与幂等重放，不调用事务锁或具体 repository 实现。
 *
 * <p>本包只通过显式接口与相邻模块协作；调用方不得绕过稳定 API 读取内部可变状态，
 * 包内实现也不得反向依赖 SQL、会话或其他上层语义。
 */
package cn.zhangyis.db.storage.redo;
