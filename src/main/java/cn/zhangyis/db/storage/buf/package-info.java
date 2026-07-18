/**
 * 缓冲池包，维护 frame、fix、页闩、替换与脏页状态，不解析 record、segment 或 SQL 语义。
 *
 * <p>本包只通过显式接口与相邻模块协作；调用方不得绕过稳定 API 读取内部可变状态，
 * 包内实现也不得反向依赖 SQL、会话或其他上层语义。
 */
package cn.zhangyis.db.storage.buf;
