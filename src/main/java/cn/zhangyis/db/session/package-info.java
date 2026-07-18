/**
 * 连接与会话编排包，负责命令分发、autocommit 与 prepared statement 生命周期，只经稳定 SQL/存储入口下推请求。
 *
 * <p>本包只通过显式接口与相邻模块协作；调用方不得绕过稳定 API 读取内部可变状态，
 * 包内实现也不得反向依赖 SQL、会话或其他上层语义。
 */
package cn.zhangyis.db.session;
