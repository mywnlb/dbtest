/**
 * 数据字典包，维护 schema、table、index 元数据及 DDL 状态，不直接管理物理页或 BufferFrame。
 *
 * <p>本包只通过显式接口与相邻模块协作；调用方不得绕过稳定 API 读取内部可变状态，
 * 包内实现也不得反向依赖 SQL、会话或其他上层语义。
 */
package cn.zhangyis.db.dd;
