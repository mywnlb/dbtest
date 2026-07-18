/**
 * 空间管理包，负责 space、extent、segment 与 page 的分配释放，不直接操作 FileChannel 或解析记录。
 *
 * <p>本包只通过显式接口与相邻模块协作；调用方不得绕过稳定 API 读取内部可变状态，
 * 包内实现也不得反向依赖 SQL、会话或其他上层语义。
 */
package cn.zhangyis.db.storage.fsp;
