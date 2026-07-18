/**
 * 存储引擎稳定门面包，向 SQL 与字典层暴露领域 API，同时隐藏 frame、裸文件与 redo buffer。
 *
 * <p>本包只通过显式接口与相邻模块协作；调用方不得绕过稳定 API 读取内部可变状态，
 * 包内实现也不得反向依赖 SQL、会话或其他上层语义。
 */
package cn.zhangyis.db.storage.api;
