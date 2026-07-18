/**
 * {@code cn.zhangyis.db.storage.api} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：存储引擎稳定门面包，向 SQL 与字典层暴露领域 API，同时隐藏 frame、裸文件与 redo buffer。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.storage.api;
