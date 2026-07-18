/**
 * {@code cn.zhangyis.db.storage.fil} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：表空间文件与物理 IO 包，负责页读写、扩容和文件锁，不解释 record 或事务语义。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.storage.fil;
