/**
 * {@code cn.zhangyis.db.storage.btree} 包的自动化测试。
 *
 * <p>测试围绕以下生产职责建立可重复断言：B+Tree 索引包，负责查找、修改、分裂、合并与范围扫描，不直接修改 redo 文件或事务活跃表。
 * 同时覆盖正常路径、边界条件与领域异常，避免用控制台输出代替行为验证。
 */
package cn.zhangyis.db.storage.btree;
