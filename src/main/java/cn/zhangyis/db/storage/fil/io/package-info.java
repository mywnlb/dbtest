/**
 * 表空间数据文件与整页 IO。
 *
 * <p>本包只处理文件创建、打开、扩容、截断、读写和 fsync 等物理动作。
 * 它不读取 registry 状态，也不解释 page0、record、segment 或事务语义；调用方必须在进入
 * {@link cn.zhangyis.db.storage.fil.io.PageStore} 前完成表空间状态准入和 WAL/flush 顺序控制。</p>
 */
package cn.zhangyis.db.storage.fil.io;
