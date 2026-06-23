/**
 * fil 层物理资源锁与 RAII guard。
 *
 * <p>本包表达数据文件生命周期、文件大小、fsync、页 IO 范围等物理资源的互斥关系。
 * 锁粒度贴合物理文件资源，不能承载 record/事务锁语义；阻塞等待必须通过显式并发工具完成。</p>
 */
package cn.zhangyis.db.storage.fil.lock;
