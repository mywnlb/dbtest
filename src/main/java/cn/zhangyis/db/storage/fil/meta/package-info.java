/**
 * 表空间运行期元数据与 registry。
 *
 * <p>本包维护表空间路径、页大小、当前文件大小、类型、运行期状态和打开句柄快照。
 * page0 仍是持久元数据权威；registry 只缓存当前进程可用的运行期视图，并由上层门面在持有
 * access lease 后复核。</p>
 */
package cn.zhangyis.db.storage.fil.meta;
