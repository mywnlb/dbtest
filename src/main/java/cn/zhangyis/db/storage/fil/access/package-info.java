/**
 * 表空间操作级准入控制。
 *
 * <p>{@code TablespaceAccessController} 提供按 SpaceId 划分的 S/X lease：
 * 普通页访问和 flush 持 S lease，truncate/discard 等生命周期变更持 X lease。
 * lease 只表达运行期互斥，不替代 page0 持久状态和 registry 复核。</p>
 */
package cn.zhangyis.db.storage.fil.access;
