/**
 * 表空间类型、状态和 flags 值对象。
 *
 * <p>这些类型描述 fil/fsp 之间共享的表空间生命周期和类型位编码。它们不打开文件，
 * 也不驱动状态转换；状态转换必须由 api/engine 层在持有正确 lease 并更新 page0/registry 后完成。</p>
 */
package cn.zhangyis.db.storage.fil.state;
