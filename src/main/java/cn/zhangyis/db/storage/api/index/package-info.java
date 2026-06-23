/**
 * INDEX 页访问门面。该子包只放跨 BufferPool、MTR 和 record page 的稳定适配入口，
 * 避免 B+Tree 直接持有底层 frame 或裸页，同时不把这类内部访问门面挤在 storage.api 顶层。
 */
package cn.zhangyis.db.storage.api.index;
