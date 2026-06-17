/**
 * fil 物理文件层：表空间数据文件、按 PageId 的整页读写、文件扩展（autoextend）和物理 IO 锁（生命周期闩 / 文件大小锁）。
 * 纯物理视角，不解析页内容、不理解 record/segment/事务语义（设计 §10）。
 */
package cn.zhangyis.db.storage.fil;
