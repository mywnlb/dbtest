/**
 * 表空间 metadata 适配器。这里的实现面向 api 侧组合 fil registry 与 fsp page0 编解码，
 * 避免 fil/fsp 底层包互相承担跨模块编排职责。
 */
package cn.zhangyis.db.storage.api.tablespace;
