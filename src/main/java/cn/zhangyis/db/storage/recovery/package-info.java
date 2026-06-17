/**
 * crash recovery 启动编排模块。它只负责恢复阶段顺序、用户流量门控和诊断结果，
 * 具体 doublewrite 修复、redo replay、事务 rollback 与 purge resume 由各自模块提供稳定接口。
 */
package cn.zhangyis.db.storage.recovery;
