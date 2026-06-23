/**
 * fil 层领域异常。
 *
 * <p>本包中的异常用于表达表空间文件缺失、越界、损坏、未打开或运行期不可用等物理层失败。
 * 捕获底层 IO 或校验异常时必须保留 cause，便于恢复和故障诊断定位真实根因。</p>
 */
package cn.zhangyis.db.storage.fil.exception;
