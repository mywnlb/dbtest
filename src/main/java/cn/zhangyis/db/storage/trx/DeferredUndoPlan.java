package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.undo.UndoRecord;
import cn.zhangyis.db.storage.undo.UndoRecordWritePlan;

import java.util.List;

/**
 * INSERT/UPDATE 在真实 LOB 首页号未知时共享的 deferred undo 物理形状协议。实现必须保证 placeholder 永远不会
 * 发布到 undo slot，并且 actual record 只替换定宽 reference first-page 字段。
 *
 * @param <T> actual ownership 元素类型；INSERT 使用 inserted ownership，UPDATE 使用 LOB version ownership。
 */
interface DeferredUndoPlan<T> {

    /** @return 已冻结 acquisition、页数和 placeholder 编码的普通 undo 计划。 */
    UndoWritePlan placeholderPlan();

    /** @return 解释聚簇主键的 exact-version key definition。 */
    IndexKeyDef keyDef();

    /** @return 解释旧 image/ownership envelope 的 exact-version schema。 */
    TableSchema schema();

    /**
     * 将真实 ownership 替换进 placeholder logical record；实现必须逐字段拒绝形状变化。
     *
     * @param actualOwnerships 业务 MTR 刚写出的真实 ownership，数量和顺序必须与 placeholder 一致。
     * @return identity、旧 image 和非 LOB tail 不变的 actual undo record。
     * @throws UndoWriteStalePlanException ownership 数量、类型、segment、长度、CRC 或 prefix 发生变化时抛出。
     */
    UndoRecord actualRecord(List<T> actualOwnerships);

    /**
     * 复核 actual 编码没有改变 inline/external 分支、payload 页数或 slot 长度。
     *
     * @param actual 已用 exact codec 冻结的 actual record 写计划。
     * @throws UndoWriteStalePlanException actual 与 placeholder 物理形状不一致时抛出。
     */
    void requireSamePhysicalShape(UndoRecordWritePlan actual);
}
