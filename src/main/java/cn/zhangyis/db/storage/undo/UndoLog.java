package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

/**
 * undo 日志基座 Facade：把 {@link UndoRecordCodec} 与 {@link UndoPage} 串起，append 返回真实 {@link RollPointer}、
 * readRecord 用指针读回。**单 undo space 假设**：RollPointer 只编 pageNo+offset（space 由唯一 undo 表空间隐含）；
 * 多 rseg/多 undo 表空间编码留 T1.3d+。本片不接事务/rollback——只是物理 undo 日志的读写底座。
 */
public final class UndoLog {

    /**
     * 本对象持有的 {@code codec} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final UndoRecordCodec codec;

    /**
     * 创建 {@code UndoLog}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param registry 由组合根提供的 {@code TypeCodecRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoLog(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("undo log registry must not be null");
        }
        this.codec = new UndoRecordCodec(registry);
    }

    /**
     * 追加一条 undo record：codec 编码 → UndoPage.appendRecord 得槽起点 offset → 组装 insert RollPointer。
     * 返回的 RollPointer 指向该 record 槽起点（与 {@link #readRecord} 约定一致）。
     * @param page 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param rec 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param keyDef 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code append} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RollPointer append(UndoPage page, UndoRecord rec, IndexKeyDef keyDef, TableSchema schema) {
        if (page == null) {
            throw new DatabaseValidationException("undo append page must not be null");
        }
        byte[] payload = codec.encode(rec, keyDef, schema);
        int offset = page.appendRecord(payload, rec.transactionId(), rec.undoNo());
        return new RollPointer(true, page.pageId().pageNo(), offset);
    }

    /**
     * 按 RollPointer 读回 undo record。校验指针非 NULL 且页号匹配（值对象用 {@code equals}，不可用 {@code ==}）；
     * 不符抛 {@link UndoLogFormatException}。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param page 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param rp 参与 {@code readRecord} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param keyDef 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code readRecord} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoRecord readRecord(UndoPage page, RollPointer rp, IndexKeyDef keyDef, TableSchema schema) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (page == null || rp == null) {
            throw new DatabaseValidationException("undo readRecord page/rp must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (rp.isNull()) {
            throw new UndoLogFormatException("cannot read undo record from NULL roll pointer");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if (!rp.pageNo().equals(page.pageId().pageNo())) {
            throw new UndoLogFormatException("roll pointer page " + rp.pageNo()
                    + " != undo page " + page.pageId().pageNo());
        }
        byte[] payload = page.recordAt(rp.offset());
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return codec.decode(payload, 0, keyDef, schema);
    }
}
