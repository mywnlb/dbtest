package cn.zhangyis.db.dd.service;

import cn.zhangyis.db.dd.cache.DictionaryPin;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.mdl.MdlTicket;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 一次安全表访问的 RAII 租约。它把 schema/table 名称身份的 MDL 与一个不可变 DD table version pin
 * 绑定为同一生命周期，执行器不得只保存裸 {@link TableDefinition} 跨语句使用。
 */
public final class TableMetadataLease implements AutoCloseable {

    /** 被本租约持有的版本 pin；DROP publish 后仍可读完该不可变版本。 */
    private final DictionaryPin<TableDefinition> pin;

    /** 按 global/schema/table 层级取得的 ticket；释放时必须反序。 */
    private final List<MdlTicket> tickets;

    /** 保障重复 close 不会重复释放 pin/ticket。 */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * 创建 {@code TableMetadataLease}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pin 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param tickets 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    TableMetadataLease(DictionaryPin<TableDefinition> pin, List<MdlTicket> tickets) {
        this.pin = pin;
        this.tickets = List.copyOf(tickets);
    }

    /** 返回本次执行固定看到的不可变表定义。 */
    public TableDefinition table() {
        return pin.value();
    }

    /** 返回被 pin 的 DD 版本，供 plan/cache 一致性诊断。 */
    public DictionaryVersion version() {
        return pin.version();
    }

    /** 新版本发布后标记为 stale，但既有执行仍可在 close 前安全读取。 */
    public boolean stale() {
        return pin.stale();
    }

    /**
     * 先释放 cache pin，再按 table→schema 的反序释放 MDL。MDL 仍覆盖 pin 释放动作，因此 DROP 获得 X 时
     * 已经看不到本执行者的旧 pin；任一 close 异常不会阻止其余 ticket 释放。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     */
    @Override
    public void close() {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        RuntimeException failure = null;
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
        try {
            pin.close();
        } catch (RuntimeException closeFailure) {
            failure = closeFailure;
        }
        for (int i = tickets.size() - 1; i >= 0; i--) {
            try {
                tickets.get(i).close();
            } catch (RuntimeException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        if (failure != null) {
            throw failure;
        }
    }
}
