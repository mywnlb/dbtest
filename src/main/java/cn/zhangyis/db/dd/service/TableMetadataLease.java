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
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RuntimeException failure = null;
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
        if (failure != null) {
            throw failure;
        }
    }
}
