package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;

import java.util.List;

/**
 * control/catalog 在修改各自权威文件前写入的灾难恢复 witness。
 *
 * <p>该接口位于 repo 层，使 control 与 catalog 只依赖窄端口，不反向依赖 recovery manifest 实现。
 * 回调必须在权威文件写入前 durable 返回；失败通过领域异常中止后续修改。实现不得回调 repository、
 * 获取 MDL/page latch 或等待事务锁。</p>
 */
public interface DictionaryDurabilityWitness {

    /**
     * 在 control 高水位写盘前记录目标快照。目标 counters 可以保守偏大，但不能低于即将发布的槽。
     *
     * @param target 即将写入非当前 control slot 的完整目标快照；不得为 {@code null}
     */
    void beforeControlReservation(DictionaryControlSnapshot target);

    /**
     * 在 catalog mutation batch append 前记录目标版本和实际编码记录。
     *
     * @param version 即将提交的严格递增 dictionary version
     * @param records 已完成边界校验、随后会原样交给 catalog store 的不可空记录集合
     */
    void beforeCatalogMutation(DictionaryVersion version, List<CatalogRecord> records);

    /**
     * 返回不产生 sidecar 的兼容实现。只供既有低层单元测试和未接公共组合根的独立 repository 使用。
     *
     * @return 无副作用、线程安全的共享 witness
     */
    static DictionaryDurabilityWitness noOp() {
        return NoOpHolder.INSTANCE;
    }

    /** 初始化时持有唯一无状态实例，避免每个测试 repository 创建匿名对象。 */
    final class NoOpHolder {
        private static final DictionaryDurabilityWitness INSTANCE = new DictionaryDurabilityWitness() {
            @Override
            public void beforeControlReservation(DictionaryControlSnapshot target) {
                // 兼容路径没有独立 disaster-recovery witness。
            }

            @Override
            public void beforeCatalogMutation(DictionaryVersion version, List<CatalogRecord> records) {
                // 兼容路径没有独立 disaster-recovery witness。
            }
        };

        private NoOpHolder() {
        }
    }
}
