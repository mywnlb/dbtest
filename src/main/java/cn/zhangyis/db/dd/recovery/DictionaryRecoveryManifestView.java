package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.repo.DictionaryControlSnapshot;

import java.util.Optional;

/**
 * 从全部 committed manifest event 推导出的不可变恢复视图。
 *
 * @param lastSequence 当前 manifest journal 最后一个 committed batch；空 journal 为 0
 * @param latestClean 最近一个完整 clean snapshot；缺失表示从未形成可重建基线
 * @param safeControl 所有 clean/control reservation 的逐分量最大高水位
 * @param unresolvedCatalogMutation 最近 clean 未覆盖的 catalog mutation intent 是否存在
 */
public record DictionaryRecoveryManifestView(
        long lastSequence,
        Optional<DictionaryRecoveryCleanSnapshot> latestClean,
        Optional<DictionaryControlSnapshot> safeControl,
        boolean unresolvedCatalogMutation) {

    public DictionaryRecoveryManifestView {
        if (lastSequence < 0 || latestClean == null || safeControl == null) {
            throw new DatabaseValidationException("dictionary recovery manifest view is invalid");
        }
    }

    /**
     * 判断该 manifest 是否足以授权 catalog-loss 重建。
     *
     * @return 存在 clean snapshot 且其后没有未裁决 catalog mutation intent 时为 {@code true}
     */
    public boolean rebuildable() {
        return latestClean.isPresent() && !unresolvedCatalogMutation;
    }
}
