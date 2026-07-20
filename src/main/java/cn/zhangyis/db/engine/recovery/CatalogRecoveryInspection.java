package cn.zhangyis.db.engine.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.recovery.DictionaryRecoveryCleanSnapshot;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFullScrubResult;

import java.util.List;
import java.util.Optional;

/**
 * 一次实例锁内完整 inspection 的不可变输出。
 *
 * @param status 顶层恢复裁决
 * @param manifest 实际用于对比候选的 clean manifest
 * @param candidates 成功完成全页 scrub 的候选
 * @param conflicts 排序后的全部冲突
 * @param token 完整枚举且 manifest 可解释时签发；缺失时禁止 quarantine/rebuild
 */
public record CatalogRecoveryInspection(
        CatalogRecoveryStatus status,
        Optional<DictionaryRecoveryCleanSnapshot> manifest,
        List<TablespaceFullScrubResult> candidates,
        List<CatalogRecoveryConflict> conflicts,
        Optional<CatalogRecoveryToken> token) {

    public CatalogRecoveryInspection {
        if (status == null || manifest == null || candidates == null || conflicts == null || token == null) {
            throw new DatabaseValidationException("catalog recovery inspection is invalid");
        }
        candidates = List.copyOf(candidates);
        conflicts = List.copyOf(conflicts);
    }
}
