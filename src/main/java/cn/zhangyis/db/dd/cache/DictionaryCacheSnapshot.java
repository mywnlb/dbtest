package cn.zhangyis.db.dd.cache;

/** 不暴露 entry/future/lock 的字典 cache 诊断快照。 */
public record DictionaryCacheSnapshot(int currentObjectCount, int versionCount,
                                      int pinnedVersionCount, int loadingCount) {
}
