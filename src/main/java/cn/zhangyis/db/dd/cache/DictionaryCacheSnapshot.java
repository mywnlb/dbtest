package cn.zhangyis.db.dd.cache;

/** 不暴露 entry/future/lock 的字典 cache 诊断快照。
 *
 * @param currentObjectCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
 * @param versionCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
 * @param pinnedVersionCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
 * @param loadingCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
 */
public record DictionaryCacheSnapshot(int currentObjectCount, int versionCount,
                                      int pinnedVersionCount, int loadingCount) {
}
