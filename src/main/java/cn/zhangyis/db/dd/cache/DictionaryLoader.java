package cn.zhangyis.db.dd.cache;

import java.util.Optional;

/** cache miss 时在 cache 锁外执行的 repository loader；允许保留 checked IO/cancellation 根因。 */
@FunctionalInterface
public interface DictionaryLoader<T> {
    Optional<T> load() throws Exception;
}
