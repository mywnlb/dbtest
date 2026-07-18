package cn.zhangyis.db.dd.cache;

import java.util.Optional;

/** cache miss 时在 cache 锁外执行的 repository loader；允许保留 checked IO/cancellation 根因。 */
@FunctionalInterface
public interface DictionaryLoader<T> {
    /**
     * 定位并读取数据字典领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @return {@code load} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    Optional<T> load() throws Exception;
}
