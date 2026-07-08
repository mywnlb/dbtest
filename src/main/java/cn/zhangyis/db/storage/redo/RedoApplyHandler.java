package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.PageId;

import java.util.List;

/**
 * redo recovery 的 record handler 扩展点。dispatcher 用它把不同持久 redo record 分发到各自模块，
 * 同时保持 batch 内 record 的原始顺序。
 */
public interface RedoApplyHandler {

    /**
     * 当前 handler 是否负责该 record。一个 record 必须恰好匹配一个 handler；零个或多个匹配都是配置错误。
     *
     * @param record redo record。
     * @return true 表示由本 handler 处理。
     */
    boolean supports(RedoRecord record);

    /**
     * 返回该 record 可能触碰的页。dispatcher 在打开 handler session 或触碰 PageStore 前用这些页执行
     * FORCE_SKIP_CORRUPT_TABLESPACE 过滤；逻辑事务类 redo 如不直接触碰页，可返回空列表。
     *
     * @param record 已支持的 redo record。
     * @return 受影响页列表，不得为 null，列表内不得有 null。
     */
    List<PageId> affectedPages(RedoRecord record);

    /**
     * 为一个 redo batch 打开 handler 会话。会话生命周期只覆盖当前 batch，不跨 batch 复用。
     *
     * @param range   原始 batch LSN 区间。
     * @param context recovery apply 上下文。
     * @return 当前 batch 的 handler 会话。
     */
    RedoApplyBatchHandler openBatch(LogRange range, RedoApplyContext context);
}
