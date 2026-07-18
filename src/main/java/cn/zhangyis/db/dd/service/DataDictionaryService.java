package cn.zhangyis.db.dd.service;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.cache.DictionaryPin;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
import cn.zhangyis.db.dd.exception.MetadataLockTimeoutException;
import cn.zhangyis.db.dd.mdl.MdlDuration;
import cn.zhangyis.db.dd.mdl.MdlKey;
import cn.zhangyis.db.dd.mdl.MdlMode;
import cn.zhangyis.db.dd.mdl.MdlRequest;
import cn.zhangyis.db.dd.mdl.MdlTicket;
import cn.zhangyis.db.dd.mdl.MetadataLockManager;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Dictionary 的稳定读取 Facade。对上层只返回 {@link TableMetadataLease}，从 API 层阻止 executor
 * 绕过 MDL 或 cache pin 持有裸表定义；持久 repository 和版本 cache 的具体实现均不向 SQL 层泄漏。
 */
public final class DataDictionaryService {

    /**
     * 本对象持有的 {@code repository} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final PersistentDictionaryRepository repository;
    /**
     * 本对象持有的 {@code cache} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DictionaryObjectCache cache;
    /**
     * 本对象持有的 {@code locks} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final MetadataLockManager locks;

    /**
     * 创建 {@code DataDictionaryService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param repository 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param cache 由实例组合根提供的引擎、缓存、worker 工厂或预读回调；不得为 {@code null}，其生命周期必须覆盖当前调用
     * @param locks 由组合根提供的 {@code MetadataLockManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DataDictionaryService(PersistentDictionaryRepository repository, DictionaryObjectCache cache,
                                 MetadataLockManager locks) {
        if (repository == null || cache == null || locks == null) {
            throw new DatabaseValidationException("dictionary service collaborators must not be null");
        }
        this.repository = repository;
        this.cache = cache;
        this.locks = locks;
    }

    /**
     * 按 canonical schema/table 名称和显式访问意图取得安全访问租约。数据流为：schema SR → table SR/SW
     * → repository 名称解析 → cache 单航班 pin；任何后置步骤失败都会反序释放已经取得的 MDL，不留下幽灵持有者。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @param owner session/transaction 的稳定 MDL owner。
     * @param name 三段限定表名。
     * @param intent READ 取得 table SR，WRITE 取得 table SW；不能为 null。
     * @param timeout 整个 MDL 与 cache miss 流程共享的等待上限。
     * @return 同时持有名称锁与不可变版本的表访问租约。
     */
    public TableMetadataLease openTable(MdlOwnerId owner, QualifiedTableName name, TableAccessIntent intent,
                                        Duration timeout) {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        validateOpen(owner, name, intent, timeout);
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        long deadline = deadline(timeout);
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
        List<MdlTicket> tickets = new ArrayList<>(2);
        DictionaryPin<TableDefinition> pin = null;
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        try {
            tickets.add(locks.acquire(new MdlRequest(owner, MdlKey.schema(name.schema().canonicalName()),
                    MdlMode.SHARED_READ, MdlDuration.TRANSACTION), remaining(deadline)));
            tickets.add(locks.acquire(new MdlRequest(owner, MdlKey.table(name.canonicalKey()),
                    intent.tableMode(), MdlDuration.TRANSACTION), remaining(deadline)));

            SchemaDefinition schema = repository.findSchema(name.schema())
                    .orElseThrow(() -> new DictionaryObjectNotFoundException(
                            "schema does not exist: " + name.schema().displayName()));
            TableDefinition resolved = repository.findTable(schema.id(), name.table())
                    .orElseThrow(() -> new DictionaryObjectNotFoundException(
                            "table does not exist: " + name.canonicalKey()));
            pin = cache.pinTable(resolved.id(), remaining(deadline), () -> repository.findTable(resolved.id()));
            return new TableMetadataLease(pin, tickets);
        } catch (RuntimeException failure) {
            if (pin != null) {
                pin.close();
            }
            closeTickets(tickets, failure);
            throw failure;
        }
    }

    /**
     * 释放本方法拥有的数据字典资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * @param tickets 参与 {@code closeTickets} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param failure 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    private static void closeTickets(List<MdlTicket> tickets, RuntimeException failure) {
        for (int i = tickets.size() - 1; i >= 0; i--) {
            try {
                tickets.get(i).close();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    private static void validateOpen(MdlOwnerId owner, QualifiedTableName name, TableAccessIntent intent,
                                     Duration timeout) {
        if (owner == null || name == null || intent == null || timeout == null
                || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("table open owner/name/intent/positive timeout required");
        }
    }

    private static long deadline(Duration timeout) {
        long nanos;
        try {
            nanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("table open timeout is too large", overflow);
        }
        long now = System.nanoTime();
        long candidate = now + nanos;
        return candidate < 0 ? Long.MAX_VALUE : candidate;
    }

    private static Duration remaining(long deadline) {
        long nanos = deadline == Long.MAX_VALUE ? Long.MAX_VALUE : deadline - System.nanoTime();
        if (nanos <= 0) {
            throw new MetadataLockTimeoutException("table metadata lease timeout");
        }
        return Duration.ofNanos(nanos);
    }
}
