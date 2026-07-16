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

    private final PersistentDictionaryRepository repository;
    private final DictionaryObjectCache cache;
    private final MetadataLockManager locks;

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
     * @param owner session/transaction 的稳定 MDL owner。
     * @param name 三段限定表名。
     * @param intent READ 取得 table SR，WRITE 取得 table SW；不能为 null。
     * @param timeout 整个 MDL 与 cache miss 流程共享的等待上限。
     * @return 同时持有名称锁与不可变版本的表访问租约。
     */
    public TableMetadataLease openTable(MdlOwnerId owner, QualifiedTableName name, TableAccessIntent intent,
                                        Duration timeout) {
        validateOpen(owner, name, intent, timeout);
        long deadline = deadline(timeout);
        List<MdlTicket> tickets = new ArrayList<>(2);
        DictionaryPin<TableDefinition> pin = null;
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
