package cn.zhangyis.db.dd.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.storage.api.ddl.SerializedDictionaryInfo;
import cn.zhangyis.db.storage.api.ddl.SerializedDictionaryInfoException;
import cn.zhangyis.db.storage.api.ddl.TableDdlStorageService;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;

/**
 * DD 层 SDI adapter：把完整 `TableDefinition` 编码为 storage-neutral payload，并以 committed DD
 * 作为恢复真相执行校验/重写。本类不读取 page bytes，也不把 SDI 内容反向发布进 catalog。
 */
@Slf4j
public final class SerializedDictionaryInfoService {

    /**
     * 本对象持有的 {@code physical} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final TableDdlStorageService physical;
    /**
     * 本对象持有的 {@code codec} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DictionarySdiCodec codec;

    /**
     * 创建 {@code SerializedDictionaryInfoService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param physical 由组合根提供的 {@code TableDdlStorageService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public SerializedDictionaryInfoService(TableDdlStorageService physical) {
        if (physical == null) {
            throw new DatabaseValidationException("SDI service physical storage must not be null");
        }
        this.physical = physical;
        this.codec = new DictionarySdiCodec();
    }

    /**
     * 在 ACTIVE DD publish 前写入完整 table 快照。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>从 table 聚合提取并校验稳定 binding；缺失 binding 时禁止进入 storage。</li>
     *     <li>由 DD codec 确定性编码列、索引和最终物理 binding，构造 storage-neutral identity/payload。</li>
     *     <li>调用 storage facade 完成 page0/page3 MTR、WAL flush 与 tablespace force；成功后调用方才可提交 DD。</li>
     * </ol>
     *
     * @param table 待发布的 ACTIVE table 聚合，必须携带最终 storage binding
     * @param timeout SDI redo/dirty durable 的正等待时限
     * @throws DatabaseValidationException table、binding 或 timeout 无效时抛出
     * @throws SerializedDictionaryInfoException 页格式、容量或持久化失败时抛出，调用方不得继续发布 ACTIVE DD
     */
    public void write(TableDefinition table, Duration timeout) {
        // 1. binding 是 page/space/path 的唯一稳定入口，不允许从 tableId 猜测物理文件。
        TableStorageBinding binding = requireBinding(table, timeout);

        // 2. codec 属于 DD 层，storage 只接收 opaque bytes 和数值 identity/version。
        SerializedDictionaryInfo information = expected(table);

        // 3. physical 返回即代表 SDI redo 与数据页均 durable；异常原样阻断后续 DD publish。
        physical.writeSerializedDictionaryInfo(binding, information, timeout);
    }

    /**
     * 用 committed ACTIVE DD 校验并在必要时覆盖 SDI。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>编码 committed table 的期望快照，并通过稳定 facade 读取实际 SDI。</li>
     *     <li>identity、version、payload 完全相同则保持页不变；空页、错配或可覆盖的逻辑损坏进入修复。</li>
     *     <li>按期望快照执行 durable write；若物理 envelope/checksum/未知 root 不可修复，write 继续 fail-closed。</li>
     * </ol>
     *
     * @param committedTable catalog 中当前 committed ACTIVE table
     * @param timeout 修复写入的正等待时限
     * @return 发生实际修复时为 {@code true}，已经一致时为 {@code false}
     * @throws DatabaseValidationException table、binding 或 timeout 无效时抛出
     * @throws SerializedDictionaryInfoException 实际页无法安全读取且也无法按 committed DD 覆盖时抛出
     */
    public boolean reconcile(TableDefinition committedTable, Duration timeout) {
        // 1. expected 只来自 committed DD；实际 SDI 无论 version 高低都没有反向发布权。
        TableStorageBinding binding = requireBinding(committedTable, timeout);
        SerializedDictionaryInfo expected = expected(committedTable);
        RuntimeException readFailure = null;
        try {
            Optional<SerializedDictionaryInfo> actual = physical.readSerializedDictionaryInfo(binding);
            if (actual.isPresent() && same(expected, actual.orElseThrow())) {
                // 2. 完全一致时不制造 redo/dirty/force，保持正常启动的写放大为零。
                return false;
            }
        } catch (SerializedDictionaryInfoException logicalOrPhysicalFailure) {
            // read 失败可能是可覆盖的逻辑 CRC/header，也可能是物理损坏；统一交给严格 write 再裁决。
            readFailure = logicalOrPhysicalFailure;
        }

        // 3. write 只允许 root 0/3 且要求可读物理 envelope；不可修复损坏仍阻止 OPEN。
        try {
            physical.writeSerializedDictionaryInfo(binding, expected, timeout);
        } catch (RuntimeException writeFailure) {
            if (readFailure != null) {
                writeFailure.addSuppressed(readFailure);
            }
            throw writeFailure;
        }
        log.warn("rewrote SDI from committed dictionary: table={} version={} path={}",
                committedTable.id().value(), committedTable.version().value(), binding.path());
        return true;
    }

    private SerializedDictionaryInfo expected(TableDefinition table) {
        return new SerializedDictionaryInfo(table.id().value(), table.version().value(), codec.encode(table));
    }

    private static boolean same(SerializedDictionaryInfo expected, SerializedDictionaryInfo actual) {
        return expected.tableId() == actual.tableId()
                && expected.dictionaryVersion() == actual.dictionaryVersion()
                && Arrays.equals(expected.payload(), actual.payload());
    }

    private static TableStorageBinding requireBinding(TableDefinition table, Duration timeout) {
        if (table == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("SDI service requires table and positive timeout");
        }
        return table.storageBinding().orElseThrow(() ->
                new DatabaseValidationException("SDI table has no physical storage binding"));
    }
}
