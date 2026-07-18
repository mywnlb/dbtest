package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

/**
 * 物理记录字节 → 逻辑记录（innodb-record-design §6）。schemaVersion 不存于记录字节（简化），解码取所给 schema 版本；
 * 列布局完全由 schema + NULL 位图 + 变长目录推导。
 *
 * <p>布局推导委托 {@link RecordFieldResolver}（与 {@code RecordCursor} 共用单一布局真相），本类只是「整条物化」的薄入口。
 */
public final class RecordDecoder {

    /**
     * 本对象持有的 {@code resolver} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final RecordFieldResolver resolver;

    /**
     * 创建 {@code RecordDecoder}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param registry 由组合根提供的 {@code TypeCodecRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecordDecoder(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("type codec registry must not be null");
        }
        this.resolver = new RecordFieldResolver(registry);
    }

    /** 解码整条记录字节为逻辑记录。
     *
     * @param buf 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code decode} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public LogicalRecord decode(byte[] buf, TableSchema schema) {
        if (buf == null || schema == null) {
            throw new DatabaseValidationException("buf/schema must not be null");
        }
        return resolver.resolve(buf, schema).materialize();
    }
}
