package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** DD coordinator 与 recovery 共用的 schema digest 门面；不读取 cache、repository、SDI 或物理页面。 */
public final class DdlSchemaDigestService {

    /** SCHEMA_V1 的固定 magic，避免 schema image 与 table image 发生跨类型摘要混淆。 */
    private static final long SCHEMA_MAGIC = 0x4D494E4944445332L;
    /** 无状态 canonical encoder；实例不持共享缓冲区，可以被多个 DDL 并发调用。 */
    private final TableSchemaDigestCodec codec = new TableSchemaDigestCodec();

    /**
     * 对一个显式 schema image 计算 TABLE_SCHEMA_V1/SHA-256。
     *
     * @param schema table 所属的 exact schema 定义
     * @param table source、intermediate 或 target 的完整不可变聚合
     * @param rowFormatVersion 聚簇 record 的正稳定格式版本；不能用 table dictionary version代替
     * @return 防御性持有 32-byte 输出的稳定 digest
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException identity、集合上限或UTF-8不合法时抛出
     */
    public DdlSchemaDigest digest(SchemaDefinition schema, TableDefinition table,
                                  long rowFormatVersion) {
        return codec.digest(new DdlTableSchemaImage(schema, table, rowFormatVersion));
    }

    /**
     * 计算 schema 生命周期对象的 SCHEMA_V1/SHA-256 摘要。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证 schema 存在，并按严格 UTF-8 取得 canonical 名称字节。</li>
     *     <li>按固定 big-endian 顺序写入 magic、identity、名称、默认字符语义、版本和生命周期 stable code。</li>
     *     <li>对完整 image 计算 SHA-256，并返回带显式 canonical format 的防御性值对象。</li>
     * </ol>
     *
     * @param schema 待进入批量 DDL manifest 的 exact ACTIVE 或 DROPPED schema 版本
     * @return 可由恢复端重算并精确比较的 SCHEMA_V1 摘要
     * @throws DatabaseValidationException schema 为空或名称超过无符号短长度时抛出
     * @throws DatabaseFatalException JVM 缺少 SHA-256 或内存编码发生不可解释错误时抛出
     */
    public DdlSchemaDigest digest(SchemaDefinition schema) {
        // 1. 名称是 canonical image 的一部分，必须在构造持久 marker 前完成长度校验。
        if (schema == null) {
            throw new DatabaseValidationException(
                    "DDL schema digest input must not be null");
        }
        byte[] name = schema.name().canonicalName()
                .getBytes(StandardCharsets.UTF_8);
        if (name.length > 65_535) {
            throw new DatabaseValidationException(
                    "DDL schema digest name exceeds unsigned-short bound");
        }
        try {
            // 2. 生命周期使用显式稳定码，不能把 enum ordinal 写入持久协议。
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeLong(SCHEMA_MAGIC);
                out.writeInt(DdlSchemaCanonicalFormat.SCHEMA_V1.stableCode());
                out.writeLong(schema.id().value());
                out.writeShort(name.length);
                out.write(name);
                out.writeInt(schema.defaultCharsetId());
                out.writeInt(schema.defaultCollationId());
                out.writeLong(schema.version().value());
                out.writeByte(switch (schema.state()) {
                    case ACTIVE -> 1;
                    case DROPPED -> 2;
                });
            }
            // 3. 算法与 canonical format 同摘要字节一起进入 marker，恢复不能按长度猜测。
            MessageDigest algorithm = MessageDigest.getInstance("SHA-256");
            return new DdlSchemaDigest(
                    DdlDigestAlgorithm.SHA_256,
                    DdlSchemaCanonicalFormat.SCHEMA_V1,
                    algorithm.digest(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new DatabaseFatalException(
                    "failed to encode in-memory DDL schema digest", impossible);
        } catch (NoSuchAlgorithmException unavailable) {
            throw new DatabaseFatalException(
                    "SHA-256 is unavailable for DDL schema digest", unavailable);
        }
    }
}
