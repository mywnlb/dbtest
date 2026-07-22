package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnDefaultDefinition;
import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexKeyPart;
import cn.zhangyis.db.dd.domain.IndexOrder;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.DigestOutputStream;
import java.util.HashSet;
import java.util.Set;

/** TABLE_SCHEMA_V1 的流式 canonical encoder；只定义 schema 语义，不复用 catalog/SDI 字节格式。 */
final class TableSchemaDigestCodec {

    /** TABLE_SCHEMA_V1 固定 8-byte ASCII magic。 */
    private static final byte[] MAGIC = "MINIDDS1".getBytes(StandardCharsets.US_ASCII);
    /** 流结束标记 "DDSE"，用于阻止未来字段被误解释为 v1。 */
    private static final int TERMINATOR = 0x44445345;
    /** 任一 canonical UTF-8 字段的独立上限。 */
    private static final int MAX_STRING_BYTES = 8 * 1024;
    /** 与 SDI 领域上限一致的最大列数。 */
    private static final int MAX_COLUMNS = 4_096;
    /** 与 SDI 领域上限一致的最大索引数。 */
    private static final int MAX_INDEXES = 1_024;
    /** ENUM/SET symbols 的最大元素数。 */
    private static final int MAX_SYMBOLS = 65_535;
    /** 单个索引 key parts 的明确上限。 */
    private static final int MAX_KEY_PARTS = 4_096;

    /**
     * 把 schema image 流式送入 SHA-256，并在任何字节写入前完成聚合级 identity/上限校验。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>创建 Java 平台强制提供的 SHA-256，并校验 image 的集合上限与重复 identity。</li>
     *     <li>按固定 big-endian grammar 写入 aggregate header、options 和 columns。</li>
     *     <li>按 table aggregate ordinal 写入 indexes/key parts，保留顺序语义并拒绝重复引用。</li>
     *     <li>写入终止标记并封装防御性 digest；任一失败都不会修改 DD、marker 或物理资源。</li>
     * </ol>
     *
     * @param image 已交叉校验 schema/table identity 的显式输入
     * @return SHA-256/TABLE_SCHEMA_V1 digest
     * @throws DatabaseValidationException 集合、identity、长度或严格UTF-8规则不满足时抛出
     * @throws DatabaseFatalException JVM缺少强制SHA-256或内存流发生不可解释IO错误时抛出
     */
    DdlSchemaDigest digest(DdlTableSchemaImage image) {
        if (image == null) {
            throw new DatabaseValidationException("DDL schema image must not be null");
        }
        // 1、先证明整个聚合满足 canonical 上限与唯一性，避免输出半个逻辑 image 后才发现结构非法。
        validateAggregate(image);
        DdlDigestAlgorithm algorithm = DdlDigestAlgorithm.SHA_256;
        MessageDigest digest = messageDigest(algorithm);

        try (DataOutputStream out = new DataOutputStream(
                new DigestOutputStream(OutputStream.nullOutputStream(), digest))) {
            // 2、header/option/column 顺序是 TABLE_SCHEMA_V1 的持久契约，不能随 Java record 声明重排。
            writeHeader(out, image);
            writeColumns(out, image);

            // 3、index list 顺序本身参与摘要；key part stable code由本codec显式映射。
            writeIndexes(out, image);

            // 4、固定终止标记封闭v1输入，随后才发布不可变摘要结果。
            out.writeInt(TERMINATOR);
            out.flush();
        } catch (IOException error) {
            throw new DatabaseFatalException("failed to stream DDL schema digest", error);
        }
        return new DdlSchemaDigest(algorithm, DdlSchemaCanonicalFormat.TABLE_SCHEMA_V1,
                digest.digest());
    }

    /** 聚合级前置校验；成功后写阶段不会遇到重复identity或越界count。 */
    private static void validateAggregate(DdlTableSchemaImage image) {
        if (image.table().columns().isEmpty() || image.table().columns().size() > MAX_COLUMNS
                || image.table().indexes().isEmpty() || image.table().indexes().size() > MAX_INDEXES) {
            throw new DatabaseValidationException("DDL schema image column/index count exceeds bounds");
        }
        Set<Long> columnIds = new HashSet<>();
        Set<String> columnNames = new HashSet<>();
        for (int ordinal = 0; ordinal < image.table().columns().size(); ordinal++) {
            ColumnDefinition column = image.table().columns().get(ordinal);
            if (column.ordinal() != ordinal || !columnIds.add(column.columnId())
                    || !columnNames.add(column.name().canonicalName())
                    || column.type().symbols().size() > MAX_SYMBOLS) {
                throw new DatabaseValidationException(
                        "DDL schema image columns must have bounded continuous unique identity");
            }
        }
        Set<Long> indexIds = new HashSet<>();
        Set<String> indexNames = new HashSet<>();
        for (IndexDefinition index : image.table().indexes()) {
            if (!indexIds.add(index.id().value()) || !indexNames.add(index.name().canonicalName())
                    || index.keyParts().isEmpty() || index.keyParts().size() > MAX_KEY_PARTS) {
                throw new DatabaseValidationException(
                        "DDL schema image indexes must have bounded unique identity");
            }
            Set<Long> keyColumns = new HashSet<>();
            for (IndexKeyPart part : index.keyParts()) {
                if (!columnIds.contains(part.columnId()) || !keyColumns.add(part.columnId())) {
                    throw new DatabaseValidationException(
                            "DDL schema image index key parts must reference unique table columns");
                }
            }
        }
    }

    /** 写入magic、identity、row format、LOB capability与表选项。 */
    private static void writeHeader(DataOutputStream out, DdlTableSchemaImage image) throws IOException {
        out.write(MAGIC);
        out.writeInt(DdlSchemaCanonicalFormat.TABLE_SCHEMA_V1.stableCode());
        out.writeLong(image.table().id().value());
        out.writeLong(image.schema().id().value());
        out.writeLong(image.table().version().value());
        writeString(out, image.schema().name().canonicalName(), "schema name");
        writeString(out, image.table().name().canonicalName(), "table name");
        out.writeLong(image.rowFormatVersion());
        out.writeByte(image.requiresLobCapability() ? 1 : 0);
        writeString(out, image.table().options().comment(), "table comment");
        out.writeInt(image.table().options().defaultCharsetId());
        out.writeInt(image.table().options().defaultCollationId());
    }

    /** 写入按ordinal冻结的列、类型、symbols和default语义。 */
    private static void writeColumns(DataOutputStream out, DdlTableSchemaImage image) throws IOException {
        out.writeInt(image.table().columns().size());
        for (ColumnDefinition column : image.table().columns()) {
            out.writeInt(column.ordinal());
            out.writeLong(column.columnId());
            writeString(out, column.name().canonicalName(), "column name");
            out.writeInt(column.type().typeId().stableCode());
            out.writeByte(column.type().unsigned() ? 1 : 0);
            out.writeByte(column.type().nullable() ? 1 : 0);
            out.writeInt(column.type().length());
            out.writeInt(column.type().scale());
            out.writeInt(column.type().charsetId());
            out.writeInt(column.type().collationId());
            out.writeInt(column.type().symbols().size());
            for (String symbol : column.type().symbols()) {
                writeString(out, symbol, "column symbol");
            }
            out.writeInt(defaultStableCode(column.defaultDefinition().kind()));
            if (column.defaultDefinition().kind() == ColumnDefaultDefinition.Kind.CONSTANT) {
                writeString(out, column.defaultDefinition().constantLiteral().orElseThrow(() ->
                        new DatabaseValidationException("constant default literal is absent")),
                        "column default literal");
            }
        }
    }

    /** 写入按aggregate ordinal冻结的index与key parts。 */
    private static void writeIndexes(DataOutputStream out, DdlTableSchemaImage image) throws IOException {
        out.writeInt(image.table().indexes().size());
        for (int ordinal = 0; ordinal < image.table().indexes().size(); ordinal++) {
            IndexDefinition index = image.table().indexes().get(ordinal);
            out.writeInt(ordinal);
            out.writeLong(index.id().value());
            writeString(out, index.name().canonicalName(), "index name");
            out.writeByte(index.unique() ? 1 : 0);
            out.writeByte(index.clustered() ? 1 : 0);
            out.writeInt(index.keyParts().size());
            for (IndexKeyPart part : index.keyParts()) {
                out.writeLong(part.columnId());
                out.writeInt(orderStableCode(part.order()));
                out.writeInt(part.prefixBytes());
            }
        }
    }

    /** 严格编码一个长度前缀字符串；非法surrogate不会被replacement字符掩盖。 */
    private static void writeString(DataOutputStream out, String value, String field) throws IOException {
        if (value == null) {
            throw new DatabaseValidationException("DDL schema " + field + " must not be null");
        }
        byte[] bytes;
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
        } catch (CharacterCodingException error) {
            throw new DatabaseValidationException(
                    "DDL schema " + field + " is not strict UTF-8 encodable", error);
        }
        if (bytes.length > MAX_STRING_BYTES) {
            throw new DatabaseValidationException(
                    "DDL schema " + field + " exceeds canonical UTF-8 bound: " + bytes.length);
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    /** default kind不使用enum ordinal，避免Java重排改变持久摘要。 */
    private static int defaultStableCode(ColumnDefaultDefinition.Kind kind) {
        return switch (kind) {
            case REQUIRED -> 1;
            case IMPLICIT_NULL -> 2;
            case CONSTANT -> 3;
        };
    }

    /** index order不使用enum ordinal，避免Java重排改变持久摘要。 */
    private static int orderStableCode(IndexOrder order) {
        return switch (order) {
            case ASC -> 1;
            case DESC -> 2;
        };
    }

    /** 创建平台强制算法；缺失表示JVM已无法满足恢复格式要求。 */
    private static MessageDigest messageDigest(DdlDigestAlgorithm algorithm) {
        try {
            return MessageDigest.getInstance(algorithm.jcaName());
        } catch (NoSuchAlgorithmException error) {
            throw new DatabaseFatalException(
                    "required DDL digest algorithm is unavailable: " + algorithm.jcaName(), error);
        }
    }
}
