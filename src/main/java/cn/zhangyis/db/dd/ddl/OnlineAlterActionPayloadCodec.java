package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 通用Online ALTER action payload v1确定性编码器。payload只保存逻辑定义与稳定identity，不保存AST、lambda、
 * root page或segment；完整前后schema仍由marker digest与target SDI共同证明。
 */
public final class OnlineAlterActionPayloadCodec {

    /** ASCII `OAAP`。 */
    private static final int MAGIC = 0x4f414150;
    /** 当前动作payload格式。 */
    private static final int VERSION = 1;

    /**
     * 按用户声明顺序冻结全部动作；ADD identity从target读取，DROP identity从source读取。
     *
     * @param actions binder产生的非空有序动作
     * @param source initial X下重读的committed source aggregate
     * @param target 已分配全部列/index identity的planned target aggregate
     * @param targetSchema target rename完成后的schema；未rename时为source schema
     * @return ordinal连续、payload稳定且对象identity完整的descriptor集合
     */
    public List<OnlineAlterActionDescriptor> encode(
            List<AlterTableAction> actions, TableDefinition source,
            TableDefinition target, SchemaDefinition targetSchema) {
        if (actions == null || actions.isEmpty() || source == null
                || target == null || targetSchema == null) {
            throw new DatabaseValidationException(
                    "online ALTER action codec requires actions/source/target/schema");
        }
        List<OnlineAlterActionDescriptor> result = new ArrayList<>(actions.size());
        for (int ordinal = 0; ordinal < actions.size(); ordinal++) {
            AlterTableAction action = actions.get(ordinal);
            OnlineAlterActionType type = typeOf(action);
            long primaryId = primaryIdentity(action, source, target, targetSchema);
            result.add(new OnlineAlterActionDescriptor(type, ordinal, primaryId, 0L,
                    encodePayload(type, action, source, target, targetSchema)));
        }
        return List.copyOf(result);
    }

    /** 写入动作类型与完整逻辑参数；任何JVM对象展示文本都不得进入持久格式。 */
    private static byte[] encodePayload(
            OnlineAlterActionType type, AlterTableAction action,
            TableDefinition source, TableDefinition target,
            SchemaDefinition targetSchema) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(VERSION);
                out.writeInt(type.stableCode());
                switch (action) {
                    case AlterTableAction.AddColumn add -> writeColumn(out,
                            requireColumn(target, add.name()));
                    case AlterTableAction.DropColumn drop -> writeColumn(out,
                            requireColumn(source, drop.name()));
                    case AlterTableAction.AddIndex add -> writeIndex(out,
                            requireIndex(target, add.index().name()));
                    case AlterTableAction.DropIndex drop -> writeIndex(out,
                            requireIndex(source, drop.name()));
                    case AlterTableAction.Rename rename -> {
                        out.writeLong(targetSchema.id().value());
                        writeUtf8(out, rename.target().schema().canonicalName());
                        writeUtf8(out, rename.target().table().canonicalName());
                    }
                    case AlterTableAction.Comment comment -> writeUtf8(out, comment.value());
                    case AlterTableAction.DefaultCharset charset -> {
                        out.writeInt(charset.charsetId());
                        out.writeInt(charset.collationId());
                    }
                    case AlterTableAction.ConvertCharset charset -> {
                        out.writeInt(charset.charsetId());
                        out.writeInt(charset.collationId());
                    }
                }
                out.flush();
            }
            byte[] payload = bytes.toByteArray();
            if (payload.length > OnlineAlterActionDescriptor.MAX_PAYLOAD_BYTES) {
                throw new DatabaseValidationException(
                        "online ALTER action payload exceeds format limit");
            }
            return payload;
        } catch (IOException impossible) {
            throw new DatabaseValidationException(
                    "encode online ALTER action payload failed", impossible);
        }
    }

    /** 编码完整列定义，包括类型symbols与canonical default。 */
    private static void writeColumn(DataOutputStream out, ColumnDefinition column)
            throws IOException {
        out.writeLong(column.columnId());
        writeUtf8(out, column.name().canonicalName());
        out.writeInt(column.ordinal());
        var type = column.type();
        out.writeInt(type.typeId().stableCode());
        out.writeBoolean(type.unsigned());
        out.writeBoolean(type.nullable());
        out.writeInt(type.length());
        out.writeInt(type.scale());
        out.writeInt(type.charsetId());
        out.writeInt(type.collationId());
        out.writeInt(type.symbols().size());
        for (String symbol : type.symbols()) {
            writeUtf8(out, symbol);
        }
        out.writeInt(switch (column.defaultDefinition().kind()) {
            case REQUIRED -> 1;
            case IMPLICIT_NULL -> 2;
            case CONSTANT -> 3;
        });
        if (column.defaultDefinition().constantLiteral().isPresent()) {
            writeUtf8(out, column.defaultDefinition().constantLiteral().orElseThrow());
        }
    }

    /** 编码完整索引定义与有序key part；不编码物理root/segment。 */
    private static void writeIndex(DataOutputStream out, IndexDefinition index)
            throws IOException {
        out.writeLong(index.id().value());
        writeUtf8(out, index.name().canonicalName());
        out.writeBoolean(index.unique());
        out.writeBoolean(index.clustered());
        out.writeInt(index.keyParts().size());
        for (var keyPart : index.keyParts()) {
            out.writeLong(keyPart.columnId());
            out.writeInt(keyPart.order()
                    == cn.zhangyis.db.dd.domain.IndexOrder.ASC ? 1 : 2);
            out.writeInt(keyPart.prefixBytes());
        }
    }

    /** 长度前缀strict UTF-8；上层领域对象已限制名称/comment/default大小。 */
    private static void writeUtf8(DataOutputStream out, String value) throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(encoded.length);
        out.write(encoded);
    }

    private static long primaryIdentity(
            AlterTableAction action, TableDefinition source,
            TableDefinition target, SchemaDefinition targetSchema) {
        return switch (action) {
            case AlterTableAction.AddColumn add -> requireColumn(target, add.name()).columnId();
            case AlterTableAction.DropColumn drop -> requireColumn(source, drop.name()).columnId();
            case AlterTableAction.AddIndex add -> requireIndex(target, add.index().name()).id().value();
            case AlterTableAction.DropIndex drop -> requireIndex(source, drop.name()).id().value();
            case AlterTableAction.Rename ignored -> targetSchema.id().value();
            case AlterTableAction.Comment ignored -> 0L;
            case AlterTableAction.DefaultCharset ignored -> 0L;
            case AlterTableAction.ConvertCharset ignored -> 0L;
        };
    }

    private static OnlineAlterActionType typeOf(AlterTableAction action) {
        if (action == null) {
            throw new DatabaseValidationException(
                    "online ALTER action must not be null");
        }
        return switch (action) {
            case AlterTableAction.AddColumn ignored -> OnlineAlterActionType.ADD_COLUMN;
            case AlterTableAction.DropColumn ignored -> OnlineAlterActionType.DROP_COLUMN;
            case AlterTableAction.AddIndex ignored -> OnlineAlterActionType.ADD_INDEX;
            case AlterTableAction.DropIndex ignored -> OnlineAlterActionType.DROP_INDEX;
            case AlterTableAction.Rename ignored -> OnlineAlterActionType.RENAME;
            case AlterTableAction.Comment ignored -> OnlineAlterActionType.COMMENT;
            case AlterTableAction.DefaultCharset ignored -> OnlineAlterActionType.DEFAULT_CHARSET;
            case AlterTableAction.ConvertCharset ignored -> OnlineAlterActionType.CONVERT_CHARSET;
        };
    }

    private static ColumnDefinition requireColumn(
            TableDefinition table, ObjectName name) {
        return table.columns().stream().filter(column -> column.name().equals(name))
                .findFirst().orElseThrow(() -> new DatabaseValidationException(
                        "online ALTER action column is absent: " + name.displayName()));
    }

    private static IndexDefinition requireIndex(
            TableDefinition table, ObjectName name) {
        return table.indexes().stream().filter(index -> index.name().equals(name))
                .findFirst().orElseThrow(() -> new DatabaseValidationException(
                        "online ALTER action index is absent: " + name.displayName()));
    }
}
