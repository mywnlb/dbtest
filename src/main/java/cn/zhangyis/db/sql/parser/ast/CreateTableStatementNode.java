package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;
import java.util.Optional;

/**
 * 基础 {@code CREATE TABLE} 纯语法树。该对象只保存用户声明的名称、类型、default 和索引 shape，
 * 不保存 TableId、SpaceId、DictionaryVersion 或物理路径；对象存在性与原子 DDL 裁决必须留给
 * implicit commit 后使用独立 owner 的 DD coordinator。
 *
 * <p>当前 SQL 切片要求显式声明且仅声明一个 PRIMARY KEY；这与 InnoDB 可生成隐藏聚簇键的完整行为
 * 不同，目的是与现有教学型物理建表命令“恰好一个聚簇索引”的不变量保持一致。</p>
 *
 * @param table Parser 保留的目标限定名；一至三段，单段名由 Binder 使用 current schema 补全
 * @param columns 按 SQL 声明顺序保存的非空列集合；名称唯一性由 Binder/DD command 校验
 * @param indexes 按 SQL 声明顺序保存的索引集合；必须最终形成恰好一个聚簇 PRIMARY
 * @param ifNotExists 是否把“表已存在”转换为 warning；对象存在性仍必须在 DD 的 table MDL X 下重验
 * @param comment 表级注释原文；空字符串表示未声明，Parser 不执行字符集转换
 */
public record CreateTableStatementNode(
        QualifiedNameNode table, List<Column> columns, List<Index> indexes,
        boolean ifNotExists, String comment)
        implements StatementNode {

    public CreateTableStatementNode {
        if (table == null || columns == null || columns.isEmpty() || indexes == null
                || comment == null
                || columns.stream().anyMatch(java.util.Objects::isNull)
                || indexes.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "CREATE TABLE AST requires table, columns and non-null indexes");
        }
        columns = List.copyOf(columns);
        indexes = List.copyOf(indexes);
    }

    /**
     * 保留 CREATE TABLE v1 的 AST 构造形状；旧调用方等价于未声明 IF NOT EXISTS 与表注释。
     *
     * @param table 目标限定名
     * @param columns 声明顺序列集合
     * @param indexes 声明顺序索引集合
     */
    public CreateTableStatementNode(
            QualifiedNameNode table, List<Column> columns, List<Index> indexes) {
        this(table, columns, indexes, false, "");
    }

    /**
     * 单列语法定义。default 仍是未绑定 literal，必须由 Binder 按最终列类型和 Session 时区校验。
     *
     * @param name 用户声明的列名与源位置
     * @param type 不包含 DD identity 的 parser 类型 shape
     * @param defaultLiteral 显式 DEFAULT literal；空表示由最终 nullable/generation 语义推导
     * @param comment 列注释原文；空字符串表示未声明
     * @param generation Parser 已识别的值生成方式；Binder 负责验证类型和主键位置
     */
    public record Column(
            IdentifierNode name, ColumnType type, Optional<LiteralNode> defaultLiteral,
            String comment, Generation generation) {
        public Column {
            if (name == null || type == null || defaultLiteral == null
                    || comment == null || generation == null) {
                throw new DatabaseValidationException(
                        "CREATE TABLE column AST fields must not be null");
            }
        }

        /**
         * 保留 v1 无注释、无生成列语义的构造入口。
         *
         * @param name 列名
         * @param type Parser 类型 shape
         * @param defaultLiteral 可选默认字面量
         */
        public Column(
                IdentifierNode name, ColumnType type,
                Optional<LiteralNode> defaultLiteral) {
            this(name, type, defaultLiteral, "", Generation.NONE);
        }
    }

    /**
     * 内联或表级索引语法。PRIMARY 固定 {@code unique=true, clustered=true, name=PRIMARY}；
     * 普通 UNIQUE/INDEX 固定 {@code clustered=false}。
     *
     * @param name 索引逻辑名；PRIMARY 使用稳定名称 PRIMARY
     * @param unique 是否执行逻辑唯一约束
     * @param clustered 是否为表的聚簇主索引
     * @param keyParts 按声明顺序保存的完整列 key part；当前不支持 prefix/expression key
     * @param algorithm 显式或隐式索引算法；当前只允许 BTREE
     */
    public record Index(
            IdentifierNode name, boolean unique, boolean clustered,
            List<IndexKeyPartNode> keyParts, IndexAlgorithm algorithm) {
        public Index {
            if (name == null || keyParts == null || keyParts.isEmpty()
                    || algorithm == null
                    || keyParts.stream().anyMatch(java.util.Objects::isNull)) {
                throw new DatabaseValidationException(
                        "CREATE TABLE index AST fields must not be empty");
            }
            if (clustered && !unique) {
                throw new DatabaseValidationException(
                        "CREATE TABLE clustered index must be unique");
            }
            keyParts = List.copyOf(keyParts);
        }

        /**
         * 保留 v1 索引构造形状；未声明算法时按当前唯一实现 BTREE 处理。
         *
         * @param name 索引名
         * @param unique 是否唯一
         * @param clustered 是否聚簇
         * @param keyParts 有序 key parts
         */
        public Index(
                IdentifierNode name, boolean unique, boolean clustered,
                List<IndexKeyPartNode> keyParts) {
            this(name, unique, clustered, keyParts, IndexAlgorithm.BTREE);
        }
    }

    /** CREATE 列的值生成方式；表达语法事实，不携带物理 high-water。 */
    public enum Generation {
        /** 普通列，缺失值由 DEFAULT/NULL 规则解析。 */
        NONE,
        /** 由存储层持久 high-water 分配值的整数列。 */
        AUTO_INCREMENT
    }

    /** 受支持的索引算法；保留字段防止 Parser 静默丢弃显式 USING。 */
    public enum IndexAlgorithm {
        /** 教学型 B+Tree 索引。 */
        BTREE
    }

    /**
     * CREATE 列的 parser 级类型。charset/collation 不在此处猜测，字符列由 DD coordinator 在
     * schema MDL 下解析 table/schema 默认值。
     *
     * @param name 类型关键字
     * @param length 可选长度或 DECIMAL 精度；零表示使用 Binder 的确定默认
     * @param scale DECIMAL 标度；未声明为零
     * @param unsigned 是否按无符号数值域解释
     * @param nullable 是否允许 SQL NULL；主键列会在 Binder 中强制为 false
     */
    public record ColumnType(
            String name, int length, int scale, boolean unsigned, boolean nullable) {
        public ColumnType {
            if (name == null || name.isBlank() || length < 0 || scale < 0) {
                throw new DatabaseValidationException(
                        "CREATE TABLE column type shape is invalid");
            }
        }
    }
}
