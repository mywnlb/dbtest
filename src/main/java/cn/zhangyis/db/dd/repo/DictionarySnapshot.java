package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;

import java.util.Map;

/** startup/recovery 从全部 committed catalog batch 重建的不可变权威字典快照。
 *
 * @param publishedVersion 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param schemas 参与 {@code 构造} 的键值映射；不得为 {@code null}，空映射表示没有条目，键和值均不得包含 Java {@code null}
 * @param tables 参与 {@code 构造} 的键值映射；不得为 {@code null}，空映射表示没有条目，键和值均不得包含 Java {@code null}
 * @param indexes 参与 {@code 构造} 的键值映射；不得为 {@code null}，空映射表示没有条目，键和值均不得包含 Java {@code null}
 */
public record DictionarySnapshot(DictionaryVersion publishedVersion,
                                 Map<SchemaId, SchemaDefinition> schemas,
                                 Map<TableId, TableDefinition> tables,
                                 Map<IndexId, IndexDefinition> indexes) {
    public DictionarySnapshot {
        schemas = Map.copyOf(schemas);
        tables = Map.copyOf(tables);
        indexes = Map.copyOf(indexes);
    }

    public static DictionarySnapshot emptyBootstrap() {
        return new DictionarySnapshot(DictionaryVersion.of(1), Map.of(), Map.of(), Map.of());
    }
}
