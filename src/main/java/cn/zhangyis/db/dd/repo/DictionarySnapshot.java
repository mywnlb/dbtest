package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;

import java.util.Map;

/** startup/recovery 从全部 committed catalog batch 重建的不可变权威字典快照。 */
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
