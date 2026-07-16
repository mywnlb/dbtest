package cn.zhangyis.db.session;

import cn.zhangyis.db.dd.ddl.*;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.engine.DatabaseEngine;
import cn.zhangyis.db.sql.executor.storage.SqlDurabilityMode;
import cn.zhangyis.db.sql.executor.storage.SqlIsolationLevel;
import cn.zhangyis.db.storage.engine.EngineConfig;

import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

/** 公开 Session 端到端测试的 DDL/config 工具；所有数据操作仍只经 SqlSession。 */
final class SqlSessionTestSupport {
    private SqlSessionTestSupport() { }

    static EngineConfig config(Path directory) {
        return new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 512,
                SpaceId.of(5), PageNo.of(128), 128, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }

    static SessionOptions options(boolean autocommit, SqlIsolationLevel isolation, Duration rowTimeout) {
        return new SessionOptions(Optional.of("app"), autocommit, isolation,
                SqlDurabilityMode.FLUSH_ON_COMMIT, ZoneId.of("Asia/Shanghai"), Duration.ofSeconds(5),
                Duration.ofSeconds(3), rowTimeout, Duration.ofSeconds(3));
    }

    static void createSchema(DatabaseEngine database) {
        database.ddl().createSchema(MdlOwnerId.of(10_000), ObjectName.of("app"), 1, 1,
                Duration.ofSeconds(3));
    }

    static void createSimpleTable(DatabaseEngine database, String name, long owner) {
        database.ddl().createTable(MdlOwnerId.of(owner), new CreateTableCommand(
                QualifiedTableName.of("app", name), PageNo.of(128),
                List.of(new CreateColumnSpec(ObjectName.of("id"), ColumnTypeDefinition.bigint(false, false))),
                List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                        List.of(new CreateIndexKeyPartSpec(ObjectName.of("id"), IndexOrder.ASC, 0))))),
                Duration.ofSeconds(3));
    }
}
