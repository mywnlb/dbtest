package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 通用 Online ALTER manifest v1 的稳定边界、action 顺序与 shadow identity 测试。 */
class OnlineAlterManifestCodecTest {

    /** 多 action、schema digest、row format 与 shadow target 必须完整往返且保持声明顺序。 */
    @Test
    void roundTripsGeneralAlterManifest() {
        OnlineAlterManifest expected = manifest();
        OnlineAlterManifest actual = new OnlineAlterManifestCodec().decode(
                new OnlineAlterManifestCodec().encode(expected));

        assertEquals(expected.ddlOperationId(), actual.ddlOperationId());
        assertEquals(expected.tableId(), actual.tableId());
        assertEquals(expected.sourceVersion(), actual.sourceVersion());
        assertEquals(expected.targetVersion(), actual.targetVersion());
        assertEquals(expected.executionProtocol(), actual.executionProtocol());
        assertEquals(expected.sourceSchemaDigest(), actual.sourceSchemaDigest());
        assertEquals(expected.targetSchemaDigest(), actual.targetSchemaDigest());
        assertEquals(expected.sourceRowFormatVersion(), actual.sourceRowFormatVersion());
        assertEquals(expected.targetRowFormatVersion(), actual.targetRowFormatVersion());
        assertEquals(expected.freezeReadViewGeneration(), actual.freezeReadViewGeneration());
        assertEquals(expected.shadowTarget(), actual.shadowTarget());
        assertEquals(expected.actions().size(), actual.actions().size());
        for (int i = 0; i < expected.actions().size(); i++) {
            OnlineAlterActionDescriptor before = expected.actions().get(i);
            OnlineAlterActionDescriptor after = actual.actions().get(i);
            assertEquals(before.type(), after.type());
            assertEquals(before.ordinal(), after.ordinal());
            assertEquals(before.primaryObjectId(), after.primaryObjectId());
            assertEquals(before.secondaryObjectId(), after.secondaryObjectId());
            assertArrayEquals(before.payload(), after.payload());
        }
    }

    /** 截断、尾随与超过 1024 actions 都不能进入 marker 准备阶段。 */
    @Test
    void rejectsMalformedOrOversizedManifest() {
        OnlineAlterManifestCodec codec = new OnlineAlterManifestCodec();
        byte[] encoded = codec.encode(manifest());

        assertThrows(DatabaseValidationException.class,
                () -> codec.decode(Arrays.copyOf(encoded, encoded.length - 1)));
        assertThrows(DatabaseValidationException.class,
                () -> codec.decode(Arrays.copyOf(encoded, encoded.length + 1)));

        List<OnlineAlterActionDescriptor> tooMany = java.util.stream.IntStream.range(0, 1_025)
                .mapToObj(i -> new OnlineAlterActionDescriptor(
                        OnlineAlterActionType.COMMENT, i, 0, 0, new byte[]{1}))
                .toList();
        OnlineAlterManifest base = manifest();
        assertThrows(DatabaseValidationException.class, () -> new OnlineAlterManifest(
                base.ddlOperationId(), base.tableId(), base.sourceVersion(), base.targetVersion(),
                base.executionProtocol(), base.sourceSchemaDigest(), base.targetSchemaDigest(),
                base.sourceRowFormatVersion(), base.targetRowFormatVersion(),
                base.freezeReadViewGeneration(), tooMany, base.shadowTarget()));
    }

    /** action payload 必须由值对象拥有，调用方修改原数组不能改变恢复命令。 */
    @Test
    void defensivelyCopiesActionPayload() {
        byte[] payload = "index-definition".getBytes(StandardCharsets.UTF_8);
        OnlineAlterActionDescriptor action = new OnlineAlterActionDescriptor(
                OnlineAlterActionType.ADD_INDEX, 0, 31, 0, payload);

        payload[0] ^= 0x20;
        byte[] firstRead = action.payload();
        firstRead[0] ^= 0x20;

        assertArrayEquals("index-definition".getBytes(StandardCharsets.UTF_8), action.payload());
    }

    private static OnlineAlterManifest manifest() {
        return new OnlineAlterManifest(
                11, TableId.of(12), DictionaryVersion.of(20), DictionaryVersion.of(21),
                DdlExecutionProtocol.ONLINE_ALTER_SHADOW_V1,
                digest((byte) 1), digest((byte) 2), 3, 4, 19,
                List.of(
                        new OnlineAlterActionDescriptor(OnlineAlterActionType.ADD_COLUMN,
                                0, 41, 0, "column-image".getBytes(StandardCharsets.UTF_8)),
                        new OnlineAlterActionDescriptor(OnlineAlterActionType.ADD_INDEX,
                                1, 51, 0, "index-image".getBytes(StandardCharsets.UTF_8))),
                Optional.of(new OnlineAlterShadowTarget(
                        SpaceId.of(61), Path.of("C:/db/tables/online-alter-shadow-11.ibd"))));
    }

    private static DdlSchemaDigest digest(byte fill) {
        byte[] bytes = new byte[32];
        Arrays.fill(bytes, fill);
        return new DdlSchemaDigest(DdlDigestAlgorithm.SHA_256,
                DdlSchemaCanonicalFormat.TABLE_SCHEMA_V1, bytes);
    }
}
