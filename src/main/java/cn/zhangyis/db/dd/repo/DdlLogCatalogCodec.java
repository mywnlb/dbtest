package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.dd.ddl.DdlLogOperation;
import cn.zhangyis.db.dd.ddl.DdlLogPhase;
import cn.zhangyis.db.dd.ddl.DdlLogRecord;
import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.ddl.DdlUndoMarker;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.domain.PageSize;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * DDL phase marker 与 InternalCatalogStore 无语义 record 的版本化 codec。key/payload 重复 identity，
 * 解码逐字段交叉校验，避免单侧损坏被恢复状态机接受。
 */
final class DdlLogCatalogCodec {

    /** kind/operation/ddl/version/phase/chunk 的固定 big-endian key 长度。 */
    private static final int KEY_V1_BYTES = 1 + Long.BYTES * 3 + Integer.BYTES * 2;
    /**
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
    private static final int KEY_V2_BYTES = KEY_V1_BYTES + Long.BYTES;
    /** payload v1 魔数 "DDL1"。 */
    private static final int MAGIC = 0x44444C31; // DDL1
    /** 当前写格式；v2 在 key/payload 同时加入 secondaryObjectId。 */
    private static final int FORMAT_VERSION = 3;
    /** CREATE/DROP TABLE 历史 marker 的兼容读格式。 */
    private static final int LEGACY_FORMAT_VERSION = 1;
    /** 为 catalog record payload 头与未来扩展保留空间后的 UTF-8 路径上限。 */
    private static final int MAX_PATH_BYTES = 900;

    /**
     * 编码单条 DDL marker；InternalCatalogStore 的一个 append 批次即一个 phase 原子提交。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param record 已验证且路径规范化的 marker。
     * @return key/payload 均独立复制的 catalog record。
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException UTF-8 路径超过 v1 上限时抛出。
     */
    CatalogRecord encode(DdlLogRecord record) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        byte[] path = record.path().toString().getBytes(StandardCharsets.UTF_8);
        if (path.length > MAX_PATH_BYTES) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "DDL log path exceeds v1 limit: " + path.length);
        }
        DdlUndoMarker marker = record.marker();
        ByteBuffer key = ByteBuffer.allocate(KEY_V2_BYTES).order(ByteOrder.BIG_ENDIAN)
                .put((byte) CatalogEntityKind.DDL_LOG.stableCode())
                .putLong(record.operation().stableCode())
                .putLong(marker.ddlOperationId())
                .putLong(marker.dictionaryVersion())
                .putLong(record.secondaryObjectId())
                .putInt(record.phase().stableCode())
                .putInt(0);
        byte[] auxiliary = record.auxiliaryPath().map(value -> value.toString().getBytes(StandardCharsets.UTF_8)).orElse(new byte[0]);
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        if (auxiliary.length > MAX_PATH_BYTES) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException("DDL log auxiliary path exceeds v3 limit");
        }
        int identityBytes = record.fileIdentity().isPresent() ? 1 + Integer.BYTES + Integer.BYTES + Integer.BYTES + Long.BYTES : 1;
        ByteBuffer payload = ByteBuffer.allocate(Integer.BYTES + 1 + Long.BYTES * 4 + 1 + 1
                        + Integer.BYTES + Short.BYTES + path.length + 1 + Short.BYTES + auxiliary.length + identityBytes)
                .order(ByteOrder.BIG_ENDIAN);
        payload.putInt(MAGIC).put((byte) FORMAT_VERSION)
                .putLong(marker.ddlOperationId()).putLong(marker.dictionaryVersion())
                .putLong(marker.affectedObjectId())
                .putLong(record.secondaryObjectId())
                .put((byte) record.operation().stableCode()).put((byte) record.phase().stableCode())
                .putInt(record.spaceId().value()).putShort((short) path.length).put(path)
                .put((byte) (record.auxiliaryPath().isPresent() ? 1 : 0));
        if (record.auxiliaryPath().isPresent()) {
            payload.putShort((short) auxiliary.length).put(auxiliary);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        payload.put((byte) (record.fileIdentity().isPresent() ? 1 : 0));
        if (record.fileIdentity().isPresent()) {
            TablespaceFileIdentity identity = record.fileIdentity().orElseThrow();
            payload.putInt(identity.pageSize().bytes()).putInt(identity.type().code())
                    .putInt(identity.serverVersion()).putLong(identity.spaceVersion());
        }
        byte[] encoded = new byte[payload.position()];
        payload.flip();
        payload.get(encoded);
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return new CatalogRecord(key.array(), encoded);
    }

    /**
     * 尝试解码一个 committed batch。普通 DD/future batch 返回 empty；DDL_LOG 形状损坏则 fail-closed。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param batch 已经 storage frame CRC/SHA/header 边界验证的原子批次。
     * @return DDL marker，或当前批次不属于 DDL log 时为空。
     * @throws DictionaryCatalogCorruptionException DDL key/payload/version/identity/path 不可解释时抛出。
     */
    Optional<DdlLogRecord> decode(CatalogBatch batch) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        List<CatalogRecord> records = batch.records();
        byte[] firstKey = records.getFirst().key();
        if (Byte.toUnsignedInt(firstKey[0]) != CatalogEntityKind.DDL_LOG.stableCode()) {
            return Optional.empty();
        }
        if (firstKey.length != KEY_V1_BYTES && firstKey.length != KEY_V2_BYTES) {
            throw new DictionaryCatalogCorruptionException("DDL log key length is invalid: " + firstKey.length);
        }
        if (records.size() != 1) {
            throw new DictionaryCatalogCorruptionException("DDL log batch must contain exactly one record");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        ByteBuffer key = ByteBuffer.wrap(firstKey).order(ByteOrder.BIG_ENDIAN);
        key.get();
        long keyOperation = key.getLong();
        long keyDdlId = key.getLong();
        long keyVersion = key.getLong();
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        long keySecondaryObjectId = firstKey.length == KEY_V2_BYTES ? key.getLong() : 0L;
        int keyPhase = key.getInt();
        int chunk = key.getInt();
        if (chunk != 0) {
            throw new DictionaryCatalogCorruptionException("DDL log v1 does not support payload chunks");
        }

        byte[] bytes = records.getFirst().payload();
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        try {
            ByteBuffer payload = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
            if (payload.remaining() < Integer.BYTES + 1 + Long.BYTES * 3 + 1 + 1
                    + Integer.BYTES + Short.BYTES || payload.getInt() != MAGIC) {
                throw new DictionaryCatalogCorruptionException("invalid DDL log payload header/version");
            }
            int format = Byte.toUnsignedInt(payload.get());
            if (format != LEGACY_FORMAT_VERSION && format != 2 && format != FORMAT_VERSION
                    || format == LEGACY_FORMAT_VERSION && firstKey.length != KEY_V1_BYTES
                    || format >= 2 && firstKey.length != KEY_V2_BYTES) {
                throw new DictionaryCatalogCorruptionException("invalid DDL log payload header/version");
            }
            long ddlId = payload.getLong();
            long version = payload.getLong();
            long objectId = payload.getLong();
            long secondaryObjectId = format == FORMAT_VERSION ? payload.getLong() : 0L;
            int operationCode = Byte.toUnsignedInt(payload.get());
            int phaseCode = Byte.toUnsignedInt(payload.get());
            int spaceId = payload.getInt();
            int pathLength = Short.toUnsignedInt(payload.getShort());
            if (pathLength > MAX_PATH_BYTES || format < 3 && pathLength != payload.remaining()
                    || format >= 3 && pathLength > payload.remaining()) {
                throw new DictionaryCatalogCorruptionException("DDL log path length/trailing bytes mismatch");
            }
            byte[] pathBytes = new byte[pathLength];
            payload.get(pathBytes);
            if (keyOperation != operationCode || keyDdlId != ddlId || keyVersion != version
                    || keySecondaryObjectId != secondaryObjectId || keyPhase != phaseCode) {
                throw new DictionaryCatalogCorruptionException("DDL log key/payload identity mismatch");
            }
            String path = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(pathBytes)).toString();
            Optional<java.nio.file.Path> auxiliaryPath = Optional.empty();
            Optional<TablespaceFileIdentity> identity = Optional.empty();
            if (format >= 3) {
                if (payload.remaining() < 1) {
                    throw new DictionaryCatalogCorruptionException("missing DDL auxiliary path flag");
                }
                if (payload.get() != 0) {
                    int auxiliaryLength = Short.toUnsignedInt(payload.getShort());
                    if (auxiliaryLength > MAX_PATH_BYTES || auxiliaryLength > payload.remaining()) {
                        throw new DictionaryCatalogCorruptionException("invalid DDL auxiliary path length");
                    }
                    byte[] auxiliaryBytes = new byte[auxiliaryLength];
                    payload.get(auxiliaryBytes);
                    auxiliaryPath = Optional.of(java.nio.file.Path.of(StandardCharsets.UTF_8.newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
                            .decode(ByteBuffer.wrap(auxiliaryBytes)).toString()));
                }
                if (payload.remaining() < 1) {
                    throw new DictionaryCatalogCorruptionException("missing DDL identity flag");
                }
                if (payload.get() != 0) {
                    if (payload.remaining() < Integer.BYTES * 3 + Long.BYTES) {
                        throw new DictionaryCatalogCorruptionException("truncated DDL tablespace identity");
                    }
                    identity = Optional.of(new TablespaceFileIdentity(SpaceId.of(spaceId),
                            PageSize.ofBytes(payload.getInt()), TablespaceType.fromCode(payload.getInt()),
                            payload.getInt(), payload.getLong()));
                }
            }
            if (payload.hasRemaining()) {
                throw new DictionaryCatalogCorruptionException("DDL log payload has trailing bytes");
            }
            return Optional.of(new DdlLogRecord(new DdlUndoMarker(ddlId, version, objectId), secondaryObjectId,
                    DdlLogOperation.fromStableCode(operationCode), DdlLogPhase.fromStableCode(phaseCode),
                    SpaceId.of(spaceId), java.nio.file.Path.of(path), auxiliaryPath, identity));
        } catch (CharacterCodingException | java.nio.BufferUnderflowException | java.nio.InvalidMarkException error) {
            throw new DictionaryCatalogCorruptionException("truncated/invalid DDL log payload", error);
        } catch (java.nio.file.InvalidPathException error) {
            throw new DictionaryCatalogCorruptionException("invalid DDL log path", error);
        } catch (cn.zhangyis.db.common.exception.DatabaseValidationException error) {
            throw new DictionaryCatalogCorruptionException("invalid DDL log domain identity", error);
        }
    }
}
