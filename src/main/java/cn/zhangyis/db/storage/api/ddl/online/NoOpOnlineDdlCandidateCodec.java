package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.format.LogicalRecord;

import java.util.Optional;

/**
 * 纯DROP通用INPLACE operation的显式无payload codec。gate仍登记source write transaction并在final seal排空，
 * 但DML不追加无意义candidate；该类型不能用于ADD或shadow协议。
 */
public final class NoOpOnlineDdlCandidateCodec implements OnlineDdlCandidateCodec {

    /** INSERT只校验调用方确实提供了已物化行。 */
    @Override
    public Optional<byte[]> encodeInsert(LogicalRecord after) {
        require(after, "INSERT");
        return Optional.empty();
    }

    /** UPDATE校验before/after均存在，但不读取列值。 */
    @Override
    public Optional<byte[]> encodeUpdate(LogicalRecord before, LogicalRecord after) {
        require(before, "UPDATE before");
        require(after, "UPDATE after");
        return Optional.empty();
    }

    /** DELETE只校验before已物化。 */
    @Override
    public Optional<byte[]> encodeDelete(LogicalRecord before) {
        require(before, "DELETE");
        return Optional.empty();
    }

    private static void require(LogicalRecord row, String operation) {
        if (row == null) {
            throw new DatabaseValidationException(
                    "no-op online DDL " + operation + " row must not be null");
        }
    }
}
