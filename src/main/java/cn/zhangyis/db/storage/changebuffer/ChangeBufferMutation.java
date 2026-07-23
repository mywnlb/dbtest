package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.Arrays;

/**
 * 一条可跨重启重放的二级物理 mutation。
 *
 * @param targetPageId 未载入的真实二级 leaf 页
 * @param sequence 全局单调正序号，与 target identity 组成物理唯一 key
 * @param tableId DD 稳定正表 id
 * @param schemaVersion exact-version 正版本
 * @param indexId 目标二级索引正 id
 * @param operation 待局部应用的物理动作
 * @param entryBytes 使用目标二级 entry schema 编码的完整紧凑记录
 */
public record ChangeBufferMutation(PageId targetPageId, long sequence, long tableId,
                                   long schemaVersion, long indexId,
                                   ChangeBufferOperation operation, byte[] entryBytes) {

    public ChangeBufferMutation {
        if (targetPageId == null || operation == null || entryBytes == null) {
            throw new DatabaseValidationException("change buffer mutation fields must not be null");
        }
        if (sequence <= 0 || tableId <= 0 || schemaVersion <= 0 || indexId <= 0 || entryBytes.length == 0) {
            throw new DatabaseValidationException("change buffer mutation identity/payload is invalid");
        }
        entryBytes = entryBytes.clone();
    }

    /** @return 防御性 payload 副本，避免写入 tree 前被调用方篡改。 */
    @Override
    public byte[] entryBytes() {
        return entryBytes.clone();
    }

    /** byte payload 按内容参与值相等，避免持久解码后的防御性副本产生伪不等。 */
    @Override
    public boolean equals(Object other) {
        return other instanceof ChangeBufferMutation mutation
                && sequence == mutation.sequence
                && tableId == mutation.tableId
                && schemaVersion == mutation.schemaVersion
                && indexId == mutation.indexId
                && targetPageId.equals(mutation.targetPageId)
                && operation == mutation.operation
                && Arrays.equals(entryBytes, mutation.entryBytes);
    }

    /** @return 与内容相等性一致的稳定 hash，供批次去重和测试断言使用。 */
    @Override
    public int hashCode() {
        int result = targetPageId.hashCode();
        result = 31 * result + Long.hashCode(sequence);
        result = 31 * result + Long.hashCode(tableId);
        result = 31 * result + Long.hashCode(schemaVersion);
        result = 31 * result + Long.hashCode(indexId);
        result = 31 * result + operation.hashCode();
        return 31 * result + Arrays.hashCode(entryBytes);
    }
}
