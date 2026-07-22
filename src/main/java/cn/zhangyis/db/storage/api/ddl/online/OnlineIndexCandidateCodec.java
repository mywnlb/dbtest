package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.storage.record.format.LogicalRecord;

import java.util.Optional;

/**
 * DML 与 reconciliation 共用的 candidate 稳定编码端口。实现必须按冻结 secondary layout 投影完整 physical
 * entry，不得使用 Java serialization，也不得在编码时访问页、事务状态或 DD repository。
 */
public interface OnlineIndexCandidateCodec extends OnlineDdlCandidateCodec {

    /**
     * @param after clustered INSERT 即将发布的完整用户行，不含未分配的 Java null 字段
     * @return INSERT after physical entry 的稳定字节；目标 layout 不适用时为空
     */
    Optional<byte[]> encodeInsert(LogicalRecord after);

    /**
     * @param before FOR UPDATE current-read 得到的权威旧聚簇行
     * @param after 即将发布且聚簇 identity 不变的新完整行
     * @return physical key 变化时的 before/after 字节；按 prefix/collation 比较未变化时为空
     */
    Optional<byte[]> encodeUpdate(LogicalRecord before, LogicalRecord after);

    /**
     * @param before FOR UPDATE current-read 得到的待删除权威旧聚簇行
     * @return DELETE before physical entry 的稳定字节；目标 layout 不适用时为空
     */
    Optional<byte[]> encodeDelete(LogicalRecord before);

    /**
     * @param payload 已通过 row-log frame CRC 的完整 candidate bytes；不得截断或带尾随数据
     * @return 至少包含 before/after 一侧的非 delete-marked physical entries
     */
    OnlineIndexCandidate decode(byte[] payload);
}
