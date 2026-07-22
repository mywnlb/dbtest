package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.storage.record.format.LogicalRecord;

import java.util.Optional;

/**
 * clustered DML到任意Online DDL capture payload的纯编码端口。实现只能读取调用方已经物化的逻辑行，
 * 不能访问页、事务表、DD repository或可变schema。
 */
public interface OnlineDdlCandidateCodec {

    Optional<byte[]> encodeInsert(LogicalRecord after);

    Optional<byte[]> encodeUpdate(LogicalRecord before, LogicalRecord after);

    Optional<byte[]> encodeDelete(LogicalRecord before);
}
