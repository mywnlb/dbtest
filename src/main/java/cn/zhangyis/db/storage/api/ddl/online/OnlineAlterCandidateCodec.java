package cn.zhangyis.db.storage.api.ddl.online;

/** 通用INPLACE多目标candidate codec；解码结果仍按manifest ordinal保持确定顺序。 */
public interface OnlineAlterCandidateCodec extends OnlineDdlCandidateCodec {

    /**
     * @param payload journal中已通过外层CRC校验的完整candidate bytes
     * @return 至少包含一个ADD INDEX目标的有序candidate
     */
    OnlineAlterCandidate decode(byte[] payload);
}
