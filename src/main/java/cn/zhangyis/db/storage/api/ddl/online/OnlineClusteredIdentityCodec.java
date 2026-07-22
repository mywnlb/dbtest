package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.storage.record.page.SearchKey;

/** SHADOW candidate codec：payload只包含完整聚簇主键，绝不复制普通列、LOB或事务隐藏列。 */
public interface OnlineClusteredIdentityCodec extends OnlineDdlCandidateCodec {

    /** @param payload 已通过journal frame CRC的完整identity bytes；@return 完整聚簇物理键 */
    SearchKey decode(byte[] payload);
}
