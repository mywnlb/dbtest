package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/** CREATE TABLE 的索引物理请求；table 请求保证恰好一个 clustered，其余为 secondary 索引。
 *
 * @param indexId 参与 {@code 构造} 的零基位置 {@code indexId}；必须非负且小于所属页面、集合或持久结构的容量
 * @param name 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
 * @param unique 索引结构属性；为 {@code true} 时必须执行相应聚簇、唯一性或主键不变量校验
 * @param clustered 索引结构属性；为 {@code true} 时必须执行相应聚簇、唯一性或主键不变量校验
 * @param keyParts 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 */
public record StorageIndexDefinition(long indexId, String name, boolean unique, boolean clustered,
                                     List<StorageIndexKeyPart> keyParts) {
    public StorageIndexDefinition {
        if (indexId <= 0 || name == null || name.isBlank() || keyParts == null || keyParts.isEmpty()) {
            throw new DatabaseValidationException("invalid storage index definition");
        }
        keyParts = List.copyOf(keyParts);
        if (clustered && !unique) {
            throw new DatabaseValidationException("clustered storage index must be unique");
        }
    }
}
