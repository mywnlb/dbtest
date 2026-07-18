package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.SourcePosition;

/** 保留用户拼写的标识符与起始位置；canonicalization 属于 Binder/DD。
 *
 * @param value 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
 * @param position SQL 源文本中的稳定位置；不得为 {@code null}，行列信息必须指向当前待绑定或解析语句
 */
public record IdentifierNode(String value, SourcePosition position) {
    public IdentifierNode {
        if (value == null || value.isBlank() || position == null) {
            throw new DatabaseValidationException("identifier value/position must not be blank/null");
        }
    }
}
