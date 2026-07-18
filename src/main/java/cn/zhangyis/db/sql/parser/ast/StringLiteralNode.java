package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.sql.parser.SourcePosition;
/**
 * 表示SQL 词法与语法解析中的 {@code StringLiteralNode} 不可变 AST 节点；保留源位置与语法值，binder 只读取该节点，不允许解析后就地改写。
 *
 * @param value 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
 * @param lexeme 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
 * @param position SQL 源文本中的稳定位置；不得为 {@code null}，行列信息必须指向当前待绑定或解析语句
 */
public record StringLiteralNode(String value, String lexeme, SourcePosition position) implements LiteralNode { }
