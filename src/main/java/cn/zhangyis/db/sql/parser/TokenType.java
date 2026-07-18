package cn.zhangyis.db.sql.parser;

/** Lexer token；关键字保留为 IDENT，由 parser 以 Locale.ROOT 大小写无关匹配。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code IDENT}：标识符或由 parser 再判定的关键字 token</li>
 *     <li>{@code STRING}：已完成引号与转义处理的字符串字面量 token</li>
 *     <li>{@code NUMBER}：保留原始词形的数值字面量 token</li>
 *     <li>{@code HEX}：十六进制字节字面量 token</li>
 *     <li>{@code BIT}：二进制位串字面量 token</li>
 *     <li>{@code COMMA}：列表元素分隔符</li>
 *     <li>{@code DOT}：限定名称各部分的分隔符</li>
 *     <li>{@code LPAREN}：括号结构边界</li>
 *     <li>{@code RPAREN}：括号结构边界</li>
 *     <li>{@code EQUALS}：等值谓词或赋值运算符</li>
 *     <li>{@code STAR}：SELECT 全列投影标记</li>
 *     <li>{@code SEMICOLON}：单条语句的可选结束符</li>
 *     <li>{@code EOF}：输入已被完整消费的终止 token</li>
 * </ul>
 */
enum TokenType { IDENT, STRING, NUMBER, HEX, BIT, COMMA, DOT, LPAREN, RPAREN, EQUALS, STAR, SEMICOLON, EOF }
