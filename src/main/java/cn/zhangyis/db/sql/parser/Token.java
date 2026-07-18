package cn.zhangyis.db.sql.parser;

/** 单个不可变 token。text 对 STRING/HEX/BIT 已去除引号并完成 framing 解码前校验。
 *
 * @param type 选择 {@code 构造} 分支的 {@code TokenType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
 * @param text 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
 * @param lexeme 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
 * @param position SQL 源文本中的稳定位置；不得为 {@code null}，行列信息必须指向当前待绑定或解析语句
 */
record Token(TokenType type, String text, String lexeme, SourcePosition position) { }
