package cn.zhangyis.db.common.json;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * RFC 8259 JSON 文本语法校验器。该类只判断文本是否具有唯一、严格的 JSON 语法，不构造对象树，也不改变数字精度；
 * 因而 Binder 与 Record codec 可以共享同一守门规则，同时继续把原始 UTF-8 文本作为 v1 稳定值保存。
 *
 * <p>与 Hutool 等便利解析器不同，本实现拒绝未加双引号的对象键、单引号字符串、尾逗号、前导零、非 JSON 空白和
 * 非法 surrogate。最大嵌套深度用于把恶意输入转成领域校验错误，而不是让 JVM 发生栈溢出。
 */
public final class StrictJsonValidator {

    /** 教学内核允许的最大容器嵌套；超限输入不应消耗无界调用栈。 */
    private static final int MAX_DEPTH = 256;

    private StrictJsonValidator() {
    }

    /**
     * 完整消费一段严格 JSON 文本。
     *
     * @param source 待校验的原始 JSON 文本，不能为 null。
     * @throws DatabaseValidationException 文本不符合 RFC 8259 或嵌套过深。
     */
    public static void validate(String source) {
        if (source == null) {
            throw new DatabaseValidationException("JSON text must not be null");
        }
        new Parser(source).parseDocument();
    }

    /** 单次调用拥有的线性 cursor；对象不共享，因此无需锁或原子变量。 */
    private static final class Parser {
        /** 当前校验的完整 Java 字符串。 */
        private final String source;
        /** 下一个尚未消费的 UTF-16 code unit 下标。 */
        private int offset;

        private Parser(String source) {
            this.source = source;
        }

        /** 根值前后只接受四种 JSON 空白，并要求 EOF。 */
        private void parseDocument() {
            skipWhitespace();
            parseValue(0);
            skipWhitespace();
            if (offset != source.length()) {
                fail("trailing content after JSON value");
            }
        }

        /** 根据首字符分派 value；容器递归显式携带深度上界。
         *
         * @param depth 参与 {@code parseValue} 的树层级或递归深度 {@code depth}；必须非负且不得超过当前页结构、MTR memo 或解析器声明的最大深度
         */
        private void parseValue(int depth) {
            if (offset >= source.length()) {
                fail("JSON value expected");
            }
            char current = source.charAt(offset);
            switch (current) {
                case '{' -> parseObject(depth);
                case '[' -> parseArray(depth);
                case '"' -> parseString();
                case 't' -> parseLiteral("true");
                case 'f' -> parseLiteral("false");
                case 'n' -> parseLiteral("null");
                default -> {
                    if (current == '-' || isDigit(current)) {
                        parseNumber();
                    } else {
                        fail("unsupported JSON value token");
                    }
                }
            }
        }

        /** object key 必须是双引号字符串；逗号后必须紧跟下一成员，因而尾逗号自然失败。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
         *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
         *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
         *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
         * </ol>
         *
         * @param depth 参与 {@code parseObject} 的树层级或递归深度 {@code depth}；必须非负且不得超过当前页结构、MTR memo 或解析器声明的最大深度
         */
        private void parseObject(int depth) {
            // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
            requireContainerDepth(depth);
            // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
            offset++;
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            skipWhitespace();
            if (consume('}')) {
                return;
            }
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            while (true) {
                if (!peek('"')) {
                    fail("JSON object key must be a double-quoted string");
                }
                parseString();
                skipWhitespace();
                require(':');
                skipWhitespace();
                parseValue(depth + 1);
                skipWhitespace();
                if (consume('}')) {
                    return;
                }
                require(',');
                skipWhitespace();
            }
        }

        /** array 元素必须由单个逗号分隔；空数组只允许直接闭合。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
         *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
         *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
         *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
         * </ol>
         *
         * @param depth 参与 {@code parseArray} 的树层级或递归深度 {@code depth}；必须非负且不得超过当前页结构、MTR memo 或解析器声明的最大深度
         */
        private void parseArray(int depth) {
            // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
            requireContainerDepth(depth);
            // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
            offset++;
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            skipWhitespace();
            if (consume(']')) {
                return;
            }
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            while (true) {
                parseValue(depth + 1);
                skipWhitespace();
                if (consume(']')) {
                    return;
                }
                require(',');
                skipWhitespace();
            }
        }

        /**
         * 严格字符串：拒绝控制字符和单独 surrogate；反斜杠只接受 JSON 定义的八类 escape，unicode escape
         * 若表示高 surrogate，必须紧跟一个低 surrogate escape。
         */
        private void parseString() {
            require('"');
            while (offset < source.length()) {
                char current = source.charAt(offset++);
                if (current == '"') {
                    return;
                }
                if (current < 0x20) {
                    fail("unescaped control character in JSON string");
                }
                if (current == '\\') {
                    parseEscape();
                } else if (Character.isHighSurrogate(current)) {
                    if (offset >= source.length() || !Character.isLowSurrogate(source.charAt(offset))) {
                        fail("unpaired high surrogate in JSON string");
                    }
                    offset++;
                } else if (Character.isLowSurrogate(current)) {
                    fail("unpaired low surrogate in JSON string");
                }
            }
            fail("unterminated JSON string");
        }

        /** 解析一个反斜杠 escape，并完整验证 unicode surrogate pair。
         * <p>数据流：</p>
         * <ol>
         *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
         *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
         *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
         *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
         * </ol>
         *
         */
        private void parseEscape() {
            // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
            if (offset >= source.length()) {
                fail("unterminated JSON escape");
            }
            char escaped = source.charAt(offset++);
            if (escaped == '"' || escaped == '\\' || escaped == '/' || escaped == 'b'
                    || escaped == 'f' || escaped == 'n' || escaped == 'r' || escaped == 't') {
                return;
            }
            // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
            if (escaped != 'u') {
                fail("unsupported JSON escape");
            }
            char decoded = parseHexCodeUnit();
            if (Character.isLowSurrogate(decoded)) {
                fail("unicode escape contains an unpaired low surrogate");
            }
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            if (!Character.isHighSurrogate(decoded)) {
                return;
            }
            if (offset + 1 >= source.length() || source.charAt(offset) != '\\'
                    || source.charAt(offset + 1) != 'u') {
                fail("unicode high surrogate escape requires a low surrogate escape");
            }
            offset += 2;
            char low = parseHexCodeUnit();
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            if (!Character.isLowSurrogate(low)) {
                fail("unicode high surrogate escape is followed by a non-low surrogate");
            }
        }

        /** 恰好读取四个十六进制字符，不接受 Java/Hutool 扩展格式。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
         *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
         *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
         *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
         * </ol>
         *
         * @return {@code parseHexCodeUnit} 从受校验输入或持久字节中得到的 {@code char} 结果；位宽、符号和特殊值语义遵循当前格式，无法表示时抛出领域异常
         */
        private char parseHexCodeUnit() {
            // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
            if (offset + 4 > source.length()) {
                fail("incomplete unicode escape");
            }
            // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
            int value = 0;
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            for (int i = 0; i < 4; i++) {
                int digit = Character.digit(source.charAt(offset++), 16);
                if (digit < 0) {
                    fail("non-hex character in unicode escape");
                }
                value = (value << 4) | digit;
            }
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            return (char) value;
        }

        /** RFC 8259 number：整数零不可带前导数字，小数与指数标记后都必须至少有一位。
         * <p>数据流：</p>
         * <ol>
         *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
         *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
         *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
         *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
         * </ol>
         *
         */
        private void parseNumber() {
            // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
            consume('-');
            // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
            if (consume('0')) {
                if (offset < source.length() && isDigit(source.charAt(offset))) {
                    fail("leading zero in JSON number");
                }
            } else {
                requireDigit("JSON integer digits expected");
                while (offset < source.length() && isDigit(source.charAt(offset))) {
                    offset++;
                }
            }
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            if (consume('.')) {
                requireDigit("JSON fraction digits expected");
                while (offset < source.length() && isDigit(source.charAt(offset))) {
                    offset++;
                }
            }
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            if (consume('e') || consume('E')) {
                if (!consume('+')) {
                    consume('-');
                }
                requireDigit("JSON exponent digits expected");
                while (offset < source.length() && isDigit(source.charAt(offset))) {
                    offset++;
                }
            }
        }

        /** true/false/null 必须逐字符完全匹配。 */
        private void parseLiteral(String literal) {
            if (!source.startsWith(literal, offset)) {
                fail("invalid JSON literal");
            }
            offset += literal.length();
        }

        private void requireContainerDepth(int depth) {
            if (depth >= MAX_DEPTH) {
                fail("JSON nesting exceeds " + MAX_DEPTH);
            }
        }

        private void requireDigit(String message) {
            if (offset >= source.length() || !isDigit(source.charAt(offset))) {
                fail(message);
            }
            offset++;
        }

        /**
         * 从当前输入 token 与局部游标解析 {@code skipWhitespace} 对应的格式结构；成功推进到确定边界，失败报告位置且不发布半解析对象。
         */
        private void skipWhitespace() {
            while (offset < source.length()) {
                char current = source.charAt(offset);
                if (current != ' ' && current != '\t' && current != '\r' && current != '\n') {
                    return;
                }
                offset++;
            }
        }

        private void require(char expected) {
            if (!consume(expected)) {
                fail("expected '" + expected + "'");
            }
        }

        private boolean consume(char expected) {
            if (!peek(expected)) {
                return false;
            }
            offset++;
            return true;
        }

        private boolean peek(char expected) {
            return offset < source.length() && source.charAt(offset) == expected;
        }

        private void fail(String message) {
            throw new DatabaseValidationException(message + " at JSON offset " + offset);
        }

        private static boolean isDigit(char value) {
            return value >= '0' && value <= '9';
        }
    }
}
