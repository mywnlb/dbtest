package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/** DDL schema 内容摘要算法；stable code 会进入 marker v4，禁止使用枚举 ordinal。 */
public enum DdlDigestAlgorithm {
    /** Java 平台强制提供的 SHA-256；输出固定为 32 bytes。 */
    SHA_256(1, "SHA-256", 32);

    /** marker 中持久化且跨版本稳定的正编码。 */
    private final int stableCode;
    /** 传给 JCA MessageDigest 的标准算法名称。 */
    private final String jcaName;
    /** 用于在构造值对象时拒绝截断或错误算法输出的固定长度。 */
    private final int digestBytes;

    DdlDigestAlgorithm(int stableCode, String jcaName, int digestBytes) {
        this.stableCode = stableCode;
        this.jcaName = jcaName;
        this.digestBytes = digestBytes;
    }

    /** @return marker v4 使用的稳定正编码。 */
    public int stableCode() {
        return stableCode;
    }

    /** @return JCA provider 识别的标准算法名称。 */
    public String jcaName() {
        return jcaName;
    }

    /** @return 该算法合法输出的精确字节数。 */
    public int digestBytes() {
        return digestBytes;
    }

    /**
     * 解码 marker 中的算法稳定码。
     *
     * @param code v4 payload 中读取的无符号稳定码
     * @return 与稳定码唯一对应的算法
     * @throws DatabaseValidationException code 未声明时抛出，调用方不得猜测摘要算法
     */
    public static DdlDigestAlgorithm fromStableCode(int code) {
        return Arrays.stream(values()).filter(value -> value.stableCode == code).findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "unknown DDL digest algorithm stable code: " + code));
    }
}
