package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.XaId;
import cn.zhangyis.db.sql.parser.SourcePosition;

import java.util.Arrays;

/**
 * XA SQL 中已完成字面量 framing 的 XID 节点；字符串按 UTF-8 转成字节，HEX 保留原字节。
 * AST 仍保存起始位置用于后续 Session 状态错误诊断。
 */
public final class XaIdentifierNode {

    /** signed 外部格式号。 */
    private final int formatId;
    /** gtrid 字节副本。 */
    private final byte[] gtrid;
    /** bqual 字节副本。 */
    private final byte[] bqual;
    /** XID 首个字面量的源位置。 */
    private final SourcePosition position;

    /**
     * 创建 XID AST 节点并复用领域边界校验。
     *
     * @param formatId signed 外部格式号
     * @param gtrid 1..64 字节全局身份
     * @param bqual 0..64 字节分支身份
     * @param position 首个字面量位置
     */
    public XaIdentifierNode(int formatId, byte[] gtrid, byte[] bqual, SourcePosition position) {
        if (position == null) {
            throw new DatabaseValidationException("XA identifier source position must not be null");
        }
        XaId validated = new XaId(formatId, gtrid, bqual);
        this.formatId = validated.formatId();
        this.gtrid = validated.gtrid();
        this.bqual = validated.bqual();
        this.position = position;
    }

    public int formatId() {
        return formatId;
    }

    public byte[] gtrid() {
        return Arrays.copyOf(gtrid, gtrid.length);
    }

    public byte[] bqual() {
        return Arrays.copyOf(bqual, bqual.length);
    }

    public SourcePosition position() {
        return position;
    }

    /** @return 与 AST 字节等值的共享领域 XID */
    public XaId toXaId() {
        return new XaId(formatId, gtrid, bqual);
    }
}
