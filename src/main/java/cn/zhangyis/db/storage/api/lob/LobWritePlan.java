package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;

import java.util.Arrays;

/**
 * begin 写 MTR 前冻结的 LOB 页链计划。它不访问 FSP/Buffer Pool，只保存从完整逻辑值确定派生的 payload、页数、
 * CRC、record inline prefix 和 redo workload；执行阶段不得重新解释调用方可变输入。
 */
public final class LobWritePlan {

    /** 权威表 binding 给出的目标 segment identity；purpose 留到物理 preflight 复核。 */
    private final SegmentRef segment;

    /** 目标 record 列类型；决定 LobCodec family 与 external envelope type。 */
    private final ColumnType type;

    /** 已通过 LobCodec 逻辑校验的完整 payload 防御性副本。 */
    private final byte[] payload;

    /** 按实例页容量计算的 canonical chain 页数。 */
    private final int pageCount;

    /** 完整 payload 的 unsigned CRC32，写页和 LobReference 必须一致。 */
    private final long crc32;

    /** record external envelope 携带的字符边界安全 prefix。 */
    private final byte[] inlinePrefix;

    /** begin 前聚合 admission 使用的保守 redo 工作量。 */
    private final RedoBudgetWorkload workload;

    LobWritePlan(SegmentRef segment, ColumnType type, byte[] payload, int pageCount, long crc32,
                 byte[] inlinePrefix, RedoBudgetWorkload workload) {
        if (segment == null || type == null || payload == null || payload.length == 0 || pageCount <= 0
                || crc32 < 0 || crc32 > 0xFFFF_FFFFL || inlinePrefix == null || workload == null) {
            throw new DatabaseValidationException("invalid frozen LOB write plan");
        }
        this.segment = segment;
        this.type = type;
        this.payload = Arrays.copyOf(payload, payload.length);
        this.pageCount = pageCount;
        this.crc32 = crc32;
        this.inlinePrefix = Arrays.copyOf(inlinePrefix, inlinePrefix.length);
        this.workload = workload;
    }

    /** 返回计划绑定的权威 segment identity。 */
    public SegmentRef segment() {
        return segment;
    }

    /** 返回冻结时使用的稳定列类型。 */
    public ColumnType type() {
        return type;
    }

    /** 返回完整 payload 的防御性副本。 */
    public byte[] payload() {
        return Arrays.copyOf(payload, payload.length);
    }

    /** 返回完整逻辑 payload 字节数。 */
    public int totalLength() {
        return payload.length;
    }

    /** 返回 canonical chain 页数。 */
    public int pageCount() {
        return pageCount;
    }

    /** 返回 unsigned CRC32。 */
    public long crc32() {
        return crc32;
    }

    /** 返回 record external envelope prefix 的防御性副本。 */
    public byte[] inlinePrefix() {
        return Arrays.copyOf(inlinePrefix, inlinePrefix.length);
    }

    /** 返回 begin 前可与 undo/B+Tree workload 合并的不可变工作量。 */
    public RedoBudgetWorkload workload() {
        return workload;
    }

    /** 同包执行器读取冻结数组；该引用绝不跨包暴露。 */
    byte[] payloadUnsafe() {
        return payload;
    }
}
