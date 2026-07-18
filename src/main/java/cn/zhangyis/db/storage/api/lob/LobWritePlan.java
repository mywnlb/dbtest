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

    /**
     * 创建 {@code LobWritePlan}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param segment 参与 {@code 构造} 的稳定领域标识 {@code SegmentRef}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param type 选择 {@code 构造} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param payload 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param pageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param crc32 参与 {@code 构造} 的位域或校验值 {@code crc32}；只允许当前格式定义的位，数值按无符号位模式解释
     * @param inlinePrefix 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param workload redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    LobWritePlan(SegmentRef segment, ColumnType type, byte[] payload, int pageCount, long crc32,
                 byte[] inlinePrefix, RedoBudgetWorkload workload) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (segment == null || type == null || payload == null || payload.length == 0 || pageCount <= 0
                || crc32 < 0 || crc32 > 0xFFFF_FFFFL || inlinePrefix == null || workload == null) {
            throw new DatabaseValidationException("invalid frozen LOB write plan");
        }
        this.segment = segment;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.type = type;
        this.payload = Arrays.copyOf(payload, payload.length);
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.pageCount = pageCount;
        this.crc32 = crc32;
        this.inlinePrefix = Arrays.copyOf(inlinePrefix, inlinePrefix.length);
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
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
