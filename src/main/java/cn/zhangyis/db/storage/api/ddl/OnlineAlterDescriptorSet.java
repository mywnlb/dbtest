package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.api.SegmentRef;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 一个通用INPLACE operation原子拥有的descriptor segment、page chain与全部index资源。
 * 该对象是storage返回给DD coordinator的完整物理所有权证明；列表顺序与manifest action ordinal一致，
 * 恢复据此避免把同一ALTER拆成多个可见字典版本。
 */
public final class OnlineAlterDescriptorSet {

    /** 持久DDL marker分配的正operation identity；descriptor chain的每一页都必须声明同一owner。 */
    private final long ddlOperationId;
    /** 仅在全部action同时发布时可见的目标字典版本；禁止某个index先行发布。 */
    private final long targetDictionaryVersion;
    /** descriptor所属逻辑表；用于阻止跨表复用同一物理资源集合。 */
    private final long tableId;
    /** 退役屏障使用的DDL generation；旧ReadView低于该值时不得回收DROP资源。 */
    private final long generation;
    /** 专属descriptor segment；它拥有page chain但不拥有被DROP索引的数据segment。 */
    private final SegmentRef descriptorSegment;
    /** 从anchor到尾页的完整物理链快照；顺序和唯一性已在构造时校验。 */
    private final List<PageId> descriptorPages;
    /** 按SQL action ordinal严格递增的ADD/DROP资源描述；列表是本operation原子资源全集。 */
    private final List<OnlineAlterIndexDescriptor> descriptors;
    /** manifest规范字节的SHA-256；恢复时用于拒绝marker、journal与sidecar串线。 */
    private final byte[] manifestDigest;

    /**
     * 冻结一个通用ALTER完整descriptor所有权集合。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验owner、目标版本、表和generation均为正值，阻止匿名资源进入恢复协议。</li>
     *     <li>复制并校验page chain全部位于descriptor segment且没有重复页，保持segment所有权闭合。</li>
     *     <li>按action ordinal检查descriptor严格递增、index identity唯一且root与segment同space。</li>
     *     <li>独占manifest digest字节，使返回对象可作为DD publish与恢复交叉验证的稳定快照。</li>
     * </ol>
     *
     * @param ddlOperationId 持久marker分配的正operation identity
     * @param targetDictionaryVersion 全部动作共同发布的正目标字典版本
     * @param tableId descriptor所属的正逻辑表identity
     * @param generation 本次物理构建与retirement fence共享的正generation
     * @param descriptorSegment 专属descriptor chain segment；不得为空
     * @param descriptorPages 从anchor到尾页的非空有序page chain
     * @param descriptors 按SQL action ordinal严格递增的非空index资源集合
     * @param manifestDigest marker/journal manifest规范字节的32-byte SHA-256
     * @throws DatabaseValidationException identity、链所有权、顺序、重复资源或digest形状非法时抛出
     */
    public OnlineAlterDescriptorSet(long ddlOperationId, long targetDictionaryVersion,
                                    long tableId, long generation,
                                    SegmentRef descriptorSegment,
                                    List<PageId> descriptorPages,
                                    List<OnlineAlterIndexDescriptor> descriptors,
                                    byte[] manifestDigest) {
        // 1. owner与版本必须先完整，不能让后续物理链替无identity的操作建立资源。
        if (ddlOperationId <= 0 || targetDictionaryVersion <= 0 || tableId <= 0
                || generation <= 0 || descriptorSegment == null
                || descriptorPages == null || descriptorPages.isEmpty()
                || descriptors == null || descriptors.isEmpty()
                || manifestDigest == null || manifestDigest.length != 32) {
            throw new DatabaseValidationException("invalid online ALTER descriptor set");
        }
        // 2. 复制并验证page chain，避免调用方在校验后替换页或跨space串链。
        this.descriptorPages = List.copyOf(descriptorPages);
        this.descriptors = List.copyOf(descriptors);
        Set<PageId> pages = new HashSet<>();
        Set<Integer> ordinals = new HashSet<>();
        Set<Long> indexIds = new HashSet<>();
        for (PageId page : this.descriptorPages) {
            if (page == null || !page.spaceId().equals(descriptorSegment.spaceId())
                    || !pages.add(page)) {
                throw new DatabaseValidationException(
                        "online ALTER descriptor pages are null/cross-space/duplicate");
            }
        }
        // 3. descriptor必须与manifest动作顺序一致，且一个operation不能重复拥有同一index。
        int previousOrdinal = -1;
        for (OnlineAlterIndexDescriptor descriptor : this.descriptors) {
            if (descriptor == null || descriptor.actionOrdinal() <= previousOrdinal
                    || !ordinals.add(descriptor.actionOrdinal())
                    || !indexIds.add(descriptor.indexBinding().indexId())
                    || !descriptor.indexBinding().rootPageId().spaceId()
                    .equals(descriptorSegment.spaceId())) {
                throw new DatabaseValidationException(
                        "online ALTER descriptors are not unique/ordered/in-space");
            }
            previousOrdinal = descriptor.actionOrdinal();
        }
        // 4. 最后发布全部不可变字段；digest克隆后调用方不能改写恢复证据。
        this.ddlOperationId = ddlOperationId;
        this.targetDictionaryVersion = targetDictionaryVersion;
        this.tableId = tableId;
        this.generation = generation;
        this.descriptorSegment = descriptorSegment;
        this.manifestDigest = manifestDigest.clone();
    }

    public long ddlOperationId() { return ddlOperationId; }
    public long targetDictionaryVersion() { return targetDictionaryVersion; }
    public long tableId() { return tableId; }
    public long generation() { return generation; }
    public SegmentRef descriptorSegment() { return descriptorSegment; }
    public List<PageId> descriptorPages() { return descriptorPages; }
    public List<OnlineAlterIndexDescriptor> descriptors() { return descriptors; }
    public byte[] manifestDigest() { return manifestDigest.clone(); }
}
