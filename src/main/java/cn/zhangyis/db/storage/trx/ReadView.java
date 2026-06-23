package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

import java.util.Set;

/**
 * 一致性读快照（Snapshot，设计 §5.4，T1.4）。捕获某一时刻"哪些读写事务的修改对本次一致性读不可见"，
 * 供 {@link MvccReader} 沿版本链选出可见版本。不可变：创建后字段与活跃集合不再变化（RR 复用、RC 每读新建）。
 *
 * <p><b>字段语义</b>：
 * <ul>
 *   <li>{@code creatorTrxId}：本快照所属事务的写 id；只读事务为 {@link TransactionId#NONE}（看不到任何自身写）。</li>
 *   <li>{@code upLimitId}：低水位——{@code recordTrxId < upLimitId} 的修改一定在本快照前提交，可见。</li>
 *   <li>{@code lowLimitId}：高水位（= 创建时 nextTransactionId）——{@code recordTrxId >= lowLimitId} 的事务在本快照之后才开始，不可见。</li>
 *   <li>{@code activeIds}：创建时活跃读写事务 id 集合（不可变副本）；落在 {@code [up, low)} 内且在此集合中的修改仍未提交，不可见。</li>
 * </ul>
 *
 * <p><b>不变量</b>（构造校验）：{@code upLimitId <= lowLimitId}；每个 active id ∈ {@code [upLimitId, lowLimitId)}
 * （up = min(active) 或集合空时 = low；active 均 &lt; 创建时 nextId）。违反即视为损坏输入。
 */
public final class ReadView {

    /** 本快照所属事务写 id；只读为 NONE。 */
    private final TransactionId creatorTrxId;
    /** 低水位：&lt; 它的 recordTrxId 必可见。 */
    private final long upLimitId;
    /** 高水位：&gt;= 它的 recordTrxId 必不可见。 */
    private final long lowLimitId;
    /** 创建时活跃读写事务 id（不可变副本）。 */
    private final Set<Long> activeIds;
    /**
     * 创建时的提交序低水位 = 当时"下一个待分配 TransactionNo"。purge 用：某已提交 undo log 的 {@code TransactionNo}
     * 小于所有存活 ReadView 的 {@code lowLimitNo} 时，说明它在每个存活快照创建前就已提交、对所有快照可见，其旧版本
     * 不再被任何一致性读需要，可被 purge 物理回收。与可见性五规则（按 TransactionId）无关，仅供 purge 边界（按 TransactionNo）。
     */
    private final long lowLimitNo;

    /**
     * @param creatorTrxId 所属事务写 id（可为 {@link TransactionId#NONE}，不可为 null）。
     * @param upLimitId    低水位。
     * @param lowLimitId   高水位，必须 &gt;= upLimitId。
     * @param activeIds    活跃事务 id，防御性复制为不可变集合；每个元素必须 ∈ {@code [upLimitId, lowLimitId)}。
     * @param lowLimitNo   创建时下一个待分配 TransactionNo（purge 边界用），必须 &gt;= 0。
     */
    public ReadView(TransactionId creatorTrxId, long upLimitId, long lowLimitId, Set<Long> activeIds, long lowLimitNo) {
        if (creatorTrxId == null || activeIds == null) {
            throw new DatabaseValidationException("read view creatorTrxId/activeIds must not be null");
        }
        if (upLimitId > lowLimitId) {
            throw new DatabaseValidationException("read view upLimitId " + upLimitId
                    + " must be <= lowLimitId " + lowLimitId);
        }
        if (lowLimitNo < 0) {
            throw new DatabaseValidationException("read view lowLimitNo must be >= 0: " + lowLimitNo);
        }
        Set<Long> copy = Set.copyOf(activeIds);
        for (long id : copy) {
            if (id < upLimitId || id >= lowLimitId) {
                throw new DatabaseValidationException("active id " + id + " out of [up=" + upLimitId
                        + ", low=" + lowLimitId + ")");
            }
        }
        this.creatorTrxId = creatorTrxId;
        this.upLimitId = upLimitId;
        this.lowLimitId = lowLimitId;
        this.activeIds = copy;
        this.lowLimitNo = lowLimitNo;
    }

    /**
     * 判断某记录版本的 writer 事务对本快照是否可见（设计 §5.4 五规则，按序短路）：
     * <ol>
     *   <li>{@code == creatorTrxId} → 可见（看见自己的修改）。</li>
     *   <li>{@code < upLimitId} → 可见。</li>
     *   <li>{@code >= lowLimitId} → 不可见。</li>
     *   <li>{@code ∈ activeIds} → 不可见。</li>
     *   <li>其余 → 可见。</li>
     * </ol>
     *
     * @param recordTrxId 记录版本的 {@code DB_TRX_ID}；为 null 或 {@link TransactionId#NONE} 视为损坏输入拒绝
     *                    （聚簇记录恒有真实 writer id）。
     */
    public boolean isVisible(TransactionId recordTrxId) {
        if (recordTrxId == null || recordTrxId.isNone()) {
            throw new DatabaseValidationException("record DB_TRX_ID must not be null/NONE for visibility check");
        }
        if (recordTrxId.equals(creatorTrxId)) {
            return true;
        }
        long id = recordTrxId.value();
        if (id < upLimitId) {
            return true;
        }
        if (id >= lowLimitId) {
            return false;
        }
        return !activeIds.contains(id);
    }

    public TransactionId creatorTrxId() {
        return creatorTrxId;
    }

    public long upLimitId() {
        return upLimitId;
    }

    public long lowLimitId() {
        return lowLimitId;
    }

    /** 创建时的提交序低水位（下一个待分配 TransactionNo）；purge 边界用。 */
    public long lowLimitNo() {
        return lowLimitNo;
    }

    /** 活跃事务 id 不可变快照。 */
    public Set<Long> activeIds() {
        return activeIds;
    }
}
