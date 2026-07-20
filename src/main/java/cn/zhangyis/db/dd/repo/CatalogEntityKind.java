package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;

import java.util.Arrays;

/** DD logical row kind 的稳定磁盘码；不得依赖 enum ordinal。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code SCHEMA}：表示“SCHEMA”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code TABLE}：表示“表”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code COLUMN}：表示“COLUMN”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code INDEX}：表示“INDEX”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code SCHEMA_TOMBSTONE}：表示“SCHEMATOMBSTONE”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code TABLE_TOMBSTONE}：表示“表TOMBSTONE”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code DDL_LOG}：表示“DDLLOG”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code CATALOG_COMMIT}：表示“CATALOG提交”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 * </ul>
 */
enum CatalogEntityKind {
    SCHEMA(1),
    TABLE(2),
    COLUMN(3),
    INDEX(4),
    SCHEMA_TOMBSTONE(5),
    TABLE_TOMBSTONE(6),
    DDL_LOG(7),
    /** 灾难恢复新 catalog 的首批全量快照元数据；既有增量批次不使用。 */
    CATALOG_BASELINE_META(8),
    /** 全量 baseline 批次的独立提交封口；126 保留在普通 commit 127 之前。 */
    CATALOG_BASELINE_COMMIT(126),
    CATALOG_COMMIT(127);

    /**
     * 记录 {@code stableCode} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final int stableCode;

    /**
     * 创建 {@code CatalogEntityKind}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param stableCode 参与 {@code 构造} 的稳定编码 {@code stableCode}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     */
    CatalogEntityKind(int stableCode) {
        this.stableCode = stableCode;
    }

    int stableCode() {
        return stableCode;
    }

    /**
     * 根据调用参数构造 {@code fromStableCode} 对应的数据字典领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param code 参与 {@code fromStableCode} 的稳定编码 {@code code}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     * @return {@code fromStableCode} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    static CatalogEntityKind fromStableCode(int code) {
        return Arrays.stream(values()).filter(kind -> kind.stableCode == code).findFirst()
                .orElseThrow(() -> new DictionaryCatalogCorruptionException(
                        "unknown dictionary catalog entity kind: " + code));
    }
}
