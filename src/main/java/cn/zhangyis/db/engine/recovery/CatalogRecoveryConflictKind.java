package cn.zhangyis.db.engine.recovery;

/**
 * catalog-loss inspection 的稳定冲突分类及其隔离权限。
 */
public enum CatalogRecoveryConflictKind {
    /** 零长度 catalog 可能是丢失证据，必须显式隔离后才允许重建。 */
    CATALOG_EMPTY(true),
    /** 非空但不能严格打开的 catalog 必须保留到隔离目录。 */
    CATALOG_CORRUPT(true),
    /** 零长度 control 不能被覆盖，必须显式隔离。 */
    CONTROL_EMPTY(true),
    /** 双槽均损坏的 control 不能被覆盖，必须显式隔离。 */
    CONTROL_CORRUPT(true),
    /** 从未生成独立 manifest，不能只靠 SDI 猜 schema/目录。 */
    MANIFEST_MISSING(false),
    /** manifest 物理或逻辑事件损坏。 */
    MANIFEST_CORRUPT(false),
    /** clean snapshot 后仍存在未裁决 catalog mutation intent。 */
    MANIFEST_DIRTY(false),
    /** clean manifest 唯一期望路径缺失；该文件不能用其它副本自动替代。 */
    EXPECTED_CANDIDATE_MISSING(false),
    /** manifest 唯一期望文件未通过全页 scrub；必须从备份修复。 */
    EXPECTED_CANDIDATE_INVALID(false),
    /** expected 文件 SDI 与 clean snapshot 的 table aggregate 不一致。 */
    EXPECTED_SDI_MISMATCH(false),
    /** 目录中出现 manifest 未声明的受控命名候选，可由管理员显式隔离。 */
    EXTRA_CANDIDATE(true),
    /** manifest 未声明且 scrub 失败的候选仍作为证据显式隔离。 */
    EXTRA_CANDIDATE_INVALID(true),
    /** 多个路径声明相同 table identity；只有非 expected 副本可隔离。 */
    DUPLICATE_TABLE_ID(true),
    /** 多个路径声明相同 space identity；只有非 expected 副本可隔离。 */
    DUPLICATE_SPACE_ID(true),
    /** 目录无法完整枚举，不能签发重建许可。 */
    DIRECTORY_SCAN_FAILED(false);

    /** 是否允许 quarantine API 对此冲突的具体路径执行显式 atomic move。 */
    private final boolean quarantinable;

    CatalogRecoveryConflictKind(boolean quarantinable) {
        this.quarantinable = quarantinable;
    }

    /**
     * @return 当前冲突类型是否允许显式隔离
     */
    public boolean quarantinable() {
        return quarantinable;
    }
}
