package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeRedoBudgetEstimator;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.trx.UndoRedoBudgetEstimator;
import cn.zhangyis.db.storage.trx.UndoWritePlan;
import cn.zhangyis.db.storage.trx.DeferredInsertUndoPlan;
import cn.zhangyis.db.storage.trx.DeferredUpdateUndoPlan;
import cn.zhangyis.db.storage.api.lob.LobWritePlan;

import java.util.List;

/** 单行 clustered DML 的 redo workload 组合器：B+Tree 结构部分 + 同批 undo append 部分。 */
final class DmlRedoBudgetEstimator {

    private DmlRedoBudgetEstimator() {
    }

    /**
     * 根据调用参数创建或转换 {@code insert} 返回的 {@code RedoBudgetWorkload}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param undoPlan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @return {@code insert} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    static RedoBudgetWorkload insert(BTreeIndex index, UndoWritePlan undoPlan) {
        requireIndex(index);
        requirePlan(undoPlan);
        return BTreeRedoBudgetEstimator.insert(index.rootLevel())
                .plus(undoPlan.redoWorkload());
    }

    /** deferred undo、B+Tree 最坏 SMO 与同一行全部 LOB allocation 在 begin 前一次性合并 admission。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param undoPlan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param lobPlans 参与 {@code insert} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code insert} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static RedoBudgetWorkload insert(BTreeIndex index, DeferredInsertUndoPlan undoPlan,
                                     List<LobWritePlan> lobPlans) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        requireIndex(index);
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        if (undoPlan == null || lobPlans == null) {
            throw new DatabaseValidationException("deferred DML redo plans must not be null");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        RedoBudgetWorkload workload = BTreeRedoBudgetEstimator.insert(index.rootLevel())
                .plus(undoPlan.redoWorkload());
        for (LobWritePlan lobPlan : lobPlans) {
            if (lobPlan == null) {
                throw new DatabaseValidationException("LOB redo plan list must not contain null");
            }
            workload = workload.plus(lobPlan.workload());
        }
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        return workload;
    }

    /** 兼容既有 estimator 单元测试；生产 DML 必须使用携带 external 页数的计划重载。
     *
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param firstUndoWrite 当前对象是否处于首次创建、首次写入或复用分支；该事实决定初始化、redo 和失败补偿路径
     * @return {@code insert} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    static RedoBudgetWorkload insert(BTreeIndex index, boolean firstUndoWrite) {
        requireIndex(index);
        return BTreeRedoBudgetEstimator.insert(index.rootLevel())
                .plus(UndoRedoBudgetEstimator.append(firstUndoWrite));
    }

    /**
     * 根据调用参数创建或转换 {@code pointRewrite} 返回的 {@code RedoBudgetWorkload}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param undoPlan 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @return {@code pointRewrite} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    static RedoBudgetWorkload pointRewrite(BTreeIndex index, UndoWritePlan undoPlan) {
        requireIndex(index);
        requirePlan(undoPlan);
        return BTreeRedoBudgetEstimator.pointRewrite()
                .plus(undoPlan.redoWorkload());
    }

    /**
     * 合并 LOB-aware UPDATE 的 leaf rewrite、deferred undo 和全部新 LOB allocation workload。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param index    exact-version 聚簇索引；root level 不参与 point rewrite，但用于统一 descriptor 校验。
     * @param undoPlan placeholder/actual 物理形状一致的 deferred UPDATE undo。
     * @param lobPlans 本次 UPDATE 新建 external chain 的逐列计划；可以为空但不能为 {@code null}。
     * @return 在业务 MTR begin 前冻结的组合 redo workload。
     * @throws DatabaseValidationException index/plan 缺失或列表包含 {@code null} 时抛出。
     */
    static RedoBudgetWorkload pointRewrite(BTreeIndex index, DeferredUpdateUndoPlan undoPlan,
                                            List<LobWritePlan> lobPlans) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        requireIndex(index);
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        if (undoPlan == null || lobPlans == null) {
            throw new DatabaseValidationException("deferred UPDATE redo plans must not be null");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        RedoBudgetWorkload workload = BTreeRedoBudgetEstimator.pointRewrite().plus(undoPlan.redoWorkload());
        for (LobWritePlan lobPlan : lobPlans) {
            if (lobPlan == null) {
                throw new DatabaseValidationException("UPDATE LOB redo plan list must not contain null");
            }
            workload = workload.plus(lobPlan.workload());
        }
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        return workload;
    }

    /** 兼容既有 estimator 单元测试；不用于 external-aware 生产 admission。
     *
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param firstUndoWrite 当前对象是否处于首次创建、首次写入或复用分支；该事实决定初始化、redo 和失败补偿路径
     * @return {@code pointRewrite} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    static RedoBudgetWorkload pointRewrite(BTreeIndex index, boolean firstUndoWrite) {
        requireIndex(index);
        return BTreeRedoBudgetEstimator.pointRewrite()
                .plus(UndoRedoBudgetEstimator.append(firstUndoWrite));
    }

    private static void requireIndex(BTreeIndex index) {
        if (index == null) {
            throw new DatabaseValidationException("DML redo budget index must not be null");
        }
    }

    private static void requirePlan(UndoWritePlan plan) {
        if (plan == null) {
            throw new DatabaseValidationException("DML undo write plan must not be null");
        }
    }
}
