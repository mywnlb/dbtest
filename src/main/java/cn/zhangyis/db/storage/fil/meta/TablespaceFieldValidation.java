package cn.zhangyis.db.storage.fil.meta;
import cn.zhangyis.db.storage.fil.io.DataFileDescriptor;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.state.SpaceFlags;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.util.List;

/**
 * 表空间元数据字段的共享校验。{@link TablespaceMetadata}(权威加载快照) 与 {@link Tablespace}(运行时快照)
 * 字段结构相同、校验不变量也相同；集中在此避免两份 record 的紧凑构造器各自维护一份易腐化的重复校验。
 *
 * <p>该类只做结构校验：必填引用非空、名称非空白、文件列表非空以及 version 非负。它不执行文件 IO，
 * 不验证 type/state 是否匹配，不比较 flags 中的类型位，不要求 freeLimit 小于 current size，也不对账
 * 多文件范围；这些跨字段或跨来源不变量由加载器和生命周期编排层负责。</p>
 */
final class TablespaceFieldValidation {

    /**
     * 纯静态 record 构造校验器，不允许实例化。
     */
    private TablespaceFieldValidation() {
    }

    /**
     * 校验两个 tablespace record 共享的结构字段，并生成数据文件列表的不可变副本。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先验证 SpaceId、类型、PageSize、状态和 flags 等身份/解释字段非空，避免构造出无法确定
     *     页几何或生命周期语义的快照。</li>
     *     <li>验证 current size 与 free limit 值对象非空；这里只保留上游读到的数值，不裁决二者关系。</li>
     *     <li>验证逻辑名称与文件列表容器，并通过 {@link List#copyOf(java.util.Collection)} 切断调用方后续
     *     修改；空列表不能供 {@link PageStore} 定位任何物理范围。</li>
     *     <li>最后拒绝负版本并返回文件快照；成功不产生 registry、文件或 page0 副作用。</li>
     * </ol>
     *
     * @param spaceId page0、registry 和 PageId 共同使用的非空表空间标识
     * @param name 非空白逻辑名称；仅作元数据身份/诊断，不用于拼接物理路径
     * @param type 从 flags 或创建请求确定的非空表空间类型
     * @param pageSize 解释所有 dataFiles 页边界的非空固定页大小
     * @param state 当前非空生命周期状态；本方法不验证类型对应的合法状态集合
     * @param dataFiles 至少一个、且元素非空的数据文件描述符；当前生产模型通常为单文件
     * @param spaceFlags page0 原始 flags 的非空快照
     * @param currentSizeInPages 已发布物理大小的非空页数值对象
     * @param freeLimitPageNo FSP 当前可分配上界的非空页号；本方法不与 current size 比较
     * @param spaceVersion 非负运行期元数据版本，用于 registry 快照替换与诊断
     * @return 与输入顺序一致的不可变 dataFiles 副本；不会返回 {@code null}
     * @throws DatabaseValidationException 必填字段为空、名称为空白、文件列表为空或版本为负时抛出；
     *                                     不会发布任何元数据
     */
    static List<DataFileDescriptor> validate(SpaceId spaceId, String name, TablespaceType type, PageSize pageSize,
            TablespaceState state, List<DataFileDescriptor> dataFiles, SpaceFlags spaceFlags,
            PageNo currentSizeInPages, PageNo freeLimitPageNo, long spaceVersion) {
        // 1. 身份与解释字段缺失时，下游无法安全解释 PageId、页大小或生命周期。
        if (spaceId == null) {
            throw new DatabaseValidationException("tablespace space id must not be null");
        }
        if (type == null) {
            throw new DatabaseValidationException("tablespace type must not be null");
        }
        if (pageSize == null) {
            throw new DatabaseValidationException("tablespace page size must not be null");
        }
        if (state == null) {
            throw new DatabaseValidationException("tablespace state must not be null");
        }
        if (spaceFlags == null) {
            throw new DatabaseValidationException("tablespace flags must not be null");
        }

        // 2. 尺寸标记必须存在；数值本身及二者关系由值对象和上游 page0/FSP 校验负责。
        if (currentSizeInPages == null) {
            throw new DatabaseValidationException("tablespace current size must not be null");
        }
        if (freeLimitPageNo == null) {
            throw new DatabaseValidationException("tablespace free limit must not be null");
        }

        // 3. 名称和文件范围形成可定位快照；复制后调用方无法通过原列表修改 record 状态。
        if (name == null || name.isBlank()) {
            throw new DatabaseValidationException("tablespace name must not be blank");
        }
        if (dataFiles == null) {
            throw new DatabaseValidationException("tablespace data files must not be null");
        }
        List<DataFileDescriptor> copy = List.copyOf(dataFiles);
        if (copy.isEmpty()) {
            throw new DatabaseValidationException("tablespace must contain at least one data file");
        }

        // 4. 版本只接受非负值；成功返回不触碰 registry、page0 或数据文件。
        if (spaceVersion < 0) {
            throw new DatabaseValidationException("tablespace version must be non-negative");
        }
        return copy;
    }
}
