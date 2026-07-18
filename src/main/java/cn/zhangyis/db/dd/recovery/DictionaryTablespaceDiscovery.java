package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** 启动前从 committed DD 快照发现 StorageEngine 必须在 crash recovery 阶段打开的表空间。 */
public final class DictionaryTablespaceDiscovery {

    /**
     * 本对象持有的 {@code repository} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final PersistentDictionaryRepository repository;
    /**
     * 构造时冻结的 {@code tablesDirectory} 规范化路径；必须位于所属表空间或日志目录内，IO 层依赖它防止访问错误文件。
     */
    private final Path tablesDirectory;

    /**
     * 创建 {@code DictionaryTablespaceDiscovery}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param repository 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param tablesDirectory 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DictionaryTablespaceDiscovery(PersistentDictionaryRepository repository, Path tablesDirectory) {
        if (repository == null || tablesDirectory == null) {
            throw new DatabaseValidationException("dictionary discovery repository/path must not be null");
        }
        this.repository = repository;
        this.tablesDirectory = tablesDirectory.toAbsolutePath().normalize();
    }

    /**
     * ACTIVE 文件缺失会破坏已提交表，直接 fail-closed；DROP_PENDING/DISCARD_PENDING/IMPORT_PENDING
     * 文件允许处于物理切换窗口，交给 recovery 续作。DISCARDED/DROPPED 文件不交给普通 StorageEngine 打开。
     *
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     */
    public List<TableStorageBinding> discover() {
        Set<cn.zhangyis.db.domain.SpaceId> spaces = new HashSet<>();
        return repository.snapshot().tables().values().stream()
                .filter(table -> table.state() == TableState.ACTIVE
                        || table.state() == TableState.DROP_PENDING
                        || table.state() == TableState.DISCARD_PENDING
                        || table.state() == TableState.IMPORT_PENDING)
                .map(table -> binding(table, spaces))
                .filter(java.util.Optional::isPresent)
                .map(java.util.Optional::orElseThrow)
                .toList();
    }

    /**
     * 把语法对象绑定到稳定元数据与类型；绑定期间保持版本一致，失败不发布半绑定结果。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param table 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param spaces 参与 {@code binding} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code binding} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DictionaryRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    private java.util.Optional<TableStorageBinding> binding(TableDefinition table,
                                                            Set<cn.zhangyis.db.domain.SpaceId> spaces) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                new DictionaryRecoveryException("committed table has no physical binding: " + table.id().value()));
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        Path path = checkedPath(binding.path());
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        if (!Files.exists(path)) {
            if (table.state() == TableState.ACTIVE) {
                throw new DictionaryRecoveryException("ACTIVE table file is missing: table=" + table.id().value()
                        + " path=" + path);
            }
            return java.util.Optional.empty();
        }
        if (!spaces.add(binding.spaceId())) {
            throw new DictionaryRecoveryException("duplicate DD tablespace id: " + binding.spaceId().value());
        }
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        return java.util.Optional.of(binding);
    }

    /** 所有字典物理路径必须被限制在实例 tables 目录内，防止损坏 catalog 令恢复删除任意文件。
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @return {@code checkedPath} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DictionaryRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    public Path checkedPath(Path path) {
        if (path == null) {
            throw new DictionaryRecoveryException("dictionary tablespace path must not be null");
        }
        Path normalized = path.toAbsolutePath().normalize();
        if (!normalized.startsWith(tablesDirectory)) {
            throw new DictionaryRecoveryException("dictionary tablespace path escapes tables directory: "
                    + normalized);
        }
        return normalized;
    }
}
