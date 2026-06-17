package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.util.List;

/**
 * 表空间元数据字段的共享校验。{@link TablespaceMetadata}(权威加载快照) 与 {@link Tablespace}(运行时快照)
 * 字段结构相同、校验不变量也相同；集中在此避免两份 record 的紧凑构造器各自维护一份易腐化的重复校验。
 *
 * <p>该类只做“字段级合法性”校验（非空、非负、文件非空），不涉及状态机流转、文件 IO 或跨源对账，
 * 保证 record 构造完成即处于结构有效状态。
 */
final class TablespaceFieldValidation {

    private TablespaceFieldValidation() {
    }

    /**
     * 校验表空间元数据字段并返回不可变数据文件列表。任一字段非法立即抛 {@link DatabaseValidationException}，
     * 避免 record 携带 null/负值/空文件列表等破坏后续 PageStore 定位与边界检查的非法状态。
     *
     * @param spaceId 表空间编号。
     * @param name 表空间逻辑名称，非空白。
     * @param type 表空间类型。
     * @param pageSize 表空间页大小。
     * @param state 生命周期状态。
     * @param dataFiles 数据文件范围，至少一个。
     * @param spaceFlags 原始标志位。
     * @param currentSizeInPages 当前文件大小页数。
     * @param freeLimitPageNo FSP 可分配页上界。
     * @param spaceVersion 元数据版本，非负。
     * @return List.copyOf 后的不可变 dataFiles，供调用方回填 record 组件。
     */
    static List<DataFileDescriptor> validate(SpaceId spaceId, String name, TablespaceType type, PageSize pageSize,
            TablespaceState state, List<DataFileDescriptor> dataFiles, SpaceFlags spaceFlags,
            PageNo currentSizeInPages, PageNo freeLimitPageNo, long spaceVersion) {
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
        if (currentSizeInPages == null) {
            throw new DatabaseValidationException("tablespace current size must not be null");
        }
        if (freeLimitPageNo == null) {
            throw new DatabaseValidationException("tablespace free limit must not be null");
        }
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
        if (spaceVersion < 0) {
            throw new DatabaseValidationException("tablespace version must be non-negative");
        }
        return copy;
    }
}
