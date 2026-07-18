package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;

import java.nio.file.Path;

/**
 * 表空间 metadata 中一个数据文件的不可变逻辑页范围描述。
 *
 * <p>该值不持有 {@code FileChannel}，不检查路径是否存在/规范化，也不读取 FSP_HDR。当前生产
 * {@link FileChannelPageStore} 是单文件且 registry-free，物理越界依据 {@code DataFileHandle}
 * 自己的实时文件页数，不通过本描述符路由；start/range 结构保留给 metadata 表达和后续多文件模型。</p>
 *
 * @param path metadata 记录的数据文件路径；非空，但可为相对路径且构造时不访问文件系统
 * @param startPageNo 该文件在表空间逻辑页号空间中的非空起始页；当前单文件生产值为 0
 * @param sizeInPages 本 metadata 快照声明的正数覆盖页数；可能在物理 autoextend 后暂时陈旧
 */
public record DataFileDescriptor(Path path, PageNo startPageNo, PageNo sizeInPages) {

    /**
     * 校验描述符具有完整路径/range，并拒绝空覆盖区间。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>确认路径、起始页和值域大小对象均存在；不执行路径或跨文件范围校验。</li>
     *     <li>要求覆盖页数严格为正，保证 {@link #endExclusivePageNo()} 位于起始页之后。</li>
     * </ol>
     *
     * @throws DatabaseValidationException 任一组件为空或 sizeInPages 非正时抛出
     */
    public DataFileDescriptor {
        // 1. 三个组件共同定义逻辑范围；缺一时 metadata 无法表达文件覆盖。
        if (path == null) {
            throw new DatabaseValidationException("data file path must not be null");
        }
        if (startPageNo == null) {
            throw new DatabaseValidationException("data file start page must not be null");
        }
        if (sizeInPages == null) {
            throw new DatabaseValidationException("data file size must not be null");
        }

        // 2. 零长度描述不能参与 metadata 文件范围表达。
        if (sizeInPages.value() <= 0) {
            throw new DatabaseValidationException("data file size must be positive pages");
        }
    }

    /**
     * 创建单文件模型使用的数据文件描述。
     *
     * <p>该工厂只是命名构造入口，不强制 {@code startPageNo == 0}；生产创建/loader 负责传入零起点。</p>
     *
     * @param path 非空数据文件路径
     * @param startPageNo 非空逻辑起始页
     * @param sizeInPages 严格为正的覆盖页数
     * @return 通过紧凑构造器校验的新描述符
     * @throws DatabaseValidationException 参数不满足描述符结构约束时抛出
     */
    public static DataFileDescriptor single(Path path, PageNo startPageNo, PageNo sizeInPages) {
        return new DataFileDescriptor(path, startPageNo, sizeInPages);
    }

    /**
     * 计算 metadata 逻辑覆盖范围的 exclusive 结束页号。
     *
     * @return {@code startPageNo + sizeInPages} 对应的页号；不会返回 {@code null}
     * @throws ArithmeticException 起始页与大小之和超出 {@code long} 表示范围时抛出
     */
    public PageNo endExclusivePageNo() {
        return PageNo.of(Math.addExact(startPageNo.value(), sizeInPages.value()));
    }
}
