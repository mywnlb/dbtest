/**
 * fil 物理文件层根包。
 *
 * <p>当前生产类按职责下沉到 {@code io}、{@code lock}、{@code access}、{@code meta}、
 * {@code state} 和 {@code exception} 子包。fil 层保持纯物理视角：负责表空间文件、
 * 页读写、文件扩展、运行期元数据与物理准入，不解析 record/segment/事务语义。</p>
 */
package cn.zhangyis.db.storage.fil;
