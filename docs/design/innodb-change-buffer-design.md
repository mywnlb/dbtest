# MiniMySQL Change Buffer 设计

## 1. 目标与依据

本设计在 Java 教学实现中还原 MySQL 8.0 / InnoDB Change Buffer 的核心语义：当非唯一二级索引叶页不在 Buffer Pool 中时，先把可延迟的物理变更写入系统表空间中的全局 Change Buffer B+Tree；目标页后续首次载入、后台批量合并或恢复访问时，再把这些变更合并进真实二级索引页。

设计依据是 MySQL 8.0 `innodb_change_buffering`、`innodb_change_buffer_max_size`、`ibuf0ibuf.cc`、`btr0cur.cc` 与当前工程的 MTR、WAL、Buffer Pool、B+Tree、undo/rollback/purge、DDL 和 crash recovery 边界。本项目不追求 MySQL 二进制兼容，但必须保持以下数据库语义：

- Change Buffer 是全局内部结构，位于系统表空间，而不是每张用户表各建一棵树。
- 只缓冲不在 Buffer Pool 的非唯一、升序二级索引叶页；聚簇、唯一、降序和系统索引永远直写。
- 页面被普通调用方看到之前必须完成合并，不能发布“磁盘旧页 + 尚未应用 Change Buffer”的中间视图。
- Change Buffer 记录、目标 bitmap 与真实目标页合并都受 redo/WAL 保护；崩溃后允许幂等重做。
- DDL 回收任何索引或表空间前必须 drain 或 discard 对应记录，不能留下悬空物理身份。

## 2. 范围与非目标

本阶段实现 INSERT、DELETE_MARK、DELETE 三类物理操作，并通过模式映射提供 `NONE`、`INSERTS`、`DELETES`、`CHANGES`、`PURGES`、`ALL`。`DELETES` 包含 delete-mark，`PURGES` 包含物理删除，`CHANGES` 包含 insert/delete-mark，`ALL` 包含全部。

非目标：MySQL 的精确磁盘字节布局、ibuf 自身专用压缩格式、所有 Performance Schema 表、按压缩页大小变化的算法，以及把唯一约束检查推迟到 merge。逻辑唯一索引必须直写，避免延迟唯一冲突。

## 3. 模块边界

新增 `cn.zhangyis.db.storage.changebuffer` 包：

- `ChangeBufferConfig` / `ChangeBufferMode`：资源边界和模式。
- `ChangeBufferHeaderRepository`：系统表空间 page 3 的持久 header。
- `ChangeBufferBitmapRepository`：用户表空间 IBUF bitmap 的 4-bit/page 状态。
- `ChangeBufferMutationCodec`：全局树 payload 与逻辑 mutation 的自校验转换。
- `ChangeBufferStore`：全局 B+Tree 的 append、prefix scan、consume。
- `SecondaryIndexMutationCoordinator`：DML、rollback、purge 的统一“buffer 或直写”决策。
- `ChangeBufferPageMerger` / `ChangeBufferPageMergeInterceptor`：已载入单页的幂等执行与发布前 merge 编排。
- `ChangeBufferMergeWorker`：有界后台调度。
- `ChangeBufferDdlBarrier`：DDL drain/discard 端口。

Buffer Pool 只负责在 LOADING 页发布前调用拦截器，不解析 record 或索引 metadata。B+Tree 只提供无目标页 IO 的叶页定位，以及对已持 X latch 叶页执行的局部 mutation；它不决定 Change Buffer 策略。事务层仍负责 undo 和可见性，Change Buffer 只延迟已经决定的二级物理变更。

## 4. 系统表空间与固定页

系统表空间固定使用 `SpaceId(0)`、文件 `system.ibd`、`TablespaceType.SYSTEM`。新实例初始化至少一个 extent：

| 页号 | 类型 | 用途 |
| --- | --- | --- |
| 0 | `FSP_HDR` | FSP header 与 XDES |
| 1 | `IBUF_BITMAP` | 系统表空间自己的 bitmap envelope |
| 2 | `INODE` | segment inode array |
| 3 | `IBUF_HEADER` | Change Buffer header |
| 4 | `IBUF_INDEX` | 全局 Change Buffer B+Tree 稳定 root |

page 0–4 是系统保留页，不得被普通 segment 分配。全局树另有 leaf/non-leaf segment，split 后的新页由 FSP 正常分配。`PageType` 编码只能在尾部追加，已有 code 不变。

用户表空间的 bitmap 按 `1 + k * pageSize.bytes()` 重复。FSP 在 freeLimit 跨入新管理区时，先以
page0 X gate 格式化区首 primary XDES、`+1 IBUF_BITMAP` 和可选 `+5` overflow XDES，再保留该区首
extent；因此普通已分配二级叶页一定有对应 bitmap。page0 兼容槽耗尽但仍处于 region0 时，page5
作为 overflow XDES 延迟格式化，并在 extent0 bitmap 中追加固定页位。

旧实例没有 `system.ibd` 时按 legacy `NONE` 打开，不在 existing-open 中静默创建文件。这样不会把升级失败伪装成初始化成功。新实例创建完整系统表空间后，才允许采用配置的模式。

## 5. Header 持久格式

page 3 的 body 从 `PageEnvelopeLayout.BODY_START` 开始，采用大端编码：

| 字段 | 语义 |
| --- | --- |
| magic/version/headerLength | 格式身份与前向拒绝边界 |
| state | `ACTIVE`、`DRAINING`、`DISABLED`、`CORRUPTED` |
| configuredMode | 创建实例时持久化的初始模式，仅作诊断；运行时收窄模式不改结构身份 |
| rootPageNo/rootLevel/indexId | 全局树稳定 root 与当前高度 |
| leaf/nonLeaf segment identity | space、inode slot、segment id |
| nextSequence | 全局单调 mutation 序号，0 保留 |
| pendingOperations | 尚未 consume 的记录数 |
| formatEpoch | 不兼容重建时递增 |
| headerCrc | header 自校验；页面 checksum 仍由 flush 管线负责 |

Header 的序号分配、树结构后置 level 和 pending 计数在同一 MTR 中更新。计数只作管理证据；启动校验发现与树扫描不一致时 fail-closed，不据计数删除数据。

## 6. 全局树 record

全局树 key 为 `(targetSpaceId, targetPageNo, sequence)`，全部升序且物理唯一。value 同一 record 内保存：

- `tableId`、`schemaVersion`、`indexId`：由 exact-version metadata resolver 恢复二级布局。
- `operation`：INSERT、DELETE_MARK、DELETE。
- `entryBytes`：按目标二级 `entrySchema` 编码的完整紧凑 entry；删除类也保存完整 entry，以便恢复物理 key。
- `payloadCrc`：在读出并触碰目标页前校验。

Change Buffer tree 使用内部非聚簇 schema，不带 `DB_TRX_ID/DB_ROLL_PTR`。记录不得引用 Java 对象地址、页内 offset 或 BufferFrame；只保存跨重启稳定身份。

## 7. Bitmap 格式与算法

每个可缓冲用户表空间使用 4 bit/page：低 2 bit 是连续可用空间等级，bit 2 表示存在 buffered mutation，bit 3 表示该页属于 Change Buffer 内部结构。等级按页大小的 `pageSize/32` 为单位：`floor(free/unit)`，0–2 原样保存，3 及以上保存 3；为保守兼容 MySQL，精确等于 3 个单位时可降为 2。

bitmap 页号公式为：

`bitmapPageNo = 1 + (targetPageNo & ~(pageSize.bytes() - 1))`

按上述公式，一个物理 bitmap 页管理 `pageSize.bytes()` 个目标页；4-bit entry 只使用 body 的前半区域，其余空间为后续格式扩展保留。目标设计要求 FSP 在扩容跨越覆盖区间前预留并格式化后续公式页。当前 v1 的普通 create/autoextend 只保证 page 1，因此 `isBootstrapBitmapCovered` 之外的 DML 明确回退真实 B+Tree 直写且不读写未预留页；IMPORT 仍枚举全部公式页并在类型不符时 fail-closed。

INSERT eligibility 使用等级下界减去同页现有 buffered INSERT 字节之和，仍不足则直写。DELETE_MARK/DELETE 不依赖可用空间，但仍需 bitmap pending bit。目标页每次直接物理变更或 merge 后，都在同一 MTR 中重算等级。

## 8. Eligibility 与决策顺序

只有全部条件成立才缓冲：

1. 系统表空间存在、header ACTIVE、运行模式允许该 operation。
2. descriptor 是非聚簇、`logicalUnique=false`、所有 key part 为 ASC。
3. 目标是 level-0 leaf，且不是系统表空间/undo/DD 内部页，并落在 v1 已预留的 page 1 bitmap 覆盖区间。
4. B+Tree 只读取 root/internal 页定位目标 PageId；目标页在 Buffer Pool 中既不 CLEAN/DIRTY，也不 LOADING。
5. per-target merge gate 获取成功，等待有明确 timeout；等待期间不持事务锁以外的 page latch/fix。
6. gate 内重新检查 residency、bitmap、容量上限和全局 tree 占用；append 前再在 page 3 X latch 下做最终容量判断，阻止不同 target 的并发快照共同越界；任一容量竞争都回退直写。

Change Buffer 满时不阻塞前台等待后台腾空，直接回退真实二级树。与 MySQL 8.0 一致，默认上限以
Buffer Pool frame 总容量的 25% 为基准，配置上限 50%；当前教学实现把每条 pending mutation 保守计作一个
完整页等价量，而不是核算全局树实际占页，因此只会比 MySQL 更早回退，不会越过配置上限。

## 9. 页面发布前合并

Buffer Pool miss 的生命周期扩展为：注册 LOADING 占位 → 磁盘读入私有 frame → 调用 `PageLoadInterceptor` → interceptor 可把该 LOADING 页以唯一 X guard 交给 MTR → commit 盖 page LSN/发布 dirty → Buffer Pool 最终转 CLEAN 或 DIRTY、加入 LRU、完成 future。拦截器返回时 guard 必须已由 MTR 关闭；认领/写入或最终发布后的失败不可回收 frame，owner 与同页等待者收到同一个 fatal 并触发实例 fail-stop。

拦截器运行时不持 page-hash、frame、LRU、flush-list 内部锁。LOADING 仍对其它线程不可见，等待者只等同一个 future。预取同样执行合并，完成后立即 unfix；普通需求读只有在合并提交后才拿到 guard。

Merge 流程：

1. 在 per-target gate 下用只读 MTR prefix-scan 并完整解码、校验全部候选。
2. 新建写 MTR，adopt LOADING target X guard；局部应用 mutation，不重新导航目标树。
3. 同一 MTR 从全局树删除已应用记录、递减 header 计数并更新目标 bitmap。
4. commit 让目标页修改与 Change Buffer consume 共享一个 redo batch；Buffer Pool 随后发布 DIRTY 页。

单个目标最多积累 64 条 mutation，merge 使用 128 个 page-image equivalent 的固定 redo admission；第 65 条在 append 前回退直写。局部 DELETE 不强制执行 leaf merge；允许暂时低填充，后续普通结构维护再回收。这是教学简化，不改变查找正确性。exact-version resolver 缺失、抛错或返回错配 binding 均转换为保留 cause 的持久格式 fatal；若 dry-run 后仍出现空间不足或后半程持久修改失败，同样视为不能安全继续的致命恢复错误，实例必须 fail-closed。

## 10. 并发、锁顺序与等待

共享状态 owner：

- Header 字段由 page 3 X latch + MTR 保护。
- 全局树结构由既有 B+Tree page latch/FSP segment 规则保护。
- bitmap byte 由 bitmap page X latch + MTR 保护。
- 同一目标页的 buffer/merge/drain 由 striped `ReentrantLock` gate 串行；等待使用 `tryLock(timeout)`。
- Worker 状态由显式 `ReentrantLock`、`Condition` 和原子状态保护，不使用 `synchronized`。

常规 buffer 顺序是系统 header/root/ibuf pages → 用户 bitmap。页面加载 merge 已先持用户 target，必须进入有文字死锁证明的 MTR out-of-order scope，再访问系统 ibuf 和用户 bitmap。该反序路径不会等待事务行锁，也没有任何“ibuf/bitmap → target leaf”的反向获取；前台 buffer 发现目标 LOADING 后直接回退并等待普通直写，因此不形成环。

## 11. DML、rollback 与 purge

`SecondaryIndexMutationCoordinator` 是唯一普通生产决策点：

- INSERT 发布 secondary entry 时可缓冲 INSERT。
- UPDATE/DELETE 的旧 entry 可缓冲 DELETE_MARK，新 entry 可缓冲 INSERT。
- rollback 的反向物理删除、revive/delete-mark 可缓冲对应操作；undo 进度只在 mutation 已经写入真实页或 Change Buffer redo 后推进。
- purge 经版本链安全检查后可缓冲 DELETE；Change Buffer record 成为该 purge 工作已持久接管的证据。

事务锁、undo append 与聚簇 anchor 顺序不因 Change Buffer 改变。逻辑唯一二级索引仍执行完整 leaf scan；
已由聚簇唯一检查和锁证明主键全新的 INSERT，在取得 non-unique logical-prefix X 锁后跳过目标 leaf 的
physical-state read，避免仅为确认必然不存在而把冷页载入。UPDATE/revive 仍读取真实 secondary 状态，覆盖
A→B→A 与 delete-marked identity。Change Buffer 不是逻辑 undo，事务回滚通过追加反向 mutation 达到最终物理效果。

## 12. DDL 屏障

DDL 在持有表/索引元数据排他准入、且阻断新 DML 后执行：

- DROP/TRUNCATE/DISCARD TABLESPACE：drain 该空间，或在确定整文件不可再访问时原子 discard 全部记录并清 bitmap。
- DROP INDEX：按 `(tableId,indexId)` discard；只有 Change Buffer 记录清零后才能回收 root/segment。
- REBUILD/ONLINE ALTER：source 索引先 drain，shadow/rebuild 路径始终直写；切换 binding 前再次 barrier。
- IMPORT：先在用户空间 X lease 外 discard 旧 incarnation 的全局记录；挂载候选并保持 page0 DISCARDED，
  按公式清零物理文件范围内所有类型正确的 bitmap 页，最后才写 NORMAL、满足 WAL、force 并发布 registry。
  若公式页尚未材料化、被 legacy 内容占用或 envelope/type 不匹配，导入 fail-closed，不覆盖未知页面。

Barrier 等待有 timeout；失败时 DDL 保持原状态或进入既有可恢复 marker，不能先删物理资源。

## 13. 后台合并与容量

后台 worker 每轮按全局 key 顺序选最多 `mergeBatchPages` 个不同目标页，通过 Buffer Pool demand load 触发同一 prepublish merge。默认预算约为 page cleaner IO capacity 的 5%，队列满或目标正被加载时跳过，不并行建立第二套合并算法。

关闭顺序：停止 read-ahead → 停 Change Buffer worker → 停 purge → 停 page cleaner/redo flusher → final flush。worker 超时则不关闭其依赖的 pool/store。

## 14. Crash recovery

恢复顺序扩展为：doublewrite repair → redo replay → redo boundary install → `CHANGE_BUFFER_RECOVER` → undo tablespace resume/reconcile → uncommitted transaction rollback → purge resume → DDL recovery completion → open traffic。

`CHANGE_BUFFER_RECOVER` 使用 `pending+1` 哨兵校验 system.ibd header、固定 root/index/segment identity、全局树可遍历性、pending 精确计数、sequence 全局唯一/nextSequence 边界，并拒绝任一 target 超过 64 条的不可合并持久状态；释放 system 页资源后，再逐 target 交叉验证 bitmap buffered/internal 位。它不启动全量 eager merge；随后 rollback/purge 访问目标页时由 prepublish interceptor 合并。恢复阶段生成的新反向 secondary mutation可以再次进入 Change Buffer，且 redo 从 recovered boundary 连续追加。

READ_ONLY_VALIDATE 只读取/报告 header 和树边界，不 attach 写拦截器、不合并。FORCE 模式不得跳过 SpaceId 0；系统表空间损坏意味着 Change Buffer 证据可能丢失，必须 fail-closed。

## 15. 可观测性

提供不可变 snapshot：configured/effective mode、pending records、buffered/merged/discarded/fallback counters、merge failures、system tree pages、本进程已观察 bitmap pages、worker state。日志只记录系统表空间初始化、模式降级、recovery 校验、DDL drain、worker 致命失败，不在每条 DML 打日志。

## 16. 测试矩阵

- 值对象/codec：mode 集合、header CRC/version、record round-trip、bitmap 页号/bit packing/free class。
- Buffer Pool：LOADING 唯一 owner、interceptor 在内部锁外、合并前不可见、未关闭 guard 拒绝、fatal 等待者传播、预取合并与超时。
- B+Tree：只定位未驻留 leaf、局部 insert/mark/delete 幂等、降序/unique/root-leaf 回退。
- 协作：buffer record + bitmap + header 同 MTR，merge target + consume 同 MTR，WAL flush gate。
- 事务：insert/update/delete rollback、purge、savepoint、并发同页 mutation 与 merge。
- DDL：drop index/table、truncate/discard/rebuild barrier，timeout 不回收资源。
- 恢复：redo 后 header/tree/count/sequence/单 target 64 条上限/bitmap 交叉校验、merge 崩溃点幂等、legacy 无 system.ibd 自动 NONE、system.ibd 损坏 fail-closed。
- 回归：全量 Gradle 测试，测试 suite/test 数不得少于实现前基线。

## 17. 与 MySQL/InnoDB 的差异与扩展

本实现复用通用 Record/B+Tree 编码，record 比 MySQL ibuf entry 更大；容量把每条 pending 按一个页等价量计费；
局部 merge 后不立即做 leaf merge；bitmap 空闲等级按当前页内连续空间而不是 MySQL 全部压缩页细节；系统表空间
固定单文件。buffered rollback/purge mutation 只编码希望达到的最终 INSERT/live、DELETE_MARK/marked、DELETE/absent
状态，不额外编码直写路径的 expected-before-state；merge 因而按最终状态幂等收敛，不能替代更严格的离线一致性
检查。后续可替换专用紧凑 codec、增加 prior-state 诊断、并行 merge partition、跨 bitmap 区间 FSP 保留、完整监控表，
以及更接近 MySQL master-thread 的自适应 IO 调度，但不得改变本设计的发布前合并、WAL、DDL barrier 和恢复顺序。
