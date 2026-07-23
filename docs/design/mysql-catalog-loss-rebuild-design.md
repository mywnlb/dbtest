# MiniMySQL Catalog Loss Recovery、Directory Manifest 与 Full-page Scrub 设计

版本：2026-07-20
关联设计：[mysql-sdi-v1-design.md](mysql-sdi-v1-design.md)、
[mysql-data-dictionary-ddl-design.md](mysql-data-dictionary-ddl-design.md)、
[innodb-crash-recovery-design.md](innodb-crash-recovery-design.md)、
[innodb-disk-manager-design.md](innodb-disk-manager-design.md)、
[current-implementation-map.md](current-implementation-map.md)

## 1. 目标

本设计闭环 backlog 2.9 的剩余能力：在 `mysql.ibd` missing、empty 或损坏时，以正常运行期间
独立持久化的 schema/directory manifest 为完整性证明，离线发现 file-per-table 候选、逐页校验、
显式隔离冲突并重建可由普通启动再次严格验证的 catalog。

该能力不是新的隐式启动分支。普通 `DatabaseEngine.open()` 保持 fail-closed；管理员必须显式执行
`inspect -> quarantine -> inspect -> rebuild`。任何无法证明的状态都保留证据并阻止重建。

## 2. 权威关系与边界

- catalog 有效时，committed `mysql.ibd` 始终是逻辑 DD 真相；manifest 是灾难恢复 witness。
- catalog 丢失时，只允许使用最后一个完整、无未决 catalog mutation 的 clean manifest。
- 单表 SDI 只证明 ACTIVE 表聚合，schema 名称/default charset/collation 和完整目录边界来自 manifest。
- storage scrubber 只解释文件、page envelope、checksum、FSP lifecycle 和 opaque SDI，不 import DD 类型。
- DD recovery 层负责 schema/table 解释、manifest 对比、冲突分类和 baseline catalog 生成。
- 不从任意 `.ibd` 猜测 schema，不执行普通 SQL。普通启动不改 catalog-loss 原始证据；只有管理员显式
  quarantine 可移动选中的冲突，或健康 catalog 场景可把损坏 manifest、受控 rebuild 临时文件原子保留到 evidence。

## 3. Recovery Manifest v1

固定路径为 `${baseDir}/mysql.dd.manifest`。物理文件使用独立魔数、双 4 KiB header、
CRC32C data frame、batch SHA-256 和 committed-length 发布边界。先 force data frames，再写另一
header generation 并 `force(true)`；崩溃尾部永远不可见。

Manifest 事件只有三类：

1. `CONTROL_RESERVATION`：在 control slot force 前持久化目标 next counters。重建时即使该预留最终
   未完成，也按 component-wise maximum 保守跳号，绝不复用可能已经暴露的 identity。
2. `CATALOG_MUTATION_INTENT`：在 catalog append 前保存目标 dictionary version 和 mutation SHA-256。
   只要它未被后续 clean snapshot 明确解析，catalog-loss rebuild 就必须失败。
3. `CLEAN_SNAPSHOT`：在 catalog、DDL recovery、SDI 和目录均收敛后保存完整稳定字典快照，并记录
   `resolvedThroughSequence`，明确此前 mutation intent 已由仍然有效的 catalog 裁决。

Clean snapshot 按 schemaId、tableId、index ordinal 固定排序，包含：

- published dictionary version 和安全 control 高水位；
- schema id/name/default charset/default collation/object version；
- 全部 ACTIVE、DISCARDED 表的完整列、索引、storage binding 和对象版本；
- ACTIVE 表规范相对路径、tableId、spaceId、SDI SHA-256；
- schema/table/index/path 数量与整批 SHA-256。

DROPPED tombstone 不进入灾难恢复快照；高水位继续防止旧 identity 复用。DROP_PENDING、
DISCARD_PENDING、IMPORT_PENDING 或未决 DDL 存在时不得发布 clean snapshot。

现有健康实例首次升级时，在正常 storage recovery、DDL recovery、SDI reconcile 和 orphan cleanup
完成后生成首个 clean snapshot。manifest 缺失不阻止读取健康 catalog；已有 manifest 损坏时先保留到
recovery evidence 目录，再从健康 catalog 重建。零长度 manifest 也属于已有损坏证据，`openOrCreate`
必须按 existing 严格失败，禁止原地初始化覆盖。反之，catalog 已丢且从未生成 manifest 时继续拒绝。

## 4. Mutation Witness

`DictionaryDurabilityWitness` 是 repo/control 只依赖的窄接口：

- control store 在写新 generation 前调用 `beforeControlReservation(target)`；
- dictionary repository 在 append mutation 前调用 `beforeCatalogMutation(version, digest)`；
- DDL/启动协调器只在全局稳定时调用 `publishCleanSnapshot(...)`。

生产组合根注入 manifest 实现；既有构造器保留并使用 no-op witness，避免低层格式测试被迫创建 sidecar。
clean publisher 固定取得 repository writer fence，再读取稳定 snapshot 并追加 manifest event；若观察到
DROP_PENDING、DISCARD_PENDING、IMPORT_PENDING 等临时状态则跳过 clean，不让旧 clean 越过并发 mutation。
锁序固定为 repository writer -> control snapshot -> manifest append，不反向取得 MDL、page latch 或用户文件锁。
若 intent 写入失败，catalog/control 修改尚未开始。catalog 已提交但 clean publish 失败时，结果按
“提交结果不确定”处理并阻止后续 DDL；重启可从仍有效的 catalog 裁决并修复 manifest。

## 5. Catalog Baseline Batch

重建后的 catalog 不伪造历史 mutation。`DictionaryCatalogCodec` 新增全量 baseline：

- `CATALOG_BASELINE_META` stable code 8；
- `CATALOG_BASELINE_COMMIT` stable code 126；
- 既有 1..7 与 127 均保持不变。

Baseline 必须是 catalog 中第一个 DD batch，最多出现一次。meta 保存 format、published version、
对象数量和摘要；对象 payload 保留各自真实 object version。解码时重建 schema/table/index maps，
校验父子关系、唯一 identity/name、ACTIVE/DISCARDED 稳定状态和 binding 一致性。之后仍允许追加
现有 mutation/DDL log batch；没有 baseline 的旧 catalog 完全兼容。

## 6. Offline Candidate Discovery 与 Full-page Scrub

扫描范围与 catalog-loss guard/orphan cleanup 一致：`tables/table_*_space_*.ibd`。候选必须是
NOFOLLOW regular file，属性探测与 `FileChannel.open` 都禁止跟随链接；文件名解析出的
tableId/spaceId、规范路径和 page0 identity 必须一致。

每个文件使用单个复用 page buffer 顺序扫描：

1. 校验 page-size 对齐、page0 FSP_HDR envelope、space flags、space version 和 GENERAL lifecycle。
2. 在消费 allocation bitmap 前，按 `ExtentManagementRegionLayout` 读取 freeLimit 已材料化范围的
   page0/primary/overflow XDES，校验独立页 header、state ordinal、owner sentinel、跨页 FLST
   prev/next 反向地址，以及 FREE/FULL_FRAG 与 bitmap 的一致性；freeLimit 外物理零填充不伪造 descriptor。
3. 对每个已初始化页验证 header/trailer checksum、trailer low32 LSN、spaceId、pageNo 和 page type。
4. 完全零页只有在 FSP/XDES 证明未分配时才接受；唯一兼容例外是旧版教学文件中 extent0 已保留但
   尚未初始化的 page1 change-buffer bitmap 占位页。当前创建路径写入 page1 IBUF_BITMAP 与 page2 INODE
   信封；非零 legacy-zero checksum 在 rebuild 中阻塞。
5. 校验 page3 SDI root、identity/version、payload CRC，并把 opaque payload 返回 DD 层。
6. 索引 BUILD/DROP footer 必须全零；footer 存在表示丢失的 DDL log 仍可能拥有物理资源。
7. 扫描结束再次要求目录项仍为 NOFOLLOW regular file，并比较 size、mtime、fileKey；随后以全文件
   SHA-256 形成 candidate fingerprint。

扫描使用正 timeout，并在逐页边界响应线程中断。它不挂载 PageStore、不写页、不尝试 doublewrite/redo
修复；checksum-invalid 的唯一预期 ACTIVE 文件必须从备份恢复，不能通过 quarantine 绕过。

## 7. Inspection、冲突与 Complete-scan Token

`CatalogRecoveryInspection` 返回 `NO_RECOVERY_NEEDED`、`BLOCKED` 或 `REBUILDABLE`，以及 manifest
摘要、候选结果、冲突集合和 token。Token 包含 manifest committed sequence/digest、排序后的路径与
文件 fingerprint、完整 conflict digest；只有目录完整枚举并完成所有页扫描后才能签发。

阻塞冲突包括 manifest 缺失/损坏/dirty、catalog/control 状态、expected missing、extra candidate、
重复 tableId/spaceId/path/name、page/SDI/lifecycle/footer 损坏和扫描期间文件变化。

只有 extra/duplicate 副本以及 empty/corrupt catalog/control 可标为 quarantinable。Manifest 唯一预期
路径永远不可隔离。`quarantine(token, ids)` 在实例文件锁下重新验证 token 和源摘要，对每个显式
选择的冲突执行同盘 `ATOMIC_MOVE` 到 `catalog-recovery/quarantine/<scanId>/`，禁止覆盖且不提供
copy/delete fallback。部分移动后 crash 是安全的；下一次 inspect 重新建立完整目录事实。

## 8. Atomic Rebuild

普通引擎与 recovery service 共用 `${baseDir}/mysql.instance.lock`。普通引擎从 open 前到 close 后持有
独占 `FileLock`；inspect/quarantine/rebuild 每次有界获取同一锁，避免两个合规进程并发修改实例。

`rebuild(token)` 固定执行：

1. 要求 catalog 目标不存在；empty/corrupt 原文件必须先显式 quarantine，绝不原地覆盖。
2. 在锁内重跑完整 inspect，token 不一致或任何 blocking conflict 都停止。
3. 计算 manifest、全部 control reservation 与有效 control 的 component-wise maximum。
4. control 缺失时以 `CREATE_NEW` 直接创建并 force 目标双槽；有效但偏低时只单调推进；损坏文件必须先隔离。
5. 先 durable 发布 control。若此后 crash，catalog 仍缺失，普通启动继续 fail-closed。
6. 在 `mysql.ibd.rebuild.<cleanDigestPrefix>` 写单个 baseline，关闭后重新打开 repository 校验完整快照；
   临时 identity 只绑定 clean manifest，不绑定会被本次 rebuild 推进的 control 原始指纹。重试时可复用
   逐值验证相等的 durable temp，不一致或损坏的受控 temp 先原子保留到 evidence 再重建。
7. 再次复核候选 fingerprint 与目标不存在，以同盘 `ATOMIC_MOVE` 发布 `mysql.ibd`；不支持则失败。
8. 返回对象数、版本、摘要和发布路径；普通启动仍须完成 storage/DDL recovery 和 SDI reconcile。

## 9. API

公共离线门面为 `cn.zhangyis.db.engine.recovery.CatalogRecoveryService`：

- `inspect(Duration timeout)`
- `quarantine(CatalogRecoveryToken token, Set<CatalogRecoveryConflictId> conflicts, Duration timeout)`
- `rebuild(CatalogRecoveryToken token, Duration timeout)`

公开结果均为不可变 record/enum，不暴露 FileChannel、catalog frame、BufferFrame 或可变 payload。
异常按 validation、busy/timeout、manifest corruption、tablespace corruption、stale token、
quarantine persistence 和 rebuild persistence 分类，全部保留 cause。

## 10. 并发、IO 与失败不变量

- 禁止 Java monitor；manifest/repository/control 内部只用 `ReentrantLock`。repository writer fence 可覆盖
  manifest 自有 journal append/force，但不跨页 IO、MDL、用户表空间 IO 或事务锁等待。
- instance file lock 获取有 timeout/中断，不进入事务 wait-for graph。
- scrub、catalog/control/manifest 与 instance-lock channel 均使用 `NOFOLLOW_LINKS`；scrub channel
  只读且 try-with-resources；quarantine/rebuild 不持 DD/MDL/page latch。
- control 必须先于 catalog 发布；catalog temp 必须完整验证后才 atomic move。
- 任一失败不得清除 manifest intent、预期表文件或原始 catalog/control 证据。
- 同盘 atomic move 是 v1 强边界；Windows/Java 无可移植目录 fsync，本设计不宣称 directory entry
  power-loss durability 等价于 MySQL/InnoDB，失败后以目录重扫裁决。

## 11. 测试与验收

- manifest 双槽、CRC/SHA、截断、未知版本、顺序、mutation/control fence 与 clean resolution。
- baseline 空库/多 schema/ACTIVE/DISCARDED、多索引/LOB、损坏、重复、旧 catalog 兼容。
- 全页 checksum/trailer/identity、zero/XDES state/owner/list/bitmap、SDI/footer、symlink/非常规路径、
  channel 打开后的扫描中变化和 timeout。
- 缺失/extra/duplicate/conflicting candidates、不可隔离 expected、stale token 与 atomic move 失败。
- control、baseline frame/force、temp reopen、final move 各 crash point 的故障注入；特别覆盖 control
  已创建/推进导致 token 改变后，下一次 inspection 仍按 clean digest 复用已验证 temp。
- 删除、截空、损坏 catalog 后显式恢复，再由普通引擎打开并执行 DD/SQL 查询。
- 健康旧实例首次启动生成 manifest；普通 open 在 catalog loss 时仍然 fail-closed。
- 固定 JDK/Gradle 全量测试、测试数不倒退、`git diff --check`、禁止项与依赖方向扫描。

## 12. 五遍完成检查

实现完成后必须回到源码而非计划进行五遍独立复核：

1. 生产调用链与 backlog 五项范围。
2. manifest/catalog/control 持久格式、旧格式兼容和 crash window。
3. instance lock、timeout、资源释放、token TOCTOU 与 atomic move。
4. 冲突矩阵、逐页 scrub、恢复故障注入和普通启动验收。
5. current map 十项清单、backlog、静态规则、测试数量与全量回归。
