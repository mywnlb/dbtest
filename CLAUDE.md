# AGENTS.md

## 项目目标

本项目是一个 Java 教学型 MiniMySQL / InnoDB 风格数据库实现工程。实现应以 `C:\coding\java\self\dbtest\dbtest\docs\design` 下的设计文档为主要依据，把文档中的数据库内核设计落地为清晰、可测试、可演进的 Java 代码。

实现目标不是与 MySQL 二进制兼容，而是在 Java 中尽量还原 MySQL 8.0 / InnoDB 的核心思想：SQL/session、parser/binder、optimizer、executor、data dictionary、B+Tree、record、transaction/MVCC/lock、buffer pool、disk manager、redo/undo、flush/checkpoint、crash recovery 等模块的职责边界、协作流程和恢复语义。

## 参考文档优先级

开发任何模块前，必须先阅读并遵守相关设计文档：

- 总览：`C:\coding\java\self\dbtest\dbtest\docs\design\innodb-storage-engine-overview.md`
- 存储层：`innodb-disk-manager-design.md`、`innodb-buffer-pool-design.md`、`innodb-record-design.md`、`innodb-btree-design.md`
- 日志与恢复：`innodb-redo-log-design.md`、`innodb-undo-log-purge-design.md`、`innodb-flush-checkpoint-doublewrite-design.md`、`innodb-crash-recovery-design.md`
- 事务与锁：`innodb-transaction-mvcc-design.md`、`mysql-lock-observability-deadlock-design.md`
- SQL 层：`mysql-parser-binder-design.md`、`mysql-query-optimizer-design.md`、`mysql-sql-executor-storage-api-design.md`、`mysql-advanced-executor-operators-design.md`
- 元数据与会话：`mysql-data-dictionary-ddl-design.md`、`mysql-session-connection-protocol-design.md`、`mysql-prepared-statement-plan-cache-design.md`、`mysql-statistics-analyze-design.md`

如果实现需要简化设计文档中的内容，必须在代码注释、测试名称或补充文档中明确简化点、与 MySQL/InnoDB 的差异和后续扩展方向。

## 技术栈与命令

- 构建工具：Gradle。
- 语言：Java，当前 `build.gradle` 使用 Java 25 toolchain。
- 固定 JDK：`C:\Program Files\Java\jdk-25.0.2`。
- 固定 Gradle：`D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1`。
- 测试框架：JUnit Jupiter。
- 常用依赖：Lombok、Guava、Apache Commons、Hutool、SLF4J、Logback、SnakeYAML。
- 编码：UTF-8。
- 后续测试优先直接使用固定 Gradle 和固定 JDK：PowerShell 下执行 `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test`。
- 如固定 Gradle 不可用，再使用项目 wrapper：`./gradlew.bat test`。

新增代码必须能通过 Gradle 编译。涉及核心逻辑的修改必须补充自动化测试，不能只依赖 `System.out.println` 或手工运行。

## 包结构约定

生产代码默认放在 `src/main/java/cn/zhangyis` 下。实现数据库内核时，优先按以下包拆分，保持职责单一、依赖方向清晰：

| 包 | 职责 | 禁止事项 |
| --- | --- | --- |
| `cn.zhangyis.db.common` | 通用异常、校验、时钟、配置、工具接口 | 放入数据库业务规则 |
| `cn.zhangyis.db.common.exception` | 项目统一异常基类、致命异常、领域运行时异常 | 散落定义模块无关异常或吞掉根因 |
| `cn.zhangyis.db.domain` | 通用值对象，如 `PageId`、`SpaceId`、`Lsn`、`TransactionId` | 依赖具体存储实现 |
| `cn.zhangyis.db.storage.api` | 存储引擎对上层的 Facade 和稳定 API | 暴露内部 frame、裸文件、redo buffer |
| `cn.zhangyis.db.storage.fil` | tablespace 文件、页读写、文件扩展、物理 IO 锁 | 理解 record、segment、事务语义 |
| `cn.zhangyis.db.storage.fsp` | space、extent、segment、page 分配释放 | 直接操作 `FileChannel` 或解析 record |
| `cn.zhangyis.db.storage.buf` | buffer pool、frame、LRU、dirty page、page latch | 解析 record header 或 SQL 语义 |
| `cn.zhangyis.db.storage.record` | 页内 record 格式、字段编码、Page Directory、隐藏列 | 分配 page、访问裸文件、持有长期事务锁 |
| `cn.zhangyis.db.storage.btree` | B+Tree 查找、插入、删除、split、merge、range scan | 修改 redo 文件、XDES、事务活跃表 |
| `cn.zhangyis.db.storage.mtr` | mini transaction、短物理临界区、latch/fix memo、redo 收集 | 处理数据库事务提交或 MVCC 可见性 |
| `cn.zhangyis.db.storage.redo` | WAL、LSN、redo buffer、writer/flusher、redo replay | 调用事务锁或依赖具体 repository 实现 |
| `cn.zhangyis.db.storage.undo` | undo record、undo page、rollback、purge 输入 | 绕过事务模块直接决定可见性 |
| `cn.zhangyis.db.storage.trx` | transaction、ReadView、MVCC、LockManager、deadlock detection | 直接操作 `BufferFrame`、裸文件、XDES bitmap |
| `cn.zhangyis.db.storage.flush` | dirty page flush、doublewrite、checkpoint 协作 | 刷出 redo 尚未 durable 的 page |
| `cn.zhangyis.db.storage.recovery` | crash recovery 编排、redo replay、undo rollback、purge resume | 执行普通 SQL 语义 |
| `cn.zhangyis.db.dd` | data dictionary、schema/table/index metadata、DDL 状态 | 直接管理物理页和 BufferFrame |
| `cn.zhangyis.db.sql.parser` | SQL 词法、语法、AST | 访问存储引擎内部实现 |
| `cn.zhangyis.db.sql.binder` | 名称绑定、类型推导、参数元数据 | 执行物理计划 |
| `cn.zhangyis.db.sql.optimizer` | 逻辑改写、访问路径、代价估算、计划选择 | 直接修改存储页 |
| `cn.zhangyis.db.sql.executor` | 执行计划、算子、结果集 | 绕过 storage API 访问内部页 |
| `cn.zhangyis.db.session` | connection/session、命令分发、autocommit、prepared statement | 直接操作 record、page、redo |

测试代码放在 `src/test/java`，包名应与被测生产代码对应。

## 依赖方向

依赖必须从上层指向下层接口，避免反向依赖和循环依赖：

`session -> sql -> dd -> storage.api -> btree/record/trx/buf/fsp/fil/redo/undo/flush/recovery`

核心约束：

- 上层只能通过稳定接口调用下层，不读取下层内部状态。
- 底层不能 import 上层 SQL/session/executor 包。
- Buffer Pool 不解析 record、segment、SQL 语义。
- Record 不分配 page、extent、segment，不直接访问 `BufferFrame` 或裸文件。
- Transaction 不直接操作 `BufferFrame`、`FileChannel` 或 XDES bitmap。
- Redo record 定义不能依赖具体 repository 实现。
- Recovery 只恢复物理页和事务状态，不执行普通 SQL 业务语义。

## 面向对象与设计模式

优先用领域对象和值对象表达数据库概念，不要用散落的基本类型传递关键含义。典型值对象包括 `PageId`、`SpaceId`、`PageNo`、`ExtentId`、`SegmentId`、`Lsn`、`TransactionId`、`RollPointer`、`RecordRef`、`IndexRecordRef`。

根据职责合理使用设计模式：

- Facade：`storage.api`、SQL executor 到 storage 的入口。
- Repository：page、tablespace、metadata、statistics 的读取和持久化抽象。
- Strategy：page replacement、flush policy、access path、join order、record encoding。
- Template Method：record 编码、redo replay、executor operator 生命周期。
- Command：redo record、DDL action、recovery step。
- Observer：dirty page、checkpoint、statistics invalidation、plan cache invalidation。
- State：transaction、lock、buffer frame、redo writer、session command lifecycle。
- Factory：page、record、plan、operator、metadata object 创建。
- Adapter：文件 IO、外部配置、协议包和内部模型转换。
- Snapshot：ReadView、metadata snapshot、statistics snapshot。
- Chain of Responsibility：crash recovery、SQL rewrite、executor pipeline。
- Unit of Work：Transaction、MiniTransaction。
- Guard / RAII 风格对象：page latch、buffer fix、lock lease、MTR memo，优先使用 `try-with-resources` 管理释放。

不要为了展示设计模式而过度抽象。只有当模式能降低耦合、隔离变化或表达生命周期时才使用。

## 并发与恢复约束

涉及并发、事务、IO、恢复的代码必须显式表达资源边界和释放顺序：

- 区分 latch、lock、mutex、condition、wait queue，不混用概念。
- 并发设计必须科学、可解释、可测试：每个共享状态都要说明 owner、保护它的锁或原子变量、锁粒度、锁顺序、等待条件、释放路径和失败恢复路径。
- 禁止使用 Java `synchronized` 关键字和隐式 monitor 锁，包括 `synchronized` 方法、`synchronized` 代码块、`wait()`、`notify()`、`notifyAll()`。
- 并发控制优先使用 `java.util.concurrent` 中的显式工具，例如 `ReentrantLock`、`ReentrantReadWriteLock`、`StampedLock`、`Condition`、`Semaphore`、`CountDownLatch`、`CompletableFuture`、`Atomic*`、`LongAdder`。
- 使用显式锁时必须使用 `try/finally` 或 `try-with-resources` 保证释放，不能把 unlock/release 隐藏在条件分支中。
- 不允许用一个全局大锁串行化数据库内核；锁粒度必须贴合资源，例如 tablespace、page、frame、record、transaction、redo segment、flush queue。
- 任何可能阻塞的等待都必须支持 timeout、取消或明确的唤醒条件，不能无界等待。
- 进入可能阻塞的事务锁等待前，释放 page latch、record cursor、buffer fix、文件锁、空间管理 latch、undo page latch。
- 行锁等待成功后必须重新通过 B+Tree 定位并校验记录。
- Data page flush 前必须满足 WAL：对应 page LSN 的 redo 已 durable。
- Crash recovery 顺序优先遵循：doublewrite 修复、redo replay、未提交事务 undo rollback、purge resume。
- Checkpoint 只能推进到 redo closed、redo flushed、dirty page oldest LSN 的安全交集。

任何锁顺序、状态机、恢复阶段都应通过枚举、状态对象或清晰方法名表达，避免隐藏在注释或布尔变量中。

## 测试要求

所有核心模块必须充分测试。新增功能至少覆盖：

- 单元测试：值对象、编码解码、状态转换、策略选择、边界条件。
- 协作测试：模块通过接口协作，如 B+Tree 与 Buffer Pool、Record 与 Transaction、Redo 与 Flush。
- 异常测试：非法页号、损坏页、重复释放、锁超时、redo 缺失、配置错误。
- 并发测试：锁等待、释放顺序、死锁检测、page latch 与事务锁边界。
- 恢复测试：redo replay 幂等、undo rollback、checkpoint、doublewrite、crash 中断点。
- 回归测试：每个修复过的 bug 都要有对应测试。

测试必须包含断言，不要只打印输出。优先测试公开接口和领域行为，避免依赖私有实现细节。需要文件 IO 的测试应使用临时目录，测试结束后清理资源。

## 代码风格

- 中文注释必须完善、准确、工程化，优先解释数据库语义、并发边界、恢复约束、简化点和与 MySQL/InnoDB 的差异。
- 禁止空泛注释和机械注释，例如“设置字段”“调用方法”。注释应说明为什么这样做、保护什么不变量、违反后会产生什么问题。
- 涉及锁顺序、WAL、checkpoint、MVCC 可见性、undo/redo 关系、crash recovery 阶段的代码必须有中文注释说明关键约束。
- 类注释和方法注释都必须完善：核心领域类、接口、枚举、策略、仓储、Facade、Guard 对象必须有中文 Javadoc；公开方法、核心包内方法和测试方法必须说明职责、输入输出语义、边界条件和异常含义。
- 字段注释必须完善：核心领域对象字段、record 组件、枚举常量、状态字段、锁对象、队列、缓存、计数器、配置项都必须有中文 Javadoc 或字段说明，解释该字段表达的数据库语义、生命周期归属、是否为权威状态、由谁保护或何时更新。
- 字段注释不能只复述字段名，例如“表空间 ID”“页号”。应说明该字段为什么存在、在哪个范围内有效、后续哪个模块依赖它，以及非法值会破坏什么不变量。
- 方法不能只有类级说明而缺少方法级说明。简单 getter、record 自动访问器、无业务语义的私有小工具方法可以不单独注释；但只要方法参与页定位、空间分配、redo/undo、MTR、锁等待、flush、checkpoint、recovery、SQL 执行或状态转换，就必须有中文方法注释。
- 如果方法逻辑较复杂，注释必须增加数据流说明，至少写清：数据从哪个对象进入、经过哪些校验或状态判断、修改哪些领域状态、调用哪些下游接口、产生哪些 redo/dirty/lock/recovery 副作用、异常时如何释放资源或保持不变量。
- 复杂方法的数据流注释应优先写在方法 Javadoc 或关键代码块前，避免散落在每一行；注释必须帮助读者理解数据库语义，而不是复述代码语句。
- 公开 API、核心领域对象、状态枚举、复杂测试用例必须有中文 Javadoc 或中文测试说明。
- 类名、方法名、包名使用英文，保持数据库术语一致。
- 保持类职责单一。发现类开始承担多个模块职责时，应拆分接口或协作者。
- 优先不可变值对象。可变状态必须有明确 owner 和生命周期。
- 构造函数和工厂方法要保证对象创建后处于有效状态。
- 异常要表达领域含义，例如 page not found、redo not durable、lock timeout、metadata version mismatch。
- 项目异常统一放在 `cn.zhangyis.db.common.exception` 或模块自己的 `exception` 子包中管理；跨模块通用异常优先放在 `common.exception`。
- 禁止在生产代码中直接抛出 `IllegalArgumentException` 或裸 `RuntimeException`。参数、配置、状态、IO、恢复等错误必须使用有领域语义的项目异常，例如 `DatabaseRuntimeException`、`DatabaseFatalException` 或更具体的子类。
- `DatabaseRuntimeException` 表示调用方可通过重试、回滚、关闭资源或报告错误继续运行的数据库运行时异常；`DatabaseFatalException` 表示破坏系统继续运行安全性的致命异常，例如不可恢复的数据损坏、核心配置非法、恢复流程不可继续。
- 项目异常必须保留 message 和 cause 构造能力，不能丢失根因；包装底层异常时必须把原始异常作为 cause 传入。
- 日志用于关键生命周期、恢复、异常诊断，不替代测试断言。
- 日志配置使用 `src/main/resources/logback.xml`。控制台日志要求 `ERROR` 红色、`WARN` 黄色、`INFO` 保持默认颜色，便于调试时快速区分严重程度。
- 生产代码按业务需要打日志，只记录关键生命周期、状态转换、恢复阶段、IO/锁等待异常、不可恢复错误和有诊断价值的上下文；不要在普通 getter/setter、频繁循环或 happy path 中刷屏。
- 使用日志时优先通过 Lombok 标记类，例如 `@Slf4j`，不要手写静态 logger 字段，除非该类无法使用 Lombok 或有明确技术原因。
- 日志不能吞掉异常。捕获异常后如果继续抛出项目异常，必须把原始异常作为 cause 保留；日志内容应包含领域上下文，不要只打印“failed”。
- 不要把所有职责塞进 `Manager` 类。必须能从类名看出它维护的领域概念或执行的协作职责。

## 开发流程

1. 开发前先定位对应设计文档，确认模块边界和依赖方向。
2. 先补充或调整测试，再实现核心逻辑。
3. 实现时优先最小正确改动，不引入无依据的兼容层。
4. 修改跨模块 API 时，同步检查调用方、测试和设计文档约束。
5. 完成后运行相关测试，核心模块改动至少运行 `./gradlew.bat test`。
6. 编辑高扇出符号（被多处引用的类、方法或 record 组件）前，先用 Grep 扫描调用点并向用户报告 blast radius（直接调用方、改动风险）后再动手；改完跑全量回归确认测试数不倒退。
7. 不要提交 build 输出、IDE 缓存、临时数据文件。

## 禁止事项

- 不要脱离 `C:\coding\java\self\dbtest\dbtest\docs\design` 的设计文档自由发挥核心架构。
- 不要用一个巨大的 `DatabaseManager`、`StorageManager` 或 `EngineManager` 吞掉所有职责。
- 不要让上层 SQL/session 直接操作 page、record、redo、undo 内部结构。
- 不要让底层存储模块依赖 SQL AST、executor 或 session。
- 不要用全局可变单例隐藏依赖。
- 不要在生产代码中直接抛出 `IllegalArgumentException` 或裸 `RuntimeException`；必须使用项目异常层次。
- 不要使用 `synchronized`、`wait()`、`notify()`、`notifyAll()` 实现并发控制。
- 不要在并发代码中遗漏释放路径。
- 不要在恢复、事务、redo、flush 代码中留下未经测试的 happy path。
- 不要用 TODO/TBD 代替明确设计或实现决策。
