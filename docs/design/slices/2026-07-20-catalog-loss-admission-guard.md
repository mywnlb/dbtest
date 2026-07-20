# Catalog Loss Admission Guard

## 目标

- 在公共 `DatabaseEngine` 打开 catalog 前识别 catalog 丢失或零长度状态。
- 仅当实例目录不存在任何权威持久痕迹时，允许初始化空 `mysql.ibd`。
- existing 实例无法证明 catalog 完整时 fail-closed，绝不进入 DDL recovery 或 orphan cleanup。
- 失败必须保留用户表空间与全部恢复证据，供备份恢复或后续 catalog rebuild 工具使用。

## 关键决策

- catalog 非空时只走 `FileInternalCatalogStore.openExisting`，损坏沿既有格式校验失败。
- catalog 缺失或零长度时扫描 DD control、redo、undo、doublewrite 与受控表空间候选。
- `mysql.dd.ctrl`、`redo.log`、任意稳定命名 redo ring、redo/checkpoint control 均是 existing 证据。
- 任意稳定命名 `undo_<数字>.ibu` 都是证据，不能只检查当前配置的 undo space id。
- 三类 doublewrite 文件与 `tables/table_*_space_*.ibd` 候选均是证据。
- tablespace glob 与现有 orphan cleanup 保持一致，保证可能被 cleanup 删除的对象一定先触发保护。
- recovery progress、buffer-pool dump、空目录、普通诊断文件和近似但不合法的文件名不是权威证据。
- 任一证据存在或证据扫描失败时抛出致命 catalog admission 异常。
- 异常诊断包含 catalog 状态和证据类别，不暴露自动绕过或隐式重建开关。
- 准入 guard 位于 engine 组合根；storage 不反向理解 DD catalog，也不改变自身 fresh 判定。

## 启动顺序

1. `DatabaseEngine` 建立或确认 base directory。
2. catalog admission 只读检查 `mysql.ibd` 状态与持久痕迹。
3. existing catalog 走严格打开；真正 fresh 才初始化双 header 空 catalog。
4. catalog 成功后才打开 DD control、重建 repository 并执行 storage/DD recovery。
5. admission 失败时组合根进入 `FAILED`，尚未创建 storage、DDL recovery 或 session registry。

## 非目标

- 不从单表 SDI 反向发布 schema/table catalog。
- 不引入 schema SDI、目录 manifest 或“完整扫描结束”提交点。
- 不自动 quarantine、移动、重命名或删除任何文件。
- 不增加管理员 bypass、force recovery 或 catalog rebuild 命令。
- 不改变 catalog、redo、undo、doublewrite、SDI 的任何持久格式。
- 不增加跨进程实例目录锁；本切片沿用单进程独占 base directory 假设。

## 验收测试

- 空目录和仅有零长度 catalog 可以首次建库。
- 仅有非权威诊断文件时仍可首次建库。
- 每类权威证据单独存在时，缺失或零长度 catalog 均不能初始化。
- 非空损坏 catalog 必须走 existing 校验，不能降级为 fresh。
- 证据目录无法扫描时 fail-closed 并保留原始 cause。
- existing 空库删除 catalog 后启动失败，证明保护不依赖用户表文件。
- existing 有表实例删除或截空 catalog 后启动失败，表空间字节不变且 catalog 不被重建。
- 定向测试、全量 Gradle 测试、静态禁用项扫描与 `git diff --check` 全部通过。

## Current Map 更新

- Public bootstrap 实线增加 `CatalogBootstrapAdmission -> FileInternalCatalogStore`。
- Recovery orchestration 明确 admission 早于 DD discovery、storage recovery 和 orphan cleanup。
- backlog 将 catalog-loss admission guard 标记完成，catalog discovery/rebuild 继续保留为缺口。
