---
title: "Resume V1 Segment-Window-Frontier 阶段性实现与验证"
description: "记录 S3 轻量化恢复第一阶段的设计、性能结果和正确性验证"
---

# Resume V1 Segment-Window-Frontier 阶段性实现与验证

## 范围

当前这一轮只落地了：

- `S3 / 对象` 的轻量化恢复第一阶段
- `segment-window-frontier`

文件/NAS 侧目前只补了状态结构，执行逻辑还没有切换到新的 `frame-task-frontier`。

当前进度更新：

- `phase 2.1` 已完成：
  - `scan.file_tree` 已可读可写
  - legacy 文件恢复路径已支持优先读取 `file_tree`，缺失时回退到 `scan.frames`
- `phase 2.2` 已部分完成：
  - `FileTask / FileWindow / frontier` 骨架与定向测试已补齐
  - 但尚未切主执行路径

## 设计摘要

对象恢复不再默认依赖“每对象重状态”作为主路径，而是引入：

- `ObjectScanState`
- `ObjectSegment`
- `ObjectWindowState`

恢复语义改为：

1. 扫描阶段按对象切段
2. 允许多个 segment 并发执行
3. checkpoint 只按**连续完成前沿**推进
4. 失败 segment 会阻塞前沿，防止越过最早未完成段

## 配置项

新增后端配置项：

- `resume_object_window_size`
  - 默认 `3`
- `resume_file_window_size`
  - 默认 `3`
- `resume_object_segment_size`
  - 默认 `1000`
- `resume_success_checkpoint_interval`
  - 默认 `5s`

## 正确性验证

### 1. 本地 frontier 语义测试

新增测试覆盖：

- 连续前缀缺口存在时，前沿不得推进
- 连续前缀补齐后，前沿可顺序推进

对应测试：

- `TestResumeObjectFrontierReadyHonorsContiguousPrefix`

### 2. 2.175 远端中断恢复实测

测试环境：

- 二进制路径：
  - `/opt/test/eas-console-main/rclone/rclone-resume`
- 源：
  - `codex-minio-bench/dataset-a/grp-00000`
- 目标：
  - `test2/codex-bench-segfront-correctness`

测试步骤：

1. 启动 `copy --resume --resume-id codex-segfront-correctness`
2. 运行约 `6s`
3. 强制中断
4. 使用相同 `resume-id` 重新执行
5. 最终对比源/目标对象数与总大小

中断后目标状态：

```json
{"count":1080,"bytes":122059276,"sizeless":0}
```

最终源/目标统计：

源：

```json
{"count":10000,"bytes":1174567532,"sizeless":0}
```

目标：

```json
{"count":10000,"bytes":1174567532,"sizeless":0}
```

结论：

- 中断恢复成功
- 最终对象数和总大小一致
- 当前验证下未观察到漏对象

## 性能验证

测试环境：

- `2.175`
- 相同链路：`MinIO -> eas-s3`
- 数据：
  - `dataset-a/grp-00000`
  - 约 `10000` 对象
  - 约 `1.09 GiB`
- 参数：
  - `transfers=200`
  - `checkers=150`

结果：

- `native_elapsed = 4.88s`
- `resume_elapsed = 5.72s`

对比历史阶段：

- 最早 `resume_v1`：`75.88s`
- 批量成功提交后：`23.21s`
- 目录目标列表复用后：`17.17s`
- 当前 `segment-window-frontier` 第一阶段：`5.72s`

结论：

- 当前 `resume_v1` 在该 S3 小对象场景下已经明显接近原生
- 相对 `native` 约为 `1.17x`

## 当前边界

当前实现仍有边界：

- 文件/NAS 仍未切到 `frame-task-frontier`
- 对象模式当前仍复用现有 page/list 行为，并未完全替换扫描器
- 失败 segment 的处理当前优先保证正确性，后续仍可继续优化

## 与 eas-console 的衔接说明

当前这版实现已经按“先兼容，再替换”的原则收口，避免 `resume_v1` 新模型和
`eas-console` 现有链路脱节。

### 1. `sync/resume/status` 协议保持向后兼容

`eas-console` 当前依赖 `sync/resume/status` 返回这些核心字段：

- `meta`
- `scan`
- `totals`
- `run`
- `history`
- `failed`

本轮改动对 `scan` 的调整是**加字段，不删字段**：

- 保留原有 `frames / overErrorLimit / complete`
- 新增可选字段：
  - `object`
  - `file_tree`

因此：

- 老版本 `eas-console` 仍能解析
- 新增信息只作为扩展能力，不会破坏现有读取逻辑

### 2. 文件/NAS 执行路径先保持 legacy 语义

虽然已经补了文件侧这些结构：

- `FileFrame`
- `FileTreeScanState`
- `FileTask`
- `FileWindowState`

但当前**没有切换文件/NAS 的实际执行路径**，而是：

- `S3 / bucket-based`
  - 走 `segment-window-frontier`
- `文件 / NAS`
  - 继续走 legacy `runResumeLegacyFileScan(...)`

这样做的目的很明确：

- 先保证 `S3` 第一阶段收益落地
- 不让文件/NAS 的半成品状态影响 `eas-console` 现有任务语义

### 3. 当前可安全宣告的兼容结论

当前这版可以认为：

- `S3` 新对象恢复模型已经可以挂在 `eas-console` 现有 `resume_v1` 流程下工作
- 文件/NAS 任务不会因为这轮改动被强制切到新模型
- `eas-console` 仍可继续使用现有：
  - `getResumeStatus`
  - `ResumeStateResolver`
  - `TaskDetail / getJobDetail`

## 文件/NAS 第二阶段 checklist

文件/NAS 这条下一阶段不直接上线切流，先按 checklist 推进：

### A. 保持恢复语义

- 保留 `alreadyInto` 目录语义
- 保留目录 frame 恢复能力
- 保证恢复后不重复整目录下钻，也不漏文件

### B. 引入轻量 task/frontier 模型

- 扫描层继续维护 `frame`
- 文件按 batch 形成 `task`
- 支持多个 task 并发执行
- checkpoint 仅按连续完成前沿推进

### C. 保持 eas-console 对接稳定

- `sync/resume/status` 继续保留现有顶层字段
- 文件侧新增状态只作为 `scan.file_tree` 扩展信息
- 不修改 `eas-console` 当前对 `totals/run/history/failed` 的读取契约

### D. 第二阶段落地前必须补齐的验证

- 深目录恢复正确性
- `alreadyInto=true` 断点恢复
- 文件/NAS stop / retry correctness
- 文件/NAS 任务详情在 `eas-console` 中正常读取
- 老任务状态文件可读

## 文件/NAS 第二阶段代码改造清单

这一节只记录**可实施顺序**，目的是让文件/NAS 逐步靠近 `frame-task-frontier`，但不一次性替换当前 legacy 路径。

### Phase 2.1：补齐状态结构与兼容读写

目标：

- 保持 `scan.frames` 仍然可读
- 新增 `scan.file_tree` 作为扩展状态
- 不改变当前 `eas-console` 消费协议

代码落点：

- [types.go](/my/eas-monorepo/rclone/fs/resume/types.go)
  - 继续保留：
    - `ScanFrame`
    - `ScanState.Frames`
  - 已新增：
    - `FileFrame`
    - `FileTreeScanState`
    - `FileTask`
    - `FileWindowState`
- [resume.go](/my/eas-monorepo/rclone/fs/sync/resume.go)
  - 已有辅助函数：
    - `resumeFileFrames(...)`
    - `resumeFileScanState(...)`
    - `resumeFileFrontierReady(...)`

本阶段动作：

- 只允许写入 `scan.file_tree`，不允许删除或重解释 `scan.frames`
- 恢复时优先：
  - `scan.file_tree.frames`
  - 若为空则回退：
  - `scan.frames`

验收：

- 旧状态文件 `sync/resume/status` 可正常解析
- 新状态文件里同时能看到：
  - `frames`
  - `file_tree`

### Phase 2.2：抽出文件任务窗口骨架，但不替换 legacy 主流程

目标：

- 先把“task 窗口”所需的执行器骨架补好
- 但 `runResumeLegacyFileScan(...)` 仍保持线上主路径

代码落点：

- [resume.go](/my/eas-monorepo/rclone/fs/sync/resume.go)
  - 已存在：
    - `resumeFileTask`
    - `resumeFileTaskResult`
    - `runResumeFileTask(...)`
    - `resumeFileWindowSize(...)`
    - `resumeFileFrontierTaskID(...)`

本阶段动作：

- 保持这些 helper 为“旁路能力”
- 不直接在 `runResumeSourceScan(...)` 中切换到新路径
- 给文件/NAS 增加一个内部实验入口，例如：
  - `runResumeFileWindowScan(...)`
  - 但默认不启用

验收：

- `go test ./fs/sync -run 'Test.*Resume.*'` 通过
- 现有文件/NAS 任务行为不变

当前状态：

- 已完成骨架函数与定向测试：
  - `newResumeFileTask(...)`
  - `resumeFileFrontierReady(...)`
  - `runResumeFileTask(...)`
- 尚未在 `runResumeSourceScan(...)` 中启用新文件窗口执行路径

### Phase 2.3：把成功 checkpoint 从 per-batch 扩成 per-task frontier

目标：

- 文件/NAS 成功项不再只靠 `flushResumeCopyBatch(...)` 的局部提交
- 改成：
  - task 可乱序完成
  - checkpoint 只推进连续前沿

代码落点：

- [resume.go](/my/eas-monorepo/rclone/fs/sync/resume.go)
  - 当前 legacy 热点：
    - `runResumeLegacyFileScan(...)`
    - `flushResumeCopyBatch(...)`

本阶段动作：

- 新增文件 task 构建逻辑：
  - 每收集一批文件形成一个 `FileTask`
- 新增 pending task map：
  - `map[int64]resumeFileTaskResult`
- 完成后调用：
  - `resumeFileFrontierReady(...)`
  - 顺序推进 `commitFrontierTaskID`
- 成功提交时更新：
  - `scan.file_tree.window.commitFrontierTaskID`
  - 对应的 frame 边界

验收：

- 乱序完成 task 时，checkpoint 不得越过最早未完成 task
- stop / retry 后最多重做窗口内 task，不重复全部目录

当前状态：

- 已完成旁路提交器与测试：
  - `commitResumeReadyFileTasks(...)`
  - `TestCommitResumeReadyFileTasksAdvancesContiguousFrontier`
  - `TestCommitResumeReadyFileTasksStopsAtFailure`
- 当前仍未接入文件/NAS 主执行路径
- 因此现有任务行为保持不变，但 phase 2.3 的核心顺序提交语义已经有代码和测试支撑

### Phase 2.4：把扫描 frame 与 task frontier 绑定

目标：

- 文件/NAS 的“从哪继续扫”不再只靠 `LastDoneEntryKey`
- 而是靠：
  - `frame + frontierTaskID + endFile`

代码落点：

- [resume.go](/my/eas-monorepo/rclone/fs/sync/resume.go)
  - `prepareResumeCopyTask(...)`
  - `resumeCopyScanState(...)`
  - `runResumeLegacyFileScan(...)`

本阶段动作：

- 每个 `FileTask` 记录：
  - `startFrameSnapshot`
  - `endFile`
  - `fileCount`
- frontier 推进时，把“最后连续完成 task”的 frame 快照写回
- `LastDoneEntryKey` 降级成兼容字段，不再是唯一恢复依据

验收：

- 深目录任务 stop / retry 后：
  - 不漏目录
  - 不重复整目录下钻
- `alreadyInto=true` 语义保持正确

### Phase 2.5：把 `alreadyInto` 显式补进文件轻状态

目标：

- 文件恢复真正保住目录树语义

代码落点：

- [types.go](/my/eas-monorepo/rclone/fs/resume/types.go)
  - 当前 `FileFrame` 只有：
    - `Dir`
    - `LastEntry`
- 需要补：
  - `AlreadyInto bool`

本阶段动作：

- `resumeFileFrames(...)` 从 `ScanFrame` 映射时补充 `AlreadyInto`
- 文件扫描推进时，目录 frame 切换同步维护这个字段

验收：

- 恢复点落在目录节点时：
  - 已进入过的目录不会被整目录重扫
  - 未进入过的目录仍能正常进入

### Phase 2.6：最后才切主路径

目标：

- 只有在 correctness 和 console 兼容都跑稳后
- 才允许从：
  - `runResumeLegacyFileScan(...)`
  - 切到：
  - `runResumeFileWindowScan(...)`

切换条件：

- 本地 resume 定向测试通过
- 深目录/NAS 人工中断恢复验证通过
- `eas-console` 任务详情、失败列表、resume 状态读取无回归
- 老状态文件可读

建议方式：

- 先做内部配置开关
- 再灰度默认值

## 第二阶段具体测试建议

### 单元测试

- `resumeFileFrontierReady(...)`
  - 连续前沿推进
  - 缺口阻塞
  - 失败 task 阻塞前沿
- `resumeFileFrames(...)`
  - `LastEntry`
  - `alreadyInto`
  映射正确

### 集成测试

- 深目录树：
  - 多级目录
  - stop / retry
  - 最终文件数和总大小一致
- 目录断点落在：
  - 目录进入前
  - 目录进入后
  两类场景
- 乱序 task 完成：
  - checkpoint 不越前沿

### eas-console 回归

- `sync/resume/status` 仍返回：
  - `meta`
  - `scan`
  - `totals`
  - `run`
  - `history`
  - `failed`
- `ResumeStateResolver` 不因 `scan.file_tree` 新字段异常
- 任务详情和 resume 状态展示不超时、不报错

## 下一步

建议下一阶段继续：

1. 文件/NAS `frame-task-frontier` 执行路径
2. 将文件扫描恢复与成功 checkpoint 粗粒度化
3. 继续减少 legacy `done`/重状态在热路径中的参与度
