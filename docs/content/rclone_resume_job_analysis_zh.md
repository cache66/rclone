---
title: "rclone 上一次处理与中断继续分析"
description: "分析 RC 任务记录、Resume V1 断点续跑能力、对应命令与输出示例"
---

# rclone 上一次处理与中断继续分析

## 结论先看

当前仓库里其实有两套容易混淆的能力：

1. `job/*` 这一套 RC 异步任务接口
2. `copy --resume` 这一套 Resume V1 断点续跑能力

它们不是同一个东西。

- `job/status` / `job/list` 关注的是“这个 RC 任务有没有跑完”
- `copy --resume` 关注的是“copy 扫描到哪里、哪些文件已完成、哪些失败、下次如何继续”

另外，当前代码库里没有发现核心代码会把 RC 任务持久化到 `rclone_jobs.json`。这个文件目前更像是一次外部测试产物，而不是当前主干代码内建的正式存储格式。

## 一、对“上一次 rclone 处理”的分析

结合仓库根目录现有的 [rclone_jobs.json](/my/eas-monorepo/rclone/rclone_jobs.json) 可以看到，最近几次任务大致分成三类。

### 1. 最近一次成功任务

按 `startTime` 倒序看，最近一次记录是：

- `id=21`
- `startTime=2026-02-17T09:56:49.0477232+08:00`
- `finished=true`
- `success=true`
- `params={"rate":"2M"}`

这条记录更像是一次 RC 参数/限速相关任务，不是一次 `sync/copy` 文件传输任务，因为它的 `output` 只有：

```json
{
  "bytesPerSecond": 2097152,
  "bytesPerSecondRx": 2097152,
  "bytesPerSecondTx": 2097152,
  "rate": "2Mi"
}
```

### 2. 和“中断”最相关的历史任务

文件里更接近“中断后继续”语义的是这两条：

- `id=17`
  - `startTime=2026-02-01T15:15:42.461186+08:00`
  - `success=false`
  - `error="context canceled"`
  - `params.srcFs="D:/anaconda3"`
  - `params.dstFs="e:/test7"`

- `id=20`
  - `startTime=2026-02-01T21:03:28.1177771+08:00`
  - `success=false`
  - `error="directory not found"`
  - `params.srcFs="D:/anaconda"`
  - `params.dstFs="e:/test7"`

其中：

- `context canceled` 更像是 RC 任务被停止、请求上下文取消，或者进程侧取消
- `directory not found` 则是源路径本身无效，不属于“可继续”的中断

### 3. 一条未完成悬挂任务

还有一条：

- `id=5`
- `startTime=2026-02-01T21:03:28.1203131+08:00`
- `finished=false`
- `success=false`
- `params={}`

这条看起来像任务状态残留，或者是在外部持久化时留下的不完整快照。由于当前仓库核心代码并不直接读写 `rclone_jobs.json`，所以它更像测试过程中的外部记录，而不是 rclone 当前内建恢复逻辑的一部分。

## 二、当前仓库真正的“中断继续”实现

当前正式实现的中断继续能力是 Resume V1。

它的入口已经写进 [cmd/copy/copy.go](/my/eas-monorepo/rclone/cmd/copy/copy.go)：

```text
--resume 支持 copy in V1
恢复 copy 扫描游标、已完成计数器、失败项状态
中断后从上次位置继续
属于文件级恢复，不恢复单个大文件的部分内容
```

对应实现主流程在：

- [fs/sync/resume.go](/my/eas-monorepo/rclone/fs/sync/resume.go)
- [fs/resume/types.go](/my/eas-monorepo/rclone/fs/resume/types.go)
- [fs/resume/store.go](/my/eas-monorepo/rclone/fs/resume/store.go)
- [fs/sync/rc.go](/my/eas-monorepo/rclone/fs/sync/rc.go)

### Resume V1 做了什么

`resumeRun()` 的实际流程是：

1. 校验当前命令是否允许 `--resume`
2. 生成或读取 `resume_id`
3. 打开持久化状态存储
4. 载入上次扫描游标、累计统计、失败项、历史事件
5. 先重试上次失败的文件
6. 再从上次扫描位置继续遍历源端目录
7. 成功完成且没有待失败项时，清理状态

### 它保存了哪些状态

[fs/resume/types.go](/my/eas-monorepo/rclone/fs/resume/types.go) 里定义的核心持久化信息包括：

- `Meta`
  - 任务签名，绑定 `op`、`src`、`dst`
- `ScanState`
  - 当前遍历到哪个目录、哪一页、哪一个条目
- `CounterState`
  - 已完成文件数、对象数、字节数、失败计数
- `HistoryEvent`
  - 最近的成功/失败事件，用来恢复进度输出
- `FailedRecord`
  - 未解决失败项，下次优先重试

### 它不做什么

Resume V1 当前明确不支持：

- `check --resume`
- `move --resume`
- 带删除语义的 `sync --resume`
- `--dry-run`
- `--interactive`
- `--no-traverse`
- `--no-check-dest`

代码校验位置在 [fs/sync/resume.go](/my/eas-monorepo/rclone/fs/sync/resume.go)。

## 三、RC 异步任务和 Resume V1 的关系

RC 异步任务实现位于 [fs/rc/jobs/job.go](/my/eas-monorepo/rclone/fs/rc/jobs/job.go)。

它的职责是：

- 给每个 RC 调用分配 `jobid`
- 返回当前 rclone 进程的 `executeId`
- 允许通过 `job/status` 查询状态
- 允许通过 `job/list` 查看运行中和已完成任务
- 允许通过 `job/stop` / `job/stopgroup` 取消任务

### 关键区别

`job/*` 是任务层。

- 关心任务有没有开始、结束、报错
- 默认保存在当前 rclone 进程内存里
- `executeId` 在进程重启后会变化

`copy --resume` 是数据层。

- 关心 copy 的扫描位置、已完成项、失败项
- 状态持久化到 resume store
- 进程被中断后，可以重新启动同一个 `copy --resume` 来继续

### 一个重要现象

仓库里只在 [test_jobs.py](/my/eas-monorepo/rclone/test_jobs.py) 里搜索到了 `rclone_jobs.json`，没有在核心实现中搜到对这个文件的正式读写逻辑。

这意味着：

- `test_jobs.py` 假设了“任务文件持久化”能力
- 但当前主干代码真正落地的“可恢复”能力，是 Resume V1 的状态存储，不是 `rclone_jobs.json`

## 四、对应命令和输出示例

### 1. 直接用命令行验证中断继续

开始一次可恢复的 copy：

```bash
rclone copy /path/src /path/dst \
  --resume \
  --resume-id demo-copy-job \
  --create-empty-src-dirs
```

强制中断一次：

```bash
timeout --signal=TERM 2s \
  rclone copy /path/src /path/dst \
    --resume \
    --resume-id demo-copy-job \
    --transfers 1 \
    --checkers 1 \
    --bwlimit 128k
```

再次执行相同命令继续：

```bash
rclone copy /path/src /path/dst \
  --resume \
  --resume-id demo-copy-job
```

典型日志语义会是：

```text
Resume enabled for copy job "demo-copy-job"
Found N pending failed item(s) to retry before continuing the scan
Resume state cleared for copy job "demo-copy-job"
```

这些日志字符串来自 [fs/sync/resume.go](/my/eas-monorepo/rclone/fs/sync/resume.go)。

### 2. 用 RC 查看 resume 状态

先启动 RC：

```bash
rclone rcd --rc-no-auth --rc-addr 127.0.0.1:55742
```

查询某个 `resume_id` 的状态：

```bash
curl -X POST http://127.0.0.1:55742/sync/resume/status \
  -H 'Content-Type: application/json' \
  -d '{"jobId":"demo-copy-job"}'
```

返回结构大致是：

```json
{
  "jobId": "demo-copy-job",
  "found": true,
  "meta": {},
  "scan": {},
  "totals": {},
  "run": {},
  "history": [],
  "failed": []
}
```

其中最关键的字段是：

- `found`
  - 是否存在可恢复状态
- `scan`
  - 扫描游标
- `totals.pending_failed_count`
  - 当前待重试失败项数量
- `failed`
  - 失败文件列表

清理状态：

```bash
curl -X POST http://127.0.0.1:55742/sync/resume/clear \
  -H 'Content-Type: application/json' \
  -d '{"jobId":"demo-copy-job"}'
```

返回结构大致是：

```json
{
  "jobId": "demo-copy-job",
  "found": true,
  "cleared": true
}
```

### 3. 用 RC 跑异步任务并查询状态

发起异步 copy：

```bash
curl -X POST http://127.0.0.1:5574/sync/copy \
  -H 'Content-Type: application/json' \
  -d '{
    "srcFs": "/abs/test_source",
    "dstFs": "/abs/test_dest",
    "createEmptySrcDirs": true,
    "_async": true
  }'
```

预期返回：

```json
{
  "jobid": 1,
  "executeId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

查询任务状态：

```bash
curl -X POST http://127.0.0.1:5574/job/status \
  -H 'Content-Type: application/json' \
  -d '{"jobid":1}'
```

预期返回字段包括：

```json
{
  "id": 1,
  "executeId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "finished": true,
  "success": true,
  "error": "",
  "duration": 0.1,
  "output": {}
}
```

列出任务：

```bash
curl -X POST http://127.0.0.1:5574/job/list
```

预期返回字段包括：

```json
{
  "executeId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "jobids": [1, 2],
  "runningIds": [2],
  "finishedIds": [1]
}
```

停止任务：

```bash
curl -X POST http://127.0.0.1:5574/job/stop \
  -H 'Content-Type: application/json' \
  -d '{"jobid":2}'
```

被取消的任务常见错误会是：

```json
{
  "error": "context canceled",
  "success": false,
  "finished": true
}
```

这和 [rclone_jobs.json](/my/eas-monorepo/rclone/rclone_jobs.json) 里的 `id=17` 是一致的。

## 五、当前仓库现成测试脚本对应关系

现成脚本在 [bin/test-resume-v1.sh](/my/eas-monorepo/rclone/bin/test-resume-v1.sh)。

它覆盖了这些关键场景：

- 本地文件系统扫描/传输中断后继续
- 失败文件记录与优先重试
- `sync/resume/status` 和 `sync/resume/clear`
- 失败上限超限停止
- 多次连续中断恢复
- `resume_id` 隔离
- `SIGKILL` 强制中断后恢复
- 状态损坏后恢复
- 大小文件混合集
- 可选的 S3 源恢复

脚本成功结束时会输出：

```text
全部 Resume V1 场景测试完成
```

另一个文件 [docs/content/resume_v1_test_rollout_zh.md](/my/eas-monorepo/rclone/docs/content/resume_v1_test_rollout_zh.md) 是上线前检查清单，适合做测试环境回归。

## 六、这次本地核对结果

本次在当前工作区我确认了这些事实：

- `copy` 帮助文案已经声明 `--resume` 仅支持 V1 `copy`
- `check` 会明确拒绝 `--resume`
- `sync/resume/status` 和 `sync/resume/clear` 已实现
- `test_jobs.py` 会检查 `rclone_jobs.json`，但核心代码里没有正式引用这个文件

本地测试执行结果：

```text
go test ./fs/sync -run 'Resume|RcResume' -count=1    -> ok
go test ./fs/operations -run CheckFnRejectsResume -count=1 -> ok
go test ./fs/resume -count=1 -> ok
```

## 七、建议怎么理解当前状态

如果你的目标是“进程中断后继续 copy”，应当使用 Resume V1：

```bash
rclone copy ... --resume [--resume-id ...]
```

如果你的目标是“查看某次 RC 调用有没有完成”，应当使用：

- `job/status`
- `job/list`
- `job/stop`

如果你的目标是“把 RC 任务历史长期持久化到文件并在重启后继续按 jobid 管理”，那当前仓库里还看不到正式实现，至少不是通过 `rclone_jobs.json` 接入到主干逻辑中的。
