---
title: "Resume V1 测试矩阵"
description: "Resume V1（copy 断点续跑）测试矩阵与用例对照"
---

# Resume V1 测试矩阵（copy 断点续跑）

## 说明

这份文档用于统一说明 Resume V1 当前的测试覆盖情况，重点回答以下问题：

- 哪些场景已经通过真实脚本验证
- 哪些场景有对应 Go 测试托底
- 出问题时应该优先看脚本还是单测

Resume V1 当前只支持 `copy`，不支持 `check`、`move` 或带删除语义的 `sync`。

## 使用方式

### Go 测试

常用命令：

```bash
go test ./fs/resume
go test ./fs/sync -run 'Resume|RcResume'
go test ./fs/operations -run 'CheckFnRejectsResume'
go test ./backend/s3 -run 'Resume|resume'
```

### 真实场景脚本

脚本位置：

- [test-resume-v1.sh](/my/eas-monorepo/rclone/bin/test-resume-v1.sh)

运行方式：

```bash
cd /my/eas-monorepo/rclone
./bin/test-resume-v1.sh
```

如果本地存在 `eas-s3` 配置，脚本会自动接入本地 S3 源场景。

## 测试矩阵

| 编号 | 场景 | 脚本覆盖 | 对应 Go 测试文件 | 重点验证 | 出问题先看哪里 |
|---|---|---|---|---|---|
| 1 | 本地文件系统扫描/传输中断后继续 | 场景 1 | [resume_test.go](/my/eas-monorepo/rclone/fs/sync/resume_test.go) | 中断后继续、最终不漏文件 | 先看脚本，再看单测 |
| 2 | 失败文件记录与下次优先重试 | 场景 2 | [resume_test.go](/my/eas-monorepo/rclone/fs/sync/resume_test.go) | 失败记录、下次优先重试 | 两边都看 |
| 3 | `status` / `clear` 接口 | 场景 3 | [rc_test.go](/my/eas-monorepo/rclone/fs/sync/rc_test.go) | 状态查看、状态清理 | 先看单测 |
| 4 | 失败上限超限后停止并保留状态 | 场景 4 | [resume_test.go](/my/eas-monorepo/rclone/fs/sync/resume_test.go) | 超限停止、保留状态 | 两边都看 |
| 5 | `status` 统计字段校验 | 场景 5 | [accounting_restore_test.go](/my/eas-monorepo/rclone/fs/resume/accounting_restore_test.go), [resume_test.go](/my/eas-monorepo/rclone/fs/sync/resume_test.go) | `totals.files`、`totals.bytes` 恢复 | 先看脚本 |
| 6 | 多次连续中断恢复 | 场景 6 | [resume_test.go](/my/eas-monorepo/rclone/fs/sync/resume_test.go) | 中断两次以上仍能完成 | 先看脚本 |
| 7 | 目标端已有部分文件 | 场景 7 | [resume_test.go](/my/eas-monorepo/rclone/fs/sync/resume_test.go) | 目标端已有文件时恢复不误传 | 先看脚本 |
| 8 | 特殊文件名 | 场景 8 | 无专门 Go 单测 | 中文、空格、大小写、特殊符号 | 先看脚本 |
| 9 | 较大规模小文件 | 场景 9 | 无专门 Go 单测 | 大量小文件恢复稳定性 | 先看脚本 |
| 10 | 失败文件反复失败 | 场景 10 | [resume_test.go](/my/eas-monorepo/rclone/fs/sync/resume_test.go), [store_test.go](/my/eas-monorepo/rclone/fs/resume/store_test.go) | 失败次数递增、失败状态稳定 | 两边都看 |
| 11 | `resume_id` 隔离 | 场景 11 | [rc_test.go](/my/eas-monorepo/rclone/fs/sync/rc_test.go), [resume_test.go](/my/eas-monorepo/rclone/fs/sync/resume_test.go) | 不同 job 状态隔离 | 两边都看 |
| 12 | `SIGKILL` 强制中断后恢复 | 场景 12 | 无专门 Go 单测 | 非温和中断后的恢复 | 先看脚本 |
| 13 | `resume` 状态损坏 | 场景 13 | [store_test.go](/my/eas-monorepo/rclone/fs/resume/store_test.go) | 状态损坏时不崩溃、可恢复 | 先看单测 |
| 14 | 大文件与混合文件集 | 场景 14 | 无专门 Go 单测 | 大文件重传、小文件不重复 | 先看脚本 |
| 15 | S3 源中断后继续 | 场景 15 | [s3_internal_test.go](/my/eas-monorepo/rclone/backend/s3/s3_internal_test.go), [resume_test.go](/my/eas-monorepo/rclone/fs/sync/resume_test.go) | continuation token、S3 真实恢复 | 两边都看 |

## Go 测试文件职责

### Resume 基础层

- [store_test.go](/my/eas-monorepo/rclone/fs/resume/store_test.go)
  - 持久化状态读写
  - 失败记录与成功恢复
  - 损坏 meta / 空状态边界

- [work_test.go](/my/eas-monorepo/rclone/fs/resume/work_test.go)
  - `WorkKey`
  - `EntryFingerprint`

- [accounting_restore_test.go](/my/eas-monorepo/rclone/fs/resume/accounting_restore_test.go)
  - 恢复统计与历史事件

### Copy Resume 主逻辑

- [resume_test.go](/my/eas-monorepo/rclone/fs/sync/resume_test.go)
  - 目录栈恢复
  - 已完成文件跳过
  - 失败重试
  - 失败上限
  - 统计恢复

### 状态接口

- [rc_test.go](/my/eas-monorepo/rclone/fs/sync/rc_test.go)
  - `sync/resume/status`
  - `sync/resume/clear`
  - `jobId` 与 `srcFs + dstFs` 推导

### S3 分页恢复

- [s3_internal_test.go](/my/eas-monorepo/rclone/backend/s3/s3_internal_test.go)
  - continuation token 正常恢复
  - token 无效回退恢复
  - S3 元信息变化不影响 copy resume 兼容性

### 非支持命令边界

- [check_resume_test.go](/my/eas-monorepo/rclone/fs/operations/check_resume_test.go)
  - `check` 明确拒绝 `--resume`

## 排查建议

### 优先看 Go 测试的情况

- 状态文件损坏
- `WorkKey` 或 `EntryFingerprint` 算法问题
- `status` / `clear` 返回结构问题
- S3 continuation token 回退逻辑问题
- `check` 是否错误地进入 `resume`

### 优先看脚本的情况

- 真实中断时机问题
- 本地文件系统和 S3 真实恢复行为
- 多次连续中断
- 特殊文件名
- 大量小文件
- 大文件混合场景
- `SIGKILL` 强制中断

### 两边都要看的情况

- 失败重试 / 失败上限
- `resume_id` 隔离
- S3 真实恢复异常
- 统计恢复和状态恢复不一致

## 当前结论

当前 Resume V1 已经具备以下测试覆盖：

- 核心可用性
- 边界恢复
- 失败重试与失败上限
- 状态查看与清理
- 统计恢复
- 强制中断
- 状态损坏
- 大文件混合
- S3 真实源恢复

也就是说，当前测试已经不只覆盖“功能能不能用”，还覆盖了“异常条件、恢复时序和状态一致性”。
