---
title: "Resume V1 测试环境上线检查清单"
description: "Resume V1（copy 断点续跑）测试环境上线前检查项"
---

# Resume V1 测试环境上线检查清单

## 目标

这份清单用于在测试环境上线 Resume V1 前，快速确认代码、配置、命令入口和验证方式都已经准备好。

Resume V1 当前定位为：

- 仅支持 `copy`
- 支持文件系统源
- 支持 S3 源
- 不支持 `check`
- 不支持 `move`
- 不支持带删除语义的 `sync`

## 一、代码版本检查

上线前先确认测试环境使用的是包含 Resume V1 改动的版本。

至少应包含以下能力：

- `copy` 支持 `--resume`
- `check` 明确拒绝 `--resume`
- `sync/resume/status`
- `sync/resume/clear`
- S3 源 continuation token 恢复
- 本地文件系统源恢复

建议确认以下文件已在版本中：

- [resume.go](/my/new/rclone/fs/sync/resume.go)
- [rc.go](/my/new/rclone/fs/sync/rc.go)
- [types.go](/my/new/rclone/fs/resume/types.go)
- [store.go](/my/new/rclone/fs/resume/store.go)

## 二、配置检查

### 全局参数

测试环境需要确认以下参数可用：

- `--resume`
- `--resume-id`
- `--resume-error-limit`
- `--resume-history-limit`

### RC 接口

如果测试环境需要查看或清理状态，确认已启用 RC，并能访问：

- `sync/resume/status`
- `sync/resume/clear`

### S3 配置

如果要验证 S3 源恢复，确认测试环境已有一个可访问的 S3 源端。

需要确认：

- endpoint 正常
- access key / secret key 正常
- 可以正常创建桶
- 可以正常执行 `copy`

## 三、上线前最小回归

建议在上线测试环境前，至少执行以下回归：

### Go 测试

```bash
go test ./fs/resume
go test ./fs/sync -run 'Resume|RcResume'
go test ./fs/operations -run 'CheckFnRejectsResume'
go test ./backend/s3 -run 'Resume|resume'
```

### 真实场景脚本

```bash
cd /my/new/rclone
./bin/test-resume-v1.sh
```

如果脚本最终输出：

```text
全部 Resume V1 场景测试完成
```

则说明当前版本已经通过本地真实场景验证。

## 四、测试环境重点验证项

测试环境上线后，建议重点验证以下 5 项：

1. 本地文件系统源中断恢复
2. S3 源中断恢复
3. 失败文件记录与重试
4. `status/clear` 接口
5. `resume_error_limit` 超限停止

## 五、预期行为

测试环境中应当看到以下行为：

- 中断后再次执行 `copy --resume`，任务从上次位置继续
- 已完成文件不会重复传输
- 失败文件会出现在 `status` 中
- 修复失败后再次执行，任务可以继续
- 完成后 `status` 返回无状态

## 六、当前不应视为缺陷的行为

以下行为属于当前版本的设计边界，不应直接判为缺陷：

- 单个大文件中断后重新从头传该文件
- 不做全量重启后 `check`
- `check` 命令拒绝 `--resume`
- `move` / `sync delete` 不支持 `--resume`

## 七、上线测试结论模板

测试环境验证完成后，可以按下面格式记录结果：

```text
Resume V1 测试环境验证结果：
- 文件系统源恢复：通过 / 未通过
- S3 源恢复：通过 / 未通过
- 失败重试：通过 / 未通过
- 状态接口：通过 / 未通过
- 失败上限：通过 / 未通过
- 结论：允许继续测试 / 需要回退
```
