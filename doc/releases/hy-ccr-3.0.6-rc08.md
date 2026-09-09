# hy-ccr-3.0.6-rc08

HYDCP 的 3.0.6 系列候选版本，唯一 Git tag 为 `hy-ccr-3.0.6-rc08`；不另建 `3.0.6-rc08` tag，不覆盖任何旧 tag。

## 代码范围

- 基线：`release/3.0.6-rc07-node-info-add@abd1e6b77f451bf2fe2c599f6d03a4bc86e073c1`，已合并 [PR #2](https://github.com/HYDCP/hy-ccr-sycner/pull/2)。
- 保留 `3.0.6-rc07-node-info-add@69184ae` 的 `/node_info`；不包含 `/migrate`、`/notify_update` 或 dev 上其他平台迁移改动。
- 回移 [PR #1](https://github.com/HYDCP/hy-ccr-sycner/pull/1) 的 `b2b5ee48`、`1ef97ad0`、`4554cffa`。回移提交的说明记录原始 SHA，便于追溯。
- 测试入口补充 `cmd/ccr_syncer`，使 `make test` 不再漏掉 JobCollector 回归测试。

## 行为变化

- FE GetBinlogLag 返回非 OK 时，`/get_lag` 保持 HTTP 200 和原 JSON 结构，但返回 `success:false` / `error_msg`。JSON 中默认的 `lag:0` 不能当作有效测量值。
- 任务列表的两个 lag 列在不可用时显示 `-1`。
- `ccr_job_running_lag_total` / `ccr_job_running_lag_seconds` 在错误期间保留旧值；首次失败不会创建假的零值 lag。`ccr_job_running_sync_state` / `ccr_job_running_sub_sync_state` 独立刷新。
- BE 缓存 TTL 为 60 秒。`POST /invalidate_backends_cache` 支持空 body / `{}` 失效本节点全部 job，也支持 `{"name":"job1"}` 指定 job。非 POST 返回 405 和 `Allow: POST`，无效 JSON 返回 400。
- `/node_info` 保持版本、主机/端口、运行时长、元数据存储类型、资源和任务统计字段。

## 已知限制

- 本版不实现 TOO_OLD 自动全量恢复；同步循环仍可能停滞，需要从 lag 查询错误等信号识别并人工处理。
- 保留旧 lag 不等于 Prometheus 自动标记该值过期，告警不能只依赖 lag 阈值。
- BE 刷新与失效并发时，较早开始的刷新仍可能重新验证旧结果，后续 TTL 刷新可恢复。
- 指定 job 的缓存失效必须发往所属 syncer；不包含自动 owner 路由。
- 验证源表删除后的 `BINLOG_NOT_FOUND_TABLE` 信号需源端 Doris 含 [HYDCP/hy-doris#74](https://github.com/HYDCP/hy-doris/pull/74)。这不是 syncer 启动的新硬性依赖，信号仍受 binlog 保留窗口限制。

## 构建与验证

与仓库 CI 一致使用 Go 1.20；本次验证使用 Go 1.20.14。SQLite 后端需要 CGO 和匹配目标平台的 C 编译工具链。

```sh
go build ./pkg/... ./cmd/ccr_syncer
go vet ./pkg/... ./cmd/ccr_syncer
make test
go test -count=10 ./cmd/ccr_syncer ./pkg/service
go test -race -count=1 ./cmd/ccr_syncer ./pkg/service
git diff --check
```

2026-09-03 在 macOS ARM64 / Go 1.20.14 上完成以上检查，全部通过；改动的 Go 文件通过 gofmt 检查。测试包含 collector 首次采集/已有指标下的 OK、两种 FE non-OK、RPC error、factory error，以及 HTTP 请求校验、`/node_info` / 缓存失效路由共存、迁移路由未注册。

本机 CGO 编译使用 `CGO_CFLAGS='-O2 -g -Wno-nullability-completeness'` 处理本机 C 头文件告警，仅作用于验证命令，未修改全局环境或仓库依赖。Makefile 的 `uname -i` 在 macOS 上有已有兼容性警告，不影响本次测试退出状态。

尚未执行真实 Doris/CCR 双集群测试，尚未构建或认证 Linux x64/ARM64 安装包。上述单元/路由测试不替代集群升级验证。

发布二进制时，从该 tag 的干净检出构建，使用 Makefile 注入版本信息，不要用不带版本注入参数的裸 `go build` 产物替代：

```sh
make ccr_syncer
bin/ccr_syncer -version
make tarball
```

版本输出应为 `hy-ccr-3.0.6-rc08:<commit-sha>`。在选定的目标 Linux 平台构建 `tar.xz` 包并生成 SHA256，再创建带安装包的 Release；本次准备不等于已经发布或部署安装包。

## 升级与回退检查

1. 记录旧版本、参数、job 清单和 progress；备份配置和元数据。SQLite 文件备份前停止对应进程，共享 MySQL/PostgreSQL 使用一致性备份。
2. 保留旧二进制及启动脚本，在测试环境先确认现有 job 可继续增量同步、正常完成全量到增量转换。
3. 检查 `/version`、`/node_info`、`/get_lag`、任务列表及 Prometheus 指标；测试缓存失效、非法请求的 400/405，以及源端删表/非 OK 响应。
4. 确认没有两个进程使用同一节点身份或同时写同一 SQLite 文件，再替换生产二进制并观察任务进度。
5. 若需回退，停止新进程，使用旧二进制和原参数启动；不要直接用旧备份覆盖仍在使用的共享元数据库。元数据恢复需单独评估，避免倒退任务进度。
