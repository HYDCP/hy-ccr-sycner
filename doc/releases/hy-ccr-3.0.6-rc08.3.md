# hy-ccr-3.0.6-rc08.3

`hy-ccr-3.0.6-rc08.2` 的补丁版本，唯一 Git tag 为 `hy-ccr-3.0.6-rc08.3`。不移动、不覆盖 `hy-ccr-3.0.6-rc08.2`。

## 代码范围

- 基线：`hy-ccr-3.0.6-rc08.2@eac07cf4`，继续在发布分支 `release/3.0.6-rc08.2` 上发布。不包含 rc09 线（`release/3.0.6-rc08.1` 分支）上的任何提交。
- 新增 [PR #24](https://github.com/HYDCP/hy-ccr-sycner/pull/24)（squash 合入为 `e5ac8092`）。
- rc08.2 的其余内容不变，见 [hy-ccr-3.0.6-rc08.2.md](hy-ccr-3.0.6-rc08.2.md)。

## 改动内容

### FE 返回不带 master 地址的 NOT_MASTER 时，任务不再永久停止

Syncer 调用 FE 的 `getBinlog` 等接口时，如果该 FE 不是 master，FE 返回 `NOT_MASTER` 并带上 master 地址，Syncer 据此重定向。但 FE 在自己也不知道 master 是谁时不带地址，FE 日志为 `skip setting invalid master address`。会出现这种响应的情况：

- follower 与 master 失联，元数据落后超过 `meta_delay_toleration_second`（默认 300 秒）后变成未就绪；
- 原 master 被 kill 后重启，作为 follower 还没追上元数据；
- master 选举进行中。

原先 Syncer 把这种响应当作 panic 错误。任务主循环一旦记下 panic 错误就不再执行同步，FE 恢复后也不会继续，日志持续输出：

```
job panic, job: <name>, err: [fe] not master compatible fe addr [<host>:<port>]
```

暂停再恢复任务不能清除这个状态，只有重启 Syncer 才能恢复。上游 master 切换时，这会让一批任务同时停止。

现在这种响应是可恢复错误：本轮同步失败，日志为 `job sync failed ... not master compatible`，下一轮重试；FE 能给出 master 地址后按原有逻辑重定向到新 master。改动只有 `pkg/rpc/fe.go` 里的一处错误类型，重试间隔、FE 切换逻辑都没有变。

## 行为变化

- FE 返回不带地址的 `NOT_MASTER` 时，任务状态保持运行并持续重试，不再进入 panic。日志关键字从 `job panic` 变为 `job sync failed`，错误文本 `not master compatible` 不变。按 `job panic` 配置的日志告警在这个场景下不再触发，需要的话改为匹配 `not master compatible`，或看同步延迟。
- 没有新增参数，接口和元数据不变。

## 已知限制

- rc08 / rc08.1 / rc08.2 的已知限制全部继续适用，见 [hy-ccr-3.0.6-rc08.2.md](hy-ccr-3.0.6-rc08.2.md#已知限制)。
- 本版不缩短故障期间的同步中断时间。Syncer 请求到的 FE 处于未就绪状态期间，同步仍然失败、延迟上涨，直到该 FE 恢复就绪或 Syncer 转向其他 FE。本版只保证 FE 恢复后任务自动继续。
- FE 地址连接失败时是否转向其他 FE，取决于错误类型：`connection has been closed by peer` 等会转向，kitex 的 `ErrGetConnection`、RPC 超时不在判断范围内。本版没有改这部分。
- 同一个集群的 FE 客户端按第一个创建它的任务的地址建立并在任务间共用，之后其他任务填写的 FE 列表不会合并进去。本版没有改这部分。
- 重定向时的日志 `addr [%s] is not master` 打印的是 master 地址，不是被请求的 FE 地址。排查时以 FE 侧日志为准。

## 构建与验证

使用 Go 1.20（与 CI 一致）。**不要用更高版本的工具链**：依赖 `choleraehyq/pid` 的 arm64 汇编会被 Go ≥1.21 的汇编器拒绝（`expected pseudo-register; found R13`），是编译失败而非告警。SQLite 后端需要 CGO 和匹配目标平台的 C 工具链。

```sh
go build ./pkg/... ./cmd/ccr_syncer
go vet ./pkg/ccr ./pkg/rpc ./pkg/service ./cmd/ccr_syncer
go test -count=1 $(go list ./... | grep -v /cmd | grep -v kitex_gen) ./cmd/ccr_syncer
go test -race -count=1 ./pkg/ccr ./pkg/rpc ./pkg/service ./cmd/ccr_syncer
```

新增用例（`pkg/rpc/fe_test.go`），在旧代码上都会失败：

- `TestNotMasterWithoutMasterAddressIsRecoverable`：不带地址的 `NOT_MASTER` 返回可恢复错误，不是 panic 错误。
- `TestRedirectToMasterAfterFeBecomesReady`：FE 就绪并给出 master 地址后，下一次调用重定向到 master 并成功。

在真实集群上复现的方法：源集群 3 个 FE，停掉其中 2 个只留 1 个 follower，等待超过 `meta_delay_toleration_second`，该 FE 日志出现 `meta out of date` 和 `skip setting invalid master address` 后，观察 Syncer 日志；再恢复 FE、写入数据，确认任务不重启即可追平。

发布二进制从 tag 的干净检出构建，用 Makefile 注入版本信息：

```sh
make ccr_syncer
bin/ccr_syncer -version    # 期望 hy-ccr-3.0.6-rc08.3:<commit-sha>
make tarball
```

## 升级与回退

- 没有元数据变更，没有新增或变更的启动参数。从 rc08.2 升级只需替换二进制并重启；滚动升级时逐台进行。`start_syncer.sh` 与 rc08.2 相同。
- 升级前已经处于 `job panic` 的任务，会在重启 Syncer 时从已保存的进度继续，不需要重建。
- 从 rc08 / rc08.1 直接升级时，同时适用 rc08.2 的升级说明（BE 列表缓存默认改为手动模式）。
- 回退到 rc08.2 只需换回二进制并重启，没有其他要撤销的配置。
