# hy-ccr-3.0.6-rc08.2

`hy-ccr-3.0.6-rc08.1` 的补丁版本，唯一 Git tag 为 `hy-ccr-3.0.6-rc08.2`。不移动、不覆盖 `hy-ccr-3.0.6-rc08.1`。

## 代码范围

- 基线：`hy-ccr-3.0.6-rc08.1@ee7a1e94`。发布分支 `release/3.0.6-rc08.2` 从该 tag 切出；`release/3.0.6-rc08.1` 分支已前进到 rc09 线，本版不包含其上的任何提交（#13、#14、#17 等）。
- 新增 [PR #19](https://github.com/HYDCP/hy-ccr-sycner/pull/19)（squash 合入为 `e82b9ac6`）。
- rc08.1 的其余内容不变，见 [hy-ccr-3.0.6-rc08.1.md](hy-ccr-3.0.6-rc08.1.md)。

## 改动内容

### BE 列表缓存的有效期改为启动参数，默认手动模式

rc08 带入的上游 selectdb/ccr-syncer#651 把 BE 列表缓存的有效期写死为 60 秒，过期后下一次访问会重新执行 `show backends`。rc07 及以前拿到 BE 列表后一直使用，生产按这个行为运维；60 秒自动刷新在 FE 偶发返回异常或 BE 列表短暂变化时会直接影响全量同步，而且无法关闭。

新增启动参数 `-backends_cache_ttl`（Go `time.Duration`，也可写进 `config_file`）：

| 取值 | 行为 |
|---|---|
| `0s`（默认） | 手动模式：缓存不按时间过期，只在首次使用（缓存为空）、调用 `POST /invalidate_backends_cache`、Syncer 重启时刷新 |
| 大于 0，如 `60s` | 按该时长自动过期；`60s` 即 rc08 / rc08.1 的行为 |
| 负数 | 启动时报错退出 |

启动时打印一行 INFO，说明当前模式和时长。`bin/start_syncer.sh` 支持 `--backends_cache_ttl <值>`，前台、后台启动都会传给二进制。

### 刷新期间的手动失效不再被旧查询覆盖

原先 `UpdateBackends` 先查 FE、再写回缓存并清掉失效标记。若一次 `show backends` 已读到旧拓扑，此时调用 `POST /invalidate_backends_cache`，接口返回成功后旧查询仍会写回旧列表并清掉失效标记。默认手动模式下这份旧列表不会再按时间过期，失效就等于没做。

现在 `Meta` 增加失效代数，每次失效加一。刷新在写回前核对代数，变了就丢弃这次结果、保持失效状态并重新查询 FE，最多 3 次；仍被失效则返回错误，缓存保持无效。rc08.1 的语义（空结果不清缓存、整体替换、读路径持锁）不变。

## 行为变化

- **默认不再自动刷新 BE 列表**。从 rc08 / rc08.1 升级、不加参数时，行为从「每 60 秒重查」变为手动模式。需要保留旧行为，启动时加 `--backends_cache_ttl 60s`。
- 手动模式下会使用旧 BE 列表的，只有全量同步时 restore 的 BE 网络映射（`genExtraInfo`），以及建任务时的副本数校验和 BE binlog 特性检查。增量同步的 ingest 每个事务都会通过 FE thrift 现取 BE 列表，不读这份缓存，不受影响。
- **源端或目标端 BE 扩缩容、换 IP 后**，要在每台 Syncer 上调用一次 `POST /invalidate_backends_cache`。BE 只是重启、地址不变时不需要。
- 参数值必须带单位，例如 `60s`、`1m`；只写 `60` 会因解析失败而启动失败。
- `config_file` 里的同名配置会覆盖命令行参数（与其他参数一致）。

## 已知限制

- rc08 / rc08.1 的已知限制全部继续适用，见 [hy-ccr-3.0.6-rc08.md](hy-ccr-3.0.6-rc08.md#已知限制)。
- 失效 BE 缓存不会让已生成的全量快照重新生成。源端 BE 变更后，restore 仍会复用内存里带旧 BE 地址的快照信息，直到 Syncer 重启、快照过期或出现 Meta 类错误。处理顺序：先在每台 Syncer 调 `POST /invalidate_backends_cache`；restore 仍反复报 `restore failed or canceled` 时重启该 Syncer；仍不行再重建任务（表级任务可用 `allow_table_exists`）。
- `start_syncer.sh --daemon` 在 Syncer 启动阶段就退出时（例如 `--backends_cache_ttl -1s`、元库打不开、端口被占）脚本仍返回 0 并留下失效的 PID 文件。这是原有行为，新参数只是多了一种触发方式，见 [#20](https://github.com/HYDCP/hy-ccr-sycner/issues/20)。启动后请以日志和 `curl <host>:<port>/version` 确认进程在运行。

## 构建与验证

使用 Go 1.20（与 CI 一致）。**不要用更高版本的工具链**：依赖 `choleraehyq/pid` 的 arm64 汇编会被 Go ≥1.21 的汇编器拒绝（`expected pseudo-register; found R13`），是编译失败而非告警。SQLite 后端需要 CGO 和匹配目标平台的 C 工具链。

```sh
go build ./pkg/... ./cmd/ccr_syncer
go vet ./pkg/ccr ./pkg/service ./cmd/ccr_syncer
go test -count=1 $(go list ./... | grep -v /cmd | grep -v kitex_gen) ./cmd/ccr_syncer
go test -race -count=1 ./pkg/ccr ./pkg/service ./cmd/ccr_syncer
```

新增用例：`TestInvalidateDuringRefreshIsNotLost`、`TestStaleRefreshDoesNotOverwriteNewerOne` 在旧的写回逻辑上会失败；另有手动模式、负数校验、代数校验和重试上限的用例，以及 `cmd/ccr_syncer` 的参数解析用例。

发布二进制从 tag 的干净检出构建，用 Makefile 注入版本信息：

```sh
make ccr_syncer
bin/ccr_syncer -version    # 期望 hy-ccr-3.0.6-rc08.2:<commit-sha>
make tarball
```

## 升级与回退

- 没有元数据变更。从 rc08 / rc08.1 升级只需替换二进制并重启；滚动升级时逐台进行。
- 决定是否保留 60 秒自动刷新：保留就在启动命令或 `config_file` 里加 `backends_cache_ttl`，否则按手动模式运维，并把「BE 变更后调用 `/invalidate_backends_cache`」写进扩缩容流程。
- 回退时要把 `bin/` 下的二进制**和 `start_syncer.sh` 一起**换回 rc08.1 包里的版本：本版的 `start_syncer.sh` 每次启动都会传 `-backends_cache_ttl=<值>`（默认 `0s`），rc08.1 的二进制不认识这个参数，会报 `flag provided but not defined` 并启动失败。同样要从 `config_file` 里去掉 `backends_cache_ttl`。回退后恢复为 rc08.1 的 60 秒自动刷新。
