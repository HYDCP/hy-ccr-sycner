# hy-ccr-3.0.6-rc09

HYDCP 3.0.6 生产线的候选版本，唯一 Git tag 为 `hy-ccr-3.0.6-rc09`，打在 `release/3.0.6-rc08.1` 分支上包含本说明的提交上。发布分支沿用 `release/3.0.6-rc08.1`，不另建分支；不移动、不覆盖 `hy-ccr-3.0.6-rc08`、`hy-ccr-3.0.6-rc08.1`。

本版是平台监控诊断方案（ccr-platform `docs/requirements/monitoring-diagnostics.md` 第 9 节）的 Syncer 首批改动 S1–S3，加上一个日志脱敏修复。原则是只增加观测：

- 不改同步流程和状态机，不改元库表结构；
- 不增加对 FE 和元库的访问；
- 新增的读取都不拿 JobManager 的锁；
- 新字段都是可选的，原有字段不变。

## 代码范围

- 基线：`hy-ccr-3.0.6-rc08.1@ee7a1e94`。
- 包含：

  | 提交 | 来源 | 内容 |
  | --- | --- | --- |
  | `279e45f7` | [PR #13](https://github.com/HYDCP/hy-ccr-sycner/pull/13) S1 | `BINLOG_TOO_OLD_COMMIT_SEQ` 写 ERROR 日志并记疑似缺口标记 |
  | `81cfccac` | PR #13 S2 | `/job_status` 暴露同步错误 |
  | `3accc326` | PR #13 S3 | `GET /job_lags` 和 `feature_job_lags` 开关 |
  | `f4aaab9e` | [PR #14](https://github.com/HYDCP/hy-ccr-sycner/pull/14) | 数据源和元库密码不写进日志和错误信息 |
  | `e9c0e698`、`86bda65d` | 文档 | 补记 rc08.1 的验证结论和产物；更正 rc08 发布说明里过时的验证状态。不涉及代码 |

  PR #13 以三个独立提交合入，S1、S2、S3 各一个，代码上可以按 S3 → S2 → S1 倒序单独回滚。
- 不包含：
  - `dev`（4.0.x 线）上的任何改动。[PR #15](https://github.com/HYDCP/hy-ccr-sycner/pull/15) 是 #14 在 `dev` 上的移植，属于另一条版本线。
  - 生产补丁分支 `release/3.0.6-rc07-no-migrate` 独有的 `8338772c`（FE RPC 诊断日志）。这个分支不在 HYDCP 仓库上，位置见「升级与回退检查」。
  - `/job_detail` 屏蔽密码（[HYDCP/hy-ccr-sycner#16](https://github.com/HYDCP/hy-ccr-sycner/issues/16)）。
  - `BINLOG_TOO_OLD_COMMIT_SEQ` 自动转全量（`feat-binlog-too-old-gap` 分支）。

## 行为变化

时间字段统一为 Unix 毫秒。新增的错误字段和缺口标记都只在内存中：不写元库，不进 `/job_detail` 的 Job JSON，进程重启后清零。

### 1. `BINLOG_TOO_OLD_COMMIT_SEQ` 不再静默（S1）

源端已经回收了已同步 commit seq 之后的 binlog 时，FE 的 GetBinlog 回答 `BINLOG_TOO_OLD_COMMIT_SEQ`。读取 binlog 的三处——增量同步（`incrementalSyncInternal`）、流水线同步（`getNextBinlogs`）、取指定 binlog（`GetSpecifiedBinlog`）——原来把它放在和 OK 共用的空分支里：没有日志、没有指标，任务状态里也看不到。

现在：

- **对返回 binlog 的处理不变**：仍按 OK 继续处理 FE 返回的 binlog。本版不新增自动转全量，也不会让任务停下。
- **每次都写 ERROR 日志**，关键字 `binlog too old`，带任务名、请求的 commit seq、出现的位置（`incremental sync`、`pipeline sync`、`get specified binlog`）、FE 返回的第一条 binlog 的 commit seq 和类型，以及 FE 的错误信息。
- **任务上记疑似缺口标记**：第一次遇到的时间和当时请求的 commit seq。之后再遇到只写日志，不更新标记。
- **接口**：`/job_status` 返回 `binlog_gap_suspected`（始终输出）、`binlog_gap_first_seen_at`、`binlog_gap_commit_seq`（只在标记后输出）；`/job_lags` 返回 `binlog_gap_suspected`。
- **清除**：一次全量同步成功走完（状态离开 `TableFullSync` / `DBFullSync`）时清除，并写一条 INFO `full sync done, clear the suspected binlog gap`。部分同步（包括整表的 partial sync）不清除，偏保守。标记不持久化，进程重启后清零；ERROR 日志是持久的记录。

标记表示「上游回答过 TOO_OLD」，不等于一定丢了数据。Doris FE（`BinlogUtils.getBinlog`）回答 TOO_OLD 时只带回 binlog 集合的第一条，也就是 FE 放在每个库、表 binlog 集合开头的 DUMMY binlog。Syncer 现有逻辑（本版未改）在增量和流水线路径上遇到 DUMMY 都会转全量（`handleDummy` → `NewSnapshot`，日志 `handle dummy binlog, need full sync`），由全量补上缺口，全量完成后标记随之清除。所以，「标记置位 → 转全量 → 全量完成后清除」是这种情况下的预期过程。只有 ERROR 日志里第一条 binlog 的类型不是 DUMMY 时，缺口不会被自动补上，需要人工处理。

### 2. `/job_status` 暴露同步错误（S2）

原来任务失败时只在日志里每 3 秒留一条 WARN：没有最近错误、失败次数和最近成功时间可查。任务协程停止后（同步轮次返回的 xerror panic、`runJob` 里 recover 到的 Go panic、`Job.Run` 返回错误），任务再也不会同步，暂停/恢复也清不掉，`/job_status` 却仍显示 `running`。

现在每一轮的结果记在任务的内存里，由任务自己的小锁保护（不访问元库，不拿 JobManager 的锁）。`/job_status` 响应的 `status` 对象在原有的 `name`、`state`、`progress_state`、`commit_seq` 之外增加：

| 字段 | 何时输出 | 含义 |
| --- | --- | --- |
| `last_error` | 出过错之后 | 最近一次错误，按 UTF-8 字符边界截断到 1KB；任务恢复后保留 |
| `last_error_category` | 同上 | `handleError` 据以处理的最外层 xerror 类别：`normal`、`rpc`、`db`、`fe`、`be`、`meta`；不是 xerror 的错误（例如 recover 到的 panic）为 `unknown` |
| `last_error_at` | 同上 | 最近一次错误的时间 |
| `first_failure_at` | 正在连续失败时 | 这一串连续失败的开始时间；成功后清除 |
| `consecutive_failures` | 始终 | 上次成功以来失败的同步轮数 |
| `last_success_at` | 成功过之后 | 最近一次成功的时间 |
| `panicked` | 始终 | 任务协程已停止同步，只有重启 Syncer 进程能恢复 |
| `recent_errors` | 出过错之后 | 最多 10 条，新的在前，每条有 `at`、`category`、`message`、`count` |

示例（节选）：

```json
{
  "success": true,
  "status": {
    "name": "job_a",
    "state": "running",
    "progress_state": "TableIncrementalSync",
    "commit_seq": 626513,
    "binlog_gap_suspected": false,
    "last_error": "[normal] ...",
    "last_error_category": "normal",
    "last_error_at": 1792468795000,
    "first_failure_at": 1792468000000,
    "consecutive_failures": 31,
    "last_success_at": 1792467990000,
    "panicked": false,
    "recent_errors": [
      {"at": 1792468795000, "category": "normal", "message": "[normal] ...", "count": 31}
    ]
  }
}
```

口径：

- **一轮**是 run 循环每 3 秒一次的 `sync()`。它返回错误就记一次失败；`context.Canceled`（删除任务）不计。
- **成功**是一轮 `sync()` 没有返回错误，或者已同步的 commit seq（`PrevCommitSeq`）前进了。后者在 `updateJobStatus` 里观察，经典路径和流水线路径每提交一条 binlog 后都会调用，这样一个持续追 binlog 的长轮次不会一直显示旧的失败。因此 `last_success_at` 不代表有数据被同步：空闲任务每轮都算成功。进程启动后第一次观察只建立基线。
- 成功会把 `consecutive_failures` 清零、清除 `first_failure_at`；`last_error*` 和 `recent_errors` 保留。
- `consecutive_failures` 是轮数，不是时长：一轮失败可能持续几分钟。按「失败了多久」判断要用 `first_failure_at`。
- `recent_errors` 里，连续失败（中间没有成功）且类别和消息都相同的合并成一条：`at` 是最近一次，`count` 是次数，这样 10 条能覆盖更长的时间。
- `panicked` 在现有能发现协程停止的三个位置置位：`run()` 里 `handleError` 返回的 xerror panic、`JobManager.runJob` 的 `recover()`、`runJob` 里 `Job.Run` 返回错误。置位后一直保持到进程重启，和停止的任务本身一样。xerror panic 之后 run 循环每 3 秒只打一条 `job panic` 的 ERROR，不再计入失败轮数。
- 任务暂停期间（状态不是 running）run 循环不调用 `sync()`，这些字段保持暂停前的值。
- 同步流程不变：只在已有的位置记录已经发生的结果。

### 3. 新接口 `GET /job_lags` 与 `feature_job_lags` 开关（S3）

平台需要每个任务的 lag。`/get_lag` 一次只查一个任务，每次读两次元库、调一次 FE；而 JobCollector 本来就每 10 秒为本节点的每个任务按已持久化的 progress commit seq 算一遍同样的 lag，只是结果只写进了 Prometheus 指标。

JobCollector 的变化：

- 每轮先建好完整的快照，再用一次原子指针替换发布（每轮一个新 map，发布后不再修改，读者看不到填到一半的结果）。
- 某个任务这一轮算失败时，保留它上一次算出的值和计算时间，带上这一轮的错误。
- 不再属于本节点的任务从快照里去掉；一轮连任务列表都取不到时，保留上一份快照。
- 访问元库和 FE 的方式、次数和 Prometheus 指标都与原来相同；JobCollector 仍由 `main` 启动，没有开关。

接口：

- 路由 `/job_lags`，HTTP 200。和其他接口一样不检查 HTTP 方法，平台用 GET。
- 只返回本节点内存中的任务，按名字排序，不含已标记删除、协程还没退出的任务。不做 303 转发；多实例时要逐个节点调用。
- 只读快照和各任务的内存字段（各自的小锁），不访问元库和 FE，也不拿 JobManager 的锁（`AddJob` 会在持有这把锁时调用 FE）。任务列表来自一份写时复制的视图，JobManager 在任务集合变化时（`AddJob`、`Recover`、删除后移出）在锁内重建。

响应：

```json
{
  "success": true,
  "snapshot_at": 1792468800123,
  "collect_interval_ms": 10000,
  "jobs": [
    {
      "name": "job_a",
      "lag": 12,
      "next_binlog_timestamp_ms": 1792468790000,
      "commit_seq": 626513,
      "computed_at": 1792468800100,
      "last_error_category": "normal",
      "last_error_at": 1792468795000,
      "first_failure_at": 1792468000000,
      "consecutive_failures": 31,
      "last_success_at": 1792467990000,
      "last_error": "[normal] ...",
      "panicked": false,
      "binlog_gap_suspected": false
    }
  ]
}
```

- `snapshot_at`：最近一轮 JobCollector 结束的时间，第一轮结束前为 `null`。`collect_interval_ms` 是它的周期，10000。
- `lag`、`next_binlog_timestamp_ms`、`commit_seq`、`computed_at`：这个任务在快照里的值，还没成功算过时都为 `null`。`lag` 是待消费 binlog 条数，口径同 `/get_lag`，不是秒数。`commit_seq` 是那一轮从元库读到的已持久化进度，和 `/job_status` 的 `commit_seq`（内存值）可能不同。`next_binlog_timestamp_ms` 是最早一条未同步 binlog 的时间，FE 没有时为 `null`，不会输出 `/get_lag` 的 1970 占位值。
- `lag_error`：这个任务在最近一轮算失败的原因（前 200 个字符），没有时不输出；这时其他 lag 字段是上一次成功的值。
- 错误摘要：`last_error_category`、`last_error_at`、`first_failure_at`、`consecutive_failures`、`last_success_at`、`panicked`、`binlog_gap_suspected` 与 `/job_status` 同义，`last_error` 只取前 200 个字符。
- 没有任务时 `jobs` 为空数组。

开关：

- 新增 bool 启动参数 `feature_job_lags`，默认 true。`/features` 会列出 `{"feature":"feature_job_lags","value":true,"default":"true"}`，平台据此判断节点带 S1–S3。
- 设为 false 时，`/job_lags` 返回 HTTP 200 和 `{"success":false,"error_msg":"job_lags is disabled by feature_job_lags=false"}`，平台回退到逐任务 `/get_lag`。JobCollector 和 Prometheus 指标不受影响。
- 只在启动时读取：命令行 `-feature_job_lags=false`（bool 参数必须写成带 `=` 的形式），或 `-config_file` 配置文件里一行 `feature_job_lags=false`。没有运行时开关，改了要重启 Syncer。

### 4. 以下日志和错误不再带数据源、元库密码（#14）

#14 修的是下表这些位置，不是所有可能带密码的输出；还没覆盖的见「已知限制」。

| 位置 | 什么时候出现 | 原来 | 现在 |
| --- | --- | --- | --- |
| `/create_ccr` 的 INFO `create ccr name: ..., src: ..., dest: ...` | 每建一个任务 | src、dest 逐字段输出，含两个集群的密码 | 走 `Spec.String()`，不含密码 |
| `updateFrontends`（任务启动时）的 WARN `get src/dest frontends failed, fe: ...` | 取 FE 列表失败 | 同上 | 同上 |
| `GetMysqlDB` 的 `connect to mysql failed, dsn: ...` | 驱动解析 DSN 失败（例如 host 里有 `@`） | 完整 DSN | DSN 中的密码换成 `******` |
| `NewMysqlDB` 的 `mysql: open <user>@tcp(<host>:<port>) failed` | 元库 DSN 解析失败 | 端口的位置打印的是元库密码 | 打印端口 |
| `-config_file` 的 INFO `force set config <key>=<value>` | 用配置文件启动 | 原样打印，含 `db_password` | key 含 `password` 的项打成 `******`；改在 `flag.Set` 成功之后才打，拼错的 key（例如 `db_pasword`）不会被打出来 |
| `-config_file` 的两条启动错误（`main` 会打印） | 某行没有 `=`；`flag.Set` 失败 | 错误里带整行原文 | `invalid line <N>, it must be key=value`；`set flag <key> (line <N>): <flag 包的错误>` |

- `Spec.String()` 改成值接收者，输出内容不变（host、各端口、user、cluster、库表名和 id，不含密码）。现在 Spec 按值、按指针、作为结构体字段用 `%s`、`%v`、`%+v` 打印都会走它，前两处和其他格式化 Spec 的日志都不再带密码。仓库里唯一内嵌 Spec 的是 `Meta`（内嵌 `*base.Spec`），它的打印不受影响。
- **日志格式变化**：前两处日志里的 src/dest 从结构体逐字段输出（`{{10.0.0.1 9030 9020 false} [] root <密码> ...}`）变成 `host: 10.0.0.1, port: 9030, thrift_port: 9020, user: root, cluster: ..., database: ..., database id: ..., table: ..., table id: ...`。
- HTTP 接口和日志级别都没变。JSON 序列化不在范围内：`/job_detail` 仍返回含明文密码的完整 Job JSON（HYDCP/hy-ccr-sycner#16）。
- 只影响升级之后新写的日志。旧版本已经写下的明文密码不会因为升级消失，见「升级与回退检查」。

## 已知限制

- rc08、rc08.1 的已知限制继续适用，见 [hy-ccr-3.0.6-rc08.md](hy-ccr-3.0.6-rc08.md#已知限制)。其中「不实现 TOO_OLD 自动恢复」需要细化：FE 返回 DUMMY 时，沿用原有逻辑转全量，缺口由全量补上；第一条 binlog 不是 DUMMY 时，Syncer 从返回的 binlog 继续同步，跳过的范围不会补上，需要人工 `force_fullsync`。本版只让这两种情况都可见，不新增自动修复。
- 错误字段和缺口标记只在内存中，进程重启后清零：重启前遇到过 TOO_OLD 的任务，重启后 `binlog_gap_suspected` 为 false，要以 ERROR 日志为准；`panicked` 的任务只能靠重启恢复，重启也会清掉这个字段。
- 缺口标记只在一次全量同步成功走完时清除，部分同步不清除。
- `/job_lags` 的新鲜度取决于 JobCollector 一轮的时长。JobCollector 串行处理本节点的全部任务，本版没有改它的并发，任务多时一轮可能超过 10 秒；用 `computed_at` 判断每个任务的值有多旧。
- `/job_lags` 只返回本节点的任务，不转发。
- `/job_status` 仍按原来的方式持 JobManager 读锁，`AddJob` 持写锁调用 FE 时会被阻塞；仍对不在本节点的任务返回 303。
- `consecutive_failures` 是轮数不是时长；`last_success_at` 不代表有数据同步；暂停期间这些字段不更新。
- `/job_detail` 响应仍带源、目标集群的明文密码（HYDCP/hy-ccr-sycner#16）。
- PostgreSQL 元库的连接报错可能带密码（已有问题，#14 未覆盖）。`NewPostgresqlDB` 把密码拼进 `postgres://<user>:<password>@<host>:<port>/...`。密码里有 URL 中不能直接出现的字符（例如后面不是两位十六进制的 `%`，或者 `#`）时，lib/pq v1.10.9 在第一次建连时解析 URL 失败，原样返回 `net/url` 的错误 `parse "<URL>": ...`，其中带着这个 URL（含密码或密码的前一部分）。这个错误经 `postgresql: create schema ... failed` 包装后，由 `main` 以 `new meta db error` 写进日志，进程退出。和 #14 修掉的 MySQL DSN 两处一样，只在解析失败时出现。

## 构建与验证

与 CI 一致使用 Go 1.20。**不要用更高版本的工具链**：依赖 `choleraehyq/pid` 的 arm64 汇编会被 Go ≥1.21 的汇编器拒绝（`expected pseudo-register; found R13`），是编译失败而不是告警。SQLite 后端需要 CGO 和匹配目标平台的 C 工具链。

```sh
export GOTOOLCHAIN=local
# 仅 macOS 本机需要，处理本机 C 头文件告警：
# export CGO_CFLAGS='-O2 -g -Wno-nullability-completeness'
go build ./pkg/... ./cmd/ccr_syncer
go vet ./pkg/ccr ./pkg/service ./cmd/ccr_syncer
gofmt -l $(git diff --name-only hy-ccr-3.0.6-rc08.1 hy-ccr-3.0.6-rc09 -- '*.go')   # 期望无输出
go test -count=1 $(go list ./... | grep -v /cmd | grep -v kitex_gen) ./cmd/ccr_syncer
go test -race -count=1 ./pkg/ccr ./pkg/service ./cmd/ccr_syncer
```

- `./cmd/ccr_syncer` 要显式带上，JobCollector 的测试在这个包里。
- `pkg/ccr/job_manager_norace_test.go`、`pkg/ccr/job_lags_norace_test.go` 带 `//go:build !race`，`-race` 下不编译：它们真实调用 `Job.Run`，而 `gls.GoID` 的指针运算在 race 检测器的 checkptr 检查下会直接崩溃，这是基线就有的问题。要在 `-race` 下跑这两个文件，可以加 `-gcflags=all=-d=checkptr=0`。

发布二进制从 tag 的干净检出构建，用 Makefile 注入版本信息，不要用裸 `go build` 的产物：

```sh
make ccr_syncer             # Makefile 调用 PATH 里的 go，先确认 go version 是 1.20
bin/ccr_syncer -version     # 期望 hy-ccr-3.0.6-rc09:<commit-sha>
make tarball
```

Makefile 的版本取自 `git describe --abbrev=0 --always --dirty --tags`，只输出最近的 tag 名。所以在没打 tag 的提交上构建时：

- `-version` 输出的是上一个 tag 名加当前提交，例如 `hy-ccr-3.0.6-rc08.1:<sha>`；冒号后的 sha 不是 `ee7a1e9` 的二进制都不是 rc08.1。
- `make tarball` 的包名 `ccr-syncer-<tag>-<platform>.tar.xz` 也取这个 tag 名，会打出 `ccr-syncer-hy-ccr-3.0.6-rc08.1-<platform>.tar.xz`，和 rc08.1 的正式包同名，容易混放。

因此发布包只能从 `hy-ccr-3.0.6-rc09` tag 的干净检出构建。

真实 Doris 双集群上的验证项（做法）：

1. 主链路：建 job → 全量 → 增量 → 暂停 → 恢复 → 删除 → 同名重建；rc08、rc08.1 的回归项照旧（`/get_lag` 故障注入、`/invalidate_backends_cache` 校验、`/node_info`）。
2. `/features` 含 `feature_job_lags`。启动 10 秒以上后，`/job_lags` 的 `snapshot_at` 有值，任务按名字排序，正常任务的 lag 字段都有值。
3. 让一个任务持续失败（例如删除它的源库）：`/job_lags` 里它的 `lag` 为 `null`（或保留旧值）并带 `lag_error`，`consecutive_failures` 随每 3 秒的重试递增，带 `first_failure_at`；`/job_status` 的 `recent_errors` 把重复错误合并成一条并计数。恢复后 `consecutive_failures` 归零，`last_error` 保留。
4. 以 `-feature_job_lags=false` 重启：`/features` 里该项为 false，`/job_lags` 返回 `success:false`，平台回退到 `/get_lag`。改回后重启恢复。
5. TOO_OLD：暂停一个任务，把源端对应库或表的 `binlog.ttl_seconds` 调小，写入数据，等 FE 回收 binlog 后恢复任务。期望日志有 `binlog too old`（看同一行的 type）、`/job_status` 的 `binlog_gap_suspected` 为 true 并带时间和 commit seq；type 为 DUMMY 时随后转全量，全量完成后标记清除，并有 `clear the suspected binlog gap` 日志。平台 v1.3.0-rc.4 上，这时异常中心出现的是「非平台发起的全量」，缺口标记只在任务诊断卡片里看得到。
6. 脱敏：只查升级之后新写的日志（按升级时间截取，或者在新的日志目录里验证）。用带密码的数据源建任务，在这段日志里搜这个密码，期望没有结果；`create ccr name` 一行的 src/dest 是 `host: ...` 格式。用 `-config_file` 启动时，`force set config db_password=******`。旧版本写下的历史日志和备份仍然带明文密码，不能拿来判断，要单独处理（见「升级与回退检查」）。
7. 平台 v1.3.0-rc.4：平台日志里该节点不再出现 `does not serve /job_lags`；打开 `monitoring.features.diagnostics` 后，链路详情的观测来源为 `/job_lags`，任务诊断卡片显示最近错误。

`panicked` 的三种置位情况在真实环境里不易触发，由单元测试覆盖。

## 升级与回退检查

本版没有元库表结构变更，原有启动参数不变，新增的 `feature_job_lags` 有默认值。升级后旧的请求和旧的配置都能继续使用，没有必须在升级前完成的迁移。两条升级路径都要事先知道以下几点：

- 按位置解析 `create ccr name` 和 `get src/dest frontends failed` 两行日志的脚本要跟着改（见行为变化 4）。
- 按 ERROR 条数告警的，升级后可能多出 `binlog too old`；原来同样的情况不留日志。
- **升级不会清掉历史日志里的明文密码。** 旧版本写下的 `create ccr name` 行带着两个集群的密码；用 `-config_file` 启动时，`force set config db_password=...` 也带着元库密码。`start_syncer.sh` 以追加方式写 `log/<host>_<port>.log`，lumberjack 滚动出的备份按保留天数和个数留存，原地升级后这些文件都还在。是否清理这些日志、是否轮换密码，由负责人决定。
- **多节点时，任务会在节点之间迁移。** 一台 Syncer 停止心跳超过约 12 秒（`pkg/ccr/checker.go` 的 `CHECK_TIMEOUT` = 2 × 5 秒 + 2 秒），其他存活的节点就会接管它的任务（`RebalanceLoadFromDeadSyncers`），这台节点重启后任务不会迁回。逐台替换时，任务会在新旧版本的节点之间移动。两边的元数据格式相同，不影响正确性；但按单个节点对比升级前后（例如 `/node_info` 的任务数）会对不上，要按全部节点的 job 清单核对（CCR 元库 `jobs` 表，按 `belong_to` 分组）。
- **回退前先去掉 `feature_job_lags`。** 如果在本版上用过 `-feature_job_lags=...`，或者在 `-config_file` 里写了这一行，旧二进制都不认识这个参数，启动会失败：命令行方式报 `flag provided but not defined`；配置文件方式在 `parseConfigFile` 的 `flag.Set` 处出错，`main` 以 `parse config file error` 退出。

### 从 `hy-ccr-3.0.6-rc08.1` 升级

1. 记录旧版本、启动参数、全部节点的 job 清单和 progress；保留旧二进制和启动脚本。
2. 替换二进制并重启。多实例时每个 Syncer 节点都要换，平台按节点分别判断。
3. 重启后：`/version` 为 `hy-ccr-3.0.6-rc09:<sha>`；`/features` 含 `feature_job_lags`；第一轮 JobCollector 结束前（约 10 秒）`/job_lags` 的 `snapshot_at` 为 `null`、各任务 lag 字段为 `null`；`/job_status` 带新字段，错误字段从零开始；`/get_lag` 与升级前一致。
4. 回退：停止新进程，去掉 `feature_job_lags`（如果加过），换回 rc08.1 的二进制和原参数启动。没有元库或配置需要还原；`/job_lags` 和新字段随之消失，平台 v1.3.0-rc.4 会自动回退到 `/get_lag`。

### 从生产补丁分支 `release/3.0.6-rc07-no-migrate`（`8338772c`）升级

这个分支不在 HYDCP 仓库上：

- 它在 fork Ryan19929/ccr-syncer 上叫 `3.0.6-rc07-node-info`，head `8338772c`。`release/3.0.6-rc07-no-migrate` 是维护者本地检出的分支名，平台文档和运维记录里用的是这个名字。
- 它从 `69184ae1`（加入 `/node_info`）分出，只多一个提交 `8338772c`。`69184ae1` 带 tag `3.0.6-rc07-node-info-add`，所以按 Makefile 构建的补丁分支，`/version` 为 `3.0.6-rc07-node-info-add:8338772`。升级前先用 `curl <syncer>:9190/version` 核对现网是这个值。

升级到本版会同时带上 rc08、rc08.1 的全部改动，它们各自的行为变化都适用，先读 [hy-ccr-3.0.6-rc08.md](hy-ccr-3.0.6-rc08.md#行为变化) 和 [hy-ccr-3.0.6-rc08.1.md](hy-ccr-3.0.6-rc08.1.md#行为变化)。要点：

- rc08：
  - FE GetBinlogLag 返回非 OK 时，`/get_lag` 仍是 HTTP 200，但返回 `success:false` 和 `error_msg`，不再是 `success:true`、`lag:0`（例如源库被删除时）。依赖 `/get_lag` 的调用方要检查 `success`；平台上这类任务从「已追平」变成采集失败。
  - `/view` 任务列表的两个 lag 列在 lag 不可用时显示 `-1`。
  - `ccr_job_running_lag_*` 指标在出错期间保留旧值，同步状态指标独立刷新。
  - BE 列表缓存加 60 秒 TTL，目标端 BE 扩缩容后不用再重启 Syncer；新增 `POST /invalidate_backends_cache`（只接受 POST，非法请求返回 400/405）；建 job 时 `replication_num` 的校验随之调整（上游 selectdb/ccr-syncer#651）。
- rc08.1：BE 缓存整体替换，消除重建时的 `concurrent map read and map write`；FE 返回空 BE 列表时报错并保留旧缓存，建 job 等路径会明确失败（`no backends returned by FE`），不再拿空列表继续。
- 补丁分支独有的 `8338772c`（FE RPC 诊断日志）不在本版内。升级后这些日志不再出现：INFO `FeRpc init start` / `FeRpc init done`、`FE RPC NOT_MASTER redirect target`、`update frontends done`，WARN `FE RPC returned NOT_MASTER`、`FE RPC fallback to cached FE clients`、`FE RPC switch to next cached FE`、`FE RPC exhausted cached FE clients`；INFO `RpcFactory: created FeRpc` 消失，`RpcFactory: reused cached FeRpc ... (cache hit)`、`RpcFactory: creating new FeRpc ... (cache miss)` 回到 TRACE，src/dest frontends 列表回到 DEBUG，WARN `FeRpc init cached FE client failed` 改回 `new fe client error`。依赖这些日志的排查脚本或告警要调整。
- 本版的四项行为变化（S1、S2、S3、#14）同样适用。
- 两边的元库表结构和 progress 格式相同，原有启动参数相同；从 `69184ae1` 到本版新增的启动参数只有 `feature_job_lags`。

步骤：

1. 用 `/version` 确认现网版本；记录启动参数、全部节点的 job 清单和 progress；备份配置和元数据。SQLite 文件备份前停止对应进程，共享 MySQL/PostgreSQL 用一致性备份。
2. 保留旧二进制和启动脚本。先在测试环境演练：现有 job 能继续增量同步，能正常完成全量到增量的转换。
3. 检查 `/version`、`/node_info`、`/features`、`/get_lag`、`/job_lags`、`/job_status`、`/view` 和 Prometheus 指标；测试 `/invalidate_backends_cache` 的正常请求和非法请求的 400/405。
4. 确认没有两个进程使用同一节点身份、或同时写同一个 SQLite 文件，再替换生产二进制，观察任务进度。
5. 回退：停止新进程，去掉 `feature_job_lags`（如果加过），用保留的 `8338772c` 二进制和原参数启动。不要用旧备份覆盖仍在使用的共享元数据库；元数据恢复要单独评估，避免任务进度倒退。回退后 rc08 起修复的问题（`/get_lag` 的假 0、BE 列表不刷新）重新出现。

## 与 ccr-platform 的兼容

平台按 Syncer 的实际能力降级，两边的升级顺序不影响可用性。平台侧的契约见 ccr-platform 的 `docs/contracts/syncer-api.md`（`GET /job_lags`、`POST /job_status`、`GET /features`），与本版一致。

- **v1.3.0-rc.4**：
  - 采集器对每个节点先调 `GET /job_lags`；返回 404，或返回 `success:false` 且错误信息含 `feature_job_lags` 时，把节点视为不支持，回退到逐任务的分级 `/get_lag`，10 分钟后再试。所以 Syncer 升级后，平台不用重启，最多约 10 分钟切到 `/job_lags`。平台用每个任务的 `computed_at` 作为观测时间，响应体上限 16 MiB。
  - 「持续报错」「任务已 panic 停止」「可能有数据缺口」三种错误类异常的判定都只依据 `/job_lags` 的错误摘要，Syncer 不支持或关闭了 `feature_job_lags` 时都不判定。rc.4 只启用了其中的「持续报错」（`task_erroring`）。
  - 「任务已 panic 停止」（`task_panicked`）和「可能有数据缺口」（`binlog_gap_suspected`）的判定已经合入平台，但在 rc.4 上没有启用，异常中心不会出现。在 rc.4 上，`panicked` 和 `binlog_gap_suspected` 只在任务诊断卡片的「最近错误」里显示。
  - TOO_OLD 之后 Syncer 自己转全量时，rc.4 报的是「非平台发起的全量」（`fullsync_auto`），证据里带 Syncer 给出的原因 `handle dummy binlog, need full sync`。
  - 任务诊断卡片（`monitoring.features.diagnostics`，默认关闭）先看节点 `/features` 有没有 `feature_job_lags`：有就调 `/job_status` 显示最近错误；没有（例如生产补丁分支）就显示「当前 Syncer 版本不支持」，不调 `/job_status`。
- **v1.2.0-rc.9 及更早**：不调用 `/job_lags`，也不读新字段。新字段和 `/features` 的新项都是追加，不影响这些版本。
