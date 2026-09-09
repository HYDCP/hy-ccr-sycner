# hy-ccr-3.0.6-rc08.1

`hy-ccr-3.0.6-rc08` 的补丁版本，唯一 Git tag 为 `hy-ccr-3.0.6-rc08.1`。不移动、不覆盖 `hy-ccr-3.0.6-rc08`：该 tag 已经发布并被现网二进制引用（`/version` 返回 `hy-ccr-3.0.6-rc08:7e84bef`），移动后同名版本会对应两份不同的代码。

## 代码范围

- 基线：`hy-ccr-3.0.6-rc08@7e84bef8`。
- 新增 [PR #7](https://github.com/HYDCP/hy-ccr-sycner/pull/7)，即 dev 上 [PR #6](https://github.com/HYDCP/hy-ccr-sycner/pull/6) 的 backport（`cherry-pick -x 66603d0e`）。两条线上 `pkg/ccr/meta.go`、`pkg/ccr/meta_test.go` 逐字节一致。
- rc08 的其余内容不变，见 [hy-ccr-3.0.6-rc08.md](hy-ccr-3.0.6-rc08.md)。

## 修复内容

BE 缓存重建的两个问题，都是 rc08 引入 60 秒 TTL 之后才出现的窗口（此前两个 map 只填充一次、不会被替换）。

1. **缓存重建期间的无锁读**。`UpdateBackends` 先把新 map 赋给 `Meta.Backends` / `Meta.BackendHostPort2IdMap`，再逐条填充，全程持写锁；而 `GetBackendId` 读 `BackendHostPort2IdMap` 完全不持锁。读者在填充窗口内拿到新 map 就会触发 `concurrent map read and map write` —— 这是 fatal error，`recover` 接不住，进程直接退出。现在两个 map 在锁外构建完成后一次性替换，`GetBackendId` 通过持读锁的 helper 查询。仓库内目前没有 `Meta.GetBackendId` 的活调用者，这是此前没有触发的唯一原因。
2. **空结果污染缓存**。清空发生在判空之前，一次成功但为空的 `show backends` 会把缓存清成 0 条，导致 `isBackendsCacheValid` 的 `hasData` 恒为 false（每次调用都打 FE）、新建 job 报 `replication N exceeds available BE 0`、`genExtraInfo` 可能把不完整的 BE network map 交给 restore。现在空结果按刷新失败上报并保留上一轮缓存。

顺带把 `GetBackendMap` 改为返回快照，避免调用方持有或修改会被下次刷新替换的内部 map。

## 行为变化

- `UpdateBackends` 在 FE 返回空 BE 列表时返回错误并保留旧缓存。调用方（`GetBackends`、`GetBackendMap`、`GetBackendId`）会把该错误上抛，因此建 job、全量同步取 `ExtraInfo` 等路径会失败并给出明确原因，而不是拿着空列表继续。
- BE 全量下线的集群会一直保留最后一次已知的 BE 列表，直到 FE 再次返回至少一个 BE。实际不会出现，但契约上需要知悉。
- `GetBackendMap` 返回的是浅拷贝：map 本身是新的，`*base.Backend` 指针仍与缓存共享，调用方需按只读对待（`GetBackends` 则逐个复制结构体）。
- TTL、失效语义和 `/invalidate_backends_cache` 的行为均未改变。

## 已知限制

rc08 的已知限制全部继续适用，见 [hy-ccr-3.0.6-rc08.md](hy-ccr-3.0.6-rc08.md#已知限制)。本版不改变其中任何一条。

## 构建与验证

与 CI 一致使用 Go 1.20（本次为 1.20.14）。**不要用更高版本的工具链**：依赖 `choleraehyq/pid` 的 arm64 汇编会被 Go ≥1.21 的汇编器拒绝（`expected pseudo-register; found R13`），是编译失败而非告警。SQLite 后端需要 CGO 和匹配目标平台的 C 工具链。

2026-09-09 在 macOS ARM64 / Go 1.20.14 上完成，全部通过：

```sh
go build ./pkg/... ./cmd/ccr_syncer
go vet ./pkg/ccr ./pkg/service ./cmd/ccr_syncer
gofmt -l pkg/ccr/meta.go pkg/ccr/meta_test.go
go test -count=1 $(go list ./... | grep -v /cmd | grep -v kitex_gen) ./cmd/ccr_syncer
go test -race -count=1 ./pkg/ccr ./pkg/service ./cmd/ccr_syncer
```

新增的并发用例在修复前的代码上跑 `-race` 会稳定报 DATA RACE 并失败，用来确认它确实覆盖了目标问题。

2026-09-09 已在 192.168.9.44 完成真实 Doris/CCR 双集群验证（Doris `apache-doris-branch-3.1-hydcp-1.2-rc02`，各 1 FE + 1 BE，syncer 以 MySQL 为元数据），全部通过，明细见 [HYDCP/hy-ccr-sycner#8](https://github.com/HYDCP/hy-ccr-sycner/issues/8)：

- 主链路：建 job → 全量 → 增量 → 暂停 → 恢复 → 删除 → 同名重建。
- 本次改动路径：`validateReplicaFail` 双向（`replication_num=1` 正常、`=2` 正确报 `exceeds available BE 1`）；`genExtraInfo` 经 BE 扩缩容 + `/invalidate_backends_cache` + force_fullsync 验证，缓存重建日志 `1 -> 2` / `2 -> 1`。
- 空 backends 保护：DROPP 唯一 BE 后建 job 报 `no backends returned by FE`，BE 恢复后可建；保留旧缓存的语义由单测覆盖。
- rc08 回归：`/get_lag` 故障注入、Prometheus 指标保留、`/invalidate_backends_cache` 校验矩阵、`/node_info`、迁移路由 404，全部通过。

安装包：Linux x64 tarball 及 SHA256 已随 [GitHub Release](https://github.com/HYDCP/hy-ccr-sycner/releases/tag/hy-ccr-3.0.6-rc08.1) 发布（Go 1.20.12 从该 tag 干净检出构建，版本注入复核为 `hy-ccr-3.0.6-rc08.1:ee7a1e9`）。ARM64 未产出：构建机交叉工具链缺 sysroot，需在 ARM64 机器上用 Go 1.20 原生构建。

发布二进制时从该 tag 的干净检出构建，用 Makefile 注入版本信息：

```sh
make ccr_syncer
bin/ccr_syncer -version    # 期望 hy-ccr-3.0.6-rc08.1:<commit-sha>
make tarball
```

## 升级与回退

升级步骤与 rc08 相同，见 [hy-ccr-3.0.6-rc08.md](hy-ccr-3.0.6-rc08.md#升级与回退检查)。本版没有元数据或配置变更，从 rc08 升级只需替换二进制并重启；回退同样只需换回 rc08 的二进制。
