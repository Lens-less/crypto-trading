# Project Refocus Acceptance — 2026-08-12

## 结论

仓库内改造与可离线复现的工程门禁通过。项目现在以“先证明 Edge、只扩
Testnet 实时通道、mainnet 持续关闭”为主线，不再把流程或安全加固本身误写
成盈利能力。

本报告不把两个外部前置条件伪装成已完成：

1. 当前环境没有 `BINANCE_API_KEY` / `BINANCE_API_SECRET`，因此没有运行带
   私有 User Data Stream 的真实 24 小时 Testnet soak，也没有生成真实
   kill/restart 运行证据。
2. 没有配置 Python legacy 的外部归档仓库，因此只完成了 366 文件的
   SHA-256 迁移清单和导出门禁，没有删除主仓库中的可恢复源树。

这两项都不授予 mainnet 权限，也不阻塞本次代码库改造合入；它们继续作为
生产候选的外部门禁。

## Operator 决策

| 决策 | 本次采用值 | 结果 |
| --- | --- | --- |
| D1 | 受控真实收益 | Edge gate 优先，工程能力不得代替盈利证据 |
| D2 | 否：近期不做无策略手动 mainnet | Live V1 automation 保留但休眠；mainnet adapter/二进制拆分继续延期 |
| D3 | 频率 → 资产 → 家族 | 首轮为 BTCUSDT 1h 冻结协议；数据准入失败后不改变经济参数 |
| D4 | 收缩 CI | 前端 bundle 一次构建；Rust 保留 Ubuntu MSRV/stable 与 Windows MSRV |
| D5 | 先封存再外迁 | 没有外部目标时不删除 Python archive；逐文件 manifest 已验证 |

## W0 — 提交卫生

- `574a4ae`：先固化已经通过门禁的安全与研究成果。
- `855d0fd`：在下载/评估前提交 1h 预注册协议。
- `ac7c7ae`：固化 1h 数据准入终止证据；未打开 selection 或 holdout。
- `.workflow/` 已忽略，编排 dump 不再与产品源树竞争权威。

## W1 — 研究与执行共用策略

- `strategy` 提供唯一新增抽象 `BarStrategy`，以及共享的 cash、buy-and-hold、
  momentum、Donchian、vol-target 实现。
- `backtest::SpotBar` 直接别名到共享 `Bar`，候选评估不再重建另一套 bar 或
  复制策略算法。
- `PaperBarTask` 消费同一策略对象并把目标敞口变化转换为 Paper rebalance
  方向；合约测试逐 bar 对比共享策略输出。
- 正式入口 `crypto-trading-research` 只接受内置冻结协议。调用者不能注入
  provenance lock hash，已终止的 W1 v1 在任何缓存、lock 或 holdout I/O 前
  硬失败。

### 1h 冻结实验的终局

- 官方 103 个月档 checksum 全部通过，但只有 75,096 条原始 1h 行，理想
  UTC 日历应有 75,216 槽。
- 43 行偏离 UTC 小时网格，合计缺少 163 个 canonical 小时；24 个月不满足
  完整小时形状。
- 最后异常延续到 2023-03，连续后缀不足冻结的 9 个 walk-forward 窗口。
- Holdout（2025-08 至 2026-07）自身完整 8,760 小时，但未解析价格、未运行
  selection、未打开 holdout。
- 协议结论是 `data-admission-aborted`，不是策略失败或盈利通过。若启动 v2，
  必须先另行冻结“79 个 1h 月档 + 24 个整月 1m 聚合、Observed/Missing 时间
  语义”的新协议，不能在 v1 后验补洞。

## W2 — Testnet 实时通道

- `monitor --live` 默认连接 Binance Spot Testnet
  `wss://stream.testnet.binance.vision/ws/<symbol>@bookTicker`；REST 只在显式
  `--live-transport polling` 时作为降级路径。
- 签名私有流使用独立的
  `wss://ws-api.testnet.binance.vision/ws-api/v3` 和
  `userDataStream.subscribe.signature`，没有混用 market-stream host。
- 传输层具备有界广播、ping/pong、指数退避与生产 jitter、连接 generation、
  update-ID 回退、lag/gap/close/expiry 分类；重连前后的 observation 可区分。
- `ContinuousTestnetOwner` 持有单写 lease，stream gap/restart/expiry/regression
  先写 durable `recovery_required`，再恢复已计划订单的 exact client ID，随后
  双次 REST reconcile；新订阅 ACK 前保持不可运行。Kill switch 先持久化且
  闩锁不可逆。
- 24h verifier 只接受 `market_stream`、`user_data_stream`、
  `authenticated_reconcile` 三条路径、真实 active duration、unclean restart 与
  clean stop；旧 REST-only 记录无法通过。
- 2026-08-12 已完成无凭证公网实连：BTCUSDT Testnet bookTicker WebSocket
  返回正 bid/ask 与 update ID。私有流和 24h 外部 soak 未运行，原因见结论。

## W3 — 减法

- capability adapter 从逐 venue 的 config-only/legacy-only 行收缩为 Binance、
  Hyperliquid、Paper 与一个 `unsupported-venues` 汇总行；兼容配置移到
  `rust/config/legacy/exchanges/`。
- volume maker、price alert、scanner 标为维护冻结；保留既有证据路径，不再
  扩 venue/automation 范围。
- 完成的 G 系列 board/runbook/handoffs 原样迁入
  `docs/internal/history/g-series-2026-08-12/`；旧路径仅留 redirect。唯一未归档
  的 Live V1 树在 D2=`no` 下休眠，不能认领 issue。
- CI 仅有一次 `pnpm build`；前端 bundle 通过 artifact 供 Rust 和 E2E job
  使用；删除 Windows stable 重复矩阵单元，Markdown/archive 变更不触发全量。
- 删除未引用 `PlaceholderPage`、设计预览和 backtest SHA-256 副本；HMAC 与
  backtest digest 共用 domain SHA-256 内核。
- Python archive manifest 重放结果：366 文件、6,861,100 字节、逐项大小与
  SHA-256 全部匹配。外迁步骤见
  `archive/python-legacy/packaging/MIGRATION-GATE-2026-08-12.md`。

## 验收矩阵

| 条目 | 状态 | 证据 |
| --- | --- | --- |
| AC-R1 共享策略实现 | PASS | backtest 与 `PaperBarTask` 共用类型和实现；共享/owner 合约全绿 |
| AC-R2 WS 与 fail-closed | PASS | 依赖、默认 CLI 路由、generation/gap/reconnect/expiry 合约及公网 Testnet 实连 |
| AC-R3 连续 owner 与 24h soak | PASS（代码与离线 verifier）/ EXTERNAL GATE（真实 24h） | query-first/kill/restart 合约全绿；无凭证，未伪造 24h 运行 |
| AC-R4 capability 收缩 | PASS | 4 个 adapter 行；fixture、CLI、HTTP、runtime capability 合约一致 |
| AC-R5 CI 去重 | PASS | workflow YAML 可解析；全仓 workflow 仅 1 个 `pnpm build`；docs/archive paths-ignore |
| AC-R6 单一 automation 权威 | PASS（内容） | G 系列归档；只有 1 棵未归档且明确 dormant 的树；最终 commit 后复核 clean status |
| AC-R7 预注册与结论存档 | PASS | 预注册先提交；v1 数据准入负结果单独提交；holdout 未打开 |

## 可复现验证

以下命令均在 Windows、Rust 1.89.0、锁文件模式下于 2026-08-12 通过：

```text
cargo +1.89.0 fmt --all -- --check
cargo +1.89.0 check --workspace --all-targets --all-features --locked
cargo +1.89.0 clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +1.89.0 test --workspace --all-targets --all-features --locked --quiet
cargo +1.89.0 test --doc --workspace --all-features --locked --quiet
RUSTDOCFLAGS=-D warnings cargo +1.89.0 doc --no-deps --workspace --all-features --locked
cargo +1.89.0 build --release --workspace --all-features --locked

corepack pnpm install --frozen-lockfile
corepack pnpm typecheck
corepack pnpm lint
corepack pnpm test -- --run        # 22 files / 235 tests
corepack pnpm build
corepack pnpm e2e                 # 6 browser tests against embedded binary

cargo audit --file Cargo.lock
cargo deny check bans licenses sources
node scripts/check-lockfile-registry.mjs
pnpm audit --prod --audit-level=high
pnpm licenses list --json | node scripts/check-licenses.mjs
```

## AC-R3 implementation clarification (post-review)

The production `testnet-soak serve` path now constructs and drives
`ContinuousTestnetOwner`; user-data items are projected by that owner and an
`authenticated_reconcile` sample is emitted only after the same owner proves
two stable authoritative REST snapshots.

The lifecycle group is all-or-none. A fresh exact campaign requires the
existing Testnet acknowledgement and cannot submit until a fresh private-stream
subscription ACK. A restart supplies the same exact intent without new submit
authority: only a pending durable campaign is accepted and its UUID client ID
is queried first. Fresh recovery, completed/failed campaigns, and conflicting
intent fail closed before remote I/O.

The offline verifier requires a same-task
`continuous_testnet_campaign_recovery_verified` fact with `query_first=true`, a
valid UUID, and a positive, arithmetically consistent query delta immediately
paired with the unclean restart. Read-only owner operation does not satisfy this
gate. No credentialed external 24-hour run was performed; fixed-clock tests
prove only the verifier contract, so the real run remains an external gate.
The owner-backed evidence schema is v2; legacy read-only-soak v1 journals are
rejected instead of being silently promoted into AC-R3 evidence.

附加卫生结果：workflow YAML 解析通过，`git diff --check` 通过，未发现 merge
markers，工作树（排除冻结 Python archive、build 与 dependency 目录）的高置信
secret pattern 命中文件数为 0。`cargo deny` 报告的 duplicate crate 与未遇到的
allowlist license 是 warning，bans/licenses/sources 三项结果均为 `ok`。

## 仍然关闭的门

- Edge gate：关闭；没有通过 holdout 的策略。
- mainnet：关闭；capability manifest 的 live/mainnet order authority 仍为
  `Unavailable`。
- 私有 Testnet 24h：等待由 operator 在受控主机注入凭证并按 runbook 执行。
- Python 外部拆库：等待明确的目标仓库和导出后逐哈希验收。
