# crypto-trading Rust

这是仓库唯一的当前运行项目。Rust 源码、当前配置、构建输出和运行数据都位于本目录。旧 Python 项目已于 2026-08-13 从工作树移除（见 [`../archive/README.md`](../archive/README.md)），运行时从未依赖它。

> [!WARNING]
> **这不是无人值守的实盘系统。** 具备外部下单权限的路径只有两条：Binance **Testnet** 的
> `testnet-lifecycle`（精确确认短语），以及 Binance Spot **MAINNET** 的一次性
> `live-lifecycle`（精确确认短语 `I AUTHORIZE BINANCE MAINNET SPOT ORDER LIFECYCLE` +
> 专用 mainnet trade 凭证 + 必填 `--max-notional` 名义上限）。自动策略 live 执行
> （`--live`／`ExecutionMode::Live`）对所有策略失败关闭：没有策略通过晋升门禁，真实账户
> 风控真值（equity、margin、持仓）和多腿故障补偿仍未达到开放门槛。Paper 只计完全成交同步
> taker 回执采用的配置手续费，不代表交易所真实费率，也不包含资金费率、滑点、撮合队列优先级
> 或跨进程持仓。本项目按原样提供，不含任何担保，也不构成投资建议。
> 项目定位、安装与部署见[仓库根 README](../README.md)。

## 能力矩阵

| 命令 | 演进状态 | 配置检查 | Paper 单次执行 | 连续运行 | Live 执行 | 当前行为 |
| --- | --- | --- | --- | --- | --- | --- |
| `capabilities [--json]` | 活跃 | 不适用 | 不适用 | 不适用 | 见 manifest | 输出版本化 capability manifest（schema 4，`release_stage: live-manual`，`live_trading_enabled: true`）与 adapter 支持矩阵；唯一可用的 mainnet 下单权限是 `runtime.live-lifecycle`，自动策略 live（`runtime.live`）仍为 `unavailable` |
| `config-check` | 活跃 | 是 | 不适用 | 不适用 | 不适用 | 在 512 条摘要和 1 MiB 输出预算内聚合检查；public path loaders、public `from_str` loaders 和 shared raw reader 共享 1 MiB / YAML 读入护栏；任一路径不受支持或预算耗尽时非零退出，并用终止错误明确标出未检查的剩余路径 |
| `testnet-smoke` | 活跃 | 不适用 | 否 | 否 | 否 | 显式选择后执行 Binance Testnet Spot/USD-M 只读行情和鉴权对账探针；不会提交或撤销订单 |
| `testnet-lifecycle` | 活跃 | 不适用 | 否 | 否 | 否 | 精确确认短语授权的 Binance Testnet submit-query-cancel owner；UUID 与 intent 先写 journal，恢复时 query-first；该命令自身没有 mainnet 开关，也从不读取 mainnet 凭证 |
| `testnet-reconcile` | 活跃 | 不适用 | 否 | 否 | 否 | 默认只报告的 clean-account gate；将签名 Testnet 余额/挂单/持仓与 exact committed Paper reservation 比对，精确确认后才写 release/failure transition |
| `testnet-soak` | 活跃 | 不适用 | 否 | 是（只读） | 否 | journal-backed `serve/status/stop/verify` host；24 小时门禁要求三类探针覆盖、一次强制终止恢复演练和干净停止 |
| `live-reconcile` | 活跃 | 不适用 | 否 | 否 | 只读 mainnet | 用 `BINANCE_MAINNET_READ_API_KEY/SECRET` 经权限类型化 read endpoints 输出 Binance Spot MAINNET 余额、挂单与可选 exchangeInfo 报告；只能构造 read-authority 适配器类型，submit/cancel 在类型层面不存在 |
| `live-lifecycle` | 活跃 | 不适用 | 否 | 否 | **一次性 mainnet LIMIT** | 精确短语 + 必填 `--max-notional` 授权的一次 Binance Spot MAINNET submit→query→cancel；journal-first、提交前 venue-truth 准入（filters / spot-no-short / 外来挂单默认拒绝）、query-first 恢复、绝不盲目重提；不安全终态闩锁 journal 级 kill switch |
| `grid` | 活跃 | 是 | 挂单模拟 | 否 | 否 | 只有同时提供 `--once --price` 才生成并提交 resting paper orders；无执行参数时仅检查配置 |
| `arbitrage` | 活跃 | 是 | 是 | 否 | 否 | 单次执行要求显式价格与盘口深度，并校验启用开关、正的 `max_position_value`、监控白名单和 `symbol_configs` 策略键；`strategy_key` 是配置选择器，可与腿 symbol 不同 |
| `paper grid/arbitrage` | 活跃 | 不适用 | 否 | 是（Paper） | 否 | 通过 loopback trusted-submit 服务启动、查询、停止或取消严格匹配的 replay-backed owner；状态只来自 journal/read model；Arbitrage owner 可选 `history_decision` 历史决策模式：以 spread-history journal 回填的自然价差（中位数）门控开仓，样本不足失败关闭、不下单，资金费率缺失时判定降级（`funding_degraded`）；两个 owner 的开仓在建立 reservation 前都要先通过账户级风控权威（单币种/全局敞口上限、总余额告警/强平线、UTC 午夜重置的当日次数上限、禁用/高风险名单、暂停位与闩锁 kill switch），拒绝写成 `account_risk_rejected` 事实并跳过该次开仓 |
| `paper risk` | 活跃 | `--enable-paper-writes` 时必须提供 `--paper-account-risk-config` 共享限额 | 不适用 | 是（Paper） | 否 | `pause`/`resume`/`kill-switch` 经同一 loopback trusted-submit 服务写入持久事实；kill switch 需要专属 `account_kill_switch_armed` 风险确认与 CLI 精确确认短语，且闩锁不可解除 |
| `monitor` | 活跃 | 是 | 否 | 是（只读 replay / `--live`） | 否 | `serve/status/stop` 运行精确双源 replay monitor owner；serve 同时把每次价差观测追加到独立 spread-history journal（默认 `var/history/spread-history.jsonl`，复用密封段轮转，写失败与主 journal 相同地失败关闭）；`--live` 默认使用 Binance Spot Testnet `bookTicker` WebSocket + Hyperliquid 永续轮询，只有显式 `--live-transport polling` 才把 Binance 降级为 REST 轮询；两条路径都不授予交易权限 |

> 2026-08-13 起，维护冻结的 `volume-maker`、`price-alert` 与 `scanner`（虚拟网格扫描）
> 命令及其配置、任务宿主与读模型已整体移除，为 Binance mainnet live V1 聚焦让路；
> 历史实现保留在 Git 历史中。

`grid` 和 `arbitrage` 的 one-shot 以及连续 Paper owner 都会先持久化计划/预留事实，再跨订单提交边界。套利批次只有全部腿均成交才写入 `execution_completed`；提交报错后的部分执行写入带自动对账摘要的 `execution_partial`，确定但未全部成交则写入 receipt 摘要明确的 `execution_incomplete`。不确定结果不得直接重试，必须先按 journal 投影和权威对账处理。

`research.backtest` 已通过正式 `crypto-trading-research` 二进制成为可用的离线能力，冻结候选注册表与 Paper bar owner 消费同一组纯 bar 策略实现。它仍不代表盈利能力：既有日线实验没有通过项，首个小时线协议又因官方历史并非连续 UTC 小时序列而在数据准入阶段终止，未运行 selection 或 holdout。`research.indicators` 仍是 `Unavailable` 的内部库能力。

## 快速验证

`web` crate 在编译期嵌入 `../frontend/dist/`（React 操作台 bundle）。与 CI 完全一致的
验证需要先构建前端；跳过则以占位 shell 模式编译（只读 API 与全部测试仍然成立，但
`ui_contract` 审计的是占位 shell 而非真实 bundle）：

```powershell
cd ../frontend
corepack enable
pnpm install --frozen-lockfile
pnpm build
cd ../rust
```

然后在本目录运行：

```powershell
cargo +1.89.0 check --workspace --all-targets --all-features --locked
cargo +1.89.0 fmt --all -- --check
cargo +1.89.0 clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +1.89.0 test --workspace --all-targets --all-features --locked
```

Web API 的响应形状由 fixture 交叉契约钉死（`crates/web/tests/api_fixture_contract.rs`
↔ `../frontend/src/lib/api-fixtures.test.ts`，共享 `fixtures/web-api/`）。有意变更
序列化后用 `UPDATE_FIXTURES=1 cargo test -p crypto-trading-web --test
api_fixture_contract` 再生成快照，并与前端 schema 变更同一提交评审。

## 命令示例

查询运行时能力单一事实源（Web Integrations 页也必须消费同一 manifest）：

```powershell
cargo run -- capabilities
cargo run -- capabilities --json
```

人类可读的 adapter 投影见
[`../docs/adapter-support.md`](../docs/adapter-support.md)。`implemented` 不等于生产可用：
Binance 的 Live 面为 `implemented`，但其 blocker 明确限定为「一次操作员确认的一次性 Spot
LIMIT lifecycle + 只读签名 reconcile」；Hyperliquid 的 Live 面仍为 `unavailable`，Paper
不适用。凭真实凭证完成的受监督 mainnet lifecycle 留证仍属于操作员的外部证据，不在仓库内。

只检查配置：

```powershell
cargo run -- config-check config/grid/paper-once-btc.yaml
$configs = Get-ChildItem config -Recurse -File -Include *.yaml,*.yml,*.json | ForEach-Object FullName
cargo run -- config-check $configs --json
```

`config-check` 使用四种明确分类：

- `runtime-executable`：当前 Rust 命令完整消费该配置，可进入已实现的执行路径。
- `legacy-parseable`：能解析已知字段，但仍有字段被忽略或对应运行时未实现，不能视为可执行。
- `auxiliary`：日志、市场元数据、符号转换或 strict monitor companion 等辅助文件；它本身不是可独立启动的策略运行配置。
- `unsupported`：未知、缺失或违反当前强校验的配置；批量检查最终非零退出。

辅助配置必须同时满足保留文件名和最小内容 schema；文件名不能把交易配置伪装为 `auxiliary`。单份输入最多 1 MiB，YAML anchor/alias 不受支持；public path loaders、public `from_str` loaders 和 shared raw reader 共用同一套 1 MiB / YAML 读入护栏；批量检查最多保留 512 条摘要，JSON 与文本输出各自最多 1 MiB。达到摘要数或输出预算后，检查会停止、追加终止错误并非零退出，不会声称剩余路径已经检查。

Grid paper 单次挂单模拟（`--once` 与 `--price` 必须成对出现；当前不模拟账户级风险）：

```powershell
cargo run -- grid config/grid/paper-once-btc.yaml --once --price 110
```

Arbitrage paper 单次执行（使用只包含当前 Rust 已消费字段的 strict profiles）：

```powershell
cargo run -- arbitrage `
  --config config/arbitrage/paper-once-eth.yaml `
  --monitor-config config/arbitrage/paper-monitor-eth.yaml `
  --once `
  --strategy-key ETH-USDC-PERP `
  --left-exchange paper-left --left-symbol ETH-USDC-PERP --left-bid 99.9 --left-ask 100 `
  --left-bid-quantity 10 --left-ask-quantity 10 `
  --right-exchange paper-right --right-symbol ETH-USDC-PERP --right-bid 101 --right-ask 101.1 `
  --right-bid-quantity 10 --right-ask-quantity 10
```

带 `--once` 的命令只接受 `runtime-executable` 的 strict schema；含未消费或拼错字段的 legacy 配置会在写 history 前失败关闭。flat 与 `default_config` 的兼容数值别名若同时出现，必须在 Decimal/整数语义上相等，否则按冲突失败关闭。若两条套利腿的 symbol 不同，必须显式传入 `--strategy-key`；该键是配置选择器而不是腿 symbol 的别名，且必须存在于 `symbol_configs` 且 `enabled: true`。CLI、套利配置和 monitor 配置中的 exchange/symbol 白名单都会在生成意图后、提交前再次校验。可执行配置还必须提供正数 `max_position_value`（兼容嵌套 risk 配置，并可由策略项覆盖）；四侧盘口深度会在写入计划前按订单方向聚合校验，缺失或不足时失败关闭。CLI 和 config crate 的公开文件 loader 以及共享 raw reader 均拒绝超过 1 MiB 的单份配置。

## 凭证与环境变量

当前 Rust 程序只读取进程环境变量，不会自动加载 `.env` 文件。凭证按权限分族：
`BINANCE_API_KEY` / `BINANCE_API_SECRET` 仅用于 Binance **Testnet** 命令；
`BINANCE_MAINNET_READ_API_KEY` / `BINANCE_MAINNET_READ_API_SECRET` 仅用于只读的
`live-reconcile`；`BINANCE_MAINNET_TRADE_API_KEY` / `BINANCE_MAINNET_TRADE_API_SECRET`
仅用于一次性的 `live-lifecycle`。合约测试钉死分离语义：Testnet 凭证不赋予任何
mainnet 权限，read 凭证不赋予 trade 权限。PowerShell 示例：

```powershell
$env:BINANCE_API_KEY = "..."
$env:BINANCE_API_SECRET = "..."
cargo run -- config-check config/exchanges/binance_config.yaml
```

不要把密钥写入仓库。各 exchange YAML 仅用于字段映射，不承载凭证。

## Binance Testnet 订单生命周期

`testnet-lifecycle` 使用独立 append-only journal 运行或恢复一个有界
submit-query-cancel campaign。它要求精确确认短语、稳定 campaign ID 与 UUID client
order ID，只从进程环境读取 Binance Testnet 凭证；恢复与含糊响应都先执行签名单订单
查询，不会直接重复提交。只有最终查询确认 `cancelled` 才完成：

```powershell
cargo run --locked -- testnet-lifecycle --help
```

真实 open-order、controlled partial-fill、kill/restart 演练和留证清单见
[`../docs/runbooks/production-candidate.md`](../docs/runbooks/production-candidate.md#binance-testnet-order-lifecycle-gate)。
确定性本地测试不替代真实凭据证据，mainnet 仍不可用。

## Binance Testnet 账户对账

`testnet-reconcile` 先冻结本地 Paper account 投影，再读取一个 Binance Testnet 产品的
签名余额、全部挂单与持仓。默认模式不会修改 journal；只有
`--apply-reconciliation "I APPLY VERIFIED BINANCE TESTNET RECONCILIATION"` 才会把
新采样结果应用为 verified release 或 durable failure。mixed-exchange/mixed-product
reservation、非 clean account、余额差异与未知 instrument 一律失败关闭：

```powershell
cargo run --locked -- testnet-reconcile --help
```

该 tracer 要求连续两次完整产品采样稳定，并拒绝任何非 settlement asset 的非零余额。
它只支持单一配置 symbol 与 settlement asset，不代表通用多资产净值换算或 mainnet
账户风控。真实 Spot/USD-M 留证步骤见
[`../docs/runbooks/production-candidate.md`](../docs/runbooks/production-candidate.md#binance-testnet-account-reconciliation-gate)。

## Binance Testnet 24 小时演练

`testnet-soak` 只执行 Spot Testnet `bookTicker` WebSocket、签名 User Data WebSocket API 和 REST 鉴权对账，不具有下单或撤单权限。凭证必须仅通过 `BINANCE_API_KEY` / `BINANCE_API_SECRET` 进程环境变量提供。生产候选要求使用同一 `task_id`、history 和 control port 完成一次真实强制终止后重启，并在累计有探针活动时长达到 24 小时后干净停止：

```powershell
cargo run --locked -- testnet-soak --mode serve --help
cargo run --locked -- testnet-soak --mode status --help
cargo run --locked -- testnet-soak --mode stop --help
cargo run --locked -- testnet-soak --mode verify --help
```

完整 PowerShell 命令见 [`../README.md`](../README.md#binance-testnet-soak)，Linux PID 捕获、kill/restart、验真和留证步骤见 [`../docs/runbooks/production-candidate.md`](../docs/runbooks/production-candidate.md#binance-testnet-24-hour-soak-gate)。本地 fixture 契约、缺失凭证或不足 24 小时的 journal 都不能满足发布门禁。

## Binance Mainnet 只读对账

`live-reconcile` 用 `BINANCE_MAINNET_READ_API_KEY` / `BINANCE_MAINNET_READ_API_SECRET`
读取 Binance Spot **MAINNET** 的签名余额与指定 symbol 的全部挂单，可选
`--include-exchange-info` 附带该 symbol 的 exchangeInfo 交易规则。命令只能构造
read-authority 适配器类型（`BinanceMainnetReadEndpoints`），submit/cancel 方法在类型
层面不存在，因此它无法下单或撤单：

```powershell
cargo run --locked -- live-reconcile --help
```

在任何 mainnet 变更操作前，先用它建立账户影子观测基线（余额、外来挂单、交易规则）。

## Binance Mainnet 一次性订单生命周期

`live-lifecycle` 在 Binance Spot **MAINNET** 上运行或恢复一次
submit→query→cancel campaign，是当前唯一具备 mainnet 下单权限的路径。硬性护栏：

- 必须提供精确确认短语 `I AUTHORIZE BINANCE MAINNET SPOT ORDER LIFECYCLE`
  （`--acknowledge-live-lifecycle`），否则不写 journal、不发任何网络请求。
- 必须提供 `--max-notional`：`price × quantity` 超过该上限时在任何 journal 写入或
  网络调用之前拒绝。
- 凭证仅从 `BINANCE_MAINNET_TRADE_API_KEY` / `BINANCE_MAINNET_TRADE_API_SECRET`
  进程环境变量读取；Testnet 凭证与 mainnet read 凭证都无法进入该路径。
- 提交前用 venue 真值准入：exchangeInfo filters（tick/step/minNotional）、签名余额
  （SELL 必须有足额基础资产，spot-no-short）、symbol 上的外来挂单默认拒绝
  （`--allow-foreign-orders` 才放行）。
- journal-first：稳定 `--campaign-id` 与 `--client-order-id`（UUID）先以 PLANNED
  写入 `--history-path`，任何 mainnet 变更都发生在持久化之后。
- 含糊结果与恢复一律 query-first：先做签名单订单查询，绝不盲目重复提交。
- 无法证明安全终态（如 cancel 后订单仍活跃）时写入 journal 级 kill-switch 闩锁，
  同一 history 上的后续运行失败关闭，需人工按 runbook 处置。

```powershell
cargo run --locked -- live-lifecycle --help
```

完整操作程序（前置 Testnet 门禁、专用账户、最小名义、留证与回滚）见
[`../docs/runbooks/production-candidate.md`](../docs/runbooks/production-candidate.md#binance-mainnet-manual-lifecycle-gate)。

## 安全边界

- 默认不授予外部写权限；外部下单路径仅限两条精确确认短语授权的一次性 lifecycle（Binance Testnet `testnet-lifecycle` 与 Binance Spot MAINNET `live-lifecycle`，后者另需专用 trade 凭证和 `--max-notional` 上限），Paper 写入口限于显式 one-shot 和 bearer-protected、严格 profile 匹配的 replay-backed owner。
- 凭证按权限分族：Testnet、mainnet read、mainnet trade 三族环境变量互不通用；`live-reconcile` 在类型层面只能构造只读适配器。
- 不受支持的外部连续或 live 路径一律失败关闭，不会以成功状态伪装为已运行；自动策略 live 执行（`--live`）对 grid/arbitrage 均不可用。
- 所有已实现路径都会校验自身的配置与市场产品身份；arbitrage 还必须通过 `monitor_only`、顶层 `enabled`、策略键开关、正的 `max_position_value`、显式盘口深度、市场数据新鲜度和 instrument 白名单后才会提交。
- `max_position_value` 按精确的 `(exchange, symbol, market_type)` 投影持仓逐腿校验，不是单批总名义价值或账户总毛敞口门禁；连续 Paper owner 另由 journal-backed `AccountRiskAuthority` 使用 settled equity、剩余 FIFO lot 敞口和 pending admission 执行余额、单币种/全局上限、暂停位与 kill switch 门禁。两层门禁都不读取真实交易所 equity、保证金、挂单或持仓，也不能跨不同 journal 自动合并风险。
- Grid one-shot 仍只验证网格规划与 paper 挂单语义；连续 Grid/Arbitrage owner 另行使用 journal-backed `PaperAccountAuthority` 做 pending/uncertain/committed 预留。连续 Grid owner 可按配置启用纯策略网格保护（止损 > 本金保护 > 价格锁定 > 止盈 > 剥头皮），其指令写入 `grid_protection` journal 事实并只作用于 owner 自身的虚拟持仓。以上都不代表真实交易所权益、保证金、持仓真相或 live 风控已经完成。
- 历史 Python 实现已从工作树移除，仅存于 Git 历史（见 [`../archive/README.md`](../archive/README.md)），不得作为当前 Rust 入口。

架构、兼容面和验收门槛见 [`docs/internal/specs/RUST_REFACTOR_PLAN.md`](../docs/internal/specs/RUST_REFACTOR_PLAN.md)；审计复核、修复证据和剩余 NO-GO 项见 [`docs/internal/audits/RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md`](../docs/internal/audits/RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md)。
