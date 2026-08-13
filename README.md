# crypto-trading

[![Rust quality gates](https://github.com/Lens-less/crypto-trading/actions/workflows/rust.yml/badge.svg)](https://github.com/Lens-less/crypto-trading/actions/workflows/rust.yml)
![MSRV](https://img.shields.io/badge/Rust-1.89.0%2B-000000?logo=rust)
![Execution status](https://img.shields.io/badge/execution-live--manual%20(gated)-orange)

一个 Rust-first 的多交易所策略内核，专注于配置兼容、确定性策略计算、受控的 paper one-shot 执行、受监督的一次性 mainnet 订单生命周期，以及为人工恢复分析保留上下文的可审计 JSONL 运行记录。

当前主线位于 [`rust/`](rust/README.md)。原 Python 项目已于 2026-08-13 从工作树移除，仅保留在 Git 历史中（见 [`archive/README.md`](archive/README.md)）。

> [!WARNING]
> **当前版本不是自动交易机器人，不得当作无人值守的实盘系统使用。**
> 自 2026-08-13 起，系统具备一条受监督的 mainnet 路径：`live-lifecycle` 可以在操作员输入精确确认短语
> `I AUTHORIZE BINANCE MAINNET SPOT ORDER LIFECYCLE`、通过专用 mainnet trade 环境变量提供凭证、
> 并给出必填的 `--max-notional` 名义上限后，在 Binance Spot **MAINNET** 上执行一次
> submit→query→cancel 的 LIMIT 订单生命周期；`live-reconcile` 提供只读的 mainnet 账户报告。
> 除此之外仍然全部关闭：没有任何策略通过晋升门禁，自动策略 live 执行（`--live`／`ExecutionMode::Live`）
> 对所有策略失败关闭；市价单、保证金、USD-M、多 symbol owner 循环均不可用。账户级风控权威仍仅覆盖
> Paper 模拟账本；Paper 只计配置的同步 taker 手续费，不代表交易所真实费率，也不包含资金费率、滑点、
> 撮合队列优先级或跨进程持仓。
> 本项目按原样提供，不含任何担保，也不构成投资建议；使用后果由使用者自行承担。
> 不得将本项目用于自成交、制造虚假成交量、市场操纵或任何违反交易所服务条款的活动。
>
> **This is not an autonomous trading bot.** Since 2026-08-13 the only mainnet
> order authority is one operator-supervised Binance Spot LIMIT order
> lifecycle (`live-lifecycle`) behind an exact acknowledgement phrase,
> dedicated mainnet trade credentials, and a required `--max-notional` cap;
> `live-reconcile` is a read-only account report. No strategy has passed the
> promotion gate: autonomous live execution fails closed for every strategy,
> and market orders, margin, USD-M, and multi-symbol owner loops are
> unavailable. The account-level risk authority still governs the paper
> ledger only. Provided as is, without warranty; not investment advice.
> Do not use this project for wash trading, artificial volume,
> market manipulation, or any activity that violates an exchange's terms of
> service.

Live-trading refocus note (2026-08-13):
- The maintenance-frozen `scanner`, `price-alert`, and `volume-maker` commands
  were removed entirely (strategy modules, config schemas, task hosts,
  capability rows, web/API projections, and configs). They only exist in Git
  history now.
- Config-only exchange-auth samples for venues without an adapter (Backpack,
  EdgeX, GRVT, Lighter, Paradex) were removed with `rust/config/legacy/`; the
  active `rust/config/exchanges/` surface only lists operator-supported
  Binance and Hyperliquid profiles.
- Genuine Binance Spot MAINNET support landed on the same day: authority-typed
  read/trade endpoints and credentials, the read-only `live-reconcile` report,
  and the operator-acknowledged one-shot `live-lifecycle` command. The
  capability manifest moved from `paper-only` to `live-manual`
  (`live_trading_enabled=true`); autonomous strategy live execution
  (`runtime.live`) remains `unavailable` pending the strategy promotion gate.

## 项目定位

这个仓库正在把旧的 Python-first 交易系统收敛为一个边界清晰、默认安全的 Rust workspace。目前可用于：

- 验证现有 YAML/JSON 配置，并区分可执行、仅兼容解析、辅助文件和不支持配置。
- 对固定网格和分段套利策略进行确定性 paper one-shot 验证。
- 在提交模拟订单前执行产品身份、行情新鲜度、盘口深度和单批风险检查。
- 记录稳定批次 ID、client order ID、提交计划、回执和对账摘要。
- 用 `live-reconcile` 只读采样 Binance Spot MAINNET 账户余额与挂单，用
  `live-lifecycle` 在人工确认与名义上限之下执行一次受监督的 mainnet Spot
  LIMIT 订单生命周期（submit→query→cancel）。
- 开发并测试交易领域模型、策略、风险控制、交易所接口与运行时边界。
- 在 workspace 内开发并测试 Decimal 指标与确定性事件带研究内核；这些 library-only crate 目前没有已出货的 CLI、HTTP 或二进制入口，不属于可用产品能力。

它不适合：

- 无人值守的自动实盘交易：没有策略通过晋升门禁，自动策略 live 执行失败关闭。
- 启动 7×24 小时网格或套利实盘服务（连续运行仅限 Paper owner 与只读 monitor/soak）。
- 用 paper 成交结果推断真实交易收益或生产就绪程度。

## 当前能力

| CLI 命令 | 演进状态 | 配置能力 | Paper one-shot | 连续运行 | Live | 当前行为 |
| --- | --- | --- | --- | --- | --- | --- |
| `capabilities` | 活跃 | 不适用 | 不适用 | 不适用 | 不可用 | 输出版本化 capability manifest 与 adapter 支持矩阵；这是「本程序被允许做什么」的权威来源 |
| `config-check` | 活跃 | 完整分类检查 | 不适用 | 不适用 | 不适用 | 检查文件或目录；对 public path loaders、public from_str loaders 和 shared raw reader 统一施加 1 MiB / YAML 读入护栏；发现不支持配置、读取错误或预算耗尽时非零退出 |
| `grid` | 活跃 | 校验网格配置 | 可用 | 不可用 | 不可用 | 仅在同时提供 `--once --price` 时模拟 resting paper orders |
| `arbitrage` | 活跃 | 校验套利与 monitor 配置 | 可用 | 不可用 | 不可用 | 要求显式双边价格、四侧盘口数量；`strategy_key` 是配置选择器，可与腿 symbol 不同，但腿仍需通过全局白名单和正的 `max_position_value` 风险门禁 |
| `paper grid`／`paper arbitrage` | 活跃 | 不适用 | 不适用 | 可用（Paper） | 不可用 | 通过 loopback trusted-submit 服务 `start/status/stop/cancel` replay-backed owner；状态只来自 journal/read model；完全成交的同步 taker 回执会写入精确 FIFO lot、手续费、已实现 PnL 与 settled equity，reduce-only 平仓额度在 reserve 与 settle 两处校验；Grid owner 会把配置启用的纯策略网格保护指令写成 `grid_protection` journal 事实并映射为受限 paper 动作；Arbitrage owner 可选 spread-history 自然价差门控，资金费率缺失时标注 `funding_degraded`；两个 owner 的开仓在 reservation 前都要通过 journal-backed 账户风控，拒绝会写成 `account_risk_rejected` 事实 |
| `paper risk` | 活跃 | `--enable-paper-writes` 时必须提供 `--paper-account-risk-config` 共享限额 | 不适用 | 可用（Paper） | 不可用 | 通过同一 loopback trusted-submit 服务控制共享账户级风控权威：`pause`/`resume`（PaperOnly 确认）与 `kill-switch`（专属 `account_kill_switch_armed` 确认 + CLI 精确确认短语）；kill switch 闩锁不可解除，触发后所有新开仓拒绝，owner 会在有可信缓存盘口时以 reduce-only 平仓后停止；缺少盘口或平仓不完整时进入 `RecoveryRequired`，不得伪装为正常停止 |
| `monitor` | 活跃 | 可解析并校验 | 不可用 | 可用（只读 replay / `--live`） | 不可用 | `serve/status/stop` 运行精确双源 monitor owner；`--live` 默认使用 Binance Spot Testnet `bookTicker` WebSocket + Hyperliquid 永续轮询，并将价差事实写入独立 spread-history journal；WebSocket 有有界队列、ping/pong、退避重连和 update-ID 回退门禁，只有显式 `--live-transport polling` 才将 Binance 降级为 REST；所有路径都拒绝超出配置 skew 的配对且不授予交易权限 |

### Binance Testnet 命令

> [!IMPORTANT]
> 下面四条命令中的 `testnet-lifecycle` 会用真实凭证向 **Binance Testnet** 提交并撤销
> **真实订单**。Testnet 使用模拟资金，但它是一个真实的外部交易所环境。这些命令需要
> 精确确认短语才会执行，且在任何情况下都不会接触主网。

| CLI 命令 | 订单权限 | 连续运行 | Live | 当前行为 |
| --- | --- | --- | --- | --- |
| `testnet-smoke` | 无 | 不可用 | 不可用 | 显式选择后执行 Spot/USD-M 只读行情和鉴权对账探针；不提交也不撤销订单 |
| `testnet-lifecycle` | **Testnet 下单/撤单** | 不可用 | 不可用 | 精确确认短语授权的 submit-query-cancel owner；UUID 与 intent 先写 journal，恢复时 query-first，绝不盲目重复提交 |
| `testnet-reconcile` | 无 | 不可用 | 不可用 | 默认只报告的 clean-account gate；把签名 Testnet 余额/挂单/持仓与 exact committed Paper reservation 比对，只有精确确认后才写 release/failure transition |
| `testnet-soak` | 无 | 可用（只读） | 不可用 | journal-backed `serve/status/stop/verify` host；24 小时门禁要求三类探针覆盖、一次强制终止恢复演练和干净停止 |

### Binance Mainnet 命令

> [!CAUTION]
> `live-lifecycle` 会用真实资金在 **Binance Spot MAINNET** 提交并撤销**一笔真实订单**。
> 它是一次性的、人工授权的命令，不是策略执行；每次运行都需要精确确认短语、
> 专用 mainnet trade 凭证和必填的 `--max-notional` 名义上限。

| CLI 命令 | 订单权限 | 连续运行 | 当前行为 |
| --- | --- | --- | --- |
| `live-reconcile` | 无（只读） | 不可用 | 用专用只读凭证（`BINANCE_MAINNET_READ_API_KEY/SECRET`）经权限类型化的 read endpoints 输出 mainnet Spot 余额、挂单与可选 exchangeInfo 报告；该命令只能构造 read-authority 适配器类型，类型层面不存在 submit/cancel 面 |
| `live-lifecycle` | **Mainnet 一次性 LIMIT 下单/撤单** | 不可用 | 精确短语 `I AUTHORIZE BINANCE MAINNET SPOT ORDER LIFECYCLE` + 必填 `--max-notional` 授权的一次 submit→query→cancel 生命周期；journal-first（任何网络变更前先写 `planned`），提交前执行 venue-truth 准入（exchangeInfo filters、签名余额 spot-no-short 检查、默认拒绝外来挂单），恢复严格 query-first、绝不盲目重提；不安全终态会闩锁 journal 级 kill switch，阻止同一 journal 上任何新的 lifecycle |

当前 adapter 矩阵只包含 Binance（public／testnet／mainnet）、Hyperliquid（public 只读）
和 Paper。配置文件中出现的其他交易所名称只是迁移期兼容字段，不代表适配器存在。
权威状态来自 `cargo run --locked -- capabilities --json`；人类可读投影见
[`docs/adapter-support.md`](docs/adapter-support.md)，其表格由合约测试与同一 manifest 保持一致。

## 核心特性

- **精确领域类型**：价格、数量和金额使用 `rust_decimal`，关键算术使用受检操作，不以二进制浮点承担交易计算。
- **纯策略内核**：固定/马丁网格、网格保护子系统（剥头皮、本金保护、止盈、价格锁定、止损，按固定优先级仲裁）、分段套利、bar 策略与虚拟网格逻辑与 I/O 分离，便于确定性测试。
- **权限类型化的 Mainnet 适配器**：Binance Spot MAINNET 的读与写是两个不同的具体类型（`BinanceMainnetReadEndpoints`／`BinanceMainnetTradeEndpoints`），host 在构造时钉死为官方 `api.binance.com`／`ws-api.binance.com`／`stream.binance.com`，凭证族分离——持有 read 类型或 read 凭证在类型与配置两个层面都无法获得下单权限。
- **可验证的 Paper 执行与账本**：`PaperExchange` 覆盖订单状态、顶层深度消耗、GTC/IOC/FOK、部分成交与撤单语义；journal-backed Paper account 对完全成交的同步 taker 回执记录 FIFO lot、即时手续费、已实现 PnL、settled equity 和 reduce-only 容量。尚不包含周期性 mark-to-market、资金费率、保证金/强平、真实排队延迟、多档冲击或 resting-maker 的持久成交回调。
- **失败关闭的执行边界**：未授权模式、过期行情、产品身份不匹配、深度不足、风险超限和未实现适配器都在提交前拒绝。
- **可审计执行历史**：先写入 `execution_planned`，再写入 `execution_completed`、`execution_partial` 或 `execution_incomplete`。
- **有界资源使用**：配置、历史记录、批次、actor 队列、HTTP 响应和聚合输出都有显式上限。
- **跨平台质量门禁**：CI 在 Windows 与 Ubuntu 上验证 MSRV 和 stable，并执行格式、Clippy、release build 与 RustSec audit。
- **历史归档在 Git 历史中**：旧 Python tree 已于 2026-08-13 从工作树移除，完整内容与逐文件校验清单保留在 Git 历史中（[`archive/README.md`](archive/README.md) 为墓碑说明）；它从未参与 Rust 的构建、测试、配置加载或运行。

## 快速开始

### 前置条件

- Git
- [Rustup](https://rustup.rs/)
- Windows PowerShell、Linux 或 macOS

仓库通过 [`rust/rust-toolchain.toml`](rust/rust-toolchain.toml) 固定 Rust `1.89.0`，并声明 `rustfmt` 与 `clippy`。进入 `rust/` 后，Rustup 会自动选择所需工具链。

本文的 `config-check`、Grid paper 和 Arbitrage paper 示例都不需要交易所凭证，也不会连接私有交易 API。

### 克隆与构建

```bash
git clone https://github.com/Lens-less/crypto-trading.git
cd crypto-trading/rust
cargo build --workspace --locked
cargo run --locked -- --help
```

### 1. 检查一份可执行配置

```bash
cargo run --locked -- config-check config/grid/paper-once-btc.yaml
```

`config-check` 会返回以下分类之一：

| 分类 | 含义 |
| --- | --- |
| `runtime-executable` | 当前 Rust CLI 完整消费关键字段，可进入已实现的执行路径 |
| `legacy-parseable` | 可以解析，但仍有字段未消费或运行时未实现，不得当作可执行配置 |
| `auxiliary` | 日志、符号映射、市场元数据或 companion 配置，不能独立启动策略 |
| `unsupported` | 未知、缺失、冲突或违反当前严格校验的配置 |

检查整个 `config/` 时出现非零退出可能是预期行为，因为目录中还保留了迁移期配置：

```powershell
$configs = Get-ChildItem config -Recurse -File -Include *.yaml,*.yml,*.json |
  ForEach-Object FullName
cargo run --locked -- config-check $configs --json
```

### 2. 运行网格 paper one-shot

```bash
cargo run --locked -- grid config/grid/paper-once-btc.yaml --once --price 110
```

这个命令在参考价 `110` 上生成并提交模拟挂单。默认历史文件为 `var/history/grid-paper.jsonl`。`--once` 与 `--price` 必须成对出现；不带它们时只校验配置。

### 3. 运行套利 paper one-shot

下面示例使用仓库内只包含当前 Rust 已消费字段的 strict profiles：

```powershell
cargo run --locked -- arbitrage `
  --config config/arbitrage/paper-once-eth.yaml `
  --monitor-config config/arbitrage/paper-monitor-eth.yaml `
  --once `
  --strategy-key ETH-USDC-PERP `
  --left-exchange paper-left `
  --left-symbol ETH-USDC-PERP `
  --left-bid 99.9 `
  --left-ask 100 `
  --left-bid-quantity 10 `
  --left-ask-quantity 10 `
  --right-exchange paper-right `
  --right-symbol ETH-USDC-PERP `
  --right-bid 101 `
  --right-ask 101.1 `
  --right-bid-quantity 10 `
  --right-ask-quantity 10
```

默认历史文件为 `var/history/arbitrage-paper.jsonl`。两条腿只有全部返回 `Filled` 才会记为完成；部分执行或确定但不完整的执行会写入恢复上下文并非零退出。

## 命令使用说明

从 `rust/` 目录运行：

```bash
cargo run --locked -- <COMMAND> --help
```

常用命令：

```text
crypto-trading capabilities [--json]
crypto-trading testnet-smoke [OPTIONS]
crypto-trading testnet-lifecycle [OPTIONS]
crypto-trading testnet-reconcile [OPTIONS]
crypto-trading testnet-soak --mode <serve|status|stop|verify> [OPTIONS]
crypto-trading live-reconcile [--json] [OPTIONS]
crypto-trading live-lifecycle --acknowledge-live-lifecycle <PHRASE> --max-notional <CAP> [OPTIONS]
crypto-trading grid <CONFIG> [OPTIONS]
crypto-trading arbitrage [OPTIONS]
crypto-trading monitor [OPTIONS]
crypto-trading config-check <PATHS>... [--json]
```

> [!IMPORTANT]
> `monitor` / `testnet-soak` 的 `serve|status|stop` loopback control host
> 现在要求环境变量 `CRYPTO_TRADING_TASK_CONTROL_TOKEN`。该值不会出现在
> 命令行参数里，且必须是 32-512 字节的可打印非空白 ASCII secret；缺失、
> 长度不符或 token 不匹配都会失败关闭。

## Binance Testnet lifecycle

`testnet-lifecycle` 是显式授权、journal-first 的 Binance Testnet
submit-query-cancel owner。它会在提交前持久化 campaign 与 UUID client order ID，
在恢复时先按该 UUID 查询，确认 open 或 controlled partial fill 后撤单，并且只有最终
查询到 `cancelled` 才报告完成。凭证只从 `BINANCE_API_KEY` /
`BINANCE_API_SECRET` 进程环境变量读取；该命令没有 mainnet 开关——mainnet 权限只存在于
独立的 `live-lifecycle` 命令及其专用凭证族。

```powershell
cargo run --locked -- testnet-lifecycle --help
```

真实 open-order、controlled partial-fill 与 kill/restart 演练必须使用不同 campaign /
client UUID，并保留 CLI JSON、journal 与候选二进制校验和。完整、可复制的门禁与
失败恢复步骤见
[`docs/runbooks/production-candidate.md`](docs/runbooks/production-candidate.md#binance-testnet-order-lifecycle-gate)。
本地确定性契约不会被当作真实凭据证据。

## Binance Testnet reconciliation

`testnet-reconcile` 是 M4 Paper reconcile transition 的首个真实账户消费者。它先冻结一个
exact committed Paper reservation，再用签名请求读取所选 Binance Testnet 产品的余额、
全部挂单和持仓。默认只输出报告；只有传入精确确认短语时，匹配结果才写入
`reconcile_release`，失配结果则写入 durable reconciliation failure。该命令没有
mainnet 开关。

```powershell
cargo run --locked -- testnet-reconcile --help
```

当前 gate 有意限定为单一 Binance 产品、单一配置 symbol 与单一 settlement asset：
连续两次完整采样必须稳定，Testnet 账户必须处于 clean-account 状态，非 settlement
asset 余额必须为零，且 settlement asset 的可用余额必须等于该 reservation 释放后的
本地可用额度。mixed-exchange reservation、未知 symbol、挂单、持仓、采样漂移或余额
差异都会失败关闭。真实凭据命令、应用确认短语与留证步骤见
[`docs/runbooks/production-candidate.md`](docs/runbooks/production-candidate.md#binance-testnet-account-reconciliation-gate)。

## Binance Testnet soak

该持久化 soak host 只读运行，自身不具有任何 mainnet 权限；每轮依次消费 Spot Testnet
`bookTicker` WebSocket、签名 User Data WebSocket API 和 Binance Testnet REST 鉴权对账。它不会提交或撤销订单，
也不会把本地 fixture、离线时间或缺失凭证伪装成通过的 24 小时证据。

```powershell
$env:BINANCE_API_KEY = "..."
$env:BINANCE_API_SECRET = "..."
$env:CRYPTO_TRADING_TASK_CONTROL_TOKEN = "<32-512 byte generated secret>"

$taskId = "binance-testnet-24h"
$history = "var/history/binance-testnet-24h.jsonl"
$controlPort = 55124
$minSuccesses = 288
$stdout = "var/history/binance-testnet-24h.stdout.log"
$stderr = "var/history/binance-testnet-24h.stderr.log"

cargo build --release --locked --package crypto-trading-apps --bin crypto-trading
$binary = (Resolve-Path "target/release/crypto-trading.exe").Path
New-Item -ItemType Directory -Force (Split-Path $history) | Out-Null

$serveArgs = @(
  "testnet-soak", "--mode", "serve",
  "--task-id", $taskId,
  "--history-path", $history,
  "--interval-ms", "300000",
  "--probe-timeout-ms", "15000",
  "--failure-threshold", "3",
  "--control-port", "$controlPort",
  "--timeout-ms", "10000"
)
$soak = Start-Process -FilePath $binary -ArgumentList $serveArgs `
  -WindowStyle Hidden -PassThru `
  -RedirectStandardOutput $stdout -RedirectStandardError $stderr
$soak.Id

& $binary testnet-soak --mode status `
  --task-id $taskId `
  --history-path $history `
  --control-port $controlPort

# status 至少确认 3 次成功探针后，执行且只执行一次强制终止恢复演练。
$killedPid = $soak.Id
Stop-Process -Id $killedPid -Force
Wait-Process -Id $killedPid -ErrorAction SilentlyContinue

$soak = Start-Process -FilePath $binary -ArgumentList $serveArgs `
  -WindowStyle Hidden -PassThru `
  -RedirectStandardOutput "$stdout.restart" -RedirectStandardError "$stderr.restart"
$soak.Id

& $binary testnet-soak --mode stop `
  --task-id $taskId `
  --history-path $history `
  --control-port $controlPort
Wait-Process -Id $soak.Id

& $binary testnet-soak --mode verify `
  --task-id $taskId `
  --history-path $history `
  --minimum-successes $minSuccesses |
  Tee-Object -FilePath "var/history/binance-testnet-24h-evidence.json"
if ($LASTEXITCODE -ne 0) { throw "Binance Testnet soak evidence is not release-ready" }
```

先让累计的有探针活动时长达到至少 24 小时，再执行 `stop`。`verify` 始终输出稳定 JSON；
在证据同时证明干净停止、至少一次可观察的非正常重启、最低成功次数，以及市场流、用户流
和鉴权对账三类非零覆盖之前，它都会非零退出。完整 Linux 演练和留证清单见
[`docs/runbooks/production-candidate.md`](docs/runbooks/production-candidate.md)。

`--debug`、`--debug-detail` 和 `--no-ui` 目前主要保留 CLI 兼容性，尚不会改变对应 handler 的行为。运行时日志过滤只由进程环境变量 `RUST_LOG` 控制；例如 PowerShell 可设置 `$env:RUST_LOG = "warn,crypto_trading_web_app=info,crypto_trading_runtime=info,crypto_trading_exchange=info"`。`rust/config/logging.yaml` 是迁移期辅助配置，Rust runtime 不会读取它。

`testnet-smoke` 只在显式选择远端探针时才出网：`--call-book-ticker` 会分别调用 Binance Spot 与 USD-M testnet 的 `bookTicker`，`--call-reconcile` 会在此基础上用 `BINANCE_API_KEY` / `BINANCE_API_SECRET` 调 Binance testnet 的开放订单和持仓对账路由。该命令只产生留证输出，不会提交新订单。

## Binance Mainnet：live-reconcile 与 live-lifecycle

`live-reconcile` 是只读的 mainnet Spot 账户报告：签名读取余额与配置 symbol 的挂单，
可选 `--include-exchange-info` 附带该 symbol 的权威 instrument 规则。它只从
`BINANCE_MAINNET_READ_API_KEY` / `BINANCE_MAINNET_READ_API_SECRET` 读取凭证，
只能构造 read-authority 适配器类型——submit/cancel 在类型层面不存在，Testnet 凭证
与 mainnet trade 凭证都不会赋予它任何权限：

```powershell
cargo run --locked -- live-reconcile --help
```

`live-lifecycle` 是唯一具有 mainnet 下单权限的命令：一次人工授权的 Spot LIMIT
submit→query→cancel 生命周期。门禁顺序（有合约测试钉死）：精确确认短语
`I AUTHORIZE BINANCE MAINNET SPOT ORDER LIFECYCLE` → 配置校验（含必填
`--max-notional`，`price*quantity` 超限在写 journal 前拒绝）→ 该 journal 的
kill-switch 闩锁检查 → 才读取 `BINANCE_MAINNET_TRADE_API_KEY` /
`BINANCE_MAINNET_TRADE_API_SECRET` → 才产生网络活动。新 campaign 在 `planned`
事实持久化之后、提交之前执行 venue-truth 准入：exchangeInfo filters 校验、按签名
余额执行 spot-no-short（卖出不得超过可用 base、买入名义不得超过可用 quote），
symbol 上存在不属于本 campaign 的挂单时默认拒绝（`--allow-foreign-orders` 才放行）。
恢复严格 query-first：任何后续运行先按持久化的 UUID client order ID 查询，绝不盲目
重提；订单出现意外终态（如在撤单前成交）会写入失败事实并闩锁 journal 级 kill
switch——同一 journal 上所有新 lifecycle 都被阻止，直到人工核对账户并启用新 journal：

```powershell
cargo run --locked -- live-lifecycle --help
```

完整的前置条件（全部 Testnet 门禁、只读 shadow 观察、专用账户、最小名义）与留证
步骤见
[`docs/runbooks/production-candidate.md`](docs/runbooks/production-candidate.md#binance-mainnet-manual-lifecycle-gate)。

需要独立二进制时：

```bash
cargo build --release --locked
./target/release/crypto-trading --help
```

Windows 对应文件为 `target\release\crypto-trading.exe`。

## Web 控制面

除了 CLI，仓库还交付一个默认只读、默认 bearer 保护的本地操作台
`crypto-trading-web`。它把同一份 journal 投影成人类可读的视图：授权状态、可取得的
运行信号、批次计划与结果、未恢复的执行状态。只有显式启用
`--enable-paper-writes`、加载受限 replay profile 和账户风控配置后，才会开放
loopback-only 的受信 Paper submit 路由；它没有 testnet/mainnet 写权限。

设计规范见 [`docs/design-system.md`](docs/design-system.md)。操作台前端是 `frontend/` 下的
React + Vite + TypeScript 应用；`pnpm build` 产出的 `frontend/dist/` 在编译期由
`rust/crates/web` 的 build script 通过 `include_dir!` 整体嵌入二进制。产物不引用任何
远程字体或 CDN 资源，运行时也没有文件系统访问——每个字节在编译期固定。

### 构建（两步：pnpm → cargo）

```bash
# 1. 构建前端 bundle（需要 Node 22+;corepack 会激活锁定的 pnpm 版本）
cd frontend
corepack enable
pnpm install --frozen-lockfile
pnpm build

# 2. 编译并运行二进制(dist 会被自动嵌入)
cd ../rust
export CRYPTO_TRADING_WEB_TOKEN="$(openssl rand -hex 32)"
cargo run --locked --package crypto-trading-web-app --bin crypto-trading-web -- \
  --history-path var/operations.jsonl \
  --journal-id "$(uuidgen)" \
  --bearer-token-env CRYPTO_TRADING_WEB_TOKEN
```

跳过第 1 步也能编译：二进制会服务一个占位 shell 并保持只读 API 可用，同时明确说明
UI 资产未构建——不会静默降级。

### 前端开发模式

```bash
cd frontend
pnpm dev        # Vite dev server,仅绑定 127.0.0.1,/api 代理到 127.0.0.1:8787
pnpm typecheck && pnpm lint && pnpm test   # 质量门禁(vitest 含 fixture 交叉契约)
```

### 真浏览器合约(Playwright)

`frontend/e2e/` 用 Playwright 驱动**真实交付物**(嵌入 dist 的二进制,而非 dev
server),覆盖权限脊柱、历史投影事实、SSE 通知徽标三态与降级恢复、浏览器存储纪律
(仅 `ct-theme`)以及受信 Paper 写路径全流程：

```bash
cd frontend && pnpm build
cd ../rust && cargo build --locked -p crypto-trading-web-app --bin crypto-trading-web
cd ../frontend
pnpm exec playwright install chromium   # 首次
pnpm e2e
```

CI 中该套件在 ubuntu 上运行(`.github/workflows/frontend.yml` 的 e2e job)。

服务只绑定 `127.0.0.1:8787`，且这是在代码里强制的，不是配置项。除非你自己加一层
经过审查的反向代理，否则它对其他主机不可达。

容器化部署（单容器、只读根文件系统、非 root、只挂载 journal 卷）：

```bash
docker compose -f deploy/compose.yaml up -d
curl --fail http://127.0.0.1:8787/api/v1/health
```

完整的部署契约、健康检查、备份/恢复演练和回滚步骤见
[`docs/runbooks/production-candidate.md`](docs/runbooks/production-candidate.md)。

## 配置与凭证

当前配置位于 [`rust/config/`](rust/config/)：

```text
rust/config/
├── arbitrage/       # 套利策略、monitor companion 与迁移期配置
├── exchanges/       # 交易所字段映射和市场元数据
├── grid/            # 网格策略配置
├── paper/           # Paper 账户级风控共享限额示例
├── logging.yaml     # 迁移期辅助配置；Rust runtime 不读取
└── symbol_conversion.yaml
```

配置处理边界：

- 单份配置最大 `1 MiB`。
- YAML anchor/alias 不受支持。
- public path loaders、public `from_str` loaders 和 shared raw reader 共享同一套 `1 MiB` / YAML 读入护栏。
- Paper one-shot 只接受 `runtime-executable` strict schema；拼错或未消费字段会在写入 history 前失败。
- 批量检查最多保留 512 条摘要，文本与 JSON 输出各有 `1 MiB` 预算。
- 目录扫描或输出预算耗尽时会停止并非零退出，不会声称剩余文件已经检查。

Rust 程序只读取**进程环境变量**，不会自动加载 `.env`。例如：

```powershell
$env:BINANCE_API_KEY = "..."
$env:BINANCE_API_SECRET = "..."
cargo run --locked -- config-check config/exchanges/binance_config.yaml
```

凭证按权限分族，互不越界：

| 环境变量 | 权限 | 消费者 |
| --- | --- | --- |
| `BINANCE_API_KEY` / `BINANCE_API_SECRET` | 仅 Binance **Testnet** | `testnet-lifecycle`、`testnet-reconcile`、`testnet-soak`、`testnet-smoke` |
| `BINANCE_MAINNET_READ_API_KEY` / `BINANCE_MAINNET_READ_API_SECRET` | 仅 mainnet **只读** | `live-reconcile` |
| `BINANCE_MAINNET_TRADE_API_KEY` / `BINANCE_MAINNET_TRADE_API_SECRET` | 仅 mainnet **一次性 lifecycle** | `live-lifecycle` |

合约测试钉死了分离语义：Testnet 凭证不会赋予任何 mainnet 权限，read 凭证不会赋予
trade 权限，`live-reconcile` 永不读取 trade 变量。通用凭证 loader 另采用
`<EXCHANGE>_<FIELD>` 命名，可覆盖 `API_KEY`、`API_SECRET`、`PRIVATE_KEY` 和
`WALLET_ADDRESS` 字段；支持解析这些变量不代表对应 live adapter 存在。

不要把密钥、私钥、JWT、助记词或真实账户信息写入已跟踪的 `rust/config/exchanges/*.yaml`、日志、issue 或提交历史。`.gitignore` 已屏蔽常见 `.env`、密钥文件和本地运行数据，但这不能替代提交前的凭证扫描。

## 安全模型与 Paper 限制

1. **自动策略 live 失败关闭；人工 mainnet 路径有独立门禁**：`ExecutionMode::Live` 对所有策略失败关闭，风险确认短语不会绕过策略晋升门禁。唯一 mainnet 变更权限是 `live-lifecycle` 一次性命令，其自身门禁为：精确确认短语、专用 trade 凭证、必填名义上限、journal-first、venue-truth 准入、query-first 恢复与不可解除的 journal 级 kill-switch 闩锁。
2. **Paper 真值仍不是交易所真值**：同一 journal generation 会跨重启重放 reservation、精确成交费用、FIFO lot 与 settled equity，并由共享账户风控消费；但它不读取真实余额、保证金、外部挂单或交易所仓位，也不能跨不同 journal 自动合并风险。
3. **网格是挂单语义验证**：它根据一个显式参考价规划 resting orders，不代表真实撮合、账户风险或跨进程仓位已经验证。
4. **套利使用显式顶层盘口**：调用方必须给出双边 bid/ask 和四侧可用数量；`strategy_key` 是配置选择器，不是腿 symbol 的别名；模型不等价于完整深度、延迟和滑点仿真。
5. **Paper 账户门禁不等于保证金引擎**：账户风控使用精确 settled equity、剩余 lot 敞口和 pending admission 做余额/单币种/全局上限判断；它仍没有未实现 PnL、维持保证金、资金费率或交易所强平规则，因此不能作为实盘资金安全证明。
6. **Paper 交易成本模型仍有限**：精确账本会计入完全成交同步 taker 回执的配置手续费；resting-maker 手续费/返佣、资金费率、网络延迟、队列优先级和多档市场冲击仍不在当前结果中。
7. **不完整执行不得直接重试**：先根据 history 中的批次、腿和对账摘要确认状态，否则可能重复提交。

更完整的门禁与剩余风险见 [`docs/internal/audits/RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md`](docs/internal/audits/RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md)。

## 架构

```text
CLI / config
      │
      ▼
strategy decision ──► risk + execution policy
                           │
                           ▼
                  execution_planned
                           │
                           ▼
                    PaperExchange
                           │
                           ▼
        completed / partial / incomplete history
```

Rust workspace 包含十一个 crate。依赖方向是单向的：`domain` 不依赖任何其他 crate，
`web` 不能构造交易权限。

| Crate | 职责 |
| --- | --- |
| `domain` | Symbol、MarketSnapshot、Order、Position、Price、Quantity、Money 等领域类型 |
| `config` | 有界文件读取、兼容反序列化、严格校验、环境变量覆盖与凭证脱敏 |
| `strategy` | 网格（含马丁递增与纯状态机网格保护：剥头皮/本金保护/止盈/价格锁定/止损）、分段套利、bar 策略、账户风控与虚拟网格纯算法 |
| `indicators` | workspace 内部的 Decimal 指标研究库；未链接到已出货 CLI/HTTP/二进制，manifest 中为 `Unavailable` |
| `backtest` | workspace 内部的确定性事件带研究库；未链接到已出货 CLI/HTTP/二进制，manifest 中为 `Unavailable` |
| `exchange` | 统一异步接口、PaperExchange、公开 Binance 行情适配器、Binance Testnet 协议、权限类型化的 Binance Spot MAINNET read/trade 适配器与钉死的官方 endpoints、有界 actor 和 instrument rules |
| `runtime` | 执行模式、路由、批次、提交策略、部分结果对账、JSONL journal、operator read model 与 capability 清单 |
| `control-plane` | journal 与各操作界面之间的最小权限读取/提交 seam |
| `web` | HTTP API 与编译期嵌入的 React 操作台 bundle(`frontend/dist`) |
| `web-app` | `crypto-trading-web` 二进制，只在 loopback 上提供只读控制面 |
| `apps` | `crypto-trading` 二进制、CLI 参数、配置检查、one-shot、Testnet 编排与 mainnet `live-reconcile`／`live-lifecycle` 编排 |

`indicators` 与 `backtest` 当前只是 workspace 内部研究内核，不是已出货产品能力：没有受支持的 CLI/HTTP 入口，也没有生产二进制链接它们。`backtest` 仍是单标的模型，尚未接入生产 `MarketDataEvent`／`StrategyMachine` 适配层，也不模拟队列、延迟、资金费率、部分成交或多档深度。研究 crate 测试全绿不能解释为 paper/live 一致，更不能解释为策略盈利。

详细的兼容面与设计目标见 [`docs/internal/specs/RUST_REFACTOR_PLAN.md`](docs/internal/specs/RUST_REFACTOR_PLAN.md)。

## 执行历史与恢复

默认路径：

- Grid：`rust/var/history/grid-paper.jsonl`
- Arbitrage：`rust/var/history/arbitrage-paper.jsonl`
- Spread history（monitor serve 的价差观测样本，供套利历史决策模式回填）：`rust/var/history/spread-history.jsonl`

关键事件：

| 事件 | 含义 |
| --- | --- |
| `execution_planned` | 提交前持久化批次 ID、全部 client order ID 与恢复所需订单信息 |
| `execution_completed` | 预期回执齐全，套利全部腿均为 `Filled` |
| `execution_partial` | 提交过程报错且已有部分结果，同时保存有界对账摘要 |
| `execution_incomplete` | 提交返回，但结果数量或状态不足以判定完整成功 |

历史写入在构造时固定相对路径；单条、单批和单文件上限仍然存在。追加即将超过 `64 MiB` 单文件上限时，活跃文件会先被密封为只读段 `<path>.<seq>`（seq 从 1 递增，密封后永不改写），再开新活跃文件续写；读侧按段序拼接重放，与未轮转时逐字节等价。段链有界：最多 63 个密封段、全链共 4 GiB，超限后追加仍失败关闭；设计上不做 compaction，以保留可重放验证的事实链。同一 journal 路径通过 sibling lock file 获取跨进程 OS writer lease（租约覆盖整条段链，密封动作只在持有租约时发生），竞争者立即失败关闭；它仍不提供通用 transactional saga。取消、进程死亡或抢占中断都可能留下 planned-only 状态，重试前必须先 reconcile。

## 项目结构

```text
crypto-trading/
├── .github/
│   ├── workflows/rust.yml       # 跨平台 CI、RustSec、cargo-deny、镜像构建
│   ├── workflows/frontend.yml   # 前端门禁、供应链策略与 Playwright 浏览器合约
│   └── ISSUE_TEMPLATE/
├── frontend/                    # React + Vite 操作台(构建产物嵌入 web 二进制)
│   ├── src/                     # 页面、组件与 zod 窄校验 API 类型
│   ├── e2e/                     # Playwright 真浏览器合约
│   └── docs/                    # UI 契约迁移映射与 parity 清单
├── rust/                        # 唯一活动项目
│   ├── crates/                  # Rust workspace 源码（十一个 crate）
│   ├── config/                  # 当前配置副本
│   ├── fixtures/                # 契约测试消费的 journal fixture
│   ├── deny.toml                # 许可证 / 依赖来源策略
│   ├── Cargo.toml
│   ├── Cargo.lock
│   ├── rust-toolchain.toml
│   └── README.md                # 详细操作与安全边界
├── docs/
│   ├── README.md                # 文档索引
│   ├── adapter-support.md       # capability 清单的人类可读投影
│   ├── design-system.md         # Web 控制面视觉规范
│   ├── releasing.md             # 发布流程
│   ├── runbooks/                # 生产候选运维手册与发布门禁
│   └── internal/                # 审计、规格、计划、研究（历史快照）
├── deploy/                      # Compose 部署与备份/恢复演练脚本
├── scripts/                     # 仓库卫生检查与研究数据准备脚本
├── Dockerfile                   # 单容器交付
├── archive/
│   └── README.md                # 已移除的 Python 归档的墓碑说明
├── SECURITY.md                  # 漏洞披露与威胁模型
├── CONTRIBUTING.md
├── CHANGELOG.md
└── README.md
```

## 开发与验证

在 `rust/` 目录运行完整本地门禁：

```bash
cargo +1.89.0 check --workspace --all-targets --all-features --locked
cargo +1.89.0 fmt --all -- --check
cargo +1.89.0 clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +1.89.0 test --workspace --all-targets --all-features --locked
cargo +1.89.0 build --release --workspace --all-features --locked
cargo +1.89.0 test --doc --workspace --all-features --locked
```

CI 还会：

- 在 Ubuntu 与 Windows 上分别运行 Rust `1.89.0` 和 stable。
- 使用 `Cargo.lock` 固定依赖解析。
- 拒绝 Clippy warning。
- 执行 RustSec advisory audit。

## 常见问题

### 为什么 `config-check config --json` 返回非零？

`legacy-parseable` 本身仍可返回 `status: ok`；非零退出表示同一批次里至少还有一条 `status: error`，例如 `unsupported` 配置、读取失败或预算耗尽。先查看每条摘要的 `classification`、`status` 和 `error`。

### 为什么配置校验成功，但命令仍然报 `runtime is unavailable`？

配置解析与运行能力是两道独立门禁。无 `--once` 的 `arbitrage` 当前只验证输入，然后明确失败。`monitor` 是例外：它的 `serve/status/stop` 模式可以运行，但只接受精确双源 replay 数据源（或显式 `--live` 只读行情），不授予交易权限。

### 为什么策略的 `--live` 仍然无法下单？

这是设计行为。`runtime.live`（grid/arbitrage 等所有 `ExecutionMode::Live` owner 循环）在 capability manifest 中仍为 `unavailable`：没有任何策略通过晋升门禁，按「不得由实现暗示策略权限」的不变量，唯一 mainnet 下单权限是人工授权的一次性 `live-lifecycle` 命令。想在 mainnet 上做一次受监督的订单生命周期，请使用 `live-lifecycle`（见上文与 runbook），而不是策略 `--live`。

### 为什么 YAML anchor/alias 被拒绝？

配置入口采用有界、可审计的严格解析边界，不允许 alias 扩展改变资源使用或隐藏真实字段。请展开为显式 YAML。

### 从仓库根目录运行时为什么找不到配置？

文档中的相对路径都以 `rust/` 为工作目录。先执行 `cd rust`，或显式传入 `rust/config/...`。

## 路线图

**当前已具备（live-manual 阶段）**：

- Binance Spot MAINNET 权限类型化 read/trade 适配器与官方 endpoints（构造期钉死）。
- `live-reconcile` 只读 mainnet 账户报告与 `live-lifecycle` 一次性人工订单生命周期（精确短语 + 名义上限 + journal-first + venue-truth 准入 + query-first 恢复 + kill-switch 闩锁），均有确定性合约测试。
- Binance Testnet lifecycle／reconcile／soak 命令面与 journal 段轮转、跨进程 writer lease。

**仍然开放的操作员留证门禁**（本仓库不签入真实凭据证据）：

- 凭真实凭证完成的 Testnet open-order、controlled partial-fill、kill/restart 三类 campaign。
- 凭真实凭证完成的 Testnet 账户对账（每个在范围内的产品）与 24 小时 soak（含一次强制终止恢复演练）。
- mainnet 只读 shadow 观察与一次受监督的 mainnet lifecycle 留证。

**策略晋升门禁（自动 live 执行开放前的硬条件）**：

- 一个显式选定的策略通过预注册的离线证据、Paper 长期观察、Testnet 与 shadow 评估；当前全部离线候选均未通过。
- 面向 live 的账户风险权威（真实余额/挂单/持仓真值，而非 Paper 账本）、多腿补偿与崩溃后续作。
- resting-maker 手续费/返佣、资金费率、周期性 mark-to-market、延迟、队列优先级、多档滑点等更完整的成本与撮合模型。

## 参与贡献

完整的贡献规则、不可跨越的边界和本地门禁见 [`CONTRIBUTING.md`](CONTRIBUTING.md)。
要点：

1. 只在 `rust/` 中开发当前功能；旧 Python 实现仅存于 Git 历史，不得复活为运行入口。
2. 新增行为时先补测试；改动 live-lifecycle／mainnet 适配器等 live 路径必须附合约测试，并保持 journal-first、query-first 与失败关闭不变量。
3. 不引入二进制浮点交易计算，不在日志和诊断中暴露凭证。
4. 运行“开发与验证”中的完整门禁。
5. 在 PR 中说明安全边界、已验证内容和未覆盖风险。

**安全问题不要开 issue**，请走 [`SECURITY.md`](SECURITY.md) 中的私有披露通道。

## 延伸文档

操作与安全：

- [`rust/README.md`](rust/README.md)：详细命令、配置分类和安全边界。
- [`docs/adapter-support.md`](docs/adapter-support.md)：adapter 支持矩阵，由合约测试与 capability 清单保持一致。
- [`docs/runbooks/production-candidate.md`](docs/runbooks/production-candidate.md)：部署契约、Testnet 发布门禁、mainnet 人工 lifecycle 门禁、备份恢复演练与回滚。
- [`SECURITY.md`](SECURITY.md)：威胁模型与漏洞私有披露流程。

参与开发：

- [`CONTRIBUTING.md`](CONTRIBUTING.md)：本地门禁、不可跨越的边界、依赖与测试规则。
- [`CHANGELOG.md`](CHANGELOG.md)：版本历史，每个版本都标注权限是否变化。
- [`docs/design-system.md`](docs/design-system.md)：Web 控制面视觉规范。

背景与证据：

- [`docs/internal/specs/RUST_REFACTOR_PLAN.md`](docs/internal/specs/RUST_REFACTOR_PLAN.md)：Rust-first 迁移目标、模块设计与验收门槛。
- [`docs/internal/audits/RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md`](docs/internal/audits/RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md)：审计复核、修复证据和剩余 NO-GO 项（2026-07-17 的快照，不是活文档）。
- [`archive/README.md`](archive/README.md)：旧 Python tree 的来源提交和完整性校验。

## 许可与免责声明

本项目以 MIT 许可证发布，权威文本见 [`LICENSE`](LICENSE)；workspace 中所有 crate 均继承该声明。

本项目仅用于软件工程、策略研究和 paper 模拟，**不构成投资建议，不提供任何形式的担保**。数字资产交易具有高风险，任何使用者都应独立评估并自行承担由配置、部署或交易决策产生的全部后果。
