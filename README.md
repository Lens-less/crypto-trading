# crypto-trading

[![Rust quality gates](https://github.com/Lens-less/crypto-trading/actions/workflows/rust.yml/badge.svg)](https://github.com/Lens-less/crypto-trading/actions/workflows/rust.yml)
![MSRV](https://img.shields.io/badge/Rust-1.85.0%2B-000000?logo=rust)
![Execution status](https://img.shields.io/badge/execution-paper%20only-orange)

一个 Rust-first 的多交易所策略内核，专注于配置兼容、确定性策略计算、受控的 paper one-shot 执行，以及为人工恢复分析保留上下文的可审计 JSONL 运行记录。

当前主线位于 [`rust/`](rust/README.md)。原 Python 项目已冻结到 [`archive/python-legacy/`](archive/python-legacy/)，只用于审计、行为对照和迁移参考。

> [!WARNING]
> **当前版本不是生产交易机器人，不得用于真实资金。** Live 适配器、连续运行、权威账户风控和多腿故障补偿尚未达到开放门槛。即使提供 `--live` 和风险确认短语，程序也会失败关闭。Paper 结果不包含真实手续费、资金费率、滑点、撮合队列优先级或跨进程持仓。

## 项目定位

这个仓库正在把旧的 Python-first 交易系统收敛为一个边界清晰、默认安全的 Rust workspace。目前可用于：

- 验证现有 YAML/JSON 配置，并区分可执行、仅兼容解析、辅助文件和不支持配置。
- 对固定网格和分段套利策略进行确定性 paper one-shot 验证。
- 在提交模拟订单前执行产品身份、行情新鲜度、盘口深度和单批风险检查。
- 记录稳定批次 ID、client order ID、提交计划、回执和对账摘要。
- 开发并测试交易领域模型、策略、风险控制、交易所接口与运行时边界。

它暂时不适合：

- 连接私有交易 API 或管理真实资产。
- 启动 7×24 小时网格、套利监控、做量或价格提醒服务。
- 用 paper 成交结果推断真实交易收益或生产就绪程度。

## 当前能力

| CLI 命令 | 配置能力 | Paper one-shot | 连续运行 | Live | 当前行为 |
| --- | --- | --- | --- | --- | --- |
| `config-check` | 完整分类检查 | 不适用 | 不适用 | 不适用 | 检查文件或目录；对 public path loaders、public from_str loaders 和 shared raw reader 统一施加 1 MiB / YAML 读入护栏；发现不支持配置、读取错误或预算耗尽时非零退出 |
| `grid` | 校验网格配置 | 可用 | 不可用 | 不可用 | 仅在同时提供 `--once --price` 时模拟 resting paper orders |
| `arbitrage` | 校验套利与 monitor 配置 | 可用 | 不可用 | 不可用 | 要求显式双边价格、四侧盘口数量；`strategy_key` 是配置选择器，可与腿 symbol 不同，但腿仍需通过全局白名单和正的 `max_position_value` 风险门禁 |
| `monitor` | 可解析并校验 | 不可用 | 不可用 | 不可用 | 校验后明确报告运行时尚未实现并非零退出 |
| `volume-maker` | 可解析并校验 | 不可用 | 不可用 | 不可用 | 校验执行控制与策略配置后失败关闭 |
| `price-alert` | 可解析并校验 | 不可用 | 不可用 | 不可用 | 校验后失败关闭 |
| `scanner` | 检查显式配置路径 | 不可用 | 不可用 | 不可用 | 只做有界存在性 / 读取安全检查；不做 scanner schema/runtime validation；以非零状态退出 |

Lighter、Hyperliquid、Backpack、Binance、Paradex、EdgeX、GRVT、OKX 或 Variational
出现在配置或冻结的 Python tree 中，只表示兼容/迁移范围，不表示对应私有交易适配器已经可以实盘下单。
权威状态来自 `cargo run --locked -- capabilities --json`；人类可读投影见
[`docs/adapter-support.md`](docs/adapter-support.md)，其表格由合约测试与同一 manifest 保持一致。

## 核心特性

- **精确领域类型**：价格、数量和金额使用 `rust_decimal`，关键算术使用受检操作，不以二进制浮点承担交易计算。
- **纯策略内核**：固定网格、分段套利、价格提醒、做量和虚拟网格逻辑与 I/O 分离，便于确定性测试。
- **可验证的 PaperExchange**：覆盖订单状态、盘口深度消耗、GTC/IOC/FOK 语义、spot sell inventory 与 reduce-only capacity 的进程内预留、部分成交收缩、撤单释放和 flat position 清理；但不包含 cash/margin ledger、fees/funding/slippage/queue impact、持久化 / 跨进程持仓或风险预留。
- **失败关闭的执行边界**：未授权模式、过期行情、产品身份不匹配、深度不足、风险超限和未实现适配器都在提交前拒绝。
- **可审计执行历史**：先写入 `execution_planned`，再写入 `execution_completed`、`execution_partial` 或 `execution_incomplete`。
- **有界资源使用**：配置、历史记录、批次、actor 队列、HTTP 响应和聚合输出都有显式上限。
- **跨平台质量门禁**：CI 在 Windows 与 Ubuntu 上验证 MSRV 和 stable，并执行格式、Clippy、release build 与 RustSec audit。
- **冻结的迁移证据**：旧 Python tree 保留原始 blob 与文件 mode，但不参与当前构建、测试、配置加载或运行。

## 快速开始

### 前置条件

- Git
- [Rustup](https://rustup.rs/)
- Windows PowerShell、Linux 或 macOS

仓库通过 [`rust/rust-toolchain.toml`](rust/rust-toolchain.toml) 固定 Rust `1.85.0`，并声明 `rustfmt` 与 `clippy`。进入 `rust/` 后，Rustup 会自动选择所需工具链。

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
crypto-trading grid <CONFIG> [OPTIONS]
crypto-trading arbitrage [OPTIONS]
crypto-trading monitor [OPTIONS]
crypto-trading volume-maker [CONFIG] [OPTIONS]
crypto-trading price-alert [CONFIG] [OPTIONS]
crypto-trading scanner [OPTIONS]
crypto-trading config-check <PATHS>... [--json]
```

`--debug`、`--debug-detail` 和 `--no-ui` 目前主要保留 CLI 兼容性，尚不会改变对应 handler 的行为。运行时日志过滤由 `RUST_LOG` 控制，例如 PowerShell 中可设置 `$env:RUST_LOG = "debug"`。

需要独立二进制时：

```bash
cargo build --release --locked
./target/release/crypto-trading --help
```

Windows 对应文件为 `target\release\crypto-trading.exe`。

## 配置与凭证

当前配置位于 [`rust/config/`](rust/config/)：

```text
rust/config/
├── arbitrage/       # 套利策略、monitor companion 与迁移期配置
├── exchanges/       # 交易所字段映射和市场元数据
├── grid/            # 网格策略配置
├── price_alert/     # 价格提醒配置
├── volume_maker/    # 做量策略配置
├── logging.yaml
└── symbol_conversion.yaml
```

配置处理边界：

- 单份配置最大 `1 MiB`。
- YAML anchor/alias 不受支持。
- public path loaders、public `from_str` loaders 和 shared raw reader 共享同一套 `1 MiB` / YAML 读入护栏。
- Paper one-shot 只接受 `runtime-executable` strict schema；拼错或未消费字段会在写入 history 前失败。
- 批量检查最多保留 512 条摘要，文本与 JSON 输出各有 `1 MiB` 预算。
- 目录扫描或输出预算耗尽时会停止并非零退出，不会声称剩余文件已经检查。
- `scanner` 显式配置只做有界存在性 / 读取安全检查，不做 schema/runtime validation；它以非零状态报告未实现。

Rust 程序只读取**进程环境变量**，不会自动加载 `.env`。例如：

```powershell
$env:PARADEX_API_KEY = "..."
$env:PARADEX_L2_ADDRESS = "..."
$env:PARADEX_WALLET_ADDRESS = "..."
cargo run --locked -- config-check config/exchanges/paradex_config.yaml
```

凭证 loader 采用 `<EXCHANGE>_<FIELD>` 命名，可覆盖 `API_KEY`、`API_SECRET`、`API_PASSPHRASE`、`PRIVATE_KEY`、`JWT_TOKEN`、`API_KEY_PRIVATE_KEY`、`STARK_PRIVATE_KEY`、`WALLET_ADDRESS`、`SUB_ACCOUNT_ID`、`L2_ADDRESS`、`ACCOUNT_ID`、`ACCOUNT_INDEX` 和 `API_KEY_INDEX` 等字段。支持解析这些变量不代表对应 live adapter 已开放。

不要把密钥、私钥、JWT、助记词或真实账户信息写入已跟踪的 `rust/config/exchanges/*.yaml`、日志、issue 或提交历史。`.gitignore` 已屏蔽常见 `.env`、密钥文件和本地运行数据，但这不能替代提交前的凭证扫描。

## 安全模型与 Paper 限制

1. **Live 永远失败关闭**：风险确认短语只建立显式授权意图，不会绕过尚未完成的适配器、风控和对账门禁。
2. **每个进程从空 paper 账本开始**：当前 one-shot 不读取真实余额、真实持仓、未成交单或其他进程的 reservation；Grid 也不做账户级 pending-order risk reservation。
3. **网格是挂单语义验证**：它根据一个显式参考价规划 resting orders，不代表真实撮合、账户风险或跨进程仓位已经验证。
4. **套利使用显式顶层盘口**：调用方必须给出双边 bid/ask 和四侧可用数量；`strategy_key` 是配置选择器，不是腿 symbol 的别名；模型不等价于完整深度、延迟和滑点仿真。
5. **仓位上限不是账户预算**：`max_position_value` 按精确的 `(exchange, symbol, market_type)` 投影持仓逐腿校验，不限制整个批次或账户的总毛名义价值；`equity` 与 `available_balance` 也尚未参与资金或保证金门禁。
6. **Paper 不计算真实交易成本**：手续费、资金费率、网络延迟、队列优先级和市场冲击不在当前结果中。
7. **不完整执行不得直接重试**：先根据 history 中的批次、腿和对账摘要确认状态，否则可能重复提交。

更完整的门禁与剩余风险见 [`rust/RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md`](rust/RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md)。

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

Rust workspace 包含六个 crate：

| Crate | 职责 |
| --- | --- |
| `apps` | `crypto-trading` 二进制、CLI 参数、配置检查与 one-shot 编排 |
| `config` | 有界文件读取、兼容反序列化、严格校验、环境变量覆盖与凭证脱敏 |
| `domain` | Symbol、MarketSnapshot、Order、Position、Price、Quantity、Money 等领域类型 |
| `strategy` | 网格、分段套利、风险、价格提醒、做量和虚拟网格算法 |
| `exchange` | 统一异步接口、PaperExchange、公开 Binance 行情适配器、有界 actor 和 instrument rules |
| `runtime` | 执行模式、路由、批次、提交策略、部分结果对账和 JSONL history |

详细的兼容面与设计目标见 [`rust/RUST_REFACTOR_PLAN.md`](rust/RUST_REFACTOR_PLAN.md)。

## 执行历史与恢复

默认路径：

- Grid：`rust/var/history/grid-paper.jsonl`
- Arbitrage：`rust/var/history/arbitrage-paper.jsonl`

关键事件：

| 事件 | 含义 |
| --- | --- |
| `execution_planned` | 提交前持久化批次 ID、全部 client order ID 与恢复所需订单信息 |
| `execution_completed` | 预期回执齐全，套利全部腿均为 `Filled` |
| `execution_partial` | 提交过程报错且已有部分结果，同时保存有界对账摘要 |
| `execution_incomplete` | 提交返回，但结果数量或状态不足以判定完整成功 |

历史写入在构造时固定相对路径；单条、单批和单文件上限仍然存在，单文件达到 `64 MiB` 后失败关闭，目前不自动轮转。同一进程内的路径别名锁协调已改进，但没有跨进程 OS lock、轮转或 transactional saga。取消、进程死亡或抢占中断都可能留下 planned-only 状态，重试前必须先 reconcile。

## 项目结构

```text
crypto-trading/
├── .github/workflows/rust.yml   # 跨平台 CI 与 RustSec
├── rust/                        # 唯一活动项目
│   ├── crates/                  # Rust workspace 源码
│   ├── config/                  # 当前配置副本
│   ├── Cargo.toml
│   ├── Cargo.lock
│   ├── rust-toolchain.toml
│   └── README.md                # 详细操作与安全边界
├── archive/
│   ├── README.md                # 归档来源与完整性说明
│   └── python-legacy/           # 冻结的旧 Python tree
└── README.md
```

`rust/config/` 与 `archive/python-legacy/config/` 所有权不同。当前代码只能读取前者；不要在归档中继续开发，也不要让新代码、测试或 CI 依赖归档文件。

## 开发与验证

在 `rust/` 目录运行完整本地门禁：

```bash
cargo +1.85.0 check --workspace --all-targets --all-features --locked
cargo +1.85.0 fmt --all -- --check
cargo +1.85.0 clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +1.85.0 test --workspace --all-targets --all-features --locked
cargo +1.85.0 build --release --workspace --all-features --locked
cargo +1.85.0 test --doc --workspace --all-features --locked
```

CI 还会：

- 在 Ubuntu 与 Windows 上分别运行 Rust `1.85.0` 和 stable。
- 使用 `Cargo.lock` 固定依赖解析。
- 拒绝 Clippy warning。
- 执行 RustSec advisory audit。

## 常见问题

### 为什么 `config-check config --json` 返回非零？

`legacy-parseable` 本身仍可返回 `status: ok`；非零退出表示同一批次里至少还有一条 `status: error`，例如 `unsupported` 配置、读取失败或预算耗尽。先查看每条摘要的 `classification`、`status` 和 `error`。

### 为什么配置校验成功，但命令仍然报 `runtime is unavailable`？

配置解析与运行能力是两道独立门禁。`monitor`、`volume-maker`、`price-alert`、`scanner` 和无 `--once` 的 `arbitrage` 当前只验证输入，然后明确失败。

### 为什么 `--live` 仍然无法下单？

这是设计行为。真实账户风险事务、私有签名适配器、testnet 证据、权威 instrument metadata 和多腿恢复尚未全部完成。

### 为什么 YAML anchor/alias 被拒绝？

配置入口采用有界、可审计的严格解析边界，不允许 alias 扩展改变资源使用或隐藏真实字段。请展开为显式 YAML。

### 从仓库根目录运行时为什么找不到配置？

文档中的相对路径都以 `rust/` 为工作目录。先执行 `cd rust`，或显式传入 `rust/config/...`。

## 路线图

以下项目完成并提供验证证据前，Live 与连续运行仍保持 NO-GO：

- 权威账户余额、持仓、挂单 reservation、kill switch 与跨进程风险事务。
- 私有交易适配器的签名向量、testnet 下单/撤单和限流验证。
- 多腿补偿、恢复锁、durable saga 和崩溃后续作。
- 从权威交易所元数据加载 tick size、lot size 和最小名义价值。
- 真实手续费、资金费率、延迟、滑点与更完整的 paper 撮合模型。
- 历史轮转、跨进程日志协调、持续凭证扫描和许可证策略门禁。

## 参与贡献

提交 issue 或 pull request 前：

1. 只在 `rust/` 中开发当前功能；把 `archive/python-legacy/` 视为只读证据。
2. 新增行为时先补测试，并保持 live 路径失败关闭。
3. 不引入二进制浮点交易计算，不在日志和诊断中暴露凭证。
4. 运行“开发与验证”中的完整门禁。
5. 在 PR 中说明安全边界、已验证内容和未覆盖风险。

## 延伸文档

- [`rust/README.md`](rust/README.md)：详细命令、配置分类和安全边界。
- [`rust/RUST_REFACTOR_PLAN.md`](rust/RUST_REFACTOR_PLAN.md)：Rust-first 迁移目标、模块设计与验收门槛。
- [`rust/RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md`](rust/RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md)：审计复核、修复证据和剩余 NO-GO 项。
- [`archive/README.md`](archive/README.md)：旧 Python tree 的来源提交和完整性校验。
- [`LICENSE`](LICENSE)：仓库根目录的权威 MIT 许可证文本。

## 许可与免责声明

Rust workspace 的 package metadata 当前声明为 MIT，仓库根目录也已提供 [`LICENSE`](LICENSE)；正式再分发时以该文件为权威许可证文本。

本项目仅用于软件工程、策略研究和 paper 模拟，不构成投资建议。数字资产交易具有高风险，任何使用者都应独立评估并承担由配置、部署或交易决策产生的后果。
