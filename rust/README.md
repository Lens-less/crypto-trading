# crypto-trading Rust

这是仓库唯一的当前运行项目。Rust 源码、当前配置、构建输出和运行数据都位于本目录；运行时不依赖 `../archive/python-legacy/` 中的任何文件。

## 能力矩阵

| 命令 | 配置检查 | Paper 单次执行 | 连续运行 | Live 执行 | 当前行为 |
| --- | --- | --- | --- | --- | --- |
| `config-check` | 是 | 不适用 | 不适用 | 不适用 | 在 512 条摘要和 1 MiB 输出预算内聚合检查；任一路径不受支持或预算耗尽时非零退出，并用终止错误明确标出未检查的剩余路径 |
| `grid` | 是 | 挂单模拟 | 否 | 否 | 只有同时提供 `--once --price` 才生成并提交 resting paper orders；无执行参数时仅检查配置 |
| `arbitrage` | 是 | 是 | 否 | 否 | 单次执行要求显式价格与盘口深度，并校验启用开关、风险上限、监控白名单和 `symbol_configs` 策略键 |
| `monitor` | 是 | 否 | 否 | 否 | 验证配置后以非零状态报告运行时尚未实现 |
| `volume-maker` | 是 | 否 | 否 | 否 | 验证配置后以非零状态报告运行时尚未实现 |
| `price-alert` | 是 | 否 | 否 | 否 | 验证配置后以非零状态报告运行时尚未实现 |
| `scanner` | 路径存在性 | 否 | 否 | 否 | 以非零状态报告运行时尚未实现 |

`grid` 和 `arbitrage` 的 one-shot 执行会先持久化 `execution_planned`（批次 ID 及全部 client order ID/legs），再跨订单提交边界。套利批次只有全部腿均成交才写入 `execution_completed`；提交报错后的部分执行写入带自动对账摘要的 `execution_partial`，确定但未全部成交则写入 receipt 摘要明确的 `execution_incomplete`。后两者都会非零退出，且不得直接重试。

## 快速验证

在本目录运行：

```powershell
cargo +1.85.0 check --workspace --all-targets --all-features --locked
cargo +1.85.0 fmt --all -- --check
cargo +1.85.0 clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +1.85.0 test --workspace --all-targets --all-features --locked
```

## 命令示例

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

辅助配置必须同时满足保留文件名和最小内容 schema；文件名不能把交易配置伪装为 `auxiliary`。单份输入最多 1 MiB，YAML anchor/alias 不受支持；批量检查最多保留 512 条摘要，JSON 与文本输出各自最多 1 MiB。达到摘要数或输出预算后，检查会停止、追加终止错误并非零退出，不会声称剩余路径已经检查。

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

带 `--once` 的命令只接受 `runtime-executable` 的 strict schema；含未消费或拼错字段的 legacy 配置会在写 history 前失败关闭。flat 与 `default_config` 的兼容数值别名若同时出现，必须在 Decimal/整数语义上相等，否则按冲突失败关闭。若两条套利腿的 symbol 不同，必须显式传入 `--strategy-key`；该键必须存在于 `symbol_configs` 且 `enabled: true`。CLI、套利配置和 monitor 配置中的 exchange/symbol 白名单都会在生成意图后、提交前再次校验。可执行配置还必须提供正数 `max_position_value`（兼容嵌套 risk 配置，并可由策略项覆盖）；四侧盘口深度会在写入计划前按订单方向聚合校验，缺失或不足时失败关闭。CLI 和 config crate 的公开文件 loader 均拒绝超过 1 MiB 的单份配置。

## 凭证与环境变量

当前 Rust 程序只读取进程环境变量，不会自动加载 `.env` 文件。PowerShell 示例：

```powershell
$env:PARADEX_API_KEY = "..."
$env:PARADEX_L2_ADDRESS = "..."
$env:PARADEX_WALLET_ADDRESS = "..."
cargo run -- config-check config/exchanges/paradex_config.yaml
```

不要把密钥写入仓库。各 exchange YAML 仅用于字段映射；私有 live 适配器仍未开放。

## 安全边界

- 默认且当前唯一可下单的模式是可重复验证的 paper one-shot。
- 不受支持的连续或 live 路径一律失败关闭，不会以成功状态伪装为已运行。
- 所有已实现路径都会校验自身的配置与市场产品身份；arbitrage 还必须通过 `monitor_only`、顶层 `enabled`、策略键开关、单批仓位价值门禁、显式盘口深度、市场数据新鲜度和 instrument 白名单后才会提交。
- 单批仓位价值门禁只覆盖当前空仓 one-shot paper；它不代表跨进程仓位、挂单 reservation、多腿补偿或真实账户风控已经完成。
- Grid one-shot 当前用于验证网格规划与 paper 挂单语义，尚未接入权威账户风险或 pending-order reservation；不得据此开放连续或 live 网格。
- 历史 Python 实现冻结在 `../archive/python-legacy/`，不得作为当前 Rust 入口。

架构、兼容面和验收门槛见 [`RUST_REFACTOR_PLAN.md`](RUST_REFACTOR_PLAN.md)。
