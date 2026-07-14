# 套利配置说明（Rust 当前实现）

套利 one-shot 同时使用两类配置：

- `arbitrage_*.yaml`：策略开关、阈值、数量、exchange/symbol 白名单及 `symbol_configs`。
- `monitor*.yaml`：允许采集和执行的 exchange/symbol universe，以及市场数据超时上限。

先检查配置：

```powershell
cargo run -- config-check `
  config/arbitrage/paper-once-eth.yaml `
  config/arbitrage/paper-monitor-eth.yaml `
  --json
```

## 执行控制

提交前必须全部满足：

1. 套利配置顶层 `enabled: true`。
2. `system_mode.monitor_only: false`。
3. 策略键存在于 `symbol_configs` 且 `enabled: true`。
4. 两条腿的 exchange 和 symbol 同时位于套利配置、monitor 配置及可选 CLI 过滤器的白名单内。
5. 每个订单对应的市场快照在 `health_check.data_timeout` 限制内。

若两条腿使用不同 symbol，必须显式提供 `--strategy-key`。同 symbol 两腿可省略，此时 symbol 本身就是策略键。

当前可覆盖并严格校验的 `symbol_configs` 字段包括：

- `enabled`
- `grid_config.initial_spread_threshold`
- `grid_config.grid_step`
- `grid_config.max_segments`
- `quantity_config.base_quantity`

布尔值和数值类型必须正确；例如字符串 `"false"` 不会被静默当作默认值。

## Paper 单次执行

```powershell
cargo run -- arbitrage `
  --config config/arbitrage/paper-once-eth.yaml `
  --monitor-config config/arbitrage/paper-monitor-eth.yaml `
  --once --strategy-key ETH-USDC-PERP `
  --left-exchange paper-left --left-symbol ETH-USDC-PERP --left-bid 99.9 --left-ask 100 `
  --left-bid-quantity 10 --left-ask-quantity 10 `
  --right-exchange paper-right --right-symbol ETH-USDC-PERP --right-bid 101 --right-ask 101.1 `
  --right-bid-quantity 10 --right-ask-quantity 10
```

可执行配置必须是 strict `runtime-executable` schema 并提供正数 `max_position_value`，策略项可用 `symbol_configs.<key>.risk_config.max_position_value` 覆盖。任何 ignored/unknown key（包括拼错的安全开关）都会在执行前拒绝；legacy 文件仍可由 `config-check` 清点。命令还必须显式提供四侧盘口深度；系统在写历史前按可执行侧聚合校验，未知或不足深度不会被解释为无限流动性。

执行会先同步写入 `execution_planned`（batch ID、全部 client order ID 和 legs），再用同一批次提交。只有全部腿均成交才写 `execution_completed`；部分执行写 `execution_partial`，确定但未全部成交写 `execution_incomplete`，并保持非零退出。

连续套利、独立 monitor runtime 和 live 下单尚未实现，调用时会失败关闭。历史 Python 命令不属于当前 Rust 运行面。
