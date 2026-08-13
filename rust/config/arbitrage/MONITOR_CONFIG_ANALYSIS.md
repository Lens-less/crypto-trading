# Monitor 配置消费状态（Rust）

当前 `crypto-trading-config` 会读取并校验以下 monitor 字段：

- `exchanges`：至少一个非空 exchange。
- `symbols`：至少一个 symbol。
- `thresholds.min_spread_pct`、`thresholds.min_funding_rate_diff`：不得为负数。
- `websocket.ping_interval`、`websocket.reconnect_delay`：必须为正数。
- `performance.analysis_interval_ms`、`performance.ui_refresh_interval_ms`：必须为正数。
- `health_check.interval`、`health_check.data_timeout`：必须为正数。

Arbitrage paper one-shot 会把 `exchanges`/`symbols` 作为提交白名单，并把 `health_check.data_timeout` 作为每个 instrument 的市场数据新鲜度上限。`paper-monitor-eth.yaml` 只包含这些已建模 section，可作为 strict companion；包含 `spread_history`、`debug_cli` 等未消费 section 的历史 monitor 文件继续分类为 `legacy-parseable`，不能进入 one-shot 提交边界。

```powershell
cargo run -- config-check config/arbitrage/paper-monitor-eth.yaml --json
```
