# Grid 配置（Rust 当前入口）

本目录中的 YAML 是从历史实现迁移来的配置。当前 Rust CLI 能解析核心网格字段，但许多保护、通知、健康检查和 scalping 字段仍未接入运行时；用 `config-check` 查看每个文件的精确分类和被忽略字段。

```powershell
cargo run -- config-check config/grid/paper-once-btc.yaml
```

- `runtime-executable`：当前字段已被完整消费。
- `legacy-parseable`：核心字段可解析，但输出会列出未消费字段；不能把它理解为完整复现了历史策略。

只清点 legacy 配置时，不传 `--once` 和 `--price`；它会打印分类，但不会执行：

```powershell
cargo run -- grid config/grid/lighter-long-perp-btc.yaml
```

Paper 单次挂单模拟必须同时传入 `--once` 与 `--price`：

```powershell
cargo run -- grid config/grid/paper-once-btc.yaml --once --price 110
```

`--once` 只接受不含 ignored/unknown keys 的 `runtime-executable` 配置；现有 Lighter/Paradex 等 legacy 文件仍可检查，但不能直接提交 paper 挂单。CLI 会在提交边界前持久化批次与全部 client order ID，并生成当前 paper 账本中的 resting/open orders；它不注入盘口深度，不把订单伪装成成交，也没有接入账户仓位、pending reservation 或集中风险授权。因此这条命令只证明网格规划与挂单语义，不是成交或账户级风险验收。当前没有连续 grid runtime，也没有 live 私有适配器。历史 Python 入口已经冻结在 `../../../archive/python-legacy/`，不是本目录的运行方式。
