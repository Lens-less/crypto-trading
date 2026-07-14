# Lighter Grid 使用说明（Rust）

当前入口是 `crypto-trading grid`。先确认配置分类：

```powershell
cargo run -- config-check config/grid/lighter-long-perp-btc.yaml
```

现有 Lighter 配置通常会被标记为 `legacy-parseable`，因为健康检查、止盈止损、价格锁、scalping 等历史字段尚未被 Rust 运行时消费。分类为可解析不等于这些功能已经生效。

现有 Lighter 文件只可作 legacy 配置检查。Paper 单次挂单模拟改用 strict profile：

```powershell
cargo run -- grid config/grid/paper-once-btc.yaml --once --price 110
```

`--once` 与 `--price` 必须成对出现。提交前会持久化 `execution_planned`；成功提交全部挂单后写入 `execution_completed`。当前命令不提供盘口深度，所以这些 receipts 应保持 `Open`，而不是被表述为成交；它也未接入账户仓位、pending reservation 或集中风险授权，只能用来验证规划与挂单语义。部分提交会写 `execution_partial` 并非零退出。

当前不支持连续运行或 live 下单，因此不需要 Lighter 私钥。若后续开放私有适配器，凭证也必须来自进程环境变量；程序不会自动加载 `.env`。
