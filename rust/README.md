# crypto-trading Rust

这是仓库唯一的当前运行项目。所有 Rust 源码、当前配置、构建输出和运行数据都收敛在本目录内；它不依赖 `../archive/` 中的任何文件。

## 快速验证

在本目录运行：

```powershell
cargo +1.85.0 check --workspace --all-targets --all-features
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```

## 命令行

```powershell
cargo run -- --help
cargo run -- config-check config/grid/lighter-long-perp-btc.yaml
cargo run -- grid config/grid/lighter-long-perp-btc.yaml --price 100000 --once
cargo run -- arbitrage --once --left-exchange paper-left --left-symbol BTC-USDC-PERP --left-bid 99.9 --left-ask 100 --right-exchange paper-right --right-symbol BTC-USDC-PERP --right-bid 101 --right-ask 101.1
```

可用子命令包括 `grid`、`arbitrage`、`monitor`、`volume-maker`、`price-alert`、`scanner` 和 `config-check`。

## 安全边界

- 默认执行模式是可重复验证的 paper 流程。
- 私有实时下单适配器尚未开放；请求不受支持的 live 流程会失败关闭。
- 凭证和运行状态不进入版本库。
- 当前配置只读取本目录的 `config/`；历史配置仅保存在归档中用于对照。

架构、兼容面和验收门槛见 [`RUST_REFACTOR_PLAN.md`](RUST_REFACTOR_PLAN.md)。
