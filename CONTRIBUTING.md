# 参与贡献 / Contributing

感谢你考虑为这个项目贡献代码。这是一个交易系统，正确性和安全边界优先于功能数量，
所以下面的规则比一般项目更严格。

*This project is a trading system. Correctness and safety boundaries take
precedence over feature count, so the rules below are stricter than usual.
Narrative documentation is written in Chinese; code, comments, commit messages,
and machine-checked documents are written in English.*

## 开始之前

- 安全问题**不要**开 issue，走 [`SECURITY.md`](SECURITY.md) 的私有通道。
- 较大的改动请先开 issue 讨论方向，避免写完才发现与安全边界冲突。
- 环境要求见 [`README.md`](README.md) 的「快速开始」。工具链由
  [`rust/rust-toolchain.toml`](rust/rust-toolchain.toml) 固定。

## 本地门禁

提交前，在 `rust/` 目录跑完整门禁。CI 跑的是同一组命令，本地跑绿可以避免来回：

```bash
cargo +1.89.0 fmt --all -- --check
cargo +1.89.0 check --workspace --all-targets --all-features --locked
cargo +1.89.0 clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +1.89.0 test --workspace --all-targets --all-features --locked
cargo +1.89.0 test --doc --workspace --all-features --locked
cargo +1.89.0 build --release --workspace --all-features --locked
```

`-D warnings` 不可协商。workspace 已开启 `unsafe_code = "forbid"` 和 clippy
`pedantic`，release 配置开启 `overflow-checks`。

## 不可跨越的边界

1. **`archive/python-legacy/` 已于 2026-08-13 从工作树移除。** 该树仅存在于
   此前的 Git 历史中([`archive/README.md`](archive/README.md) 为墓碑说明);
   代码和测试都不得依赖或复活其中任何内容。
2. **交易计算不使用二进制浮点。** 价格、数量、金额一律用 `rust_decimal`，
   关键算术使用 `checked_*`。
3. **凭证不得离开进程环境。** 不进日志、`--json` 输出、错误信息、`Debug`
   输出、journal、HTTP 响应或测试快照。新增的诊断输出要考虑这一点。
4. **新的外部权限路径必须失败关闭，并附带契约测试。** 未实现的能力要在
   capability 清单、CLI 和 Web 三处一致地拒绝，而不是静默降级。
5. **live 路径的不变量不可削弱。** 任何触及 `live-lifecycle`、`live-reconcile`
   或 mainnet 适配器（`rust/crates/exchange` 的 mainnet endpoints/adapter）的
   改动都必须附带契约测试，并保持：journal-first（任何 mainnet 变更前先持久化
   intent）、query-first（含糊结果与恢复先做签名查询，绝不盲目重提）、
   fail-closed（确认短语、`--max-notional` 上限、凭证分族、kill-switch 闩锁
   缺一即拒绝）。放宽任何一条门禁的 PR 需要在描述中单独论证并更新威胁模型。
6. **能力清单是权威。** [`docs/adapter-support.md`](docs/adapter-support.md)
   是 `rust/crates/runtime/src/capability.rs` 中 manifest 的人类可读投影，
   由契约测试保持同步。不要手工编辑表格；改 manifest，让测试更新期望。
7. **文档中的每个「可用」都要有测试或环境证据。** 没有证据时写「待验证」。

## 新增依赖

新依赖必须服务于当前这个改动，并在 PR 描述里记录选择理由和被拒绝的替代方案。
所有 cargo 调用都带 `--locked`，所以 `Cargo.lock` 的变化本身就是一次需要过完整
门禁的改动。许可证策略见 [`rust/deny.toml`](rust/deny.toml)。

## 测试

- 新行为先补测试。跨模块的行为放在 `crates/*/tests/` 的契约测试里。
- 断言尽量针对类型化的错误变体，而不是错误信息的字符串内容；字符串断言只用于
  CLI stderr，因为那里文本本身就是契约。
- 测试不得访问网络。HTTP 路径使用 `TcpListener::bind("127.0.0.1:0")` 起本地
  stub，或使用已签入的 fixture。
- 临时文件写到 `std::env::temp_dir()`，不要写进工作目录。

## 提交与 PR

提交信息用英文祈使句，描述这次改动带来的能力或约束，例如
`Keep testnet signing inside the approved dependency boundary`。

PR 描述请覆盖：本次改动的安全边界影响、已验证的内容、以及未覆盖的风险。
模板会提示这几项。

## 行为准则

参与本项目即表示你同意遵守
[`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)。
