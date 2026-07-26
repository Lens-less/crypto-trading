# Rust 项目审计修复说明（2026-07-17）

> 证据快照：以下结论记录的是 2026-07-17 在 Windows 上的验证结果。它们是当日状态，不是对未来状态的保证。
>
> 范围：仅 `rust/` 活动工作区。`archive/python-legacy/` 只是冻结的只读证据，不参与当前构建、测试或运行。

## 结论

- 以最小改动修复了会影响执行语义、风控门禁、历史恢复和文档事实性的缺陷。
- 采用 first principles / Occam：凡是能被一条共享契约解释的，就不拆成多层适配器特例；能用行为保持的修复，就不做更大范围重构。
- 当前仍是 paper-first / fail-closed 基线，未解除 NO-GO。

## 已修复项

| 领域 | 修复 |
| --- | --- |
| arbitrage | `strategy_key` 作为配置选择器，允许与腿 symbol 不同；有效配置必须有正的 `max_position_value`，腿仍受全局白名单约束。该上限按精确 `(exchange, symbol, market_type)` 投影持仓逐腿执行，不是批次或账户总毛敞口预算。 |
| 风控 / 持仓 | malformed position、同一产品同时出现 long/short 等矛盾状态现在失败关闭，同时复用既有公开拒绝类型，未扩大公共枚举。 |
| grid | 修正 gap crossing / geometry 处理；不引入账户级 pending-order reservation。 |
| Paper ledger | spot sell inventory 与 reduce-only capacity 在进程内按订单优先级预留，部分成交后收缩、撤单后释放；同时修正 flat row、position capacity、entry price 和 reversal 回归。 |
| Binance / adapter | 非成功响应保留 HTTP status 与 Binance code/message；JSON 与非 JSON 原因统一按 UTF-8 边界截断，完整 reason 不超过 256 bytes。 |
| config | 所有公开字符串 loader 与文件入口统一 1 MiB / YAML 护栏；exchange symbol 反向映射大小写归一。 |
| history | 历史相对路径在构造时固定；同进程 alias lock 改进；补齐批次精确上限、超一字节不落盘和跨 adapter partial reconciliation 测试。 |
| CLI / scanner | Grid/Arbitrage 只有在执行与 history 写入成功后才打印成功标记；scanner 明确只做有界文件访问 / 输入安全检查，不做 schema/runtime validation，且非零退出。 |
| LICENSE / CI | 新增并链接根 MIT `LICENSE`；CI path filters 现在覆盖许可证、根 README、重构计划和审计报告。 |
| config reader | 去重 public loader/raw reader 逻辑，减少重复入口导致的行为分叉。 |

## 回归证据

- 2026-07-17 的 Windows 证据显示：`cargo +1.85.0 check/fmt/clippy/test --locked`、release build、doc tests 通过。
- 同日 stable 1.97.0 的 `check/clippy/test` 通过。
- `cargo audit` 扫描 217 个依赖，0 known vulnerabilities；这不消除下文所列的已知设计与供应链风险。
- 这些结果只说明当日状态，不表示后续提交不会引入新问题。

## 显式 NO-GO

- live / continuous / private adapters。
- account cash / margin / equity / batch-or-account global gross budget gate；现有 `max_position_value` 只约束单一精确产品投影。
- durable multi-leg saga、执行取消 / 进程死亡恢复与跨进程 history locking / rotation。
- full market-cost model，包括 fees、funding、slippage、queue impact。
- global broadcast subscription 对无关 symbol 可能滞后。
- `serde_yaml` / `unsafe-libyaml` 仍保留，因未授权新增依赖，替换延后。
- apps/config schema 分类与 strategy->config 适配耦合仍是架构债，已刻意推迟大改。
- scanner 的 runtime/schema 能力仍未提供。
- archive-only scripts 可能在手工运行时打印秘密；未发现真实 tracked credentials，归档仍冻结。

## 验证命令

```powershell
cargo +1.85.0 check --workspace --all-targets --all-features --locked
cargo +1.85.0 fmt --all -- --check
cargo +1.85.0 clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +1.85.0 test --workspace --all-targets --all-features --locked
cargo +1.85.0 build --release --workspace --all-features --locked
cargo +1.85.0 test --doc --workspace --all-features --locked
cargo +stable check --workspace --all-targets --all-features --locked
cargo +stable clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +stable test --workspace --all-targets --all-features --locked
cargo audit --file Cargo.lock
```
