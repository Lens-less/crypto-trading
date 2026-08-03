# 整改复查闭环报告

> 日期：2026-08-03
>
> 输入：[`2026-08-03-post-remediation-reaudit.md`](2026-08-03-post-remediation-reaudit.md)
>
> 执行计划：[`2026-08-03-remediation-plan.md`](2026-08-03-remediation-plan.md)
>
> 固定审查基点：`b1c8734`

## 结论

复查中的 B1–B4 已全部按“journal 是唯一真相、未实现能力必须失败关闭”的原则修复。整改同时覆盖了可以在不引入新依赖、不改变外部交易权限的前提下安全落地的高危、中危项。没有通过覆盖 `risk` 或 `capabilities` golden fixture 来隐藏兼容性问题：旧 v1 风控事实由版本化重放兼容，能力清单则直接降级为真实的 `Unavailable`。

本次没有把 checkpoint/retention、保证金/强平、生产策略回测同构等不同风险域硬塞进同一个数据格式迁移。这些项目需要新的持久化 manifest、游标代际或产品语义，贸然实现会让现有 journal、幂等身份或外部权限边界不可逆地改变。它们在下文逐项保留为显式边界，不计入本次已完成宣称。

## 阻断项闭环

| 项目 | 状态 | 闭环证据 |
|---|---|---|
| B1 门禁红灯 / 内存领先磁盘 | 已修复 | alert 的 `delivered`、`failed`、`timed_out` 只在终态事实 append 成功后递增；取消、panic 与 append 失败均有精确危险窗口测试；整批共享一个绝对停机期限。 |
| B2 append 热路径全目录枚举 | 已修复 | 只定点探测规范 `.1`–`.64`；100、10,000 个无关邻居不会扩大 probe 数；规范 gap、非普通文件、超限仍失败关闭。 |
| B3 在途 admission 永久泄漏 | 已修复 | 新 v2 fact 持久化 exact ticket 和 5 分钟 wall-clock lease；过期写入显式 compensation fact；第 65 条在写盘前拒绝；冷重放允许 4,096 条旧泄漏用于自修复。旧 v1 admission 从 envelope event ID 和 timestamp 确定性派生 ticket/lease。 |
| B4 虚假 research capability | 已修复 | `research.indicators`、`research.backtest` 保留稳定 ID，但在 manifest、CLI、HTTP、fixture、README 与 adapter 文档中一致为 `Unavailable`；所有 `Available` capability 必须引用真实存在的已出货边界。 |

## 高危项处置

| 项目 | 处置 | 说明 |
|---|---|---|
| H1 默认无日志 | 已修复 | 容器提供 scoped `RUST_LOG`/`RUST_BACKTRACE`；记录生命周期、owner 转换、首次 degraded 与错误，不记录 token、payload 或每次成功 append。 |
| H2 只按长度相信 authority cache | 有界修复 | authority 提供显式持久态等价校验，并且每 64 次 cache-backed refresh 强制冷校验；完整冷重放比较 Paper、risk、open admissions、sequence 与历史 reservation identity，完成后重查 head；校验发现同长度篡改会永久 latch `Degraded`。仍保留最多 63 次缓存刷新才检测到等长篡改的有界窗口。 |
| H3 无 checkpoint / 冷重放 O(N²) | 部分修复 | 冷重放直接构建 terminal identity map，消除了保留全部 reservation 再反复扫描的二次复杂度。持久 checkpoint 尚未引入，见“保留边界”。 |
| H4 4 GiB journal 硬顶 | 保留边界 | 当前继续在容量上限失败关闭。安全 retention 必须先有 state-complete checkpoint、版本化 chain manifest、全局 sequence/offset floor 与 reader generation pinning。 |
| H5 余额恒等式 | 部分修复 | 风控总余额统一使用 exact settled equity，保护读取 degraded 投影时失败关闭；未实现 PnL、保证金、资金费率与强平仍明确不可用。 |
| H6 回测指标口径 | 已修复 | 胜率/profit factor 只看已关闭数量的净 PnL，并按比例分摊双侧费用；年化从 tape 数量/跨度推导；方差使用 checked 在线算法。 |
| H7 回测成交真实性 | 安全降级并增强 | 单标的身份、bid/ask、taker 对手价、现货买力已强制；maker/limit typed reject；walk-forward 真正逐训练窗选策略且只返回 OOS。尚未共享生产 matcher/`StrategyMachine`，所以能力保持 `Unavailable`。 |
| H8 并发 receipt 乱序 | 已修复 | 按单调完成时刻发射并以 route index 破同刻平局；receipt timestamp 保持事实值；projection high-water 单调但不伪造事件时间。 |
| H9 节流失效 | 主要路径已修复 | 下一轮从上一轮完成时刻计算；保留任意长度 `Retry-After`；429/418 分类；Hyperliquid due routes 合并成一次全 universe 请求。Binance header-driven 动态 token bucket 仍是后续增强。 |

## 中危项处置

| 项目 | 状态 | 处置摘要 |
|---|---|---|
| M1 | 已修复 | rolling Welford add/remove，覆盖高价低波动输入。 |
| M2 | 已修复 | lag 保留 oldest-to-latest 有界窗口并累计精确 drop count；task schema v2，reader 兼容 v1。 |
| M3 | 已修复 | append receipt registry 改为 per-path `Arc` + 全局 `Weak`，清理死亡路径。 |
| M4/M5 | 已修复 | CHANGELOG 明确 Paper authority 放宽及 accounting 语义变化，新增子系统归入 Added/Changed。 |
| M6/M7 | 已修复 | owner 并发 drain，共享 60 秒期限；Compose 70 秒；CI 用两个经鉴权的 writable Paper owner 验证 SIGTERM、SSE 与 exactly-once stop facts。 |
| M8 | 有界修复 | 小型未变 journal 使用完整前缀验证后的常驻投影；大 journal 主动绕过 cache，仍全量读取以避免不受控驻留内存。 |
| M9 | 已修复 | risk facts 校验 journal ID、outer scope/strategy、exact ticket 与正 notional。 |
| M10 | 已修复 | 保护性读取使用 fail-closed decision snapshot；degraded 不再静默进入保护机。 |
| M11 | 部分修复 | B3 可重启自修复，transient refresh 可重试，首次 degraded 有错误日志；可疑持久态等价失败仍故意 latch，必须人工调查。 |
| M12 | 已修复 | 明确 `LocalReceipt`/`VenueEvent` provenance；REST 不再伪造 venue time 或 latency。 |
| M13 | 已修复 | `max_pair_skew_ms` 成为受校验配置，所有签入 profile 显式设置；monitor 等待/评估用不同门限。 |
| M14 | 已修复 | walk-forward runner 每个训练 slice 独立选参，只执行/返回样本外区间。 |
| M15 | 已修复 | ATR/EMA warm-up 为 `Option`，第 `period` 根用窗口均值 seed，溢出不污染状态。 |
| M16 | 已修复 | tape 保留 exchange/symbol/market/bid/ask，拒绝混合标的，同 timestamp 保序。 |
| M17 | 已修复 | 指标返回 `Result<Option<_>>`，区分 unavailable 与 arithmetic failure。 |
| M18 | 已修复 | quarantine 独立目录，最多 16 文件/64 MiB，仅管理规范普通文件，同步成功后才能截断。 |
| M19 | 已修复 | damaged-tail 全量分类整体移入 `spawn_blocking`。 |
| M20 | 已修复 | 删除死的 history error 分支，使用 typed repair outcome 与真实成功/失败 telemetry。 |

## 额外完成项

- `PaperAccountAuthority::reserve` 按事先保存的 reservation ID 精确回读，不再认领 `.last()`。
- `SimClock` 是 backtest strategy context 的唯一时间来源。
- SIGINT/SIGTERM 在任何 task spawn 或 journal 写入前同步注册。
- release/dev 保留 `line-tables-only` 调试信息且 release 不再 strip symbols。
- 新增无第三方依赖的 journal directory-scaling harness；在 0/100/10,000 个邻居下 append 均值未随目录规模增长。

## 有意保留的安全边界

以下事项不是本次实现遗漏，而是需要独立协议、迁移或产品决策：

1. **Authority checkpoint + retention。** 必须作为一套版本化持久化协议落地；否则删除前缀会改变 cursor offset/event ID，并破坏 forever-idempotency。当前继续容量到顶即失败关闭。
2. **未实现 PnL、保证金、资金费率与强平。** Paper settled equity 不冒充交易所账户净值，mainnet 权限不扩大。
3. **生产策略与回测同构。** 当前研究 engine 不支持 maker queue、部分成交、深度、延迟或永续保证金，因此 manifest 保持不可用。
4. **确定性 strategy plan / runtime ID 分配。** 需要跨 `domain → strategy → runtime → apps` 的独立兼容改造，不能在 journal/schema 安全补丁中混入随机 ID 语义迁移。
5. **Risk policy durable binding。** 需要先决定 legacy scope 的 adopt/reject 迁移，以及 policy 是否允许带审计的 rebind；不能由“首个启动进程”静默决定权威 policy。
6. **新依赖工作。** Criterion、WebSocket、proptest、SHA/HMAC 与 YAML parser 替换均未获本次依赖授权。本次性能基线使用 workspace 内无依赖 harness。
7. **大型结构重构。** `apps`/`tasks` 拆分、integration test binary 合并与依赖反转应在行为修复之后单独提交，以保持回滚与审查边界。

## 验证

仓库规定的六道本地门禁全部通过：

| 门禁 | 结果 |
|---|---|
| `cargo +1.89.0 fmt --all -- --check` | 通过 |
| `cargo +1.89.0 check --workspace --all-targets --all-features --locked` | 通过 |
| `cargo +1.89.0 clippy --workspace --all-targets --all-features --locked -- -D warnings` | 通过，0 warning |
| `cargo +1.89.0 test --workspace --all-targets --all-features --locked` | 通过，0 failure |
| `cargo +1.89.0 test --doc --workspace --all-features --locked` | 通过 |
| `cargo +1.89.0 build --release --workspace --all-features --locked` | 通过，产物包含调试信息；Windows 同时生成两个 PDB |

此外，`git diff --check` 与 workflow/Compose YAML 解析通过；以 `b1c8734` 为固定点的 Standards 与 Spec 双轴复查在提交前执行，结果记录在 PR 描述中。

本机没有 Docker，因此容器内双 owner SIGTERM 演练由 GitHub Actions 执行；本地已经验证 workflow/Compose YAML、停机单测、SSE 终止和 dispatcher 并发 drain。
