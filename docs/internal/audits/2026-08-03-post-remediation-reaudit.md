# 整改后复查报告

> 日期：2026-08-03
> 复查范围：`a64c4f2..b1c8734`（10 个提交，92 文件，+13336 / −1044）
> 依据：[`2026-08-02-sub-hf-quant-evolution-spec.md`](../specs/2026-08-02-sub-hf-quant-evolution-spec.md)
> 方法：5 路并行代码复查 + 本机实测（门禁执行、可证伪实验、目录枚举压测），关键结论逐条人工验证

---

## 0. 总体判定

**整改做了大量真实且高质量的工作**，其中若干项完全达标：

- 额度回收（`settle_execution` + FIFO lot 释放）**真正修好了**，测试覆盖到「1024+8 笔预留-释放循环后仍能继续开仓」
- 写序正确：**先落盘后改内存**，内存永不领先磁盘（`history.rs:708-712` → `paper_account.rs:1186-1205`）
- 并发安全做得干净：锁粒度、路径别名归一、无 std Mutex 跨 await、无锁序倒置、TOCTOU 已闭合
- SIGTERM 处理完整（Unix/Windows 四路信号 + `init: true` 让 tini 当 PID 1，这是真正的根因修复）
- 崩溃尾自愈（隔离 + 截断 + UTF-8 中断尾的三重保守判定）方向正确
- 并发轮询把 10 标的刷新周期从 ~10.5s 降到 ~1.0s，**实打实的 10 倍改善**
- 没有任何虚假的实时/流式行情宣称，依赖约束被严格遵守

**但复查发现 4 个阻断级问题，其中 3 个是整改新引入的**，且 `main` 分支当前带着红灯。

### 门禁实测

| 门禁 | 结果 |
|---|---|
| `cargo fmt --check` | ✅ 通过 |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | ✅ 通过（0 warning） |
| `cargo test --workspace --all-targets --all-features --locked` | ❌ **921 通过 / 1 失败** |

仓库自己的纪律是「五门禁全绿即提交」。这条纪律被破了。

---

## 1. 阻断级问题

### B1. `main` 上有必现失败的测试

**性质**：整改引入的回归（由 B2 放大）

`crates/apps/tests/alert_runtime_contract.rs:450` 的
`local_and_deterministic_adapters_expose_typed_notices_and_bounded_failures` **连续 5 次全部失败**：

```
left: 1, right: 2
```

症状极具诊断价值：`probe.deliveries() == 2` ✅、`status.failed == 2` ✅、`status.delivered == 2` ✅ —— **内存计数说发生了 2 次失败，journal 只落了 1 条**。

**根因已用可证伪实验锁定**（未改动仓库任何字节，仅切换进程临时目录）：

| 条件 | TEMP 条目数 | 结果 | 耗时 |
|---|---|---|---|
| 原状 | 26,857 | FAILED 5/5 | 0.60–0.81 s |
| `TMP`/`TEMP` 指向空目录 | 0 | **ok** | **0.05 s** |

机制链条：
1. B2 让每次 journal append 付出一次全目录枚举（本机 ~90 ms）
2. 测试的 `shutdown_grace` 只有 200 ms（`alert_runtime_contract.rs:820-826`）
3. `NotificationDispatcher::stop()`（`apps/src/alert/notification.rs:350-368`）对所有 worker **共享同一个 deadline**，`deterministic` 只能吃 `local` 用剩的时间
4. grace 耗尽 → `join.abort()` 丢弃正在 `await` 的 `history.append()` → 记录永不落盘
5. 而 `status.failed` 在 append **之前**就已自增（`notification.rs:443-448`），且 `is_err()` 分支从未执行，连 `status_persistence_failures` 都不会 +1

**这不只是测试问题**：它证明了一条真实的语义违反 —— 系统对自己的自查报告可以是假的。对一个信任模型建立在「journal 是唯一真相」之上的仓库，这是要害。

**修复**：
1. 修 B2（治本）
2. `stop()` 改为每个 worker 独立 grace，而非共享 deadline
3. 「计数」与「落盘」顺序反转，或在 abort 后把差额显式记入 `status_persistence_failures`

---

### B2. `inspect_sealed_segments` 被改成全目录枚举，成本从 O(64) 变为 O(目录条目数)

**性质**：**新引入的缺陷**（整改把一个有界问题换成了无界问题）

整改前是对 `.1`..`.64` 的定点探测 —— 64 次 syscall，但**与目录大小无关**。现在是：

```rust
// rust/crates/runtime/src/history.rs:240
let entries = match std::fs::read_dir(parent) {
// :251
for entry in entries {
```

调用点仍在每次 append 的必经之路上（`history.rs:726` → `open_active_within_chain_budget`），而 `inspect_chain_head`（`history.rs:1449`）走同一函数，且它在 paper 账户的**每次预留热路径**上被调用（`paper_account.rs:1529/1544/1558/1578` → `authority_state.rs:185`）。一次「预留 + 落 planned」要付 2～3 次全目录枚举。

**本机实测的规模曲线**：

| 目录条目数 | 单次枚举耗时 |
|---|---|
| 100 | 0.47 ms |
| 1,000 | 2.40 ms |
| 10,000 | **90.68 ms** |

**影响的诚实界定**：生产默认 journal 在 `var/history/`，条目数通常只有几十（≤63 个封存段 + 少量隔离文件），实测成本 &lt;1 ms —— **正常部署下影响温和**。真正的问题是三重的：

1. 成本从**有界**变成**由环境决定**，且落在最热的路径上
2. `.quarantine` 文件永不清理（`history.rs:1476-1503`，全仓无任何代码删除它们），与 journal 同目录堆积 → **崩溃恢复越多，append 越慢**，形成正反馈
3. fail-closed 打击面扩大：现在任何同名数字后缀的邻居文件（`decisions.jsonl.0`、`.999999`）都会让 `inspect_sealed_segments` 报错（`history.rs:261-265`），而它在 append 路径上 —— **一个无关文件可以让整个交易系统永久停写**

**修复**：恢复定点探测，或把封存段状态缓存在 `JsonlHistory` 内（封存段只在本 writer 持 lease 触发 rotation 时变化，完全可维护内存状态）；隔离文件移入子目录并加保留策略。

---

### B3. 在途准入泄漏无 TTL，累积 64 条后整个 authority 永久砖化且重启无法恢复

**性质**：**新引入的缺陷**（`open_admissions` 是本次新增机制）

```rust
// rust/crates/runtime/src/account_risk.rs:1029-1039
if pending.iter().filter(|e| e.scope_id == scope_id).count()
    >= MAX_ACCOUNT_RISK_SCOPE_POSITIONS      // = 64
{
    return Err(AccountRiskProjectionError::OpenAdmissionLimitExceeded { .. }.into());
}
```

而冷重放路径把这个错误直接升级为整体降级：

```rust
// rust/crates/runtime/src/authority_state.rs:437-438
account_risk::apply_open_admission_event(&mut open_admissions, event.payload())
    .map_err(|_| AuthorityStateError::Degraded)?;
```

在途准入只有三条出口（ticket 匹配的 `AdmissionCancelled`、task_id 匹配的 `PositionClosed`、前缀匹配的 `PAPER_ACCOUNT_RESERVED`）。`OpenAdmission` **带 `recorded_at` 字段但全仓无一处用它做超时判定**（已验证）。

**后果**：`admit` 落盘后、`reserve` 之前进程崩溃 → 该准入永久留在投影里，持续压低所有 owner 的可用敞口。累积 64 条后 `refresh()` 永久返回 `Degraded` → paper + risk 两个 authority 同时失效 → **而冷重放会撞同一个错误，重启无法自愈**，只能人工编辑 journal。

`open_positions` 同理（同一常量，`account_risk.rs:365-369`）：owner 崩溃时不写 `position_closed`，累计 64 个僵尸持仓时钟即砖化。

**这是整改前不存在的失效模式，也是当前唯一一条重启都救不回来的路径。**

**修复**：给 `OpenAdmission` 加 TTL 或启动时自动补偿；超限时不要把**整个** projection 变 Degraded，至少区分「风控准入不可用」与「账户不可写」。

---

### B4. capability manifest 宣称两个 `Available` 能力，但零调用入口

**性质**：**虚假宣称**（违反仓库核心纪律）

```rust
// rust/crates/runtime/src/capability.rs:759-762, 775-778
"research.backtest",  CapabilityArea::Research, CapabilityLevel::Available,
"research.indicators", CapabilityArea::Research, CapabilityLevel::Available,
```

**实测验证**：
- `crates/apps/Cargo.toml`、`web-app`、`web` **都不依赖** `crypto-trading-backtest` / `crypto-trading-indicators`
- 全仓 `crypto_trading_backtest|crypto_trading_indicators` 在这两个 crate 之外**零命中**
- 没有 CLI 子命令、没有 HTTP 端点

`capabilities --json` 会向操作者（以及消费同一 manifest 的 Web Integrations 页）报告一个**不存在的可用能力**。`README.md:32` 更写着「**运行**增量指标、确定性事件带回测」，而操作者除 `cargo test` 外无法运行。

这直接违反 `MEMORY.md` 记录的硬纪律：**「文档不能宣称比 CLI 更多的权限」**。

雪上加霜的是，本该拦住它的门禁拦不住 —— `capability_contract.rs:392-422` 新增的断言全是字符串包含检查（`summary.contains("out-of-sample-only")`），没有任何存在性校验；而唯一的 evidence 文件存在性门禁（`:768-810`）**只遍历 `manifest.adapters`，不覆盖 `manifest.capabilities`**。

另：四处同步纪律也破了一处 —— `rust/README.md:11-27` 的能力矩阵没有 research 行。

**修复**：二选一 —— (a) 降级为反映真实状态的 level 并改 README 措辞为「以库形式提供」；(b) 补 `backtest` CLI 子命令并四处同步。同时给契约测试加两条通用断言：每个 capability 的 evidence 路径必须存在；每个 `Available` 必须有对应 CLI 子命令或显式标注 `library-only`。

---

## 2. 高危问题

### H1. 默认部署下日志输出为空 —— 可观测性形式存在，实质仍为零

**实测统计**：`error!` **0** / `info!` **0** / `trace!` 0 / `#[instrument]` **0**，只有 `warn!` 10 个 + `debug!` 4 个。

两个 subscriber 都用 `EnvFilter::from_default_env()`（读 `RUST_LOG`），而 `Dockerfile` 和 `deploy/compose.yaml` **都没有设 `RUST_LOG`**（已实测确认）。缺省下 `EnvFilter` 最多放行 `ERROR` 级 —— 而代码里没有任何 `error!`。

**结论：`docker compose up` 起来的生产候选容器，日志是空的。** compose 还配了 `json-file` 10m×5 轮转，轮转的是一个不会被写入的流。整改前「可观测性为零」这一问题，**在默认部署路径下依然成立**。

更糟的是 CHANGELOG 宣称 tracing 覆盖了 "exchange dispatch"，但该埋点在 `exchange/src/bounded.rs:505`，位于 `BoundedExchangeHandle` 内部 —— 而 `.bounded(` 在 `crates/*/src` 中**零调用**（已实测）。生产下单路径走 `PaperAccountAuthority` + `JsonlHistory::append`，从不经过它。**这条日志永远不会输出。**

叠加 `strip = "symbols"` 未改（`Cargo.toml:58`），panic 时既无日志上下文也无符号化 backtrace。

### H2. 内存↔磁盘无交叉校验，一致性判据仅为「文件字节长度」

spec 第 388 行把「保留低频后台交叉校验，不一致时 fail-closed」列为引入内存投影的**唯一风险缓解措施**。**未实现**（全仓无相关代码）。

`HistoryChainHead` 只有 `sealed_segment_bytes` 和 `active_bytes`（`history.rs:362-366`），`inspect_chain_head` 只做 `metadata().len()`。**无内容摘要、无 mtime、无 inode**。任何**等长**的原地覆写（外部编辑、备份还原、文件系统回滚）都会被判为 `Same`（`authority_state.rs:192-197` 直接返回缓存不碰磁盘），内存与磁盘静默分叉。

delta 路径的字节校验很严格（`authority_state.rs:482-499` 用 `serde_json::to_vec` 重算比对），**这部分做得好**；但它只校验「我们写的字节数对得上」，不校验「盘上的内容是我们写的」。

`PaperAccountReadModel` 与 `AccountRiskReadModel` 都已 `derive(PartialEq)`，实现成本很低。

### H3. 无快照/checkpoint，冷重放对历史预留是 O(N²)，历史索引无上限

spec 1.2 要求的 `<journal>.snapshot.N` **未实现**。`journal_reader.rs:741-759` 的 `AnchorCheckpoint` 只是游标定位锚点，不是状态快照。

更严重的是新引入的复杂度问题：

```rust
// rust/crates/runtime/src/authority_state.rs:425-426
let mut paper_all = paper_account::ProjectionBuilder::new(snapshot.journal_id())
    .retain_terminal_reservations(true);
```

`retain_terminal_reservations(true)` 使 journal 里出现过的**每一笔预留永久留在 Vec 里**，而每条事件的处理都是对该 Vec 的全扫描（`paper_account.rs:1978-1986`），`settle_execution` 分支还带一次全量深拷贝（`:2091-2096`）。历史索引三张 HashMap 同样无界（`authority_state.rs:56-60`）。

**净效果：稳态很快，但每次重启的代价随运行时长平方增长。** 这抵消了整改想解决的问题。

### H4. journal 4 GiB 硬顶完全未动，7×24 目标仍不可达

`history.rs:31-38` 的常量一行未改，全仓**无任何封存段删除/归档/淘汰代码**。写满后 `append_batch` 永久返回 `ChainTooLarge`，所有 fail-closed 边界同时触发。

spec 阶段 1.7 明确列出此项，未做。

（反面：因为不淘汰，「读者读到一半段被删」的竞态不存在 —— 这是唯一被正确保住的性质，代价是根本没实现保留策略。）

### H5. 余额恒等式只修了一半

新公式确实引入了 PnL 记账（`paper_account.rs:2120-2125` 的 `settled_equity_base`），但风控读到的余额是分支的：

```rust
// rust/crates/runtime/src/account_risk.rs:1112-1117
let account_balance = match account.ledger_kind {
    LegacyReservationOnly => checked_add(account.available, account_exposure)?,
    ExactExecution        => account.settled_equity_base,
};
```

两个残留漏洞：

1. **Legacy 分支恒等式原封不动**。`available + exposure = (settled_equity_base − held) + held = settled_equity_base`，而 `ledger_kind` 只在首次 `settle_execution` 时翻转。所以首笔成交前的账户、以及只走 `reserve → commit → reconcile_release` 的路径，`min_balance_close` **依旧永不触发**。
2. **即使翻转后也只反映已实现盈亏**。`held_exposure` 是入场名义值，全程无 mark-to-market。一个浮亏 50% 的多头，`total_balance` 纹丝不动 —— **「强平线」只能被已实现亏损触发，无法被未实现亏损触发**。

另：`available_after_holds`（`paper_account.rs:2813-2818`）对负权益截断为 0，掩盖资不抵债，而保护机的 `current_collateral`（`paper_grid_task.rs:1502-1508`）正是基于它 —— 恰好在最该动作的场景下失真。

### H6. 回测引擎的三个头条指标口径错误

**`win_rate` / `profit_factor` 按 fill 计数而非按回合**（`backtest/src/engine.rs:446-449`），而开仓腿的 `realized_pnl_delta` 恒为 `-fee`（`ledger.rs:79-83`），只有 `>0` 计入分子（`metrics.rs:117-128`）。

用仓库自己的 golden vector 代入（一次 100% 盈利的完美回合）：**`win_rate = 0.5`**、`profit_factor = 98.9`。**胜率上限永远是 50%。**

**夏普默认年化因子 = 1**（`metrics.rs:36-42` + `engine.rs:387`），而字段文档写的是「annualized by sqrt(periods_per_year)」。秒级磁带的真实因子是 `sqrt(31_536_000) ≈ 5616`，**差 3 个数量级**，且没有任何代码从磁带时间戳推导它。

`backtest_contract.rs` 里**没有一行断言过** `win_rate`/`profit_factor`/`sharpe`/`sortino`（已验证）—— 门禁抓不到。

### H7. 回测成交模型没有「成交判定」，且跑不了任何生产策略

`engine.rs:248-302` 的 `fill()` 里**不存在任何是否成交的判定**，只有价格调整。`MarketEvent` 只有 `{occurred_at, price}`，没有 bid/ask/depth。`Liquidity::Maker` 由调用方自己填 —— 策略声明 maker 就能拿到 maker 费率 + 零队列风险 + 零未成交风险。

这比 spec 2.4 批评的「穿越即全量成交」**更宽松一档**。次高频网格 maker 成交率是决定性变量，这个模型把它设成了 100%。

默认价格源是 `LastOrMid`（`engine.rs:17-21`），taker 买单成交在 mid/last 而非 ask，**半个价差凭空消失**。

账本**无买力约束**（`ledger.rs:51-66` 扣款后无非负校验），现金可为负、无保证金、无强平；权益一旦为负，`equity_returns` 的符号翻转，Sharpe/回撤全变噪声。

而 `backtest` **不依赖 `crypto-trading-strategy`**（`Cargo.toml:11-16`），自定义了一个新 trait，且对非市价单直接拒绝（`engine.rs:172-174`）—— `GridPlanner`/`VirtualGrid`/套利全部产出限价单，**一个都进不来**。

**减轻情节**：`capability.rs:764-766`、`README.md:463` 已诚实声明「不是盈利证据」「不模拟队列/延迟/资金费率/部分成交/多档深度」。但上述四条（无买力约束、默认非年化、多标的静默压平、默认非对手价）**不在**已声明的限制里。

### H8. 并发轮询引入 `received_at` 乱序，可永久杀死 monitor 任务

`market_polling.rs:288-305` 各请求以**自己完成时刻**作为 `received_at`，却按 route 下标排序发出。而 `market_data.rs:775-784` 做的是**跨 instrument** 的时间回退检查：

同一轮里若 route 0 慢（`T+300`）、route 1 快速失败（`T+20`），发出顺序就是「先 T+300 后 T+20」→ 返回 `SourceEventTimeRollback` → 在 `continuous_monitor.rs:478-489` 被 `fail_owner` 捕获 → **任务进入 Failed 终态，不重启**。

整改前串行轮询保证 `received_at` 单调，此路径不存在。测试抓不到是因为多路由测试全用 `FixedClock`，且生产 wiring 每个 venue 只有 1 条 route。

### H9. 节流从轮次开始计时，RTT ≥ interval 时完全失效，而全链路仍零限流

```rust
// rust/crates/runtime/src/market_polling.rs:740-742
fn schedule_next_poll(poll_started_at: Instant, backoff: Duration) -> Instant {
    poll_started_at + backoff
}
```

RTT ≥ poll_interval 时，下一轮的 `next_poll_at` 在轮次完成时已是过去时刻 → 不 sleep → 以网络能跑多快就跑多快地连打。整改前 `sleep(interval)` 在取数**之后**，天然有下界。

叠加：出站限流仍未实现（`x-mbx-used-weight` 仍零消费者），Hyperliquid 仍是每个 coin 一次全宇宙 POST（`hyperliquid_public.rs:191-194`）且并发化后放大 8 倍。这是被 418/429 封 IP 的直接路径。

---

## 3. 中危问题

| # | 问题 | 证据 | 性质 |
|---|---|---|---|
| M1 | `RollingZScore` 用抵消式方差 `E[x²]−E[x]²`，容差 1e-18 比实际抵消误差小约 8 个数量级 | `indicators/src/zscore.rs:109-114, 7` | 新引入。高价低波动（次高频最常见场景）会硬 `Err` 或输出数量级错误的 z-score。同文件的 `metrics.rs:288-303` 用的是数值稳定形式，两处不一致本身说明是疏漏 |
| M2 | broadcast lag 后「一路丢到最新」，有效保留深度从 4096 塌回 1；且无累计丢弃计数器 | `market_supervisor.rs:342-362` vs 文档注释 `:183-189` | 整改不彻底。一轮 8 条观测只剩 1 条，其余 7 个标的转由 `SourceGap` 把该 venue 全部 instrument 打成非 Continuous |
| M3 | `APPEND_RECEIPTS` 全局注册表按路径无限增长 | `history.rs:48, 837-848` | 新引入内存泄漏。队列内部有界，但 `HashMap<PathBuf,_>` 的 key 从不移除。对比 `PATH_LOCKS`(`:1389`) 和 `CROSS_PROCESS_LEASE_STATES`(`:1422`) 都实现了 Weak 扫除，唯独这个漏了 |
| M4 | CHANGELOG Authority 段声明「No change」，但 paper 权限实际放宽 | `CHANGELOG.md:121-125` vs Fixed 段自述「release closed-lot capacity」 | 虚假宣称。同一敞口上限下 owner 可执行严格更多的开仓 |
| M5 | CHANGELOG 把新增子系统（`authority_state.rs` +560、新 journal 事实类型）写进 **Fixed** 段 | — | 分类错误。读者会以为 paper PnL 数字不受影响，实际费用与已实现 PnL 口径全变了 |
| M6 | `stop_grace_period: 70s` vs 最坏 240s | `compose.yaml:9` vs `paper_grid_task.rs:432`(grace×2) + `paper_dispatcher.rs:240-266`(串行遍历 slot) | 配置相关悬崖：grace 配到接近 60s 上限时 `docker stop` 会在结算完成前 SIGKILL |
| M7 | CI SIGTERM 冒烟只验证**只读空载**容器 | `rust.yml:189-196` 未传 `--enable-paper-writes` → `dispatcher = None` → shutdown 是空操作 | 只证明「空载 axum server 收到 SIGTERM 会以 0 退出」。顺带：`compose.yaml:38-47` 的出厂命令本身也是只读的，70s grace 目前无处可用 |
| M8 | 控制面读路径仍每请求整链全量重放 | `control-plane/src/lib.rs:105-113` | spec 1.3 只完成前半句（7 次 fan-out 降为 1 次遍历），常驻投影未做 |
| M9 | `account_risk` 事实不校验 `journal_id`，也不校验 `notional` 符号 | `account_risk.rs:301-307, 941-945` vs `paper_account.rs:2456-2468` 的严格校验 | 校验严重不对称。负 notional 会**降低**总敞口，绕过风控上限 |
| M10 | `snapshot()` 不做 `require_writable`，degraded 投影被静默喂给保护机 | `paper_account.rs:1125-1128` vs 每个写方法都有的 `require_writable` | 读写 fail-closed 语义不一致。消费点（`paper_grid_task.rs:1501` 等）无一检查 `projection_status` |
| M11 | `Degraded` 是进程级永久毒化，无自愈、无告警分级 | `authority_state.rs:195-197` 回退时不清缓存 | 一次异常 = 必须人工介入 + 重启；叠加 B3 时重启也无效 |
| M12 | `MarketDataObservation::new` 默认谎报 `VenueEventTime` | `market_data.rs:347-359` | replay 与 subscription 源仍会落进 `DuplicateTimestamp` 拒绝分支（地雷未完全拆除），且会上报凭空捏造的 `source_latency_millis` |
| M13 | 跨腿 skew 容差默认借用 `future_tolerance`（1s），与轮询周期同量级，生产从未显式配置 | `market_data.rs:160-164`；`command.rs:2099/2188/3121` 全传 1s，`with_max_pair_skew` 只有测试调用 | RTT 200ms 时约 17% 的评估退化为 `Waiting{PairSkew}`，套利腿间歇性失明 |
| M14 | `walk_forward.rs` 只是索引生成器，不跑回测、不选参、与 engine 零调用关系 | `walk_forward.rs:77` 入参是 `usize`，出参是 `Vec<Range<usize>>` | spec 3.6 的「每窗口独立选参」在此不可实现 |
| M15 | `Atr`/`Ema` 第一根就返回「值」且类型上无法识别 warm-up | `atr.rs:41-46, 72-73`；`ema.rs:42-43` vs `ewma_volatility.rs:43`/`zscore.rs:52` 返回 `Option` | 四个指标两套约定。`width = k × ATR(n)` 会在前几十根用一个可能差数倍的值开仓 |
| M16 | `from_market_snapshots` 静默丢弃 symbol/exchange/bid/ask | `engine.rs:37-42, 75-85` | 多标的混入同一 Vec 会成功返回一条「合法」磁带，价格在标的间来回跳。单标的限制只写在文档注释里，无代码强制 |
| M17 | 溢出被静默降级为 `None`，「算失败」与「没数据」不可区分 | `metrics.rs:95-100` | `max_drawdown: Option` 的 `None` 同时表示「空曲线」和「计算溢出」，报表上读作「无回撤」 |
| M18 | 隔离文件（`.quarantine`）永不清理，且直接反噬 append 延迟 | `history.rs:1476-1503`，全仓无删除代码 | 与 B2 形成正反馈 |
| M19 | `validate_complete_prefix` 在 async 上下文同步解析最多 64 MiB | `history.rs:1030-1042`，由每次 append 的 `ensure_active_tail_is_complete` 触发 | 崩溃后首次恢复会阻塞 tokio worker 数百 ms 到数秒。同文件的 `truncate_active_tail`/`quarantine_partial_tail` 都已 `spawn_blocking`，唯独最贵的解析没有 |
| M20 | `HistoryError::PartialTail` 成为死变体，`category = "partial_tail"` 遥测永不出现 | `history.rs:1573-1576` 无构造点，`:604` 只匹配 | 静默失效的监控 |

低危项另有 6 条（`reserve` 用 `reservations.last()` 认领、`remove_open_position` 平行索引其一为 `#[serde(skip)]`、`AdmissionCancelled` 连带删持仓时钟、`settle_open_admission` 前缀误匹配、`SimClock::now()` 零调用、CLI serve 路径信号注册竞态窗口），详见各路复查原始记录。

---

## 4. spec 中完全未执行的项

| spec 项 | 状态 | 实测证据 |
|---|---|---|
| **0.5 criterion 基线** | **未做** | lockfile 无 criterion，全仓无 `benches/` 目录 |
| 0.6 清 `target/` + `strip` 改 `line-tables-only` | 未做 | `Cargo.toml:58` 仍 `strip = "symbols"` |
| 1.2 投影快照 / checkpoint | 未做 | 见 H3 |
| 1.7 journal 保留策略 | 未做 | 见 H4 |
| 3.5 确定性 client_order_id | 未做 | `domain/src/order.rs:110-129` 仍 `Uuid::new_v4()` |
| 4.x WebSocket | 未做（依赖未获批，合理） | lockfile 无任何 WS 库 |
| 5.13 policy 指纹进 journal | 未做 | `account_risk.rs:481` policy 仅在实例 |
| **6.1 strategy 内联单测** | **未做** | `strategy`/`domain` 仍 0 个 `#[cfg(test)]`；新建的 `backtest`/`indicators` 也是 0 |
| **6.4 拆 apps crate** | **未做且反向** | apps src 从 26,019 **涨到 27,572** |
| 6.10/6.11 sha2+hmac / 替换 serde_yaml | 未做（依赖未获批，合理） | `Cargo.toml:38` 仍 `serde_yaml = "0.9"` |

**0.5 缺失是结构性问题**：阶段 1 重写（内存投影、head/delta）的**唯一理由是性能**，而现在没有任何测量证明它变快了。13,336 行改动的收益无法验证 —— 而 B2 恰恰证明了一个真实的性能**回归**从测量缺位中溜了过去。

---

## 5. 修复优先级

### 立刻（阻塞提交）

1. **B2** —— 恢复定点探测或缓存封存段状态。单点修复，同时消灭 B1 的红灯、消除热路径上的无界成本、收窄 fail-closed 打击面。
2. **B1 剩余部分** —— 即使 B2 修好，`stop()` 的共享 grace 与「计数先于落盘」仍应独立修掉。「内存说 2、磁盘落 1」是信任模型的直接违反。
3. **B3** —— 在途准入 TTL + 超限不摧毁整个 projection。这是唯一一条重启都救不回来的路径。
4. **B4** —— capability 降级或补 CLI 入口，并给契约测试加存在性断言。这条纪律一旦松动，整个 manifest 的可信度就没了。

### 紧接着

5. **H1** —— compose/Dockerfile 设 `RUST_LOG` 默认值；把 journal append 成功、owner 启停、账户风控拒绝三类提到 `info!`；首次进入 `Degraded` 打 `error!`。低成本、直接决定能否排障。
6. **补 criterion 基线（spec 0.5）** —— 在做任何进一步性能改动之前。B2 说明没有基线就会有回归溜过去。
7. **H2** —— 后台交叉校验。两个 read model 都已 `derive(PartialEq)`，成本很低，而它是引入内存投影时唯一承诺的缓解措施。
8. **H6** —— 回测三个头条指标的口径修正（round-trip 胜率、从磁带推导年化因子、Welford 方差）。这三条不依赖任何其他改动，且不修的话回测数字会主动误导。

### 之后

9. H3（快照）+ H4（保留策略）—— 7×24 的两个硬阻断，需要设计决策。
10. H5 —— 明确记录「余额阈值风控只对已实现亏损、且只对已结算账户成立」，不要让它看起来像已完全解决。
11. H7 —— 回测接生产策略 seam + 成交判定 + 买力约束；在此之前所有回测数字不能挂生产策略的名字。
12. H8/H9 + M2/M13 —— 行情层的并发化后遗症与限流。

---

## 6. 一句话总结

**整改在「额度回收、写序正确性、并发安全、信号处理、并发轮询」五项上是真正到位的，但它引入了 3 个整改前不存在的阻断级缺陷（目录枚举回归、在途准入永久砖化、门禁红灯），宣称了 2 个不存在的能力，并且因为跳过了 spec 阶段 0 的基准测试，整个重写的性能收益至今无法验证 —— 而 B2 正是一个从测量缺位中溜过去的真实性能回归。**
