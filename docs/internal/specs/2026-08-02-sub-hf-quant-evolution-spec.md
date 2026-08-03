# 次高频量化演进 Spec

> 日期：2026-08-02
> 基线：`main @ a64c4f2` + 18 个未提交文件
> 目标：把当前仓库演进为可承载**次高频**（秒级到分钟级信号、单日数百至数千订单、7×24 连续运行）的量化交易系统
> 方法：6 路并行代码审计（行情管道 / 执行链路 / 策略与研究闭环 / 运行时架构 / 风控与资金 / 工程质量），全部论断附 `file:line`，关键结论已逐条人工复核

---

## 0. 判定先行

**当前仓库是一个工程质量很高的「确定性执行安全内核」，不是量化交易系统。**

值得肯定的真实资产（这些是护城河，不要在改造中破坏）：

- 交易计算全程 `rust_decimal` + `checked_*`，计算路径上 f64 出现次数为 **0**
- `unsafe_code = "forbid"`、clippy pedantic `-D warnings`、release 保留 `overflow-checks`
- fail-closed 边界成体系：6 处独立的实盘关卡互不依赖（`runtime/src/execution.rs:751`、`exchange/src/unsupported.rs:44`、`runtime/src/capability.rs:333` 等）
- append-only journal + 幂等 saga + 两阶段 reservation + 跨进程写者租约
- capability manifest 与 `docs/adapter-support.md` 由契约测试强制同步
- 830 个测试全绿，双 OS × 双工具链 CI，RustSec + cargo-deny + 前端供应链门禁

**但它的架构是为 one-shot CLI 设计的**，有三个结构性决策与次高频直接冲突。下面列出的所有问题，没有一个是「代码写得烂」，全部是「为了次高频而缺的能力」或「one-shot 假设在长跑下失效」。

### 现实预期

| 场景 | 当前可支撑 | 说明 |
|---|---|---|
| 单策略、单标的、分钟级、跑几天 | ✅ 可以 | 这是系统被设计的工况 |
| 单策略、秒级信号 | ❌ 数十分钟后失效 | 决策延迟随 journal 线性增长 |
| 多标的（≥10）秒级观测 | ❌ 物理不可能 | 串行轮询 → 单标的刷新 = N × interval |
| 7×24 连续运行 | ❌ 约 6 天硬停机 | journal 4 GiB 上限无淘汰 |
| 判断策略是否盈利 | ❌ 无手段 | 无回测、无 PnL、无绩效指标 |
| 多策略共享账户风控 | ❌ 不成立 | 风控的「账户」实际是「journal 文件」 |

---

## 1. 根因收敛

6 路审计共产出 ~70 条发现，收敛为 **5 个根因 + 4 个横切问题**。修根因，症状会成片消失。

### 根因 A：状态读取 = 全量重放（无投影、无快照）

事件溯源做了一半：写入侧（append-only、密封段、cursor anchor）非常扎实，读取侧却是**每次查询都 replay from genesis**。

| 调用点 | 每次操作的全链重放次数 | 证据 |
|---|---|---|
| 账户预留 `reserve()` | 2 | `runtime/src/paper_account.rs:1025,1075` |
| 账户提交 `commit()` | 2 | `runtime/src/paper_account.rs:1124,1341` |
| 风控准入 `admit()` | 3（未提交改动 +1） | `runtime/src/account_risk.rs:474,482` |
| **单笔订单合计** | **7 次** | — |
| 网格每个行情事件 `directives()` | 2 | `apps/src/paper_grid_task.rs:1193` → `account_risk.rs:721-727` |
| 控制面 `snapshot()` | **7 次**（7 个读模型各扫一遍同一份数据） | `control-plane/src/lib.rs:104-124` |
| Web SSE | 每秒一次全链磁盘读 | `web/src/api.rs:36,527` |

`journal_reader.rs:160-162` 的 `snapshot()` 把整条链（上限 4 GiB，`history.rs:35`）一次性读进 `Vec<u8>`。全仓无任何 snapshot/compaction 实现，`history.rs:346-349` 注释明确「设计上不做 compaction」。

**后果**：单次决策成本 = O(journal 总字节)。按 10 条事实/秒、800 B/条估算，运行 1 天后 journal ≈ 691 MB，单个行情事件的风控检查约 14 秒，单笔订单约 49 秒（估算，非实测）。这不是慢，是**吞吐随运行时长单调塌陷**。

同时 `from_legacy_snapshot` 的 CPU 密集重放跑在 async worker 线程上（`paper_account.rs:1365`、`account_risk.rs:726-727` 的 `spawn_blocking` 只包住了读文件），N 核机器上 N 个并发重放即 runtime 饿死。

### 根因 B：落盘在决策热路径上同步 fsync

`history.rs:501-503` 每个 batch 以 `sync_data()` 收尾，无组提交；`append()` 单条记录也是一次完整 fsync（`history.rs:397-399`）。

更隐蔽的是 `history.rs:517` 每次 append 前调用 `inspect_sealed_segments`，而该函数在**零封存段的常态路径**上（`history.rs:234-264`）：`sequence=1` 的 `std::fs::metadata` 返回 NotFound 后进入内层循环遍历 `2..=64`，做 **63 次 `Path::exists()`**。合计每次 append **64 次同步阻塞 syscall**，且直接在 `async fn` 里调 `std::fs`，未走 `spawn_blocking`。

monitor 每个行情事件写两次（主 journal + spread-history），即 **128 次阻塞 stat + 2 次 fsync/事件**。

`history.rs:489` 的 `path_lock` 是进程内全局 AsyncMutex，跨越整个 `write + flush + sync_data`。Web 模式下 grid task / arbitrage task / account risk / submit service 共用同一 history_path，全进程所有写入串行排在同一条 fsync 链后。

**A + B 构成正反馈死循环**：行情事件通道是深度 1 的 `watch`（`market_supervisor.rs:236`，`send_replace` 覆盖式写入），消费者被重放和 fsync 拖慢 → 行情被静默覆盖 → 覆盖被翻译成 `SourceGap` → `SourceGap` 把该交易所所有标的的 continuity 打成非 `Continuous`（`market_data.rs:651-686`）→ `is_ready()` 为 false → 策略停在 `Waiting`。**越忙越瞎**。

### 根因 C：行情入口是 REST 短轮询，且时间戳是本地生成的

| 问题 | 证据 | 次高频后果 |
|---|---|---|
| 无任何 WebSocket 实现或依赖 | `Cargo.lock` 无 WS 库；`binance.rs:301-309`、`hyperliquid_public.rs:478-486` 的 `subscribe()` 一律 `Unsupported` | 物理上拿不到 tick/增量 |
| 配置里有 WS 字段但零消费者 | `config/src/monitor.rs:18-20,220-222` 的 `ws_ping_interval` 等三个字段全仓无读取点 | 「看起来支持 WS」的假象 |
| 串行 round-robin 轮询 | `market_polling.rs:243-254`，每标的取数前先 sleep 一个完整 interval | 单标的刷新周期 = **N × (interval + RTT)**；10 标的 @1s → 每标的 11 秒 |
| **快照时间戳 = 本地 `Utc::now()`** | `binance.rs:110`、`hyperliquid_public.rs:189` | staleness 恒等于「本进程解析耗时」≈ 0 |
| 新鲜度判定拿本地戳比本地戳 | `market_data.rs:500,182-192` | `data_timeout_seconds`（默认 30s）**永远不会触发** |
| 无 venue 序列号连续性校验 | `revision` 是本地自增（`market_polling.rs:259-262`）；连 bookTicker 自带的 `u` 字段都未解析（`binance.rs:27-35`） | `MarketContinuity::Gap` 分支在轮询模式下永不命中 |
| 跨所价差无时间对齐 | `arbitrage.rs:47-58` 不比较两腿时间戳；唯一约束是各腿独立的 30s max_age | 两腿可相差 30 秒，波动时凭空造出几十 bp 假价差 |
| Hyperliquid 的 bid/ask 取的是 `impactPxs` | `hyperliquid_public.rs:252-268` | 冲击价冒充最优价，价差信号方向可能是错的 |
| 无任何出站限流 | 权重头被解析保留（`remote.rs:405-417`）但零消费者 | 提频即触发 429/418/IP 封禁 |

**两颗待引爆的地雷**（当前被「时间戳是本地的」掩盖，修好时间戳后立即引爆）：

1. `market_data.rs:231-235` 的 `is_fresh()` 只匹配 `Fresh`，`Future { within_tolerance: true }` 被计算出来却从不被读取 → `max_future_skew` 是死参数。改用交易所时间戳后，1 ms 的本地时钟落后就会让标的**持续**不可用。
2. `market_data.rs:610-617` 把同时间戳更新硬判为 `DuplicateTimestamp`，拒绝更新且污染 continuity。真实行情毫秒精度下同戳极常见，接 WS 后会把大量合法更新判为重复并持续踢出可用状态。

**这两条必须与时间戳改造同批处理，否则「修好 C」会让系统更瘫。**

### 根因 D：Paper 账本不是账本

这是最容易被忽视、后果最严重的一条 —— 一套严谨的风控机制在守护**一组不反映真实风险的数字**。

**D1. 账户总余额恒等于配置常数。** 这是恒等式，不是近似：

```
available      = initial_available − held                (paper_account.rs:1728)
total_exposure = pending + uncertain + committed = held  (account_risk.rs:948-979)
total_balance  = available + total_exposure ≡ initial_available
```

于是 `min_balance_warning` 和 `min_balance_close`（强平线）退化为**常量谓词**：配置那一刻就决定了永远触发或永远不触发，与盈亏完全无关。亏到爆仓也不会触发。

**D2. 手续费从未被扣除。** `PaperCostModel` 的 `fee_bps`/`funding_buffer_bps`/`slippage_bps`（默认 10/5/15 bps）唯一消费点是 `paper_account.rs:1917` 的预留计算 —— 它影响「能不能下单」，不影响「下完赚多少」。commit 时 `held_exposure = confirmed`（`paper_account.rs:1662`，≤ reserved），**缓冲被原样退回**。

**D3. 额度只减不增。** `release()` 对 `Committed` 阶段直接 `Err(InvalidTransition)`（`paper_account.rs:1177`），唯一出路 `reconcile_release` 需要人工确认短语产生的 proof。committed 预留的 `held_exposure` 永久计入（`paper_account.rs:1719`）。默认 10,000 额度、100/单 → **约 100 笔后必然 `InsufficientAvailable` 停机**。

**D4. 无 PnL、无保证金、无交易所真相。** 全仓搜索 `margin|leverage|liquidation|maintenance|isolated` **零命中**。`domain::Position` 只有 `side/quantity/updated_at`，无 `entry_price`、无 `margin`、无 `liquidation_price`。敞口按**名义值**计算 —— 10x 杠杆下 `max_total_exposure: 10000` 实际只占 1000 保证金。`RiskEngine` 唯一调用点传入的 `AccountRiskSnapshot` 是伪造的（`command.rs:3908-3915`：`kill_switch: false` 硬编码、`positions: &[]` 硬编码）。

**D5. 撮合模型对次高频不可用。** 成交价就是限价或对手价（`paper.rs:960-983`），无费用调整；可成交量只看顶档一档（`paper.rs:1083-1105`）；挂单按插入顺序无条件成交（`paper.rs:447-524`），无队列位置概念。次高频网格单次穿越毛利常在 5–30 bp，而往返手续费 4–20 bp —— **费用是主导项**。零费用的 paper 撮合会把必亏策略显示成稳定盈利。

**D6. 风控的「账户」实际是「journal 文件」。** scope 边界是「journal 路径 + scope_id 字符串」，不是真实交易所账户。两个进程用不同 `--history-path` 跑同一个交易所账户，各自维护独立的敞口上限、当日计数、kill switch。而同一 journal 路径的第二个进程会被跨进程租约（`history.rs:670-693`）直接拒绝启动。**多进程和单进程两条路都堵死**：多进程风控失效，单进程被 catalog 限死在 1 grid + 1 arbitrage（`paper_profile.rs:86-90,149-157`）且全局锁串行化所有下单。

### 根因 E：没有量化研究闭环

全仓搜索 `backtest|walk.?forward|sharpe|drawdown|win.?rate|optimi[sz]ation` **零命中**（唯一命中在 `docs/internal/research/` 里描述**别人的仓库**）。

| 环节 | 现状 |
|---|---|
| 回测引擎 | **不存在**。验证策略盈利能力的唯一手段是「跑 paper 真实时间 + 人工读 JSONL」，一次实验成本 = 真实时间 |
| 指标库 | **不存在**。`alert.rs:13-16` 的 "volatility" 只是两点百分比变化；全仓唯一的滚动统计是套利的价差中位数（`arbitrage_history.rs:211-216`）。无 EMA/ATR/VWAP/RV/z-score，无 K 线聚合 |
| 绩效评估 | **不存在**。无夏普/索提诺/回撤/胜率/盈亏比。journal 里落了完整 `TradingReceipt`（含 `filled_quantity`/`average_fill_price`），逐笔 PnL 可反算，但**等间隔权益曲线不可重建**（无周期性 mark-to-market 事实） |
| 参数寻优 | **不存在**。参数用绝对价格硬编码（`paper-once-btc.yaml` 的 `lower_price: 100` / `upper_price: 120`），币价移动 20% 后网格完全失效。27 份 grid 配置中仅 2 份 runtime-executable |
| 可复现性 | **被破坏**。`OrderIntent::market()` 内嵌 `Uuid::new_v4()`（`domain/src/order.rs:110-129`），策略层直接调用 → 同一份磁带两次跑出的 intent 不相等，与 crate 自称的 "deterministic" 矛盾 |

**E 与 D 相乘**：即使补上回测，只要撮合模型零费用（D5）、账本无 PnL（D1/D2），回测结论依然不可信。**两者必须一起做。**

### 横切问题

| # | 问题 | 证据 | 影响 |
|---|---|---|---|
| X1 | **可观测性为零** | `tracing` 仅被 apps/web-app 引入用于初始化 subscriber，全仓 `info!`/`warn!`/`error!`/`#[instrument]` 调用数为 **0**；无 metrics/prometheus；46 处 `println!`；release `strip = "symbols"` | 出问题无法区分行情慢/下单慢/重放慢/fsync 慢。7×24 无任何时间序列可回溯 |
| X2 | **无 SIGTERM 处理** | 全仓只有 `tokio::signal::ctrl_c()`（`task_host.rs:131`、`web-app/src/main.rs:14`）；Dockerfile 用 exec form → 二进制是 PID 1；compose 无 `stop_grace_period`/`init` | Linux 内核对 PID 1 不执行未注册信号的默认动作 → `docker stop` 的 SIGTERM 被忽略 → 10 秒后 SIGKILL。若落在 `write_all` 与 `sync_data` 之间会留下 `PartialTail`，之后**所有 append 永久 fail-closed**。**每一次正常的 `docker stop` 都在赌 journal 不被写坏** |
| X3 | **反馈环 ~115 秒** | strategy 与 domain 的 `#[cfg(test)]` 文件数为 **0**（全部测试在 `tests/`）；102 个测试二进制；实测：改 1 个 strategy 文件 → check 13.4s + 重链 75.5s + 执行 26s | 改一行 grid 公式要等近 2 分钟。直接压制策略迭代速度 |
| X4 | **apps crate 承担 runtime 职责** | apps src 26,019 行（全仓 37.5%），导出 20 个模块中仅 2 个是 CLI 职责；`web-app → apps` 使 HTTP 服务传递依赖 `clap`；`command.rs` 单文件 6,470 行 | 违反项目自己的 §3.2 分层；新增策略必须改编译链最深的 crate |

其余次要项：`strategy → config` 依赖倒置（`strategy/Cargo.toml:15`，历史审计已标记为「刻意推迟的架构债」）；手写 SHA-256 391 行（`exchange/src/sha256.rs`）；`serde_yaml` 已 archived 仍在用；`target/` **83 GB** 导致构建时间 6 倍波动；CI 无任何 `timeout-minutes`；无 property test / fuzz / bench。

---

## 2. 一处审计更正

初审报告称「连续网格丢弃马丁语义，把马丁配置当固定网格偷偷跑掉」。**复核后推翻此结论**：

- `paper_profile.rs:683` 对非 `FixedLong` 配置直接 `bail!("grid paper write mode currently supports only fixed long configs")`
- `build_virtual_grid` 全仓仅一个调用者（`paper_profile.rs:308`），必经上述检查

正确描述是：`GridPlanner` 的马丁/跟随/做空语义（含 golden 测试，`grid.rs:308-329`）**只能从 CLI one-shot `--once` 到达，连续 paper 路径够不到**。这是能力缺口，不是行为不符 —— fail-closed 纪律在此处生效。定性差异影响优先级：属于 P2 而非 P0。

---

## 3. 目标架构

保留现有的深模块划分与 fail-closed 边界，只改三处结构：

```
                    ┌─────────────────────────────────────┐
                    │  StateAuthority (常驻内存投影)        │  ← 新
                    │  snapshot + delta，O(1) 读            │
                    └──────┬──────────────────▲────────────┘
                           │ 查询              │ apply(fact)
   ┌────────────┐   ┌──────▼──────┐    ┌──────┴────────┐
   │ MarketData │──▶│  Strategy    │───▶│ JournalWriter │  ← 改
   │  Stream    │   │  (纯计算)     │    │ 组提交+异步    │
   │ WS + REST  │   └─────────────┘    └───────┬───────┘
   └────────────┘          │                    │ 批量 fsync
        │ 有界 mpsc         ▼                    ▼
        │ 显式溢出策略  ┌─────────┐         ┌──────────┐
        └─────────────│ Executor │         │ JSONL 链  │
                      └─────────┘         │ +保留策略 │  ← 改
                                          └──────────┘
   ┌──────────────────────────────────────────────────┐
   │  crates/backtest ── 复用同一 Strategy + FillModel  │  ← 新
   │  crates/indicators ── 增量式，回测/实盘同一实现     │  ← 新
   └──────────────────────────────────────────────────┘
```

三个不变量：

1. **策略层保持纯计算、无 I/O**（当前已满足，实测 `strategy/src` 零 `std::fs|tokio::|reqwest`）。这是最大的架构红利，回测能直接复用。
2. **journal 仍是唯一的持久真相源**，只是不再作为每次查询的数据源 —— 降级为 WAL + 崩溃恢复。
3. **fail-closed 边界一处不减**，capability manifest 仍是权威。

---

## 4. 分阶段方案

每个阶段独立可验证，遵循仓库现有纪律：每个 tracer-bullet 跑五门禁全绿即提交；能力变化四处同步（`capability.rs` + `capability_contract.rs` + `docs/adapter-support.md` + README 能力矩阵）。

---

### 阶段 0 · 收口与装度量（前置，~2 天）

> **没有基线数字，后面所有优化都是猜。** 这一阶段不产生新功能。

| # | 任务 | 落点 | 退出条件 |
|---|---|---|---|
| 0.1 | 提交当前 18 文件，补 CHANGELOG `### Changed` 段 | `CHANGELOG.md` | 明确记录 `account_risk` 日界单调化、`open_admissions` 敞口回放，以及 `grid.rs`/`grid_protection.rs` 两处**与 legacy Python 的刻意语义偏离**（当前 diff 中有注释但无 changelog 条目）。建议拆成 3–5 个提交 |
| 0.2 | **SIGTERM handler** | `apps/src/task_host.rs:131`、`web-app/src/main.rs:14` | shutdown future 改为 `select!(ctrl_c, unix::signal(SignalKind::terminate))`；Windows 用 `ctrl_close`/`ctrl_shutdown`；`deploy/compose.yaml` 加 `stop_grace_period` > `MAX_SHUTDOWN_GRACE`(60s)。**约 10 行，消除数据损坏风险** |
| 0.3 | `PartialTail` 自愈流程 | `runtime/src/history.rs:594-598` | 启动时检测到部分尾能自动隔离并继续，而非永久 fail-closed 等人工 |
| 0.4 | **接上 tracing** | 各 crate `Cargo.toml` + 四个关键 seam | `#[instrument]` + latency 埋点于：行情 `fetch_next`、`history.append`(fsync 耗时)、read model 重放、exchange `dispatch_execute` |
| 0.5 | **criterion 基线** | 新增 `benches/` | 至少 3 个：`AccountRiskAuthority::admit`（按 journal 大小参数化）、`GridPlanner::intents`、`history.append_batch`。**这是阶段 1 的验收标尺** |
| 0.6 | 清 `target/` 83 GB | — | 把一次性验证证据（`m2-browser-qa`、`release-restore-drill-*` 等 12 个目录）移出 `target/`；`CARGO_PROFILE_DEV_DEBUG=line-tables-only`；release 的 `strip = "symbols"` 改 `debug = "line-tables-only"` |
| 0.7 | CI 加 `timeout-minutes` | `.github/workflows/*.yml` | 当前 13 个 job 全无 timeout；仓库有 213 处时序敏感 sleep，最近 3 条提交都在修 CI 抖动 |

**依赖审批点**：`criterion`（dev-dependency）。

---

### 阶段 1 · 状态层重构（核心，解根因 A + B）

> 这是唯一一个「不修就什么都做不了」的阶段。修完后 80% 的性能问题消失。

| # | 任务 | 落点 | 设计要点 |
|---|---|---|---|
| 1.1 | **常驻内存增量投影** | `runtime/src/paper_account.rs`、`runtime/src/account_risk.rs` | 进程启动时全量重放一次建立内存状态；之后每次 `append_fact` 直接 apply 到内存，不再回读磁盘。保留「读回磁盘交叉验证」的 fail-closed 语义，但降级为低频后台校验（如每 5 分钟），而非每次决策 |
| 1.2 | **投影快照 / checkpoint** | `runtime/src/journal_reader.rs` | `<journal>.snapshot.N` + 其覆盖的 sequence；重启时 `snapshot + tail replay`。这同时是 1.4 保留策略的前置 |
| 1.3 | **控制面单遍 fan-out** | `control-plane/src/lib.rs:104-124` | 7 个读模型共享一次遍历（一次 `read_page` 循环 fan-out 到 7 个 applier），立刻拿到 7× 收益；再改为常驻投影 + `Arc` clone 返回 |
| 1.4 | **SSE 改推送** | `web/src/api.rs:36,527` | 从写入侧 `broadcast`/`watch` 推送，取消每秒全链磁盘读；限流改 per-client（当前 `WEB_REQUEST_LIMIT_PER_MINUTE = 240` 是全局 4 req/s，且 SSE 内部轮询绕过了限流器） |
| 1.5 | **journal 组提交** | `runtime/src/history.rs:397-503` | 热路径写内存 ring buffer，后台 writer 批量 `append_batch` + 单次 fsync（`append_batch` 已支持批量单同步，只是没人用）。关键状态转移保留「落盘后才返回」语义，诊断记录改异步批写 |
| 1.6 | **缓存 `inspect_sealed_segments`** | `runtime/src/history.rs:516` | 封存段只在 rotation 时变化，rotation 由本 writer 独占 lease 触发 → 完全可维护内存状态，只在获取 lease 时扫一次。**消除每次 append 的 64 次阻塞 syscall** |
| 1.7 | **journal 保留策略** | `runtime/src/history.rs:22-35` | 新增 `RetentionPolicy { max_segments, max_age }`，允许归档/淘汰最旧封存段。解除 4 GiB 硬顶。建议同时**拆分两条 journal**：低频「状态权威事实」（永久保留）与高频「诊断/行情样本」（可轮转丢弃） |
| 1.8 | 重放移出 async worker | `paper_account.rs:1365`、`account_risk.rs:726-727` | 把 `from_legacy_snapshot` 一起移进 `spawn_blocking` 闭包（一行改动）；`canonicalize` 结果在 authority 构造时缓存一次 |
| 1.9 | 行情通道有界化 | `runtime/src/market_supervisor.rs:236` | `watch` 换成显式容量 `mpsc` + 溢出策略枚举 `DropOldest`/`Backpressure`/`FailClosed`；丢弃计数提升为 `MarketSupervisorStatus` 一等字段并进 metrics |

**退出条件**（用 0.5 的 bench 验收）：
- `admit()` 延迟与 journal 大小**解耦**（O(1)，不随字节数增长）
- 单笔订单 fsync 次数从 4 降到 1
- 每次 append 的阻塞 syscall 从 64 降到 ≤2
- 连续跑 24 小时不触发容量停机

---

### 阶段 2 · 让 Paper 账本成为真账本（解根因 D）

> 这是「模拟结果是否可信」的分水岭。不做这一步，回测和 paper 的所有结论都无意义。

| # | 任务 | 落点 | 设计要点 |
|---|---|---|---|
| 2.1 | **PnL 记账** | `runtime/src/paper_account.rs` | 引入按 (symbol, market_type) 的净头寸账本，含 `entry_price`。成交时结算已实现盈亏并写入账户权益；周期性 mark-to-market 写 `account_marked` 事实（哪怕 1 分钟一次），让**等间隔权益曲线可离线重建** |
| 2.2 | **额度回收** | `paper_account.rs:1177,1719` | 区分「资金占用」与「已实现平仓」：平仓腿归还开仓腿占用的敞口。当前 `release()` 拒绝 committed 是为了保护对账语义，需引入独立的 `settle()` transition（本地平仓即可释放，与需要交易所 proof 的 `reconcile_release` 分开） |
| 2.3 | **手续费真实扣除** | `exchange/src/paper.rs` | maker/taker 分档费率进入成交后果而非预留缓冲；`PaperCostModel` 从「预留 buffer」升级为「成交成本」 |
| 2.4 | **队列优先级模型** | `paper.rs:447-524` | 最简版本：要求价格穿越超过 limit 至少 k 个 tick 才成交，k 可配。当前「穿越即 100% 全量成交」系统性高估 maker 成交率 |
| 2.5 | 多档吃单 + 冲击成本 | `paper.rs:1083-1105` | 依赖阶段 4.1 的 `OrderBook` 类型。当前只看顶档，冲击成本恒为 0 |
| 2.6 | 资金费率结算 | — | `FundingRateFeed` 已存在（`market_polling.rs:337-368`）但零消费者。**先接线**（改动量最小），再做结算 |
| 2.7 | **保证金模型** | `domain/src/order.rs:218` 附近 | `Position` 增加 `entry_price`/`leverage`/`margin_mode`/`initial_margin_rate`/`maintenance_margin_rate`；风控主约束从**名义值**改为**保证金率**；计算强平价并支持接近强平前预警 |
| 2.8 | 修复余额阈值风控 | `strategy/src/account_risk.rs:255-263,300-307` | 2.1 完成后 `total_balance` 才有意义。**在 2.1 落地前，应把两个 balance 阈值在配置加载时显式拒绝**，而不是让它静默无效 |

**退出条件**：
- 存在一个契约测试：亏损到低于 `min_balance_close` 时强平指令**确实触发**
- 模拟 PnL 与「手工按真实费率反算」一致（新增 golden vector）
- 连续 1000 笔往返订单不触发 `InsufficientAvailable`

---

### 阶段 3 · 研究闭环（解根因 E）

> 依赖阶段 2 —— 回测必须复用同一个 fill model 与成本模型，否则回测与 paper 结论不一致。

| # | 任务 | 落点 | 设计要点 |
|---|---|---|---|
| 3.1 | **`crates/indicators`** | 新 crate | 只依赖 `rust_decimal` + `chrono`，纯计算。**全部做成增量式**（`fn update(&mut self, x) -> Option<Output>`），回测与实盘用同一份实现且 O(1)。优先级：ATR / EWMA-RV（网格宽度自适应必需）→ EMA / 滚动 z-score → OBI / microprice（依赖阶段 4 的深度数据）。每个指标配 golden vector，风格照抄 `virtual_grid_golden.rs` |
| 3.2 | **`crates/backtest`** | 新 crate | 只依赖 `domain` + `strategy` + `indicators`，**不依赖 runtime/exchange**。核心是 `SimClock + EventTape + FillModel + Ledger` 事件循环。复用 `MarketDataEvent` 作为磁带格式，让回测与实盘走同一策略入口 |
| 3.3 | 磁带存储格式 | `crates/backtest` | **不要复用 `JsonlHistory`**（fsync 会让回测慢 3 个数量级），也不要复用 journal 的观测限额（`scanner.rs:21-29` 的 8192 观测/标的在秒级只有 2.3 小时）。用自定义定长二进制记录（避免新依赖）或申请 parquet |
| 3.4 | **绩效指标库** | `crates/backtest` 或独立 | 纯函数（`fn sharpe(equity: &[(DateTime, Decimal)], rf) -> Decimal`），回测与实盘复用。基于 2.1 产出的权益曲线派生：夏普、索提诺、最大回撤、Calmar、胜率、盈亏比、换手率、暴露时间 |
| 3.5 | **确定性可复现** | `domain/src/order.rs:110-129` | 把 `client_order_id` 从策略层剥离（由 runtime 在执行边界赋 ID），或改为确定性派生 `uuid_v5(ns, task_id + operation_sequence + leg_index)`。加契约测试：同 state + snapshot 调两次 `evaluate` 必须 `assert_eq!` |
| 3.6 | walk-forward runner | `crates/backtest` | 滚动 train/test 窗口、每窗口独立选参、**只报告样本外结果** |
| 3.7 | 参数改为相对量 | `config/src/grid.rs` + 配置 | `width = k × ATR(n)`、`interval = width / levels`，替代硬编码绝对价格。**这一步不依赖回测就能做，直接解决「币价移动 20% 后网格失效」** |
| 3.8 | 校准 scanner | `strategy/src/virtual_grid.rs:647-648` | `FEE_RATE_PERCENT = 0.004`（即 0.4 bp）比主流交易所真实费率低 5–25 倍，改为配置注入并在输出中显示所用假设。APR 公式把穿越次数线性外推到一年，在波动率聚集下严重高估 —— 在回测校准前应明确标注为「启发式排序」而非「预期收益」 |

**依赖审批点**：`proptest`（dev-dependency，用于 journal 往返不变量、账本守恒、grid 几何边界）。

**退出条件**：能对同一份历史磁带跑出「资金曲线 + 逐笔明细」两个原始产物，且回测 PnL 与 paper 重放 PnL 在同一份数据上**逐笔一致**。

---

### 阶段 4 · 数据平面（解根因 C）

> **顺序不能反**：类型没定就接 WS 会返工。

| # | 任务 | 落点 | 设计要点 |
|---|---|---|---|
| 4.1 | **domain 类型定型** | `crates/domain`（当前仅 621 行、13 个类型） | 下沉 `MarketInstrument`（现在在 `runtime/src/market_data.rs:26`）；新增 `OrderBook`（分档）、`Trade`（逐笔）、`Fill`、`Fee`。`MarketSnapshot` 保留为 `OrderBook` 的顶档投影而非唯一形态。**分层事件契约**：`Quote` / `Depth` / `Trade` 三种事件，策略按需订阅，不要塞进一个大结构体 |
| 4.2 | **时间戳语义修正** | `domain/src/market.rs:72-86`、`market_data.rs:182-192` | 区分 `event_time`（venue 提供）与 `received_at`（本地）；`MarketFreshnessPolicy::classify` 只吃 `event_time`。把 `binance_testnet_exchange.rs:359-390` 已有的 offset 机制抽成 runtime 层共享的 `VenueClockOffset`，并暴露为可观测量 |
| 4.3 | **拆除两颗地雷**（与 4.2 同批） | `market_data.rs:231-235`、`market_data.rs:610-617` | 修 `is_fresh()` 让 `Future { within_tolerance: true }` 视为可用；同戳更新改为「以 venue 序列号为准，序列号更大则接受」，无序列号的源接受最新值且不污染 continuity |
| 4.4 | venue 序列号连续性 | `market_data.rs:592-636` | `MarketDataObservation` 新增 `source_sequence: Option<u64>`（venue 的 `updateId`/`u`/`seq`），优先用它做连续性判定，本地 `revision` 降级为 tie-breaker |
| 4.5 | **跨所时间对齐** | `market_data.rs:544-557` | `current_pair` 加显式 `max_pair_skew` 参数，两腿 `event_time` 差超限返回新增的 `PairSkewExceeded`；实际 skew 作为字段进 `ObservedMarketPair` 与 `SpreadHistoryRecord`，让历史样本可事后验证 |
| 4.6 | **WebSocket adapter** | `crates/exchange` 新增 | 立 `MarketDataStream` seam（`subscribe → Stream<Item = MarketDataEvent>`），现有 `SubscriptionMarketDataAdapter`（`market_data.rs:799-884`）作为 paper 实现。Binance `bookTicker`/`depth@100ms`、Hyperliquid `l2Book`/`trades`。REST 保留为降级路径 |
| 4.7 | user data stream | `exchange/src/binance_testnet_exchange.rs:609-617` | 成交/订单事件主通道（listenKey + WS），REST 查单降级为对账兜底。当前回报延迟 = 轮询周期（秒级到分钟级），与决策周期同量级 |
| 4.8 | **并发轮询 + 限流** | `market_polling.rs:243-254`、`remote.rs:405-417` | sleep 改为「每轮一次」而非「每标的一次」，用 `FuturesUnordered` 并发抓取，或直接用批量端点（Binance 不带 symbol 的 `bookTicker` 返回全市场；Hyperliquid 本来就是全量返回，当前却对每个 coin 重复拉一次全宇宙）。加 venue 级权重令牌桶，消费已保留的 `x-mbx-used-weight-1m` 做闭环 |
| 4.9 | 修 Hyperliquid 价格语义 | `hyperliquid_public.rs:252-268` | 切到 `l2Book` 取真实 L1；若保留 impact 价，放到独立字段由策略显式选择，不冒充 `bid`/`ask` |
| 4.10 | 死配置清理 | `config/src/monitor.rs:18-23` | `ws_ping_interval`/`analysis_interval_ms`/`ui_refresh_interval_ms` 等六个字段全仓无消费者，却在做非零校验。要么接线要么删除；`poll_interval_ms` 从 `hide = true` 的隐藏 CLI 参数提升为一等配置项 |

**依赖审批点**：**`tokio-tungstenite`**（rustls，与现有 reqwest TLS 栈同源）。此项早在 2026-07-26 的计划中就被列为待批准决策点，一直未批 —— **它正好卡在次高频最关键的路径上**。

**退出条件**：10 个标的的单标的刷新周期 < 200 ms；staleness 检测能真实触发（注入延迟 fixture 验证）；跨所 skew 可观测且超限拒绝。

---

### 阶段 5 · 执行与风控闭环

| # | 任务 | 落点 |
|---|---|---|
| 5.1 | **部分成交/挂单作为合法非终态** | `paper_single_leg_saga.rs:358-367`、`paper_grid_task.rs:1539` — 当前每一次部分成交都会杀死策略任务并留下 Uncertain 预留需人工恢复 |
| 5.2 | **本地 open-order registry** | client_order_id ↔ venue order_id 持久映射；支持按 `origClientOrderId` 撤单（`binance_testnet.rs:396-401` 当前只发 `orderId`） |
| 5.3 | **自动 query-first 恢复** | 把 `testnet_lifecycle.rs:253-310` 的模式下沉为通用能力，替换 `paper_single_leg_saga.rs:281-287` 的一律 `RecoveryRequired` |
| 5.4 | cancel-replace / amend | `exchange/src/model.rs:29-38` 当前只有 Submit/Cancel/CancelAll。次高频挂单策略核心动作是随盘口持续改价 |
| 5.5 | **周期性对账 owner** | 当前 `.reconcile(` 仅 4 个调用点，全是失败诊断或人工 CLI。需固定周期（5–30s）拉 open orders + positions + balances 与本地投影 diff |
| 5.6 | 运行期对账放宽 | `testnet_reconciliation.rs:321-355` 的 clean-account 硬断言只适用于发布门禁。拆成两个东西：发布门禁（保留严格）与运行期对账（允许挂单/持仓，逐项 diff + 余额容差） |
| 5.7 | quarantine 自动解除 | `bounded.rs:472-481,212-224` — 一次 30 秒超时即隔离整个 adapter 直到成功对账，而当前无自动对账 → 策略永久卡死。依赖 5.5 |
| 5.8 | **次高频风控维度** | 全部缺失，逐条补：滑动窗口下单频率限制（`max_daily_trades` 的 UTC 日粒度对次高频无意义）、最大回撤熔断、日内亏损限额、价格跳变熔断、**自成交防护**（volume-maker 同时挂 bid/ask 是结构性自成交场景，真实交易所会触发风控甚至封号） |
| 5.9 | **kill switch 语义补全** | `account_risk.rs:683-689` 的 `CloseAllPositions` 实际只写一条 journal 事实然后停止 owner —— **不撤单、不平仓**。操作员按「急停」得到的是「停止开新仓，旧仓裸奔」。需真正驱动撤单 + reduce-only 平仓，并补自动触发条件（当前 100% 依赖人工）。另：行情断线时整个 directive 分支被跳过（`paper_grid_task.rs:584-586` 的 `observed` 为 `None`），而断线正是最需要强平的时刻 —— directive 评估应挂在独立时钟循环上 |
| 5.10 | kill switch 持久性 | `account_risk.rs:764-773` — 整条 journal 不存在时返回空快照 → 闩锁被静默解除。「不可解除」的保证等价于「journal 文件不被删除」。需独立 sidecar 标记 + 空 journal 时 fail-closed |
| 5.11 | 双向风控准入 | `paper_grid_task.rs:916-918` 只对 `GridFill::Buy` 做准入 → 空头网格开仓完全不过风控。应基于「是否增加净敞口」判定（`strategy/src/risk.rs:374-379` 的 `strictly_reduces` 已有正确范式） |
| 5.12 | 风控非 Option | `paper_grid_task.rs:75` 等的 `account_risk: Option<...>` 默认 `None` = 静默放行一切，与仓库其余部分的 fail-closed 纪律相反。改必填，测试用显式 `permissive()` |
| 5.13 | scope 一致性 | `VOLUME_MAKER_ACCOUNT_RISK_SCOPE` 与 `ACCOUNT_RISK_SCOPE` 同为 `"paper"`，共享持久化状态但**限额策略不共享**（policy 存在实例里不进 journal）→ 谁先跑谁说了算。应把 policy 指纹写进 journal 并在构造时校验 |
| 5.14 | `ExecutionMode::Testnet` | `runtime/src/mode.rs:8-12` 无此模式 → testnet adapter 无法走完整 IntentExecutor 链路，只能走绕过风控与 saga 的 `testnet-lifecycle`。**「有风控保护」和「能真下单」是两条不相交的路径**，合流是实盘化的核心工作量 |
| 5.15 | 单进程多策略 | `paper_profile.rs:86-90,149-157` 的 catalog 从 `Option<Grid> + Option<Arbitrage>` 改 `Vec<Profile>`，共享一个 `AccountRiskAuthority`（阶段 1 完成后共享成本才可接受） |

---

### 阶段 6 · 工程效率（可与 1–5 并行）

| # | 任务 | 预期收益 |
|---|---|---|
| 6.1 | **strategy 内联单测** | `strategy/src` 4,691 行纯函数改 `#[cfg(test)]`，`cargo test -p strategy --lib` 只链接 1 个二进制 → 秒级反馈 |
| 6.2 | **合并 apps 测试二进制** | 22 个文件收敛为 3–5 个（每个二进制链接成本固定）→ 砍掉 75.5s 链接时间的大半 |
| 6.3 | **反转 `strategy → config`** | 把 `TryFrom<&XxxConfig>` impl 挪到 config 侧或 adapter 层。改一行 YAML schema 不再重编译整个策略层 |
| 6.4 | **抽 `crates/tasks`** | 把 `*_task.rs` / `*_saga.rs` / `continuous_*.rs` / `task_host.rs` 从 apps 搬出（纯搬运无逻辑改动），apps 从 26k 行降到 ~7k；`web-app` 改为依赖新 crate，不再传递依赖 `clap` |
| 6.5 | **泛型 `TaskOwner<S: Strategy>`** | 三个 paper task（5,969 行）的生命周期骨架高度重复。抽象后新增策略成本从 ~2000 行降到 ~300 行。**这是解锁快速迭代的关键** |
| 6.6 | 拆 `command.rs` 6,470 行 | 按命令族拆成 `command/{capabilities,paper,testnet,grid,...}.rs`；内嵌的手写 HTTP 客户端（`build_trusted_http_request:556`）提取为独立模块 |
| 6.7 | 重设计 `Strategy` seam | 当前 `StrategyMachine`（返回 `Vec<OrderIntent>`）表达力不足，被 5/7 的策略绕开，实际存在 4 套互不兼容接口。参照表达力最强的 `GridDirective`（`grid_protection.rs:60-78`）重设计：`on_event(&mut self, ev, ctx) -> Vec<Action>` + 可序列化 `state()` + `warm_up()`。**`&mut self` 是对的** —— 次高频策略必然有状态，关键不是「无状态」而是「状态可序列化 + 无 I/O」 |
| 6.8 | 网格状态可恢复 | 重启后 `VirtualGrid` 计数归零、`GridProtectionMachine::capital_baseline` 用当时权益重新锚定（`grid_protection.rs:875-877`）→ **亏损中重启会把亏损后权益当作「本金」，止盈线随之下移**；`cycles_per_hour` 归零又会让止损判定倾向 `ExitAll` 而非 `ResetGrid`。这是重启诱发的行为翻转，须作为 durable fact 落盘 |
| 6.9 | 统一 warm-up 概念 | alert 有 journal warm-up、套利有 cold-start backfill、**网格完全没有**。应由 runtime 统一：策略声明所需历史窗口，未填满前处于 `NotReady` |
| 6.10 | 替换手写 SHA-256 | `exchange/src/sha256.rs` 391 行 → `sha2` + `hmac`（RustCrypto，符合 `deny.toml` 许可证白名单，有 SHA-NI 加速）。**需依赖审批** |
| 6.11 | 替换 `serde_yaml` | 已 archived + 拖入 `unsafe-libyaml`（与 `unsafe_code = "forbid"` 形成事实缺口）。迁 `serde_yaml_ng`/`serde_norway`。`domain` 只在 dev-dependency 用它，可零成本先摘掉。**需依赖审批** |
| 6.12 | CI 补门禁 | bench 回归 job（criterion baseline 比对）；覆盖率；把 4 处重复的前端构建收敛为一次 artifact |

---

## 5. 依赖审批清单

仓库纪律是「不擅自新增依赖」，以下需要你逐项决策：

| 依赖 | 用途 | 阶段 | 建议 |
|---|---|---|---|
| **`tokio-tungstenite`** | WebSocket 行情与 user data stream | 4.6 / 4.7 | **强烈建议批准**。这是次高频的物理前提，且早在 2026-07-26 就已挂起。rustls 后端与现有 reqwest TLS 栈同源，不引入新 TLS 实现 |
| `criterion` | 基准测试（dev-dependency） | 0.5 | **建议批准**。没有它无法验证阶段 1 是否真的变快 |
| `proptest` | 属性测试（dev-dependency） | 3 | 建议批准。当前脏树修的 3 个 bug（journal 漏段、部分尾、resting limit 成交价）全属 property test 能自动发现的类型 |
| `sha2` + `hmac` | 替换手写 SHA-256 | 6.10 | 建议批准。签名是信任根，手写实现无第三方向量验证、无 side-channel 防护、无硬件加速 |
| `serde_yaml_ng` 或 `serde_norway` | 替换 archived 的 `serde_yaml` | 6.11 | 建议批准 |
| metrics 后端（`metrics` / `prometheus`） | `/metrics` 端点 | 0.4 之后 | 可选 —— 也可先只用已有的 `tracing` 埋点，暂不暴露 HTTP metrics |
| parquet / arrow | 回测磁带列式存储 | 3.3 | **暂不建议**。先用自定义定长二进制记录，避免拖入大型依赖树 |

---

## 6. 明确不做

| 项 | 理由 |
|---|---|
| **跨所高频套利的高频化** | 需要的基础设施（毫秒级多源、跨所库存管理、亚秒撮合）是另一个数量级的工程，且与「次高频」定位不匹配 —— 跨所套利要么做到毫秒级要么没有 alpha，中间地带不存在。历史价差套利（`arbitrage_history.rs`，分钟级均值回复）**保留并优先**，它是全仓最贴近次高频量化范式的模块 |
| **打开 mainnet 实盘** | 6 处 fail-closed 关卡的设计是正确的，保持关闭。真正的工作量在阶段 1–5，与是否实盘无关 —— **阶段 1 和 2 的问题在 Paper 模式下就已使系统无法完成一天的目标订单量** |
| **重写架构** | 深模块划分、fail-closed 边界、capability manifest、journal-first 顺序都是真资产。所有改造点集中且明确，不需要推倒重来 |
| **把 legacy Python 的 49 份 `legacy-parseable` 配置补齐** | 配置面 ≫ 实现面本身就是认知负担。应该收缩配置面去匹配实现，而不是反过来 |

---

## 7. 验收与风险

### 分阶段验收

| 阶段 | 可量化的退出条件 |
|---|---|
| 0 | 五门禁全绿；criterion 基线数字入库；`docker stop` 后 journal 无 `PartialTail`（10 次演练） |
| 1 | bench 证明 `admit()` 延迟与 journal 大小解耦；单笔订单 fsync 从 4→1；连续 24h 无容量停机 |
| 2 | 亏损触发强平的契约测试通过；1000 笔往返订单不耗尽额度；PnL 含真实费率 |
| 3 | 同一磁带的回测 PnL 与 paper 重放 PnL **逐笔一致**；walk-forward 只报样本外 |
| 4 | 10 标的刷新周期 < 200 ms；注入延迟 fixture 能触发 staleness；跨所 skew 超限拒绝 |
| 5 | 部分成交不再杀任务；周期性对账运行中可用；kill switch 真正撤单平仓 |

### 主要风险

1. **阶段 1 触碰资金安全语义**。内存投影引入了「内存状态与磁盘事实可能不一致」的新失效模式。缓解：保留低频后台交叉校验，不一致时 fail-closed；property test 覆盖「重放 = 增量应用」的等价性。
2. **阶段 4.2/4.3 必须同批**。单独修时间戳会引爆两颗地雷让系统更瘫。缓解：作为一个 tracer-bullet 提交，契约测试同时覆盖三处。
3. **阶段 2 改变既有 journal 语义**。PnL 事实与额度回收 transition 是新事件类型，旧 journal 需兼容读取。缓解：schema version 已有机制（`PAPER_ACCOUNT_SCHEMA_VERSION`），走既有版本化路径。
4. **能力矩阵同步**。每个阶段都会改变 capability，必须四处同步（`capability.rs` + `capability_contract.rs` + `docs/adapter-support.md` + README），否则契约测试会红。这是纪律不是风险，但容易遗漏。

### 建议的前三步

如果只做三件事，按此顺序：

1. **阶段 0**（~2 天）—— 装度量 + 修 SIGTERM。没有基线数字后面全是猜；SIGTERM 是 10 行代码消除一个真实的数据损坏风险。
2. **阶段 1**（核心）—— 内存增量投影 + 组提交 + 保留策略。这一项解决 80% 的性能问题，且是其余所有阶段的前提。
3. **阶段 2 + 3 一起做** —— 账本与回测互为前提。单独做回测而不修撮合成本，得到的结论依然不可信。

同时**阶段 6.1/6.2/6.3 可以立刻并行**（工作量小、无依赖、直接把策略迭代反馈环从 115 秒压到 10 秒以内，性价比最高）。
