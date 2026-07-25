# M4-M6 执行续期计划（续 2026-07-24 对齐计划）

> 状态：M0-M2 完成；M3 库代码完成但无可运行入口（见 §2.2 修正）；M4 进行中（约 35-40%）；M5/M6 未开始。
>
> 日期：2026-07-25
>
> 本文档不重写 [`2026-07-24-project-alignment-web-goal-plan.md`](2026-07-24-project-alignment-web-goal-plan.md)（下称"母计划"）的架构决策，只在其基础上做两件事：
> 1. 用直接读代码得到的证据，修正/收紧完成度判断；
> 2. 把 M4 剩余工作、M5、M6 拆成有序、可独立验证的 tracer-bullet，并标出需要你决策的分叉点。

## 1. 结论先行

- 母计划的架构（Control Plane / Operator Read Model / Runtime Supervisor / Risk Authority / Operation Journal 五个深模块）本身没有问题，**不需要推倒重来**，剩余工作是在这五个模块上继续叠 tracer-bullet。
- 之前"55% 完成"的估计方向是对的，但 M3 的完成度需要向下修正：连续引擎（monitor/alert/scanner）**库代码和契约测试确实完整**，但 CLI/服务没有任何入口能把它们跑起来——这不是 M4 的债，是 M3 遗留的债，会直接卡住 M4 的"连续 Grid/Arbitrage owner"。
- 当前工作树有 **23 个文件、2359 行未提交修改**，对应 `rust/RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md`（同样未提交）里列出的修复：grid 多级穿越、risk 持仓矛盾状态、paper 库存预留、Binance/config 边界。这些是正确性修复，必须先落地、跑绿全量门禁，再继续叠新功能——否则新代码会建立在已知有 bug 的 grid 几何模型上。
- 找到一个此前被当作"硬约束"的假设其实可以解除：`std::fs::File::lock` 在 **Rust 1.89.0** 已转正（见 §5），而工作区当前 MSRV 锁定在 1.85.0。跨进程单写者锁一直被列为"不能诚实宣称完成"的 NO-GO 项，根源就是这个 MSRV 锁定，而不是能力上做不到。这是本计划里最值得你现在拍板的一件事。

## 2. 证据核验

### 2.1 与母计划一致的部分

- M0（capability 清单）、M1（Journal/Read Model）、M2（Control Plane + 只读 Web）：直接读 `rust/crates/control-plane`、`rust/crates/web/src/api.rs`、`rust/crates/web-app/src/lib.rs` 确认路由只有 `GET /system /capabilities /monitor /alerts /tasks /scanner /executions /events`，无写路由，与声明一致。
- M4 的 `PaperAccountAuthority`（`runtime/src/paper_account.rs`，1409 行）与 `DurablePaperArbitrageSaga`（`apps/src/paper_arbitrage_saga.rs`，740 行）确实实现了 pending/uncertain/committed 状态机、cost model v1、幂等键；`release()` 函数（行 704-734）的注释明确写着"committed exposure 需要后续携带已验证对账证据的独立 transition"——即显式 reconcile 确实还没做，母计划的自述准确。

### 2.2 需要修正的部分：M3 "完成" 只是库层完成

- `apps/src/command.rs` 中 `run_price_alert`、`run_scanner` 目前**始终 `bail!`**（运行时不可用）；`run_monitor` 只接受 `--replay <fixture>`，拒绝任何连续外部源。
- `apps/src/cli.rs` 没有任何 daemon/continuous 子命令。
- 但 `apps/src/continuous_monitor.rs`（921 行）、`apps/src/alert/*`（约 2100 行）、`apps/src/scanner.rs`（805 行）都是完整、有 start/status/stop 生命周期、有契约测试的深模块——**只是没有被接到任何二进制入口上**。
- 根 `README.md` 与 `rust/README.md` 也如实标注"7×24 服务运行时尚未实现"。

**含义**：这不只是文档口径问题。M4 要做"连续 Grid 状态机"和"连续 Arbitrage owner"，天然需要一个能长跑的进程宿主（start/status/stop + 优雅关闭 + 重启恢复）。与其为 Grid/Arbitrage 各建一套，不如先把 M3 已经验证过的 `ContinuousMonitorTask` 模式（`select!` + 双 source + durable checkpoint）**抽成一个可复用的任务宿主**，monitor/alert/scanner/grid/arbitrage 都接到同一个宿主上。这把"M3 CLI bootstrap 缺口"和"M4 任务生命周期"两个独立列在母计划里的条目，合并成一个更小的垂直切片。

## 3. 立即前置项：落地未提交的修复

在写任何新代码之前：

1. `git diff` 逐文件核对 23 个已改文件是否都对应 `RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md` 里的条目（不能有意外的、未被文档记录的改动）。
2. 跑全量门禁（母计划 §6 的 Rust 门禁七条命令）。
3. 补一次 `cargo audit`。
4. 提交时说明这是"落地已审计但未提交的修复"，不与后续 tracer-bullet 混在一个提交里。
5. 把 `LICENSE`（未跟踪）和 `RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md`（未跟踪）一并纳入版本控制。

这一步不产生新功能，纯粹是把已完成但悬空的工作收口，同时是后续所有 M4 工作的正确性前提（尤其是 grid 多级穿越修复，直接影响即将要建的连续 Grid 状态机）。

## 4. M4 剩余工作：有序 tracer-bullet

在 §3 完成后，按以下顺序推进（每条都是母计划 §7 纪律要求的"一次只推进一个可独立验证的 tracer-bullet"）：

### 4.1 任务宿主抽象（解 M3 遗留 + 为 4.3/4.4 铺路）

- 把 `ContinuousMonitorTask` 的 `start/status/stop` 契约抽成 `apps` 内的通用任务宿主 trait/结构，`ContinuousMonitorTask` 自身改为该宿主的一个实现，行为不变（有既有契约测试兜底）。
- 加一个真正的服务化 CLI 子命令（例如 `monitor --serve`，区别于现有 `--replay`），让 monitor 第一个跑通"能作为长跑进程启动"。
- 退出条件：现有 `continuous_monitor_task_contract.rs` 全绿 + 新增"CLI 可启动/可停止"的进程级冒烟测试。

### 4.2 显式 Reconcile

- 给 `PaperAccountAuthority` 加一个独立 transition：只接受"已验证对账证据"（例如交易所回执快照的哈希/序列号，而不是调用方一个 reason 字符串），才允许把 committed exposure 转为 released。
- 对账证据本身要写入 journal，可重放验证。
- 退出条件：伪造/缺失证据的 release 请求失败关闭；补齐"对账失败"恢复 fixture（母计划 M4 退出条件里点名的最后一项缺口）。

### 4.3 跨进程单写者锁（依赖 §5 的 MSRV 决策）

- 若 MSRV 提升到 1.89+：直接用 `std::fs::File::lock`/`try_lock` 包一层，journal writer 启动时获取独占锁，取不到锁立即失败关闭（不重试、不等待），锁文件路径与 journal 目录绑定。
- 若不提升 MSRV：退化方案是"PID 文件 + 存活性探测"，但这只是启发式，不是真锁，必须在 capability 清单里诚实标注为"best-effort"而非"durable"。**不建议这条路**，见 §5。

### 4.4 Paper 任务生命周期（start/status/stop/cancel）

- 复用 4.1 的任务宿主，把 `PaperAccountAuthority` + `DurablePaperArbitrageSaga` 接成一个可 start/stop 的任务。
- 优雅关闭：收到 stop 后不再新开 reservation，等在途 batch 落 terminal 事实再退出。
- 重启恢复：复用 4.1 宿主已有的"冷启动只重建最后持久事实"语义。

### 4.5 连续 Grid Paper 状态机

- 前提：§3 的 grid 多级穿越修复已落地。
- `strategy/src/virtual_grid.rs` 目前只服务 scanner 的确定性回放；连续版本需要一个新的 owner（类比 4.1 宿主 + 4.4 的账户接线），把行情事件推进 grid 几何，每次穿越触发一次 paper 下单意图，走已有的 `PaperAccountAuthority`。
- 不做马丁/移动/剥头皮/止盈止损——那些是 P1 之后的策略语义恢复项，母计划已明确排序为"先做可恢复的连续 paper supervisor，再逐项恢复策略语义"。

### 4.6 连续 Arbitrage Owner

- 把现有一次性两腿 `DurablePaperArbitrageSaga` 升级为常驻 owner：监听 4.1 里已经跑通的连续 monitor 事件流，事件触发时机成熟才发起 saga，而不是外部一次性调用。
- 复用 monitor 的 exact-pair 双 source 校验模式，避免重新发明。

### 4.7 CLI/Web 写入口

- 母计划已经约束好 interface：Control Plane 只加一个 `submit(command)`，命令必须带幂等键、权限上下文、风险确认。
- 先上 CLI（`paper grid start/status/stop`、`paper arbitrage start/status/stop`），验证读回路径（结果必须来自 journal/read model，不能是内存态)；Web 写入口在 CLI 验证过一轮后再接，复用同一个 Control Plane `submit`。

### 4.8 剩余恢复 fixture

- timeout、撤单不确定、对账失败（4.2 产出）——补齐后母计划 M4 退出条件才算真正闭合。

## 5. 关键决策：MSRV 1.85.0 → 1.89.0+

- `std::fs::File::lock`/`try_lock` 在 [Rust 1.89.0 转正](https://github.com/rust-lang/rust/pull/136794)（Cargo 自身也在同版本切换为用这个标准库 API 做构建锁，而不是内部临时实现）。
- 这条路径完全不违反"不擅自新增依赖"的约束——它是标准库能力从 unstable 变 stable，不是新增 crate。
- 代价：MSRV 上移意味着 CI 矩阵、`rust-toolchain` 文件、README 里所有"1.85.0"都要同步改，且要重新跑一遍母计划 §6 的全量门禁确认无回归。这是一次性成本，不是持续负担。
- 不这么做的代价：4.3（跨进程锁）要么无限期挂起（capability 永远标注"process-local only"），要么退化成 PID 文件之类的弱语义，与母计划"durable、可证明"的标准冲突。
- **建议：批准 MSRV 提升**，这是我认为唯一值得在开工前由你明确拍板的技术决策，其余 tracer-bullet 顺序是工程判断，不需要逐条确认。

## 6. M5：单交易所 Testnet 纵向打通

现状比母计划记录的"0%"要好：`exchange/src/binance_testnet.rs`（898 行）和 `hyperliquid_testnet.rs`（940 行）已经实现了带签名的请求协议骨架（`BinanceRequestSigner`、HMAC seam、recv-window、订单/盘口 wire 类型），capability 清单标注为 `testnet_protocol: protocol-only`。但两者都没有接到任何可运行命令上，也没有真实 testnet 网络验证。

母计划已给出正确顺序（§ M5），这里只补一个选型建议和具体化：

1. **选 Binance testnet 而不是 Hyperliquid**：现有 `exchange/src/binance.rs` 已经有公开行情的生产验证（`binance_public_contract.rs`），testnet 认证层复用同一套错误处理/截断/边界护栏，增量最小。Hyperliquid 留到第二个 venue。
2. 官方签名向量：用 Binance testnet 官方文档的已知请求/签名对写离线契约测试，不依赖真实网络也能锁定签名正确性。
3. 下单/查询/撤单/部分成交/断线恢复/限流/时钟偏差：每一项都对应一个独立契约测试 + 一次真实 testnet 手工验证记录（证据留痕，不是"跑过一次就算"）。
4. 权威对账：testnet 账户余额/持仓/挂单作为 truth source，与本地 `PaperAccountAuthority` 投影比对，这一步直接复用 4.2 做出来的 reconcile 机制——M5 事实上是 M4 reconcile 能力的第一个真实消费者。
5. Soak 测试：至少 24 小时连续跑 4.1 任务宿主 + testnet 源，人工验证一次恢复演练（kill -9 + 重启）。

## 7. M6：生产候选与部署

母计划 §5 M6 的交付项没有需要修改的地方，这里补两块母计划提得较略的内容：

### 7.1 Web 前端信息架构扩展

当前 Web 只有 Overview / Executions / Integrations / Alerts / Tasks / Scanner 六页，全部只读。参考 `docs/research/upstream-repository-alignment.md` 里对 tickflow-stock-panel 的结论（IA 按任务切分、深链筛选、SSE 长任务反馈值得借鉴；A 股业务/巨型前端 facade 不借），M6 阶段要补的页面：

- **Strategies**：Grid/Arbitrage/Alert/Scanner 配置浏览 + 4.7 写入口落地后的 paper 启停控件。
- **Risk**：账户级 reservation、总敞口、kill switch——P0 完成前保持只读，与母计划一致。
- **Replay**：历史快照回放，明确标注"不是真实撮合回测"。
- **Settings**：数据目录、日志、通知、只读凭证状态。

技术选型问题（需要你决策，见 §8）：现有 `crates/web/assets/app.js` 是无构建步骤的原生 JS，嵌入同一个 Rust 二进制里，符合"单容器交付、无外部依赖"的母计划精神。tickflow 用的是 React+Vite+TS+Tailwind 独立前端。四个新页面主要是表格/表单，不需要图表库级别的复杂度，**建议继续走无构建原生 JS 路线**，避免引入前端工具链这个新的供应链面。如果你后续想要 tickflow 那种图表密度（K 线、ECharts），再单独评估引入构建步骤的成本。

### 7.2 部署

- Dockerfile：单阶段构建 release 二进制 + 静态资源，比照 tickflow 的两阶段思路但去掉前端构建阶段（因为 §7.1 建议不引入前端构建）。
- 持久卷只挂 journal 目录；配置以只读 volume 挂载；secret 通过环境变量注入，不进镜像。
- 备份/恢复：journal 是 append-only JSONL，备份即"定期复制 + 校验 FNV 边界锚点可重放"，恢复演练要作为发布门禁的一部分,不能只是"理论上能恢复"。

## 8. 需要你决策的点

1. **MSRV 1.85.0 → 1.89.0+**：批准后我会先做这一步（连带四条门禁全部重跑），再开始 4.3。如果不批准，4.3 只能做退化的 PID 文件方案并在文档里如实降级标注。
2. **M5 首个 testnet adapter**：Binance（建议）还是 Hyperliquid？
3. **Web 前端技术栈**：继续无构建原生 JS（建议，延续 M2/M3 已验证路线）还是切到 React/Vite 独立前端（更贴近 tickflow，但引入新工具链）？
4. **执行节奏**："Goal 模式"在这个项目里不是 Claude Code 的内置功能，是母计划 §7 自定义的执行纪律（一轮一个 tracer-bullet、每个里程碑跑全量门禁、只提交本轮明确拥有的文件）。我可以按这个纪律继续——但每个 tracer-bullet 本身（比如 4.2 显式 reconcile）都是数百行、涉及资金安全语义的改动，逐条做完大概率跨多轮对话/多次提交。你希望我：
   - (a) 现在直接开始 §3（落地未提交修复）并按 §4 顺序连续推进，每完成一个 tracer-bullet 就提交并简报，还是
   - (b) 每完成一个里程碑（比如整个 M4）暂停一次，等你确认再继续？

### 8.1 决策记录（2026-07-25）

| 决策点 | 结论 |
| --- | --- |
| MSRV | 批准提升到 1.89.0+，解锁标准库 `File::lock` |
| M5 首个 testnet adapter | Binance |
| Web 前端技术栈 | 继续无构建原生 JS，延续 `crates/web/assets/app.js` 路线 |
| 执行节奏 | 连续推进：落地 §3 未提交修复后，按 §4 顺序连续做 tracer-bullet，每条完成即提交并简报 |
