# 项目重聚焦规格（Refocus Spec）— 2026-08-12

## 0. 文档控制

| 字段 | 值 |
| --- | --- |
| 状态 | 建议稿，等待 operator 决策后转为执行基线 |
| 日期 | 2026-08-12 |
| 输入证据 | 全仓代码探查、`docs/reports/release-readiness-2026-08-12.md`、`docs/internal/specs/LIVE_TRADING_V1_SPEC.md`、G-001…G-006 交接记录、当前 dirty worktree（52 文件，+5455/−304） |
| 方法约束 | 第一性原理（从目标函数推导瓶颈）+ 奥卡姆剃刀（凡不在最短价值链上的表面一律冻结或删除，不引入新抽象） |

本文档不推翻已有的安全边界与 `LIVE_TRADING_V1_SPEC.md`，而是给出**投入顺序的修正**和**表面积的收敛清单**。所有失败关闭、能力清单、journal-as-truth 的既有约束继续有效。

## 1. 第一性原理：目标函数与瓶颈

交易系统的期望价值是乘法结构，不是加法结构：

```text
E[价值] ≈ Edge(正期望策略) × Execution(信号变成交的能力) × Risk(存活能力) × Capital
```

任何一个乘子为零，整体为零。对照当前仓库的实测状态：

| 乘子 | 当前状态 | 证据 |
| --- | --- | --- |
| Risk（风控/安全） | **优秀，边际收益已递减** | fail-closed 能力清单、journal 先行、~1,176 个 Rust 测试、两轮独立评审通过 |
| Execution（执行通道） | **严重缺失** | 全依赖树无任何 WebSocket 客户端；实时行情=REST 轮询；无私有账户流；连续 owner 全部 replay-backed；唯一真实下单路径是 Testnet 一次性 lifecycle |
| Edge（策略正期望） | **当前为零且证据为负** | G-005 冻结实验 22 个配置全部未通过 holdout；搜索已关闭 |
| 研究↔执行连通性 | **断裂** | `backtest/candidates.rs` 评估 momentum/Donchian/vol-target；`strategy` crate 实现 grid/arbitrage/volume-maker；两个集合**没有交集、没有共享契约** |

结论：继续在 Risk 乘子上加固（更多安全文档、更多流程证据、更多合约测试）对 E[价值] 的贡献趋近于零。**瓶颈是 Edge 和 Execution，以及连接两者的策略契约。**全部新增投入应转向这三处。

## 2. 现状诊断

### 2.1 P0 — 战略层问题

- **S1 无验证 edge。**这不是耻辱而是诚实的负结果，但它意味着当前任何 mainnet 方向的投入（三二进制拆分、mainnet 适配器、shadow soak）都在为一个不存在的策略修高速公路。
- **S2 研究与执行的策略集不相交。**即使 G-005 有候选通过，仓库里也没有能执行它的 owner；反过来，真正能执行的 grid/arbitrage 从未被系统性历史评估过。评估管线（G-004 seam）和执行管线（paper owners）消费的是两套互不认识的策略类型。
- **S3 无实时数据层。**`LIVE_TRADING_V1_SPEC.md` 第 9/10 章（行情流、User Data Stream）是 V1 的地基，但目前连 `tokio-tungstenite` 依赖都不存在。这是从"回放/轮询系统"到"交易系统"之间缺失的整层。
- **S4 投入错配。**63 个 markdown 文档、三处重复记录同一证据（goal board + handoffs + `.workflow/ultracode` 的 38-agent 编排 dump）、两棵自动化树（已完成的 G 系列 + 未启动的 Live V1）。流程密度显著高于产品迭代速度。

### 2.2 P1 — 结构层问题

- **A1 `apps` 是 god-crate。**`command.rs` 单文件 6,551 行；五个 task-host（monitor/scanner/alert/volume-maker/soak）结构高度雷同；paper owner 各 2.5–3.5k 行。改动成本被人为放大。
- **A2 能力表面积虚宽。**capability manifest 与配置目录仍然承载 5 个 config-only 交易所（backpack/edgex/grvt/lighter/paradex）和 2 个 legacy-only（okx/variational），每一行都有真实的同步维护成本（manifest + `adapter-support.md` + 合约测试），而价值为零。
- **A3 CI 过重。**前端在几乎每个 Rust job 里重复构建；双 OS × 双工具链矩阵对文档级改动也全量执行；单 PR 墙钟时间被小前端树不成比例地放大。
- **A4 流程资产未归档。**已完成的 G 系列 board/handoffs 与未启动的 Live V1 board 并存，索引只指向后者；Live V1 board 引用尚不存在的 handoff 文件。
- **A5 研究入口寄居在 example。**`backtest/examples/g005_evaluation.rs`（1,404 行）承担了产品级职责（校验 provenance、写 artifact、type-state 门禁），却以 example 形式存在。可以接受，但下一轮实验前值得收敛为 `crypto-trading-research` 的正式入口（`LIVE_TRADING_V1_SPEC` 第 7.1 节已预留此二进制名）。

### 2.3 P2 — 卫生层问题

- **H1** 全部 G-001…G-006 成果（+5,455 行、已通过全部本地门禁和两轮独立评审）仍未提交，是当前最大的单点丢失风险。
- **H2** 零散残留：`PlaceholderPage` 仅被自己的测试引用；`router.tsx` 注释仍称页面是占位骨架；`config/src/monitor.rs` 解析无实现消费的 `websocket.*` 字段；`exchange` 与 `backtest` 各有一份 `sha256.rs`；`design-previews/` 无引用；Windows 工作区 CRLF 告警噪音。
- **H3** `archive/python-legacy/` 占仓库约 44% 字节（~6.9 MB/367 文件），`LIVE_TRADING_V1_SPEC` 16.3 节已允许拆库。

## 3. 目标与非目标

### 3.1 目标（本 spec 完成时为真）

1. 研究与执行共享同一策略契约：一个策略实现能同时被回测评估器和 paper owner 消费。
2. 存在真实的流式行情 seam（WebSocket），并有 fail-closed 的断线/乱序/过期证据。
3. 下一轮 edge 实验已预注册并执行，结论无论正负都冻结存档。
4. 与最短价值链无关的表面（config-only 交易所、重复流程树、重复 CI 构建）被移除或冻结。
5. 工作区干净：已验证的工作全部落为提交。

### 3.2 非目标

- 不新增交易所、不做多腿跨所套利、不做 ML 策略。
- 不重写 journal、不换数据库、不引入消息队列。
- 不在 Edge 门禁通过前投入 mainnet 适配器、三二进制拆分、shadow soak（除非 operator 明确选择"手动实盘"路线，见第 9 节 D2）。
- 不删除 Paper/Testnet/回放/故障注入等验证工具（它们是恢复证据的来源）。

## 4. 方案总览

四条工作流，按依赖排序。W0 立即执行；W3 与 W1/W2 并行；W1 与 W2 可并行但共享策略契约这个前置。

| 工作流 | 内容 | 为什么它在最短价值链上 |
| --- | --- | --- |
| W0 | 提交卫生 | 已验证工作不落盘等于没做 |
| W1 | Edge 发现：统一策略契约 + 下一轮预注册实验 | 打掉为零的乘子 |
| W2 | 实时通道：WS 行情 → Testnet 账户流 → 连续 owner | 打掉缺失的乘子；全部在 Testnet 内，不触碰 mainnet 边界 |
| W3 | 减法：表面积、流程、CI 收敛 | 降低其余一切工作的单位成本 |

## 5. W0 — 立即行动（当天）

1. 将当前 52 文件的改动按逻辑分组提交（或单提交，附 release-readiness 报告为提交说明依据）。`.workflow/` 编排 dump 二选一：随提交入库到 `docs/internal/history/`，或加入 `.gitignore`——不要留在未跟踪状态。
2. 顺手清理 H2 中零成本项：删除 `PlaceholderPage` 及其测试、修正 `router.tsx` 过时注释。

## 6. W1 — Edge 发现

### 6.1 统一策略契约（前置，唯一新增的抽象）

在 `strategy` crate 定义一个 bar 驱动的纯策略接口（目标仓位模型，Decimal，无 I/O），例如：

```text
trait BarStrategy {
    fn on_bar(&mut self, bar: &SpotBar) -> TargetExposure;  // [0,1] 的目标敞口
}
```

- `backtest/src/candidates.rs` 的五个家族改为实现此 trait（评估器消费方式不变）。
- 未来任何 paper/live owner 消费同一实现。研究通过的策略即执行的策略，消除 S2。
- 这是本 spec 允许的唯一一处新抽象；除此之外不加 trait、不加层。

### 6.2 下一轮预注册实验（沿用 G-004 seam，不改评估语义）

G-005 的负结果只证伪了"BTCUSDT 日线、长-或-空仓、趋势/波动家族"这一个格子。下一轮沿**一个**维度扩展（奥卡姆：一次只动一个变量），推荐顺序：

1. **频率**：BTCUSDT 小时线（Binance Vision 同源 1h 归档），同一家族重跑。日线趋势失效不代表更高信息密度下同样失效；数据获取成本最低，seam 改动最小（`spot_data.rs` 已按 cadence 参数化）。
2. **资产**：加入 ETHUSDT 等 2–4 个高流动性 Spot 对，检验结论是否 BTC 特异。
3. **家族**（最后才做）：为既有可执行的 grid 家族建立评估适配器。注意诚实声明：历史盘口/深度不可得，maker 成交保真度天然弱，结论只能作为弱证据。

规则不变：预注册冻结 → 选择持久化 → 一次性 holdout → conjunctive 判据 → 负结果照样存档关闭。**在某个简单家族通过之前，不引入更复杂的家族。**

### 6.3 研究入口转正

将 `g005_evaluation.rs` 收敛为 `crypto-trading-research` 二进制（或 `apps` 下的 `research` 子命令，capability 仍标 research-only），消除 1,400 行 example 承担产品职责的错位。

## 7. W2 — 实时执行通道（全程 Testnet，mainnet 边界不动）

### 7.1 阶段 E1：WebSocket 行情 seam

- 引入 `tokio-tungstenite`（走 `cargo-deny`/audit 供应链门禁）。
- 实现 Binance Spot bookTicker 单符号流：心跳、服务端轮换、有界队列、带抖动的指数退避、freshness 门禁、序列/代际标记——即 `LIVE_TRADING_V1_SPEC` FR-MD-01/03/04，先对 Testnet/公开端点。
- **第一个消费者是现有 `monitor --live`**：把 REST 轮询替换为流观测（保留轮询作为显式降级），落地点风险最低、已有双源对账语义可直接复用。
- 顺带删除 `config/src/monitor.rs` 中无实现的 `websocket.*` 配置字段，或让它开始被真实消费——二者取一，不允许继续"解析但无语义"。

### 7.2 阶段 E2：Testnet User Data Stream + 连续 lifecycle owner

- 用 Testnet 凭证订阅账户流（executionReport/余额），实现去重、单调累计、regression 触发对账——即 FR-AD-01/02/03。
- 把 `testnet_lifecycle` 的一次性 submit-query-cancel 升级为**一个**连续 owner：journal 先行、query-first 恢复、单 writer 租约、闩锁 kill switch。这直接复用第 12/14 章的状态机定义。
- 交付判据：kill/restart 演练 + 24h Testnet soak 在流式路径上通过（现有 soak host 语义扩展，不新建平行系统）。

### 7.3 阶段 E3（被门禁阻塞，见第 9 节）

mainnet read-only shadow 及其后的一切，仅在 D2 决策做出后启动。三二进制拆分（spec 16.1 节）推迟到该阶段，现阶段用既有 capability manifest + 依赖图断言维持边界即可。

## 8. W3 — 减法清单（奥卡姆剃刀）

| # | 动作 | 说明 |
| --- | --- | --- |
| R1 | capability manifest 中 5 个 config-only 交易所折叠为一条 `unsupported venues` 记录（或整行移除），对应 YAML 移入 `config/legacy/` | 同步修改 `adapter-support.md` 与其一致性合约测试；测试驱动地做 |
| R2 | `volume-maker`、`price-alert`、`scanner` 声明为 **maintenance-frozen**：CI 保绿，不再演进 | 在 README 能力表加一列冻结状态；不删代码 |
| R3 | 已完成的 G 系列 automation 树（board/runbook/handoffs）移入 `docs/internal/history/`；只保留一棵活动 board | 消除双树；Live V1 board 待 D2 决策后再激活或改写 |
| R4 | `.workflow/` 归档或 gitignore（与 W0 一致） | 编排 dump 是证据不是产品 |
| R5 | CI：前端 `dist` 构建一次、artifact 传递给 Rust jobs；docs-only 改动加 paths-ignore；矩阵砍掉 `windows × stable`（保留 windows×MSRV 与 ubuntu×两者） | 目标：中位 PR 墙钟时间减半 |
| R6 | `archive/python-legacy/` 拆到独立归档仓库，主仓库留 README 指针 | spec 16.3 已允许；−44% 仓库体积 |
| R7 | 合并两份 `sha256.rs` 为一处（放 `domain` 或独立小模块） | 纯去重 |
| R8 | 删除 `design-previews/` | 无引用的设计考古 |
| R9 | 中期：从五个雷同 task-host 中提取共享 serve/status/stop 骨架，`command.rs` 按子命令拆文件 | 仅在下次触碰相应代码时顺带做，不发起专项重构 |

## 9. 门禁与开放决策

### 9.1 新增门禁（叠加在既有 promotion stages 之上）

- **Edge gate**：任何策略的自动执行（Paper 连续运行之后的推广）要求：通过预注册评估 + holdout，且完成一段 Paper 观察期。G-005 判据沿用。
- **Streaming gate**：任何连续 owner 接入 Testnet 之前，WS 行情/账户流必须有断线、gap、过期、重放的 fail-closed 合约测试证据。

### 9.2 需要 operator 决策的问题

| # | 决策 | 影响 |
| --- | --- | --- |
| D1 | 项目首要目标：受控真实收益（默认） vs 工程作品集 | 若为后者，W1 可降级，现状已近达标 |
| D2 | 是否要"无策略的手动 mainnet 交易"作为近期目标 | 决定 E3/mainnet 适配器是否启动；默认**否**，被 Edge gate 阻塞 |
| D3 | W1 扩展维度确认（默认：频率→资产→家族） | 决定下一轮预注册内容 |
| D4 | CI 矩阵取舍确认（R5） | 平台覆盖 vs 迭代速度 |
| D5 | archive 拆库时间点（R6） | 纯体积问题，无功能影响 |

## 10. 验收标准

- **AC-R1** 至少一个策略实现同时被回测评估器与一个 paper owner 消费，有合约测试证明两处行为一致。
- **AC-R2** 依赖树含 WS 客户端；`monitor --live` 走流式路径，断线/过期/gap 的 fail-closed 测试通过。
- **AC-R3** Testnet 连续 owner 通过 kill/restart query-first 恢复演练与流式 24h soak。
- **AC-R4** capability manifest 不再逐行宣告 config-only 交易所；`adapter-support.md` 同步且合约测试绿。
- **AC-R5** 前端在 CI 中只构建一次；docs-only PR 不触发全矩阵。
- **AC-R6** `git status` 干净；仓库中只有一棵活动 automation 树。
- **AC-R7** 下一轮实验的预注册文档存在、在 holdout 打开前冻结、结论（含负结果）已存档。

## 11. 风险

- **Grid/maker 回测保真度**：历史 spread/深度不可得，paper maker 语义近期才修正过一次高估。任何 grid 家族的评估结论必须显式降级为弱证据，成本取保守上界。
- **减法触发合约测试连锁**：manifest/文档/测试三方同步是设计使然，R1 必须测试先行，禁止为了删除而放宽任何断言。
- **新增 WS 依赖的供应链面**：走既有 `cargo-deny` + audit 门禁；不因引入流式层而放松任何有界资源约束（有界队列、溢出即降级是硬要求）。
- **负结果重复出现**：W1 第二轮仍可能全灭。这是可接受的结果——它以低成本继续证伪，Edge gate 保证零成本的资金暴露。不允许因连续负结果而放宽判据（这正是该管线存在的意义）。

## 12. 一句话版本

> 安全壳已经远超当前权限所需；停止加固，把全部新增投入换到"找到一个能通过自家 holdout 的策略"和"从轮询/回放升级到真实流式 Testnet 执行"上，同时删掉所有不在这条链上的表面积。
