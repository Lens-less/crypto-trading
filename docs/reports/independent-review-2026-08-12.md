# Independent Defect Review — 2026-08-12（refocus 落地后）

> **Historical fixed-point review.** This report describes commit `470e74b`.
> Commit `e025c0d` remediated the nine P1 findings and the associated Testnet,
> soak, and research acceptance gaps. The current disposition of the remaining
> findings is recorded in
> [`open-source-live-readiness-2026-08-13.md`](open-source-live-readiness-2026-08-13.md).
> Do not use this dated defect list as a current capability statement.

## 0. 范围与方法

- 审查对象：`574a4ae..470e74b` 五个提交（W0–W3 重聚焦改造），工作树 clean，最终提交 `470e74b`。
- 方法：三路独立深读（WebSocket 流层；连续 Testnet owner 与 soak 证据；共享策略与研究 runner），关键发现由主审逐条回读源码复核；研究 seam 的 P1 由审查者用真实冻结 lock 实测复现。
- 本报告只列缺陷与差距。`docs/reports/project-refocus-acceptance-2026-08-12.md` 声明的验收矩阵中，除下述条目外均经抽样核实成立。

## 1. 总览

核心安全不变量经逐点验证**成立**：journal-before-side-effect（含 future 被取消场景）、query-first 恢复且无任何盲目重提交路径、OS 级单写者租约、歧义（超时/5xx/-1007）永不判成功或失败重试、网络输入不可达 panic、TLS/主机固定、密钥不落日志。

但三块新表面共发现 **9 个 P1、10 个 P2**。多数 P1 不威胁资金安全（方向仍 fail-closed），威胁的是**产品旗舰声明本身的有效性**：kill/restart 恢复会被 kill 自己打穿、24h soak 证据在私有流死亡时照常通过、官方研究二进制无法复现自己内嵌哈希锁定的 G-005 协议、AC-R1 的"paper 执行消费同一策略"实际上尚未接线。

## 2. P1 清单（9 项）

### WebSocket 流层（`runtime/src/market_stream.rs`、`binance_user_data.rs`）

| # | 缺陷 | 位置 | 后果 |
| --- | --- | --- | --- |
| WS-1 | 无 `u` 字段的帧绕过序列回退检测，且第 722 行无条件覆盖 `last_source_sequence` 为 `None`，清空跟踪历史 | `market_stream.rs:698-722` | 过期重放帧可冒充新鲜行情进入下单闸门。修复：缺 `u` 判 `InvalidPayload`；`None` 不得覆盖已有序列 |
| WS-2 | broadcast 溢出（`Lagged`）后源不重连，而 owner 只接受新连接才会出现的 `Subscribed` ACK → 用户数据链路死锁至 24h 轮换 | `binance_user_data.rs:228-233` + `continuous_testnet.rs:297-361` | 成交/余额状态停摆数小时。修复：`Lagged` 置空会话走 `schedule_reconnect` |
| WS-3 | 一条畸形文本帧被映射为 `Err(SourceIdentityMismatch)`，supervisor 视为契约违规**永久终止**行情任务，不再重连 | `market_stream.rs:676-683`、`market_supervisor.rs:305-309` | 远端可触发的永久 DoS；decode/传输/身份错误全部塌缩为一个变体。修复：解析失败走 gap+计数重连；增加独立 decode 错误变体 |
| WS-4 | 未知符号路径置空会话但不设 `pending_retry`、不计失败 → 零延迟重连忙循环 | `market_stream.rs:685-697` | CPU 空转 + 连接风暴 → 429/418 IP 封禁，殃及同 IP 下单通道。修复：复用 `schedule_reconnect` |
| WS-5 | 序列回退分支硬编码 `retry_delay(1)`，不计数、不受 `max_reconnect_attempts` 约束；`last_source_sequence` 跨重连保留且不重置 | `market_stream.rs:703-714` | 交易所侧 update ID 回退（Testnet 周期性重置）时固定延迟无限循环，永不恢复也永不停止。修复：走计数重连；`connection_generation` 递增时重置序列跟踪 |
| WS-6 | `connect_async` 无超时；只发 Ping 从不校验 Pong；无"最近入站帧"看门狗 | `market_stream.rs:438-508` | TCP 半开时用户数据流静默挂死（行情侧有新鲜度兜底，用户流没有），soak 循环阻塞在 `next_item().await`。修复：握手 deadline + pong/idle 看门狗 |

### 连续 owner 与 soak 证据（`apps/src/continuous_testnet.rs`、`testnet_soak.rs`）

| # | 缺陷 | 位置 | 后果 |
| --- | --- | --- | --- |
| OW-1 | kill 落在 journal 追加的 syscall 中间会留半行；owner/soak/verifier 启动读取要求每行完整 JSON 否则 `InvalidJournal`/`PartialRecord`；runtime 已有的 `repair_recoverable_tail` 是 `pub(crate)` 且只在写路径触发，重启是先读后写，永远到不了 | `continuous_testnet.rs:752-753`、`testnet_soak.rs:1208-1210`、`history.rs:554` | **产品旗舰场景（kill/restart 恢复）被验收要求的 kill -9 自己打穿**：重启永久失败关闭，手工修 journal 又等同篡改证据。修复：启动时在写者租约保护下执行有界尾部修复，修复动作本身记入 journal |
| OW-2 | 失败阈值是"连续"计数且任何一类探测成功即清零；探测按 market/user/reconcile 三步轮转 → 私有流永久死亡时失败最多 1/3 连续，阈值 ≥2 永不触发；`segment_last_probe_at` 在失败探测上也推进；verifier 对 `user_data_stream` 只要求总数 >0 | `testnet_soak.rs:484,1119,1142` | 用户流开局 10 分钟死亡，任务照跑 23.8 小时、clean stop、`verify` 通过——**24h 流式证据门形同虚设**。修复：按样本类别独立陈旧度计数；verifier 加每类最小密度/最大间隔 |

### 研究 seam（`backtest/src/research_runner_shared.rs`）

| # | 缺陷 | 位置 | 后果 |
| --- | --- | --- | --- |
| RS-1 | 搬运 runner 时把毫秒月份 `last_close` 公式从 `next_open - 1ms` 改成 `next_open - (interval_micros/1000 - 1)µs`（日线 ≈ 提前 86.4 秒） | `research_runner_shared.rs:604-609`（对照 `574a4ae` 原公式已确认） | **已实测**：`crypto-trading-research` 通过 lock 的 SHA-256 校验后在 2018-01 行日历校验即拒绝自己内嵌哈希锁定的 lock 文件；2018–2024 全为毫秒月份，G-005 日线协议 100% 不可重跑，错误信息还会诱导"重新生成 lock"这种结果驱动操作。修复：恢复减 1 tick；补一条用真实 lock 首行钉住 `expected_month` 的测试 |

## 3. P2 清单（10 项）

1. **WS-7** 用户流 `Expired`/`ServerShutdown`/`StreamTerminated` 全部 `retry_delay(1)` 绕过失败计数；关闭原因分类靠对端可控的 reason 子串（`binance_user_data.rs:261-289`、`market_stream.rs:795-806`）。恶意/故障端点可致固定速率无限重连。
2. **WS-8** 成交去重键取自 executionReport 的 `"I"`（官方文档标注 Ignore），真正的成交标识是 `"t"`；`I` 缺失时指纹退化为 `(order_id, None, E, T)`，同一订单同毫秒两笔部分成交第二笔被判 `Duplicate` 丢弃，更高的累计量 `z` 被抛弃且不触发 reconcile——**fail-open**（`binance_testnet.rs:2346`、`binance_user_data.rs:422-430`）。修复：以 `t` 为键、`z` 入指纹；"指纹同而 `z` 不同"判 regression。
3. **WS-9** 连接内 spawn 任务无 abort 句柄，session 丢弃后旧 socket 最多存活一个 ping 周期（20s），重连叠加时积压旧连接，可能触到 Binance 单 IP 连接上限（`market_stream.rs:453-508`）。
4. **WS-10** 重连耗尽返回 `Ok(None)`，supervisor 记为 `SourceEnded` 干净退出——"重试 10 次全失败"与"正常播完"运维不可区分（`market_stream.rs:623-625`、`market_supervisor.rs:298-304`）。
5. **OW-3** 完整 lifecycle（submit→轮询→cancel）运行在单次 probe 的 5 分钟超时预算内，超时 drop future：owner 楔死在 `CampaignRunning`（健康连接上无自愈事件）；取消落在 `next_item()` 与 `ingest` 之间会永久丢弃条目（可能是 ACK）。安全性靠下层 PLANNED/query-first 兜住，宿主层逻辑本身是错的（`testnet_soak.rs:472`、`continuous_testnet.rs:456,290-299`）。
6. **OW-4** `killed_clean` 的"两次稳定对账"只比较相等不断言干净：自有 open orders 不断言为空；对账无余额字段、positions 仅永续——**现货成交残留对检查完全不可见**（`continuous_testnet.rs:607-728`、`exchange/src/model.rs:156`）。
7. **OW-5** 证据文件无完整性保护（纯 JSONL，可平凡伪造）；活跃时长基于可前跳的墙钟，段内拨快时钟一次探测即满足 `MinimumDuration`（`testnet_soak.rs:1153-1233`）。若威胁模型含伪造需哈希链/签名；至少 runbook 声明信任边界。
8. **OW-6** kill switch 按 `(owner_id, campaign_id)` 锁存而非账户/journal 级；优雅关停时若 pending campaign 遇网络不可用，`KILL_SWITCH_ENGAGED` 落盘而恢复未完成 → 该 campaign 对此 owner 永久不可达（`continuous_testnet.rs:596-605,765-774`）。
9. **RS-2** `PaperBarTask` 是**仅测试消费**的薄决策件：无 CLI、无 paper account、无 journal、无风控准入接线（对比 grid/arbitrage owner 均有真实接线）；capability 与验收报告把它当 AC-R1 证据，措辞夸大。要么接线成真 owner，要么把声明降级为"共享决策核就绪、执行接线未完成"。
10. **RS-3** `PaperBarTask` 与回测引擎的上下文语义不一致：`bar_index` 起点（任务本地 vs 数据集绝对）导致 momentum/vol-target 的 `is_multiple_of(rebalance_every_bars)` 再平衡日错位；`current_target` 语义（上次请求值 vs 账本实际达成值）使 vol-target 带宽判断漂移出不同交易序列。现有契约测试只对拍"任务 vs 裸策略"，测不出与引擎循环的分歧（`paper_bar_task.rs:61-73` vs `evaluation.rs:647-718`）。

## 4. P3 择要

- ws-api 订阅 ACK 未关联请求 `id`；`BinanceWsApiUserDataStreamSubscription` 派生 `Debug` 含明文 api_key 与可重放签名；HMAC key 无 zeroize（testnet-only，记录在案）。
- `FixedMarketStreamJitter` 接受 0 bps（生产装配用的是有校验的类型）；用户流代际 `saturating_add` 与行情侧 `checked_add` 风格不一致；Heartbeat（Pong）即重置重连预算。
- owner 进程内锁用 `absolute_path`，history 锁用 `normalized_lock_key`（Windows 大小写折叠），同进程不同拼写可各拿"独占"租约（可达性低）。
- 关停是"预算有界"（最坏 ~16h）而非"截止时间有界"；`stop_owner` 失败不写终态记录；serve 早退错误路径留孤儿 tokio 任务。
- 全 exchange crate 无 `X-MBX-USED-WEIGHT`/`X-MBX-ORDER-COUNT` 解析（无主动限速预算）；soak 对账探测 429 后无视 Retry-After 固定间隔继续；submit 429 永久烧掉 campaign 且 FAILED 事实不保留 retry_after。
- 研究产物写入非原子（`File::create` 截断后半途失败留半截 JSON）；W1 中止证据里那组聚合统计（75,096/43/163/22）的审计脚本未入库，数字不可独立复现；类型态门在库层可被 `|_,_| Ok(())` 回调绕过（CLI 边界正确，文档应明示）。
- 文档漂移：`docs/releasing.md` 仍声称 CI 覆盖双 OS × 双工具链全矩阵，实际 CI 已移除 Windows×stable 单元格。

## 5. 明确验证为可靠的部分

- 恢复正确性：`PLANNED` 落盘先于 submit；planned 后只走 `RESUMED` + 精确 `origClientOrderId` 查询（传输层测试确认全程无 POST）；无路径可无 pending 计划提交或重提交终态 campaign。
- kill switch 时序（落盘先于网络动作）、写者租约（OS 锁、崩溃自动释放）、submit 前 ACK 门控、歧义分类、panic/算术卫生、TLS/主机固定、路径白名单、密钥不落日志。
- 共享策略五族与 574a4ae 的 G-005 实现**逐 token 等价**（warmup 判据、回看下标、Donchian 窗口、样本方差、年化、带宽、`checked_sqrt` 全部一致）；研究路径无 f64、无 wall-clock、`BTreeMap` 有序、bootstrap 种子确定；1h 准入算术自洽（75,216−75,096=120；75,096−43=75,053；75,216−75,053=163）；holdout 结构审计只读时间戳/校验和，不构成泄漏；中止路径零产物落盘。
- v1 soak 记录确实被 schema v2 拒绝；query-delta 一致性检查算术可靠；`CAMPAIGN_RECOVERY_VERIFIED` 与 `UNCLEAN_RESTART` 的紧邻绑定设计正确。
- 工作树 clean、`.workflow/` 已忽略、CI 收缩与前端单次构建属实、capability 收缩为 4 行属实。

## 6. 生产/商业级最低补齐清单（按序）

**第一优先：修复正确性 P1/P2（本地可完成，先于一切外部门禁）**

1. WS-1/WS-8（数据正确性：序列绕过、成交去重 fail-open）。
2. OW-1（journal 尾部修复接线——否则 kill/restart 门禁本身无法通过）。
3. RS-1（`last_close` 公式回归 + 钉住真实 lock 的回归测试）。
4. 统一所有重连旁路进 `schedule_reconnect`（WS-3/4/5/7），补连接/pong 看门狗（WS-6），修复 Lagged 死锁（WS-2）。
5. OW-2（按类别陈旧度计数 + verifier 密度要求——否则 24h 证据无效）。
6. OW-3/OW-4（probe 超时结构、killed_clean 断言自有单为空并记录 residue）。

**第二优先：把"防再犯"变成门禁**

7. 增加"真实数据重现冒烟"合约测试：用冻结 lock 的首行（或截断样本）驱动 `expected_month`/准入路径——RS-1 之所以漏网，是因为 `cargo test` 全绿但从未对真实 lock 跑过一行。发布检查表加入"官方 runner 重跑 G-005 哈希逐字节一致"。
8. AC-R1 补完或降级：`PaperBarTask` 接入真实 paper owner（准入 + journal + 数量转换），并与引擎对齐 `bar_index`/`current_target` 语义，补"任务 vs 引擎"端到端等价测试（RS-2/RS-3）。这是 Edge→执行链路的最后一公里，不接线则 W1 的价值主张只完成一半。

**第三优先：可观测性与限速（运营一个 24h 连续进程的最低要求）**

9. 指标导出（Prometheus 文本端点或结构化周期日志均可）：流 generation/age/重连次数/gap 计数、REST 延迟与状态类、Binance 权重头、owner 相位、journal 追加延迟。目前只有 RUST_LOG 与 `/health`，spec §15 要求的投影没有可供告警系统消费的出口；配套最小告警规则（进程死亡、流陈旧、owner 进入 `RecoveryRequired`）。
10. 解析并预算 `X-MBX-USED-WEIGHT`/`X-MBX-ORDER-COUNT`；soak 探测尊重 Retry-After（P3 项一并处理）。

**第四优先：外部门禁执行（在 1–6 修复后才有意义）**

11. 真实凭证 24h Testnet soak + kill -9/restart + 对账 + 备份/恢复演练（现有 runbook 流程），证据按修复后的 verifier v2 归档。
12. 运维小项：备份调度与保留策略、时钟同步监控（spec §17 要求）、`releasing.md` 矩阵声明与 CI 同步、证据信任边界写入 runbook（OW-5）。
13. Python 归档外迁（已有 manifest，纯体积问题）。

**保持关闭**

- Edge gate 仍为零候选；以上全部完成也只意味着"平台生产级"，不改变"无策略可上线"的事实。mainnet 继续 fail-closed。
