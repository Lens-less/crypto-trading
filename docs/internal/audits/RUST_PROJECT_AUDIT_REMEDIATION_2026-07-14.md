# Rust 项目对抗审查复核与修复报告（2026-07-14）

## 1. 最终结论

对 `RUST_PROJECT_AUDIT_ADVERSARIAL_REVIEW_2026-07-14.md` 的独立复核结论是：Claude 对原报告的主要事实判断可靠，原列 18 项中没有发现凭空捏造的问题；其中 16 项事实完全成立，P1-04、P1-07 的代码层缺陷成立，但对当时可达 paper CLI 的影响被放大。

本轮已经对当前可达的 Rust/paper 路径完成系统性修复。18 项原始问题目前为：

- **已关闭：15 项**；
- **部分缓解、仍是 live 硬门槛：3 项**（P1-05、P1-09、P2-08）；
- **未处理：0 项**；
- **live 结论：仍为 `NO-GO`**。

当前 paper one-shot 可作为有边界的策略与执行内核验证面：Arbitrage 使用调用方显式提供的顶层盘口和深度；Grid 只使用参考价格模拟 resting/open 挂单。两者每次进程都从空 paper 账本开始。它们不是 testnet，更不是生产验收证据。

## 2. 复核范围与方法

| 项目 | 内容 |
| --- | --- |
| 固定审查基线 | `1a6bf0fb98f682df45d8761a4dffa3e97717571d` |
| 活跃修改范围 | `rust/`、`.github/workflows/rust.yml` |
| 冻结范围 | `archive/python-legacy/`，本轮未修改 |
| 复核方式 | 静态调用链、失败回归、动态 CLI、MSRV/stable 双工具链、Clippy、release、RustSec |
| 修复原则 | 当前可达路径先失败关闭；live 不因局部缓解而开放 |

每个核心修复均有对应回归覆盖。验证重点不是“可以编译”，而是禁用控制、风险授权、盘口深度、账本原子性、恢复上下文和资源上限是否真正落到提交边界。

## 3. 原 18 项逐项裁决

| ID | 当前状态 | 复核与修复结果 |
| --- | --- | --- |
| P1-01 | 已关闭 | `monitor`、`volume-maker`、`price-alert`、`scanner` 及无 `--once` 的 arbitrage 均明确非零退出；scanner 会先检查显式配置路径。 |
| P1-02 | 已关闭 | 顶层 `enabled`、`monitor_only`、exchange/symbol 白名单及 `symbol_configs.*.enabled` 在配置解析、`ArbitrageConfig -> ArbitrageStrategy` 的受检转换、策略求值和提交前强制校验；会静默丢弃控制字段的公共 `From` 已删除。缺失 `enabled` 默认关闭，禁用、缺失或越界策略不会创建历史。纯数值 `SegmentedArbitrageConfig` 构造器只用于无 operator scope 的确定性策略内核。 |
| P1-03 | 已关闭当前运行面 | `config-check` 区分 `runtime-executable`、`legacy-parseable`、`auxiliary`、`unsupported`，在明确的摘要数与输出预算内聚合输入并做有界、确定性目录递归；预算耗尽时追加终止错误、标明尚有路径未检查并失败关闭。辅助文件先按内容 schema 验证，保留文件名不能把交易配置伪装成 auxiliary。paper one-shot 只接受实际消费字段，显式要求执行开关、`segmented` 模式、非空 allowlist、仓位限额与行情新鲜度。未消费的 symbol profile 字段被拒绝，flat/default-config 六组数值别名只有语义等值时才可并存，冲突时失败关闭。CLI 与 config crate 的全部公开路径 loader 均以共享的 1 MiB 上限读取；`emergency_stop` 在配置、计划和策略边界均会阻止运行。 |
| P1-04 | 已关闭 | Grid、VolumeMaker、Arbitrage direction/state 和 Risk 均使用 exchange + symbol + market type 完整产品身份；跨 market type 快照会被拒绝。Runtime 还会在提交前闭环校验注册路由名、intent exchange 与 adapter 自报 exchange，身份不一致时保持零提交。 |
| P1-05 | **部分缓解** | Risk 算法已修复保守估值、reduce-only、全持仓聚合、checked arithmetic 和批量累计授权；arbitrage one-shot 会读取正数 `max_position_value` 并在历史/提交前调用 `authorize_batch`。仍缺真实账户仓位、跨运行 pending reservation、kill-switch 状态和失败回滚事务，因此 live 仍关闭。 |
| P1-06 | 已关闭 | Paper 候选状态完成全部 checked 计算后才原子提交；即时/延迟溢出均回滚整个状态，不再 panic 或留下 Filled/position 不一致。 |
| P1-07 | 已关闭当前 actor 契约 | Bounded handle 将 operation timeout 与 caller dispatch deadline 分离，两者都覆盖排队；派发截止已过的请求在 poll adapter 前确定性拒绝，已开始的 I/O 只受独立 operation timeout 约束。CancelOne、`CancelAll` 与权威 reconcile 各有独立有界保留通道；`CancelAll` admission barrier 阻止已接纳但尚未派发的同 scope Submit。ambiguous 或已完成但无法送达回执的操作会记录 operation key、所需 scope、adapter-relative watermark 与本地 reconcile generation 并隔离 Submit；歧义 Submit 只能由 scope/request 一致、覆盖 `All`、watermark 不回退且来自歧义后新 generation 的权威回执解封。该契约不比较本机 UTC 与适配器时间。 |
| P1-08 | 已关闭当前运行契约 | ExecutionPolicy 按完整产品逐 intent 检查缺失、未来和陈旧快照，并用可注入的单调逻辑时钟在每条腿提交前重验；剩余行情寿命会转换为绝对 monotonic dispatch deadline，通过 `ExchangeHandle::execute_before` 传到有队列的 wrapper，在实际 poll adapter 前再次检查。PaperExchange 还会在锁内重验，锁等待不能绕过 freshness 或令时间回退。 |
| P1-09 | **部分缓解** | 非空 UUID 批次与每腿 client ID 会在提交前持久化；恢复使用 `deny_unknown_fields` 的专用 wire，所有订单安全字段必须显式存在，并与构造器共享 exchange、数量、market/limit price、TIF、非空/唯一 ID 不变量。completed/incomplete/partial 的有界回执与对账对象进入 JSONL，执行结果与 outcome journal 同时失败时使用复合错误保留原结果。仍没有 live 多腿补偿、恢复锁、逐腿 durable saga 状态机，以及从 exchange future 返回到上层 durable 消费之间的显式 ACK；因此不能开放真实资金路径。 |
| P2-01 | 已关闭 | Grid 的 `--price` 与 `--once` 成为原子 CLI 契约，任一单独出现都会失败。 |
| P2-02 | 已关闭 | VirtualGrid 更新改为一次提交并排除查询时刻之后的事件；套利完全归零会清除方向锁。 |
| P2-03 | 已关闭 | `PartialExecution` 已包含稳定批次身份、失败索引、失败腿和全部未尝试 intents。 |
| P2-04 | 已关闭声明范围 | JSONL 使用流式 byte-budget 序列化，限制单条 1 MiB、单批 8 MiB、单文件 64 MiB；文件总量在同一路径锁内按实际句柄 metadata 检查，超限不写入。`append_batch` 在进程内按 canonical/词法等价路径串行化并执行 flush + `sync_data`；失效锁条目会回收，文档不再声称跨进程事务性。 |
| P2-05 | 已关闭当前 paper 语义 | Paper 使用注入时钟、严格递增快照时间、有限顶层深度、深度消耗、GTC/IOC/FOK 部分成交语义；同时间戳重放不能补回已消耗深度，缺失深度按零而非无限处理。Arbitrage CLI 强制四侧深度并在写计划前聚合校验，只有全部腿 `Filled` 才记录 completed。 |
| P2-06 | 已关闭有界性 | Grid/Arbitrage/VirtualGrid/Alert、HTTP body、actor 队列、配置发现与所有公开配置文件 loader、恢复批次、快照与账本均增加业务级上限和 checked/try 构造；执行批次上限收紧为 256。`config-check` 还限制保留摘要数、路径/消息/详情/schema issue 大小与 JSON/text 总输出，并预留可交付的终止错误；配置 crate 的全部公开路径与字符串入口共享 YAML anchor/alias guard。guard 按 LF、CRLF、裸 CR、NEL、LS、PS 逻辑分行，跨物理行保存 plain/quoted/tag/flow、待绑定结构父级与 tag property 起点，以结构父缩进识别复杂显式键和 tagged key 内的 block scalar；config crate loader 入口规范化一个 UTF-8 BOM，raw public guard 仍按解析器列语义检查，根级等缩进 plain 会在文档边界终止。flow 嵌套限制为 128 层，恰好 1 MiB 的单行输入只做单遍前向扫描，避免别名扩张、换行/缩进差异或 CPU 退化绕过文件上限。Paper 的必拒条件在复制候选账本前完成，snapshot 预建持仓索引后为有界 O(orders + positions)；每次 submit 的原子候选账本克隆仍会令连续大批提交呈二次复制成本，列入剩余性能边界。 |
| P2-07 | 已关闭 | 当前文档明确 Rust 只读取进程环境变量，不会隐式加载 `.env`；示例变量名与 loader 对齐，敏感 Debug 保持脱敏。 |
| P2-08 | **部分缓解** | toolchain 固定到 1.85.0；CI 覆盖 Windows/Ubuntu × MSRV/stable，所有支持该参数的依赖解析 Cargo 命令使用 `--locked`，Actions 固定 SHA，并加入 RustSec。尚无 license/secret-history gate，`serde_yaml 0.9` 仍为弃用依赖。 |
| P2-09 | 已关闭 | 活跃文档和配置注释改为 Rust 命令/当前运行边界，不再把冻结 Python 入口描述为可运行入口。 |

## 4. Claude 新增发现的处理

### 4.1 Instrument filters

已增加精确 Decimal 的 tick size、lot size、最小数量和最小名义价值规则；catalog key 包含 exchange、symbol、market type，支持显式 strict/permissive 模式并有资源上限。Paper strict 模式会在账本变更前拒绝 off-tick、off-step 和 minimum 违规。

当前公开 CLI 构造的是空 catalog 的 permissive `PaperExchange`，因此这些过滤器是可复用基础设施和未来 live 门禁，不是当前 CLI 已加载真实交易所 metadata 的证据。真实交易所规则仍需要由未来私有 adapter 从权威元数据加载并验证；因此这项修复不能作为 live 解锁证据。

### 4.2 稳定 client order ID 与恢复

`ExecutionBatch` 会生成并校验非空批次 ID、拒绝批内重复 client ID，CLI 在跨提交边界前同步记录可反序列化的 batch/legs。重试不重新生成身份，而应从 `execution_planned.recovery_batch` 恢复原批次；失败上下文可确定哪些腿已完成、失败或尚未尝试。

进程重启后自动补偿和恢复锁仍未实现，归入 P1-09 剩余门槛。

### 4.3 YAML guard 最终交叉审查

初版修复在最终独立审查中又暴露出四类边界：跨行 plain scalar 中的引号会污染 quote 状态；复杂显式键、tag-only/tagged key 与 UTF-8 BOM 会令 block scalar 父缩进估算偏移；`key:`/sequence/tag 后换行和根级等缩进 plain 的结构父级会丢失；逐字符重数前缀会令 1 MiB 单行退化为二次时间。最终实现按 [YAML 1.2.2](https://yaml.org/spec/1.2.2/) 的 plain、node property、flow 与 document productions 分离持久状态，删除文本缩进启发式并改为单遍列计数。独立合法/拒绝差分矩阵结果为 `TOTAL_BAD=0`；32 个定向单测与 config crate 60 个测试共同锁定这些回归。

## 5. 本轮关键实现变化

### 执行与控制面

- 无实现的命令全部失败关闭，不再成功 no-op。
- Arbitrage 的受检配置转换会保留 operator scope，并在决策和提交两个边界重检 `enabled`、`monitor_only` 与 exchange/symbol allowlists；纯数值策略构造器保持无 operator scope，只用于确定性内核测试或显式调用。
- ExecutionPolicy 按完整 instrument 做新鲜度和未来时钟校验，并用调用方逻辑时间锚定的单调时钟在每条腿提交前重验；剩余 freshness 会作为绝对 monotonic dispatch deadline 传入 exchange seam，覆盖 bounded actor 排队时间。
- 一次性套利要求显式四侧 bid/ask price + quantity，未知深度不再等价于无限流动性。
- 可执行侧深度按 instrument + side 聚合；不足时在创建历史前拒绝。
- 套利只有收齐与预期腿数相同且全部为 `Filled` 的 receipts 才写 `execution_completed`。
- Bounded actor 分离 dispatch 与 operation 两类绝对 deadline；Submit、CancelOne、`CancelAll` 和权威 reconcile 使用独立有界通道。ambiguous outcome 或无法送达的已完成结果会隔离 Submit，只有覆盖 `All`、scope/request 匹配、adapter watermark 不回退且属于歧义后新 generation 的权威回执才能解除。
- Runtime 会同时校验 router 注册键、intent exchange 与 adapter 自报 exchange；批内混合 exchange 或任一身份不一致都在零提交状态失败。

### 风险、数值与账本

- `Price` 在构造和反序列化边界拒绝零值与负值。
- Risk 使用可执行侧保守价格、聚合全部匹配仓位，合法 reduce-only 可在超限状态下降险。
- `authorize_batch` 会累计同一 instrument 的批内暴露，防止逐腿各自通过后合计超限。
- Paper 账本先在候选状态完成全部 checked arithmetic，再一次性提交订单、仓位和深度。
- Paper snapshot 预建持仓索引，正常发布路径为 O(orders + positions)；执行批次上限收紧为 256，约束候选账本逐次复制的最坏规模。
- 所有主要可控分配和历史写入均有明确资源上限。

### 恢复、审计与供应链

- 执行计划先于提交落盘，completed/partial/incomplete 使用同一 batch ID；恢复使用拒绝未知字段且不允许安全字段缺失的专用 wire，并与正常构造共享订单不变量、批量上限、非空 ID 和 client ID 唯一性校验。exchange future 返回后的上层 durable 消费 ACK 仍属于 live saga 门槛。
- Partial execution 自动触发 adapter reconcile；JSONL 有界保存回执、订单、仓位、scope、观测时间和截断元数据，日志失败的复合错误仍保留原执行结果。
- JSONL 用 byte-budget writer 流式序列化；限制单条 1 MiB、单批 8 MiB、单文件 64 MiB。批量追加在同进程同路径内串行化，超限时不写入，并调用 `sync_data`。
- CI 增加双 OS、双工具链、锁文件、固定 action SHA 和 RustSec gate。

## 6. 验证证据

| 验证 | 当前结果 |
| --- | --- |
| `rustc +1.85.0 --version` / `rustc +stable --version` | PASS；`1.85.0` / `1.97.0` |
| MSRV `check --workspace --all-targets --all-features --locked` | PASS |
| MSRV `test --workspace --all-targets --all-features --locked` | PASS；269 passed / 0 failed |
| MSRV `clippy --workspace --all-targets --all-features --locked -- -D warnings` | PASS；0 warning |
| Stable `check` / `test`（同一全工作区参数） | PASS；269 passed / 0 failed |
| Stable `fmt --all -- --check` / 严格 Clippy | PASS；0 diff / 0 warning |
| MSRV `build --release --workspace --all-features --locked` | PASS |
| MSRV `test --doc --workspace --all-features --locked` | PASS；6 crates / 0 doctest failure |
| 本地 `cargo audit --file Cargo.lock` | PASS；`cargo-audit 0.22.2` 扫描 217 个锁定依赖、加载 1160 条 advisory，退出码 0、未报告漏洞 |
| CI workflow YAML | PASS；解析出 `verify`、`quality`、`audit` 三个 job；4 个固定 SHA 的 action manifest 均可从对应上游以 HTTP 200 读取且所用 inputs 存在；远端 Ubuntu/Windows job 尚未在本地冒充为已运行 |
| `cargo metadata --manifest-path rust/Cargo.toml --no-deps --locked` | PASS；6 packages，workspace/target 均位于 `rust/`；活跃代码与 CI 对 `archive/python-legacy/` 的运行时引用为 0 |
| 全目录 `config-check config --json` | EXPECTED NONZERO；58 个文件全部报告：45 `legacy-parseable`、9 `auxiliary`、2 `runtime-executable`、2 `unsupported`；56 个状态为 `ok`，两个空 symbol monitor 被诚实标记为 `error`，退出码 1 符合预期，不等价于“全配置通过” |
| 当前工作树常见凭证模式扫描 | 高置信凭证模式 0 命中；23 个宽泛赋值命中均为留空示例、环境变量说明、测试占位或脱敏容器代码；这是本次启发式证据，不替代尚未建立的 secret-history 持续门禁 |
| 未实现命令 / live | PASS；monitor 非零失败关闭；live 在正确确认短语下仍因 adapter 未验收而非零失败关闭 |
| Grid one-shot | PASS；严格配置在参考价 `110` 下产生 2 条 history records、2 receipts、2 Open、0 Filled、0 Cancelled；输出明确为 placement simulation |
| Arbitrage 深度不足 | PASS；提交前退出 1，history 不存在 |
| Arbitrage 风险超限 | PASS；`projected=40000.00 > limit=5000`，提交前退出 1，history 不存在 |
| Arbitrage 正常路径 | PASS；2 records、2 legs、2 Filled、0 Open、0 Cancelled |
| Paper 缺失深度 / Decimal::MAX 即时与延迟路径 | PASS；缺失深度不成交；溢出返回显式错误且候选状态整体回滚 |
| 最终交叉审查回归 | PASS；bounded 契约 20/20：adapter-relative watermark/generation、无关/回退/mismatched reconcile、caller abort、排队 freshness 与慢响应双截止；strict config 覆盖未消费 symbol 字段、六组 alias 冲突、7 个公开路径 loader 与全部公开字符串入口、YAML 跨行/root plain、tag-only/tagged key、复杂显式键 block scalar、BOM/文档边界、六类逻辑换行、128 层 flow 与 1 MiB 单行前向扫描、辅助文件名伪装、alias 扩张和 config-check 摘要/输出预算；独立 YAML 差分矩阵 `TOTAL_BAD=0` |
| disabled / monitor-only / disabled symbol | PASS；零提交、零 history |
| `git diff --check` / `archive/python-legacy/` | PASS / 未修改 |

以上命令均在 Windows 当前工作树实际执行；CI 中声明的 Ubuntu 组合只做了 workflow 语法与矩阵审查，未将尚未发生的远端运行写成通过。任何 paper 通过均不改变第 7 节的剩余门槛。

## 7. 仍然存在的风险与硬门槛

以下不是遗漏，而是有意保留的 `NO-GO` 边界：

1. **真实账户风控**：arbitrage one-shot 使用空 paper 仓位；没有权威余额、仓位、挂单 reservation、kill switch 和跨进程原子风险事务。Grid one-shot 仍只验证规划/挂单语义，未接入集中风险授权。
2. **多腿恢复与消费确认**：已有 durable plan、partial context、actor quarantine 和 reconcile，但没有补偿下单、恢复锁、逐腿 saga 状态机、崩溃后自动续作，也没有从 exchange future 返回到上层 durable journal 之间的显式消费 ACK。actor 能识别发送前已丢失的 receiver，但不能证明上层已持久化一个刚发送成功的结果。
3. **私有 adapter**：尚无签名向量、testnet 订单/撤单/对账证据；公开 Binance adapter 明确拒绝私有操作。
4. **真实 instrument metadata**：规则引擎已存在，但当前 CLI 仍使用空 catalog 的 permissive paper 模式，live catalog 尚未从交易所权威元数据加载。
5. **Paper 模型与复制成本边界**：只模拟显式顶层盘口，不模拟多档滑点、延迟、真实撮合队列优先级、手续费、资金费率或跨运行仓位。snapshot 已收敛为 O(orders + positions)，但每次 submit 仍通过候选 `state.clone()` 保证失败原子性，连续大批提交的复制成本最坏可呈二次增长；当前以 256 条批次上限约束规模，不等价于生产撮合性能证明。
6. **持久化身份与跨进程边界**：JSONL 只保证进程内 canonical/词法路径别名串行化与 `sync_data`；单文件达到 64 MiB 后会失败关闭，尚无轮转。硬链接、文件被重定向后的底层 file-id 等价、跨进程事务隔离以及新文件 ACL/umask 强化不在当前契约内。
7. **供应链残余**：尚未建立 license policy、secret-history scan，也未迁移弃用的 `serde_yaml`。

## 8. 交付裁决

- **Paper-mode 策略/执行内核开发：CONDITIONAL GO**。Arbitrage 仅限显式价格/深度的空仓单批验证；Grid 仅限规划与 paper 挂单语义；两者均不得当作账户级风险验收。
- **连续运行：NO-GO**。缺真实状态、reservation 和恢复协调。
- **Live / 真实资金：NO-GO**。运行时和 adapter 均继续失败关闭。

任何后续修改都不得仅凭“测试全绿”或“paper 两腿成交”删除 `LiveExecutionUnavailable`。解除 live 门槛必须同时提供权威账户风险事务、私有 adapter testnet 证据、多腿恢复/补偿、真实 instrument metadata 和故障注入验收。
