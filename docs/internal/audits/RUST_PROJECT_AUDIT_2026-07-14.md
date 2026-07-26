# Rust 项目问题排查报告（2026-07-14）

> **状态说明：修复前基线快照。** 本文记录固定 HEAD `1a6bf0fb98f682df45d8761a4dffa3e97717571d` 的问题与当时验证结果，不代表当前工作树。逐项修复状态与最终复验见 [Rust 项目对抗审查复核与修复报告](RUST_PROJECT_AUDIT_REMEDIATION_2026-07-14.md)。

## 1. 结论

**总体结论：当前版本适合作为 paper-mode/策略内核的开发基线，但不具备生产或实盘就绪条件，实盘结论为 `NO-GO`。**

积极的一面是：workspace 能在最低声明版本 Rust 1.85 上编译和运行全部测试，74 个现有测试全部通过，格式、严格 Clippy、release 构建和两条 paper 端到端 smoke 均通过；live 路径目前也确实失败关闭，没有发现当前 Rust 配置提交了真实凭证。

阻断生产化的原因不是常规编译质量，而是运行语义没有贯穿配置、策略、风险与执行层：多个公开命令实际是成功退出的 no-op，套利禁用/监控开关可被绕过，`config-check` 会误报，产品类型可串线，风险引擎既未接入执行链又存在估值绕过，极值 Decimal 可造成 panic 和 paper 账本不一致，队列超时、陈旧行情、多腿恢复及审计日志也未达到交易系统要求。

本次共整理：

- 9 项高优先级问题（P1）；
- 9 项中优先级问题（P2）；
- 0 项已确认的 P0/真实凭证泄露；
- live 当前关闭，因此若干 P1 是“接通 live 前的硬阻断”，而不是已经发生的实盘事故。

## 2. 审计范围与基线

| 项目 | 内容 |
| --- | --- |
| 审计范围 | `rust/` 下 6 个 workspace crate、当前配置、CLI、CI、测试和文档 |
| 排除范围 | `archive/python-legacy/` 仅作兼容背景，不作为当前运行项目审计 |
| 分支 | `main`，相对 `origin/main` ahead 2 |
| 提交 | `1a6bf0fb98f682df45d8761a4dffa3e97717571d` |
| 平台 | Windows / PowerShell |
| 工具链 | Rust 1.85.0（MSRV 验证）与 stable 1.97.0（fmt/Clippy） |
| 修改范围 | 除本报告外未修改业务源码；所有临时探针、测试夹具和运行历史均已清理 |

项目目标来自 `RUST_REFACTOR_PLAN.md:5-10,24-35,39-50`：保留 YAML 和主要 CLI 的可观察行为，提供安全默认的 paper 执行、集中风险检查和稳定 JSONL 历史。以下结论均按这些公开契约评估。

## 3. 验证结果

| 检查 | 结果 |
| --- | --- |
| `cargo +1.85.0 check --workspace --all-targets --all-features --locked` | PASS |
| `cargo +1.85.0 test --workspace --all-targets --all-features --locked` | PASS，74 passed / 0 failed |
| `cargo fmt --all -- --check` | PASS（stable 1.97.0） |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | PASS，0 warnings（stable 1.97.0） |
| `cargo +1.85.0 build --release --workspace --all-features` | PASS |
| `cargo +1.85.0 test --workspace --doc` | PASS，6 个 crate 均无 doctest |
| README 的 grid paper smoke | PASS，100 receipts（10 filled / 90 open） |
| README 的 arbitrage paper smoke | PASS，2 legs filled |
| 当前配置凭证字段启发式扫描 | 未发现非空、非占位疑似密钥 |
| `cargo audit` / `cargo deny` / secret scanner | 未安装，CI 也未配置；不能声明依赖安全已通过 |

“74 个测试全绿”与本报告并不矛盾：现有测试主要覆盖正常路径和类型契约，缺少极值、乱序时间、禁用开关、错误产品、队列饱和、恢复和全量配置矩阵。

## 4. 高优先级问题（P1）

### P1-01：公开 CLI 中 4 个命令是静默 no-op，多项参数被接受后忽略

**状态：动态确认。**

CLI 声称会运行 monitor、volume-maker、price-alert 和 scanner：`crates/apps/src/cli.rs:14-32`。实际实现只加载/打印配置后返回成功：

- monitor：`crates/apps/src/command.rs:148-158`；
- volume-maker：`crates/apps/src/command.rs:160-174`；
- price-alert：`crates/apps/src/command.rs:176-190`；
- scanner：`crates/apps/src/command.rs:192-197`，甚至不读取 `--config`；
- arbitrage 未带 `--once` 时也只验证后退出：`command.rs:103-145`。

动态结果：上述命令均在约 34–39 ms 内 `EXIT=0`。monitor 传入有效与明显无效的 `--symbols` 后输出完全相同；scanner 传入不存在的 `--config` 仍成功。`debug`、`debug_detail`、`symbols`、`no_ui` 等参数定义于 `cli.rs:45-195`，但没有对应行为。

**影响：** 操作员可能认为监控、告警或策略已启动，实际进程已经成功退出；price-alert 的“成功但无告警”尤其危险。

**建议：** 功能完成前明确返回非零 `unsupported`；完成后增加真正的进程行为测试，验证参数改变过滤、日志、配置加载和生命周期，而不只是验证 Clap 可解析。

### P1-02：套利 `enabled`、`monitor_only` 和分交易对禁用没有到达执行边界

**状态：动态确认。**

- 配置模型包含顶层 `enabled`、`monitor_only`、`exchanges`、`symbols`：`crates/config/src/arbitrage.rs:9-20`；
- 策略适配只保留阈值、格数和数量：`crates/strategy/src/arbitrage.rs:115-132`；
- CLI 的 one-shot 执行没有检查 `enabled` 或 `monitor_only`：`crates/apps/src/command.rs:96-145`；
- `symbol_configs[*].enabled` 没有进入 Rust 模型。现有 PAXG 配置明确禁用：`config/arbitrage/arbitrage_segmented.yaml:114-127`。

复现（使用当前配置、写入 Windows `NUL`）：

```powershell
.\target\debug\crypto-trading.exe arbitrage `
  --config config/arbitrage/arbitrage_segmented.yaml --once `
  --left-exchange paper-left --left-symbol PAXG-USD-PERP `
  --left-bid 99.9 --left-ask 100 `
  --right-exchange paper-right --right-symbol PAXG-USD-PERP `
  --right-bid 101 --right-ask 101.1 --history-path NUL
```

实际结果：

```text
paper executed: decision=Open segment=5 receipts=2; history=NUL
EXIT=0
```

另一个临时配置同时设置 `enabled: false`、`system_mode.monitor_only: true`，仍得到 `Open` 和 2 张 filled receipt。

**影响：** 当前是 paper 语义旁路；未来若直接接通 live adapter，会跨越操作员的显式安全边界。

**建议：** 完整建模 symbol 配置；在构造策略和提交 intent 两个边界都强制顶层开关、monitor-only 和 symbol 开关；为每条禁用路径增加“零 intent、零 receipt”回归测试。

### P1-03：`config-check` 同时存在误报、漏报和安全字段静默丢弃

**状态：动态确认。**

`config-check` 使用字段启发式识别 schema：`crates/apps/src/command.rs:395-475`，但多数 loader 只做局部解析。

确认的误报：

- 仅含 `system_mode.monitor_only`、没有交易所/交易对/数量的 arbitrage 文件被判 `valid`；真正 one-shot 随后报 `base quantity must be positive`；
- `order_mode: definitely-not-a-mode`、`interval_seconds: -1` 的 volume-maker 文件被判 `valid`，对应命令也 `EXIT=0`；
- `exchanges: []`、`symbols: []`、`analysis_interval_ms: 0` 的 monitor 文件被判 `valid`。

确认的漏报：逐个检查 `config/` 下 55 个 YAML/YML/JSON，结果为 **47 pass / 8 fail**：

```text
config/logging.yaml
config/arbitrage/extra_symbols.yaml
config/arbitrage/multi_exchange_arbitrage.yaml
config/arbitrage/multi_leg_pairs.yaml
config/arbitrage/segment_symbol_filters.yaml
config/exchanges/edgex_lighter_markets.json
config/exchanges/edgex_markets.json
config/exchanges/lighter_markets.json
```

其中 `multi_exchange_arbitrage.yaml` 使用文档允许的 YAML `null`，`decimal_at_any` 却把它当成非法 Decimal：`crates/config/src/arbitrage.rs:170-193`。

更严重的是安全/运营字段会被静默忽略：Serde 默认接受未知字段，测试甚至固化了该行为：`crates/config/tests/config_compatibility.rs:19-31,34-48`。当前 grid 配置中的 scalping、capital protection、take profit、price lock、stop loss、exit cleanup、order health 等字段没有完整运行模型；volume-maker 虽解析 `emergency_stop`，策略适配却丢弃它。默认 Backpack 配置当前为 `emergency_stop: true`：`config/volume_maker/backpack_btc_volume_maker.yaml:267-271`，适配代码见 `crates/strategy/src/volume_maker.rs:24-45`。

**影响：** `valid` 只表示部分字段可读，不代表配置能运行，更不代表风险控制生效；错拼安全字段也不会失败。

**建议：** 分离“legacy 可解析”与“runtime 可执行”两种检查；runtime schema 对未知安全字段 fail closed，至少输出所有 ignored keys；`config-check` 必须执行 config → strategy → runtime preflight，并为全部 55 个当前文件维护显式清单与分类。

### P1-04：Grid、VolumeMaker 和持仓套利没有隔离 market type

**状态：动态确认。**

- Grid 快照只校验 exchange/symbol，不校验 `market_type`：`crates/strategy/src/grid.rs:192-206`；
- VolumeMaker 同样遗漏：`crates/strategy/src/volume_maker.rs:97-107`；
- 两者随后使用配置的 market type 生成订单：`grid.rs:180-187`、`volume_maker.rs:132-159`；
- `ArbitrageDirection` 从 `SpreadQuote` 丢弃两腿 market type：`crates/strategy/src/arbitrage.rs:23-31,135-146`；方向和快照匹配也只比较 exchange/symbol：`arbitrage.rs:283-300,356-370`。

临时探针确认：Spot snapshot 可让 Perpetual Grid/VolumeMaker 生成 Perpetual intent；先用 Perpetual 开套利仓，再喂相同 exchange/symbol 的 Spot snapshots，会生成 `market_type=Spot` 的 reduce-only 平仓 intent。

**影响：** 现货行情/深度可驱动永续订单；原 Perpetual 暴露可能没有关闭，却尝试在 Spot 平仓。

**建议：** 所有快照校验同时比较 market type；套利方向保存两腿完整产品标识；方向、对账和关闭必须匹配 exchange + standard symbol + market type。补充持仓期间切换产品类型的回归测试。

### P1-05：RiskEngine 未接入执行链，且自身估值可绕过或阻止减仓

**状态：模块内动态确认；当前 live 仍关闭。**

静态搜索显示 `RiskEngine::authorize` 只出现在实现和测试，`runtime/apps` 没有调用。虽然设计声称集中 pre-trade 检查（`RUST_REFACTOR_PLAN.md:47-49,58-61`），当前 paper grid/arbitrage 并不经过 kill switch 或最大持仓检查。

算法本身还有三类缺陷：

1. `Price` 允许零，风险引擎对 limit order 直接用用户限价估算整个 projected position：`crates/domain/src/value.rs:19-23`、`crates/strategy/src/risk.rs:129-139`。市场 `bid=99/ask=101`、上限 500 时，`Sell Limit qty=100 price=0` 被 `Authorized`，但 PaperExchange 会按 bid 99 成交，实际名义价值约 9,900。
2. 已超限 Long 10、reduce-only Sell 1、市场约 99、上限 500 时，被拒绝为 `projected=891`；风险降低订单反而无法退出：`risk.rs:121-139`。
3. 授权不包含未成交挂单/reservation；两个各自 60 的订单都可在上限 100 下独立通过，合计成交后超限。

此外，`AccountRiskSnapshot` 的 equity/available balance 当前没有参与授权，且只取第一条匹配持仓：`risk.rs:14-18,96-112`。这两点需要先明确产品契约。

**建议：** 先定义统一的有效订单不变量（价格严格大于零）；按订单类型、方向和盘口使用保守成交价；对确认的 reduce-only 降风险路径单独处理；将 pending exposure 作为原子 reservation；最后把授权、提交、失败回滚和成交更新接入同一一致性边界。

### P1-06：未检查 Decimal 运算可 panic；PaperExchange 会先记 Filled 再留下旧仓位

**状态：动态确认。**

代表性未检查运算包括：

- risk：`crates/strategy/src/risk.rs:117,134`；
- grid：`crates/strategy/src/grid.rs:86,149-159,214-238`；
- arbitrage：`crates/strategy/src/arbitrage.rs:88-89,227-240,408-409`；
- virtual grid：`crates/strategy/src/virtual_grid.rs:74-108,290-297,356-380`；
- paper ledger：`crates/exchange/src/paper.rs:534,590-593,627-630`。

PaperExchange 更存在提交顺序问题：先把 Filled order 写入 `state.orders`，再调用 `apply_fill`：`paper.rs:217-233`；延迟成交也先改订单状态再更新仓位：`paper.rs:118-131`。

临时集成探针用两张 `Quantity(Decimal::MAX)` 市价买单复现：

```text
Addition overflowed
PANIC_REPRO join_is_panic=true orders=2 statuses=[Filled, Filled]
position_quantity=79228162514264337593543950335
```

第二张订单已被记录为 Filled，但仓位仍只有第一张的数量；用相同 client order ID 重试会得到 `AlreadyProcessed`，账本无法自愈。

**建议：** 所有金融运算改为 `checked_add/sub/mul/div` 并映射成显式错误；配置/行情/订单增加业务上限；订单与仓位先在局部临时状态完成全部计算，再原子提交。

### P1-07：BoundedExchangeHandle 的 timeout 不含排队时间，撤单没有紧急通道

**状态：timeout 动态确认；撤单饱和问题为结构性风险。**

请求先进入单一 FIFO：`crates/exchange/src/bounded.rs:24-44,80-83`；只有出队后才开始 `timeout`：`bounded.rs:90-138`。临时探针设置 100 ms timeout、两个串行慢请求，第二个实际耗时 213 ms：

```text
QUEUE_REPRO configured_timeout_ms=100 second_elapsed_ms=213
```

默认 30 秒并不是端到端截止时间，队列越长，真实等待可接近 N × timeout。execute/reconcile/subscribe/status/cancel 共用队列与 semaphore；饱和时紧急撤单会 Backpressure 或排在慢请求后。Cancel 的 ambiguous error 也不携带 order ID：`crates/exchange/src/model.rs:23-37`。

**建议：** admission 时记录绝对 deadline，出队只使用剩余时间，调用方等待 oneshot 也受同一 deadline；cancel/kill-switch 使用独立高优先级通道和保留容量；ambiguous outcome 携带结构化 operation key。

### P1-08：readiness 不验证目标产品行情新鲜度，陈旧快照仍可成交

**状态：代码路径确认。**

PaperExchange 无条件报告 Ready：`crates/exchange/src/paper.rs:369-386`；runtime preflight 只检查 adapter mode/availability：`crates/runtime/src/execution.rs:146-162`；submit 直接使用最后一张 snapshot，不检查年龄：`paper.rs:203-211,426-440`。

**影响：** 数小时甚至数年前的快照仍可让市价单成交；paper 结果失真。若这一 readiness 契约复用于 live，会形成错误下单边界。

**建议：** runtime 配置 `max_market_age` 并注入时钟；按每个 intent 的 exchange/symbol/market type 做 freshness preflight；缺行情、陈旧行情、未来行情都在任何订单提交前 fail closed。

### P1-09：多腿套利顺序提交，部分结果后没有补偿或自动 reconciliation

**状态：结构性高风险；live 当前关闭。**

`ExchangeRouter` 只做全部 adapter preflight，随后按顺序逐腿 submit，第一处错误立即返回：`crates/runtime/src/execution.rs:93-134`。错误只要求调用方自行 reconcile：`execution.rs:180-188`，没有自动撤单、对冲或恢复状态机。`ArbitrageState` 也只有一个总数量，无法表示单腿/部分成交：`crates/strategy/src/arbitrage.rs:143-146`。

**影响：** 第一腿成功、第二腿拒绝/超时/ambiguous 时会留下裸方向敞口。

**建议：** live 开放前实现明确 saga：稳定 client order ID、逐腿 durable journal、ambiguous 强制 reconcile、补偿撤单/反向对冲、未恢复前阻止同策略继续下单；状态模型保存每腿订单和成交数量。

## 5. 中优先级问题（P2）

### P2-01：`grid --price` 未带 `--once` 仍产生执行副作用

`--once` 的帮助说明才表示处理一个 snapshot：`crates/apps/src/cli.rs:51-56`；实现却只校验 “once 必须有 price”，并对任意 `Some(price)` 执行：`crates/apps/src/command.rs:71-92`。

复现：

```powershell
.\target\debug\crypto-trading.exe grid `
  config/grid/lighter-long-perp-btc.yaml `
  --price 100000 --history-path NUL
```

结果为 `paper executed: 100 orders ...`、`EXIT=0`。

建议只在 `args.once` 分支执行；若产品决定 `--price` 隐式等于 `--once`，则统一 CLI help、README 和测试。

### P2-02：VirtualGrid 存在 look-ahead bias、失败后部分提交和方向锁残留

- APR 查询没有要求 `now >= last_update_at`，事件过滤只有 `event >= window_start`、没有 `event <= now`：`crates/strategy/src/virtual_grid.rs:247-306`。在 t=120 记录 cycle、再在 t=60 查询，得到约 52,349.76% APR。
- `update_price_at` 在所有派生价格校验完成前就修改 current price、timestamp、cross counter 和一侧 pending price：`virtual_grid.rs:195-235`。边界错误返回 Err 后状态已变化。
- Arbitrage 完全归零时，decision 仍总是携带 `Some(direction)`：`crates/strategy/src/arbitrage.rs:394-445`，可能让下一次机会继续锁在旧方向。

建议所有更新先在局部变量中计算并一次提交；APR 查询和事件使用 `[window_start, now]`；flat 状态清除旧套利方向并重新选择 best。

### P2-03：`PartialExecution` 丢失失败项之后尚未尝试的 intents

两个执行循环遇错立即 return：`crates/runtime/src/execution.rs:45-60,118-132`；错误只保存 `completed` 与 `failed_intent`：`execution.rs:180-188`。

批次 `[成功, 失败, 未尝试]` 中第三项无法从错误对象恢复，容易漏单或粗暴重放整个批次。建议增加 `unattempted`、失败索引、batch/run ID，并把恢复状态持久化。

### P2-04：JSONL history 不是其注释声称的 durable history，并发写也没有串行化

crate 声称 “durable decision history”：`crates/runtime/src/lib.rs:1`；实现只 `flush()`，没有 `sync_data/sync_all`：`crates/runtime/src/history.rs:34-61`。append 返回 Ok 后掉电仍可能丢失记录。

`JsonlHistory` 可 Clone，每次 append 独立 open，同进程/跨进程都没有锁；`write_all` 不保证只有一个底层 write，较大并发记录可能交错。当前测试只覆盖两个串行小记录：`crates/runtime/tests/runtime_contract.rs:15-50`。

建议先明确 durability 等级；交易恢复日志使用 single-writer task + bounded channel、sequence/run ID，并在需要的边界调用 `sync_data()`；若允许多进程，增加文件锁或改用事务存储。

### P2-05：PaperExchange 的时间和成交模型会产生误导性结果

- 没有 snapshot 时，`observed_at` 为 UNIX_EPOCH：`crates/exchange/src/paper.rs:35-44`；GTC limit 的 created/updated time 与撤单时间会成为 1970：`paper.rs:203-211,238-250,426-468`。
- resting limit 只要价格穿越就全量成交，不使用 bid/ask quantity，也没有 partial fill：`paper.rs:106-131,472-493`。
- 多张订单可在同一份有限深度 snapshot 上全部 full fill。

建议注入可测试时钟，把命令时间与行情时间分离；paper 成交模型至少支持可用深度、部分成交和确定的撮合顺序，并在报告中明确仿真假设。

### P2-06：大时间/数量配置可 panic、长时间阻塞或 OOM

- PriceAlert 将过大的 u64 饱和成 `i64::MAX` 后调用会 panic 的 `Duration::seconds`：`crates/strategy/src/alert.rs:190-215`。临时探针确认 `TimeDelta::seconds out of bounds`。
- Grid、Arbitrage、VirtualGrid 只验证数量可转成 u32，随后直接按该数量分配 Vec：`grid.rs:214-254`、`arbitrage.rs:227-240`、`virtual_grid.rs:74-100`。`max_segments=u32::MAX` 可触发巨大分配。

建议使用 `Duration::try_seconds`，所有可控数量设置业务级上限（如 `MAX_GRID_LEVELS`、`MAX_SEGMENTS`），再配合 `try_reserve`；不要以实际 OOM 作为验证手段。

### P2-07：`.env` 说明与实现不一致，环境变量命名也有漂移

Rust 只调用 `std::env::var`：`crates/config/src/auth.rs:9-15`，workspace 没有 dotenv/env-file 加载。但多份当前 exchange 配置声称会自动读取 `.env`，且引用不存在的 `rust/docs/ENV_CONFIG_TEMPLATE.md`。Paradex 文档要求的 `PARADEX_L2_PRIVATE_KEY` 与实现读取的通用 `PARADEX_API_KEY/PARADEX_PRIVATE_KEY` 也不一致：`auth.rs:157-175`。

**影响：** 把凭证放进 `.env` 并不会自动注入进程，操作员可能误以为已配置，甚至转而把值写入受 Git 跟踪 YAML。

建议短期明确要求在启动前注入进程环境并给 PowerShell 示例；若需要 env file，提供显式 `--env-file`，不要隐式搜索；统一每个 exchange 的变量名并增加来源优先级测试。

### P2-08：CI、MSRV 和供应链门禁不足

- workspace 声明 Rust 1.85：`Cargo.toml:13-16`，`rust-toolchain.toml` 却跟随移动的 stable；
- CI 仅用 1.85 做 check，fmt/Clippy/test 使用 stable，且命令未加 `--locked`：`../.github/workflows/rust.yml:31-42`；
- 仅覆盖 `ubuntu-latest`，未覆盖主要使用环境 Windows；
- GitHub Actions 使用可变 tag，未固定 commit SHA：`rust.yml:31-35`；
- 没有 RustSec/OSV、license policy 或 secret scan；
- 直接依赖 `serde_yaml = "0.9"`：`Cargo.toml:31`，锁定为 `0.9.34+deprecated`。其官方文档明确说明项目已不再维护：[serde_yaml 0.9.34+deprecated](https://docs.rs/serde_yaml/latest/serde_yaml/)。

当前没有执行漏洞数据库审计，因此不能把这一项写成“存在已知 CVE”，但也不能声明依赖安全通过。

建议：MSRV + stable 双矩阵，MSRV 至少跑 check/test；所有 Cargo 命令加 `--locked`；增加 Windows；Action 固定 SHA；加入 RustSec/OSV、deny/license 和 secret scan；评估迁移到受维护的 YAML 解析方案。

### P2-09：当前项目文档仍指向不存在的 Python 脚本/配置

根 README 声明 Rust 是唯一当前项目，但 `config/grid/README.md`、`config/grid/LIGHTER_使用说明.md` 和多份 arbitrage 文档仍给出不存在的 `run_grid_trading.py`、`main_unified.py`、`run_arbitrage_monitor_v2.py` 等入口；若干配置又引用不存在的 `docs/ENV_CONFIG_TEMPLATE.md`。

**影响：** 运维按当前目录文档无法启动项目，也无法判断哪些 legacy 功能已经迁移。

建议把旧说明移动到 archive 或明显标记为历史参考；当前文档只保留可执行 Rust 命令，并对仍为 no-op 的命令标出状态。

## 6. 已确认的正向安全属性

- live 当前确实双重 fail-closed：runtime 对 `ExecutionMode::Live` 返回 `LiveExecutionUnavailable`（`crates/runtime/src/execution.rs:138-143`），UnsupportedLiveExchange 和 Binance public adapter 也拒绝交易操作；
- Paper submit 对 `client_order_id` 幂等，重复撤单有确定结果；
- actor 丢失提交响应时返回 `AmbiguousOutcome`，没有伪造成功；
- 金融基础类型使用 Decimal，JSON 字符串序列化保持精度；
- MarketSnapshot 构造和反序列化会拒绝 crossed quotes；
- Secret 的 Debug 输出脱敏；当前 Rust 配置中的凭证字段为空/占位符；
- `unsafe_code = "forbid"`，当前严格 Clippy 0 warning。

这些正向属性应保留，但不足以抵消上述执行语义与恢复缺口。

## 7. 推荐修复顺序

### 阶段 0：保持 live 关闭

在以下阶段全部完成并有回归证据前，不要放开 `LiveExecutionUnavailable`：

1. 禁用/monitor-only/symbol-enabled 在策略与提交双边界 fail closed；
2. runtime 强制 RiskEngine，并具备 pending reservation；
3. 产品标识包含 market type；
4. Decimal 全路径 checked arithmetic；
5. 多腿 reconciliation/补偿与 durable journal；
6. 撤单高优先级通道和端到端 deadline。

### 阶段 1：先锁定行为契约

优先新增失败回归测试：

- disabled / monitor-only / disabled symbol 必须零 intent、零 receipt；
- no-op 命令必须明确 unsupported，不能成功伪装已运行；
- `config-check` 的 invalid/unknown/ignored-key 矩阵与 55 个当前配置清单；
- Spot snapshot 不能驱动 Perpetual intent；
- zero/low-price marketable limit 不能绕过风险；
- 超限持仓允许合法 reduce-only 退出；
- Decimal::MAX 返回错误而非 panic，失败后账本原子不变；
- 队列总耗时不超过 deadline，撤单可在饱和下抢占；
- stale/future snapshot fail closed；
- 多腿 partial/ambiguous 可重启恢复；
- VirtualGrid 时间单调、错误原子性和资源上限；
- JSONL 并发/崩溃语义。

### 阶段 2：修配置到执行的垂直切片

不要继续只增加可解析字段。每个运行字段必须有明确归宿：

```text
YAML schema -> validated runtime config -> strategy/risk -> exchange/runtime -> observable test
```

无法在当前 Rust 版本实现的 legacy 字段，应在 `config-check` 中明确报 unsupported，而不是静默忽略。

### 阶段 3：再扩充 adapter 与持续运行命令

依次完成 monitor / price-alert / scanner 的 read-only 运行闭环，再完成 paper volume-maker 和持续 arbitrage。每条命令必须具有 shutdown、freshness、backpressure、history 和错误退出契约。

## 8. 剩余审计限制

- 未接入任何真实 exchange 账户或 testnet；没有验证签名、限频、WebSocket 重连和真实 reconcile；
- live 路径当前不可达，这是安全正项，也意味着本报告不能证明任何实盘 adapter 能用；
- 未运行 RustSec/OSV 漏洞数据库、license policy 或 secret-history scan；只确认当前 working tree 未见非占位凭证；
- 未做长期性能、模糊测试、property test、故障注入、掉电和跨进程并发测试；
- 未修改业务代码，因此所有问题仍存在；本报告给出的修复顺序应作为后续实施计划，而不是已完成状态。

## 9. 本次生成文件

- `rust/RUST_PROJECT_AUDIT_2026-07-14.md`（本报告）

没有提交、暂存或修改其他源码/配置文件。
