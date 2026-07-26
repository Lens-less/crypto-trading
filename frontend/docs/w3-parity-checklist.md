# W3 Parity 清单:overview / executions / alerts

来源:`git show e5fdbc8:rust/crates/web/tests/ui_contract.rs`(757 行旧 UI 契约,锁定旧
`rust/crates/web/assets/app.js` 的中文文案与行为)+ 对应 app.js 渲染逻辑。
逐条提炼旧断言中属于三页(及三页共享的全局语义)的行为语义。

状态含义:

- **保留原样**:语义与关键文案照搬(或仅排版差异);
- **改进措辞**:语义保留,文案/呈现方式在 React 版中改进;
- **不适用**:旧实现细节在 React + Vite 架构下不复存在,附理由;
- **W4**:属于其余六页(scanner / integrations / strategies / risk / replay / settings)
  或提交面(submit),本轮不做。

## 1. 全局 / 三页共享语义

| # | 旧契约断言(语义) | 状态 | 说明 |
|---|---|---|---|
| 1 | 不暴露 `live-enable` / `order-submit` 等交易权限入口 | 保留原样 | 前端只读展示;浏览器永不构造 live 权限(overview 权限卡也只读后端声明) |
| 2 | 禁 `localStorage`/`sessionStorage`/`document.cookie` 存业务态(storage 白名单仅 ct-theme) | 保留原样 | bearer token 只存内存(`lib/api.ts`);游标只存 React state / query key(`lib/cursorPager.ts`) |
| 3 | 禁 `.innerHTML` / `insertAdjacentHTML` | 不适用 | React JSX 渲染路径本身不拼 HTML,契约由框架保证 |
| 4 | 禁 PUT/PATCH/DELETE 写方法 | 保留原样 | `lib/api.ts` 的 `request()` 只发 GET;POST 提交面属 W4 |
| 5 | 消费 `/api/v1/system` `/alerts` `/executions` `/events` `/capabilities` `/tasks` | 保留原样 | W3 三页覆盖以上端点;`/scanner` `/risk` `/settings` 消费属 W4 |
| 6 | 「不透明恢复游标只保存在页面内存」 | 保留原样 | 文案与语义都保留(executions「最近事件通知」卡副标题) |
| 7 | 呈现 `market_data_freshness` / `kill_switch` / `adapter_health`(not_available 一等呈现) | 保留原样 | system 卡「kill switch / 行情新鲜度 / 适配器健康:暂不可用(受监督前不声明健康)」 |
| 8 | `TOKEN_LABELS` 稳定 token → 中文映射 | 改进措辞 | 收敛为 `lib/labels.ts` 的 `humanizeToken`;关键条目(最后记录:运行中 / 最后记录:未决 等)原文保留 |
| 9 | 中文优先界面,不残留英文标题 | 保留原样 | 三页所有标题、状态、横幅均中文 |
| 10 | SSE 徽标只说「已连接 / 仅通知」,禁「实时」「新鲜 / 流式更新」 | 保留原样 | 既有 `NotificationChannelBadge`(已连接 · 仅通知 / 重连中 / 通知不可用);W3 未新增任何「实时」措辞 |
| 11 | 401 会话作废:清空受保护状态 + session generation 递增,旧代际回调丢弃 | 保留原样 | 既有 `lib/useOperationEvents.ts` 失效协议(invalidateSession / generation)已覆盖,三页全部走 React Query |
| 12 | 错误脱敏:稳定错误码分类文案(journal_unavailable / read_limit_exceeded / journal_invalid / authentication_required / 网络),不透传原始文本 | 保留原样 | `lib/errorPresentation.ts`;文案与旧 `errorDescription` 语义一致 |
| 13 | 空态说明缺什么、查过哪个事实来源(「已检查 /api/v1/…」) | 保留原样 | `components/EmptyState.tsx` 强制 `checkedFact` 必填 |
| 14 | 刷新失败但留有旧快照 → 「保留旧快照」横幅,不解释为最新状态 | 保留原样 | React Query 错误时保留缓存数据 + `DegradedBanner`(alerts / executions / overview 各卡) |
| 15 | 加载骨架与最终行几何一致、无闪光动画 | 保留原样 | `SkeletonRows`(表面色阶,无 shimmer) |
| 16 | shell 首帧「正在加载只读运行事实」 | 不适用 | Vite 入口的首帧加载由 React 骨架接管;各卡有自己的 loading 一等状态 |
| 17 | CSP / 安全响应头(nosniff、frame-ancestors 等) | 不适用 | 由后端 `api.rs`/静态服务负责,前端仓不产 HTTP 头;前端不引外域资源以兼容 `'self'` CSP |
| 18 | 汇总横幅「操作事件流已断开…不代表监控行情仍然新鲜」 | 改进措辞 | 断流语义由权限脊柱通知徽标三态(连接/重连/不可用)承载,连续失败 ≥3 才降级 |

## 2. /overview

| # | 旧契约断言(语义) | 状态 | 说明 |
|---|---|---|---|
| 19 | 系统 ribbon:投影 / 批次 / 恢复 / 警告 / 冲突 计数 + PAPER / LIVE 已关闭 | 改进措辞 | 收敛进 system 卡(批次 / 需恢复 / 冲突 / 警告 一行计数 + LIVE CLOSED pill);权限大字仍在权限脊柱第一区 |
| 20 | monitor:消费 `/api/v1/monitor`,标题「只读套利监控」 | 保留原样 | monitor 摘要卡 |
| 21 | monitor:「这是持久化监控事件的最后一次投影,不代表当前实时行情仍然新鲜」 | 保留原样 | 卡片脚注原文保留;副标题再加「持久化历史投影」 |
| 22 | monitor:显示 记录时间(recorded_at)与 市场代次(market_generation) | 保留原样 | `recorded_at(记录时间)` + `market generation` 事实行 |
| 23 | monitor:「读取方式:历史快照」 | 保留原样 | 状态 pill |
| 24 | monitor:`projection_status !== "complete"` → 「监控投影已降级」「最后一个有效结果停止展示」,保留事实标「已隐藏」 | 保留原样 | `lib/banners.ts` `monitorBanner` + MonitorCard 隐藏 latest,有测试 |
| 25 | monitor:waiting 显示等待腿 + 新鲜度 / 连续性;机会态显示方向 / 价差 / 阈值;拒绝态显示拒绝分类 | 保留原样 | 按 projection.type 分支渲染 |
| 26 | monitor 空态:缺失行情不提升为健康状态 | 保留原样 | EmptyState 原语义 |
| 27 | tasks:「只读连续任务」,running/stopping 只代表 journal 最后记录,不证明进程仍存活 | 保留原样 | tasks 摘要卡脚注原文;phase 文案「最后记录:运行中」 |
| 28 | tasks:「没有启动、停止、重连或自动恢复入口」 | 保留原样 | 卡片脚注;页面无任何任务操作入口 |
| 29 | tasks:投影降级 → 「任务投影已降级」最后有效事实横幅 | 保留原样 | `taskBanners`,有测试 |
| 30 | tasks:investigate → 「任务存活性未验证」「历史事实 / 不自动重放」横幅 | 保留原样 | `taskBanners`,有测试 |
| 31 | tasks:任务明细表(双源健康、事件计数、恢复判断列) | W4 | W3 总览只承载计数摘要(运行中/已停止/失败/总数/无效事件);明细表随策略/任务页深化 |
| 32 | 能力脉冲:capabilities 计数(可用/只读/单次模拟/仅校验/不可用)+ 发布阶段 / 实盘交易 / 能力项 | 保留原样 | 「权限总览」卡:PAPER 可用 / LIVE CLOSED + 六级计数 + 能力项总数 |
| 33 | 能力脉冲副标题「蓝色表示交互,而非权限」 | 改进措辞 | 改为「浏览器永不构造 live 权限」,直接陈述红线 |
| 34 | 最近执行摘要:最近批次 + 「绝不构造交易权限」 | 保留原样 | 侧栏「最近执行」卡(按 last_sequence 取最近 5 条,状态 pill 文字+安全色) |
| 35 | 预警摘要:最新预警 + 确认状态,「投影降级时宁可隐藏 occurrence,也不猜测最新事实」 | 保留原样 | 侧栏「告警流」卡:最近 8 条按 severity 安全色着色(文字+颜色),降级即整体停止展示 |
| 36 | 最近事件通知区(payload-free 通知表) | 改进措辞 | 移到 /executions「最近事件通知」卡(与游标分页同处);总览侧栏保留执行摘要 |
| 37 | 投影证据区:投影状态 / 批次已截断 / 警告已截断 / 最近更新 | 保留原样 | system 卡:投影 pill + 截断 pill(批次窗口化:已截断 / 警告窗口化:已截断)+「数据截至」 |
| 38 | 「数据截至 / 最近更新」为读取投影时间,不代表外部行情时间 | 保留原样 | 每卡 `数据截至` 行 + system 卡脚注原文 |

## 3. /executions

| # | 旧契约断言(语义) | 状态 | 说明 |
|---|---|---|---|
| 39 | 执行账本表列:批次 / 策略·交易对 / 状态 / 恢复 / 序号 / 更新时间 / 阶段 / 检查 | 保留原样 | `lib/columns/executionColumns.tsx`(数据驱动列定义,为列自定义留口) |
| 40 | 批次状态→色调映射(completed→ok;partial/incomplete/outcome_unknown→warning;failed/conflict→danger) | 保留原样 | `toneForBatchState`,状态=文字+安全色 |
| 41 | 恢复指令→色调(none→ok;reconcile_required→warning;investigate→danger) | 保留原样 | `toneForRecovery` |
| 42 | 阶段→色调(completed→ok;partial/incomplete→warning;failed→danger) | 保留原样 | `toneForPhase` |
| 43 | 窗口化投影横幅:「有界读取模型保留了未解决批次,并可能淘汰较早的已完成批次…」 | 保留原样 | `executionBanners`,有测试 |
| 44 | 降级投影横幅:「读取模型只接受安全的部分事实…不会被提升为健康状态」 | 保留原样 | `executionBanners`,有测试 |
| 45 | 批次/警告截断(truncation)显式呈现 | 保留原样 | `executionBanners` 「部分投影」横幅 + system 卡截断 pill |
| 46 | 游标分页:`?cursor=` 请求变更页,沿 next_cursor 前进 | 保留原样 | 「加载更多」+ `lib/cursorPager.ts` reducer(有测试);boundary=page_limit 才可加载更多,snapshot_end 显式说明已到边界 |
| 47 | invalid_cursor:「当前游标不适用于这个日志。清除游标…」;cursor_expired:「游标已不再匹配当前日志代次。清除游标…」 | 保留原样 | `errorPresentation` cursorInvalidated;命中即丢弃 pager 状态 + invalidate,SSE 侧走既有失效协议(`useOperationEvents`) |
| 48 | journal 代次更替(journal_id 变化)→ 旧通知作废 | 保留原样 | `cursorPagerReducer` 按 journal_id 重建,有测试 |
| 49 | 事件通知表:序号 / 类型 / 聚合对象 / 记录时间;「事件页不携带原始载荷」 | 保留原样 | `lib/columns/noticeColumns.tsx` + 卡片副标题原文 |
| 50 | 抽屉:role=dialog + aria-labelledby,打开聚焦关闭按钮 | 保留原样 | `components/DetailDrawer.tsx`,有焦点测试 |
| 51 | 抽屉:关闭后焦点还给触发行;禁把 batch_id 插值进选择器 | 保留原样 | 通过保存元素引用还焦(结构上不存在选择器插值),有测试 |
| 52 | 抽屉:计划与结果事实 16 字段(策略/交易对/序号/时间/腿/回执/失败索引/对账/已记录失败) | 保留原样 | `DrawerFacts` 全字段 + status_summary |
| 53 | 抽屉:持久化阶段带 + 空态「批次存在,但缺少阶段证据」 | 保留原样 | `DrawerPhases` |
| 54 | 抽屉:批次范围警告(按 batch_id 筛选 operator.warnings)+ 空态原文 | 保留原样 | `DrawerWarnings` |
| 55 | 抽屉:展示封套元数据 | 保留原样 | `DrawerEnvelope`:schema_version / journal_id / head_sequence / head_event_id / 投影状态 / 变更页边界 |
| 56 | 抽屉动作:复制批次 ID / 复制游标 | 改进措辞 | 完整 batch_id 直接全文呈现于抽屉头(可选中复制);「复制游标」不再提供 —— 游标是内存实现细节,鼓励复制反而助长把游标带出内存 |
| 57 | 执行筛选:全部/需要关注/已完成/部分完成/失败/冲突/结果未知;筛选与所选批次保留在 URL | 保留原样 | `useSearchParams`(`state` / `batch`);游标仍只存内存 |
| 58 | 空态:「没有执行批次符合当前筛选」/「这个有界快照中没有执行批次」 | 保留原样 | 按是否筛选区分两种空态 |
| 59 | 全宽表横向滚动容器(role=region + tabindex + aria-label),页面不横向滚动 | 保留原样 | `DataTable` 容器 `overflow-x-auto` + min-width,`aria-label="执行账本,可横向滚动"` |
| 60 | `outcome_unknown` 锁提交等 submit 联动(pendingSubmission / 提交回执校验) | W4 | 属提交面(strategies);执行页本轮纯只读 |

## 4. /alerts

| # | 旧契约断言(语义) | 状态 | 说明 |
|---|---|---|---|
| 61 | `MAX_ALERT_OCCURRENCES = 256` 窗口常量 | 保留原样 | `api-types.ts` 导出,与后端 `MAX_ALERT_READ_MODEL_OCCURRENCES` 对齐 |
| 62 | 可信投影集合 {complete, windowed} | 保留原样 | `lib/alerts.ts` `TRUSTED_ALERT_PROJECTION_IDS` |
| 63 | 信任判定:occurrences > 256 / invalid_event_count ≠ 0 / boundary ≠ snapshot_end / windowed↔truncated 矛盾 → 不可信 | 保留原样 | `isTrustedAlertProjection` 完整移植,有测试 |
| 64 | 不可信 → 隐藏全部 occurrence(`visibleAlertOccurrences` 返回空) | 保留原样 | 有测试;明细卡同时给出「不会把不可信的最近预警提升成可操作事实」说明 |
| 65 | 投影标签:契约不一致 → 「降级 / 契约不一致」 | 保留原样 | `alertProjectionLabel`,有测试 |
| 66 | 窗口化横幅:「预警投影已窗口化」「可信 / 已截断」+ 当前展示 N 条、更早记录已被有界淘汰 | 保留原样 | `alertBanners`,有测试;消息补充窗口上限 256 |
| 67 | 降级横幅:「预警投影已降级」「停止展示」「所有 occurrence 与最近预警都停止展示」 | 保留原样 | `alertBanners`,有测试(danger,role=alert) |
| 68 | 刷新失败且旧快照可信 → 「预警快照刷新失败」「保留旧快照」 | 保留原样 | `alertBanners({refreshFailed})`,有测试(含不可信时不出现) |
| 69 | 未决投递:`countPendingAlertDeliveries` + 「存在未决通知记录」「历史事实 / 不保证重放」「恢复默认不重放,本页不会把它解释为仍在排队」 | 保留原样 | `alertBanners`,有测试 |
| 70 | pending 文案「最后记录:未决」 | 保留原样 | `labels.ts`;投递 pill 与摘要均用 |
| 71 | 「通知 worker 异常终止」(worker_failed)等失败分类文案 | 保留原样 | `labels.ts` 全量投递失败分类 |
| 72 | 明细表列:序号 / 标的(含市场类型)/ 类型 / 价格·波动 / 通知结果(各 adapter 状态+失败+更新时间)/ 触发·确认时间 | 保留原样 | `lib/columns/alertColumns.tsx`;每条另加 severity pill(文字+安全色) |
| 73 | 「触发 ${时间}」「确认 ${时间}」/ 未确认「确认 --」 | 保留原样 | timing 列原格式 |
| 74 | 确认状态 pill:已确认(ok)/ 待确认(warning) | 保留原样 | 文字+颜色同现,有测试 |
| 75 | 投递状态色调:succeeded→ok;failed/timed_out→danger;dropped/pending→warning | 保留原样 | `toneForAlertDelivery`,有测试 |
| 76 | 表格横向滚动 aria-label「价格预警明细,可横向滚动」 | 保留原样 | `DataTable` ariaLabel 原文 |
| 77 | 投影状态卡:投影 / 可展示 occurrence(不可信→「已隐藏」)/ 无效事件 / 窗口截断 / 边界 / 头序号 | 保留原样 | `ProjectionCard` 全字段 |
| 78 | 「规则定义」「冷却状态」=「当前投影未提供」(一等不可用状态) | 保留原样 | `ProjectionCard` 两行原文 |
| 79 | 「未发生窗口截断」显式陈述 | 保留原样 | 窗口截断行:「否(未发生窗口截断)」 |
| 80 | 空态:「当前冻结快照中还没有价格预警 occurrence」+ 已检查来源说明 | 保留原样 | EmptyState 原文 |
| 81 | 概览侧摘要(最新序号/标的/类型/价格/确认/通知计数) | 改进措辞 | 总览侧栏「告警流」以最近 N 条流式呈现(severity 着色);计数摘要由投影状态卡承载 |

## 5. 其余六页(本轮不做,标 W4)

| # | 旧契约面 | 状态 |
|---|---|---|
| 82 | scanner 面(确定性虚拟网格排行、benchmark 优先、排行截断、scanner 降级横幅、980px 表) | W4 |
| 83 | strategies 面(策略运行面 / 只读策略证据 / 配置区 / Paper profile 表) | W4 |
| 84 | trusted submit 面(/api/v1/submit、pendingSubmission、outcome_unknown 锁、回执校验、幂等键) | W4 |
| 85 | risk 面(风险总览 / pending_reserved / committed_exposure / 恢复账本) | W4 |
| 86 | replay 面(回放快照 / 历史投影时间 / 回放面表) | W4 |
| 87 | settings 面(访问与外壳 / 只读边界 / data_directory / request_limit / 凭据投影) | W4 |
| 88 | integrations 面(适配器矩阵 / 能力证据筛选) | W4 |

## 统计

- 保留原样:**69** 条(#1–2, 4–7, 9–15, 20–30, 32, 34–35, 37–55, 57–59, 61–80)
- 改进措辞:**7** 条(#8, 18–19, 33, 36, 56, 81)
- 不适用:**3** 条(#3, 16–17)
- W4:**9** 条(#31, 60, 82–88)
