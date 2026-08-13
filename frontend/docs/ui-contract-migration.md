# 旧 UI 契约断言迁移映射(W5 定稿)

> **历史文档(2026-08-13 起部分失效)**:alerts / scanner 页面及其读模型已在
> live-trading 重聚焦 Wave A 中整体移除;涉及 alerts / scanner 的迁移落点
> (S3 / S5 等)不再对应现存代码,仅作历史记录保留。

来源:`git show e5fdbc8:rust/crates/web/tests/ui_contract.rs`(757 行,锁定已删除的
`rust/crates/web/assets/{app.js,app.css,index.html}` 免构建原生 JS 前端)。
该文件在 W1 重写为第一层合约(现 `rust/crates/web/tests/ui_contract.rs`,7 个测试,
对任意嵌入 bundle 成立),语义断言随 W3/W4 迁入 React + vitest,W5 补齐
fixture 交叉契约与 Playwright 真浏览器层。旧资产本体已于 W5 删除。

四层新契约(去向列使用的缩写):

| 缩写 | 层 | 位置 |
|---|---|---|
| **R** | Rust 第一层合约(传输 / 安全边界,与 bundle 内容无关) | `rust/crates/web/tests/ui_contract.rs`、`http_contract.rs` |
| **F** | fixture 交叉契约(类型同步锁) | `rust/crates/web/tests/api_fixture_contract.rs` ↔ `frontend/src/lib/api-fixtures.test.ts` + `rust/fixtures/web-api/` |
| **V** | vitest 组件 / 纯函数测试(217+ 用例) | `frontend/src/**/*.test.{ts,tsx}` |
| **P** | Playwright 真浏览器合约(真实二进制 + 嵌入 dist) | `frontend/e2e/*.spec.ts` |

语义断言(中文文案、投影信任判定、页面行为)的逐条落点已在
[`w3-parity-checklist.md`](./w3-parity-checklist.md) 枚举为 88 条(69 保留 / 7 改进 /
3 不适用 / 9 W4 完成);本表按旧文件结构给出**每个断言组**的测试层去向,与该清单
配合构成完整映射。

## 1. 传输与安全边界断言(旧测试函数 → 去向)

| # | 旧断言(函数 / 断言组) | 去向 | 说明 |
|---|---|---|---|
| T1 | `root_and_semantic_paths…`:10 条语义路由 200 + HTML | **R** `semantic_routes_serve_the_shell_with_ui_csp_and_no_store` | 路由清单不变(`SHELL_PATHS`) |
| T2 | shell 安全响应头(no-store / nosniff / no-referrer / DENY / permissions-policy) | **R** `assert_security_headers`(每个测试复用) | 原样保留 |
| T3 | UI CSP(style/script/connect-src 'self')与 CSP 同源约束 | **R** `assert_ui_csp` + `assert_security_headers` | 原样保留 |
| T4 | API CSP 收口(default-src 'none',不扩 style/script/connect) | **R** `assert_api_csp`(bearer/404 路径) | 原样保留 |
| T5 | shell 不含 `live-enable` / `order-submit` | **R** `embedded_files_expose_no_forbidden_control_surface`(`FORBIDDEN_SURFACES`,扩至全部嵌入文件) | 加严:含 `document.cookie`、`order_submit`、`reconcile_release` 等 |
| T6 | shell 资产引用同源 + MIME 正确 | **R** `shell_asset_references_are_same_origin_and_served_with_expected_mime` | 原样保留 |
| T7 | (隐含)资产按预期字节服务 | **R** `every_embedded_file_is_served_byte_identical_with_correct_mime` | 加严:逐文件逐字节 |
| T8 | 资产无外域引用(`http://`、`//cdn`、`//fonts`) | **R** `embedded_files_reference_no_external_origins` | 改进:React/Zod/Tailwind 的惰性标识 URL 进入显式 `INERT_URL_PREFIXES` 白名单,文档(HTML)仍零绝对 URL |
| T9 | HTML 无 `<style` / `style="` 内联样式 | **R** `semantic_routes…` 内断言 | 原样保留(UI CSP 禁内联) |
| T10 | `src="/assets/app.js"` / `href="/assets/app.css"` 固定文件名 | **有意废除** | Vite 产出内容哈希文件名;替代断言:嵌入 shell 必须引用 `/assets/` 与 `theme-init.js`(**R** T1/T7) |
| T11 | `class="initial-loading"` + 「正在加载只读运行事实」首帧 | **有意废除**(checklist #16) | React 骨架接管首帧;骨架一等状态由 **V** 各页测试覆盖 |
| T12 | shell 含英文导航标签 `Overview/Alerts/…` | **有意废除** | 旧断言本身与「中文优先」矛盾;导航现由 React 渲染,中文标签由 **V**(AppShell 经各页测试挂载)与 **P** `read-only.spec.ts`(点击「执行 / 预警 / 设置」导航)覆盖 |
| T13 | `bearer_protects_the_api_but_not_the_data_free_shell`(API 401 + WWW-Authenticate;shell 数据自由不漏 token) | **R** `bearer_protects_the_api_but_the_data_free_shell_never_leaks_the_token` | 原样保留 |
| T14 | `app_router_remains_read_only_and_unknown_routes_fail_closed`(PUT/PATCH/DELETE/POST 拒绝;未知路由 JSON 404) | **R** `write_methods_are_rejected_and_unknown_routes_fail_closed` | 加严:写方法遍历全部嵌入资产路由;`/index.html` 与未知 `/assets/*` 显式 404 |

## 2. 浏览器资产内容断言(旧 `embedded_assets_lock_the_operator_design_and_secret_boundary`)

旧模式是对 app.js/app.css 全文做字符串匹配;React + Vite 产物经打包混淆,字符串匹配
既脆弱又失真,因此这些断言迁移为**对行为而非源码文本**的测试。

| # | 旧断言组 | 去向 | 说明 |
|---|---|---|---|
| C1 | JS 禁 `localStorage` / `sessionStorage` / `document.cookie` | **P** `read-only.spec.ts`「存储键集合 ⊆ {ct-theme}」+ **V** `theme.test.ts` + **R** `FORBIDDEN_SURFACES`(document.cookie) | 改进(checklist #2):白名单从「零存储」放宽为「仅 `ct-theme` 主题键」;bearer token / 游标仍仅内存,由 **P** 实测(整页刷新即丢 token 是 paper-writes spec 的前置契约) + cookies 为空断言 |
| C2 | JS 禁 `.innerHTML` / `insertAdjacentHTML` | **有意废除**(checklist #3) | React JSX 渲染路径不拼 HTML,契约由框架结构性保证 |
| C3 | JS 禁 `method: "PUT"/"PATCH"/"DELETE"` | **V** `api.test.ts`(`request()` 只发 GET)+ `submit.test.ts`(唯一 POST 是 `/api/v1/submit`)+ **R** T14 服务端拒绝 | checklist #4 |
| C4 | JS 必须消费 9 个 `/api/v1/*` 端点 | **F** 全套(9 端点字节锁 + zod 全解析,含 `/risk` 组合响应)+ **V** 各页测试消费对应 schema | checklist #5;fixture 契约同时锁住「消费什么形状」 |
| C5 | CSS 禁 `gradient` / `transition: all` / `font-style: italic`;表格 min-width 像素值;`.metric-value` 等选择器 | **有意废除** | 设计系统改由 `src/styles/tokens.css` 设计令牌 + Tailwind 原子类承载(`docs/design-system.md`);像素级选择器断言与实现耦合过深。语义等价物保留:表格横向滚动容器(checklist #59、#76,**V**)、骨架无 shimmer(#15,**V**) |
| C6 | CSS 含 `@media (prefers-reduced-motion: reduce)` | **保留在源**(`src/styles/tokens.css`),无字符串测试 | 构建后该媒体查询仍在产物中;作为可访问性基线随设计令牌维护 |
| C7 | `TOKEN_LABELS` 稳定 token→中文映射 | **V** `labels`(经各页测试)| checklist #8:收敛为 `humanizeToken` |
| C8 | 「不透明恢复游标只保存在页面内存」 | **V** `cursorPager.test.ts` + executions 文案 | checklist #6 |
| C9 | `market_data_freshness` / `kill_switch` 一等呈现 | **V** overview(经 system 卡)+ **F** `system.json` 字段锁 | checklist #7 |

## 3. 页面语义断言组(旧 `assert_*_surface_contract` → 去向)

| # | 旧断言组 | checklist 条目 | 测试层去向 |
|---|---|---|---|
| S1 | monitor 面(标题、历史投影脚注、recorded_at / market_generation、降级横幅、waiting/机会/拒绝分支) | #20–26 | **V** `banners.test.ts`、overview/replay 页测试;**F** `monitor.json`(latest 非空);**P** `read-only.spec.ts`(真浏览器渲染 recorded_at(记录时间)与 market generation、历史投影脚注) |
| S2 | SSE 文案(「已连接 / 仅通知」,禁「实时」「新鲜 / 流式更新」) | #10、#18 | **V** `NotificationChannelBadge.test.tsx`、`sse.test.ts`、`useOperationEvents.test.tsx`;**P** `read-only.spec.ts`(徽标文案 ∈ 三态集合)+ `notification-channel.spec.ts`(降级→恢复全程无「实时」措辞的三态) |
| S3 | alerts 面(256 窗口、信任判定、可见性、横幅、投递分类、表列、时间格式) | #61–81 | **V** `alerts.test.ts`、`banners.test.ts`、alertColumns(经 alerts 页);**F** `alerts.json` + `MAX_ALERT_OCCURRENCES` 常量对齐 |
| S4 | tasks 面(只读连续任务、不证明存活、无控制入口、降级/investigate 横幅、明细表) | #27–31 | **V** `banners.test.ts`、`strategies.test.tsx`(任务明细列);**F** `tasks.json` |
| S5 | scanner 面(确定性排行、benchmark 优先、截断/降级、980px 表、禁控制入口) | #82 | **V** `scanner.test.tsx`、`banners.w4.test.ts`;**F** `scanner.json` |
| S6 | trusted submit 面(envelope 结构、pendingSubmission、outcome_unknown 锁、回执校验、幂等键复用、422、secure_random、投影非 complete 禁提交、禁 reconciler 角色) | #60、#84 | **V** `submit.test.ts`(状态机纯函数逐条)、`strategies.test.tsx`;**P** `paper-writes.spec.ts`(真实二进制上的 start_paper_grid → durable receipt → 投影生效 → stop_task 二次确认全流程);**R** `FORBIDDEN_SURFACES`(reconcile_release 等永不进浏览器资产) |
| S7 | M6 信息架构(strategies/risk/replay/settings 四页导航与区域、pending_reserved、paper_profiles、data_directory、request_limit、中文标题无英文残留) | #83–88、#9 | **V** `risk.test.tsx`、`replay.test.tsx`、`settings.test.tsx`、`integrations.test.tsx`、`strategies.test.tsx`;**F** `settings.json`、`risk.json`(paper 敞口 + account_risk 组合形状);**P** 导航点击(read-only spec) |
| S8 | protected state(401 清态、session generation 递增、旧代际丢弃、focus 不插值 batch_id) | #11、#47–48、#50–51 | **V** `useOperationEvents.test.tsx`(invalidateSession / generation)、`api.test.ts`、`DetailDrawer.test.tsx`(焦点还原用元素引用,结构上无选择器插值) |
| S9 | executions 面(表列、色调映射、游标分页、横幅、抽屉 16 字段、URL 筛选) | #39–59 | **V** cursorPager / banners / executionColumns / DetailDrawer 测试;**F** `executions.json`(operator + changes 双 schema) |

## 4. TODO

-(已完成)`/api/v1/risk` fixture 快照:账户风控投影落地后已在
  `SNAPSHOT_ENDPOINTS` 中启用并生成 `rust/fixtures/web-api/risk.json`
  (`{schema_version, paper_accounts, account_risk}` 组合响应),前端
  `api-fixtures.test.ts` 用 `riskResponseSchema` 全量解析。

## 5. 统计

- 旧 757 行契约提炼为 **23 个传输/内容断言组(T1–T14、C1–C9)+ 9 个页面语义断言组(S1–S9,
  内含 checklist 88 条)**;
- 去向:**R** 12 组(T1–T9、T13–T14、C1 部分)、**V** 覆盖全部 9 个语义组与 C3/C7–C9、
  **F** 锁 9 个端点形状(C4、S1、S3–S5、S7、S9)、**P** 6 个真浏览器用例(权限脊柱、
  monitor 事实、SSE 三态、存储纪律、降级恢复、写路径全流程);
- **有意废除 5 组**(T10 哈希文件名、T11 首帧占位、T12 英文导航、C2 innerHTML、
  C5 像素级 CSS 字符串),废除理由均在上表逐条注明;
- checklist 88 条语义断言全部有落点:69 保留原样 + 7 改进措辞 + 9 W4 完成(均有
  vitest),3 不适用(#3、#16、#17,理由见清单)。
