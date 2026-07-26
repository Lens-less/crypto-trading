# Crypto Trading Control Plane Design System

> Status: selected and active
> Selection: A — 金融精确
> User adjustment: use B's developer-native typography and C's audit-ledger palette
> Dials: visual variance 4/10, motion 3/10, information density 8/10

This file is the visual single source of truth for the project Web control plane. Change the
design here before changing individual pages.

## 1. Visual Theme & Atmosphere

The product is a **financial precision ledger** for one operator. Its memorable shape is a
fixed risk spine beside a cross-lane execution ribbon: authority, freshness, adapter health,
and unrecovered batches are visible before charts or decoration.

The interface combines:

- A's asymmetric operating-desk structure and risk-first hierarchy.
- B's developer-native mono rhythm for identifiers, cursors, times, prices, and keyboard hints.
- C's daylight audit palette, rectangular geometry, ruled tables, and print-like evidence.

It must feel exact, calm, and inspectable. It must not resemble a marketing dashboard,
terminal cosplay, or a wall of rounded cards.

Functional contract:

- In 3 seconds the operator can identify paper/live mode, live authority, journal projection,
  data freshness, adapter health, and unrecovered execution state.
- The operator can locate a batch, inspect its plan/outcome/recovery facts, and copy a stable
  cursor without constructing trading authority.
- `Unavailable`, `stale`, `degraded`, `windowed`, `partial`, `empty`, and `error` are first-class
  product states, not implementation footnotes.
- CLI, HTTP, and Web must display the same capability and execution truth.
- A monitor result is a durable historical projection. It must show its recorded time and market
  generation, and must never imply that the current external feed is connected or fresh.
- The operation-event SSE badge describes notification-channel connectivity only. It uses
  “connected / notification-only”, never “real-time” or “fresh”, and cannot upgrade monitor age.

## 2. Color Palette & Roles

The core palette follows IBM Carbon's white-to-gray audit hierarchy with one engineering blue.

| Token | Value | Role |
| --- | --- | --- |
| `--canvas` | `#ffffff` | Main page and table surface |
| `--layer-01` | `#f4f4f4` | Navigation groups, filters, alternate rows |
| `--layer-02` | `#e0e0e0` | Selected structural regions and skeletons |
| `--ink` | `#161616` | Primary text, dark risk spine |
| `--ink-secondary` | `#525252` | Secondary copy |
| `--ink-muted` | `#6f6f6f` | Metadata that still meets readable contrast |
| `--border` | `#c6c6c6` | Hairlines and table rules |
| `--blue` | `#0f62fe` | Sole interactive accent and focus |
| `--blue-hover` | `#0043ce` | Link/interactive hover |
| `--blue-active` | `#002d9c` | Pressed interaction |
| `--blue-tint` | `#edf5ff` | Selected row and informational surface |
| `--danger` | `#da1e28` | Failed, unsafe, or blocked |
| `--warning` | `#8e6a00` | Stale, degraded, or recovery required |
| `--success` | `#198038` | Complete, fresh, or reconciled |

Rules:

- Blue is for interaction or information, never decoration.
- Red is never used for ordinary negative P&L; it means action is blocked or evidence is unsafe.
- Status always has text plus color. Color alone never carries meaning.
- No gradients, purple glow, pure black, translucent text, or low-contrast gray body copy.

## 3. Typography Rules

Chinese UI copy uses the native CJK stack:

```css
-apple-system, BlinkMacSystemFont, "PingFang SC", "Hiragino Sans GB",
"Microsoft YaHei", "Noto Sans SC", sans-serif
```

Latin identifiers and numeric data use a developer-native mono stack inspired by Warp:

```css
"IBM Plex Mono", "JetBrains Mono", "Cascadia Code", "SFMono-Regular",
Consolas, monospace
```

The font plan intentionally ships no remote Chinese font and no blocking font dependency.

| Role | Size | Weight | Line height | Treatment |
| --- | --- | --- | --- | --- |
| Page title | `clamp(28px, 3vw, 42px)` | 400 | 1.19 | Chinese system sans, at most 2 lines |
| Section title | 20px | 600 | 1.4 | System sans |
| Body | 14px | 400 | 1.7 | System sans, `text-wrap: pretty` |
| Navigation | 14px | 500 | 1.5 | System sans |
| Compact label | 12px | 500 | 1.33 | 0.32px tracking; no forced English-only uppercase |
| Data / code | 13–14px | 400 | 1.5 | Mono, tabular numbers |
| Hero status | `clamp(24px, 2.6vw, 38px)` | 400 | 1.16 | Mono Latin/numbers, system CJK |

Never use italic, weight 700, negative tracking on Chinese, or type smaller than 12px.

## 4. Component Styling

### Navigation and risk spine

- Desktop uses a 248px fixed left spine with `#161616` background.
- The first region is authority, not branding: `PAPER`, `LIVE CLOSED`, bind/access mode.
- Active navigation is a 2px blue left rule plus a Gray 80 background shift.
- Rows are 48px high and remain real links so URL state is shareable.

### Buttons and links

- Buttons are rectangular with 0px radius, minimum 40px height, and 16px horizontal padding.
- Primary: `#0f62fe`; hover `#0353e9`; active `#002d9c`.
- Secondary: `#393939`; ghost actions use blue text on transparent background.
- Focus follows Carbon: 2px blue inset plus a 1px white separation ring.
- Pointer press feedback may use `transform: scale(0.98)` for 120ms; keyboard activation does not animate.

### Tiles and panels

- Default grouping uses whitespace, alternating layers, and 1px rules.
- Tiles use 0px radius and no shadow. `#f4f4f4` may distinguish a raised information layer.
- Only a real overlay/drawer may use `0 2px 6px rgba(0, 0, 0, 0.30)`.
- Status tags are the sole rounded exception: 24px pill radius, 4px 8px padding.

### Tables and execution ribbon

- Numeric columns are right-aligned with tabular mono numerals.
- Headers remain visible while scrolling and expose `aria-sort` where sorting exists.
- Selected rows use `#edf5ff` plus a 2px blue leading rule.
- The execution ribbon is a full-width ruled band, not a card. It shows batch id, phase,
  latest sequence, recovery state, and cursor before any expanded detail.
- Long identifiers truncate visually but expose the full value through an accessible title/copy action.

### States

- Loading skeletons match final row geometry and use Gray 10/20 only.
- Empty states state what is absent and what fact source was checked.
- Errors include one safe next step and never display raw adapter/journal text.
- Stale and degraded states remain usable but pin a visible warning band above the affected region.
- `Unavailable` is neutral and explicit; it must never be styled as healthy or as a user setup task.
- Monitor freshness and continuity labels describe the persisted observation only. System-level
  market freshness remains `not_available` until a live read-only source is actually supervised.
- A degraded monitor projection may retain its last valid fact internally for recovery, but the Web
  must withhold that outcome and pin a danger band until the complete journal projects safely again.

## 5. Layout Principles

- Base unit: 8px; micro-adjustments may use 2px or 4px.
- Desktop content uses an asymmetric 12-column grid beside the 248px risk spine.
- Max content width is 1584px with 32px gutters; mobile gutters are 16px.
- Major vertical transitions use 48px; dense related controls use 8px or 16px.
- Overview begins with one dominant cross-lane system ribbon, then a 2fr/1fr evidence layout.
- Executions prioritizes the full-width ledger and a right-side detail drawer.
- Integrations uses a capability matrix, not equal marketing cards.
- The selected section uses a semantic path; filters and batch id use the URL query string.
- Opaque journal cursors and bearer tokens stay in the current page's memory and never enter
  browser history, persistent storage, or copied deep links.

## 6. Depth & Elevation

Depth is communicated by information authority and surface value:

1. Canvas: white.
2. Structural layer: Gray 10.
3. Selected/temporary layer: Gray 20 or Blue 10.
4. True overlay: one restrained shadow plus a scrim.

Cards and tables do not cast shadows. Borders and background shifts carry hierarchy.

## 7. Do's and Don'ts

Do:

- Put real operating truth before release gates or missing features.
- Keep mode, authority, freshness, adapter state, and recovery state continuously visible.
- Use mono type for cursors, sequences, ids, timestamps, prices, and counts.
- Preserve exact capability names and safe recovery directives from the read model.
- Provide keyboard focus, skip navigation, copy affordances, and visible URL state.

Don't:

- Construct or imply live authority in the browser.
- Turn unavailable capabilities into a blocker wall or a setup checklist.
- Use rounded dashboard cards, decorative charts, glass, gradients, glow, or fake market data.
- Treat toast messages as durable evidence.
- Hide stale/degraded/windowed status behind a tooltip.
- Use raw secrets, payloads, filesystem paths, or source errors in visible copy.

## 8. Responsive Behavior

Breakpoints follow the Carbon grid:

- `>= 1056px`: fixed 248px risk spine, full 12-column workspace, optional detail drawer.
- `672–1055px`: 176px compact spine with visible text labels; content collapses before evidence
  becomes unreadable.
- `< 672px`: top authority strip, single-column content, 16px margins.
- Tables become horizontally contained data regions with a visible scroll affordance; the page itself
  never scrolls horizontally.
- Long symbols, exchange pairs, cursors, and decimal evidence wrap inside their own grid cell instead
  of widening the page or being removed from the trust line.
- Execution details move below the selected row on mobile.
- All interactive targets are at least 40px; primary navigation rows are 48px.
- The UI must remain usable at 320 CSS px and at 200% browser zoom.

## 9. Motion Philosophy

Motion is restrained at 3/10 and exists only to explain state:

- Color/focus/press feedback: 100–160ms.
- Pointer-opened drawer or confirmation: 180–240ms, ease-out.
- SSE updates do not animate row position; a 160ms blue leading-rule fade marks newly observed facts.
- Keyboard-triggered navigation and high-frequency commands never animate.
- Only `transform` and `opacity` may move.
- `prefers-reduced-motion` removes translation and leaves immediate state/color changes.

Signature craft actions:

1. A low-opacity ruled-grid atmosphere outside reading surfaces (maximum 3%).
2. Blue `::selection`.
3. Carbon-style blue/white focus ring.
4. One signature interaction: the execution ribbon and detail drawer share the same blue leading rule.
5. Flat white → Gray 10 → Gray 20 depth, with shadow reserved for the drawer.
6. A narrow blue custom scrollbar for data regions.
7. Hairline audit rules and mono cursor stamps.

DNA provenance:

- From **IBM Carbon**: `#0f62fe` as the sole accent, white/Gray 10/Gray 20 layer sequence,
  0px primary geometry, 48px navigation rhythm, and the 2px blue + 1px white focus treatment.
- From **Warp**: mono-forward identifiers and telemetry, weight 400 dominance, quiet compact labels,
  and restrained interactions that do not animate high-frequency keyboard work.
