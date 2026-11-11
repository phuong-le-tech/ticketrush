# DESIGN.md - TicketRush dashboard (visual design)

Layered on top of the global design rules; where they conflict, this file wins.
Status: DRAFT. Items marked (call) are proposals, not decisions.
System design (schemas, topics, versions) lives in [../DESIGN.md](../DESIGN.md), not here.
What gets built and how it is accepted: [../superpowers/specs/2026-11-11-live-dashboard-design.md](../superpowers/specs/2026-11-11-live-dashboard-design.md).

## 1. Intent

A live control room that a recruiter watches for two minutes during a demo and leaves understanding
four things: how concurrency is kept safe, how data moves through Kafka, what Redis does, and how
Kubernetes scales consumers. Every claim is shown by a number that moves, read from the running system.

Feeling: sober, precise, demonstrative.

References, measured on 2026-11-11 with the audit script:

| Site | What we take | Measured |
|---|---|---|
| linear.app | near-black neutral ground, hairline borders, short feedback motion | body `rgb(8,9,10)`, borders 0.5px white 8%, 100/160ms feedback, 700ms large moves, h1 38px/510/-0.84px |
| vercel.com | tight display type, mono for data, almost no borders | h1 48px/400/-2.88px, Geist + Geist Mono, 100-150ms |
| railway.com | an architecture diagram as the hero | h1 40px/-1.96px, 3 text greys |

Must not be: a generic metrics grid of equal cards, a marketing landing page, or a fake demo.
Nothing on screen is sample data.

## 2. Layout

Design width 1440px, desktop only. One scrolling page.

```
+--------------------------------------------------------------------+
| TicketRush                         live . kind-ticketrush . 2s     |  top bar, 56px
+--------------------------------------------------------------------+
| SYSTEM MAP (leads, ~420px tall, full width)                        |
| buyer -> gateway -> [orders.requested] -> inventory -> [tickets.   |
| reserved] -> payment -> [payments.result] -> order -> [orders.     |
| confirmed] -> notification.  Redis under inventory, Postgres under |
| order. Orders travel as dots. Live counters on each node.          |
+----------------------------------------------+---------------------+
| 01 CONCURRENCY   claim + stock chart         | BUY (sticky, 360px) |
| 02 KAFKA         claim + lag chart           | form + my order's   |
| 03 REDIS         claim + commands chart      | saga steps          |
| 04 KUBERNETES    claim + pods and lag chart  |                     |
+----------------------------------------------+---------------------+
| Recent orders table (full width)                                   |
+--------------------------------------------------------------------+
```

- The system map leads. Chapters are secondary. The orders table is tertiary.
- Each chapter: number, one-sentence claim ("what this proves"), one live chart at about 2/3 width,
  one key figure in mono at 1/3, one line naming the mechanism in the code.
- Chapters are separated by space (96px) and a hairline, not by boxes.
- Below 1100px the buy rail drops under the chapters. No further mobile work (owner decision).

## 3. Signature interaction

An order is a dot that travels the system map from gateway to notification. The dot of the order the
presenter just bought is the accent color and leaves a trail; background load is drawn as neutral
dots whose density follows the measured rate per edge (capped, see section 8). A rejected or failed
order visibly leaves the main path at the service that refused it. Motion budget is spent here and
on charts gaining points; nowhere else.

## 4. Color

Dark only (owner decision).

| Token | Value | Use |
|---|---|---|
| `--bg` | `#0a0a0b` | page |
| `--surface` | `#111113` | chart ground, rail |
| `--raised` | `#1a1a1e` | inputs, hovered rows |
| `--hairline` | `rgba(255,255,255,0.08)` | the only border |
| `--fg` | `#f4f4f5` | primary text |
| `--fg-2` | `#a1a1aa` | secondary text (7.6:1 on bg) |
| `--fg-3` | `#71717a` | labels, axes (4.1:1: large or non-text only) |
| `--accent` | `#f5a524` | "moving right now": my order's dot, live chart head, Buy button (9.7:1 on bg; text on accent is `#0a0a0b`) |
| `--ok` | `#4ade80` | confirmed |
| `--warn` | `#f5a524` | pending retry, stale (same hue as accent, always paired with a label) |
| `--bad` | `#f87171` | rejected, failed |

Removed: the blue-tinted ground, the sky-blue accent, opaque 1px borders on every panel.
No gradients, no glow, no shadows.

## 5. Type

One family: Geist (sans) with Geist Mono for every number, id, topic name and code. Self-hosted
woff2 in `dashboard/public/fonts` so the demo works offline (see spec, dependencies).

| Role | Size / weight / tracking |
|---|---|
| Display (page claim) | 40px / 500 / -1.6px |
| Chapter claim | 24px / 500 / -0.5px |
| Key figure | 56px mono / 500 / -1px, tabular numbers |
| Body | 15px / 400 / 0, line-height 24px |
| Label, axis | 12px mono / 400 / +0.2px, uppercase for chapter numbers only |
| Table | 13px |

No other sizes.

## 6. Space, radius, borders

Spacing scale 4, 8, 12, 16, 24, 32, 48, 96. Radii: 6px (controls, nodes), 999px (status dots,
badges). A border is allowed only as a hairline between regions and around inputs. No shadows.

## 7. Density

Controls 40px tall. Table rows 44px. Charts 200px tall, system map about 420px. Data is drawn, not
listed: areas and lines for time series, small squares for pods and partitions, mono numbers.

## 8. Motion

| Token | Value | Use |
|---|---|---|
| `--ease-out` | `cubic-bezier(0.32, 0.72, 0, 1)` | everything |
| `--t-fast` | 150ms | hover, press, badge change |
| `--t-move` | 600ms | a dot crossing one edge, a pod appearing, a chart shifting by one sample |

- Transform and opacity only. Charts shift by translating the path, not by re-laying out.
- Numbers do not count up; they change, and the changed figure flashes accent for 600ms.
- At most 40 dots on the map at once (call); above that, density stops growing and the edge label
  shows the true rate.
- `prefers-reduced-motion`: no travelling dots and no chart slide; nodes pulse opacity once per
  change, charts redraw in place.

## 9. States to design

- Loading (first poll not back): skeleton chart ground, figure shows a dash.
- Live.
- Stale (a source failed): last good data stays, the chapter gets a `--warn` "stale since hh:mm:ss".
- Metrics unavailable (running on Compose, no Prometheus): chapters 02-04 show one line,
  "Live metrics need the kind stack", and no empty chart. Chapter 01, the map counters fed by the
  order API, Buy and the table still work.
- Idle (no load): charts are flat and say so: "No orders in the last minute. Start load with
  `scripts/...`".
- Sold out: stock at 0, rejected counter rising, stock line flat on the zero floor.
- Long ids: order ids truncate in the middle in the rail, full in the table.

## 10. Components to restyle first

1. Tokens and type (`styles.css`).
2. Status badge and live/stale indicator.
3. Time-series chart (one component, used four times).
4. Chapter block.
5. System map.
6. Buy rail and orders table.

## 11. Do and don't

- Do put the claim before the chart. Do name the real topic, key and consumer group.
- Do keep the existing honest copy ("acceptance is not confirmation").
- Don't show a number that is not read from the system. Don't call Redis a cache: here it is the
  atomic stock counter and the idempotency record.
- Don't add a second accent, a card grid, icons-as-decoration or emoji.

## 12. Decisions (owner, locked 2026-11-11)

- Direction "sober control room", amber accent, Geist, dark only, desktop only.
- Everything live, demo runs on kind.

## 13. Remaining (call) items

- Exact amber `#f5a524` and the grey ramp, pending the mockup.
- 40-dot cap on the system map.
- Whether the buy rail is sticky or sits under the map.
