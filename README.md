# live-odds

Scrape live betting odds from bookmaker websites using Playwright. Currently supports `https://www.betsson.lt/en/live-betting`. Designed so each new bookmaker is a sibling package under `scrapers/`.

## Setup

```bash
uv sync
uv run playwright install chromium
# Linux/WSL only — install system libs Chromium needs (sudo):
sudo uv run playwright install-deps chromium
```

Then open `live_scraping/scrape_betsson.ipynb` for the main scraping flow, `investigate_odds/investigate_betsson.ipynb` to analyse the resulting parquet files, or `testing/debug_betsson.ipynb` for inspecting raw HTML.

## Layout

```
scrapers/
  betsson/
    page.py                   # PageSession + open_page() / load_page()
live_scraping/
  scrape_betsson.ipynb        # main UI: tabs per sport, scrape on demand, parquet snapshots
investigate_odds/
  investigate_betsson.ipynb   # read snapshot parquet files, explore odds movement
testing/
  debug_betsson.ipynb         # raw-HTML inspection
data/
  betsson.png                 # last screenshot from open_page()
  snapshots/                  # one parquet file per (bookmaker, sport); appended to on each scrape
```

Add new bookmakers as sibling packages under `scrapers/` (e.g. `scrapers/topsport/page.py`).

## Usage

```python
from scrapers.betsson.page import open_page

# Open Chromium (headless=False by default), land on the live-betting hub.
session = await open_page(headless=False)

# Pre-open a tab per sport — each gets its own browser tab.
for sport in ("basketball", "football", "tennis"):
    await session.open_sport(sport)

# Scrape any sport's tab. Each call:
#  - scrolls the events container top→bottom→top, scraping at each step
#  - concatenates per-step snapshots, dedups by event_id
#  - appends rows to data/snapshots/betsson_<sport>.parquet
df = await session.get_odds(sport="basketball")

# Or just get the rendered HTML for that tab.
html = await session.get_html(sport="basketball")

await session.close()
```

`load_page(...)` is a one-shot helper that opens, screenshots, and closes — useful when all you need is the screenshot.

## How `get_odds` works

`betsson.lt` renders live events inside an inner scrollable container, and items outside the viewport may not be in the DOM. So `get_odds`:

1. Routes to the sport's tab (creating it if needed).
2. Snapshots odds at the top of the events container.
3. Scrolls the container down by 300 px, waits 700 ms for new rows to mount, snapshots again.
4. Repeats up to 40 times, stopping when `scrollTop` stops changing.
5. Scrolls back to the top.
6. Concatenates every snapshot and drops duplicates by `event_id` (`keep="first"`).
7. If non-empty and `save=True` (default), appends to `data/snapshots/betsson_<sport>.parquet`.

The scroll target is auto-detected — the closest scrollable ancestor of the first `section.wel-tournament`. Falls back to window scrolling if no inner container is found.

## Snapshot files

There is one parquet file per (bookmaker, sport):

```
data/snapshots/betsson_basketball.parquet
data/snapshots/betsson_football.parquet
data/snapshots/betsson_tennis.parquet
```

Every `get_odds(sport=X)` call (with `save=True`, the default) **appends** that scrape's rows to its sport's file (creating it the first time). Each row's `scraped_at` UTC timestamp distinguishes snapshots over time, so you can analyse odds movement by filtering on `scraped_at`. Pass `save=False` to skip the write.

Sports never share files — `betsson_basketball.parquet` only contains basketball rows.

## DataFrame schema

One row per event, 17 columns:

| column | dtype | notes |
|---|---|---|
| `scraped_at` | datetime64 UTC | wall-clock at the snapshot start |
| `sport` | string | from the URL (`basketball`, `football`, …) |
| `league` | string | tournament name (e.g. "Europe, Euroleague") |
| `home_team` | string | |
| `away_team` | string | |
| `clock` | string | e.g. `"04:01"` |
| `period` | string | e.g. `"3 quarter"` |
| `event_id` | string | betsson `data-event-id` |
| `market` | string | `1X2` / `spread` / `moneyline` |
| `home_odds` | float64 | NaN if the cell wasn't visible |
| `draw_odds` | float64 | NaN unless `market == "1X2"` |
| `away_odds` | float64 | |
| `home_line` | string | spread line, e.g. `"-9.5"`; empty otherwise |
| `away_line` | string | |
| `home_odd_id` | string | betsson `data-event-odd-id` |
| `draw_odd_id` | string | |
| `away_odd_id` | string | |

## Known limitations

- **Geo-block.** `betsson.lt` is restricted to Lithuanian IPs. From elsewhere the page may redirect or render blank — connect via an LT VPN.
- **Headless on WSL2.** `open_page` defaults to `headless=False` for interactive inspection — that needs WSLg or an X server. Pass `headless=True` if you have neither.
- **DOM-coupled.** The scraper depends on betsson's CSS classes (`wel-tournament`, `wel-table__row`, `wel-odd`, `data-symbol-name`, `data-odd-value`, `data-additional-value`). If they redesign, update the selectors in `scrapers/betsson/page.py`.
- **Headline market only.** Only the row's headline market columns (1, X, 2 / home, away) are scraped. Side markets (totals, props, +N) are not.
