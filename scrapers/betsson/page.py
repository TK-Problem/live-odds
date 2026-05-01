"""Open the betsson live-betting page and scrape odds out of the DOM.

Entry points:
- ``load_page()``  — one-shot: open, screenshot, close.
- ``open_page()``  — interactive: open and return a ``PageSession`` you close
  yourself.

``PageSession`` opens one browser tab per sport on demand. It exposes:

- ``await session.open_sport("basketball")``  — pre-open a tab for a sport.
- ``await session.get_html(sport=...)``        — raw HTML for that sport's tab.
- ``await session.get_odds(sport=...)``        — scrape the sport's tab into a
  per-event DataFrame and write a parquet snapshot under ``data/snapshots/``.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)

URL_BASE = "https://www.betsson.lt/en/live-betting"
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_SCREENSHOT = _PROJECT_ROOT / "data" / "betsson.png"
_SNAPSHOTS_DIR = _PROJECT_ROOT / "data" / "snapshots"

_ODDS_COLUMNS = [
    "scraped_at",
    "sport",
    "league",
    "home_team",
    "away_team",
    "clock",
    "period",
    "event_id",
    "market",
    "home_odds",
    "draw_odds",
    "away_odds",
    "home_line",
    "away_line",
    "home_odd_id",
    "draw_odd_id",
    "away_odd_id",
]

# Helper JS, repeated inside each scroll fn: find the scrollable container
# (closest ancestor of a tournament section with vertical overflow), or null
# to signal we should fall back to window scrolling.
_JS_FIND_CONTAINER = """
const findContainer = () => {
  const seed = document.querySelector('section.wel-tournament');
  if (!seed) return null;
  let el = seed.parentElement;
  while (el && el !== document.body) {
    const s = window.getComputedStyle(el);
    if ((s.overflowY === 'auto' || s.overflowY === 'scroll')
        && el.scrollHeight > el.clientHeight) return el;
    el = el.parentElement;
  }
  return null;
};
"""

# Scroll one step. Returns new scrollTop and whether we appear to be at the bottom.
_JS_SCROLL_STEP = """
(opts) => {
""" + _JS_FIND_CONTAINER + """
  const c = findContainer();
  if (!c) {
    const before = window.scrollY;
    window.scrollBy(0, opts.stepPx);
    return { scrollTop: window.scrollY, atBottom: window.scrollY === before, container: 'window' };
  }
  const before = c.scrollTop;
  c.scrollBy(0, opts.stepPx);
  return {
    scrollTop: c.scrollTop,
    atBottom: c.scrollTop === before,
    container: c.className || '<no-class>',
  };
}
"""

_JS_SCROLL_TOP = """
() => {
""" + _JS_FIND_CONTAINER + """
  const c = findContainer();
  if (c) c.scrollTo({ top: 0 });
  else window.scrollTo(0, 0);
}
"""

# Single round-trip extractor — runs in the page context, returns plain JSON.
_JS_EXTRACT_ODDS = """
() => {
  const rows = [];
  const tournaments = document.querySelectorAll('section.wel-tournament');
  for (const t of tournaments) {
    const leagueEl = t.querySelector('.wel-table__col__title a');
    const league = leagueEl ? leagueEl.innerText.trim() : '';
    const headerCols = t.querySelectorAll('header .wel-table__col[data-symbol-name]');
    const colLabels = Array.from(headerCols).map(c => c.getAttribute('data-symbol-name') || '');
    const hasDraw = colLabels.some(c => c.includes('draw'));
    const eventRows = t.querySelectorAll('.wel-tournament__body .wel-table__row[data-event-id]');
    for (const er of eventRows) {
      const event_id = er.getAttribute('data-event-id') || '';
      const teamLinks = er.querySelectorAll('.wel-teams__team a');
      const teams = Array.from(teamLinks).map(a => a.innerText.trim());
      const home = teams[0] || '';
      const away = teams[1] || '';
      const clockEl = er.querySelector('.wsb-timeSecondsWrapper');
      const periodEl = er.querySelector('.wsb-periodInfo');
      const clock = clockEl ? clockEl.innerText.trim() : '';
      const period = periodEl ? periodEl.innerText.trim() : '';
      const oddCells = er.querySelectorAll('.wel-odd[data-odd-value]');
      let i = 0;
      for (const oc of oddCells) {
        rows.push({
          event_id,
          league,
          home,
          away,
          clock,
          period,
          col_label: colLabels[i] || '',
          line: oc.getAttribute('data-additional-value') || '',
          odd_id: oc.getAttribute('data-event-odd-id') || '',
          odd_value: oc.getAttribute('data-odd-value') || '',
          has_draw_col: hasDraw,
        });
        i += 1;
      }
    }
  }
  return rows;
}
"""


@dataclass
class PageSession:
    """An open Playwright session with one tab per sport.

    The initial ``page`` lands on the live-betting hub. Per-sport tabs are
    created on demand (or up-front via ``open_sport``); each call to
    ``get_odds`` / ``get_html`` is routed to the tab matching its ``sport``
    argument. Call ``await session.close()`` when done.
    """

    playwright: Playwright
    browser: Browser
    context: BrowserContext
    page: Page
    screenshot: Path
    tabs: dict[str, Page] = field(default_factory=dict)
    snapshots_dir: Path = field(default=_SNAPSHOTS_DIR)

    async def close(self) -> None:
        for tab in list(self.tabs.values()):
            try:
                if not tab.is_closed():
                    await tab.close()
            except Exception:
                pass
        self.tabs.clear()
        try:
            await self.context.close()
        except Exception:
            pass
        try:
            await self.browser.close()
        except Exception:
            pass
        await self.playwright.stop()

    async def open_sport(self, sport: str) -> Page:
        """Open *sport*'s live-betting page in a new browser tab (or return
        the existing tab if already open). Brings the tab to the front."""
        return await self._page_for_sport(sport)

    async def get_html(self, sport: str | None = None) -> str:
        """Return the current HTML of *sport*'s tab (or the landing page)."""
        page = await self._page_for_sport(sport)
        return await page.content()

    async def get_odds(
        self,
        sport: str | None = None,
        save: bool = True,
    ) -> pd.DataFrame:
        """Scrape odds for *sport* and save a parquet snapshot.

        Routes to *sport*'s dedicated tab (creating it if needed). Snapshots
        odds at the top of the events container, scrolls down one step,
        snapshots again, repeats until the bottom is reached, then scrolls
        back to the top. Per-step DataFrames are concatenated and
        de-duplicated by ``event_id`` (``keep="first"``).

        If ``save=True`` (default) and the result is non-empty, appends the
        DataFrame to ``data/snapshots/betsson_<sport>.parquet`` (creating it
        if missing) — one file per (bookmaker, sport).
        """
        page = await self._page_for_sport(sport)
        sport_label = sport or _sport_from_url(page.url)
        dfs = await self._scroll_and_scrape(page=page, sport=sport_label)

        if not dfs:
            df = _rows_to_df([], sport_hint=sport_label, page_url=page.url)
        else:
            df = pd.concat(dfs, ignore_index=True)
            if not df.empty:
                df = df.drop_duplicates(subset=["event_id"], keep="first").reset_index(drop=True)

        if save and not df.empty:
            _save_snapshot(df, sport=sport_label, out_dir=self.snapshots_dir)
        return df

    async def _page_for_sport(self, sport: str | None) -> Page:
        if not sport:
            return self.page
        cached = self.tabs.get(sport)
        if cached and not cached.is_closed():
            try:
                await cached.bring_to_front()
                return cached
            except Exception:
                # tab is dead or detached; fall through to recreate
                pass
        tab = await self.context.new_page()
        await tab.goto(_url_for_sport(sport), wait_until="domcontentloaded")
        try:
            await tab.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass
        try:
            await tab.bring_to_front()
        except Exception:
            pass
        self.tabs[sport] = tab
        return tab

    async def _scroll_and_scrape(
        self,
        page: Page,
        sport: str | None,
        max_iterations: int = 40,
        step_px: int = 300,
        step_delay_ms: int = 700,
    ) -> list[pd.DataFrame]:
        """Scroll *page*'s events container down in steps, scraping at each step.

        Returns the list of per-step DataFrames (one entry per scroll
        position, including the initial top-of-page snapshot). After
        reaching the bottom, scrolls back to the top.
        """
        dfs: list[pd.DataFrame] = []

        raw = await page.evaluate(_JS_EXTRACT_ODDS)
        dfs.append(_rows_to_df(raw, sport_hint=sport, page_url=page.url))

        last_top: int | None = None
        for _ in range(max_iterations):
            result = await page.evaluate(_JS_SCROLL_STEP, {"stepPx": step_px})
            await page.wait_for_timeout(step_delay_ms)
            raw = await page.evaluate(_JS_EXTRACT_ODDS)
            dfs.append(_rows_to_df(raw, sport_hint=sport, page_url=page.url))
            cur_top = result.get("scrollTop") if isinstance(result, dict) else None
            if cur_top is not None and cur_top == last_top:
                break
            last_top = cur_top

        await page.evaluate(_JS_SCROLL_TOP)
        await page.wait_for_timeout(400)
        return dfs


async def open_page(
    url: str = URL_BASE,
    screenshot_path: str | Path = _DEFAULT_SCREENSHOT,
    headless: bool = False,
    timeout_ms: int = 30_000,
) -> PageSession:
    """Open *url* in Chromium, screenshot, leave the browser running."""
    screenshot_path = Path(screenshot_path)
    screenshot_path.parent.mkdir(parents=True, exist_ok=True)

    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=headless)
    context = await browser.new_context(
        viewport={"width": 1440, "height": 900},
        locale="en-US",
    )
    page = await context.new_page()
    await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    try:
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except Exception:
        pass
    await page.screenshot(path=str(screenshot_path), full_page=True)
    return PageSession(
        playwright=pw,
        browser=browser,
        context=context,
        page=page,
        screenshot=screenshot_path,
    )


async def load_page(
    url: str = URL_BASE,
    screenshot_path: str | Path = _DEFAULT_SCREENSHOT,
    headless: bool = True,
    timeout_ms: int = 30_000,
) -> Path:
    """Open *url*, screenshot, close. Returns the screenshot path."""
    session = await open_page(
        url, screenshot_path=screenshot_path, headless=headless, timeout_ms=timeout_ms
    )
    try:
        return session.screenshot
    finally:
        await session.close()


def _url_for_sport(sport: str) -> str:
    return f"{URL_BASE}/{sport.lower()}"


def _sport_from_url(url: str) -> str:
    m = re.search(r"/live-betting/([^/?#]+)", url)
    return m.group(1) if m else "unknown"


def _selection_label(col_label: str) -> str:
    return {
        "result_home": "home",
        "result_away": "away",
        "result_draw": "draw",
    }.get(col_label, col_label or "unknown")


def _safe_sport_slug(sport: str) -> str:
    """Sanitise a sport name for use in a filename."""
    return re.sub(r"[^A-Za-z0-9_-]+", "_", sport).strip("_") or "unknown"


def _save_snapshot(df: pd.DataFrame, sport: str, out_dir: Path) -> Path:
    """Append *df* to ``betsson_<sport>.parquet`` (creating it if missing).

    There is one file per (bookmaker, sport). Each scrape appends its rows;
    each row's ``scraped_at`` distinguishes snapshots over time.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    slug = _safe_sport_slug(sport)
    path = out_dir / f"betsson_{slug}.parquet"
    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df
    combined.to_parquet(path, index=False)
    return path


def _rows_to_df(
    raw: list[dict], sport_hint: str | None, page_url: str
) -> pd.DataFrame:
    """Pivot the event×cell extractor output into one row per event."""
    scraped_at = datetime.now(timezone.utc)
    sport = sport_hint or _sport_from_url(page_url)
    by_event: dict[str, dict] = {}

    for r in raw:
        try:
            odds = float(r["odd_value"])
        except (TypeError, ValueError):
            continue
        eid = r.get("event_id", "")
        if not eid:
            continue
        sel = _selection_label(r.get("col_label", ""))
        if sel not in {"home", "draw", "away"}:
            continue
        line = r.get("line") or ""
        if r.get("has_draw_col"):
            market = "1X2"
        elif line:
            market = "spread"
        else:
            market = "moneyline"

        ev = by_event.setdefault(
            eid,
            {
                "scraped_at": scraped_at,
                "sport": sport,
                "league": r.get("league", ""),
                "home_team": r.get("home", ""),
                "away_team": r.get("away", ""),
                "clock": r.get("clock", ""),
                "period": r.get("period", ""),
                "event_id": eid,
                "market": market,
                "home_odds": float("nan"),
                "draw_odds": float("nan"),
                "away_odds": float("nan"),
                "home_line": "",
                "away_line": "",
                "home_odd_id": "",
                "draw_odd_id": "",
                "away_odd_id": "",
            },
        )
        ev[f"{sel}_odds"] = odds
        ev[f"{sel}_odd_id"] = r.get("odd_id", "")
        if sel != "draw":
            ev[f"{sel}_line"] = line

    df = pd.DataFrame(list(by_event.values()), columns=_ODDS_COLUMNS)
    if df.empty:
        return df
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=True)
    for col in (
        "sport",
        "league",
        "home_team",
        "away_team",
        "clock",
        "period",
        "event_id",
        "market",
        "home_line",
        "away_line",
        "home_odd_id",
        "draw_odd_id",
        "away_odd_id",
    ):
        df[col] = df[col].astype("string")
    for col in ("home_odds", "draw_odds", "away_odds"):
        df[col] = df[col].astype("float64")
    return df.reset_index(drop=True)
