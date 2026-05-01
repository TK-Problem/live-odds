# live-odds

Load `https://www.betsson.lt/en/live-betting` in headless Chromium via Playwright and screenshot it. Foundation for adding scrapers per bookmaker.

## Setup

```bash
uv sync
uv run playwright install chromium
# Linux/WSL only — install system libs Chromium needs (sudo):
sudo uv run playwright install-deps chromium
```

Then open `test_betsson.ipynb` and run the cell.

## Layout

```
scrapers/
  betsson/
    page.py          # load_page() — open URL, screenshot, return path
test_betsson.ipynb   # one cell: load + display screenshot
data/                # screenshots
```

Add new bookmakers as sibling packages under `scrapers/` (e.g. `scrapers/topsport/`).

## Known limitations

- **Geo-block.** `betsson.lt` is restricted to Lithuanian IPs. From elsewhere the page may redirect or render blank — connect via an LT VPN.
- **Headless on WSL2.** Default is `headless=True`. If you have WSLg / an X server, pass `headless=False` for a visible browser window.
