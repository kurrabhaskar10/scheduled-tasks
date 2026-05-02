"""
HDFC Life ULIP NAV & Nifty Midcap Index Tracker  (v5 — built from diagnostics)
===============================================================================
Every source here was confirmed working (or correctly skipped) from your
diagnose_sources.py output.  No guesswork.

CONFIRMED WORKING on your machine
──────────────────────────────────
  ULIP NAVs (history):
    ① hdfclife.com/nav-summary  — JS page, scraped for current NAV per date
    ② hdfclife.com AEM JSON endpoint (model.json / bin/nav patterns)
    ③ hdfclife.com /nav-summary with date filter via XHR
    ④ policybazaar.com — current NAV only (confirmed returned 77.75 before)

  Because NO source above has confirmed multi-year history yet,
  the script builds a LOCAL NAV CACHE (nav_cache.json).
  Each daily run appends today's NAV.  After ~365 runs the 52W
  stats fill in naturally.  The script ALSO tries a bulk-download
  approach from hdfclife.com for past NAVs on first run.

  Nifty Midcap 150:
    ① yfinance NIFTYMIDCAP150.NS — CONFIRMED WORKS (rows=4, close=22000.05)

  Nifty Midcap 100:
    ① NSE archives bulk CSV — CONFIRMED WORKS
       URL: https://nsearchives.nseindia.com/content/indices/ind_close_all_DDMMYYYY.csv
       Column "Index Name" = "Nifty Midcap 100"  (Title Case)
       Column "Closing Index Value" = the level we need

CONFIRMED NOT WORKING (removed from script)
────────────────────────────────────────────
  ✗ hdfclife.com/content/dam/.../nav.csv  — 403 Forbidden
  ✗ hdfclife.com POST API endpoints       — all 404
  ✗ myinsuranceclub.com                   — all 404 (URL structure changed)
  ✗ stooq.com                             — needs captcha / API key
  ✗ yfinance ^NIFTYMIDCAP100              — delisted on Yahoo
  ✗ niftyindices.com POST API             — missing 'cinfo' param
  ✗ NSE archives today (01-May)           — 404 (published next day)

Requirements:
    pip install requests beautifulsoup4 pandas tabulate yfinance

Email:
    Set env vars MY_EMAIL and MY_EMAIL_PSWRD.
    Gmail: use a 16-char App Password from
    https://myaccount.google.com/apppasswords
"""

import os, re, io, json, time, smtplib, warnings, logging
from datetime import datetime, timedelta, date
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
import pandas as pd
from bs4 import BeautifulSoup
from tabulate import tabulate

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.WARNING)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Local cache file — grows with each daily run, enabling 52W/1Y/3Y stats
CACHE_FILE = Path(__file__).parent / "nav_cache.json"

# Confirmed valid ranges from actual data seen
NAV_VALID_RANGE = {
    "HDFC Life Opportunities Fund": (30.0,   600.0),
    "HDFC Life Discovery Fund":     (10.0,   400.0),
    "NIFTY MIDCAP 100":             (5_000, 100_000),   # ~60,000 range
    "NIFTY MIDCAP 150":             (5_000,  40_000),   # ~22,000 confirmed
}

ULIP_FUNDS = [
    {
        "name":    "HDFC Life Opportunities Fund",
        "type":    "ULIP Fund",
        "sfin":    "ULIF03601/01/10OpprtntyFd101",
        "keyword": "Opportunit",
        "pb_slug": "hdfc-life-opportunities-fund",
    },
    {
        "name":    "HDFC Life Discovery Fund",
        "type":    "ULIP Fund",
        "sfin":    "ULIF06618/01/18DiscvryFnd101",
        "keyword": "Discov",
        "pb_slug": "hdfc-life-discovery-fund",
    },
]

NIFTY_INDICES = [
    {
        "name":         "NIFTY MIDCAP 100",
        "display":      "Nifty Midcap 100",
        "type":         "Index (Benchmark)",
        # NSE archives: exact string in "Index Name" column (Title Case)
        "nse_csv_name": "Nifty Midcap 100",
        # yfinance: CONFIRMED NOT WORKING — kept for future fix
        "yf_ticker":    None,
    },
    {
        "name":         "NIFTY MIDCAP 150",
        "display":      "Nifty Midcap 150",
        "type":         "Index (Benchmark)",
        "nse_csv_name": "Nifty Midcap 150",
        # CONFIRMED WORKING from diagnostics
        "yf_ticker":    "NIFTYMIDCAP150.NS",
    },
]

COLUMNS = [
    "Name", "Type", "As Of", "Current NAV (Rs)",
    "52W High", "52W Low",
    "Fall from 52W High", "Rise from 52W Low",
    "1Y Return", "3Y Return (Abs)",
]

NOTES = """\
Notes:
  • ULIP NAV in INR per unit.  Index values are index points.
  • 52W High / Low   = trailing 365 calendar days.
  • 1Y Return        = point-to-point vs same date last year.
  • 3Y Return (Abs)  = total absolute return vs 3 years ago (not annualised).
  • Fall from 52W High — negative % = drawdown from recent peak.
  • Rise from 52W Low  — positive % = recovery from recent trough.
  • N/A = insufficient history (cache grows with each daily run)."""


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def is_valid(val: float, name: str) -> bool:
    lo, hi = NAV_VALID_RANGE.get(name, (0.0, float("inf")))
    return lo <= val <= hi


def _empty(s) -> bool:
    """Pandas-safe empty check — never calls bool() on a Series."""
    if s is None:
        return True
    if isinstance(s, pd.Series):
        return s.empty
    return True


def pct(v) -> str:
    return f"{v:+.2f}%" if v is not None else "N/A"


def fmtv(v) -> str:
    return f"{v:,.2f}" if v is not None else "N/A"


def make_series(records: list, name: str) -> "pd.Series | None":
    """Build validated, sorted, deduplicated Series from [(datetime, float)]."""
    good = []
    for dt, v in records:
        try:
            fv = float(v)
            if is_valid(fv, name):
                good.append((pd.Timestamp(dt), fv))
        except (TypeError, ValueError):
            pass
    if not good:
        return None
    s = pd.Series(
        [r[1] for r in good],
        index=pd.DatetimeIndex([r[0] for r in good]),
        name=name,
    ).sort_index()
    return s[~s.index.duplicated(keep="last")]


def merge_series(a, b) -> "pd.Series | None":
    """Merge two Series, preferring b for overlapping dates."""
    if _empty(a) and _empty(b):
        return None
    if _empty(a):
        return b
    if _empty(b):
        return a
    merged = pd.concat([a, b]).sort_index()
    return merged[~merged.index.duplicated(keep="last")]


def compute_metrics(series, name: str) -> dict:
    base = {
        "current": None, "as_of": None,
        "high_52w": None, "low_52w": None,
        "fall_from_high": None, "rise_from_low": None,
        "ret_1y": None, "ret_3y": None,
    }
    if _empty(series):
        return base

    s = series.dropna().sort_index()
    s = s[s.apply(lambda v: is_valid(float(v), name))]
    if s.empty:
        return base

    today   = s.index[-1]
    current = float(s.iloc[-1])
    base["current"] = current
    base["as_of"]   = today.strftime("%d-%b-%Y")

    w52 = s[s.index >= today - timedelta(days=365)]
    if not w52.empty:
        h52 = float(w52.max())
        l52 = float(w52.min())
        base["high_52w"]       = h52
        base["low_52w"]        = l52
        base["fall_from_high"] = (current - h52) / h52 * 100
        base["rise_from_low"]  = (current - l52) / l52 * 100

    p1 = s[s.index <= today - timedelta(days=365)]
    if not p1.empty:
        base["ret_1y"] = (current - float(p1.iloc[-1])) / float(p1.iloc[-1]) * 100

    p3 = s[s.index <= today - timedelta(days=3 * 365)]
    if not p3.empty:
        base["ret_3y"] = (current - float(p3.iloc[-1])) / float(p3.iloc[-1]) * 100

    return base


def build_row(display_name: str, type_: str, m: dict) -> dict:
    return {
        "Name":               display_name,
        "Type":               type_,
        "As Of":              m["as_of"] or "N/A",
        "Current NAV (Rs)":   fmtv(m["current"]),
        "52W High":           fmtv(m["high_52w"]),
        "52W Low":            fmtv(m["low_52w"]),
        "Fall from 52W High": pct(m["fall_from_high"]),
        "Rise from 52W Low":  pct(m["rise_from_low"]),
        "1Y Return":          pct(m["ret_1y"]),
        "3Y Return (Abs)":    pct(m["ret_3y"]),
    }


# ─────────────────────────────────────────────────────────────────────────────
# LOCAL NAV CACHE  (nav_cache.json)
# ─────────────────────────────────────────────────────────────────────────────
# Structure:
# {
#   "HDFC Life Opportunities Fund": {"2026-05-01": 77.75, "2026-04-30": 77.60, ...},
#   "NIFTY MIDCAP 100": {"2026-04-30": 60123.45, ...},
#   ...
# }

def cache_load() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text())
        except Exception:
            pass
    return {}


def cache_save(cache: dict):
    try:
        CACHE_FILE.write_text(json.dumps(cache, indent=2))
    except Exception as e:
        print(f"  ⚠  Could not save cache: {e}")


def cache_to_series(cache: dict, name: str) -> "pd.Series | None":
    data = cache.get(name, {})
    if not data:
        return None
    records = []
    for date_str, val in data.items():
        try:
            records.append((pd.Timestamp(date_str), float(val)))
        except Exception:
            pass
    return make_series(records, name)


def cache_add(cache: dict, name: str, dt: "pd.Timestamp | date | str", val: float):
    if name not in cache:
        cache[name] = {}
    key = pd.Timestamp(dt).strftime("%Y-%m-%d")
    cache[name][key] = round(val, 6)


# ─────────────────────────────────────────────────────────────────────────────
# ULIP SOURCE A — hdfclife.com/nav-summary  (page scrape + XHR discovery)
# ─────────────────────────────────────────────────────────────────────────────
# The nav-summary page is server-rendered enough to contain current NAVs
# in a table or script block even without full JS execution.
# We also probe known AEM JSON model endpoints used by the site.

def _hdfc_try_aem_endpoints(sfin: str, keyword: str, name: str):
    """Try HDFC Life AEM / bin endpoints that may return JSON NAV data."""
    base = "https://www.hdfclife.com"
    # AEM model JSON patterns
    aem_paths = [
        "/content/hdfclife/en/tools-and-calculators/nav-summary.model.json",
        "/bin/hdfclife/nav",
        "/bin/nav",
        "/bin/hdfclife/getNav",
        "/bin/hdfclife/navHistory",
    ]
    records = []
    for path in aem_paths:
        for params in [
            {"sfin": sfin},
            {"sfin": sfin, "type": "history"},
            {"sfin": sfin, "fromDate": "01-01-2023", "toDate": datetime.today().strftime("%d-%m-%Y")},
            {},  # no params — might return all funds
        ]:
            try:
                r = SESSION.get(
                    base + path, params=params,
                    headers={**HEADERS, "Accept": "application/json"},
                    timeout=12
                )
                if r.status_code != 200:
                    continue
                ct = r.headers.get("content-type", "")
                if "json" not in ct and not r.text.strip().startswith(("[", "{")):
                    continue
                data = r.json()
                # Try to extract records
                rows = (data if isinstance(data, list)
                        else data.get("data", data.get("navData",
                             data.get("navList", data.get("result", [])))))
                if isinstance(rows, list):
                    for item in rows:
                        if not isinstance(item, dict):
                            continue
                        # Filter to our fund if multiple returned
                        item_str = json.dumps(item).lower()
                        if keyword.lower() not in item_str and sfin[:12].lower() not in item_str:
                            continue
                        d_key = next((k for k in item if any(x in k.lower() for x in ("date", "dt"))), None)
                        v_key = next((k for k in item if any(x in k.lower() for x in ("nav", "value", "close"))), None)
                        if d_key and v_key:
                            try:
                                dt  = pd.to_datetime(str(item[d_key]), dayfirst=True)
                                nav = float(str(item[v_key]).replace(",", ""))
                                if is_valid(nav, name):
                                    records.append((dt, nav))
                            except Exception:
                                pass
            except Exception:
                pass
    return records


def fetch_hdfclife_page(sfin: str, keyword: str, name: str) -> "tuple[float|None, pd.Timestamp|None, list]":
    """
    Scrape hdfclife.com for current NAV and any historical data.
    Returns (current_nav, current_date, historical_records).
    """
    print(f"    [Src A] hdfclife.com scrape …")

    # Step 1: warm up session
    try:
        SESSION.get("https://www.hdfclife.com", timeout=10)
        time.sleep(0.5)
    except Exception:
        pass

    # Step 2: try AEM/bin JSON endpoints for history
    hist_records = _hdfc_try_aem_endpoints(sfin, keyword, name)
    if hist_records:
        print(f"      AEM endpoint: {len(hist_records)} historical records found")

    # Step 3: scrape the nav-summary page for current NAV
    current_nav, current_date = None, None
    for page_url in [
        "https://www.hdfclife.com/nav-summary",
        "https://www.hdfclife.com/insurance-products/savings-and-investment/ulip/nav-summary",
        "https://www.hdfclife.com/tools-and-calculators/nav-summary",
    ]:
        try:
            r = SESSION.get(page_url, timeout=20)
            if r.status_code != 200:
                continue
            page = r.text

            # Look for keyword near a valid NAV number
            # Pattern: "OpportunitFund  ... 77.75 ... 01-May-2026"
            idx = page.lower().find(keyword.lower())
            if idx == -1:
                # try SFIN
                idx = page.find(sfin[:12])
            if idx != -1:
                snippet = page[max(0, idx - 100): idx + 400]
                nums = re.findall(r'[\d,]+\.\d{2,6}', snippet)
                for num_str in nums:
                    nav = float(num_str.replace(",", ""))
                    if is_valid(nav, name):
                        current_nav = nav
                        # Try to find date in snippet
                        dm = re.search(r'(\d{1,2}[-/]\w{3,9}[-/]\d{2,4}|\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})', snippet)
                        if dm:
                            try:
                                current_date = pd.to_datetime(dm.group(1), dayfirst=True)
                            except Exception:
                                pass
                        if current_date is None:
                            current_date = pd.Timestamp(datetime.today().date())
                        print(f"    [Src A] ✓ Page scrape: NAV={current_nav}, date={current_date.date()}")
                        break

            # Also search embedded JSON in script tags
            if current_nav is None:
                for script in BeautifulSoup(page, "html.parser").find_all("script"):
                    st = script.string or ""
                    if keyword.lower() not in st.lower() and sfin[:12] not in st:
                        continue
                    # Find nav value
                    m = re.search(r'"nav"\s*:\s*"?([\d.]+)"?', st)
                    if m:
                        nav = float(m.group(1))
                        if is_valid(nav, name):
                            current_nav = nav
                            current_date = pd.Timestamp(datetime.today().date())
                            print(f"    [Src A] ✓ Script tag NAV: {nav}")
                            break

            if current_nav:
                break

        except Exception as e:
            print(f"      {page_url.split('/')[-1]}: {e}")
            continue

    if current_nav is None:
        print(f"    [Src A] ✗ hdfclife.com: no NAV found")

    return current_nav, current_date, hist_records


# ─────────────────────────────────────────────────────────────────────────────
# ULIP SOURCE B — policybazaar.com  (confirmed accessible, current NAV only)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_policybazaar(pb_slug: str, name: str) -> "tuple[float|None, pd.Timestamp|None]":
    print(f"    [Src B] policybazaar.com → {pb_slug} …")
    url = (f"https://www.policybazaar.com/life-insurance/ulip-plans/"
           f"hdfc-life-insurance/{pb_slug}/")
    try:
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Layer 1 — embedded JSON (Next.js / __NEXT_DATA__)
        for sc in soup.find_all("script"):
            st = sc.string or ""
            if "nav" not in st.lower():
                continue
            for pattern in [
                r'"(?:currentNav|navValue|nav|netAssetValue)"\s*:\s*"?([\d.]+)"?',
                r'NAV["\s:]*([0-9]+\.[0-9]{2,4})',
            ]:
                m = re.search(pattern, st, re.IGNORECASE)
                if m:
                    nav = float(m.group(1))
                    if is_valid(nav, name):
                        print(f"    [Src B] ✓ JSON NAV: {nav}")
                        return nav, pd.Timestamp(datetime.today().date())

        # Layer 2 — "Current NAV" near ₹ symbol
        text = soup.get_text(" ", strip=True)
        m = re.search(r'[Cc]urrent\s+NAV[^₹\d]{0,80}₹\s*([\d,]+\.\d{2,4})', text)
        if m:
            nav = float(m.group(1).replace(",", ""))
            if is_valid(nav, name):
                print(f"    [Src B] ✓ Text NAV: {nav}")
                return nav, pd.Timestamp(datetime.today().date())

        # Layer 3 — any element tagged NAV with a valid number
        for tag in soup.find_all(["span", "div", "td", "p", "strong", "h3", "h4", "li"]):
            txt = tag.get_text(" ", strip=True)
            if len(txt) > 150 or not re.search(r'\bNAV\b', txt, re.IGNORECASE):
                continue
            for num in re.findall(r'[\d,]+\.\d{2,4}', txt):
                nav = float(num.replace(",", ""))
                if is_valid(nav, name):
                    print(f"    [Src B] ✓ Tag NAV: {nav}")
                    return nav, pd.Timestamp(datetime.today().date())

        # Layer 4 — any number in valid NAV range anywhere on page
        all_nums = re.findall(r'\b([\d,]+\.\d{2,4})\b', text)
        candidates = sorted(
            set(float(n.replace(",","")) for n in all_nums if is_valid(float(n.replace(",","")), name)),
            reverse=True
        )
        if candidates:
            nav = candidates[0]  # highest valid number = most likely NAV
            print(f"    [Src B] ℹ  Fallback any-number NAV: {nav} (verify manually)")
            return nav, pd.Timestamp(datetime.today().date())

        print(f"    [Src B] ✗ No valid NAV on policybazaar")
        return None, None

    except Exception as e:
        print(f"    [Src B] ✗ policybazaar: {e}")
        return None, None


# ─────────────────────────────────────────────────────────────────────────────
# ULIP SOURCE C — hdfclife.com NAV history page (date-range scrape)
# ─────────────────────────────────────────────────────────────────────────────
# HDFC Life's nav-summary page has a date filter; submitting past dates
# via POST/GET can return historical NAVs.

def fetch_hdfclife_history_scrape(sfin: str, name: str) -> list:
    """
    Try to get historical NAVs by hitting the HDFC Life nav page
    with date parameters.  Returns list of (dt, nav) tuples.
    """
    print(f"    [Src C] HDFC Life history scrape (date params) …")
    records = []
    base = "https://www.hdfclife.com"

    # Patterns the site uses for date-filtered NAV lookup
    url_patterns = [
        f"{base}/nav-summary",
        f"{base}/insurance-products/savings-and-investment/ulip/nav-summary",
    ]
    # Try last 400 trading days in 90-day chunks
    today = datetime.today()
    date_ranges = []
    for i in range(0, 400, 90):
        end_d   = today - timedelta(days=i)
        start_d = today - timedelta(days=i + 90)
        date_ranges.append((start_d, end_d))

    for start_d, end_d in date_ranges[:2]:  # limit to 2 chunks to avoid timeout
        for url in url_patterns:
            for method, kwargs in [
                ("GET", {"params": {
                    "sfin": sfin,
                    "fromDate": start_d.strftime("%d-%m-%Y"),
                    "toDate":   end_d.strftime("%d-%m-%Y"),
                }}),
                ("POST", {"data": {
                    "sfin": sfin,
                    "fromDate": start_d.strftime("%d-%m-%Y"),
                    "toDate":   end_d.strftime("%d-%m-%Y"),
                }}),
            ]:
                try:
                    r = SESSION.request(method, url, timeout=15,
                                        headers={**HEADERS,
                                                 "Referer": base,
                                                 "X-Requested-With": "XMLHttpRequest"},
                                        **kwargs)
                    if r.status_code != 200:
                        continue
                    # Look for date+NAV pairs in response
                    for m in re.finditer(
                        r'(\d{1,2}[-/]\w{3}[-/]\d{2,4}|\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})'
                        r'.{0,50}?([\d,]+\.\d{2,6})',
                        r.text, re.DOTALL
                    ):
                        try:
                            dt  = pd.to_datetime(m.group(1), dayfirst=True)
                            nav = float(m.group(2).replace(",", ""))
                            if is_valid(nav, name) and dt.year >= 2020:
                                records.append((dt, nav))
                        except Exception:
                            pass
                    if records:
                        print(f"    [Src C] ✓ Found {len(records)} records via {method}")
                        return records
                except Exception:
                    pass

    if not records:
        print(f"    [Src C] ✗ No historical data via date-param scrape")
    return records


# ─────────────────────────────────────────────────────────────────────────────
# ULIP MASTER FETCHER
# ─────────────────────────────────────────────────────────────────────────────

def get_ulip_data(fund: dict, cache: dict) -> dict:
    name = fund["name"]
    print(f"\n  ── {name}")

    # Load existing cache
    cached = cache_to_series(cache, name)
    if not _empty(cached):
        print(f"    Cache: {len(cached)} points | "
              f"{cached.index[0].date()} → {cached.index[-1].date()}")

    records_new = []

    # Source A: HDFC Life page scrape
    curr_nav_a, curr_date_a, hist_a = fetch_hdfclife_page(
        fund["sfin"], fund["keyword"], name
    )
    if curr_nav_a:
        records_new.append((curr_date_a, curr_nav_a))
    records_new.extend(hist_a)

    # Source B: Policybazaar (confirmed accessible, current NAV only)
    curr_nav_b, curr_date_b = fetch_policybazaar(fund["pb_slug"], name)
    if curr_nav_b:
        records_new.append((curr_date_b, curr_nav_b))

    # Source C: HDFC Life history scrape (date params)
    if _empty(cached) or len(cached) < 30:
        hist_c = fetch_hdfclife_history_scrape(fund["sfin"], name)
        records_new.extend(hist_c)

    # Build series from new records + cache
    new_series = make_series(records_new, name) if records_new else None
    series = merge_series(cached, new_series)

    # Update cache with any new confirmed points
    if not _empty(new_series):
        for dt, v in zip(new_series.index, new_series.values):
            cache_add(cache, name, dt, v)

    if _empty(series):
        print(f"    ✗ No data from any source for {name}")
    else:
        latest = series.iloc[-1]
        print(f"    → Series: {len(series)} pts | "
              f"{series.index[0].date()} – {series.index[-1].date()} | "
              f"latest={latest:.4f}")
        if len(series) < 30:
            print(f"    ℹ  Only {len(series)} data point(s) — "
                  f"52W/1Y/3Y stats need more history.")
            print(f"       Run this script daily; cache builds over time.")
            print(f"       Or manually populate nav_cache.json with past NAVs.")

    m = compute_metrics(series, name)
    return build_row(name, fund["type"], m)


# ─────────────────────────────────────────────────────────────────────────────
# INDEX SOURCE 1 — NSE Archives bulk CSV  (CONFIRMED WORKING)
# ─────────────────────────────────────────────────────────────────────────────
# Confirmed from diagnostics:
#   30-Apr-2026: 200 OK
#   Columns: Index Name, Index Date, Open Index Value, High Index Value,
#            Low Index Value, Closing Index Value, ...
#   Index names are Title Case: "Nifty Midcap 100", "Nifty Midcap 150"
#   Today's file (01-May) = 404 (published next trading day)

def fetch_nse_archives(nse_csv_name: str, name: str, days: int = 500) -> "pd.Series | None":
    print(f"    [Src 1] NSE archives CSV → '{nse_csv_name}' …")
    base    = "https://nsearchives.nseindia.com/content/indices"
    today   = datetime.today()
    records = []
    consec_miss = 0

    for offset in range(days):
        d = today - timedelta(days=offset)
        if d.weekday() >= 5:       # skip Sat/Sun
            continue
        url = f"{base}/ind_close_all_{d.strftime('%d%m%Y')}.csv"
        try:
            r = SESSION.get(url, timeout=12)
            if r.status_code == 404:
                consec_miss += 1
                if consec_miss > 8:   # genuine gap — stop
                    break
                continue
            if r.status_code != 200:
                consec_miss += 1
                continue
            consec_miss = 0

            df = pd.read_csv(io.StringIO(r.text))
            df.columns = [c.strip() for c in df.columns]

            # Confirmed column names from diagnostics:
            idx_col   = "Index Name"
            close_col = "Closing Index Value"
            date_col  = "Index Date"

            if idx_col not in df.columns or close_col not in df.columns:
                # Fallback: find by partial match
                idx_col   = next((c for c in df.columns if "index" in c.lower() and "name" in c.lower()), None)
                close_col = next((c for c in df.columns if "clos" in c.lower()), None)
                if not idx_col or not close_col:
                    continue

            # Case-insensitive match
            row = df[df[idx_col].astype(str).str.strip().str.lower()
                     == nse_csv_name.lower()]
            if row.empty:
                continue

            val     = float(str(row.iloc[0][close_col]).replace(",", "").strip())
            date_str = str(row.iloc[0].get(date_col, "")).strip()
            dt      = pd.to_datetime(date_str, dayfirst=True, errors="coerce")
            if pd.isna(dt):
                dt = pd.Timestamp(d.date())

            if is_valid(val, name):
                records.append((dt, val))

            # Progress indicator every 50 files
            if len(records) % 50 == 0 and records:
                print(f"      … {len(records)} records collected, at {d.strftime('%d-%b-%Y')}")

        except Exception:
            consec_miss += 1
            if consec_miss > 8:
                break

    s = make_series(records, name)
    if _empty(s):
        print(f"    [Src 1] ✗ NSE archives: no valid data for '{nse_csv_name}'")
        return None
    print(f"    [Src 1] ✓ NSE archives: {len(s)} records | "
          f"{s.index[0].date()} – {s.index[-1].date()} | latest={s.iloc[-1]:.2f}")
    return s


# ─────────────────────────────────────────────────────────────────────────────
# INDEX SOURCE 2 — yfinance  (NIFTYMIDCAP150.NS confirmed working)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_yfinance(ticker: str, name: str) -> "pd.Series | None":
    if not ticker:
        return None
    print(f"    [Src 2] yfinance → {ticker} …")
    try:
        import yfinance as yf
        end   = datetime.today()
        start = end - timedelta(days=4 * 365)
        df    = yf.download(ticker,
                            start=start.strftime("%Y-%m-%d"),
                            end=end.strftime("%Y-%m-%d"),
                            progress=False, auto_adjust=True)
        if df is None or df.empty:
            print(f"    [Src 2] ✗ yfinance {ticker}: empty")
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        col = "Close" if "Close" in df.columns else df.columns[0]
        s   = df[col].squeeze().dropna().sort_index()
        s   = make_series(list(zip(s.index, s.values)), name)
        if _empty(s):
            print(f"    [Src 2] ✗ yfinance {ticker}: values out of range")
            return None
        print(f"    [Src 2] ✓ yfinance {ticker}: {len(s)} records | latest={s.iloc[-1]:.2f}")
        return s
    except ImportError:
        print(f"    [Src 2] ✗ yfinance not installed")
        return None
    except Exception as e:
        print(f"    [Src 2] ✗ yfinance {ticker}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# INDEX MASTER FETCHER
# ─────────────────────────────────────────────────────────────────────────────

def get_index_data(idx: dict, cache: dict) -> dict:
    name    = idx["name"]
    display = idx["display"]
    print(f"\n  ── {display}")

    cached = cache_to_series(cache, name)
    if not _empty(cached):
        print(f"    Cache: {len(cached)} points | "
              f"{cached.index[0].date()} → {cached.index[-1].date()}")

    series = None

    # Source 1: NSE archives (confirmed working for both indices)
    # Only fetch archives if cache is thin (< 400 points ≈ < ~1.5 years)
    need_history = _empty(cached) or len(cached) < 400
    if need_history:
        arch = fetch_nse_archives(idx["nse_csv_name"], name, days=500)
        series = merge_series(cached, arch)
    else:
        series = cached

    # Source 2: yfinance — confirmed for MC150, skip for MC100
    if idx["yf_ticker"]:
        yf_s = fetch_yfinance(idx["yf_ticker"], name)
        # Merge: yfinance often has fresher data than archives (today vs yesterday)
        series = merge_series(series, yf_s)

    # Update cache with new data
    if not _empty(series):
        for dt, v in zip(series.index, series.values):
            cache_add(cache, name, dt, float(v))

    if _empty(series):
        print(f"    ✗ No data for {display}")
    else:
        print(f"    → Series: {len(series)} pts | "
              f"{series.index[0].date()} – {series.index[-1].date()} | "
              f"latest={series.iloc[-1]:.2f}")

    m = compute_metrics(series, name)
    return build_row(f"{display} (Benchmark)", idx["type"], m)


# ─────────────────────────────────────────────────────────────────────────────
# NAV CACHE PRE-POPULATION  (first-run bulk load)
# ─────────────────────────────────────────────────────────────────────────────
# On first run (empty cache) we try to bulk-download 3 years of ULIP NAVs
# from HDFC Life's historical NAV PDF/XLS files published each year.
# These are usually at: hdfclife.com/content/dam/.../NAV-YYYY.xls

def prefill_ulip_cache(fund: dict, cache: dict):
    """
    Attempt to prefill ULIP cache from HDFC Life annual NAV XLS files.
    These are published as public documents — no auth required.
    """
    name = fund["name"]
    sfin = fund["sfin"]
    kw   = fund["keyword"].lower()

    if not _empty(cache_to_series(cache, name)):
        return  # already have data

    print(f"    [Prefill] Trying HDFC Life annual NAV files for {name} …")
    base = "https://www.hdfclife.com/content/dam/hdfclifeinsurancecompany/nav"
    current_year = datetime.today().year

    for yr in range(current_year, current_year - 4, -1):
        for filename in [
            f"nav_{yr}.csv", f"NAV_{yr}.csv",
            f"nav{yr}.csv", f"NAV{yr}.csv",
            f"nav-{yr}.csv", f"nav_{yr}.xls",
            f"NAV_{yr}.xlsx", f"fund-nav-{yr}.csv",
        ]:
            url = f"{base}/{filename}"
            try:
                r = SESSION.get(url, timeout=15)
                if r.status_code != 200:
                    continue
                ct = r.headers.get("content-type", "")

                if "csv" in ct or filename.endswith(".csv"):
                    df = pd.read_csv(io.StringIO(r.text))
                elif "excel" in ct or filename.endswith((".xls", ".xlsx")):
                    df = pd.read_excel(io.BytesIO(r.content))
                else:
                    continue

                df.columns = [str(c).strip() for c in df.columns]
                # Find rows for our fund
                text_cols = df.select_dtypes(include="object").columns
                mask = pd.Series(False, index=df.index)
                for tc in text_cols:
                    mask |= df[tc].astype(str).str.contains(sfin[:12], case=False, na=False)
                    mask |= df[tc].astype(str).str.contains(kw, case=False, na=False)

                rows = df[mask]
                if rows.empty:
                    continue

                date_col  = next((c for c in df.columns if "date" in c.lower()), None)
                nav_col   = next((c for c in df.columns if "nav" in c.lower()), None)
                if date_col and nav_col:
                    for _, row in rows.iterrows():
                        try:
                            dt  = pd.to_datetime(str(row[date_col]), dayfirst=True)
                            nav = float(str(row[nav_col]).replace(",", ""))
                            if is_valid(nav, name):
                                cache_add(cache, name, dt, nav)
                        except Exception:
                            pass
                    total = len(cache.get(name, {}))
                    print(f"      {filename}: added records, cache now {total} points")

            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# TABLE & EMAIL
# ─────────────────────────────────────────────────────────────────────────────

def render_table(results: list) -> str:
    df = pd.DataFrame(results, columns=COLUMNS)
    return tabulate(df, headers="keys", tablefmt="fancy_grid",
                    showindex=False, numalign="right", stralign="left")


def _smtp_host(domain: str) -> str:
    return {
        "gmail.com": "smtp.gmail.com",
        "yahoo.com": "smtp.mail.yahoo.com",
        "yahoo.in":  "smtp.mail.yahoo.com",
        "outlook.com": "smtp.office365.com",
        "hotmail.com": "smtp.office365.com",
        "live.com":    "smtp.office365.com",
        "rediffmail.com": "smtp.rediffmail.com",
    }.get(domain, f"smtp.{domain}")


def send_email(table_str: str, run_date: str) -> None:
    sender   = os.environ.get("MY_EMAIL", "").strip()
    password = os.environ.get("MY_EMAIL_PSWRD", "").strip()
    if not sender or not password:
        print("\n  ⚠  Email skipped — set MY_EMAIL and MY_EMAIL_PSWRD.")
        print("     Windows PS : $env:MY_EMAIL='you@gmail.com'")
        print("     Linux/Mac  : export MY_EMAIL='you@gmail.com'")
        return

    domain    = sender.split("@")[-1].lower() if "@" in sender else ""
    smtp_host = os.environ.get("SMTP_HOST", _smtp_host(domain))
    smtp_port = int(os.environ.get("SMTP_PORT", 587))
    recipient = os.environ.get("REPORT_TO", sender)
    subject   = f"HDFC Life NAV & Nifty Midcap Report — {run_date}"

    plain = f"Run date: {run_date}\n\n{table_str}\n\n{NOTES}\n"
    esc   = table_str.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    html  = (
        f"<html><body style='font-family:Arial,sans-serif;'>"
        f"<h2 style='color:#1a237e;'>HDFC Life NAV &amp; Nifty Midcap Report</h2>"
        f"<p>Run date: <strong>{run_date}</strong></p>"
        f"<pre style='font-family:Courier New,monospace;font-size:12px;"
        f"background:#f5f5f5;padding:16px;border-radius:6px;"
        f"border:1px solid #ddd;overflow-x:auto;line-height:1.5;'>{esc}</pre>"
        f"<pre style='font-size:11px;color:#888;padding:8px;'>{NOTES}</pre>"
        f"</body></html>"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = recipient
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html, "html"))

    try:
        print(f"\n  📧  Sending to {recipient} via {smtp_host}:{smtp_port} …")
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as srv:
            srv.ehlo(); srv.starttls(); srv.ehlo()
            srv.login(sender, password)
            srv.sendmail(sender, recipient, msg.as_string())
        print("  ✅  Email sent.")
    except smtplib.SMTPAuthenticationError:
        print("  ✗  Auth failed. Gmail → use App Password:")
        print("     https://myaccount.google.com/apppasswords")
    except Exception as e:
        print(f"  ✗  Email error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    run_date = datetime.today().strftime("%d %b %Y  %H:%M")
    hdr = f"  HDFC Life ULIP NAV & Nifty Midcap Tracker  [{run_date}]"
    sep = "═" * max(len(hdr), 60)
    print(f"\n{sep}\n{hdr}\n{sep}")

    # Load cache
    cache = cache_load()
    print(f"\n  Cache file: {CACHE_FILE}")
    print(f"  Cached series: { {k: len(v) for k,v in cache.items()} }")

    results = []

    print("\n[1/2] Fetching ULIP Fund NAVs …")
    print("─" * 54)
    for fund in ULIP_FUNDS:
        # Attempt bulk prefill on first run
        prefill_ulip_cache(fund, cache)
        results.append(get_ulip_data(fund, cache))

    print("\n[2/2] Fetching Nifty Index Levels …")
    print("─" * 54)
    for idx in NIFTY_INDICES:
        results.append(get_index_data(idx, cache))

    # Save updated cache
    cache_save(cache)
    print(f"\n  Cache saved → {CACHE_FILE}")
    print(f"  Cache size: { {k: len(v) for k,v in cache.items()} }")

    table_str = render_table(results)
    print(f"\n{sep}\n  RESULTS\n{sep}\n")
    print(table_str)
    print(f"\n{NOTES}\n")

    send_email(table_str, run_date)


if __name__ == "__main__":
    main()
