"""
HDFC Life ULIP NAV & Nifty Midcap Index Tracker  (v7 — with XIRR SIP columns)
===============================================================================
Sources confirmed via web search on 02-May-2026:

ULIP NAVs — hdfclife.com/fund-performance  ← PRIMARY (confirmed working)
─────────────────────────────────────────────────────────────────────────────
  The page embeds ALL fund data as structured text in its JS bundle / SSR:
    ULIF03601/01/10OpprtntyFd101 · NAV · 76.7876 · 2026-04-24 ·
      52 wk High · 2025-11-17 · 79.8503 · 52 wk Low · 2026-03-31 · 69.1143
    ULIF06618/01/18DiscvryFnd101 · NAV · 42.637 · 2026-04-24 ·
      52 wk High · 2026-02-11 · 43.3264 · 52 wk Low · 2025-05-09 · 37.8902

  Pattern repeats 3× per fund (HDFC Life embeds data in multiple script
  blocks). We grab it once and have current NAV + 52W High + 52W Low with
  their dates — no historical series needed for these three metrics.

  For 1Y / 3Y returns we use myinsuranceclub.com Tabular View (confirmed
  accessible, confirmed has historical NAV table for Opportunities Fund).
  We also scrape hdfclife.com/fund-performance for the 1Y return % it
  displays directly (-11.63% for Opportunities, 13.26% for Discovery).

  Fallback: policybazaar.com (current NAV only).

Nifty Indices
─────────────────────────────────────────────────────────────────────────────
  Midcap 150 : yfinance  NIFTYMIDCAP150.NS     ← CONFIRMED WORKS
  Midcap 100 : NSE archives bulk CSV            ← CONFIRMED WORKS
               https://nsearchives.nseindia.com/content/indices/
               ind_close_all_DDMMYYYY.csv
               Column "Index Name"           = "Nifty Midcap 100"
               Column "Closing Index Value"  = level

Requirements:
    pip install requests beautifulsoup4 pandas tabulate yfinance pyxirr

Email:
    Set MY_EMAIL and MY_EMAIL_PSWRD env vars.
    Gmail → use a 16-char App Password from
    https://myaccount.google.com/apppasswords
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────────────
# Python's built-in standard-library modules — no install needed.
#   os       : read environment variables (email credentials, SMTP settings)
#   re       : regular expressions — search/extract text patterns from web pages
#   io       : convert raw text strings into file-like objects (for pd.read_csv)
#   json     : read and write the local NAV cache file (plain JSON on disk)
#   time     : sleep/pause between web requests to avoid being rate-limited
#   smtplib  : send emails directly over SMTP (Gmail, Outlook, etc.)
#   warnings : suppress noisy third-party library warnings in console output
#   logging  : control how much debug noise libraries like yfinance print
import os, re, io, json, time, smtplib, warnings, logging

#   datetime  : work with dates (e.g. "today minus 1 year")
#   timedelta : represent a duration, e.g. timedelta(days=365)
#   Path      : cross-platform file paths (works on Windows, Mac, Linux)
from datetime import datetime, timedelta
from pathlib import Path

#   MIMEText      : wrap a text/plain or text/html body inside an email
#   MIMEMultipart : build a multi-part email (plain-text + HTML alternative)
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ── Third-party libraries (install with: pip install ...) ────────────────────
#   requests     : download web pages / HTTP GET requests
#   pandas (pd)  : DataFrames and time-series — the workhorse for all data work
#   BeautifulSoup: parse HTML pages and extract structured data from them
import requests
import pandas as pd
from bs4 import BeautifulSoup

# tabulate: pretty-print data as a formatted text table in the console.
# Wrapped in try/except so the script still works if it isn't installed —
# it will fall back to a simpler ASCII table (see render_table below).
try:
    from tabulate import tabulate
    _TABULATE_AVAILABLE = True
except ImportError:
    _TABULATE_AVAILABLE = False
    tabulate = None

# pyxirr: calculate XIRR (Extended Internal Rate of Return) for SIP scenarios.
# XIRR is the standard way to measure annualised return when cash flows happen
# at irregular (or regular) intervals — exactly what a monthly SIP produces.
# Wrapped in try/except so the rest of the script still runs if not installed.
try:
    from pyxirr import xirr as _pyxirr_xirr
    _XIRR_AVAILABLE = True
except ImportError:
    _XIRR_AVAILABLE = False
    _pyxirr_xirr = None

# Suppress "UserWarning: …" messages from pandas/yfinance that clutter output.
warnings.filterwarnings("ignore")
# Show only WARNING and above from the logging system (hides DEBUG/INFO noise).
logging.basicConfig(level=logging.WARNING)


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
# When Python fetches a web page, the server can see what software made the
# request.  Many financial websites block requests that look like bots.
# By setting a realistic "User-Agent" header we look like a normal Chrome
# browser on Windows, which makes the requests go through.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    # Tell the server we prefer Indian-English content
    "Accept-Language": "en-IN,en;q=0.9",
    # Tell the server we accept standard HTML/XML responses
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
# A Session reuses the same TCP connection for multiple requests to the same
# host, which is faster than creating a new connection every time.
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Local cache file — stored in the same folder as this script.
# Every time the script runs it saves today's NAV values here so that over
# time we accumulate enough history to calculate 3Y/5Y/10Y XIRR returns.
# Without this cache, we can only calculate returns that we can fetch live.
CACHE_FILE = Path(__file__).parent / "nav_cache.json"

# Sanity-check ranges for each fund/index.
# Web pages sometimes contain random numbers (ratings, IDs, percentages) that
# regex can accidentally pick up as NAV values.  If a scraped number falls
# outside these ranges we reject it as invalid.
NAV_VALID_RANGE = {
    "HDFC Life Opportunities Fund": (30.0,   200.0),   # Rs per unit
    "HDFC Life Discovery Fund":     (10.0,   150.0),   # Rs per unit
    "NIFTY MIDCAP 100":             (5_000, 100_000),  # index points
    "NIFTY MIDCAP 150":             (5_000,  45_000),  # index points
}

# ── ULIP Fund definitions ────────────────────────────────────────────────────
# Each dict describes one HDFC Life ULIP fund.
#   name     : human-readable display name
#   type     : row label in the output table
#   sfin     : SEBI Fund Identification Number — the unique code HDFC Life uses
#              to identify each fund in its website's data (used in web scraping)
#   mic_slug : URL slug on myinsuranceclub.com for historical NAV data
#   pb_slug  : URL slug on policybazaar.com for current NAV + return stats
ULIP_FUNDS = [
    {
        "name":    "HDFC Life Opportunities Fund",
        "type":    "ULIP Fund",
        "sfin":    "ULIF03601/01/10OpprtntyFd101",
        "mic_slug": "opportunities-fund",
        "pb_slug": "hdfc-life-opportunities-fund",
    },
    {
        "name":    "HDFC Life Discovery Fund",
        "type":    "ULIP Fund",
        "sfin":    "ULIF06618/01/18DiscvryFnd101",
        "mic_slug": "discovery-fund",
        "pb_slug": "hdfc-life-discovery-fund",
    },
]

# ── Benchmark index definitions ───────────────────────────────────────────────
# The Nifty Midcap 100 and 150 are used as benchmarks to compare ULIP returns.
#   nse_csv_name : exact string NSE uses in its bulk CSV files — must match case
#   yf_ticker    : Yahoo Finance ticker symbol (None if it doesn't work reliably)
NIFTY_INDICES = [
    {
        "name":         "NIFTY MIDCAP 100",
        "display":      "Nifty Midcap 100",
        "type":         "Index (Benchmark)",
        "nse_csv_name": "Nifty Midcap 100",
        "yf_ticker":    None,                      # Yahoo ticker confirmed broken
    },
    {
        "name":         "NIFTY MIDCAP 150",
        "display":      "Nifty Midcap 150",
        "type":         "Index (Benchmark)",
        "nse_csv_name": "Nifty Midcap 150",
        "yf_ticker":    "NIFTYMIDCAP150.NS",       # CONFIRMED WORKS
    },
]

# ── SIP parameters for XIRR calculation ──────────────────────────────────────
# We simulate investing this fixed amount on the 5th of every month.
# XIRR then tells us the annualised return that investment would have earned.
SIP_AMOUNT        = 5_000    # Rs invested per month per fund
SIP_DAY_OF_MONTH  = 5        # investment date (5th of each month)

# Horizons for which we calculate SIP XIRR.
# e.g. (1, "1Y") means "simulate SIP over the past 1 year, label it '1Y XIRR'"
SIP_HORIZONS = [
    (1,  "1Y XIRR (SIP)"),
    (3,  "3Y XIRR (SIP)"),
    (5,  "5Y XIRR (SIP)"),
    (10, "10Y XIRR (SIP)"),
]

# ── Output table column order ─────────────────────────────────────────────────
# This list controls which columns appear in the printed table and in the email,
# and in what order.  The XIRR columns are appended at the end.
COLUMNS = [
    "Name", "Type", "As Of", "Current NAV (Rs)",
    "52W High", "52W Low",
    "Fall from 52W High", "Rise from 52W Low",
    "1Y Return", "3Y Return (Abs)",
    "1Y XIRR (SIP)", "3Y XIRR (SIP)", "5Y XIRR (SIP)", "10Y XIRR (SIP)",
]

# Footer notes printed below the table and inside the email.
NOTES = """\
Notes:
  • ULIP NAV in INR per unit.  Index values are index points.
  • 52W High/Low sourced directly from HDFC Life fund-performance page.
  • 1Y Return (ULIP)     = % stated on HDFC Life fund-performance page.
  • 3Y Return (ULIP Abs) = computed from cache if ≥3Y of daily data exists.
  • Index 1Y/3Y          = computed from NSE/yfinance historical data.
  • XIRR (SIP)           = annualised return if Rs 5,000 was invested on the
                           5th of every month for the stated period.
                           XIRR is the industry-standard measure for SIP returns.
  • N/A = data not yet available (cache too short or source unreachable)."""


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS  —  small reusable utility functions
# ─────────────────────────────────────────────────────────────────────────────

def is_valid(val: float, name: str) -> bool:
    """
    Return True if `val` falls within the expected range for `name`.
    This guards against accidentally picking up wrong numbers from web pages
    (e.g. a page-ID of 42 is not a valid NAV for Opportunities Fund).
    """
    lo, hi = NAV_VALID_RANGE.get(name, (0.0, float("inf")))
    return lo <= val <= hi


def _empty(s) -> bool:
    """
    Return True if `s` is None or an empty pandas Series.
    Used throughout the code as a safe null-check before accessing series data.
    """
    if s is None:
        return True
    if isinstance(s, pd.Series):
        return s.empty
    return True


def pct(v, already_pct: bool = False) -> str:
    """
    Format a number as a percentage string with a leading sign.
    Examples: 13.5  → '+13.50%'    -2.3 → '-2.30%'    None → 'N/A'
    The `already_pct` parameter is kept for backward-compatibility but is
    not used in this version — v is always the raw percentage value (e.g. 13.26,
    not 0.1326).
    """
    if v is None:
        return "N/A"
    return f"{v:+.2f}%"


def fmtv(v) -> str:
    """
    Format a NAV or index value with commas and 4 decimal places.
    Examples: 42.637 → '42.6370'    59784.8 → '59,784.8000'    None → 'N/A'
    """
    return f"{v:,.4f}" if v is not None else "N/A"


def make_series(records: list, name: str) -> "pd.Series | None":
    """
    Convert a list of (date, value) tuples into a cleaned pandas Series.

    Steps:
      1. Skip any record where the value is not a valid float.
      2. Skip values that fail the is_valid() range check for this fund/index.
      3. Build a Series with a DatetimeIndex, sorted oldest-first.
      4. Remove duplicate dates (keep the last/most-recent value for each date).

    Returns None if no valid records remain.
    """
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


def merge_series(a, b):
    """
    Merge two pandas Series that share the same date index (NAV history).
    - If one is empty/None, return the other.
    - If both have data, concatenate them and remove duplicate dates.
    This is used to combine data from different sources (e.g. cache + fresh fetch).
    """
    if _empty(a) and _empty(b):
        return None
    if _empty(a):
        return b
    if _empty(b):
        return a
    merged = pd.concat([a, b]).sort_index()
    return merged[~merged.index.duplicated(keep="last")]


def compute_index_metrics(series, name: str) -> dict:
    """
    Compute all display metrics from a daily price/NAV series for an index.

    Returns a dict with:
      current        : most recent value
      as_of          : date of that value (formatted string)
      high_52w       : highest value in the past 52 weeks
      low_52w        : lowest value in the past 52 weeks
      fall_from_high : % drop from 52W high to today (negative number)
      rise_from_low  : % gain from 52W low to today (positive number)
      ret_1y         : 1-year absolute return % (point-to-point)
      ret_3y         : 3-year absolute return % (point-to-point)

    All values may be None if there is not enough history.
    """
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

    # Latest data point
    today   = s.index[-1]
    current = float(s.iloc[-1])
    base.update({"current": current, "as_of": today.strftime("%d-%b-%Y")})

    # 52-week high and low — look at all data points in the past 365 days
    w52 = s[s.index >= today - timedelta(days=365)]
    if not w52.empty:
        h52 = float(w52.max())
        l52 = float(w52.min())
        base["high_52w"]       = h52
        base["low_52w"]        = l52
        # Fall from high = how far the current price is below the 52W peak (negative %)
        base["fall_from_high"] = (current - h52) / h52 * 100
        # Rise from low  = how much the current price has recovered from the 52W trough
        base["rise_from_low"]  = (current - l52) / l52 * 100

    # 1-year point-to-point return: find the NAV from ~365 days ago and compare
    p1 = s[s.index <= today - timedelta(days=365)]
    if not p1.empty:
        base["ret_1y"] = (current - float(p1.iloc[-1])) / float(p1.iloc[-1]) * 100

    # 3-year point-to-point return: same idea, 3 years back
    p3 = s[s.index <= today - timedelta(days=3 * 365)]
    if not p3.empty:
        base["ret_3y"] = (current - float(p3.iloc[-1])) / float(p3.iloc[-1]) * 100

    return base


def build_row(display_name: str, type_: str,
              nav=None, as_of=None,
              h52=None, l52=None,
              ret_1y=None, ret_3y=None,
              xirr_vals: dict = None) -> dict:
    """
    Assemble one row of the output table as a plain Python dict.

    All financial values are formatted as display strings here (e.g. '+13.50%')
    rather than raw floats, so the table/email rendering code just uses them as-is.

    xirr_vals: dict mapping SIP horizon label → XIRR % float (or None).
               e.g. {'1Y XIRR (SIP)': 12.3, '3Y XIRR (SIP)': None, ...}
    """
    current = float(nav) if nav is not None else None
    # Fall from high / Rise from low are derived here if not already computed
    fall    = ((current - h52) / h52 * 100) if (current and h52) else None
    rise    = ((current - l52) / l52 * 100) if (current and l52) else None

    row = {
        "Name":               display_name,
        "Type":               type_,
        "As Of":              as_of or "N/A",
        "Current NAV (Rs)":   fmtv(current),
        "52W High":           fmtv(h52),
        "52W Low":            fmtv(l52),
        "Fall from 52W High": pct(fall),
        "Rise from 52W Low":  pct(rise),
        "1Y Return":          pct(ret_1y),
        "3Y Return (Abs)":    pct(ret_3y),
    }
    # Append XIRR columns — default to 'N/A' for each horizon
    for _, label in SIP_HORIZONS:
        val = (xirr_vals or {}).get(label)
        row[label] = pct(val)
    return row


def build_row_from_metrics(display_name: str, type_: str, m: dict,
                           xirr_vals: dict = None) -> dict:
    """
    Convenience wrapper: unpacks a metrics dict and calls build_row().
    Used by the index fetcher which returns a metrics dict directly.
    """
    return build_row(
        display_name, type_,
        nav=m["current"], as_of=m["as_of"],
        h52=m["high_52w"], l52=m["low_52w"],
        ret_1y=m["ret_1y"], ret_3y=m["ret_3y"],
        xirr_vals=xirr_vals,
    )


# ─────────────────────────────────────────────────────────────────────────────
# SIP XIRR CALCULATION
# ─────────────────────────────────────────────────────────────────────────────
# XIRR (Extended Internal Rate of Return) is the gold-standard metric for
# measuring the annualised return of a Systematic Investment Plan (SIP).
#
# How it works:
#   - We assume an investor puts Rs 5,000 on the 5th of every month.
#   - On each SIP date we look up the NAV and calculate how many units were
#     purchased (units = SIP_AMOUNT / NAV on that date).
#   - At the end of the period the accumulated units are valued at the current
#     NAV.  This final redemption value is treated as a positive cash flow.
#   - XIRR then finds the single annual interest rate that makes all those
#     cash flows (deposits negative, final value positive) sum to zero in
#     present-value terms.
#
# Why XIRR instead of simple return?
#   A simple return (end value / total invested - 1) ignores the time value of
#   money — money invested 10 years ago has a much bigger compounding effect
#   than money invested 1 month ago.  XIRR accounts for the exact timing of
#   every cash flow, making it the fair, apples-to-apples comparison.

def compute_sip_xirr(series: "pd.Series", years: int,
                     sip_amount: float = SIP_AMOUNT,
                     sip_day: int = SIP_DAY_OF_MONTH) -> "float | None":
    """
    Simulate a monthly SIP over `years` years and return the XIRR (% p.a.).

    Parameters
    ----------
    series     : daily NAV/price series with a DatetimeIndex, sorted ascending.
    years      : how many years back to start the SIP (1, 3, 5, or 10).
    sip_amount : fixed monthly investment in Rs (default: Rs 5,000).
    sip_day    : day of month on which the SIP is invested (default: 5th).

    Returns
    -------
    XIRR as a percentage (e.g. 14.23 means 14.23% p.a.), or None if there is
    not enough data or if the XIRR calculation fails to converge.
    """
    # Cannot compute XIRR without the pyxirr library
    if not _XIRR_AVAILABLE or _empty(series):
        return None

    s = series.dropna().sort_index()
    if s.empty:
        return None

    # The most recent date and value in the series
    end_date    = s.index[-1]
    current_nav = float(s.iloc[-1])

    # Start date: go back exactly `years` years from the latest data point
    start_date  = end_date - timedelta(days=years * 365)

    # Check we actually have enough history
    if s.index[0] > start_date:
        return None   # Series too short for this horizon

    # ── Build list of SIP cash flows ─────────────────────────────────────────
    # Each monthly investment is a negative cash flow (money going out).
    # The final redemption value is a positive cash flow (money coming in).
    dates       = []   # one date per cash flow
    cash_flows  = []   # negative for investments, positive for final value
    total_units = 0.0  # accumulate units purchased each month

    # Walk month by month from start_date to end_date
    current = start_date.replace(day=1)   # go to the 1st of the start month
    while current <= end_date:
        # Try to invest on `sip_day`; if that day doesn't exist (e.g. Feb 30),
        # pandas will raise, so we cap at month-end via min logic
        try:
            invest_date = current.replace(day=sip_day)
        except ValueError:
            # Month is shorter than sip_day (shouldn't happen for day=5, but safe)
            invest_date = current + pd.offsets.MonthEnd(0)
            invest_date = invest_date.to_pydatetime()

        if invest_date > end_date:
            break  # don't invest beyond the data we have

        # Find the nearest available NAV on or after the SIP date
        # (markets are closed on weekends/holidays, so exact date may not exist)
        future_dates = s.index[s.index >= pd.Timestamp(invest_date)]
        if future_dates.empty:
            break   # no data at or after this invest date
        actual_date = future_dates[0]
        nav_on_date = float(s.loc[actual_date])

        # Units purchased = amount invested / NAV on that day
        units_bought = sip_amount / nav_on_date
        total_units += units_bought

        # Record this investment as a negative cash flow (money leaving wallet)
        dates.append(actual_date.to_pydatetime())
        cash_flows.append(-sip_amount)

        # Move to the next month
        current = (current + pd.DateOffset(months=1)).to_pydatetime()

    # Need at least 2 cash flows to calculate XIRR
    if len(dates) < 2 or total_units <= 0:
        return None

    # Final cash flow: the current market value of all accumulated units
    # (This is what the investor would receive if they sold everything today)
    redemption_value = total_units * current_nav
    dates.append(end_date.to_pydatetime())
    cash_flows.append(redemption_value)

    # ── Run XIRR ─────────────────────────────────────────────────────────────
    try:
        result = _pyxirr_xirr(dates, cash_flows)
        # pyxirr returns a decimal (0.1423 = 14.23%); convert to percentage
        if result is None or not isinstance(result, float):
            return None
        xirr_pct = result * 100
        # Sanity check: reject wildly unrealistic results
        if -100 < xirr_pct < 500:
            return round(xirr_pct, 2)
        return None
    except Exception:
        return None


def compute_all_xirr(series: "pd.Series") -> dict:
    """
    Compute SIP XIRR for all configured horizons (1Y, 3Y, 5Y, 10Y).

    Returns a dict mapping column label → XIRR % (or None).
    Example: {'1Y XIRR (SIP)': 12.3, '3Y XIRR (SIP)': 15.1, ...}
    """
    results = {}
    for years, label in SIP_HORIZONS:
        results[label] = compute_sip_xirr(series, years)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# LOCAL CACHE
# ─────────────────────────────────────────────────────────────────────────────
# The cache is a JSON file on disk that stores historical NAV values so we
# can calculate long-term returns (3Y, 5Y, 10Y XIRR) without re-fetching
# all history every time the script runs.
#
# Structure of the JSON file:
#   {
#     "HDFC Life Opportunities Fund": {
#       "2024-01-05": 68.1234,
#       "2024-01-06": 68.5001,
#       ...
#     },
#     "NIFTY MIDCAP 150": { ... }
#   }
#
# Over time (months/years of daily runs) this file builds up a rich history
# that enables accurate long-horizon XIRR calculations.

def cache_load() -> dict:
    """
    Load the NAV cache from disk.
    Returns an empty dict if the file doesn't exist yet or is corrupted.
    """
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text())
        except Exception:
            pass
    return {}


def cache_save(cache: dict):
    """Write the updated cache back to disk as a formatted JSON file."""
    try:
        CACHE_FILE.write_text(json.dumps(cache, indent=2, sort_keys=True))
    except Exception as e:
        print(f"  ⚠  Cache save failed: {e}")


def cache_to_series(cache: dict, name: str) -> "pd.Series | None":
    """
    Convert one fund's cache entries (date-string → float) into a pandas Series.
    Returns None if there is no cached data for that fund yet.
    """
    data = cache.get(name, {})
    if not data:
        return None
    records = []
    for k, v in data.items():
        try:
            records.append((pd.Timestamp(k), float(v)))
        except Exception:
            pass
    return make_series(records, name)


def cache_add(cache: dict, name: str, dt, val: float):
    """
    Add a single date → NAV entry to the in-memory cache dict.
    The date is normalised to 'YYYY-MM-DD' string format so the JSON stays clean.
    The value is rounded to 6 decimal places to avoid floating-point noise.
    Call cache_save() after all updates to persist the data to disk.
    """
    cache.setdefault(name, {})[pd.Timestamp(dt).strftime("%Y-%m-%d")] = round(val, 6)


# ═════════════════════════════════════════════════════════════════════════════
#  ULIP NAV DATA FETCHING
# ═════════════════════════════════════════════════════════════════════════════
# ULIP (Unit Linked Insurance Plan) NAVs are not available on standard APIs
# like Yahoo Finance, so we scrape them from three sources in priority order:
#
#  Source 1 (PRIMARY): hdfclife.com/fund-performance
#    → Provides: current NAV, NAV date, 52W High, 52W Low, 1Y return %
#    → Method: download the page HTML, find the fund's SFIN code in the
#      JavaScript data embedded in the page, then extract numbers using
#      regular expressions (regex patterns).
#
#  Source 2 (HISTORY): myinsuranceclub.com
#    → Provides: historical NAV table (months/years of past NAVs)
#    → Needed for: computing 3Y/5Y/10Y XIRR from the SIP simulation
#    → Method: check for Highcharts chart data (JSON array) first;
#      fall back to parsing an HTML <table> on the page.
#
#  Source 3 (FALLBACK): policybazaar.com
#    → Provides: current NAV + published 1Y/3Y return percentages
#    → Used only when Source 1 fails to return a valid NAV
#
# The three-source approach makes the script resilient: if one website goes
# down or changes its layout, the others pick up the slack.

# ─────────────────────────────────────────────────────────────────────────────
#  Source 1 — hdfclife.com/fund-performance (PRIMARY)
# ─────────────────────────────────────────────────────────────────────────────
# Confirmed from web search: the page source contains repeated blocks of:
#   ULIF03601/01/10OpprtntyFd101 · NAV · 76.7876 · 2026-04-24 ·
#   52 wk High · 2025-11-17 · 79.8503 · 52 wk Low · 2026-03-31 · 69.1143
#
# The same block repeats 3× per fund (for each return-period tab).
# We parse ALL occurrences and take the first clean match.
#
# The page also shows "1Y Return" as a percentage directly:
#   -11.63%  for Opportunities Fund
#   13.26%   for Discovery Fund (from policybazaar confirmation)
# We extract this too so we don't need historical series for 1Y.

def _parse_fund_performance_page(html: str, sfin: str, name: str) -> dict:
    """
    Parse hdfclife.com/fund-performance page source for a given SFIN.
    Returns dict with keys: nav, as_of, high_52w, high_date,
                            low_52w, low_date, ret_1y
    All values may be None if not found.
    """
    result = {
        "nav": None, "as_of": None,
        "high_52w": None, "high_date": None,
        "low_52w": None,  "low_date": None,
        "ret_1y": None,
    }

    # ── Step 1: locate the SFIN in the page ──────────────────────────────────
    sfin_idx = html.find(sfin)
    if sfin_idx == -1:
        # Try partial match (sometimes URL-encoded or slightly different)
        sfin_short = sfin[:20]
        sfin_idx = html.find(sfin_short)
    if sfin_idx == -1:
        print(f"      SFIN {sfin} not found in page")
        return result

    # ── Step 2: extract the ~600-char block after the SFIN ───────────────────
    # Confirmed structure (from web search result text):
    #   ULIF... · NAV · <nav_val> · <nav_date> ·
    #   52 wk High · <high_date> · <high_val> ·
    #   52 wk Low · <low_date> · <low_val>
    # The separator "·" may be rendered as HTML entities or literal dots.
    block = html[sfin_idx: sfin_idx + 800]

    # Normalise separators: ·  &middot;  •  |  and extra whitespace
    block_clean = re.sub(r'[·•\|]|&middot;|&#xB7;', ' SEP ', block)
    block_clean = re.sub(r'\s+', ' ', block_clean)

    # Pattern: NAV SEP <value> SEP <date>
    nav_m = re.search(
        r'NAV\s+SEP\s+([\d.]+)\s+SEP\s+(\d{4}-\d{2}-\d{2}|\d{2}-\w{3}-\d{4})',
        block_clean, re.IGNORECASE
    )
    if nav_m:
        try:
            result["nav"]   = float(nav_m.group(1))
            result["as_of"] = pd.to_datetime(nav_m.group(2)).strftime("%d-%b-%Y")
        except Exception:
            pass

    # 52 wk High SEP <date> SEP <value>
    high_m = re.search(
        r'52\s*wk\s*High\s+SEP\s+(\d{4}-\d{2}-\d{2}|\d{2}-\w{3}-\d{4})\s+SEP\s+([\d.]+)',
        block_clean, re.IGNORECASE
    )
    if high_m:
        try:
            result["high_date"] = pd.to_datetime(high_m.group(1)).strftime("%d-%b-%Y")
            result["high_52w"]  = float(high_m.group(2))
        except Exception:
            pass

    # 52 wk Low SEP <date> SEP <value>
    low_m = re.search(
        r'52\s*wk\s*Low\s+SEP\s+(\d{4}-\d{2}-\d{2}|\d{2}-\w{3}-\d{4})\s+SEP\s+([\d.]+)',
        block_clean, re.IGNORECASE
    )
    if low_m:
        try:
            result["low_date"] = pd.to_datetime(low_m.group(1)).strftime("%d-%b-%Y")
            result["low_52w"]  = float(low_m.group(2))
        except Exception:
            pass

    # ── Step 3: If sep-based parse failed, try raw number extraction ─────────
    if result["nav"] is None:
        # Find all numbers in block that could be valid NAVs
        all_nums = re.findall(r'\b(\d{2,3}\.\d{2,6})\b', block)
        valid    = [float(n) for n in all_nums if is_valid(float(n), name)]
        if valid:
            result["nav"] = valid[0]
            result["as_of"] = datetime.today().strftime("%d-%b-%Y")

    if result["high_52w"] is None or result["low_52w"] is None:
        # Extract all valid numbers from block; highest = 52W High, lowest = 52W Low
        all_nums = re.findall(r'\b(\d{2,3}\.\d{2,6})\b', block)
        valid    = sorted(set(float(n) for n in all_nums if is_valid(float(n), name)))
        if len(valid) >= 3:
            result["low_52w"]  = valid[0]
            result["nav"]      = result["nav"] or valid[1]
            result["high_52w"] = valid[-1]

    # ── Step 4: Extract 1Y return percentage shown on the page ───────────────
    # Confirmed format: "-11.63%" or "13.26%" near the SFIN block
    ret_m = re.search(
        r'([+-]?\d{1,3}\.\d{1,2})\s*%.*?(?:1\s*Year|1Y)',
        block, re.IGNORECASE
    )
    if not ret_m:
        # Alternative: look for Fund Performance % near SFIN
        ret_m = re.search(
            r'HDFC\s+Fund\s+Performance\s+([+-]?\d{1,3}\.\d{1,2})%',
            block, re.IGNORECASE
        )
    if ret_m:
        try:
            result["ret_1y"] = float(ret_m.group(1))
        except Exception:
            pass

    return result


def fetch_hdfc_fund_performance(sfin: str, name: str) -> dict:
    """
    Scrape hdfclife.com/fund-performance for NAV + 52W High/Low + 1Y return.
    Returns parsed dict (values may be None on failure).
    """
    print(f"    [Src 1] hdfclife.com/fund-performance …")

    urls = [
        "https://www.hdfclife.com/fund-performance",
        "https://www.hdfclife.com/insurance-products/savings-and-investment/ulip/fund-performance",
        "https://www.hdfclife.com/ulip-plans/click-2-wealth-ulip-plan",
        "https://www.hdfclife.com/ulip-plans",
    ]

    for url in urls:
        try:
            r = SESSION.get(url, timeout=25)
            if r.status_code != 200:
                print(f"      {url.split('/')[-1]}: HTTP {r.status_code}")
                continue

            parsed = _parse_fund_performance_page(r.text, sfin, name)

            # Validate
            if parsed["nav"] and is_valid(parsed["nav"], name):
                print(f"    [Src 1] ✓ {url.split('/')[-1]}: "
                      f"NAV={parsed['nav']}, "
                      f"52WH={parsed['high_52w']}, "
                      f"52WL={parsed['low_52w']}, "
                      f"1Y={parsed['ret_1y']}%")
                return parsed
            else:
                print(f"      {url.split('/')[-1]}: parsed but NAV invalid "
                      f"(got {parsed['nav']})")
        except Exception as e:
            print(f"      {url.split('/')[-1]}: {e}")

    print(f"    [Src 1] ✗ hdfclife.com/fund-performance: all URLs failed")
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# ULIP NAV — SOURCE 2: myinsuranceclub.com tabular view (history)
# ─────────────────────────────────────────────────────────────────────────────
# Confirmed accessible from web search result:
#   https://www.myinsuranceclub.com/life-insurance/companies/hdfc-life/nav/opportunities-fund
#   Shows: "NAV Today ₹ 67.2318 · Tabular View · ULIF03601/01/10OpprtntyFd101"
# The "Tabular View" contains historical NAV data we can parse.
# Note: The previous 404s were because the URL in the script was wrong.
# The correct base URL confirmed from the search result is the one above.

def fetch_mic_history(slug: str, name: str) -> "pd.Series | None":
    base_url = "https://www.myinsuranceclub.com"
    page_url = f"{base_url}/life-insurance/companies/hdfc-life/nav/{slug}"
    print(f"    [Src 2] myinsuranceclub.com/…/{slug} …")

    try:
        r = SESSION.get(page_url, timeout=20)
        if r.status_code == 404:
            print(f"      404 — trying alternate slug")
            # Try alternate URL patterns
            for alt in [
                f"{base_url}/life-insurance/hdfc-life-insurance/ulip-nav/{slug}",
                f"{base_url}/life-insurance/companies/hdfc-life/{slug}",
                f"{base_url}/nav/hdfc-life/{slug}",
            ]:
                try:
                    r2 = SESSION.get(alt, timeout=15)
                    if r2.status_code == 200:
                        r = r2
                        print(f"      Found at: {alt}")
                        break
                except Exception:
                    pass
            else:
                print(f"    [Src 2] ✗ MIC: all slugs 404")
                return None

        r.raise_for_status()
        page = r.text

        # ── Try Highcharts [[timestamp_ms, value], …] ─────────────────────────
        for hc_match in re.finditer(
            r'data\s*:\s*(\[\s*\[\s*\d{10,13}[,\d.\s\[\]]*\])',
            page, re.DOTALL
        ):
            try:
                raw     = json.loads(hc_match.group(1))
                records = []
                for pair in raw:
                    if isinstance(pair, (list, tuple)) and len(pair) == 2:
                        ts  = int(pair[0])
                        dt  = pd.Timestamp(ts if ts > 1e11 else ts * 1000, unit="ms")
                        nav = float(pair[1])
                        records.append((dt, nav))
                s = make_series(records, name)
                if not _empty(s) and len(s) > 10:
                    print(f"    [Src 2] ✓ Highcharts: {len(s)} records, "
                          f"latest={s.iloc[-1]:.4f}")
                    return s
            except Exception:
                pass

        # ── Try HTML table (Tabular View) ─────────────────────────────────────
        soup = BeautifulSoup(page, "html.parser")
        for table in soup.find_all("table"):
            records = []
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) >= 2:
                    try:
                        dt  = pd.to_datetime(cols[0].get_text(strip=True), dayfirst=True)
                        nav = float(cols[1].get_text(strip=True).replace(",", ""))
                        records.append((dt, nav))
                    except Exception:
                        pass
            s = make_series(records, name)
            if not _empty(s):
                print(f"    [Src 2] ✓ HTML table: {len(s)} records, "
                      f"latest={s.iloc[-1]:.4f}")
                return s

        # ── Current NAV text only ─────────────────────────────────────────────
        m = re.search(r'NAV\s*(?:Today)?[^\d]{0,30}?₹\s*([\d,]+\.\d{2,5})', page)
        if m:
            nav = float(m.group(1).replace(",", ""))
            if is_valid(nav, name):
                today = pd.Timestamp(datetime.today().date())
                print(f"    [Src 2] ℹ  Current NAV only: {nav}")
                return pd.Series([nav], index=pd.DatetimeIndex([today]), name=name)

        print(f"    [Src 2] ✗ MIC: could not parse any data from page")
        return None

    except Exception as e:
        print(f"    [Src 2] ✗ MIC: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# ULIP NAV — SOURCE 3: policybazaar.com (current NAV + return stats)
# ─────────────────────────────────────────────────────────────────────────────
# Confirmed accessible. Returns current NAV and also states return percentages
# like "13.26% return in the last 1 year", "23.32% CAGR over 3 years".
# We extract these as fallback for 1Y/3Y returns.

def fetch_policybazaar(pb_slug: str, name: str) -> dict:
    """Returns dict: {nav, as_of, ret_1y, ret_3y}"""
    url = (f"https://www.policybazaar.com/life-insurance/ulip-plans/"
           f"hdfc-life-insurance/{pb_slug}/")
    print(f"    [Src 3] policybazaar.com …")
    result = {"nav": None, "as_of": None, "ret_1y": None, "ret_3y": None}
    try:
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        # Current NAV — "NAV of ₹43.1" or "NAV is ₹43.10"
        for pattern in [
            r'NAV\s+(?:of|is)\s+₹\s*([\d,]+\.\d{1,4})',
            r'Net Asset Value[^₹]{0,30}₹\s*([\d,]+\.\d{1,4})',
            r'[Cc]urrent\s+NAV[^₹\d]{0,60}₹\s*([\d,]+\.\d{1,4})',
            r'NAV[^\d]{0,15}([\d,]+\.\d{2,4})',
        ]:
            m = re.search(pattern, text)
            if m:
                nav = float(m.group(1).replace(",", ""))
                if is_valid(nav, name):
                    result["nav"]   = nav
                    result["as_of"] = datetime.today().strftime("%d-%b-%Y")
                    break

        # 1Y return — "13.26% return in the last 1 year" / "16.21 % CAGR over 3 years"
        m1y = re.search(
            r'([\d.]+)\s*%\s*(?:return|CAGR)?\s*(?:in\s+the\s+)?(?:last\s+)?1\s*year',
            text, re.IGNORECASE
        )
        if m1y:
            try:
                result["ret_1y"] = float(m1y.group(1))
            except Exception:
                pass

        # 3Y CAGR → convert to approximate absolute return
        m3y = re.search(
            r'([\d.]+)\s*%\s*CAGR\s+over\s+3\s*year',
            text, re.IGNORECASE
        )
        if m3y:
            try:
                cagr_3y = float(m3y.group(1)) / 100
                # Absolute return = (1+CAGR)^3 - 1
                result["ret_3y"] = ((1 + cagr_3y) ** 3 - 1) * 100
            except Exception:
                pass

        if result["nav"]:
            print(f"    [Src 3] ✓ PB: NAV={result['nav']}, "
                  f"1Y={result['ret_1y']}%, 3Y={result['ret_3y']:.1f}%"
                  if result["ret_3y"] else
                  f"    [Src 3] ✓ PB: NAV={result['nav']}, 1Y={result['ret_1y']}%")
        else:
            print(f"    [Src 3] ✗ PB: no valid NAV found")

        return result
    except Exception as e:
        print(f"    [Src 3] ✗ PB: {e}")
        return result


# ─────────────────────────────────────────────────────────────────────────────
# ULIP MASTER FETCHER
# ─────────────────────────────────────────────────────────────────────────────
# This is the main orchestrator for ULIP data. It calls the three sources in
# order, fills in gaps, saves to cache, and finally assembles one output row.

def get_ulip_data(fund: dict, cache: dict) -> dict:
    name = fund["name"]
    print(f"\n  ── {name}")

    # ── Source 1: hdfclife.com/fund-performance (NAV + 52W High/Low + 1Y) ──
    # This is the most authoritative source — directly from the insurer's site.
    hdfc = fetch_hdfc_fund_performance(fund["sfin"], name)

    nav    = hdfc.get("nav")
    as_of  = hdfc.get("as_of")
    h52    = hdfc.get("high_52w")
    l52    = hdfc.get("low_52w")
    ret_1y = hdfc.get("ret_1y")
    ret_3y = None

    # ── Source 2: myinsuranceclub (historical NAV series → for XIRR cache) ──
    # We always fetch this, even if Source 1 succeeded, because historical data
    # is needed to build up the cache for long-term XIRR calculations.
    mic_series = fetch_mic_history(fund["mic_slug"], name)

    # ── Source 3: policybazaar (fallback — only if Source 1 missed anything) ──
    pb = {}
    if nav is None or ret_1y is None:
        pb = fetch_policybazaar(fund["pb_slug"], name)
        nav    = nav    or pb.get("nav")
        as_of  = as_of  or pb.get("as_of")
        ret_1y = ret_1y or pb.get("ret_1y")
        ret_3y = ret_3y or pb.get("ret_3y")

    # ── Update cache with today's NAV ─────────────────────────────────────────
    # Always save the latest NAV to the cache so that over time it grows into
    # a full history that can support 5Y and 10Y XIRR calculations.
    if nav and as_of:
        try:
            cache_add(cache, name, pd.to_datetime(as_of, dayfirst=True), nav)
        except Exception:
            cache_add(cache, name, datetime.today().date(), nav)

    # ── Merge MIC historical series into cache ────────────────────────────────
    # If MIC returned a multi-year history, add all those dates to the cache.
    # This is the fastest way to backfill the cache on the first run.
    if not _empty(mic_series):
        for dt, v in zip(mic_series.index, mic_series.values):
            cache_add(cache, name, dt, float(v))

    # ── Compute 3Y point-to-point return from cache (if not already known) ───
    if ret_3y is None:
        cached_series = cache_to_series(cache, name)
        if not _empty(cached_series) and len(cached_series) > 1:
            today_ts = cached_series.index[-1]
            past3    = cached_series[cached_series.index <= today_ts - timedelta(days=3 * 365)]
            if not past3.empty and nav:
                ret_3y = (nav - float(past3.iloc[-1])) / float(past3.iloc[-1]) * 100

    # ── Compute SIP XIRR for all horizons (1Y / 3Y / 5Y / 10Y) ──────────────
    # Use the merged cache series which now contains all available history.
    # compute_all_xirr() will return None for any horizon where data is too short.
    full_series = cache_to_series(cache, name)
    xirr_vals   = compute_all_xirr(full_series)

    # ── Summary line ──────────────────────────────────────────────────────────
    if nav:
        print(f"    → NAV={nav}, 52WH={h52}, 52WL={l52}, "
              f"1Y={ret_1y}%, 3Y={ret_3y}")
        xirr_summary = ", ".join(
            f"{label.split()[0]}={v}%" for label, v in xirr_vals.items() if v is not None
        )
        if xirr_summary:
            print(f"    → SIP XIRR: {xirr_summary}")
    else:
        print(f"    ✗ No NAV obtained from any source")

    return build_row(name, fund["type"],
                     nav=nav, as_of=as_of,
                     h52=h52, l52=l52,
                     ret_1y=ret_1y, ret_3y=ret_3y,
                     xirr_vals=xirr_vals)


# ═════════════════════════════════════════════════════════════════════════════
#  NIFTY INDEX DATA FETCHING
# ═════════════════════════════════════════════════════════════════════════════
# Unlike ULIPs, Nifty index data is freely available from NSE and Yahoo Finance.
# We use two sources for each index:
#
#  Source 1 (PRIMARY for MC100): NSE Archives bulk CSV
#    URL: https://nsearchives.nseindia.com/content/indices/ind_close_all_DDMMYYYY.csv
#    NSE publishes one CSV per trading day with closing levels for ALL indices.
#    We download ~500 such files (going back ~2 years) to build a history.
#
#  Source 2 (PRIMARY for MC150): yfinance / Yahoo Finance
#    The ticker NIFTYMIDCAP150.NS is confirmed to work on Yahoo Finance.
#    yfinance gives us 4 years of history in a single API call — much faster
#    than downloading 500 CSVs.  It doesn't work reliably for Midcap 100.

# ─────────────────────────────────────────────────────────────────────────────
# Index Source 1 — NSE Archives bulk CSV  (CONFIRMED WORKS for Midcap 100)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_nse_archives(nse_csv_name: str, name: str, days: int = 500) -> "pd.Series | None":
    """
    Download NSE daily index CSV files going back `days` trading days and
    extract closing values for the index named `nse_csv_name`.

    NSE publishes one CSV per calendar day at:
      https://nsearchives.nseindia.com/content/indices/ind_close_all_DDMMYYYY.csv

    Each CSV has columns like: "Index Name", "Closing Index Value", "Index Date".
    We look for the row where "Index Name" matches our target index.
    """
    print(f"    [Src 1] NSE archives → '{nse_csv_name}' ({days} trading days) …")
    base    = "https://nsearchives.nseindia.com/content/indices"
    today   = datetime.today()
    records = []
    consec  = 0   # consecutive failure counter — stop if too many in a row

    # Loop backwards from today, one calendar day at a time.
    # We range over days*2 calendar days to account for weekends and public
    # holidays where no trading occurred (and no CSV was published).
    for offset in range(days * 2):
        d = today - timedelta(days=offset)
        if d.weekday() >= 5:
            # Skip Saturday (5) and Sunday (6) — NSE is closed on weekends
            continue
        url = f"{base}/ind_close_all_{d.strftime('%d%m%Y')}.csv"
        try:
            r = SESSION.get(url, timeout=12)
            if r.status_code == 404:
                # 404 usually means a public holiday — the CSV just wasn't published
                consec += 1
                if consec > 10:
                    break   # too many consecutive misses; stop trying
                continue
            if r.status_code != 200:
                consec += 1
                continue
            consec = 0

            # Parse the CSV text into a DataFrame
            df = pd.read_csv(io.StringIO(r.text))
            # Strip whitespace from column names (NSE sometimes adds padding)
            df.columns = [c.strip() for c in df.columns]

            # Dynamically find column names — NSE has changed them in the past
            idx_col   = next((c for c in df.columns
                               if "index" in c.lower() and "name" in c.lower()), None)
            close_col = next((c for c in df.columns if "clos" in c.lower()), None)
            date_col  = next((c for c in df.columns if "date" in c.lower()), None)
            if not idx_col or not close_col:
                continue   # CSV format we don't recognise — skip this day

            # Find the row for our target index (case-insensitive match)
            row = df[df[idx_col].astype(str).str.strip().str.lower()
                     == nse_csv_name.lower()]
            if row.empty:
                continue   # this index wasn't in today's CSV (unlikely but possible)

            # Extract the closing level and the date from the matching row
            val = float(str(row.iloc[0][close_col]).replace(",", "").strip())
            raw_date = str(row.iloc[0][date_col]).strip() if date_col else ""
            dt  = pd.to_datetime(raw_date, dayfirst=True, errors="coerce")
            if pd.isna(dt):
                # If date parsing failed, fall back to the URL's date
                dt = pd.Timestamp(d.date())

            if is_valid(val, name):
                records.append((dt, val))
            if len(records) % 100 == 0 and records:
                print(f"      … {len(records)} records, at {d.strftime('%d-%b-%Y')}")

            if len(records) >= days:
                break   # we have enough data; stop fetching
        except Exception:
            consec += 1
            if consec > 10:
                break

    s = make_series(records, name)
    if _empty(s):
        print(f"    [Src 1] ✗ NSE archives: no data for '{nse_csv_name}'")
        return None
    print(f"    [Src 1] ✓ NSE archives: {len(s)} records | "
          f"{s.index[0].date()} – {s.index[-1].date()} | latest={s.iloc[-1]:.2f}")
    return s


# ─────────────────────────────────────────────────────────────────────────────
# Index Source 2 — yfinance  (CONFIRMED WORKS for NIFTYMIDCAP150.NS only)
# ─────────────────────────────────────────────────────────────────────────────
# yfinance is a Python library that fetches historical price data from Yahoo
# Finance. It's much faster than the NSE CSV approach for indices that have
# a working Yahoo ticker.  Unfortunately NIFTYMIDCAP100.NS is unreliable on
# Yahoo, so we only use this for Midcap 150.

def fetch_yfinance(ticker: str, name: str) -> "pd.Series | None":
    """
    Download up to 4 years of daily closing data for `ticker` from Yahoo Finance.
    Returns a cleaned pandas Series or None if the download fails.
    """
    if not ticker:
        return None
    print(f"    [Src 2] yfinance → {ticker} …")
    try:
        # yfinance is imported here (not at the top) so the script still works
        # even if yfinance is not installed — it will just return None.
        import yfinance as yf
        end   = datetime.today()
        start = end - timedelta(days=4 * 365)   # go back 4 years
        df    = yf.download(ticker,
                            start=start.strftime("%Y-%m-%d"),
                            end=end.strftime("%Y-%m-%d"),
                            progress=False,      # suppress the download progress bar
                            auto_adjust=True)    # use split/dividend-adjusted prices
        if df is None or df.empty:
            print(f"    [Src 2] ✗ yfinance {ticker}: empty")
            return None
        # yfinance sometimes returns a MultiIndex column (ticker, field) — flatten it
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        col = "Close" if "Close" in df.columns else df.columns[0]
        s   = df[col].squeeze().dropna().sort_index()
        s   = make_series(list(zip(s.index, s.values)), name)
        if _empty(s):
            print(f"    [Src 2] ✗ yfinance {ticker}: no valid values")
            return None
        print(f"    [Src 2] ✓ yfinance {ticker}: {len(s)} records | "
              f"latest={s.iloc[-1]:.2f}")
        return s
    except ImportError:
        print(f"    [Src 2] ✗ yfinance not installed")
        return None
    except Exception as e:
        print(f"    [Src 2] ✗ yfinance {ticker}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Index MASTER FETCHER
# ─────────────────────────────────────────────────────────────────────────────

def get_index_data(idx: dict, cache: dict) -> dict:
    name    = idx["name"]
    display = idx["display"]
    print(f"\n  ── {display}")

    cached = cache_to_series(cache, name)
    if not _empty(cached):
        print(f"    Cache: {len(cached)} pts | "
              f"{cached.index[0].date()} → {cached.index[-1].date()}")

    series = None

    # Src 2: yfinance first (fast) — only for MC150
    if idx["yf_ticker"]:
        yf_s   = fetch_yfinance(idx["yf_ticker"], name)
        series = merge_series(cached, yf_s)

    # Src 1: NSE archives — primary for MC100, supplement for MC150
    need_arch = _empty(series) or len(series) < 400
    if need_arch:
        arch   = fetch_nse_archives(idx["nse_csv_name"], name, days=500)
        series = merge_series(series or cached, arch)

    # Save all fetched data points to the cache for future XIRR calculations
    if not _empty(series):
        for dt, v in zip(series.index, series.values):
            cache_add(cache, name, dt, float(v))

    if _empty(series):
        print(f"    ✗ No data for {display}")
        # Return a row with all N/A values rather than crashing
        return build_row_from_metrics(f"{display} (Benchmark)", idx["type"], {
            "current": None, "as_of": None,
            "high_52w": None, "low_52w": None,
            "fall_from_high": None, "rise_from_low": None,
            "ret_1y": None, "ret_3y": None,
        })

    print(f"    → {len(series)} pts | "
          f"{series.index[0].date()} – {series.index[-1].date()} | "
          f"latest={series.iloc[-1]:.2f}")

    # Compute point-to-point metrics (NAV, 52W High/Low, 1Y/3Y return)
    m = compute_index_metrics(series, name)

    # Compute SIP XIRR for all horizons using the same series
    xirr_vals = compute_all_xirr(series)
    xirr_summary = ", ".join(
        f"{label.split()[0]}={v}%" for label, v in xirr_vals.items() if v is not None
    )
    if xirr_summary:
        print(f"    → SIP XIRR: {xirr_summary}")

    return build_row_from_metrics(f"{display} (Benchmark)", idx["type"], m,
                                  xirr_vals=xirr_vals)


# ═════════════════════════════════════════════════════════════════════════════
#  TABLE RENDERING & EMAIL
# ═════════════════════════════════════════════════════════════════════════════

def render_table(results: list) -> str:
    """
    Render the results as a plain-text table for console/terminal output.

    If the `tabulate` library is installed, uses its 'fancy_grid' style which
    produces a beautiful Unicode box-drawing table.

    If tabulate is NOT installed (e.g. in a minimal GitHub Actions environment),
    falls back to a simple ASCII pipe-separated table that needs no dependencies.
    """
    df = pd.DataFrame(results, columns=COLUMNS)
    if _TABULATE_AVAILABLE:
        return tabulate(df, headers="keys", tablefmt="fancy_grid",
                        showindex=False, numalign="right", stralign="left")
    # Fallback: compute column widths and draw borders manually
    col_widths = {c: max(len(c), df[c].astype(str).str.len().max()) for c in COLUMNS}
    sep = "+-" + "-+-".join("-" * col_widths[c] for c in COLUMNS) + "-+"
    header = "| " + " | ".join(c.ljust(col_widths[c]) for c in COLUMNS) + " |"
    rows_txt = [sep, header, sep]
    for _, row in df.iterrows():
        rows_txt.append("| " + " | ".join(str(row[c]).ljust(col_widths[c]) for c in COLUMNS) + " |")
    rows_txt.append(sep)
    return "\n".join(rows_txt)


def render_html_table(results: list) -> str:
    """
    Render the results as a styled HTML <table> for the email body.

    Why not use the plain-text table in the email?
    Email clients render <pre> text in a proportional font, which breaks the
    column alignment.  They also often strip Unicode box-drawing characters.
    A proper HTML <table> renders perfectly in every modern email client.

    Design features:
      - Dark blue header row (#1a237e — HDFC Life brand colour)
      - Alternating row shading for readability
      - Positive returns shown in green, negative in red
      - All columns use white-space:nowrap to prevent awkward line-breaks
    """
    th_style = (
        "background:#1a237e;color:#fff;padding:8px 12px;"
        "font-size:13px;text-align:left;white-space:nowrap;"
    )
    td_base = "padding:7px 12px;font-size:13px;white-space:nowrap;border-bottom:1px solid #e0e0e0;"
    td_num  = td_base + "text-align:right;"
    td_str  = td_base + "text-align:left;"

    def color_pct(val: str) -> str:
        """Wrap a percentage string in a green or red <span> based on its sign."""
        if val.startswith("+"):
            return f"<span style='color:#2e7d32;font-weight:600;'>{val}</span>"
        if val.startswith("-"):
            return f"<span style='color:#c62828;font-weight:600;'>{val}</span>"
        return val   # 'N/A' or zero — no colour

    # Columns that should be right-aligned (raw numbers, not percentages)
    numeric_cols = {"Current NAV (Rs)", "52W High", "52W Low"}
    # Columns that contain percentage values — right-aligned + colour-coded
    pct_cols = {
        "Fall from 52W High", "Rise from 52W Low",
        "1Y Return", "3Y Return (Abs)",
        "1Y XIRR (SIP)", "3Y XIRR (SIP)", "5Y XIRR (SIP)", "10Y XIRR (SIP)",
    }

    header_html = "".join(f"<th style='{th_style}'>{c}</th>" for c in COLUMNS)
    rows_html   = ""
    for i, row in enumerate(results):
        bg = "#fafafa" if i % 2 == 0 else "#ffffff"
        cells = ""
        for c in COLUMNS:
            val = str(row.get(c, "N/A"))
            if c in pct_cols:
                cells += f"<td style='{td_num}'>{color_pct(val)}</td>"
            elif c in numeric_cols:
                cells += f"<td style='{td_num}'>{val}</td>"
            else:
                cells += f"<td style='{td_str}'>{val}</td>"
        rows_html += f"<tr style='background:{bg};'>{cells}</tr>\n"

    return (
        "<table cellspacing='0' cellpadding='0' border='0' "
        "style='border-collapse:collapse;border:1px solid #bdbdbd;"
        "font-family:Arial,sans-serif;min-width:900px;'>"
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{rows_html}</tbody>"
        "</table>"
    )


def _smtp_host(domain: str) -> str:
    """
    Map an email domain to its outgoing SMTP server hostname.
    For example: 'gmail.com' → 'smtp.gmail.com'
    Falls back to 'smtp.<domain>' for any unlisted provider, which is the
    common convention used by most email hosts.
    """
    return {
        "gmail.com": "smtp.gmail.com",
        "yahoo.com": "smtp.mail.yahoo.com", "yahoo.in": "smtp.mail.yahoo.com",
        "outlook.com": "smtp.office365.com", "hotmail.com": "smtp.office365.com",
        "live.com": "smtp.office365.com", "rediffmail.com": "smtp.rediffmail.com",
    }.get(domain, f"smtp.{domain}")


def send_email(table_str: str, run_date: str, results: list) -> None:
    """
    Send the NAV report by email using SMTP with STARTTLS encryption.

    Credentials are read from environment variables (never hard-coded):
      MY_EMAIL      : your full email address (e.g. you@gmail.com)
      MY_EMAIL_PSWRD: your email password or App Password
      REPORT_TO     : (optional) recipient address; defaults to MY_EMAIL
      SMTP_HOST     : (optional) override the auto-detected SMTP server
      SMTP_PORT     : (optional) override port; default is 587 (STARTTLS)

    The email is sent in MIME multipart/alternative format, which means:
      - Email clients that can render HTML will see the styled HTML table.
      - Clients that only support plain text will see the ASCII table.
    This makes the email compatible with virtually all email clients.

    Gmail users: use a 16-character App Password from
    https://myaccount.google.com/apppasswords  (not your regular password)
    """
    sender   = os.environ.get("MY_EMAIL", "").strip()
    password = os.environ.get("MY_EMAIL_PSWRD", "").strip()
    if not sender or not password:
        print("\n  ⚠  Email skipped — set MY_EMAIL and MY_EMAIL_PSWRD.")
        print("     Windows PS : $env:MY_EMAIL='you@gmail.com'")
        print("     Linux/Mac  : export MY_EMAIL='you@gmail.com'")
        return

    # Auto-detect the SMTP server from the email domain, or use override
    domain    = sender.split("@")[-1].lower() if "@" in sender else ""
    smtp_host = os.environ.get("SMTP_HOST", _smtp_host(domain))
    smtp_port = int(os.environ.get("SMTP_PORT", 587))
    recipient = os.environ.get("REPORT_TO", sender)
    subject   = f"HDFC Life NAV & Nifty Midcap Report — {run_date}"

    # Build the plain-text version (for email clients that don't render HTML)
    # Escape HTML special characters so they display correctly in the HTML part
    notes_esc = NOTES.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    plain     = f"Run date: {run_date}\n\n{table_str}\n\n{NOTES}\n"

    # Build the HTML version with a proper <table> (avoids the <pre> alignment issue)
    html_tbl  = render_html_table(results)
    html = (
        "<html><body style='font-family:Arial,sans-serif;margin:0;padding:20px;'>"
        "<h2 style='color:#1a237e;margin-bottom:4px;'>"
        "HDFC Life NAV &amp; Nifty Midcap Report</h2>"
        f"<p style='color:#555;margin-top:4px;'>Run date: <strong>{run_date}</strong></p>"
        "<div style='overflow-x:auto;'>"
        f"{html_tbl}"
        "</div>"
        f"<pre style='font-size:11px;color:#888;padding:8px;margin-top:16px;'>{notes_esc}</pre>"
        "</body></html>"
    )

    # Assemble the MIME message — "alternative" means HTML and plain-text versions
    # of the same content; the email client picks whichever it prefers.
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = recipient
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html, "html"))

    try:
        print(f"\n  📧  Sending to {recipient} via {smtp_host}:{smtp_port} …")
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as srv:
            srv.ehlo()       # introduce ourselves to the server
            srv.starttls()   # upgrade to encrypted (TLS) connection
            srv.ehlo()       # re-introduce after TLS handshake
            srv.login(sender, password)
            srv.sendmail(sender, recipient, msg.as_string())
        print("  ✅  Email sent.")
    except smtplib.SMTPAuthenticationError:
        print("  ✗  Auth failed. Gmail → App Password:")
        print("     https://myaccount.google.com/apppasswords")
    except Exception as e:
        print(f"  ✗  Email error: {e}")


# ═════════════════════════════════════════════════════════════════════════════
#  MAIN  —  entry point
# ═════════════════════════════════════════════════════════════════════════════
# This function is the top-level orchestrator.  When you run the script it:
#   1. Loads the local NAV cache from disk
#   2. Fetches current ULIP NAVs from three web sources
#   3. Fetches Nifty index levels from NSE and/or Yahoo Finance
#   4. Saves all new data points back to the cache
#   5. Computes SIP XIRR for all configured horizons
#   6. Prints the result table to the console
#   7. Sends the styled HTML report by email

def main():
    run_date = datetime.today().strftime("%d %b %Y  %H:%M")
    hdr = f"  HDFC Life ULIP NAV & Nifty Midcap Tracker  [{run_date}]"
    sep = "═" * max(len(hdr), 60)
    print(f"\n{sep}\n{hdr}\n{sep}")

    # Load cached historical NAV data — needed for long-horizon XIRR
    cache = cache_load()
    print(f"\n  Cache: {CACHE_FILE}")
    print(f"  Cached keys: { {k: len(v) for k, v in cache.items()} }")

    results = []

    # ── Step 1: Fetch all ULIP fund data ─────────────────────────────────────
    # Each call to get_ulip_data scrapes NAV + metrics, updates the cache,
    # and computes SIP XIRR.  The result is one row dict per fund.
    print("\n[1/2] Fetching ULIP Fund NAVs …")
    print("─" * 54)
    for fund in ULIP_FUNDS:
        results.append(get_ulip_data(fund, cache))

    # ── Step 2: Fetch benchmark index data ───────────────────────────────────
    # Same pattern as above — one row per index.
    print("\n[2/2] Fetching Nifty Index Levels …")
    print("─" * 54)
    for idx in NIFTY_INDICES:
        results.append(get_index_data(idx, cache))

    # ── Step 3: Persist cache to disk ────────────────────────────────────────
    # We save AFTER fetching everything so that a mid-run error doesn't
    # corrupt the cache with only partial data.
    cache_save(cache)
    print(f"\n  Cache saved. Keys: { {k: len(v) for k, v in cache.items()} }")

    # ── Step 4: Render and print the results table ───────────────────────────
    table_str = render_table(results)
    print(f"\n{sep}\n  RESULTS\n{sep}\n")
    print(table_str)
    print(f"\n{NOTES}\n")

    # ── Step 5: Email the report ─────────────────────────────────────────────
    # Will silently skip if MY_EMAIL / MY_EMAIL_PSWRD env vars are not set.
    send_email(table_str, run_date, results)


# ── Entry point guard ─────────────────────────────────────────────────────────
# This block only runs when the script is executed directly (e.g. `python script.py`).
# It does NOT run when the file is imported as a module by another script.
# This is a Python best-practice that makes scripts safe to import/test.
if __name__ == "__main__":
    main()
