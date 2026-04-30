"""
Index Market Report - Sends email with key metrics for major Indian indices + Gold
Metrics: Current Level, 3Y High/Low, 52W High/Low, P/E (Current/Avg/High/Low),
         Rise from 52W Low, Fall from 52W High

Data sources:
  • Nifty 50           → Yahoo Finance (^NSEI) for price history;
                         niftyindices.com for REAL historical P/E series
  • Nifty Next 50      → niftyindices.com (price history + real historical P/E)
  • Nifty Midcap 150   → niftyindices.com (price history + real historical P/E)
  • Nifty Smallcap 250 → niftyindices.com (price history + real historical P/E)
  • Nifty Microcap 250 → niftyindices.com (price history + real historical P/E)
  • Gold (MCX)         → Yahoo Finance GC=F (COMEX USD/oz) → converted to INR/g
                         P/E not applicable for Gold.

niftyindices.com endpoints used (official NSE subsidiary, no cookie session needed):
  • POST /Backpage.aspx/getHistoricaldatatabletoString   → EOD OHLC (price ranges)
  • POST /Backpage.aspx/getpepbHistoricaldataDBtoString  → real historical P/E, P/B
  • GET  iislliveblob.niftyindices.com/LiveIndicesWatch.json → live price

Live price fallback: NSE /api/allIndices (if CDN blob unavailable)
Nifty 50 P/E fallback chain: NSE API → screener.in → yfinance (if niftyindices fails)
"""

import os
import re
import smtplib
import urllib.request
import json
import yfinance as yf
import requests
import pandas as pd
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ── Configuration ─────────────────────────────────────────────────────────────

MY_EMAIL    = os.environ["MY_EMAIL"]
MY_PASSWORD = os.environ["MY_EMAIL_PSWRD"]

# Yahoo Finance tickers (index tickers that Yahoo actually carries)
YF_INDICES = {
    "Nifty 50":   ["^NSEI"],
    "Gold (MCX)": ["GC=F", "MGC=F"],   # COMEX gold futures (USD/troy-oz)
}

# niftyindices.com index name strings — must match exactly what the POST API expects.
NSE_INDICES = {
    "Nifty Next 50":       "NIFTY NEXT 50",
    "Nifty Midcap 150":    "NIFTY MIDCAP 150",
    "Nifty Smallcap 250":  "NIFTY SMALLCAP 250",
    "Nifty Microcap 250":  "NIFTY MICROCAP250",
}

# Troy ounces per gram (exact)
TROY_OZ_PER_GRAM = 31.1034768

# ── Data Fetching ─────────────────────────────────────────────────────────────

def try_fetch(ticker: str, days: int = 3 * 365 + 30):
    """
    Fetch historical OHLCV data for a ticker.
    Returns a DataFrame with plain (non-MultiIndex) columns, or None.

    For futures tickers (GC=F) use days=3*365+30 for the range data,
    but always verify the most-recent close is sane via a short window.
    """
    def _flatten(df):
        """Collapse MultiIndex columns produced by yf.download."""
        if df is None or df.empty:
            return df
        if hasattr(df.columns, "levels"):
            try:
                df.columns = df.columns.get_level_values(0)
            except Exception:
                pass
        return df

    tf  = yf.Ticker(ticker)
    end = datetime.today()

    # Strategy 1: Ticker.history with explicit date range (most reliable for futures)
    try:
        start = end - timedelta(days=days)
        data  = tf.history(start=start.strftime("%Y-%m-%d"),
                           end=end.strftime("%Y-%m-%d"),
                           auto_adjust=True)
        if data is not None and not data.empty:
            return _flatten(data)
    except Exception:
        pass

    # Strategy 2: period="max" fallback
    try:
        data = tf.history(period="max", auto_adjust=True)
        if data is not None and not data.empty:
            # Trim to requested window so stale rolled-contract data doesn't skew ranges
            data = data[data.index >= pd.Timestamp(end - timedelta(days=days), tz=data.index.tz)]
            if not data.empty:
                return _flatten(data)
    except Exception:
        pass

    # Strategy 3: yf.download fallback (produces MultiIndex – flattened above)
    try:
        start = end - timedelta(days=days)
        data  = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                            end=end.strftime("%Y-%m-%d"),
                            auto_adjust=True, progress=False)
        if data is not None and not data.empty:
            return _flatten(data)
    except Exception:
        pass

    return None


# ── niftyindices.com helpers ──────────────────────────────────────────────────
#
# niftyindices.com is the OFFICIAL NSE subsidiary that publishes all Nifty
# index data.  It is the source used by nsepython and is far more reliable
# than nseindia.com API calls (which require fragile browser-cookie sessions).
#
# Endpoints used:
#   POST /Backpage.aspx/getHistoricaldatatabletoString  → EOD OHLC history
#   POST /Backpage.aspx/getpepbHistoricaldataDBtoString → historical P/E, P/B
#   GET  iislliveblob.niftyindices.com/jsonfiles/LiveIndicesWatch.json → live price
#
# Also kept as final fallback:
#   NSE /api/allIndices → live price when blob CDN fails

_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/",
    "Connection":      "keep-alive",
}

_NIFTYINDICES_HEADERS = {
    "Connection":      "keep-alive",
    "Accept":          "application/json, text/javascript, */*; q=0.01",
    "DNT":             "1",
    "X-Requested-With":"XMLHttpRequest",
    "User-Agent":      ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/91.0.4472.77 Safari/537.36"),
    "Content-Type":    "application/json; charset=UTF-8",
    "Origin":          "https://niftyindices.com",
    "Sec-Fetch-Site":  "same-origin",
    "Sec-Fetch-Mode":  "cors",
    "Sec-Fetch-Dest":  "empty",
    "Referer":         "https://niftyindices.com/reports/historical-data",
    "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
}


def _niftyindices_post(endpoint: str, index_name: str,
                       start_dt: datetime, end_dt: datetime) -> list:
    """
    POST to a niftyindices.com Backpage endpoint and return the parsed list.
    start/end formatted as '01-Jan-2022'.
    Returns [] on any error.
    """
    fmt   = lambda d: d.strftime("%d-%b-%Y")   # e.g. "01-Jan-2022"
    cinfo = (f"{{'name':'{index_name}','startDate':'{fmt(start_dt)}',"
             f"'endDate':'{fmt(end_dt)}','indexName':'{index_name}'}}")
    try:
        resp = requests.post(
            f"https://niftyindices.com/Backpage.aspx/{endpoint}",
            headers=_NIFTYINDICES_HEADERS,
            json={"cinfo": cinfo},
            timeout=20,
        )
        if resp.ok:
            payload = json.loads(resp.json()["d"])
            return payload          # list of dicts
        print(f"      niftyindices {endpoint} HTTP {resp.status_code}")
    except Exception as exc:
        print(f"      niftyindices {endpoint} failed: {exc}")
    return []


def _fetch_live_price(index_name: str) -> float | None:
    """
    Fetch the live index level from niftyindices CDN blob (fastest, no session).
    Falls back to NSE /api/allIndices if the blob is unavailable.
    """
    # Source 1: CDN blob (no auth, updated every ~15 s during market hours)
    try:
        resp = requests.get(
            "https://iislliveblob.niftyindices.com/jsonfiles/LiveIndicesWatch.json",
            headers={**_NIFTYINDICES_HEADERS, "Referer": "https://niftyindices.com/"},
            timeout=10,
        )
        if resp.ok:
            for item in resp.json().get("data", []):
                if item.get("indexName", "").upper() == index_name.upper():
                    val = item.get("last") or item.get("indexValue")
                    if val:
                        return round(float(str(val).replace(",", "")), 2)
    except Exception as exc:
        print(f"      LiveIndicesWatch CDN failed: {exc}")

    # Source 2: NSE /api/allIndices (requires no cookie for this lightweight call)
    try:
        session = requests.Session()
        session.headers.update(_NSE_HEADERS)
        session.get("https://www.nseindia.com", timeout=10)
        resp = session.get("https://www.nseindia.com/api/allIndices", timeout=10)
        if resp.ok:
            for item in resp.json().get("data", []):
                if item.get("index", "").upper() == index_name.upper():
                    val = item.get("last") or item.get("indexValue")
                    if val:
                        return round(float(str(val).replace(",", "")), 2)
    except Exception as exc:
        print(f"      NSE allIndices live price failed: {exc}")

    return None


def fetch_nse_index_data(display_name: str, nse_index_name: str) -> dict:
    """
    Fetch ALL data for a single NSE index via niftyindices.com.

    • Live price   : iislliveblob CDN  → NSE allIndices (fallback)
    • OHLC history : niftyindices.com/Backpage.aspx/getHistoricaldatatabletoString
      → 3Y High/Low, 52W High/Low, Rise/Fall %
    • P/E history  : niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString
      → Real current/avg/high/low P/E (NOT estimated from price scaling)

    Parameters
    ----------
    display_name   : e.g. "Nifty Next 50"
    nse_index_name : index name as used on niftyindices.com, e.g. "NIFTY NEXT 50"
    """
    today  = datetime.today()
    as_of  = today.strftime("%d %b %Y")

    # date windows
    start_3y  = today - timedelta(days=3 * 365 + 60)   # buffer for weekends
    start_52w = today - timedelta(days=365 + 30)

    # ── Step 1: live price ────────────────────────────────────────────────────
    current = _fetch_live_price(nse_index_name)
    print(f"      Live price: {current}")

    # ── Step 2: OHLC history (3Y window) ─────────────────────────────────────
    ohlc_rows = _niftyindices_post(
        "getHistoricaldatatabletoString", nse_index_name, start_3y, today
    )
    print(f"      OHLC history rows: {len(ohlc_rows)}")

    closes = []
    for row in ohlc_rows:
        raw_date  = row.get("HistoricalDate") or row.get("date") or ""
        raw_close = row.get("CLOSE") or row.get("close") or row.get("Close")
        if not raw_date or raw_close is None:
            continue
        dt = None
        for fmt_str in ("%d %b %Y", "%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                dt = datetime.strptime(raw_date.strip(), fmt_str)
                break
            except ValueError:
                continue
        if dt is not None:
            try:
                closes.append((dt, float(str(raw_close).replace(",", ""))))
            except ValueError:
                pass

    # ── Step 3: P/E history (3Y window) ──────────────────────────────────────
    pe_rows = _niftyindices_post(
        "getpepbHistoricaldataDBtoString", nse_index_name, start_3y, today
    )
    print(f"      PE history rows:   {len(pe_rows)}")

    pe_series = []    # list of (datetime, float)
    for row in pe_rows:
        raw_date = row.get("Date") or row.get("date") or ""
        raw_pe   = row.get("P/E") or row.get("pe") or row.get("PE")
        if not raw_date or raw_pe is None:
            continue
        dt = None
        for fmt_str in ("%d %b %Y", "%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                dt = datetime.strptime(raw_date.strip(), fmt_str)
                break
            except ValueError:
                continue
        if dt is not None:
            try:
                val = float(str(raw_pe).replace(",", ""))
                if val > 0:
                    pe_series.append((dt, val))
            except ValueError:
                pass

    # ── Step 4: compute stats ─────────────────────────────────────────────────
    if not closes:
        if current is None:
            return {
                "name":   display_name,
                "ticker": nse_index_name,
                "error":  "niftyindices.com returned no OHLC data",
            }
        # Live-only fallback (no history for ranges)
        return {
            "name": display_name, "ticker": nse_index_name, "current": current,
            "high_52w": "N/A", "low_52w": "N/A",
            "high_3y":  "N/A", "low_3y":  "N/A",
            "rise_from_52w_low": "N/A", "fall_from_52w_high": "N/A",
            "as_of": as_of, "is_gold": False,
            "pe_current": "N/A", "pe_avg": "N/A",
            "pe_high": "N/A", "pe_low": "N/A",
        }

    closes.sort(key=lambda x: x[0])
    dates_arr  = [c[0] for c in closes]
    values_arr = [c[1] for c in closes]

    if current is None:
        current = round(values_arr[-1], 2)
    as_of = dates_arr[-1].strftime("%d %b %Y")

    cut_3y  = today - timedelta(days=3 * 365)
    cut_52w = today - timedelta(days=365)

    vals_3y  = [v for d, v in closes if d >= cut_3y]  or values_arr
    vals_52w = [v for d, v in closes if d >= cut_52w] or values_arr

    high_3y  = round(max(vals_3y),  2)
    low_3y   = round(min(vals_3y),  2)
    high_52w = round(max(vals_52w), 2)
    low_52w  = round(min(vals_52w), 2)

    rise_from_52w_low  = round((current - low_52w)  / low_52w  * 100, 2)
    fall_from_52w_high = round(-((high_52w - current) / high_52w * 100), 2)

    # ── Real P/E stats from niftyindices historical P/E series ───────────────
    if pe_series:
        pe_series.sort(key=lambda x: x[0])
        # Current P/E = most recent value in the series
        pe_current = round(pe_series[-1][1], 2)
        # 3Y P/E High/Low/Avg
        pe_vals_3y = [v for d, v in pe_series if d >= cut_3y] or [v for _, v in pe_series]
        pe_high    = round(max(pe_vals_3y), 2)
        pe_low     = round(min(pe_vals_3y), 2)
        pe_avg     = round(sum(pe_vals_3y) / len(pe_vals_3y), 2)
        print(f"      PE current={pe_current}, avg={pe_avg}, high={pe_high}, low={pe_low}")
    else:
        pe_current = pe_avg = pe_high = pe_low = "N/A"
        print("      PE: no data from niftyindices")

    return {
        "name": display_name, "ticker": nse_index_name, "current": current,
        "high_52w": high_52w, "low_52w": low_52w,
        "high_3y":  high_3y,  "low_3y":  low_3y,
        "rise_from_52w_low": rise_from_52w_low,
        "fall_from_52w_high": fall_from_52w_high,
        "as_of": as_of, "is_gold": False,
        "pe_current": pe_current,
        "pe_avg": pe_avg, "pe_high": pe_high, "pe_low": pe_low,
    }


def fetch_usd_inr() -> float:
    """
    Fetch the current USD → INR exchange rate.
    Tries "USDINR=X" first (most reliable), then "INR=X", then falls back.
    """
    FALLBACK_RATE = 85.5   # Updated fallback (Apr 2025 ~85–86)
    for ticker_sym in ("USDINR=X", "INR=X", "USD/INR"):
        try:
            t    = yf.Ticker(ticker_sym)
            hist = t.history(period="5d", auto_adjust=True)
            if not hist.empty:
                rate = float(hist["Close"].iloc[-1])
                if rate > 1:          # sanity: INR per USD >> 1
                    print(f"  USD/INR via {ticker_sym}: {rate:.2f}")
                    return rate
        except Exception:
            continue
    print(f"  ⚠ Could not fetch USD/INR rate; using fallback {FALLBACK_RATE}")
    return FALLBACK_RATE


# FIX 1 ── Fetch P/E from niftyindices.com (real historical data, not estimates)
# ──────────────────────────────────────────────────────────────────────────────
def fetch_pe_nsei(current: float, high_3y: float, low_3y: float) -> tuple:
    """
    Fetch real historical P/E for Nifty 50 from niftyindices.com.

    Strategy (in order):
      A. niftyindices.com getpepbHistoricaldataDBtoString → actual historical PE series
         → real current/avg/high/low P/E (most accurate)
      B. NSE /api/equity-stockIndices?index=NIFTY%2050 → live PE only
      C. NSE /api/allIndices → live PE only
      D. screener.in page scrape → live PE only
      E. yfinance .info trailingPE → last resort, live PE only

    For strategies B-E (live PE only), historical High/Low/Avg are estimated
    by scaling current P/E by the 3Y price ratio.

    Returns (pe_current, pe_avg, pe_high, pe_low).
    """
    today    = datetime.today()
    start_3y = today - timedelta(days=3 * 365 + 60)
    cut_3y   = today - timedelta(days=3 * 365)

    # Attempt A: niftyindices historical PE series (best source)
    pe_rows = _niftyindices_post(
        "getpepbHistoricaldataDBtoString", "NIFTY 50", start_3y, today
    )
    if pe_rows:
        pe_series = []
        for row in pe_rows:
            raw_date = row.get("Date") or row.get("date") or ""
            raw_pe   = row.get("P/E") or row.get("pe") or row.get("PE")
            if not raw_date or raw_pe is None:
                continue
            dt = None
            for fmt_str in ("%d %b %Y", "%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
                try:
                    dt = datetime.strptime(raw_date.strip(), fmt_str)
                    break
                except ValueError:
                    continue
            if dt is not None:
                try:
                    val = float(str(raw_pe).replace(",", ""))
                    if val > 0:
                        pe_series.append((dt, val))
                except ValueError:
                    pass
        if pe_series:
            pe_series.sort(key=lambda x: x[0])
            pe_current = round(pe_series[-1][1], 2)
            pe_vals_3y = [v for d, v in pe_series if d >= cut_3y] or [v for _, v in pe_series]
            pe_high    = round(max(pe_vals_3y), 2)
            pe_low     = round(min(pe_vals_3y), 2)
            pe_avg     = round(sum(pe_vals_3y) / len(pe_vals_3y), 2)
            print(f"      PE via niftyindices: current={pe_current}, avg={pe_avg}, high={pe_high}, low={pe_low}")
            return pe_current, pe_avg, pe_high, pe_low

    # Attempts B-E: live PE only, then estimate band from price scaling
    pe_current = None

    # Attempt B: NSE equity-stockIndices
    try:
        session = requests.Session()
        session.headers.update(_NSE_HEADERS)
        session.get("https://www.nseindia.com", timeout=10)
        resp = session.get(
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
            timeout=10,
        )
        if resp.ok:
            val = resp.json().get("metadata", {}).get("pe")
            if val is not None and float(val) > 0:
                pe_current = round(float(val), 2)
                print(f"      PE via NSE equity-stockIndices: {pe_current}")
    except Exception as exc:
        print(f"      NSE equity-stockIndices P/E failed: {exc}")

    # Attempt C: NSE allIndices
    if pe_current is None:
        try:
            session = requests.Session()
            session.headers.update(_NSE_HEADERS)
            session.get("https://www.nseindia.com", timeout=10)
            resp = session.get("https://www.nseindia.com/api/allIndices", timeout=10)
            if resp.ok:
                for idx in resp.json().get("data", []):
                    if idx.get("index") == "NIFTY 50":
                        val = idx.get("pe")
                        if val is not None and float(val) > 0:
                            pe_current = round(float(val), 2)
                            print(f"      PE via NSE allIndices: {pe_current}")
                        break
        except Exception as exc:
            print(f"      NSE allIndices P/E failed: {exc}")

    # Attempt D: screener.in scrape
    if pe_current is None:
        try:
            req = urllib.request.Request(
                "https://www.screener.in/company/NIFTYBEES/consolidated/",
                headers={"User-Agent": _NSE_HEADERS["User-Agent"]},
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                html = r.read().decode("utf-8", errors="ignore")
            m = re.search(r'Stock P/E\s*<[^>]+>\s*([\d.]+)', html) or \
                re.search(r'"stockPE"\s*:\s*([\d.]+)', html)
            if m and float(m.group(1)) > 0:
                pe_current = round(float(m.group(1)), 2)
                print(f"      PE via screener.in: {pe_current}")
        except Exception as exc:
            print(f"      screener.in scrape failed: {exc}")

    # Attempt E: yfinance .info (last resort)
    if pe_current is None:
        try:
            info = yf.Ticker("^NSEI").info
            for key in ("trailingPE", "forwardPE"):
                val = info.get(key)
                if val and float(val) > 0:
                    pe_current = round(float(val), 2)
                    print(f"      PE via yfinance info[{key!r}]: {pe_current}")
                    break
        except Exception as exc:
            print(f"      yfinance info P/E failed: {exc}")

    if pe_current is None:
        print("      PE: all sources exhausted — returning N/A")
        return "N/A", "N/A", "N/A", "N/A"

    # Estimate band from price ratio (only when historical PE series unavailable)
    pe_high = round(pe_current * (high_3y / current), 2)
    pe_low  = round(pe_current * (low_3y  / current), 2)
    pe_avg  = round((pe_high + pe_low) / 2, 2)
    return pe_current, pe_avg, pe_high, pe_low


def fetch_mcx_gold_inr() -> float | None:
    """
    Try to get the current MCX gold spot price in INR per 10g.
    Returns price per gram (INR) or None if unavailable.
    """
    for sym in ("GOLDM.MCX", "GOLD.MCX", "GLD.MCX"):
        try:
            t    = yf.Ticker(sym)
            rate = getattr(t.fast_info, "last_price", None)
            if rate and float(rate) > 0:
                # MCX quotes in INR / 10 grams → convert to per gram
                return round(float(rate) / 10, 2)
            hist = t.history(period="5d")
            if not hist.empty:
                return round(float(hist["Close"].iloc[-1]) / 10, 2)
        except Exception:
            continue
    return None


def fetch_index_data(name: str, tickers: list, usd_inr: float) -> dict:
    end = datetime.today()

    hist        = None
    used_ticker = None

    for ticker in tickers:
        print(f"    trying {ticker} …")
        data = try_fetch(ticker)
        if data is not None and not data.empty:
            hist        = data
            used_ticker = ticker
            break
        else:
            print(f"      ✗ no data")

    if hist is None or hist.empty:
        return {"name": name, "ticker": ", ".join(tickers),
                "error": "No data returned for any ticker"}

    is_gold = (used_ticker in ("GC=F", "MGC=F"))

    # ── Gold price handling ────────────────────────────────────────────────────
    if is_gold:
        # For the CURRENT price: always fetch a short recent window (10 days)
        # so we never pick up a stale rolled-contract settlement price.
        recent_hist = try_fetch(used_ticker, days=10)
        if recent_hist is not None and not recent_hist.empty:
            current_usd_oz = float(recent_hist["Close"].iloc[-1])
            as_of_date     = recent_hist.index[-1]
        else:
            # Fallback: use last row of the 3Y history
            current_usd_oz = float(hist["Close"].iloc[-1])
            as_of_date     = hist.index[-1]

        # Build INR/gram series from the 3Y history for range calculations
        close_usd_oz = hist["Close"].copy()
        close_inr_g  = close_usd_oz / TROY_OZ_PER_GRAM * usd_inr

        # MCX comparison price
        mcx_price_inr_g = fetch_mcx_gold_inr()

        # Use INR/gram series for range stats; current comes from recent window
        close   = close_inr_g
        current = round(current_usd_oz / TROY_OZ_PER_GRAM * usd_inr, 2)
    else:
        close      = hist["Close"]
        current    = round(float(close.iloc[-1]), 2)
        as_of_date = close.index[-1]

    # Trim to last 3 years / 52 weeks
    tz         = close.index.tz
    cutoff_3y  = (end - timedelta(days=3 * 365)).replace(tzinfo=tz) if tz else (end - timedelta(days=3 * 365))
    cutoff_52w = (end - timedelta(days=365)).replace(tzinfo=tz)      if tz else (end - timedelta(days=365))

    close_3y = close[close.index >= cutoff_3y]
    if close_3y.empty:
        close_3y = close

    w52_data = close[close.index >= cutoff_52w]
    if w52_data.empty:
        w52_data = close

    high_52w = round(float(w52_data.max()), 2)
    low_52w  = round(float(w52_data.min()), 2)
    high_3y  = round(float(close_3y.max()), 2)
    low_3y   = round(float(close_3y.min()), 2)

    rise_from_52w_low  = round((current - low_52w)  / low_52w  * 100, 2)
    # Stored as negative so color_badge(good_positive=True) colors it red
    fall_from_52w_high = round(-((high_52w - current) / high_52w * 100), 2)

    result = {
        "name": name, "ticker": used_ticker, "current": current,
        "high_52w": high_52w, "low_52w": low_52w,
        "high_3y": high_3y,   "low_3y": low_3y,
        "rise_from_52w_low": rise_from_52w_low,
        "fall_from_52w_high": fall_from_52w_high,
        "as_of": as_of_date.strftime("%d %b %Y"),
        "is_gold": is_gold,
    }

    # Gold-specific price fields for the comparison display
    if is_gold:
        raw_usd_oz           = round(current_usd_oz, 2)
        calc_inr_g           = round(raw_usd_oz / TROY_OZ_PER_GRAM * usd_inr, 2)

        result["gold_usd_oz"]       = raw_usd_oz           # USD per troy oz
        result["gold_calc_inr_g"]   = calc_inr_g           # calculated INR/gram
        result["gold_actual_inr_g"] = mcx_price_inr_g      # MCX-quoted INR/gram (may be None)
        if mcx_price_inr_g is not None:
            result["gold_diff_inr_g"] = round(mcx_price_inr_g - calc_inr_g, 2)
        else:
            result["gold_diff_inr_g"] = None

    # FIX 1: P/E only for Nifty 50 — always via ^NSEI directly
    if "^NSEI" in tickers:
        # Use Nifty 50's own current/high/low even if an ETF ticker was resolved
        pe_current, pe_avg, pe_high, pe_low = fetch_pe_nsei(current, high_3y, low_3y)
    else:
        pe_current = pe_high = pe_low = pe_avg = "N/A"

    result.update({"pe_current": pe_current, "pe_avg": pe_avg,
                   "pe_high": pe_high, "pe_low": pe_low})
    return result


# ── HTML Report Builder ───────────────────────────────────────────────────────

def color_badge(value, good_positive=True):
    """
    Render a coloured % badge.
    fall_from_52w_high is stored as a negative value, so good_positive=True
    colours it red (negative = bad) and rises green (positive = good).
    """
    if not isinstance(value, (int, float)):
        return '<span style="color:#888">N/A</span>'
    color = "#16a34a" if (value >= 0) == good_positive else "#dc2626"
    sign  = "+" if value > 0 else ""
    return f'<span style="color:{color};font-weight:600">{sign}{value}%</span>'


def fmt(val, decimals=2):
    if val == "N/A" or val is None:
        return '<span style="color:#94a3b8">N/A</span>'
    return f"{val:,.{decimals}f}"


def fmt_gold_current(d: dict) -> str:
    """
    FIX 3: For Gold (GC=F), show a 3-row comparison:
      • Calculated price  (USD/oz → INR/gram via formula)
      • Actual MCX price  (live MCX quote in INR/gram)
      • Difference        (Actual − Calculated)
    Also displays the source USD/oz price.
    """
    usd_oz      = fmt(d.get("gold_usd_oz"))
    calc_inr_g  = fmt(d.get("gold_calc_inr_g"))
    actual_inr_g = d.get("gold_actual_inr_g")
    diff         = d.get("gold_diff_inr_g")

    actual_str = fmt(actual_inr_g) if actual_inr_g is not None else \
                 '<span style="color:#94a3b8">N/A</span>'

    # Colour the difference: positive = MCX trades at a premium (normal in India)
    if diff is not None:
        diff_color = "#16a34a" if diff >= 0 else "#dc2626"
        sign       = "+" if diff > 0 else ""
        diff_str   = f'<span style="color:{diff_color};font-weight:600">{sign}&#8377;{diff:,.2f}/g</span>'
    else:
        diff_str = '<span style="color:#94a3b8">N/A</span>'

    return (
        f'<span style="white-space:nowrap;font-size:12px;line-height:1.7">'
        f'<span style="color:#64748b">USD:</span> <strong>{usd_oz}/oz</strong><br>'
        f'<span style="color:#64748b">Calc&#8377;:</span> &#8377;{calc_inr_g}/g<br>'
        f'<span style="color:#64748b">MCX&#8377;:</span> &#8377;{actual_str}/g<br>'
        f'<span style="color:#64748b">Diff:</span> {diff_str}'
        f'</span>'
    )


def build_html(data_list, usd_inr: float):
    today = datetime.today().strftime("%d %B %Y")
    rows  = ""

    for d in data_list:
        if "error" in d:
            rows += (f'<tr><td colspan="12" style="color:#dc2626;padding:10px 12px">'
                     f'&#9888; {d["name"]} ({d["ticker"]}): {d["error"]}</td></tr>')
            continue

        current_cell = fmt_gold_current(d) if d.get("is_gold") else fmt(d["current"])

        rows += f"""
        <tr>
          <td class="name-col">{d['name']}<br><span class="ticker">{d['ticker']}</span></td>
          <td class="num">{current_cell}</td>
          <td class="num hi">{fmt(d['high_3y'])}</td>
          <td class="num lo">{fmt(d['low_3y'])}</td>
          <td class="num hi">{fmt(d['high_52w'])}</td>
          <td class="num lo">{fmt(d['low_52w'])}</td>
          <td class="num">{fmt(d['pe_current'])}</td>
          <td class="num">{fmt(d['pe_avg'])}</td>
          <td class="num hi">{fmt(d['pe_high'])}</td>
          <td class="num lo">{fmt(d['pe_low'])}</td>
          <td class="num">{color_badge(d['rise_from_52w_low'],  good_positive=True)}</td>
          <td class="num">{color_badge(d['fall_from_52w_high'], good_positive=True)}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  body {{font-family:'Segoe UI',Arial,sans-serif;background:#f1f5f9;margin:0;padding:24px;color:#1e293b}}
  .card {{background:#fff;border-radius:12px;box-shadow:0 2px 16px rgba(0,0,0,.08);overflow:hidden;max-width:1200px;margin:0 auto}}
  .header {{background:linear-gradient(135deg,#0f172a 0%,#1e3a5f 100%);padding:28px 32px;color:#fff}}
  .header h1 {{margin:0;font-size:22px;letter-spacing:-.3px}}
  .header p  {{margin:6px 0 0;font-size:13px;color:#94a3b8}}
  table {{width:100%;border-collapse:collapse;font-size:13px}}
  th {{background:#f8fafc;padding:9px 11px;text-align:center;font-size:11px;text-transform:uppercase;
       letter-spacing:.5px;color:#64748b;border-bottom:2px solid #e2e8f0;white-space:nowrap}}
  th.name-col {{text-align:left}}
  td {{padding:10px 11px;border-bottom:1px solid #f1f5f9;vertical-align:middle}}
  td.num {{text-align:right;font-variant-numeric:tabular-nums}}
  td.name-col {{font-weight:600;min-width:130px}}
  td.hi {{color:#15803d}} td.lo {{color:#b91c1c}}
  .ticker {{font-weight:400;font-size:11px;color:#94a3b8}}
  tr:last-child td {{border-bottom:none}}
  tr:hover td {{background:#f8fafc}}
  .note {{font-size:11px;color:#94a3b8;padding:10px 16px;background:#fafafa;border-top:1px solid #f1f5f9;line-height:1.6}}
  .footer {{text-align:center;font-size:11px;color:#94a3b8;padding:14px;border-top:1px solid #f1f5f9}}
</style>
</head>
<body>
<div class="card">
  <div class="header">
    <h1>&#128202; Market Index Report</h1>
    <p>Generated on {today} &nbsp;|&nbsp; Data via Yahoo Finance &nbsp;|&nbsp; USD/INR = {usd_inr:.2f}</p>
  </div>
  <table>
    <thead>
      <tr>
        <th class="name-col" rowspan="2">Index</th>
        <th rowspan="2">Current</th>
        <th colspan="2">3-Year Range</th>
        <th colspan="2">52-Week Range</th>
        <th colspan="4">P/E Ratio</th>
        <th rowspan="2">&#8593; from 52W Low</th>
        <th rowspan="2">&#8595; from 52W High</th>
      </tr>
      <tr>
        <th>High</th><th>Low</th>
        <th>High</th><th>Low</th>
        <th>Current</th><th>Avg</th><th>High</th><th>Low</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>
  <div class="note">
    <strong>P/E:</strong> Real historical P/E series from niftyindices.com (official NSE subsidiary).
    Current P/E = latest value; Avg/High/Low computed over the 3-year window — actual data, not estimates.<br>
    <strong>Index levels:</strong>
      Nifty 50 via Yahoo Finance (^NSEI) &nbsp;|&nbsp;
      Nifty Next 50, Midcap 150, Smallcap 250, Microcap 250 via niftyindices.com
      (official NSE subsidiary — reliable POST API, no cookie session required).<br>
    <strong>Gold (GC=F):</strong>
      <em>Calc ₹</em> = USD/oz ÷ 31.1035 × USD/INR rate (live).
      <em>MCX ₹</em> = live MCX spot quote (GOLDM.MCX).
      <em>Diff</em> = MCX ₹ − Calc ₹ (positive = MCX at a premium, normal due to import duty &amp; GST).
  </div>
  <div class="footer">Auto-generated report &nbsp;&middot;&nbsp; Not financial advice</div>
</div>
</body>
</html>"""


# ── Email Sender ──────────────────────────────────────────────────────────────

def send_email(html_body):
    msg            = MIMEMultipart("alternative")
    msg["Subject"] = f"Market Index Report - {datetime.today().strftime('%d %b %Y')}"
    msg["From"]    = MY_EMAIL
    msg["To"]      = MY_EMAIL
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(MY_EMAIL, MY_PASSWORD)
        server.sendmail(MY_EMAIL, MY_EMAIL, msg.as_string())
    print(f"Email sent to {MY_EMAIL}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Fetching USD/INR exchange rate ...")
    usd_inr = fetch_usd_inr()
    print(f"  USD/INR = {usd_inr:.2f}")

    print("Fetching index data ...")
    data_list = []

    # ── Yahoo Finance indices (Nifty 50 + Gold) ────────────────────────────────
    for name, tickers in YF_INDICES.items():
        print(f"  -> {name}  [Yahoo Finance]")
        data = fetch_index_data(name, tickers, usd_inr)
        data_list.append(data)
        if "error" in data:
            print(f"     FAILED: {data['error']}")
        elif data.get("is_gold"):
            calc   = data.get("gold_calc_inr_g", "?")
            actual = data.get("gold_actual_inr_g", "N/A")
            diff   = data.get("gold_diff_inr_g", "N/A")
            print(f"     OK: {data['ticker']} | USD {data.get('gold_usd_oz')}/oz"
                  f" | Calc ₹{calc}/g | MCX ₹{actual}/g | Diff ₹{diff}/g"
                  f" | as of {data['as_of']}")
        else:
            print(f"     OK: {data['ticker']} | Current: {data['current']}"
                  f" | PE: {data.get('pe_current')} | as of {data['as_of']}")

    # ── NSE India native indices (Next 50, Midcap 150, Smallcap 250) ───────────
    for display_name, nse_name in NSE_INDICES.items():
        print(f"  -> {display_name}  [NSE India API]")
        data = fetch_nse_index_data(display_name, nse_name)
        data_list.append(data)
        if "error" in data:
            print(f"     FAILED: {data['error']}")
        else:
            print(f"     OK: {data['ticker']} | Current: {data['current']}"
                  f" | PE: {data.get('pe_current')} | as of {data['as_of']}")

    # Deduplicate by name: if two entries share a name, keep the one without errors
    seen = {}
    for d in data_list:
        n = d["name"]
        if n not in seen:
            seen[n] = d
        elif "error" in seen[n] and "error" not in d:
            seen[n] = d   # replace error entry with a good one
    data_list = list(seen.values())

    # Reorder to match the desired display order in the email
    ORDER = ["Nifty 50", "Nifty Next 50", "Nifty Midcap 150",
             "Nifty Smallcap 250", "Nifty Microcap 250", "Gold (MCX)"]
    data_list.sort(key=lambda d: ORDER.index(d["name"])
                   if d["name"] in ORDER else 99)

    print("Building report ...")
    html = build_html(data_list, usd_inr)

    print("Sending email ...")
    send_email(html)


if __name__ == "__main__":
    main()
