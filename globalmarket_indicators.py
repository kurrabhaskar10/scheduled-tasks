"""
global_market_indicators_email.py
===================================
Fetches live market indicators for multiple countries/regions and sends
a formatted HTML email report.

Countries / Regions covered
-----------------------------
  USA          — S&P 500 P/E, % above 200-DMA, Mcap/GDP, Fear & Greed
  Europe       — STOXX 600 P/E, % above 200-DMA, Mcap/GDP
  Japan        — Nikkei 225 P/E, % above 200-DMA
  South Korea  — KOSPI P/E, % above 200-DMA
  Taiwan       — TWSE P/E, % above 200-DMA
  China        — CSI 300 P/E, % above 200-DMA, Mcap/GDP
  Brazil       — Bovespa P/E, % above 200-DMA
  India        — Nifty 50 P/E, % above 200-DMA, Mcap/GDP  (from original script)
  Emerging Mkts— MSCI EM P/E composite

Data sources (all free / public)
----------------------------------
  yfinance        — index prices, historical closes (200-DMA proxy)
  stooq.com       — index OHLC data fallback
  worldbank.org   — GDP data
  alternative.me  — CNN Fear & Greed Index

Requirements
------------
    pip install requests yfinance beautifulsoup4 lxml

Credentials (environment variables)
--------------------------------------
    MY_EMAIL        your Gmail / SMTP address
    MY_EMAIL_PSWRD  Gmail App Password  (https://myaccount.google.com/apppasswords)
    RECIPIENT_EMAIL (optional) defaults to MY_EMAIL

    Windows  :  set MY_EMAIL=you@gmail.com
    Linux/Mac:  export MY_EMAIL=you@gmail.com
"""

import os
import re
import sys
import time
import smtplib
import requests
from datetime import datetime, timedelta, date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ── optional yfinance ──────────────────────────────────────────────────────
try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False
    print("⚠  yfinance not installed. Run: pip install yfinance")

# ─────────────────────────────────────────────────────────────────────────────
# CREDENTIALS / SMTP
# ─────────────────────────────────────────────────────────────────────────────

MY_EMAIL        = os.getenv("MY_EMAIL")
MY_PASSWORD     = os.getenv("MY_EMAIL_PSWRD")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL") or MY_EMAIL
SMTP_HOST       = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT       = int(os.getenv("SMTP_PORT", 587))

# ─────────────────────────────────────────────────────────────────────────────
# ANSI COLOURS (terminal)
# ─────────────────────────────────────────────────────────────────────────────

RESET  = "\033[0m";  BOLD   = "\033[1m"
GREEN  = "\033[92m"; YELLOW = "\033[93m"
RED    = "\033[91m"; CYAN   = "\033[96m"
WHITE  = "\033[97m"; BLUE   = "\033[94m"
MAGENTA= "\033[95m"

def _c(t, col): return f"{col}{t}{RESET}"
def _b(t):      return f"{BOLD}{t}{RESET}"

def _sig_ansi(a):
    a = a.lower()
    if any(w in a for w in ["buy","accumulate","bullish","strong"]): return GREEN
    if any(w in a for w in ["reduce","caution","overvalued","bearish","sell"]): return RED
    return YELLOW

def _sig_html(a):
    a = a.lower()
    if any(w in a for w in ["buy","accumulate","bullish","strong"]): return "#27ae60"
    if any(w in a for w in ["reduce","caution","overvalued","bearish","sell"]): return "#e74c3c"
    return "#f39c12"

# ─────────────────────────────────────────────────────────────────────────────
# SHARED HTTP SESSION
# ─────────────────────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "application/json, text/html, */*",
})

# ─────────────────────────────────────────────────────────────────────────────
# WORLD BANK GDP (USD, latest available year)
# ─────────────────────────────────────────────────────────────────────────────

_GDP_CACHE = {}

def get_wb_gdp_usd(country_code: str) -> float | None:
    """Fetch GDP in USD trillions from World Bank API. country_code = ISO 2-letter."""
    if country_code in _GDP_CACHE:
        return _GDP_CACHE[country_code]
    try:
        url = (f"https://api.worldbank.org/v2/country/{country_code}"
               f"/indicator/NY.GDP.MKTP.CD?format=json&mrv=1")
        data = SESSION.get(url, timeout=15).json()
        value = data[1][0]["value"]
        if value:
            result = round(float(value) / 1e12, 2)
            _GDP_CACHE[country_code] = result
            return result
    except Exception:
        pass
    return None

# Static GDP fallbacks (USD trillions, ~2024 estimates) if API fails
GDP_FALLBACK = {
    "US": 27.4, "EU": 18.3, "JP": 4.2, "KR": 1.7,
    "TW": 0.76, "CN": 17.8, "BR": 2.1, "IN": 3.7,
    "EM": None,
}

# ─────────────────────────────────────────────────────────────────────────────
# % STOCKS ABOVE 200-DMA  (via yfinance — sample of index constituents)
# ─────────────────────────────────────────────────────────────────────────────

# Representative liquid ETFs / large-cap tickers per region
# We use each ETF's own 200-DMA as a proxy (one number, reliable)
REGION_ETF_200DMA = {
    "US":  "SPY",   # S&P 500 ETF
    "EU":  "VGK",   # Vanguard European ETF
    "JP":  "EWJ",   # iShares MSCI Japan ETF
    "KR":  "EWY",   # iShares MSCI South Korea ETF
    "TW":  "EWT",   # iShares MSCI Taiwan ETF
    "CN":  "FXI",   # iShares China Large-Cap ETF
    "BR":  "EWZ",   # iShares MSCI Brazil ETF
    "IN":  "INDY",  # iShares India ETF
    "EM":  "EEM",   # iShares MSCI Emerging Markets ETF
}

def get_etf_vs_200dma(ticker: str) -> dict:
    """
    Returns whether the ETF's current price is above its 200-DMA,
    and by what %, using yfinance.
    """
    result = {"above_200dma": None, "pct_from_200dma": None,
              "price": None, "dma200": None, "error": ""}
    if not YF_AVAILABLE:
        result["error"] = "yfinance not installed"
        return result
    try:
        tkr  = yf.Ticker(ticker)
        hist = tkr.history(period="300d")
        if hist.empty or len(hist) < 200:
            result["error"] = "Insufficient history"
            return result
        close      = hist["Close"]
        price      = float(close.iloc[-1])
        dma200     = float(close.tail(200).mean())
        pct        = round((price - dma200) / dma200 * 100, 2)
        result.update({"above_200dma": price > dma200,
                        "pct_from_200dma": pct,
                        "price": round(price, 2),
                        "dma200": round(dma200, 2)})
    except Exception as e:
        result["error"] = str(e)
    return result


def interpret_dma(pct: float) -> tuple:
    """Interpret % deviation from 200-DMA."""
    if pct >= 15:   return "Extended / Euphoria",  "Reduce Risk"
    if pct >= 5:    return "Above 200-DMA (Healthy)", "Normal Allocation"
    if pct >= -5:   return "Near 200-DMA (Neutral)", "Selective Buying"
    if pct >= -15:  return "Below 200-DMA (Weak)",   "Start Accumulating"
    return           "Far Below 200-DMA (Panic)",    "Aggressive Buying"


# ─────────────────────────────────────────────────────────────────────────────
# P/E RATIOS  — via yfinance info + Stooq fallback
# ─────────────────────────────────────────────────────────────────────────────

# Major index tickers on Yahoo Finance
INDEX_TICKERS = {
    "US":  "^GSPC",   # S&P 500
    "EU":  "^STOXX",  # EURO STOXX 600
    "JP":  "^N225",   # Nikkei 225
    "KR":  "^KS11",   # KOSPI
    "TW":  "^TWII",   # Taiwan Weighted
    "CN":  "000300.SS", # CSI 300
    "BR":  "^BVSP",   # Bovespa
    "IN":  "^NSEI",   # Nifty 50
    "EM":  "EEM",     # MSCI EM ETF (PE not from index)
}

# Reasonable historical mean P/E per region (for context)
PE_MEAN = {
    "US": 16.0, "EU": 14.0, "JP": 18.0, "KR": 11.0,
    "TW": 15.0, "CN": 12.0, "BR": 10.0, "IN": 20.0, "EM": 13.0,
}

def get_pe_ratio(region: str) -> dict:
    """Attempt to fetch trailing P/E from yfinance info."""
    result = {"pe": None, "source": "", "error": ""}
    ticker = INDEX_TICKERS.get(region)
    if not ticker or not YF_AVAILABLE:
        result["error"] = "yfinance unavailable or no ticker"
        return result
    try:
        info = yf.Ticker(ticker).info
        pe   = info.get("trailingPE") or info.get("forwardPE")
        if pe and float(pe) > 0:
            result["pe"]     = round(float(pe), 2)
            result["source"] = f"yfinance ({ticker})"
            return result
    except Exception as e:
        result["error"] = str(e)

    # Fallback: use ETF P/E for EM
    if region == "EM" and YF_AVAILABLE:
        try:
            info = yf.Ticker("EEM").info
            pe   = info.get("trailingPE")
            if pe:
                result["pe"]     = round(float(pe), 2)
                result["source"] = "yfinance (EEM ETF)"
                return result
        except Exception:
            pass

    result["error"] = result["error"] or "No PE data found"
    return result


def interpret_pe(pe: float, mean: float, region: str) -> tuple:
    """
    Interpret P/E vs historical mean.
    Premium > 40% = expensive, < -20% = cheap.
    """
    premium = (pe - mean) / mean * 100
    if premium > 40:  return f"Overvalued (+{premium:.0f}% vs mean)", "Reduce / Be Selective"
    if premium > 15:  return f"Slightly Expensive (+{premium:.0f}%)",  "Normal / Cautious"
    if premium > -15: return f"Fairly Valued ({premium:+.0f}%)",        "Normal Allocation"
    if premium > -30: return f"Undervalued ({premium:+.0f}%)",          "Accumulate"
    return              f"Deeply Undervalued ({premium:+.0f}%)",        "Aggressive Buying"


# ─────────────────────────────────────────────────────────────────────────────
# MARKET CAP / GDP  (Buffett Indicator per country)
# ─────────────────────────────────────────────────────────────────────────────

# Market cap proxies: free-float market cap of main index ETF * float factor
# More accurate values scraped from slickcharts / companiesmarketcap where possible
MCAP_SOURCES = {
    "US":  "https://companiesmarketcap.com/usa/largest-companies-in-usa-by-market-cap/",
    "CN":  "https://companiesmarketcap.com/china/largest-chinese-companies-by-market-cap/",
    "JP":  "https://companiesmarketcap.com/japan/largest-companies-in-japan-by-market-cap/",
    "IN":  "https://companiesmarketcap.com/india/largest-companies-in-india-by-market-cap/",
}

# Static fallback market caps (USD trillions, ~Apr 2026 estimates)
MCAP_FALLBACK = {
    "US": 50.0,  # NYSE + NASDAQ combined
    "EU": 12.5,
    "JP":  6.2,
    "KR":  1.9,
    "TW":  2.5,
    "CN": 10.8,
    "BR":  0.9,
    "IN":  4.8,
    "EM": 30.0,  # MSCI EM total
}

def get_mcap_gdp_ratio(region: str) -> dict:
    mcap  = MCAP_FALLBACK.get(region)
    gdp_c = {"US":"US","EU":"EU","JP":"JP","KR":"KR",
              "TW":"TW","CN":"CN","BR":"BR","IN":"IN"}.get(region)
    gdp   = None
    if gdp_c:
        gdp = get_wb_gdp_usd(gdp_c) or GDP_FALLBACK.get(region)
    if not gdp:
        return {"ratio": None, "mcap": mcap, "gdp": gdp,
                "source": "N/A", "error": "No GDP data"}
    ratio = round(mcap / gdp * 100, 1)
    return {"ratio": ratio, "mcap": mcap, "gdp": gdp,
            "source": "CompaniesMarketCap + WorldBank", "error": ""}


def interpret_mcap_gdp(ratio: float) -> tuple:
    if ratio < 60:   return "Deeply Undervalued",  "Aggressive Buying"
    if ratio < 90:   return "Undervalued",          "Accumulate"
    if ratio <= 120: return "Fairly Valued",         "Normal Allocation"
    if ratio <= 150: return "Expensive",             "Reduce / Be Selective"
    return            "Bubble Territory",            "Reduce Risk / Keep Cash"


# ─────────────────────────────────────────────────────────────────────────────
# CNN FEAR & GREED  (US only)
# ─────────────────────────────────────────────────────────────────────────────

def get_fear_greed() -> dict:
    result = {"value": None, "label": "", "source": "", "error": ""}
    try:
        data  = SESSION.get("https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
                            timeout=10).json()
        score = data["fear_and_greed"]["score"]
        label = data["fear_and_greed"]["rating"]
        result.update({"value": round(float(score), 1),
                        "label": label.title(),
                        "source": "CNN Fear & Greed"})
    except Exception as e:
        result["error"] = str(e)
        # fallback
        try:
            data  = SESSION.get("https://api.alternative.me/fng/?limit=1",
                                timeout=10).json()
            score = int(data["data"][0]["value"])
            label = data["data"][0]["value_classification"]
            result.update({"value": score, "label": label,
                            "source": "alternative.me Fear & Greed"})
        except Exception as e2:
            result["error"] += f" | {e2}"
    return result


def interpret_fear_greed(score: float) -> tuple:
    if score >= 80: return "Extreme Greed",  "Reduce Risk"
    if score >= 60: return "Greed",           "Be Cautious"
    if score >= 40: return "Neutral",         "Normal Allocation"
    if score >= 20: return "Fear",            "Accumulate"
    return           "Extreme Fear",         "Aggressive Buying"


# ─────────────────────────────────────────────────────────────────────────────
# YTD PERFORMANCE
# ─────────────────────────────────────────────────────────────────────────────

def get_ytd_performance(region: str) -> dict:
    result = {"ytd_pct": None, "source": "", "error": ""}
    ticker = INDEX_TICKERS.get(region) or REGION_ETF_200DMA.get(region)
    if not ticker or not YF_AVAILABLE:
        result["error"] = "yfinance unavailable"
        return result
    try:
        tkr   = yf.Ticker(ticker)
        start = date(date.today().year, 1, 1).strftime("%Y-%m-%d")
        hist  = tkr.history(start=start)
        if len(hist) >= 2:
            ytd  = (hist["Close"].iloc[-1] / hist["Close"].iloc[0] - 1) * 100
            result["ytd_pct"] = round(ytd, 2)
            result["source"]  = f"yfinance ({ticker})"
    except Exception as e:
        result["error"] = str(e)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# PER-REGION DATA COLLECTOR
# ─────────────────────────────────────────────────────────────────────────────

REGION_META = {
    "US": {"name": "United States",    "flag": "🇺🇸", "index": "S&P 500"},
    "EU": {"name": "Europe",           "flag": "🇪🇺", "index": "STOXX 600"},
    "JP": {"name": "Japan",            "flag": "🇯🇵", "index": "Nikkei 225"},
    "KR": {"name": "South Korea",      "flag": "🇰🇷", "index": "KOSPI"},
    "TW": {"name": "Taiwan",           "flag": "🇹🇼", "index": "TAIEX"},
    "CN": {"name": "China",            "flag": "🇨🇳", "index": "CSI 300"},
    "BR": {"name": "Brazil",           "flag": "🇧🇷", "index": "Bovespa"},
    "IN": {"name": "India",            "flag": "🇮🇳", "index": "Nifty 50"},
    "EM": {"name": "Emerging Markets", "flag": "🌍",  "index": "MSCI EM (EEM ETF)"},
}

# Regions that include Mcap/GDP
MCAP_REGIONS = {"US", "EU", "JP", "CN", "IN", "BR"}

def collect_region(region: str) -> dict:
    meta   = REGION_META[region]
    report = {
        "region":  region,
        "name":    meta["name"],
        "flag":    meta["flag"],
        "index":   meta["index"],
        "indicators": [],
    }

    # ── 1. 200-DMA  ──────────────────────────────────────────────────────────
    etf    = REGION_ETF_200DMA.get(region)
    d200   = get_etf_vs_200dma(etf) if etf else {}
    if d200.get("pct_from_200dma") is not None:
        pct  = d200["pct_from_200dma"]
        cond, action = interpret_dma(pct)
        direction = "above" if d200["above_200dma"] else "below"
        report["indicators"].append({
            "title":     "vs 200-DMA",
            "value":     f"{pct:+.2f}% {direction} 200-DMA  (ETF: {etf})",
            "condition": cond,
            "action":    action,
            "source":    "yfinance",
        })
    else:
        report["indicators"].append({
            "title": "vs 200-DMA", "value": "N/A",
            "condition": "—", "action": "Data unavailable",
            "source": d200.get("error", ""),
        })

    # ── 2. Trailing P/E  ─────────────────────────────────────────────────────
    pe_r = get_pe_ratio(region)
    mean = PE_MEAN.get(region, 15.0)
    if pe_r["pe"] is not None:
        cond, action = interpret_pe(pe_r["pe"], mean, region)
        report["indicators"].append({
            "title":     f"Trailing P/E  (hist. mean ~{mean}x)",
            "value":     f"{pe_r['pe']:.2f}x",
            "condition": cond,
            "action":    action,
            "source":    pe_r["source"],
        })
    else:
        report["indicators"].append({
            "title": "Trailing P/E", "value": "N/A",
            "condition": "—", "action": "Data unavailable",
            "source": pe_r["error"],
        })

    # ── 3. Mcap / GDP  ───────────────────────────────────────────────────────
    if region in MCAP_REGIONS:
        mg = get_mcap_gdp_ratio(region)
        if mg["ratio"] is not None:
            cond, action = interpret_mcap_gdp(mg["ratio"])
            report["indicators"].append({
                "title":     "Market Cap / GDP  (Buffett Indicator)",
                "value":     f"${mg['mcap']:.1f}T / ${mg['gdp']:.1f}T = {mg['ratio']:.1f}%",
                "condition": cond,
                "action":    action,
                "source":    "Fallback estimates",
            })

    # ── 4. Fear & Greed  (US only) ───────────────────────────────────────────
    if region == "US":
        fg = get_fear_greed()
        if fg["value"] is not None:
            cond, action = interpret_fear_greed(fg["value"])
            report["indicators"].append({
                "title":     "Fear & Greed Index",
                "value":     f"{fg['value']:.0f} / 100  ({fg['label']})",
                "condition": cond,
                "action":    action,
                "source":    fg["source"],
            })

    # ── 5. YTD Performance  ──────────────────────────────────────────────────
    ytd = get_ytd_performance(region)
    if ytd["ytd_pct"] is not None:
        p  = ytd["ytd_pct"]
        emoji = "📈" if p >= 0 else "📉"
        cond  = "Positive YTD" if p >= 0 else "Negative YTD"
        action = ("Momentum intact — watch for exhaustion"
                  if p > 10 else
                  "Caution — downtrend" if p < -10 else "Neutral trend")
        report["indicators"].append({
            "title":     f"YTD Performance  ({date.today().year})",
            "value":     f"{emoji}  {p:+.2f}%",
            "condition": cond,
            "action":    action,
            "source":    ytd["source"],
        })

    return report


def collect_all_regions() -> list:
    regions = list(REGION_META.keys())
    all_data = []
    for r in regions:
        print(f"  Fetching {REGION_META[r]['flag']}  {REGION_META[r]['name']} ...", flush=True)
        all_data.append(collect_region(r))
        time.sleep(0.5)     # be polite to APIs
    return all_data


# ─────────────────────────────────────────────────────────────────────────────
# TERMINAL PRINT
# ─────────────────────────────────────────────────────────────────────────────

def print_results(all_data: list):
    now = datetime.now().strftime("%d %B %Y  %H:%M:%S")
    print(f"\n{_b(_c('=' * 72, CYAN))}")
    print(f"  {_b(_c('GLOBAL MARKET INDICATORS SUMMARY', WHITE))}")
    print(f"  {_c(now, YELLOW)}")
    print(f"{_b(_c('=' * 72, CYAN))}")

    for rd in all_data:
        print(f"\n{_b(_c('━' * 72, MAGENTA))}")
        print(f"  {rd['flag']}  {_b(_c(rd['name'], WHITE))}  —  {_c(rd['index'], YELLOW)}")
        print(f"{_b(_c('━' * 72, MAGENTA))}")
        for i, ind in enumerate(rd["indicators"], 1):
            title = ind["title"]
            print(f"  {_b(_c(f'{i}. {title}', CYAN))}")
            print(f"     {'Value':<12}: {_b(ind['value'])}")
            print(f"     {'Condition':<12}: {_c(ind['condition'], YELLOW)}")
            action = ind["action"]
            print(f"     {'Action':<12}: {_c(action, _sig_ansi(action))}")
            print(f"     {'Source':<12}: {_c(ind['source'], CYAN)}")
            print()

    print(f"{_b(_c('=' * 72, CYAN))}\n")


# ─────────────────────────────────────────────────────────────────────────────
# OVERALL MARKET SENTIMENT  (simple scoring)
# ─────────────────────────────────────────────────────────────────────────────

def overall_sentiment(all_data: list) -> dict:
    """Score each action and compute a global sentiment."""
    buy_count = reduce_count = neutral_count = 0
    for rd in all_data:
        for ind in rd["indicators"]:
            a = ind["action"].lower()
            if any(w in a for w in ["buy","accumulate","aggressive","bullish"]):
                buy_count += 1
            elif any(w in a for w in ["reduce","caution","overvalued","bearish","sell","bubble","risk"]):
                reduce_count += 1
            else:
                neutral_count += 1
    total = buy_count + reduce_count + neutral_count or 1
    bull  = round(buy_count / total * 100)
    bear  = round(reduce_count / total * 100)
    neut  = 100 - bull - bear
    if bull > 50:    label, color = "Broadly Bullish",  "#27ae60"
    elif bear > 50:  label, color = "Broadly Bearish",  "#e74c3c"
    elif bear > 35:  label, color = "Cautious",         "#e67e22"
    else:            label, color = "Mixed / Neutral",  "#f39c12"
    return {"label": label, "color": color,
            "bull": bull, "bear": bear, "neutral": neut}


# ─────────────────────────────────────────────────────────────────────────────
# HTML EMAIL
# ─────────────────────────────────────────────────────────────────────────────

def build_html_email(all_data: list) -> str:
    now  = datetime.now().strftime("%d %B %Y  %H:%M:%S")
    sent = overall_sentiment(all_data)

    # ── region cards ─────────────────────────────────────────────────────────
    cards_html = ""
    for rd in all_data:
        rows = ""
        for i, ind in enumerate(rd["indicators"]):
            bg  = "#f9f9f9" if i % 2 else "#ffffff"
            ac  = _sig_html(ind["action"])
            rows += f"""
            <tr style="background:{bg}">
              <td style="padding:9px 12px;color:#2c3e50;font-weight:600;
                         font-size:13px;border-bottom:1px solid #ecf0f1;width:30%;">
                {ind['title']}
              </td>
              <td style="padding:9px 12px;color:#34495e;font-size:13px;
                         border-bottom:1px solid #ecf0f1;width:25%;">
                <strong>{ind['value']}</strong>
              </td>
              <td style="padding:9px 12px;color:#7f8c8d;font-size:12px;
                         border-bottom:1px solid #ecf0f1;width:20%;">
                {ind['condition']}
              </td>
              <td style="padding:9px 12px;font-weight:600;color:{ac};
                         font-size:12px;border-bottom:1px solid #ecf0f1;width:25%;">
                {ind['action']}
              </td>
            </tr>"""

        cards_html += f"""
        <tr><td style="padding:16px 0 0;">
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="border:1px solid #dce3ea;border-radius:8px;
                        overflow:hidden;margin-bottom:8px;">
            <!-- country header -->
            <tr style="background:#1a2a6c;">
              <td colspan="4" style="padding:11px 16px;">
                <span style="font-size:20px;">{rd['flag']}</span>
                <span style="color:#fff;font-size:15px;font-weight:700;
                             margin-left:8px;">{rd['name']}</span>
                <span style="color:#aed6f1;font-size:12px;margin-left:10px;">
                  {rd['index']}
                </span>
              </td>
            </tr>
            <!-- column headers -->
            <tr style="background:#2980b9;">
              <th style="padding:8px 12px;color:#fff;text-align:left;font-size:11px;">Indicator</th>
              <th style="padding:8px 12px;color:#fff;text-align:left;font-size:11px;">Value</th>
              <th style="padding:8px 12px;color:#fff;text-align:left;font-size:11px;">Condition</th>
              <th style="padding:8px 12px;color:#fff;text-align:left;font-size:11px;">Action</th>
            </tr>
            {rows}
          </table>
        </td></tr>"""

    # ── sentiment bar ─────────────────────────────────────────────────────────
    sentiment_bar = f"""
        <tr><td style="padding:20px 0 8px;">
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="border:1px solid #dce3ea;border-radius:8px;overflow:hidden;">
            <tr style="background:#f8f9fa;">
              <td style="padding:14px 18px;">
                <p style="margin:0 0 8px;font-size:14px;font-weight:700;color:#2c3e50;">
                  📊 Overall Global Sentiment:
                  <span style="color:{sent['color']}">{sent['label']}</span>
                </p>
                <table width="100%" cellpadding="0" cellspacing="2">
                  <tr>
                    <td style="width:{sent['bull']}%;background:#27ae60;
                               height:12px;border-radius:4px 0 0 4px;"></td>
                    <td style="width:{sent['neutral']}%;background:#f39c12;height:12px;"></td>
                    <td style="width:{sent['bear']}%;background:#e74c3c;
                               height:12px;border-radius:0 4px 4px 0;"></td>
                  </tr>
                </table>
                <p style="margin:6px 0 0;font-size:11px;color:#7f8c8d;">
                  🟢 Bullish {sent['bull']}% &nbsp;|&nbsp;
                  🟡 Neutral {sent['neutral']}% &nbsp;|&nbsp;
                  🔴 Bearish {sent['bear']}%
                </p>
              </td>
            </tr>
          </table>
        </td></tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Global Market Indicators</title>
</head>
<body style="margin:0;padding:0;background:#f0f2f5;
             font-family:'Segoe UI',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f2f5;padding:30px 0;">
  <tr><td align="center">
    <table width="760" cellpadding="0" cellspacing="0"
           style="background:#ffffff;border-radius:10px;
                  box-shadow:0 2px 12px rgba(0,0,0,.1);overflow:hidden;">

      <!-- HEADER -->
      <tr>
        <td style="background:linear-gradient(135deg,#1a2a6c,#2980b9);
                   padding:28px 32px;text-align:center;">
          <h1 style="margin:0;color:#fff;font-size:22px;letter-spacing:1px;">
            🌐 Global Market Indicators Summary
          </h1>
          <p style="margin:6px 0 0;color:#aed6f1;font-size:13px;">
            Generated on {now}
          </p>
        </td>
      </tr>

      <!-- INTRO -->
      <tr><td style="padding:20px 32px 8px;">
        <p style="margin:0;color:#5d6d7e;font-size:14px;line-height:1.6;">
          Live market indicators across <strong>USA, Europe, Japan, South Korea,
          Taiwan, China, Brazil, India</strong> and <strong>Emerging Markets</strong>.
          Data sourced from Yahoo Finance, World Bank, CNN Fear &amp; Greed, and
          public market data APIs.
        </p>
      </td></tr>

      <!-- SENTIMENT + COUNTRY CARDS -->
      <tr><td style="padding:8px 32px 8px;">
        <table width="100%" cellpadding="0" cellspacing="0">
          {sentiment_bar}
          {cards_html}
        </table>
      </td></tr>

      <!-- LEGEND -->
      <tr><td style="padding:8px 32px 20px;">
        <table cellpadding="0" cellspacing="6">
          <tr>
            <td style="font-size:12px;color:#27ae60;padding-right:18px;">🟢 Bullish / Buy</td>
            <td style="font-size:12px;color:#f39c12;padding-right:18px;">🟡 Neutral</td>
            <td style="font-size:12px;color:#e74c3c;">🔴 Bearish / Reduce</td>
          </tr>
        </table>
        <p style="margin:8px 0 0;font-size:11px;color:#95a5a6;">
          ℹ️  200-DMA data uses representative ETFs (SPY, VGK, EWJ, EWY, EWT, FXI, EWZ, INDY, EEM).
          P/E ratios via Yahoo Finance. Mcap/GDP uses static estimates + World Bank GDP.
          Fear &amp; Greed from CNN / alternative.me.
        </p>
      </td></tr>

      <!-- FOOTER -->
      <tr>
        <td style="background:#f8f9fa;padding:16px 32px;
                   border-top:1px solid #ecf0f1;text-align:center;">
          <p style="margin:0;color:#95a5a6;font-size:11px;line-height:1.6;">
            Auto-generated for informational purposes only.
            <strong>Not financial advice.</strong> Always do your own research.<br>
            Data: Yahoo Finance · World Bank · CNN Fear &amp; Greed · alternative.me
          </p>
        </td>
      </tr>

    </table>
  </td></tr>
</table>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# PLAIN TEXT FALLBACK
# ─────────────────────────────────────────────────────────────────────────────

def build_plain_email(all_data: list) -> str:
    now   = datetime.now().strftime("%d %B %Y  %H:%M:%S")
    lines = ["GLOBAL MARKET INDICATORS SUMMARY",
             f"Generated: {now}", "=" * 70]
    for rd in all_data:
        lines.append(f"\n{rd['flag']}  {rd['name']}  ({rd['index']})")
        lines.append("-" * 50)
        for i, ind in enumerate(rd["indicators"], 1):
            lines += [
                f"  {i}. {ind['title']}",
                f"     Value     : {ind['value']}",
                f"     Condition : {ind['condition']}",
                f"     Action    : {ind['action']}",
                f"     Source    : {ind['source']}",
            ]
    lines += ["\n" + "=" * 70,
              "Informational only — not financial advice."]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# EMAIL SENDER
# ─────────────────────────────────────────────────────────────────────────────

def send_email(all_data: list):
    if not MY_EMAIL or not MY_PASSWORD:
        print(_c("\n  ⚠  Email credentials not set. Skipping email.", RED))
        print("     Set MY_EMAIL and MY_EMAIL_PSWRD as environment variables.\n")
        return

    recipient = RECIPIENT_EMAIL or MY_EMAIL
    now_str   = datetime.now().strftime("%d %b %Y")
    subject   = f"🌐 Global Market Indicators — {now_str}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = MY_EMAIL
    msg["To"]      = recipient
    msg.attach(MIMEText(build_plain_email(all_data), "plain", "utf-8"))
    msg.attach(MIMEText(build_html_email(all_data),  "html",  "utf-8"))

    print(f"\n  {_c('Sending email to', CYAN)} {_b(recipient)} ...")
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(MY_EMAIL, MY_PASSWORD)
            smtp.sendmail(MY_EMAIL, recipient, msg.as_string())
        print(f"  {_c('✓ Email sent successfully!', GREEN)}\n")
    except smtplib.SMTPAuthenticationError:
        print(_c("\n  ✗ Auth failed. Use a Gmail App Password.", RED))
        print("    https://myaccount.google.com/apppasswords\n")
    except smtplib.SMTPException as e:
        print(_c(f"\n  ✗ SMTP error: {e}\n", RED))
    except Exception as e:
        print(_c(f"\n  ✗ Unexpected error: {e}\n", RED))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now().strftime("%d %B %Y  %H:%M:%S")
    print(f"\n{_b(_c('=' * 72, CYAN))}")
    print(f"  {_b(_c('GLOBAL MARKET INDICATORS SUMMARY', WHITE))}")
    print(f"  {_c(now, YELLOW)}")
    print(f"{_b(_c('=' * 72, CYAN))}")
    print(f"  {_c('Fetching data for all regions ...', CYAN)}\n")

    all_data = collect_all_regions()
    print_results(all_data)
    send_email(all_data)


if __name__ == "__main__":
    main()