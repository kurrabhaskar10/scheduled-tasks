"""
market_indicators_summary.py
=============================
Fetches live data for each indicator in the
"Percentage of stocks with Price above 200 DMA" report,
prints a summary to the terminal, and sends it as a
formatted HTML email.

Indicators covered
------------------
1. % of Nifty 500 stocks above their 200-DMA
2. Market-Cap / GDP ratio  (Mcap/GDP)
3. Nifty 50 Trailing P/E ratio
4. Small-cap vs Large-cap ratio  (Nifty Smallcap 250 / Nifty 50)
5. IPO heat index  (qualitative)
6. Equity MF cash level

Requirements
------------
    pip install requests beautifulsoup4 lxml

Credentials (set as environment variables before running)
----------------------------------------------------------
    Windows:
        set MY_EMAIL=you@gmail.com
        set MY_EMAIL_PSWRD=your_app_password

    Linux / macOS:
        export MY_EMAIL=you@gmail.com
        export MY_EMAIL_PSWRD=your_app_password

Gmail users MUST use an App Password (not your account password).
Generate one at: https://myaccount.google.com/apppasswords
"""

import os
import re
import sys
import time
import smtplib
import requests
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ─────────────────────────────────────────────────────────────────────────────
# ENV / CREDENTIALS
# ─────────────────────────────────────────────────────────────────────────────
MY_EMAIL    = os.getenv("MY_EMAIL")
MY_PASSWORD = os.getenv("MY_EMAIL_PSWRD")

# Who receives the report (defaults to sender; override if needed)
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL") or MY_EMAIL

# SMTP settings — defaults are for Gmail.
# Outlook/Hotmail: host="smtp-mail.outlook.com", port=587
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587


# ─────────────────────────────────────────────────────────────────────────────
# Shared HTTP session — NSE requires a warm-up cookie
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
    "Referer": "https://www.nseindia.com/",
})

def _init_nse_session():
    try:
        SESSION.get("https://www.nseindia.com", timeout=15)
        time.sleep(1.5)
        SESSION.get("https://www.nseindia.com/market-data/live-market-indices", timeout=15)
        time.sleep(1)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Terminal colour helpers (ANSI)
# ─────────────────────────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
WHITE  = "\033[97m"

def _c(text, color): return f"{color}{text}{RESET}"
def _b(text):        return f"{BOLD}{text}{RESET}"

def _signal_ansi(action: str) -> str:
    a = action.lower()
    if any(w in a for w in ["buy", "accumulate", "aggressive", "bull"]):  return GREEN
    if any(w in a for w in ["reduce", "caution", "expensive", "bubble", "panic"]): return RED
    return YELLOW

def _signal_html(action: str) -> str:
    a = action.lower()
    if any(w in a for w in ["buy", "accumulate", "aggressive", "bull"]):  return "#27ae60"
    if any(w in a for w in ["reduce", "caution", "expensive", "bubble", "panic"]): return "#e74c3c"
    return "#f39c12"


# ─────────────────────────────────────────────────────────────────────────────
# INDICATOR 1 — % stocks above 200-DMA  (Nifty 500)
# ─────────────────────────────────────────────────────────────────────────────

def get_pct_above_200dma() -> dict:
    result = {"value": None, "source": "", "error": ""}

    # Attempt 1: Trendlyne breadth page
    try:
        from bs4 import BeautifulSoup
        resp = SESSION.get("https://trendlyne.com/equity/breadth/nifty500/", timeout=20)
        soup = BeautifulSoup(resp.text, "lxml")
        for row in soup.find_all("tr"):
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if any("200" in c for c in cells):
                for c in cells:
                    m = re.search(r"(\d+\.?\d*)%?$", c)
                    if m:
                        val = float(m.group(1))
                        if 0 < val <= 100:
                            result["value"] = val
                            result["source"] = "Trendlyne"
                            return result
    except Exception as e:
        result["error"] += f"Trendlyne: {e}; "

    # Attempt 2: NSE Nifty 500 (YTD proxy)
    try:
        resp   = SESSION.get(
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500",
            timeout=30
        )
        stocks = resp.json().get("data", [])
        if stocks:
            above = sum(1 for s in stocks if float(s.get("perChange365d") or 0) > 0)
            result["value"]  = round(above / len(stocks) * 100, 1)
            result["source"] = "NSE (YTD proxy)"
            return result
    except Exception as e:
        result["error"] += f"NSE: {e}; "

    result["error"] = result["error"].strip("; ") or "All sources failed"
    return result


def interpret_200dma(pct: float) -> tuple:
    if pct >= 80:  return "Euphoria",   "Reduce Risk"
    if pct >= 60:  return "Healthy",    "Normal Allocation"
    if pct >= 40:  return "Neutral",    "Selective Buying"
    if pct >= 20:  return "Attractive", "Start Accumulating"
    return          "Panic",            "Aggressive Buying"


# ─────────────────────────────────────────────────────────────────────────────
# INDICATOR 2 — Mcap / GDP
# ─────────────────────────────────────────────────────────────────────────────

def get_mcap_gdp() -> dict:
    result = {"mcap_usd": None, "gdp_usd": 4.5, "ratio_pct": None,
              "source": "", "error": ""}
    try:
        resp = requests.get(
            "https://priceapi.moneycontrol.com/pricefeed/overview/equity?type=BSE",
            timeout=15, headers={"User-Agent": SESSION.headers["User-Agent"]}
        )
        mcap = resp.json().get("data", {}).get("totalMarketCap")
        if mcap:
            result["mcap_usd"] = round(float(mcap) / 83.5 / 1e12, 2)
            result["source"]   = "MoneyControl"
    except Exception as e:
        result["error"] = str(e)

    if result["mcap_usd"] is None:
        result["mcap_usd"] = 4.8                  # Apr 2026 estimate (from report)
        result["source"]   = "Report estimate ($4.8T)"

    result["ratio_pct"] = round(result["mcap_usd"] / result["gdp_usd"] * 100, 1)
    return result


def interpret_mcap_gdp(ratio: float) -> tuple:
    if ratio < 70:   return "Undervalued",     "Aggressive Buying"
    if ratio <= 100: return "Fairly Valued",    "Normal Allocation"
    if ratio <= 120: return "Expensive",        "Reduce / Be Selective"
    return            "Bubble Territory",       "Reduce Risk / Keep Cash"


# ─────────────────────────────────────────────────────────────────────────────
# INDICATOR 3 — Nifty 50 Trailing P/E
# ─────────────────────────────────────────────────────────────────────────────

def get_nifty_pe() -> dict:
    result = {"value": None, "source": "", "error": ""}

    try:
        indices = SESSION.get("https://www.nseindia.com/api/allIndices",
                              timeout=20).json().get("data", [])
        for idx in indices:
            if idx.get("index") in ("NIFTY 50", "Nifty 50"):
                pe = idx.get("pe")
                if pe:
                    result["value"]  = float(pe)
                    result["source"] = "NSE allIndices"
                    return result
    except Exception as e:
        result["error"] += f"allIndices: {e}; "

    try:
        data = SESSION.get(
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
            timeout=20
        ).json()
        for item in data.get("data", []):
            if item.get("symbol") == "NIFTY 50":
                pe = item.get("pe")
                if pe:
                    result["value"]  = float(pe)
                    result["source"] = "NSE stockIndices"
                    return result
        meta = data.get("metadata", {})
        pe = meta.get("pe") or meta.get("indexPe")
        if pe:
            result["value"]  = float(pe)
            result["source"] = "NSE metadata"
            return result
    except Exception as e:
        result["error"] += f"stockIndices: {e}; "

    try:
        today_str = date.today().strftime("%d-%m-%Y")
        rows = SESSION.get(
            f"https://www.nseindia.com/api/historical/indicesHistory"
            f"?indexType=NIFTY%2050&from={today_str}&to={today_str}",
            timeout=20
        ).json().get("data", {}).get("indexCloseOnlineRecords", [])
        if rows:
            pe = rows[-1].get("EOD_INDEX_NAME_PE")
            if pe:
                result["value"]  = float(pe)
                result["source"] = "NSE indicesHistory"
                return result
    except Exception as e:
        result["error"] += f"indicesHistory: {e}; "

    result["error"] = result["error"].strip("; ") or "All sources failed"
    return result


def interpret_nifty_pe(pe: float) -> tuple:
    if pe < 18:   return "Buy",       "Expected Return 25-40%"
    if pe <= 22:  return "Neutral",   "Expected Return 12-18%"
    if pe <= 26:  return "Expensive", "Expected Return 5-10%"
    return         "Reduce",          "Expected Return -15% to 0%"


# ─────────────────────────────────────────────────────────────────────────────
# INDICATOR 4 — Smallcap vs Largecap ratio
# ─────────────────────────────────────────────────────────────────────────────

def get_smallcap_ratio() -> dict:
    result = {"smallcap": None, "nifty50": None, "ratio": None,
              "source": "", "error": ""}
    try:
        indices = SESSION.get("https://www.nseindia.com/api/allIndices",
                              timeout=20).json().get("data", [])
        for idx in indices:
            name = idx.get("index", "")
            val  = float(idx.get("last") or idx.get("indexValue") or 0)
            if name == "NIFTY 50":           result["nifty50"]  = val
            if name == "NIFTY SMALLCAP 250": result["smallcap"] = val
        if result["smallcap"] and result["nifty50"]:
            result["ratio"]  = round(result["smallcap"] / result["nifty50"] * 100, 2)
            result["source"] = "NSE allIndices"
    except Exception as e:
        result["error"] = str(e)
    return result


def interpret_smallcap_ratio(ratio: float) -> tuple:
    if ratio < 35:  return "Bull Run Beginning", "Buy Small Caps"
    if ratio <= 50: return "Middle Stage",        "Buy Mid Caps"
    return           "Late Stage",               "Reduce Small/Mid; Buy Large Caps or Hold Cash"


# ─────────────────────────────────────────────────────────────────────────────
# INDICATOR 5 — IPO Heat Index
# ─────────────────────────────────────────────────────────────────────────────

def get_ipo_heat() -> dict:
    result = {"recent_count": None, "avg_subscription": None,
              "signal": "", "source": "", "error": ""}
    try:
        data   = SESSION.get("https://www.nseindia.com/api/allIpo?status=listed",
                             timeout=20).json()
        ipos   = data if isinstance(data, list) else data.get("data", [])
        cutoff = date.today() - timedelta(days=180)
        recent = []
        for ipo in ipos:
            try:
                ld = datetime.strptime(
                    ipo.get("listingDate") or ipo.get("ListingDate") or "", "%d-%b-%Y"
                ).date()
                if ld >= cutoff:
                    recent.append(ipo)
            except Exception:
                pass
        result["recent_count"] = len(recent)
        subs = [float(ipo.get("totalSubscription") or ipo.get("subTimes") or 0)
                for ipo in recent
                if ipo.get("totalSubscription") or ipo.get("subTimes")]
        result["avg_subscription"] = round(sum(subs) / len(subs), 1) if subs else None
        result["source"] = "NSE IPO API"
        count   = result["recent_count"] or 0
        avg_sub = result["avg_subscription"] or 0
        if   count > 20 and avg_sub > 50: result["signal"] = "HIGH — Many IPOs, very high oversubscription → Keep Cash"
        elif count > 10 and avg_sub > 20: result["signal"] = "MODERATE — Decent IPO activity"
        elif count <= 5  or  avg_sub < 5: result["signal"] = "LOW — Weak market signal → Start Buying"
        else:                             result["signal"] = "MODERATE — Watch closely"
    except Exception as e:
        result["error"]  = str(e)
        result["signal"] = "Could not determine (data fetch failed)"
    return result


# ─────────────────────────────────────────────────────────────────────────────
# INDICATOR 6 — Equity MF Cash Level
# ─────────────────────────────────────────────────────────────────────────────

def get_mf_cash_level() -> dict:
    result = {"cash_pct": None, "source": "", "error": ""}
    try:
        from bs4 import BeautifulSoup
        resp = requests.get(
            "https://www.amfiindia.com/modules/AumCategoryWise",
            timeout=20, headers={"User-Agent": SESSION.headers["User-Agent"]}
        )
        soup = BeautifulSoup(resp.text, "lxml")
        equity_aum = liquid_aum = 0.0
        for row in soup.find_all("tr"):
            cells = [td.get_text(strip=True).replace(",", "") for td in row.find_all("td")]
            if len(cells) >= 3:
                cat = cells[0].lower()
                try:   aum = float(cells[-1]) if cells[-1] else 0.0
                except ValueError: aum = 0.0
                if "equity" in cat and "hybrid" not in cat:
                    equity_aum += aum
                if any(k in cat for k in ("liquid", "money market", "overnight")):
                    liquid_aum += aum
        if equity_aum > 0:
            result["cash_pct"] = round(liquid_aum / (equity_aum + liquid_aum) * 100, 1)
            result["source"]   = "AMFI AUM Category"
    except Exception as e:
        result["error"] = str(e)

    if result["cash_pct"] is None:
        result["cash_pct"] = 5.0
        result["source"]   = "AMFI estimate (Mar 2026)"
    return result


def interpret_mf_cash(pct: float) -> tuple:
    if pct < 5:   return "Fully Invested",     "Bull Run Expected"
    if pct <= 15: return "Neutral Positioning", "Neutral"
    return         "Caution",                  "Stretched Valuations — Book Profits, Keep Watchlist"


# ─────────────────────────────────────────────────────────────────────────────
# COLLECT ALL RESULTS into a single dict
# ─────────────────────────────────────────────────────────────────────────────

def collect_all() -> dict:
    results = {}

    r = get_pct_above_200dma()
    if r["value"] is not None:
        cond, action = interpret_200dma(r["value"])
        results["dma"] = dict(title="% Nifty 500 Stocks Above 200-DMA",
                              value=f"{r['value']:.1f}%",
                              condition=cond, action=action, source=r["source"])
    else:
        results["dma"] = dict(title="% Nifty 500 Stocks Above 200-DMA",
                              value="N/A", condition="—",
                              action="Data unavailable", source=r["error"])

    r = get_mcap_gdp()
    cond, action = interpret_mcap_gdp(r["ratio_pct"])
    results["mcap"] = dict(
        title="Market Cap / GDP  (Buffett Indicator)",
        value=f"${r['mcap_usd']:.1f}T / ${r['gdp_usd']:.1f}T = {r['ratio_pct']:.1f}%",
        condition=cond, action=action, source=r["source"]
    )

    r = get_nifty_pe()
    if r["value"] is not None:
        cond, action = interpret_nifty_pe(r["value"])
        results["pe"] = dict(title="Nifty 50 Trailing P/E",
                             value=f"{r['value']:.2f}",
                             condition=cond, action=action, source=r["source"])
    else:
        results["pe"] = dict(title="Nifty 50 Trailing P/E",
                             value="N/A", condition="—",
                             action="Data unavailable", source=r["error"])

    r = get_smallcap_ratio()
    if r["ratio"] is not None:
        cond, action = interpret_smallcap_ratio(r["ratio"])
        results["sc"] = dict(
            title="Small Cap / Large Cap Ratio  (SC250 ÷ Nifty50)",
            value=f"{r['smallcap']:,.0f} ÷ {r['nifty50']:,.0f} = {r['ratio']:.2f}%",
            condition=cond, action=action, source=r["source"]
        )
    else:
        results["sc"] = dict(title="Small Cap / Large Cap Ratio  (SC250 ÷ Nifty50)",
                             value="N/A", condition="—",
                             action="Data unavailable", source=r["error"])

    r = get_ipo_heat()
    count_str = str(r["recent_count"]) if r["recent_count"] is not None else "N/A"
    sub_str   = f"{r['avg_subscription']:.1f}x" if r["avg_subscription"] else "N/A"
    heat_parts = r["signal"].split("—", 1)
    results["ipo"] = dict(
        title="IPO Heat Index  (Last 6 Months)",
        value=f"{count_str} IPOs listed, avg {sub_str} subscribed",
        condition=heat_parts[0].strip(),
        action=heat_parts[1].strip() if len(heat_parts) > 1 else r["signal"],
        source=r["source"] or r["error"]
    )

    r = get_mf_cash_level()
    cond, action = interpret_mf_cash(r["cash_pct"])
    results["mf"] = dict(title="Equity MF Cash Level",
                         value=f"{r['cash_pct']:.1f}%",
                         condition=cond, action=action, source=r["source"])

    return results


# ─────────────────────────────────────────────────────────────────────────────
# TERMINAL PRINT
# ─────────────────────────────────────────────────────────────────────────────

def print_results(results: dict):
    now = datetime.now().strftime("%d %B %Y  %H:%M:%S")
    print(f"\n{_b(_c('=' * 68, CYAN))}")
    print(f"  {_b(_c('MARKET INDICATORS SUMMARY REPORT', WHITE))}")
    print(f"  {_c(now, YELLOW)}")
    print(f"{_b(_c('=' * 68, CYAN))}")

    for i, (key, d) in enumerate(results.items(), 1):
        print(f"\n{_b(_c('─' * 68, CYAN))}")
#        print(f"  {_b(_c(f'{i}. {d[\"title\"]}', WHITE))}")
        title = d["title"]
        print(f"  {_b(_c(f'{i}. {title}', WHITE))}")
        print(f"{_b(_c('─' * 68, CYAN))}")
        print(f"  {'Value':<12}: {_b(d['value'])}")
        print(f"  {'Condition':<12}: {_c(d['condition'], YELLOW)}")
        print(f"  {'Action':<12}: {_c(d['action'], _signal_ansi(d['action']))}")
        print(f"  {'Source':<12}: {_c(d['source'], CYAN)}")

    print(f"\n{_b(_c('=' * 68, CYAN))}\n")


# ─────────────────────────────────────────────────────────────────────────────
# HTML EMAIL BODY
# ─────────────────────────────────────────────────────────────────────────────

def build_html_email(results: dict) -> str:
    now = datetime.now().strftime("%d %B %Y  %H:%M:%S")

    rows_html = ""
    for i, (key, d) in enumerate(results.items(), 1):
        ac_color = _signal_html(d["action"])
        bg = "#f9f9f9" if i % 2 == 0 else "#ffffff"
        rows_html += f"""
        <tr style="background:{bg};">
          <td style="padding:11px 14px;font-weight:600;color:#2c3e50;
                     border-bottom:1px solid #ecf0f1;width:28%;">
            {i}. {d['title']}
          </td>
          <td style="padding:11px 14px;color:#34495e;
                     border-bottom:1px solid #ecf0f1;width:22%;">
            <strong>{d['value']}</strong>
          </td>
          <td style="padding:11px 14px;color:#7f8c8d;
                     border-bottom:1px solid #ecf0f1;width:15%;">
            {d['condition']}
          </td>
          <td style="padding:11px 14px;font-weight:600;color:{ac_color};
                     border-bottom:1px solid #ecf0f1;width:25%;">
            {d['action']}
          </td>
          <td style="padding:11px 14px;font-size:11px;color:#95a5a6;
                     border-bottom:1px solid #ecf0f1;width:10%;">
            {d['source']}
          </td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Market Indicators Report</title>
</head>
<body style="margin:0;padding:0;background:#f0f2f5;
             font-family:'Segoe UI',Arial,sans-serif;">

  <table width="100%" cellpadding="0" cellspacing="0"
         style="background:#f0f2f5;padding:30px 0;">
    <tr><td align="center">
      <table width="720" cellpadding="0" cellspacing="0"
             style="background:#ffffff;border-radius:10px;
                    box-shadow:0 2px 12px rgba(0,0,0,.10);overflow:hidden;">

        <!-- HEADER -->
        <tr>
          <td style="background:linear-gradient(135deg,#1a2a6c,#2980b9);
                     padding:28px 32px;text-align:center;">
            <h1 style="margin:0;color:#fff;font-size:22px;letter-spacing:1px;">
              &#128202; Market Indicators Summary
            </h1>
            <p style="margin:6px 0 0;color:#aed6f1;font-size:13px;">
              Generated on {now}
            </p>
          </td>
        </tr>

        <!-- INTRO -->
        <tr>
          <td style="padding:20px 32px 8px;">
            <p style="margin:0;color:#5d6d7e;font-size:14px;line-height:1.6;">
              Below is the latest reading for each indicator from your
              <em>% Stocks Above 200-DMA</em> report, with live data
              pulled from NSE, AMFI, and other public sources.
            </p>
          </td>
        </tr>

        <!-- DATA TABLE -->
        <tr>
          <td style="padding:12px 32px 24px;">
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="border-collapse:collapse;border-radius:8px;
                          overflow:hidden;border:1px solid #ecf0f1;">
              <tr style="background:#2980b9;">
                <th style="padding:10px 14px;color:#fff;text-align:left;
                           font-size:12px;">Indicator</th>
                <th style="padding:10px 14px;color:#fff;text-align:left;
                           font-size:12px;">Value</th>
                <th style="padding:10px 14px;color:#fff;text-align:left;
                           font-size:12px;">Condition</th>
                <th style="padding:10px 14px;color:#fff;text-align:left;
                           font-size:12px;">Action</th>
                <th style="padding:10px 14px;color:#fff;text-align:left;
                           font-size:12px;">Source</th>
              </tr>
              {rows_html}
            </table>
          </td>
        </tr>

        <!-- LEGEND -->
        <tr>
          <td style="padding:0 32px 20px;">
            <table cellpadding="0" cellspacing="6">
              <tr>
                <td style="font-size:12px;color:#27ae60;padding-right:18px;">
                  &#9679; Bullish / Buy
                </td>
                <td style="font-size:12px;color:#f39c12;padding-right:18px;">
                  &#9679; Neutral
                </td>
                <td style="font-size:12px;color:#e74c3c;">
                  &#9679; Bearish / Reduce
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- FOOTER -->
        <tr>
          <td style="background:#f8f9fa;padding:16px 32px;
                     border-top:1px solid #ecf0f1;text-align:center;">
            <p style="margin:0;color:#95a5a6;font-size:11px;line-height:1.6;">
              Auto-generated for informational purposes only.
              <strong>Not financial advice.</strong> Always do your own research.<br>
              Data: NSE India &middot; AMFI &middot; MoneyControl &middot; Trendlyne
            </p>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# PLAIN-TEXT FALLBACK
# ─────────────────────────────────────────────────────────────────────────────

def build_plain_email(results: dict) -> str:
    now   = datetime.now().strftime("%d %B %Y  %H:%M:%S")
    lines = [
        "MARKET INDICATORS SUMMARY REPORT",
        f"Generated: {now}",
        "=" * 64,
    ]
    for i, (key, d) in enumerate(results.items(), 1):
        lines += [
            f"\n{i}. {d['title']}",
            f"   Value     : {d['value']}",
            f"   Condition : {d['condition']}",
            f"   Action    : {d['action']}",
            f"   Source    : {d['source']}",
        ]
    lines += ["\n" + "=" * 64,
              "Informational only — not financial advice."]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# EMAIL SENDER
# ─────────────────────────────────────────────────────────────────────────────

def send_email(results: dict):
    if not MY_EMAIL or not MY_PASSWORD:
        print(_c("\n  ⚠  Email credentials not set. Skipping email.", RED))
        print("     Set MY_EMAIL and MY_EMAIL_PSWRD as environment variables.\n")
        return

    recipient = RECIPIENT_EMAIL or MY_EMAIL
    now_str   = datetime.now().strftime("%d %b %Y")
    subject   = f"Market Indicators Summary — {now_str}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = MY_EMAIL
    msg["To"]      = recipient

    # Attach plain-text first (fallback), HTML second (preferred by mail clients)
    msg.attach(MIMEText(build_plain_email(results), "plain", "utf-8"))
    msg.attach(MIMEText(build_html_email(results),  "html",  "utf-8"))

    print(f"\n  {_c('Sending email to', CYAN)} {_b(recipient)} ...")
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
            smtp.ehlo()
            smtp.starttls()        # upgrade to encrypted connection
            smtp.ehlo()
            smtp.login(MY_EMAIL, MY_PASSWORD)
            smtp.sendmail(MY_EMAIL, recipient, msg.as_string())
        print(f"  {_c('✓ Email sent successfully!', GREEN)}\n")
    except smtplib.SMTPAuthenticationError:
        print(_c("\n  ✗ Authentication failed. Check your credentials.", RED))
        print("    For Gmail, use an App Password — not your account password.")
        print("    Generate one at: https://myaccount.google.com/apppasswords\n")
    except smtplib.SMTPException as e:
        print(_c(f"\n  ✗ SMTP error: {e}\n", RED))
    except Exception as e:
        print(_c(f"\n  ✗ Unexpected error: {e}\n", RED))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{_b(_c('=' * 68, CYAN))}")
    print(f"  {_b(_c('MARKET INDICATORS SUMMARY REPORT', WHITE))}")
    print(f"  {_c(datetime.now().strftime('%d %B %Y  %H:%M:%S'), YELLOW)}")
    print(f"{_b(_c('=' * 68, CYAN))}")
    print(f"  {_c('Initialising NSE session ...', CYAN)}")
    _init_nse_session()
    print(f"  {_c('Fetching all indicators ...', CYAN)}")

    results = collect_all()
    print_results(results)
    send_email(results)


if __name__ == "__main__":
    main()
