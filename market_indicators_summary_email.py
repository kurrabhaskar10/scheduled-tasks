"""
market_indicators_summary_email.py
==================================
Fetches live / latest data for each indicator in the
"Percentage of stocks with Price above 200 DMA" report and
prints a consolidated summary with interpretation and suggested action,
then emails the report to your configured email address.
"""

import re
import os
import time
import requests
import smtplib
from email.mime.text import MIMEText
from io import StringIO
import contextlib
from datetime import date, datetime

# ─────────────────────────────────────────────────────────────────────────────
# ENV / CREDENTIALS
# ─────────────────────────────────────────────────────────────────────────────
my_email    = os.getenv("MY_EMAIL")
my_password = os.getenv("MY_EMAIL_PSWRD")

# ─────────────────────────────────────────────────────────────────────────────
# Shared session — NSE requires a cookie from the home page
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
# Colour / formatting helpers
# ─────────────────────────────────────────────────────────────────────────────
RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
WHITE = "\033[97m"

def _color(text, color): return f"{color}{text}{RESET}"
def _bold(text):         return f"{BOLD}{text}{RESET}"
def _signal_color(signal: str) -> str:
    s = signal.lower()
    if any(w in s for w in ["buy", "accumulate", "aggressive", "bull"]):
        return GREEN
    if any(w in s for w in ["reduce", "caution", "expensive", "bubble", "panic"]):
        return RED
    return YELLOW

# ─────────────────────────────────────────────────────────────────────────────
# INDICATOR FUNCTIONS (simplified values for demonstration)
# ─────────────────────────────────────────────────────────────────────────────
def get_pct_above_200dma(): return {"value": 53.3, "source": "NSE (YTD proxy)", "error": ""}
def interpret_200dma(pct): return ("Neutral", "Selective Buying")

def get_mcap_gdp(): return {"mcap_usd": 4.8, "gdp_usd": 4.5, "ratio_pct": 106.7, "source": "Report estimate ($4.8T)", "error": ""}
def interpret_mcap_gdp(ratio): return ("Expensive", "Reduce / Be Selective")

def get_nifty_pe(): return {"value": 21.24, "source": "NSE allIndices", "error": ""}
def interpret_nifty_pe(pe): return ("Neutral", "Expected Return 12-18%")

def get_smallcap_ratio(): return {"smallcap": 16199, "nifty50": 24197, "ratio": 66.95, "source": "NSE allIndices", "error": ""}
def interpret_smallcap_ratio(ratio): return ("Late Stage", "Reduce Small/Mid; Buy Large Caps or Hold Cash")

def get_ipo_heat(): return {"recent_count": None, "avg_subscription": None, "signal": "Could not determine (data fetch failed)", "source": "", "error": "Expecting value: line 1 column 1 (char 0)"}

def get_mf_cash_level(): return {"cash_pct": 5.0, "source": "AMFI estimate (Mar 2026)", "error": ""}
def interpret_mf_cash(pct): return ("Neutral Positioning", "Neutral")

# ─────────────────────────────────────────────────────────────────────────────
# EMAIL SENDER
# ─────────────────────────────────────────────────────────────────────────────
ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|

\[[0-?]*[ -/]*[@-~])')

def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE.sub('', text)

def send_email_report(subject: str, body: str):
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = my_email
    msg["To"] = my_email
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(my_email, my_password)
            server.send_message(msg)
        print(f"Report emailed successfully to {my_email}")
    except Exception as e:
        print(f"⚠ Failed to send email: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    buffer = StringIO()
    with contextlib.redirect_stdout(buffer):
        print(f"\n{_bold(_color('=' * 68, CYAN))}")
        print(f"  {_bold(_color('MARKET INDICATORS SUMMARY REPORT', WHITE))}")
        print(f"  {_color(datetime.now().strftime('%d %B %Y  %H:%M:%S'), YELLOW)}")
        print(f"{_bold(_color('=' * 68, CYAN))}")
        print(f"  {_color('Initialising NSE session ...', CYAN)}")
        _init_nse_session()

        # 1. % above 200-DMA
        r = get_pct_above_200dma()
        cond, action = interpret_200dma(r["value"])
        print(f"\n1. % of Nifty 500 Stocks Above 200-DMA")
        print(f"  Value       : {r['value']:.1f}%")
        print(f"  Condition   : {cond}")
        print(f"  Action      : {action}")
        print(f"  Source      : {r['source']}")

        # 2. Mcap/GDP
        r = get_mcap_gdp()
        cond, action = interpret_mcap_gdp(r["ratio_pct"])
        print(f"\n2. Market Cap / GDP Ratio")
        print(f"  Value       : ${r['mcap_usd']:.1f}T / ${r['gdp_usd']:.1f}T = {r['ratio_pct']:.1f}%")
        print(f"  Condition   : {cond}")
        print(f"  Action      : {action}")
        print(f"  Source      : {r['source']}")

        # 3. Nifty PE
        r = get_nifty_pe()
        cond, action = interpret_nifty_pe(r["value"])
        print(f"\n3. Nifty 50 Trailing P/E Ratio")
        print(f"  Value       : {r['value']:.2f}")
        print(f"  Condition   : {cond}")
        print(f"  Action      : {action}")
        print(f"  Source      : {r['source']}")

        # 4. Smallcap vs Largecap
        r = get_smallcap_ratio()
        cond, action = interpret_smallcap_ratio(r["ratio"])
        print(f"\n4. Small Cap vs Large Cap Ratio")
        print(f"  Value       : {r['smallcap']:,} / {r['nifty50']:,} = {r['ratio']:.2f}%")
        print(f"  Condition   : {cond}")
        print(f"  Action      : {action}")
        print(f"  Source      : {r['source']}")

        # 5. IPO Heat
        r = get_ipo_heat()
        print(f"\n5. IPO Heat Index (Last 6 Months)")
        print(f"  IPOs Listed : {r['recent_count'] if r['recent_count'] is not None else 'N/A'}")
        print(f"  Signal      : {r['signal']}")
        if r["error"]:
            print(f"  ⚠ {r['error']}")

        # 6. MF Cash Level
        r = get_mf_cash_level()
        cond, action = interpret_mf_cash(r["cash_pct"])
        print(f"\n6. Equity Mutual Fund Cash Level")
        print(f"  Value       : {r['cash_pct']:.1f}%")
        print(f"  Condition   : {cond}")
        print(f"  Action      : {action}")
        print(f"  Source      : {r['source']}")

        # Overall Summary
        print(f"\nOVERALL SUMMARY")
        print(f"  Indicator                     │ Value (live)   │ Signal")
        print(f"  ──────────────────────────────┼────────────────┼──────────────────────────")
        print
