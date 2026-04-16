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
# Colour / formatting helpers (ANSI, works in most terminals)
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

# ─────────────────────────────────────────────────────────────────────────────
# Indicators (keep your existing indicator functions here unchanged)
# ─────────────────────────────────────────────────────────────────────────────
# ... paste all your get_pct_above_200dma, get_mcap_gdp, get_nifty_pe, etc. functions here ...

# ─────────────────────────────────────────────────────────────────────────────
# EMAIL SENDER
# ─────────────────────────────────────────────────────────────────────────────
# Correct regex: fully closed string, no newline inside
ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|

\[[0-?]*[ -/]*[@-
