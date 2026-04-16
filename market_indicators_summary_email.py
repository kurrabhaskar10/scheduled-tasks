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
# Indicators (paste your existing indicator functions here unchanged)
# ─────────────────────────────────────────────────────────────────────────────
# Example placeholder:
def get_dummy_indicator():
    return {"value": 42, "source": "Dummy"}

# ─────────────────────────────────────────────────────────────────────────────
# EMAIL SENDER
# ─────────────────────────────────────────────────────────────────────────────
# Correct regex: all on one line, properly closed
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
        # your existing reporting logic goes here
        print(f"\n{_bold(_color('=' * 68, CYAN))}")
        print(f"  {_bold(_color('MARKET INDICATORS SUMMARY REPORT', WHITE))}")
        print(f"  {_color(datetime.now().strftime('%d %B %Y  %H:%M:%S'), YELLOW)}")
        print(f"{_bold(_color('=' * 68, CYAN))}")
        print(f"  {_color('Initialising NSE session ...', CYAN)}")
        _init_nse_session()
        # ... call each indicator and print results as in your original code ...

    raw_report = buffer.getvalue()
    clean_report = strip_ansi(raw_report)
    send_email_report("Market Indicators Summary Report", clean_report)

if __name__ == "__main__":
    main()
