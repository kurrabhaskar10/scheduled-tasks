"""
Multi-ETF iNAV vs LTP Alert System
=====================================
Monitors multiple international ETFs simultaneously.
Alerts when |LTP - NAV| / NAV <= threshold% for any ETF.

Works 24x7 — weekends and market holidays included.
On non-trading days, uses AMFI's last declared NAV (always online).

ETFs monitored (verified ISINs, cross-checked from NSE/AMFI/Cbonds):
  MAFANG     — Mirae Asset NYSE FANG+ ETF          ISIN: INF769K01EW3
  MASPTOP50  — Mirae Asset S&P 500 Top 50 ETF      ISIN: INF769K01HP3
  MAHKTECH   — Mirae Asset Hang Seng TECH ETF      ISIN: INF769K01HS7
  HNGSNGBEES — Nippon India ETF Hang Seng BeES     ISIN: INF204KB19I1
  MON100     — Motilal Oswal NASDAQ 100 ETF        ISIN: INF247L01AP3

Data sources (no API keys / cookies / bot-blocking):
  LTP  → Yahoo Finance (.NS suffix)
  NAV  → NSE iNAV CSV  (live, market hours)     ← primary during trading
       → BSE ETF iNAV  (live, market hours)     ← secondary during trading
       → AMFI NAVAll.txt by ISIN (24x7)         ← always-on fallback

Dependencies:
    pip install requests yfinance
"""

import json
import time
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, time as dtime
from pathlib import Path

import requests
import yfinance as yf
import os

# ─────────────────────────────────────────────────────────────────────────────
# ETF REGISTRY
# Each entry:  NSE symbol → (ISIN, human-readable name)
#
# ISINs verified from NSE / Cbonds / AMC factsheets.
# The script uses ISINs to look up NAV in the AMFI file, so it never
# depends on a hardcoded scheme code that could go stale.
# ─────────────────────────────────────────────────────────────────────────────

ETF_REGISTRY: dict[str, dict] = {
    "MAFANG": {
        "isin": "INF769K01HF4",
        "name": "Mirae Asset NYSE FANG+ ETF",
        "threshold_pct": 10.0,        # alert when |diff| <= this %
    },
    "MASPTOP50": {
        "isin": "INF769K01HP3",
        "name": "Mirae Asset S&P 500 Top 50 ETF",
        "threshold_pct": 10.0,
    },
    "MAHKTECH": {
        "isin": "INF769K01HS7",
        "name": "Mirae Asset Hang Seng TECH ETF",
        "threshold_pct": 10.0,
    },
    "HNGSNGBEES": {
        "isin": "INF204KB19I1",
        "name": "Nippon India ETF Hang Seng BeES",
        "threshold_pct": 10.0,
    },
    "MON100": {
        "isin": "INF247L01AP3",
        "name": "Motilal Oswal NASDAQ 100 ETF",
        "threshold_pct": 10.0,
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL CONFIG
# ─────────────────────────────────────────────────────────────────────────────

CONFIG = {
    # Seconds between each full poll cycle (all ETFs)
    "poll_interval_seconds": 300,

    # Where to cache last known values (per ETF) across restarts
    "cache_file": "etf_cache.json",

    # ── Email ──────────────────────────────────────────────────────────────
    "email": {
        "enabled":         True,
        "smtp_host":       "smtp.gmail.com",
        "smtp_port":       587,
        "sender_email":    "kurra.bhaskar10@gmail.com",
        "sender_password": "xlui blcb tmpx ufro",  #Gmail App Password
        "recipient_email": "kurra.bhaskar10@gmail.com",
    },

    # ── SMS via Twilio ────────────────────────────────────────────────────
    "sms": {
        "enabled":     False,
        "account_sid": "AC121419facad76572a7ee9ec768876aa8",
        "auth_token":  "fcdefc1a48759ccbf173699189a446c4",
        "from_number": "+12025586449",
        "to_number":   "+919094085810",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Cache — one JSON file, keyed by symbol
# ─────────────────────────────────────────────────────────────────────────────

def _load_all_cache() -> dict:
    p = Path(CONFIG["cache_file"])
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception as e:
        log.warning("Cache read failed: %s", e)
        return {}


def _save_all_cache(cache: dict) -> None:
    try:
        Path(CONFIG["cache_file"]).write_text(json.dumps(cache, indent=2))
    except Exception as e:
        log.warning("Cache write failed: %s", e)


def save_cache(symbol: str, ltp: float, nav: float, nav_date: str) -> None:
    cache = _load_all_cache()
    cache[symbol.upper()] = {
        "ltp":       ltp,
        "nav":       nav,
        "nav_date":  nav_date,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    _save_all_cache(cache)


def load_cache(symbol: str) -> dict | None:
    return _load_all_cache().get(symbol.upper())


# ─────────────────────────────────────────────────────────────────────────────
# Market status
# ─────────────────────────────────────────────────────────────────────────────

_NSE_HOLIDAYS = {
    # 2025
    "2025-01-26", "2025-02-19", "2025-03-14", "2025-03-31",
    "2025-04-10", "2025-04-14", "2025-04-18", "2025-05-01",
    "2025-08-15", "2025-08-27", "2025-10-02", "2025-10-24",
    "2025-11-05", "2025-12-25",
    # 2026
    "2026-01-26", "2026-03-19", "2026-04-02", "2026-04-03",
    "2026-04-14", "2026-04-30", "2026-05-01", "2026-08-17",
    "2026-09-16", "2026-10-01", "2026-10-20", "2026-11-24",
    "2026-12-25",
}


def _ist_now() -> datetime:
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("Asia/Kolkata"))


def is_market_open() -> bool:
    now = _ist_now()
    if now.weekday() >= 5:
        return False
    if now.strftime("%Y-%m-%d") in _NSE_HOLIDAYS:
        return False
    t = now.time()
    return dtime(9, 15) <= t <= dtime(15, 30)


def market_status_label() -> str:
    now = _ist_now()
    if now.weekday() >= 5:
        return "Weekend"
    if now.strftime("%Y-%m-%d") in _NSE_HOLIDAYS:
        return "Market Holiday"
    t = now.time()
    if t < dtime(9, 15):
        return "Pre-market"
    if t > dtime(15, 30):
        return "Post-market"
    return "Open"


# ─────────────────────────────────────────────────────────────────────────────
# LTP — Yahoo Finance
# ─────────────────────────────────────────────────────────────────────────────

def fetch_ltp_yahoo(symbol: str) -> float:
    ticker = yf.Ticker(f"{symbol}.NS")
    ltp    = getattr(ticker.fast_info, "last_price", None)
    if ltp is None:
        hist = ticker.history(period="5d", interval="1d")
        if hist.empty:
            raise ValueError(f"Yahoo Finance: no data for {symbol}.NS")
        ltp = float(hist["Close"].iloc[-1])
    return float(ltp)


# ─────────────────────────────────────────────────────────────────────────────
# NAV — AMFI NAVAll.txt  (24x7, lookup by ISIN)
#
# File format (semicolon-delimited):
#   SchemeCode ; ISIN_Growth ; ISIN_DivReinvest ; SchemeName ; NAV ; Date
#
# We match on ISIN_Growth (col 1) OR ISIN_DivReinvest (col 2).
# This is more reliable than matching on scheme code, which can change.
# ─────────────────────────────────────────────────────────────────────────────

_AMFI_URL  = "https://www.amfiindia.com/spages/NAVAll.txt"
_AMFI_TEXT: str | None = None          # module-level cache (refreshed each cycle)
_AMFI_FETCHED_AT: float = 0.0
_AMFI_TTL   = 600                      # re-fetch at most every 10 minutes

_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0",
    "Accept-Language": "en-IN,en-US;q=0.9",
}


def _get_amfi_text() -> str | None:
    global _AMFI_TEXT, _AMFI_FETCHED_AT
    if _AMFI_TEXT and (time.time() - _AMFI_FETCHED_AT) < _AMFI_TTL:
        return _AMFI_TEXT
    try:
        r = requests.get(_AMFI_URL, headers=_HEADERS, timeout=25)
        r.raise_for_status()
        _AMFI_TEXT       = r.text
        _AMFI_FETCHED_AT = time.time()
        log.debug("AMFI NAVAll.txt refreshed (%d bytes)", len(_AMFI_TEXT))
        return _AMFI_TEXT
    except Exception as e:
        log.warning("AMFI fetch failed: %s", e)
        return _AMFI_TEXT   # return stale copy if available


def fetch_nav_amfi_by_isin(isin: str) -> tuple[float, str] | None:
    """
    Parse AMFI NAVAll.txt and find the row where col[1] or col[2] == isin.
    Returns (nav, date_str) or None.
    """
    text = _get_amfi_text()
    if not text:
        return None

    for line in text.splitlines():
        parts = [p.strip() for p in line.split(";")]
        if len(parts) < 5:
            continue
        if parts[1] == isin or parts[2] == isin:
            try:
                nav  = float(parts[4].replace(",", ""))
                date = parts[5].strip() if len(parts) > 5 else "unknown"
                if nav > 0:
                    return nav, date
            except (ValueError, IndexError):
                pass

    log.warning("ISIN '%s' not found in AMFI NAVAll.txt", isin)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# NAV — NSE live iNAV CSV (market hours only)
# ─────────────────────────────────────────────────────────────────────────────

_NSE_INAV_CACHE: dict = {}
_NSE_INAV_FETCHED_AT: float = 0.0
_NSE_INAV_TTL = 60   # refresh at most every 60s


def _get_nse_inav_map() -> dict[str, float]:
    """Return {SYMBOL: inav} dict, cached for 60s."""
    global _NSE_INAV_CACHE, _NSE_INAV_FETCHED_AT
    if _NSE_INAV_CACHE and (time.time() - _NSE_INAV_FETCHED_AT) < _NSE_INAV_TTL:
        return _NSE_INAV_CACHE
    try:
        r = requests.get(
            "https://archives.nseindia.com/content/equities/ETFINAV.csv",
            headers=_HEADERS, timeout=15,
        )
        if r.status_code == 200 and r.text.strip():
            result = {}
            for line in r.text.splitlines()[1:]:   # skip header
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 3:
                    try:
                        val = float(parts[2].replace(",", ""))
                        if val > 0:
                            result[parts[0].upper()] = val
                    except ValueError:
                        pass
            _NSE_INAV_CACHE       = result
            _NSE_INAV_FETCHED_AT  = time.time()
            log.debug("NSE iNAV CSV refreshed: %d entries", len(result))
    except Exception as e:
        log.warning("NSE iNAV CSV fetch failed: %s", e)
    return _NSE_INAV_CACHE


# ─────────────────────────────────────────────────────────────────────────────
# NAV — BSE live iNAV feed (market hours only)
# ─────────────────────────────────────────────────────────────────────────────

_BSE_INAV_CACHE: list = []
_BSE_INAV_FETCHED_AT: float = 0.0
_BSE_INAV_TTL = 60


def _get_bse_inav_list() -> list:
    global _BSE_INAV_CACHE, _BSE_INAV_FETCHED_AT
    if _BSE_INAV_CACHE and (time.time() - _BSE_INAV_FETCHED_AT) < _BSE_INAV_TTL:
        return _BSE_INAV_CACHE
    try:
        r = requests.get(
            "https://api.bseindia.com/BseIndiaAPI/api/ETFiNav/w",
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.bseindia.com/"},
            timeout=15,
        )
        if r.status_code == 200:
            _BSE_INAV_CACHE      = r.json()
            _BSE_INAV_FETCHED_AT = time.time()
            log.debug("BSE iNAV refreshed: %d entries", len(_BSE_INAV_CACHE))
    except Exception as e:
        log.warning("BSE iNAV fetch failed: %s", e)
    return _BSE_INAV_CACHE


def _fetch_inav_bse_for_symbol(symbol: str) -> float | None:
    for item in _get_bse_inav_list():
        name = str(item.get("scname", "")).upper()
        if symbol.upper() in name:
            val = item.get("inav") or item.get("iNav") or item.get("nav")
            if val:
                try:
                    fval = float(str(val).replace(",", ""))
                    if fval > 0:
                        return fval
                except ValueError:
                    pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Combined fetch: LTP + NAV for one ETF
# ─────────────────────────────────────────────────────────────────────────────

def fetch_ltp_and_nav(symbol: str, isin: str) -> tuple[float, float, str, bool]:
    """
    Returns (ltp, nav, nav_date_label, is_live_inav).

    Priority:
      Market hours → NSE iNAV → BSE iNAV → AMFI declared NAV
      Closed       → AMFI declared NAV
    """
    ltp = fetch_ltp_yahoo(symbol)

    if is_market_open():
        # Try NSE iNAV
        nav = _get_nse_inav_map().get(symbol.upper())
        if nav:
            log.info("%-12s  LTP=%.4f  iNAV=%.4f (NSE live)", symbol, ltp, nav)
            return ltp, nav, "live (NSE iNAV)", True

        # Try BSE iNAV
        nav = _fetch_inav_bse_for_symbol(symbol)
        if nav:
            log.info("%-12s  LTP=%.4f  iNAV=%.4f (BSE live)", symbol, ltp, nav)
            return ltp, nav, "live (BSE iNAV)", True

        log.debug("%s: live iNAV unavailable — falling back to AMFI", symbol)

    # AMFI declared NAV (always available)
    result = fetch_nav_amfi_by_isin(isin)
    if result:
        nav, nav_date = result
        log.info("%-12s  LTP=%.4f  NAV=%.4f (AMFI declared %s)", symbol, ltp, nav, nav_date)
        return ltp, nav, nav_date, False

    raise ValueError(
        f"{symbol}: could not fetch NAV from any source "
        f"(ISIN={isin}). Verify the ISIN is correct."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Alerts
# ─────────────────────────────────────────────────────────────────────────────

def _build_body(symbol, name, nav, ltp, diff_pct, nav_date, is_live_inav):
    direction = "DISCOUNT" if ltp < nav else "PREMIUM"
    nav_label = "iNAV (live)" if is_live_inav else f"NAV (declared {nav_date})"
    threshold = ETF_REGISTRY[symbol]["threshold_pct"]
    return (
        f"ETF Alert: {symbol}  —  {name}\n"
        f"  {nav_label:<28}: ₹{nav:.4f}\n"
        f"  LTP                        : ₹{ltp:.4f}\n"
        f"  Diff                       : {diff_pct:+.2f}% ({direction})\n"
        f"  Market status              : {market_status_label()}\n"
        f"  Alert time                 : {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}\n\n"
        f"{symbol} is trading at a {direction} of {abs(diff_pct):.2f}% to its NAV, "
        f"within your alert threshold of {threshold}%."
    )


def send_email(subject: str, body: str) -> None:
    cfg = CONFIG["email"]
    msg = MIMEMultipart()
    msg["From"]    = cfg["sender_email"]
    msg["To"]      = cfg["recipient_email"]
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))
    with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"]) as srv:
        srv.ehlo(); srv.starttls(); srv.ehlo()
        srv.login(cfg["sender_email"], cfg["sender_password"])
        srv.sendmail(cfg["sender_email"], cfg["recipient_email"], msg.as_string())
    log.info("Email sent → %s", cfg["recipient_email"])


def send_sms(body: str) -> None:
    cfg = CONFIG["sms"]
    r = requests.post(
        f"https://api.twilio.com/2010-04-01/Accounts/{cfg['account_sid']}/Messages.json",
        auth=(cfg["account_sid"], cfg["auth_token"]),
        data={"From": cfg["from_number"], "To": cfg["to_number"], "Body": body},
        timeout=10,
    )
    r.raise_for_status()
    log.info("SMS sent (SID: %s)", r.json().get("sid"))


def send_alert(symbol, nav, ltp, diff_pct, nav_date, is_live_inav):
    name      = ETF_REGISTRY[symbol]["name"]
    body      = _build_body(symbol, name, nav, ltp, diff_pct, nav_date, is_live_inav)
    direction = "discount" if ltp < nav else "premium"
    subject   = (
        f"[ETF Alert] {symbol} {abs(diff_pct):.2f}% {direction} to NAV"
        + ("" if is_live_inav else f" [NAV: {nav_date}]")
    )
    if CONFIG["email"]["enabled"]:
        try:   send_email(subject, body)
        except Exception as e: log.error("Email failed: %s", e)
    if CONFIG["sms"]["enabled"]:
        try:
            send_sms(
                f"ETF {symbol}: NAV={nav:.2f} LTP={ltp:.2f} "
                f"Diff={diff_pct:+.2f}% ({direction})"
            )
        except Exception as e:
            log.error("SMS failed: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

def main():
    interval = CONFIG["poll_interval_seconds"]

    log.info("Multi-ETF Alert Monitor")
    log.info("Tracking %d ETFs: %s", len(ETF_REGISTRY), ", ".join(ETF_REGISTRY))
    log.info("Poll interval: %ds | NAV: NSE/BSE iNAV (live) → AMFI (24x7)", interval)
    log.info("─" * 70)

    # Per-symbol state
    last_alert_time:    dict[str, datetime | None] = {s: None for s in ETF_REGISTRY}
    consecutive_errors: dict[str, int]             = {s: 0    for s in ETF_REGISTRY}

    while True:
        status = market_status_label()
        log.info("── Cycle start  [market: %s]  %s",
                 status, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        for symbol, meta in ETF_REGISTRY.items():
            isin      = meta["isin"]
            threshold = meta["threshold_pct"]

            try:
                ltp, nav, nav_date, is_live = fetch_ltp_and_nav(symbol, isin)
                save_cache(symbol, ltp, nav, nav_date)
                consecutive_errors[symbol] = 0

                diff_pct  = ((ltp - nav) / nav) * 100
                direction = "DISCOUNT" if ltp < nav else "PREMIUM"

                log.info(
                    "  %-12s  NAV=%-10.4f  LTP=%-10.4f  Diff=%+7.2f%%  [%s]",
                    symbol, nav, ltp, diff_pct, direction,
                )

                if abs(diff_pct) <= threshold:
                    now = datetime.now()
                    last = last_alert_time[symbol]
                    if last is None or (now - last).total_seconds() >= interval:
                        log.info("  %-12s  ⚡ Threshold met — sending alert!", symbol)
                        send_alert(symbol, nav, ltp, diff_pct, nav_date, is_live)
                        last_alert_time[symbol] = now
                    else:
                        log.info("  %-12s  Alert suppressed (cooldown).", symbol)

            except Exception as e:
                consecutive_errors[symbol] += 1
                log.error("  %-12s  ERROR: %s  (consecutive: %d)",
                          symbol, e, consecutive_errors[symbol])
                if consecutive_errors[symbol] >= 10:
                    log.error("  %-12s  10 consecutive errors — disabling for this session.", symbol)
                    ETF_REGISTRY.pop(symbol)
                    break

        if not ETF_REGISTRY:
            log.error("All ETFs have errored out — exiting.")
            return

        log.info("── Sleeping %ds …\n", interval)
        time.sleep(interval)


if __name__ == "__main__":
    main()
