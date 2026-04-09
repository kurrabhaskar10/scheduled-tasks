"""
Multi-ETF iNAV vs LTP Alert System  (Excel-integrated edition)
===============================================================
Reads ETF symbols from an Excel file, fetches live/declared data,
and writes the following columns back to the SAME file:

  LTP | Expense Ratio | 52W High | 52W Low | Rise from 52W Low |
  Fall from 52W High | Beta | PEG Ratio | P/E Ratio | Volume

A separate sheet ("PE History") accumulates a timestamped P/E snapshot
each run, enabling month-over-month P/E comparison on a third sheet
("PE Comparison").

Expected input Excel layout (Sheet1 / first sheet):
  Column A  – ETF Symbol (NSE, e.g. MAFANG)   ← required
  Column B  – ISIN                              ← required
  Column C  – Name (optional, filled if blank)
  Column D  – Threshold % (optional, defaults to 15.0)
  Columns E onward → written by this script (overwritten each run)

Dependencies:
    pip install requests yfinance openpyxl
"""

import json
import time
import smtplib
import logging
import os
from datetime import datetime, date, time as dtime
from pathlib import Path

import requests
import yfinance as yf
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
# pip install google-api-python-client google-auth
import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account
import io

DRIVE_FILE_ID = os.getenv("GDRIVE_FILE_ID")   # stored as GitHub Secret
GDRIVE_CREDS  = os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON")  # JSON string as GitHub Secret

def _get_drive_service():
    import json
    creds_dict = json.loads(GDRIVE_CREDS)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive.file"]
    )
    return build("drive", "v3", credentials=creds)


def download_excel_from_drive(local_path: str):
    """Download the Excel file from Drive to local runner before processing."""
    service = _get_drive_service()
    request = service.files().get_media(fileId=DRIVE_FILE_ID)
    with open(local_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    log.info("Downloaded Excel from Google Drive → %s", local_path)


def upload_excel_to_drive(local_path: str):
    """Upload the updated Excel file back to the same Drive file (overwrites)."""
    service = _get_drive_service()
    media = MediaFileUpload(
        local_path,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=True
    )
    service.files().update(
        fileId=DRIVE_FILE_ID,
        media_body=media
    ).execute()
    log.info("Uploaded updated Excel back to Google Drive (file ID: %s)", DRIVE_FILE_ID)

# ─────────────────────────────────────────────────────────────────────────────
# ENV / CREDENTIALS
# ─────────────────────────────────────────────────────────────────────────────

my_email            = os.getenv("MY_EMAIL")
my_password         = os.getenv("MY_EMAIL_PSWRD")
twilio_account_sid  = os.getenv("TWILIO_ACCOUNT_SID_NEW")
twilio_number       = os.getenv("TWILIO_NUMBER")
twilio_to_number    = os.getenv("TWILIO_TO_NUMBER_NEW")
twilio_auth_token   = os.getenv("TWILIO_AUTH_TOKEN_NEW")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

CONFIG = {
    "excel_file":             "etf_data_amfi1.xlsx",       # path to input/output Excel
    "poll_interval_seconds":  7200,
    "cache_file":             "etf_cache.json",
    "default_threshold_pct":  15.0,

    "email": {
        "enabled":         True,
        "smtp_host":       "smtp.gmail.com",
        "smtp_port":       587,
        "sender_email":    my_email,
        "sender_password": my_password,
        "recipient_email": my_email,
    },
    "sms": {
        "enabled":     True,
        "account_sid": twilio_account_sid,
        "auth_token":  twilio_auth_token,
        "from_number": twilio_number,
        "to_number":   twilio_to_number,
    },
}

# Sheet names
MAIN_SHEET    = "ETF Data"
PE_HIST_SHEET = "PE History"
PE_CMP_SHEET  = "PE Comparison"

# Output columns written to MAIN_SHEET (after symbol / ISIN / name / threshold)
OUTPUT_COLS = [
    "LTP (₹)",
    "Expense Ratio (%)",
    "52W High (₹)",
    "52W Low (₹)",
    "Rise from 52W Low (%)",
    "Fall from 52W High (%)",
    "Beta",
    "PEG Ratio",
    "P/E Ratio",
    "Volume",
    "NAV / iNAV (₹)",
    "NAV Date",
    "Diff vs NAV (%)",
    "Last Updated",
]

# Fixed input columns
COL_SYMBOL    = 1   # A
COL_ISIN      = 2   # B
COL_NAME      = 3   # C
COL_THRESHOLD = 4   # D
COL_OUTPUT_START = 5  # E

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
# Excel helpers
# ─────────────────────────────────────────────────────────────────────────────

HEADER_FILL   = PatternFill("solid", fgColor="1F4E79")
HEADER_FONT   = Font(bold=True, color="FFFFFF", name="Arial", size=10)
DATA_FONT     = Font(name="Arial", size=10)
ALT_FILL      = PatternFill("solid", fgColor="D6E4F0")
BORDER_THIN   = Border(
    left=Side(style="thin"), right=Side(style="thin"),
    top=Side(style="thin"),  bottom=Side(style="thin"),
)
CENTER        = Alignment(horizontal="center", vertical="center")
LEFT          = Alignment(horizontal="left",   vertical="center")

def _style_header(cell, text):
    cell.value     = text
    cell.font      = HEADER_FONT
    cell.fill      = HEADER_FILL
    cell.alignment = CENTER
    cell.border    = BORDER_THIN

def _style_data(cell, value, alt_row=False):
    cell.value     = value
    cell.font      = DATA_FONT
    cell.fill      = ALT_FILL if alt_row else PatternFill()
    cell.alignment = CENTER
    cell.border    = BORDER_THIN

def _autofit(ws, min_width=12, max_width=30):
    for col in ws.columns:
        length = max(
            len(str(c.value)) if c.value is not None else 0
            for c in col
        )
        ws.column_dimensions[get_column_letter(col[0].column)].width = \
            max(min_width, min(length + 4, max_width))


# ─────────────────────────────────────────────────────────────────────────────
# Read ETF registry from Excel
# ─────────────────────────────────────────────────────────────────────────────

def load_etf_registry(excel_path: str) -> dict:
    """
    Read ETF symbols, ISINs, names, and thresholds from the Excel file.
    Creates the file with a template if it doesn't exist.
    Returns {symbol: {isin, name, threshold_pct, row}} dict.
    """
    p = Path(excel_path)
    if not p.exists():
        _create_template_excel(excel_path)
        log.warning("Created template Excel at %s — populate it and restart.", excel_path)
        return {}

    wb = openpyxl.load_workbook(excel_path)
    ws = wb[MAIN_SHEET] if MAIN_SHEET in wb.sheetnames else wb.active

    registry = {}
    for row in range(2, ws.max_row + 1):   # row 1 = header
        symbol = ws.cell(row=row, column=COL_SYMBOL).value
        isin   = ws.cell(row=row, column=COL_ISIN).value
        if not symbol or not isin:
            continue
        symbol = str(symbol).strip().upper()
        isin   = str(isin).strip()
        name   = ws.cell(row=row, column=COL_NAME).value or symbol
        thr_v  = ws.cell(row=row, column=COL_THRESHOLD).value
        threshold = float(thr_v) if thr_v else CONFIG["default_threshold_pct"]
        registry[symbol] = {
            "isin":          isin,
            "name":          str(name).strip(),
            "threshold_pct": threshold,
            "row":           row,
        }
    log.info("Loaded %d ETFs from %s", len(registry), excel_path)
    return registry


def _create_template_excel(excel_path: str):
    """Create a blank template so users know the expected layout."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = MAIN_SHEET
    headers = ["Symbol", "ISIN", "Name", "Alert Threshold (%)"] + OUTPUT_COLS
    for c, h in enumerate(headers, 1):
        _style_header(ws.cell(row=1, column=c), h)
    # Sample rows
    samples = [
        ("MAFANG",     "INF769K01HF4", "Mirae Asset NYSE FANG+ ETF",        15.0),
        ("MASPTOP50",  "INF769K01HP3", "Mirae Asset S&P 500 Top 50 ETF",    15.0),
        ("MAHKTECH",   "INF769K01HS7", "Mirae Asset Hang Seng TECH ETF",     15.0),
        ("HNGSNGBEES", "INF204KB19I1", "Nippon India ETF Hang Seng BeES",    15.0),
        ("MON100",     "INF247L01AP3", "Motilal Oswal NASDAQ 100 ETF",       15.0),
    ]
    for r, (sym, isin, name, thr) in enumerate(samples, 2):
        ws.cell(row=r, column=1).value = sym
        ws.cell(row=r, column=2).value = isin
        ws.cell(row=r, column=3).value = name
        ws.cell(row=r, column=4).value = thr

    # Create PE History and Comparison sheets
    _ensure_pe_sheets(wb)
    _autofit(ws)
    wb.save(excel_path)


def _ensure_pe_sheets(wb: openpyxl.Workbook):
    """Add PE History and PE Comparison sheets if missing."""
    if PE_HIST_SHEET not in wb.sheetnames:
        ws = wb.create_sheet(PE_HIST_SHEET)
        for c, h in enumerate(["Run Date", "Symbol", "Name", "P/E Ratio"], 1):
            _style_header(ws.cell(row=1, column=c), h)

    if PE_CMP_SHEET not in wb.sheetnames:
        ws = wb.create_sheet(PE_CMP_SHEET)
        for c, h in enumerate(
            ["Symbol", "Name", "P/E (This Run)", "P/E (Last Month)", "Change", "Change (%)"], 1
        ):
            _style_header(ws.cell(row=1, column=c), h)


# ─────────────────────────────────────────────────────────────────────────────
# Write results back to Excel
# ─────────────────────────────────────────────────────────────────────────────

def write_results_to_excel(excel_path: str, registry: dict, results: dict):
    """
    results = {symbol: {ltp, expense_ratio, high_52w, low_52w, beta, peg,
                         pe, volume, nav, nav_date, diff_pct, is_live}}
    """
    wb = openpyxl.load_workbook(excel_path)

    # ── MAIN SHEET ────────────────────────────────────────────────────────
    ws = wb[MAIN_SHEET] if MAIN_SHEET in wb.sheetnames else wb.active

    # Write / refresh column headers
    fixed_headers = ["Symbol", "ISIN", "Name", "Alert Threshold (%)"]
    all_headers   = fixed_headers + OUTPUT_COLS
    for c, h in enumerate(all_headers, 1):
        _style_header(ws.cell(row=1, column=c), h)
    ws.row_dimensions[1].height = 22
    ws.freeze_panes = "A2"

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for symbol, meta in registry.items():
        row   = meta["row"]
        r     = results.get(symbol, {})
        alt   = (row % 2 == 0)

        ltp          = r.get("ltp")
        high_52w     = r.get("high_52w")
        low_52w      = r.get("low_52w")
        rise_from_low = (
            round((ltp - low_52w) / low_52w * 100, 2)
            if ltp and low_52w else None
        )
        fall_from_high = (
            round((high_52w - ltp) / high_52w * 100, 2)
            if ltp and high_52w else None
        )

        values = [
            _fmt(ltp,                "₹"),
            _fmt(r.get("expense_ratio"), "%"),
            _fmt(high_52w,           "₹"),
            _fmt(low_52w,            "₹"),
            _fmt(rise_from_low,      "%"),
            _fmt(fall_from_high,     "%"),
            _fmt(r.get("beta")),
            _fmt(r.get("peg")),
            _fmt(r.get("pe")),
            _fmt(r.get("volume"),    fmt="int"),
            _fmt(r.get("nav"),       "₹"),
            r.get("nav_date", "N/A"),
            _fmt(r.get("diff_pct"),  "%"),
            now_str,
        ]

        # Fill fixed columns with styles
        for c in range(1, 5):
            cell = ws.cell(row=row, column=c)
            cell.font      = DATA_FONT
            cell.fill      = ALT_FILL if alt else PatternFill()
            cell.alignment = LEFT
            cell.border    = BORDER_THIN

        # Fill output columns
        for i, val in enumerate(values):
            _style_data(ws.cell(row=row, column=COL_OUTPUT_START + i), val, alt)

    _autofit(ws)

    # ── PE HISTORY SHEET ─────────────────────────────────────────────────
    _ensure_pe_sheets(wb)
    ws_hist = wb[PE_HIST_SHEET]
    run_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    hist_row = ws_hist.max_row + 1
    for symbol, r in results.items():
        pe = r.get("pe")
        if pe is None:
            continue
        ws_hist.cell(row=hist_row, column=1).value = run_date
        ws_hist.cell(row=hist_row, column=2).value = symbol
        ws_hist.cell(row=hist_row, column=3).value = registry[symbol]["name"]
        ws_hist.cell(row=hist_row, column=4).value = pe
        hist_row += 1

    _autofit(ws_hist)

    # ── PE COMPARISON SHEET ───────────────────────────────────────────────
    _write_pe_comparison(wb, registry, results)

    wb.save(excel_path)
    log.info("Excel updated: %s", excel_path)


def _write_pe_comparison(wb, registry, results):
    """Compare current P/E vs the closest run from ~30 days ago."""
    ws_hist = wb[PE_HIST_SHEET]
    ws_cmp  = wb[PE_CMP_SHEET]

    # Clear old comparison data (keep header row 1)
    for row in ws_cmp.iter_rows(min_row=2):
        for cell in row:
            cell.value = None

    # Build history: {symbol: [(date, pe), ...]}
    history: dict[str, list] = {}
    for row in ws_hist.iter_rows(min_row=2, values_only=True):
        if not row[0]:
            continue
        run_dt = row[0]
        sym    = row[1]
        pe_val = row[3]
        if sym and pe_val is not None:
            history.setdefault(sym, []).append((run_dt, float(pe_val)))

    today = datetime.now()
    cmp_row = 2
    for symbol, meta in registry.items():
        current_pe = results.get(symbol, {}).get("pe")
        last_month_pe = _find_closest_pe(history.get(symbol, []), today, days_ago=30)
        change     = None
        change_pct = None
        if current_pe is not None and last_month_pe is not None:
            change     = round(current_pe - last_month_pe, 4)
            change_pct = round(change / last_month_pe * 100, 2) if last_month_pe else None

        row_vals = [
            symbol,
            meta["name"],
            _fmt(current_pe),
            _fmt(last_month_pe),
            _fmt(change),
            _fmt(change_pct, "%"),
        ]
        alt = (cmp_row % 2 == 0)
        for c, v in enumerate(row_vals, 1):
            _style_data(ws_cmp.cell(row=cmp_row, column=c), v, alt)

        # Colour change cell: green = improved (lower P/E), red = worse (higher P/E)
        if change is not None:
            cell = ws_cmp.cell(row=cmp_row, column=5)
            cell.font = Font(
                name="Arial", size=10,
                color="006100" if change < 0 else ("9C0006" if change > 0 else "000000"),
                bold=True,
            )

        cmp_row += 1

    _autofit(ws_cmp)

    # Re-write headers (they may have been cleared)
    for c, h in enumerate(
        ["Symbol", "Name", "P/E (This Run)", "P/E (Last Month)", "Change", "Change (%)"], 1
    ):
        _style_header(ws_cmp.cell(row=1, column=c), h)


def _find_closest_pe(history: list, reference: datetime, days_ago: int) -> float | None:
    """Find the PE value from the entry closest to (reference - days_ago)."""
    if not history:
        return None
    target = reference.replace(hour=0, minute=0, second=0)
    from datetime import timedelta
    target -= timedelta(days=days_ago)

    best = None
    best_delta = None
    for run_dt, pe in history:
        if isinstance(run_dt, str):
            try:
                dt = datetime.strptime(run_dt, "%Y-%m-%d %H:%M")
            except ValueError:
                continue
        else:
            dt = run_dt
        delta = abs((dt - target).total_seconds())
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best = pe
    return best


def _fmt(val, unit="", fmt=""):
    if val is None:
        return "N/A"
    if fmt == "int":
        return f"{int(val):,}"
    if unit == "₹":
        return f"₹{val:,.4f}"
    if unit == "%":
        return f"{val:+.2f}%" if isinstance(val, float) and val < 0 else f"{val:.2f}%"
    return round(val, 4) if isinstance(val, float) else val


# ─────────────────────────────────────────────────────────────────────────────
# Market status
# ─────────────────────────────────────────────────────────────────────────────

_NSE_HOLIDAYS = {
    "2025-01-26", "2025-02-19", "2025-03-14", "2025-03-31",
    "2025-04-10", "2025-04-14", "2025-04-18", "2025-05-01",
    "2025-08-15", "2025-08-27", "2025-10-02", "2025-10-24",
    "2025-11-05", "2025-12-25",
    "2026-01-26", "2026-03-19", "2026-04-02", "2026-04-03",
    "2026-04-14", "2026-04-30", "2026-05-01", "2026-08-17",
    "2026-09-16", "2026-10-01", "2026-10-20", "2026-11-24",
    "2026-12-25",
}


def _ist_now() -> datetime:
    from zoneinfo import ZoneInfo
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
# NSE Session  (cookie warm-up required by NSE's anti-bot measures)
# ─────────────────────────────────────────────────────────────────────────────
# NSE India's API endpoints (api/quote-equity, api/etf) require a valid
# session cookie obtained by first hitting the NSE homepage. Without it,
# the API returns 401 or an empty/error JSON. We maintain a single
# requests.Session, refreshing the cookie at most once every 30 minutes.

_NSE_SESSION: requests.Session | None = None
_NSE_SESSION_AT: float = 0.0
_NSE_SESSION_TTL = 1800  # 30 min

_NSE_BASE     = "https://www.nseindia.com"
_NSE_HEADERS  = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-IN,en-US;q=0.9,en;q=0.8",
    "Referer":         "https://www.nseindia.com/",
}


def _get_nse_session() -> requests.Session:
    """
    Return a warmed-up NSE session, refreshing the cookie if stale.
    Warm-up sequence: GET homepage → GET /market-data/exchange-traded-funds-etf
    Both GETs must succeed to set the required nsit / nseappid cookies.
    """
    global _NSE_SESSION, _NSE_SESSION_AT
    if _NSE_SESSION and (time.time() - _NSE_SESSION_AT) < _NSE_SESSION_TTL:
        return _NSE_SESSION

    session = requests.Session()
    session.headers.update(_NSE_HEADERS)
    try:
        # Step 1: hit the homepage to get initial cookies (nsit, nseappid)
        session.get(_NSE_BASE, timeout=15)
        time.sleep(1)
        # Step 2: hit the ETF market-data page to prime the session fully
        session.get(f"{_NSE_BASE}/market-data/exchange-traded-funds-etf", timeout=15)
        time.sleep(0.5)
        _NSE_SESSION    = session
        _NSE_SESSION_AT = time.time()
        log.debug("NSE session refreshed (cookies: %s)", list(session.cookies.keys()))
    except Exception as e:
        log.warning("NSE session warm-up failed: %s", e)
        _NSE_SESSION = session   # use even if warm-up failed
    return _NSE_SESSION


# ─────────────────────────────────────────────────────────────────────────────
# iNAV — NSE quote-equity API  (FIXED)
# ─────────────────────────────────────────────────────────────────────────────
# NSE's per-symbol quote endpoint returns a JSON with metadata.iNavValue
# for ETF symbols during market hours. This replaces the defunct CSV at
# archives.nseindia.com/content/equities/ETFINAV.csv.
#
# Endpoint:  GET https://www.nseindia.com/api/quote-equity?symbol={SYMBOL}
# Key path:  response["metadata"]["iNavValue"]   (string, e.g. "163.45")
#            Also available: response["priceInfo"]["lastPrice"] for LTP cross-check.
#
# Note: The endpoint requires valid NSE session cookies (see _get_nse_session).
#       iNavValue is typically present only during market hours; outside hours
#       it may be absent or "0" — in that case we fall back to declared NAV.

_NSE_INAV_SYMBOL_CACHE: dict[str, tuple[float, float]] = {}  # symbol → (inav, fetched_at)
_NSE_INAV_SYMBOL_TTL = 60  # seconds


def fetch_inav_nse(symbol: str) -> float | None:
    """
    Fetch live iNAV for an NSE ETF symbol using the NSE quote-equity API.
    Returns the iNAV as a float, or None if unavailable/zero/invalid.

    The iNavValue field is populated by NSE's calculation agent during
    market hours only. Outside hours it is either absent or "0".
    """
    cached = _NSE_INAV_SYMBOL_CACHE.get(symbol.upper())
    if cached and (time.time() - cached[1]) < _NSE_INAV_SYMBOL_TTL:
        return cached[0] if cached[0] > 0 else None

    try:
        session = _get_nse_session()
        url = f"{_NSE_BASE}/api/quote-equity?symbol={symbol.upper()}"
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # iNavValue lives inside the "metadata" block as a string
        inav_raw = (
            data.get("metadata", {}).get("iNavValue")
            or data.get("priceInfo", {}).get("iNavValue")  # fallback key location
        )

        if inav_raw is None:
            log.debug("NSE quote-equity: iNavValue absent for %s", symbol)
            return None

        inav_str = str(inav_raw).replace(",", "").strip()
        if not inav_str or inav_str in ("-", "0", "0.0", "null", "NA", "N/A"):
            log.debug("NSE quote-equity: iNavValue is zero/null for %s: %r", symbol, inav_raw)
            return None

        inav = float(inav_str)
        if inav <= 0:
            return None

        _NSE_INAV_SYMBOL_CACHE[symbol.upper()] = (inav, time.time())
        log.debug("NSE iNAV  symbol=%-12s  iNav=%.4f", symbol, inav)
        return inav

    except Exception as e:
        log.warning("NSE iNAV fetch failed for %s: %s", symbol, e)
        # Force session refresh on next call so stale cookies don't persist
        global _NSE_SESSION_AT
        _NSE_SESSION_AT = 0.0
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Expense Ratio  (FIXED)
# ─────────────────────────────────────────────────────────────────────────────
# Two-source strategy (both free, no auth required):
#
# PRIMARY  — mfdata.in/api/v1/schemes/{amfi_code}
#   Returns JSON with "expense_ratio" field (float).
#   Uses the AMFI scheme code resolved via NAVAll.txt (same code used for mfapi).
#   This is a modern, well-maintained open API with daily updates.
#
# FALLBACK — mf.captnemo.in/search?q={isin}
#   Legacy source; still alive but the ISIN field in the response is uppercase
#   "ISIN" (not lowercase). The original code was correct about the field name,
#   but the API sometimes returns no results for ETF ISINs. Used as backup only.
#
# Both sources report TER (%) as a decimal, e.g. 0.59 means 0.59%.

_EXPENSE_CACHE: dict[str, float | None] = {}   # isin → expense_ratio or None
_EXPENSE_FETCHED: dict[str, float]      = {}   # isin → timestamp
_EXPENSE_TTL = 86400   # TER changes at most monthly; cache for 24 h

_MFDATA_BASE  = "https://mfdata.in/api/v1/schemes"
_CAPTNEMO_URL = "https://mf.captnemo.in/search"

_GENERIC_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0",
    "Accept-Language": "en-IN,en-US;q=0.9",
}


def fetch_expense_ratio(isin: str) -> float | None:
    """
    Returns expense ratio (%) for the given ISIN.
    Tries mfdata.in first (AMFI-code based), then mf.captnemo.in (ISIN-search).
    Returns None if unavailable from both sources.
    """
    # Cache check
    if isin in _EXPENSE_CACHE:
        if (time.time() - _EXPENSE_FETCHED.get(isin, 0)) < _EXPENSE_TTL:
            return _EXPENSE_CACHE[isin]

    # ── Source 1: mfdata.in (primary) ────────────────────────────────────
    val = _fetch_expense_mfdata(isin)

    # ── Source 2: mf.captnemo.in (fallback) ──────────────────────────────
    if val is None:
        val = _fetch_expense_captnemo(isin)

    _EXPENSE_CACHE[isin]   = val
    _EXPENSE_FETCHED[isin] = time.time()
    if val is not None:
        log.debug("Expense ratio  ISIN=%-16s  TER=%.4f%%", isin, val)
    else:
        log.debug("Expense ratio  ISIN=%-16s  TER=N/A (both sources failed)", isin)
    return val


def _fetch_expense_mfdata(isin: str) -> float | None:
    """
    mfdata.in  GET /api/v1/schemes/{amfi_code}
    Response:  {"status": "success", "data": {"amfi_code": ..., "expense_ratio": 0.59, ...}}
    Requires resolving ISIN → AMFI scheme code via NAVAll.txt first.
    """
    scheme_code = _scheme_code_for_isin(isin)
    if not scheme_code:
        log.debug("mfdata.in: no scheme code for ISIN %s", isin)
        return None
    try:
        r = requests.get(
            f"{_MFDATA_BASE}/{scheme_code}",
            headers=_GENERIC_HEADERS, timeout=15,
        )
        r.raise_for_status()
        body = r.json()
        if body.get("status") == "success":
            raw = body.get("data", {}).get("expense_ratio")
            if raw not in (None, "", "null"):
                val = float(raw)
                if val > 0:
                    return val
    except Exception as e:
        log.debug("mfdata.in expense fetch failed (ISIN=%s, code=%s): %s", isin, scheme_code, e)
    return None


def _fetch_expense_captnemo(isin: str) -> float | None:
    """
    mf.captnemo.in  GET /search?q={isin}
    Response: JSON array of scheme objects; the expense_ratio field is a string.
    Matches on the "ISIN" key (uppercase) in each item.
    """
    try:
        r = requests.get(
            _CAPTNEMO_URL,
            params={"q": isin},
            headers=_GENERIC_HEADERS,
            timeout=15,
        )
        r.raise_for_status()
        items = r.json()
        if isinstance(items, list):
            for item in items:
                # captnemo uses uppercase "ISIN" key
                if item.get("ISIN") == isin or item.get("isin") == isin:
                    raw = item.get("expense_ratio") or item.get("expenseRatio")
                    if raw not in (None, "", "null", "0", 0):
                        return float(raw)
    except Exception as e:
        log.debug("captnemo expense fetch failed (ISIN=%s): %s", isin, e)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# AMFI NAVAll.txt — declared NAV by ISIN (24×7 fallback)
# ─────────────────────────────────────────────────────────────────────────────
# Format: SchemeCode ; ISIN_Growth ; ISIN_DivReinvest ; SchemeName ; NAV ; Date
# Also used to resolve ISIN → AMFI scheme code for mfapi / mfdata lookups.

_AMFI_URL         = "https://www.amfiindia.com/spages/NAVAll.txt"
_AMFI_TEXT: str | None = None
_AMFI_FETCHED_AT  = 0.0
_AMFI_TTL         = 600   # re-fetch at most every 10 min

# AMFI scheme-code cache built once per process from NAVAll.txt
_ISIN_TO_SCHEME_CODE: dict[str, str] = {}


def _get_amfi_text() -> str | None:
    global _AMFI_TEXT, _AMFI_FETCHED_AT
    if _AMFI_TEXT and (time.time() - _AMFI_FETCHED_AT) < _AMFI_TTL:
        return _AMFI_TEXT
    try:
        r = requests.get(_AMFI_URL, headers=_GENERIC_HEADERS, timeout=25)
        r.raise_for_status()
        _AMFI_TEXT       = r.text
        _AMFI_FETCHED_AT = time.time()
        log.debug("AMFI NAVAll.txt refreshed (%d bytes)", len(_AMFI_TEXT))
    except Exception as e:
        log.warning("AMFI fetch failed: %s", e)
    return _AMFI_TEXT


def _parse_amfi_line(isin: str) -> tuple[str, float, str] | None:
    """Return (scheme_code, nav, date) for first line matching isin, or None."""
    text = _get_amfi_text()
    if not text:
        return None
    for line in text.splitlines():
        parts = [p.strip() for p in line.split(";")]
        if len(parts) < 6:
            continue
        if parts[1] == isin or parts[2] == isin:
            try:
                nav = float(parts[4].replace(",", ""))
                if nav > 0:
                    return parts[0], nav, parts[5]
            except (ValueError, IndexError):
                pass
    return None


def fetch_nav_amfi_by_isin(isin: str) -> tuple[float, str] | None:
    result = _parse_amfi_line(isin)
    if result:
        _, nav, date = result
        return nav, date
    log.warning("ISIN '%s' not found in AMFI NAVAll.txt", isin)
    return None


def _scheme_code_for_isin(isin: str) -> str | None:
    """Resolve ISIN → AMFI scheme code (cached)."""
    if isin in _ISIN_TO_SCHEME_CODE:
        return _ISIN_TO_SCHEME_CODE[isin]
    result = _parse_amfi_line(isin)
    if result:
        code = result[0]
        _ISIN_TO_SCHEME_CODE[isin] = code
        return code
    return None


# ─────────────────────────────────────────────────────────────────────────────
# mfapi.in — latest declared NAV + scheme metadata
# ─────────────────────────────────────────────────────────────────────────────

_MFAPI_BASE = "https://api.mfapi.in/mf"
_MFAPI_CACHE: dict[str, dict] = {}   # scheme_code → {nav, date, fetched_at}
_MFAPI_TTL = 600


def fetch_nav_mfapi(isin: str) -> tuple[float, str] | None:
    """
    Fetch latest NAV from mfapi.in using ISIN → scheme code resolution.
    Returns (nav, date_str) or None.
    """
    scheme_code = _scheme_code_for_isin(isin)
    if not scheme_code:
        return None

    cached = _MFAPI_CACHE.get(scheme_code)
    if cached and (time.time() - cached["fetched_at"]) < _MFAPI_TTL:
        return cached["nav"], cached["date"]

    try:
        r = requests.get(
            f"{_MFAPI_BASE}/{scheme_code}/latest",
            headers=_GENERIC_HEADERS, timeout=15,
        )
        r.raise_for_status()
        body = r.json()
        data = body.get("data", [])
        if data:
            nav  = float(data[0]["nav"])
            date = data[0]["date"]
            _MFAPI_CACHE[scheme_code] = {"nav": nav, "date": date, "fetched_at": time.time()}
            log.debug("mfapi NAV  scheme=%s  nav=%.4f  date=%s", scheme_code, nav, date)
            return nav, date
    except Exception as e:
        log.warning("mfapi fetch failed for scheme %s: %s", scheme_code, e)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# BSE live iNAV (market hours, kept as secondary fallback for iNAV)
# ─────────────────────────────────────────────────────────────────────────────

_BSE_INAV_CACHE:      list  = []
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
# Yahoo Finance — LTP, 52W H/L, Beta, P/E, Volume
# ─────────────────────────────────────────────────────────────────────────────

def fetch_yahoo_metrics(symbol: str) -> dict:
    ticker = yf.Ticker(f"{symbol}.NS")
    info   = ticker.info or {}
    fi     = ticker.fast_info

    # LTP — fast_info is quicker; fall back to recent history close
    ltp = getattr(fi, "last_price", None)
    if not ltp:
        hist = ticker.history(period="5d", interval="1d")
        ltp  = float(hist["Close"].iloc[-1]) if not hist.empty else None

    # 52-week High / Low
    high_52w = getattr(fi, "year_high", None) or info.get("fiftyTwoWeekHigh")
    low_52w  = getattr(fi, "year_low",  None) or info.get("fiftyTwoWeekLow")

    # Volume
    volume = (
        getattr(fi, "last_volume", None)
        or info.get("regularMarketVolume")
        or getattr(fi, "three_month_average_volume", None)
        or info.get("averageVolume")
    )

    # P/E
    pe = info.get("trailingPE") or info.get("forwardPE")

    # Beta
    beta = info.get("beta") or info.get("beta3Year")

    return {
        "ltp":      float(ltp)      if ltp      else None,
        "high_52w": float(high_52w) if high_52w else None,
        "low_52w":  float(low_52w)  if low_52w  else None,
        "beta":     float(beta)     if beta     else None,
        "peg":      None,   # Not available for Indian ETFs from any free source
        "pe":       float(pe)       if pe       else None,
        "volume":   int(volume)     if volume   else None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Combined fetch
# ─────────────────────────────────────────────────────────────────────────────

def fetch_all_data(symbol: str, isin: str) -> dict:
    """
    Fetch all metrics for one ETF.

    iNAV / NAV priority (most to least live):
      Market hours → NSE quote-equity API (iNavValue field)  [FIXED]
                   → BSE iNAV API (secondary live fallback)
      Any time     → mfapi.in latest NAV
                   → AMFI NAVAll.txt (final fallback)

    Expense ratio priority:  [FIXED]
      1. mfdata.in  GET /api/v1/schemes/{amfi_code}  (primary, AMFI-sourced)
      2. mf.captnemo.in  GET /search?q={isin}        (secondary fallback)

    LTP / 52W / Beta / P/E / Volume: Yahoo Finance (.NS)
    PEG ratio: not available for Indian ETFs from any free source → None
    """
    metrics       = fetch_yahoo_metrics(symbol)
    expense_ratio = fetch_expense_ratio(isin)
    ltp           = metrics.get("ltp")

    nav, nav_date, is_live = None, "N/A", False

    # Live iNAV during market hours — NSE quote-equity API (primary)
    if is_market_open():
        nav = fetch_inav_nse(symbol)
        if nav:
            nav_date, is_live = "live (NSE iNAV)", True

        # BSE iNAV as secondary live source if NSE returned nothing
        if not nav:
            nav = _fetch_inav_bse_for_symbol(symbol)
            if nav:
                nav_date, is_live = "live (BSE iNAV)", True

    # Declared NAV fallback: mfapi.in
    if not nav:
        result = fetch_nav_mfapi(isin)
        if result:
            nav, nav_date = result
            nav_date = f"declared {nav_date} (mfapi)"

    # Last resort: raw AMFI NAVAll.txt
    if not nav:
        result = fetch_nav_amfi_by_isin(isin)
        if result:
            nav, nav_date = result
            nav_date = f"declared {nav_date} (AMFI)"

    diff_pct = round((ltp - nav) / nav * 100, 4) if ltp and nav else None

    return {
        **metrics,
        "expense_ratio": expense_ratio,
        "nav":           nav,
        "nav_date":      nav_date,
        "diff_pct":      diff_pct,
        "is_live":       is_live,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Alerts
# ─────────────────────────────────────────────────────────────────────────────

def _alert_body(symbol, meta, r):
    ltp, nav = r.get("ltp"), r.get("nav")
    diff_pct = r.get("diff_pct", 0)
    direction = "DISCOUNT" if diff_pct and diff_pct < 0 else "PREMIUM"
    return (
        f"ETF Alert: {symbol}  —  {meta['name']}\n"
        f"  NAV/iNAV  : ₹{nav:.4f}  ({r.get('nav_date','N/A')})\n"
        f"  LTP       : ₹{ltp:.4f}\n"
        f"  Diff      : {diff_pct:+.2f}% ({direction})\n"
        f"  P/E       : {r.get('pe','N/A')}\n"
        f"  52W H/L   : ₹{r.get('high_52w','N/A')} / ₹{r.get('low_52w','N/A')}\n"
        f"  Market    : {market_status_label()}\n"
        f"  Time      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}\n"
    )


def send_email(subject, body):
    cfg = CONFIG["email"]
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    msg = MIMEMultipart()
    msg["From"], msg["To"], msg["Subject"] = (
        cfg["sender_email"], cfg["recipient_email"], subject
    )
    msg.attach(MIMEText(body, "plain"))
    with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"]) as srv:
        srv.ehlo(); srv.starttls(); srv.ehlo()
        srv.login(cfg["sender_email"], cfg["sender_password"])
        srv.sendmail(cfg["sender_email"], cfg["recipient_email"], msg.as_string())
    log.info("Email sent")


def send_sms(body):
    cfg = CONFIG["sms"]
    r = requests.post(
        f"https://api.twilio.com/2010-04-01/Accounts/{cfg['account_sid']}/Messages.json",
        auth=(cfg["account_sid"], cfg["auth_token"]),
        data={"From": cfg["from_number"], "To": cfg["to_number"], "Body": body},
        timeout=10,
    )
    r.raise_for_status()
    log.info("SMS sent (SID: %s)", r.json().get("sid"))


def send_alert(symbol, meta, r):
    diff_pct  = r.get("diff_pct", 0) or 0
    direction = "discount" if diff_pct < 0 else "premium"
    body      = _alert_body(symbol, meta, r)
    subject   = f"[ETF Alert] {symbol} {abs(diff_pct):.2f}% {direction} to NAV"
    if CONFIG["email"]["enabled"]:
        try:
            send_email(subject, body)
        except Exception as e:
            log.error("Email failed: %s", e)
    if CONFIG["sms"]["enabled"]:
        try:
            nav, ltp = r.get("nav", 0), r.get("ltp", 0)
            send_sms(
                f"ETF {symbol}: NAV={nav:.2f} LTP={ltp:.2f} "
                f"Diff={diff_pct:+.2f}% ({direction})"
            )
        except Exception as e:
            log.error("SMS failed: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Cache
# ─────────────────────────────────────────────────────────────────────────────

def save_cache(symbol, data):
    p = Path(CONFIG["cache_file"])
    cache = {}
    if p.exists():
        try:
            cache = json.loads(p.read_text())
        except Exception:
            pass
    cache[symbol.upper()] = {**data, "timestamp": datetime.now().isoformat()}
    try:
        p.write_text(json.dumps(cache, indent=2))
    except Exception as e:
        log.warning("Cache write failed: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────
"""
def main():
    excel_path = CONFIG["excel_file"]
    interval   = CONFIG["poll_interval_seconds"]

    log.info("Multi-ETF Monitor (Excel-integrated)")
    log.info("Excel file : %s", excel_path)
    log.info("Poll every : %ds", interval)

    last_alert_time:    dict[str, datetime | None] = {}
    consecutive_errors: dict[str, int]             = {}

    while True:
        registry = load_etf_registry(excel_path)
        if not registry:
            log.error("No ETFs found in %s — sleeping and retrying.", excel_path)
            time.sleep(60)
            continue

        for s in registry:
            last_alert_time.setdefault(s, None)
            consecutive_errors.setdefault(s, 0)

        log.info("── Cycle start [market: %s]  %s",
                 market_status_label(), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        results: dict[str, dict] = {}
        to_remove = []

        for symbol, meta in registry.items():
            try:
                r = fetch_all_data(symbol, meta["isin"])
                save_cache(symbol, r)
                results[symbol] = r
                consecutive_errors[symbol] = 0

                ltp, nav, diff_pct = r.get("ltp"), r.get("nav"), r.get("diff_pct")
                log.info(
                    "  %-12s  LTP=%-10s  NAV=%-10s  Diff=%s",
                    symbol,
                    f"₹{ltp:.4f}" if ltp else "N/A",
                    f"₹{nav:.4f}" if nav else "N/A",
                    f"{diff_pct:+.2f}%" if diff_pct is not None else "N/A",
                )

                # Alert check
                threshold = meta["threshold_pct"]
                if diff_pct is not None and abs(diff_pct) <= threshold:
                    now  = datetime.now()
                    last = last_alert_time.get(symbol)
                    if last is None or (now - last).total_seconds() >= interval:
                        log.info("  %-12s  ⚡ Threshold met — alerting!", symbol)
                        send_alert(symbol, meta, r)
                        last_alert_time[symbol] = now
                    else:
                        log.info("  %-12s  Alert in cooldown.", symbol)

            except Exception as e:
                consecutive_errors[symbol] += 1
                log.error("  %-12s  ERROR (%d): %s",
                          symbol, consecutive_errors[symbol], e)
                results[symbol] = {}
                if consecutive_errors[symbol] >= 10:
                    log.error("  %-12s  Disabling after 10 consecutive errors.", symbol)
                    to_remove.append(symbol)

        for s in to_remove:
            registry.pop(s, None)

        try:
            write_results_to_excel(excel_path, registry, results)
        except Exception as e:
            log.error("Excel write failed: %s", e)

        log.info("── Sleeping %ds …\n", interval)
        time.sleep(interval)


if __name__ == "__main__":
    main()
"""
def main():
    excel_path = CONFIG["excel_file"]   # just a local temp path e.g. "etf_data_amfi1.xlsx"

    log.info("Multi-ETF Monitor — GitHub Actions single-cycle run")

    # ── 1. Pull latest Excel from Drive ──────────────────────────────────
    download_excel_from_drive(excel_path)

    # ── 2. Load registry ─────────────────────────────────────────────────
    registry = load_etf_registry(excel_path)
    if not registry:
        log.error("No ETFs in Excel — aborting.")
        return

    last_alert_time:    dict[str, datetime | None] = {s: None for s in registry}
    consecutive_errors: dict[str, int]             = {s: 0    for s in registry}

    log.info("── Single cycle [market: %s]  %s",
             market_status_label(), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    results: dict[str, dict] = {}

    for symbol, meta in registry.items():
        try:
            r = fetch_all_data(symbol, meta["isin"])
            save_cache(symbol, r)
            results[symbol] = r

            ltp, nav, diff_pct = r.get("ltp"), r.get("nav"), r.get("diff_pct")
            log.info(
                "  %-12s  LTP=%-10s  NAV=%-10s  Diff=%s",
                symbol,
                f"₹{ltp:.4f}" if ltp else "N/A",
                f"₹{nav:.4f}" if nav else "N/A",
                f"{diff_pct:+.2f}%" if diff_pct is not None else "N/A",
            )

            # Alert check
            threshold = meta["threshold_pct"]
            if diff_pct is not None and abs(diff_pct) <= threshold:
                log.info("  %-12s  ⚡ Threshold met — alerting!", symbol)
                send_alert(symbol, meta, r)

        except Exception as e:
            consecutive_errors[symbol] += 1
            log.error("  %-12s  ERROR: %s", symbol, e)
            results[symbol] = {}

    # ── 3. Write Excel locally ────────────────────────────────────────────
    try:
        write_results_to_excel(excel_path, registry, results)
    except Exception as e:
        log.error("Excel write failed: %s", e)
        return

    # ── 4. Push updated Excel back to Drive ───────────────────────────────
    upload_excel_to_drive(excel_path)


if __name__ == "__main__":
    main()
