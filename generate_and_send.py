"""
Daily Market Brief — Generate & Send (v2)
Layout D: Unified market card + bottom line + advisor sections.
10-day summary ledger prevents repetition across briefs.
"""

import os
import sys
import json
import re
import smtplib
import ssl
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, date, timedelta
import urllib.request
import urllib.error
import urllib.parse
import yfinance as yf


# ── Config ─────────────────────────────────────────────────────

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
GMAIL_ADDRESS = os.environ["GMAIL_ADDRESS"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
UNSUBSCRIBE_BASE_URL = os.environ.get(
    "UNSUBSCRIBE_BASE_URL", "https://brieflywealth.com/unsubscribe.html"
)
SUBSCRIBE_URL = os.environ.get(
    "SUBSCRIBE_URL", "https://brieflywealth-newsletter.netlify.app/subscribe"
)

MODEL = "claude-haiku-4-5-20251001"


# ── US Market Holiday Check ────────────────────────────────────

def is_us_market_holiday(dt_date) -> bool:
    year, month, day = dt_date.year, dt_date.month, dt_date.day
    weekday = dt_date.weekday()
    if weekday >= 5:
        return True
    for h_m, h_d in [(1,1),(6,19),(7,4),(12,25)]:
        if month == h_m:
            hol = date(year, h_m, h_d)
            hw = hol.weekday()
            obs = date(year, h_m, h_d - 1) if hw == 5 else date(year, h_m, h_d + 1) if hw == 6 else hol
            if dt_date == obs:
                return True
    if month == 1 and weekday == 0 and 15 <= day <= 21: return True
    if month == 2 and weekday == 0 and 15 <= day <= 21: return True
    a=year%19; b=year//100; c=year%100; d=b//4; e=b%4; f=(b+8)//25; g=(b-f+1)//3
    h=(19*a+b-d-g+15)%30; i=c//4; k=c%4; l=(32+2*e+2*i-h-k)%7; m=(a+11*h+22*l)//451
    em=(h+l-7*m+114)//31; ed=((h+l-7*m+114)%31)+1
    if dt_date == date(year, em, ed) - timedelta(days=2): return True
    if month == 5 and weekday == 0 and day >= 25: return True
    if month == 9 and weekday == 0 and day <= 7: return True
    if month == 11 and weekday == 3 and 22 <= day <= 28: return True
    return False


# ── Supabase ───────────────────────────────────────────────────

def get_subscribers() -> list[dict]:
    url = f"{SUPABASE_URL}/rest/v1/subscribers?status=eq.active&select=email,name,firm,unsubscribe_token"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    })
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def get_recent_summaries(n: int = 10) -> str | None:
    """Fetch the last N brief summaries from Supabase for anti-repetition context."""
    url = (
        f"{SUPABASE_URL}/rest/v1/briefs"
        f"?select=brief_date,summary"
        f"&summary=not.is.null"
        f"&order=brief_date.desc&limit={n}"
    )
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    })
    try:
        with urllib.request.urlopen(req) as resp:
            rows = json.loads(resp.read())
        if not rows:
            return None
        lines = []
        for row in reversed(rows):  # chronological order
            lines.append(f"[{row['brief_date']}] {row['summary']}")
        result = "\n".join(lines)
        print(f"Loaded {len(rows)} previous summaries ({len(result)} chars)")
        return result
    except Exception as e:
        print(f"Warning: Could not fetch summaries: {e}", file=sys.stderr)
    return None


def save_brief(brief_date: str, greeting_hook: str, analysis: str, summary: str):
    """Save today's brief and summary to Supabase (upsert on brief_date)."""
    try:
        d = datetime.strptime(brief_date, "%B %d, %Y")
        iso_date = d.strftime("%Y-%m-%d")
    except Exception:
        iso_date = brief_date

    payload = json.dumps({
        "brief_date": iso_date,
        "greeting_hook": greeting_hook,
        "analysis": analysis,
        "summary": summary,
    }).encode()
    # Use on_conflict to upsert when running multiple times on the same day
    url = f"{SUPABASE_URL}/rest/v1/briefs?on_conflict=brief_date"
    req = urllib.request.Request(url, data=payload, method="POST", headers={
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    })
    try:
        with urllib.request.urlopen(req) as resp:
            status = resp.getcode()
            print(f"Brief saved for {iso_date} (HTTP {status})")
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')
        print(f"Warning: Could not save brief (HTTP {e.code}): {body}", file=sys.stderr)
    except Exception as e:
        print(f"Warning: Could not save brief: {e}", file=sys.stderr)


# ── Market Data (Yahoo Finance) ────────────────────────────────

TICKERS = {
    "sp500":    "^GSPC",
    "nasdaq":   "^IXIC",
    "dow":      "^DJI",
    "yield10y": "^TNX",
    "wti":      "CL=F",
    "gold":     "GC=F",
    "btc":      "BTC-USD",
}

FUTURES_TICKERS = {
    "S&P 500": "ES=F",
    "Nasdaq":  "NQ=F",
    "Dow":     "YM=F",
}

CONTINUOUS_TICKERS = {"CL=F", "GC=F", "BTC-USD"}  # futures & crypto: trade outside regular hours


def fetch_market_data() -> dict:
    """Fetch closing prices + YTD/MTD returns from Yahoo Finance."""
    today = date.today()
    year_start = date(today.year, 1, 1)
    month_start = date(today.year, today.month, 1)
    start_date = year_start - timedelta(days=10)

    result = {}
    for key, ticker in TICKERS.items():
        try:
            t = yf.Ticker(ticker)

            # yfinance `end` is exclusive. Stocks are fine with end=today
            # (yesterday's close), but futures/crypto trade continuously
            # and need end=tomorrow to capture the latest data.
            if ticker in CONTINUOUS_TICKERS:
                end_date = today + timedelta(days=1)
            else:
                end_date = today

            hist = t.history(start=start_date.isoformat(), end=end_date.isoformat())
            hist = hist.dropna(subset=["Close"])
            if hist.empty:
                print(f"  Warning: No valid Close data for {ticker}", file=sys.stderr)
                continue

            latest_close = hist["Close"].iloc[-1]
            is_yield = key == "yield10y"

            ytd_ref = hist.loc[hist.index >= str(year_start)]
            ytd_base = ytd_ref["Close"].iloc[0] if not ytd_ref.empty else hist["Close"].iloc[0]

            mtd_ref = hist.loc[hist.index >= str(month_start)]
            mtd_base = mtd_ref["Close"].iloc[0] if not mtd_ref.empty else hist["Close"].iloc[-1]

            if is_yield:
                level = f"{latest_close:.2f}%"
                ytd_bp = round((latest_close - ytd_base) * 100)
                mtd_bp = round((latest_close - mtd_base) * 100)
                ytd_str = f"+{ytd_bp}bp" if ytd_bp >= 0 else f"{ytd_bp}bp"
                mtd_str = f"+{mtd_bp}bp" if mtd_bp >= 0 else f"{mtd_bp}bp"
            elif key in ("wti", "gold", "btc"):
                level = f"${latest_close:,.2f}" if key != "btc" else f"${latest_close:,.0f}"
                ytd_pct = ((latest_close / ytd_base) - 1) * 100
                mtd_pct = ((latest_close / mtd_base) - 1) * 100
                ytd_str = f"+{ytd_pct:.1f}%" if ytd_pct >= 0 else f"{ytd_pct:.1f}%"
                mtd_str = f"+{mtd_pct:.1f}%" if mtd_pct >= 0 else f"{mtd_pct:.1f}%"
            else:
                level = f"{latest_close:,.2f}"
                ytd_pct = ((latest_close / ytd_base) - 1) * 100
                mtd_pct = ((latest_close / mtd_base) - 1) * 100
                ytd_str = f"+{ytd_pct:.1f}%" if ytd_pct >= 0 else f"{ytd_pct:.1f}%"
                mtd_str = f"+{mtd_pct:.1f}%" if mtd_pct >= 0 else f"{mtd_pct:.1f}%"

            result[key] = {"level": level, "ytd": ytd_str, "mtd": mtd_str}
            print(f"  {key}: {level}  YTD {ytd_str}  MTD {mtd_str}")

        except Exception as e:
            print(f"  Warning: Failed to fetch {ticker}: {e}", file=sys.stderr)

    return result


def fetch_futures() -> str:
    """Fetch pre-market futures from Yahoo Finance. Returns compact text for the AI."""
    lines = []
    for name, ticker in FUTURES_TICKERS.items():
        try:
            t = yf.Ticker(ticker)
            data = t.history(period="2d")
            if data.empty or len(data) < 2:
                continue
            prev_close = data["Close"].iloc[-2]
            current = data["Close"].iloc[-1]
            change_pct = ((current / prev_close) - 1) * 100
            sign = "+" if change_pct >= 0 else ""
            lines.append(f"{name} futures: {sign}{change_pct:.2f}%")
        except Exception:
            continue

    if not lines:
        return ""
    result = "PRE-MARKET FUTURES: " + " | ".join(lines)
    print(f"  {result}")
    return result


# ── Earnings Calendar (Yahoo Finance) ──────────────────────────

# Top 100 S&P 500 by market cap
EARNINGS_WATCHLIST = [
    # Mega-cap tech
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "TSLA", "AVGO", "BRK-B",
    # Financials
    "JPM", "V", "MA", "BAC", "GS", "MS", "BLK", "SCHW", "C", "AXP", "WFC",
    "USB", "PNC", "CB", "MMC",
    # Healthcare
    "UNH", "LLY", "JNJ", "ABBV", "MRK", "PFE", "TMO", "ABT", "DHR", "BMY",
    "GILD", "ISRG", "MDT", "AMGN", "CVS", "ELV", "CI",
    # Consumer
    "WMT", "PG", "COST", "HD", "MCD", "KO", "PEP", "NKE", "SBUX", "TGT",
    "LOW", "TJX", "ROST", "DG", "DLTR", "CL", "EL",
    # Tech / Software / Semis
    "ADBE", "CRM", "NFLX", "AMD", "INTC", "QCOM", "TXN", "ORCL", "NOW",
    "AMAT", "MU", "LRCX", "KLAC", "SNPS", "CDNS", "PANW", "CRWD",
    # Industrials / Defense
    "CAT", "DE", "HON", "GE", "BA", "RTX", "LMT", "UPS", "FDX",
    "MMM", "GD", "NOC",
    # Energy
    "XOM", "CVX", "COP", "SLB", "EOG",
    # Comm / Media
    "DIS", "CMCSA", "TMUS", "VZ",
    # Other
    "ACN", "LEN", "ULTA", "DKS", "LULU", "FIS",
]

def fetch_earnings_calendar() -> str:
    """Fetch earnings for top 100 S&P 500 companies: today + next 5 trading days."""
    today = date.today()
    window_end = today + timedelta(days=7)

    earnings = []
    skipped = 0
    for ticker in EARNINGS_WATCHLIST:
        try:
            t = yf.Ticker(ticker)
            dates = t.earnings_dates
            if dates is None or dates.empty:
                skipped += 1
                continue
            for dt in dates.index:
                ed = dt.date()
                if today <= ed <= window_end:
                    time_str = ""
                    if hasattr(dt, 'hour'):
                        if dt.hour < 10:
                            time_str = " (BMO)"
                        elif dt.hour >= 16:
                            time_str = " (AMC)"
                    earnings.append((ed, ticker, time_str))
                    break
        except Exception:
            skipped += 1
            continue

    print(f"  Checked {len(EARNINGS_WATCHLIST)} tickers, skipped {skipped}")

    if not earnings:
        print("  No top-100 earnings found this week")
        return ""

    earnings.sort(key=lambda x: (x[0], x[1]))

    lines = []
    current_date = None
    for ed, ticker, time_str in earnings:
        if ed != current_date:
            day_label = "Today" if ed == today else ed.strftime("%A %b %d")
            lines.append(f"\n{day_label}:")
            current_date = ed
        lines.append(f"  {ticker}{time_str}")

    result = "EARNINGS THIS WEEK (top S&P 500 companies):" + "".join(lines)
    print(f"  Found {len(earnings)} earnings this week")
    return result


# ── Economic Calendar (FRED API) ──────────────────────────────

# Release IDs for the data advisors actually care about
FRED_RELEASES = {
    10: "CPI",
    46: "PPI",
    53: "GDP",
    21: "Personal Income & Outlays (incl. PCE)",
    50: "Employment Situation (Jobs Report)",
    180: "Unemployment Claims",
    19: "Retail Sales",
    13: "Industrial Production",
    86: "Consumer Confidence (Mich.)",
    22: "Existing Home Sales",
    166: "New Home Sales",
    31: "New Residential Construction (Housing Starts)",
    39: "FOMC Press Release",
    11: "Employment Cost Index",
    57: "JOLTs",
    14: "Consumer Credit",
    56: "ISM Manufacturing (PMI)",
}

def fetch_fred_calendar() -> str:
    """Fetch upcoming economic releases from FRED API for the next 7 days."""
    if not FRED_API_KEY:
        print("  No FRED_API_KEY set, skipping economic calendar")
        return ""

    today = date.today()
    window_end = today + timedelta(days=7)

    releases = []
    for release_id, name in FRED_RELEASES.items():
        try:
            url = (
                f"https://api.stlouisfed.org/fred/release/dates"
                f"?release_id={release_id}"
                f"&api_key={FRED_API_KEY}"
                f"&file_type=json"
                f"&include_release_dates_with_no_data=true"
                f"&sort_order=asc"
            )
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())

            for rd in data.get("release_dates", []):
                rd_date = date.fromisoformat(rd["date"])
                if today <= rd_date <= window_end:
                    releases.append((rd_date, name))
                    break  # only next occurrence
        except Exception:
            continue

    if not releases:
        print("  No FRED releases found in this window")
        return ""

    releases.sort(key=lambda x: x[0])

    lines = []
    current_date = None
    for rd_date, name in releases:
        if rd_date != current_date:
            day_label = "Today" if rd_date == today else rd_date.strftime("%A %b %d")
            lines.append(f"\n{day_label}:")
            current_date = rd_date
        lines.append(f"  {name}")

    result = "ECONOMIC CALENDAR (upcoming releases):" + "".join(lines)
    print(f"  Found {len(releases)} releases in this window")
    return result


# ── Prompt ─────────────────────────────────────────────────────

SYSTEM_PROMPT = """You write daily pre-market morning briefings for financial advisors. MORNING BRIEFING, not a recap. Prepare them for the day ahead.

Tone: sharp colleague in the hallway before the first call. Professional, clear, occasionally wry.

THREE CRITICAL RULES:

1. ZERO REDUNDANCY — THIS IS THE MOST IMPORTANT RULE. Read this carefully.
This email has four content blocks: greeting hook, bottom line, advisor talking point, and water cooler. Each block must contain ENTIRELY NEW information. Specifically:
- Never state the same fact twice anywhere in the email. If the bottom line says "Morgan Stanley capped redemptions," the talking point CANNOT mention Morgan Stanley capping redemptions. Not even rephrased.
- Never repeat a company name, data point, or event across sections. Once you've mentioned it, it's used up. EXCEPTION: Companies appearing in the What to Watch earnings calendar may also be mentioned in the bottom line or talking point if their earnings are relevant to the day's narrative.
- The talking point must ADD ANALYSIS the bottom line didn't cover. If the bottom line covered what happened, the talking point covers what it means for portfolios or what advisors should do.
- The water cooler must be a completely unrelated story.
- Before writing each section, re-read what you've already written and ask: "Am I about to repeat anything?" If yes, find new material.

2. SIMPLE LANGUAGE EVERYWHERE. Every sentence in this email should be understandable by someone in their first year in finance. When you have a choice between an industry term and a simpler way to say it, always pick the simpler version. "Collateral calls on levered credit positions are cascading" should be "Lenders are demanding their money back, and it's spreading." Industry terms are okay when there's no simpler alternative, but default to plain English. Specific rules:
- Write short, clear sentences. If a sentence needs two commas, split it.
- Headlines must read like a newspaper, not a trading desk: "The Fed rate cut timeline just got pushed back" not "Duration is pain again."
- Explain any law, policy, or non-obvious concept in the same sentence.
- Never say "the 10-year" alone — say "the 10-year Treasury yield."
- Avoid dense compound phrases like "stagflation risk reverses the rate-cut narrative." Say what's actually happening: "Inflation is rising and the economy is slowing, which makes it harder for the Fed to cut rates."
- The client script especially must sound like a human talking, not a research note.

3. POLITICAL NEUTRALITY. This newsletter is for financial advisors across the political spectrum. When covering policy (tariffs, regulation, fiscal spending, executive orders, Fed appointments, etc.), present what happened and why it matters for markets/portfolios without editorializing on whether the policy is good or bad. Specific rules:
- State the policy or action factually. "The White House proposed X" not "The White House's misguided X."
- Present market implications, not political opinions. "Tariffs on steel would raise input costs for manufacturers" is fine. "The tariff plan misses the real problem" is not.
- If there are competing views, attribute them: "Supporters say X, while critics argue Y." Don't adopt either side.
- Never use language like "soundbite solution," "political theater," "common-sense reform," or any phrasing that signals approval or disapproval of a policy.
- The Water Cooler section is especially prone to this. Keep it observational and wry, not editorial. The italic sentence at the end should connect to advising or markets, not pass judgment on policymakers.
- When covering trade policy, immigration policy, energy policy, defense spending, or tax changes, always frame through the lens of "what this means for portfolios" rather than "whether this is the right call."

IMPORTANT: Market data, pre-market futures, earnings calendar, AND economic calendar are ALL pre-computed in the user message. Do NOT search for any of these. Use 1 web search ONLY for: overnight news/developments and a US-focused Water Cooler story.

OUTPUT FORMAT — in this EXACT order:

LINE 1 — GREETING HOOK: One sentence, what matters THIS MORNING. <p class="greeting-hook"> tags.

LINE 2 — BOTTOM LINE: 2-3 flowing sentences. Plain prose, NO labels, NO sub-headings. Use pre-market futures data to mention direction. Highlight the 1-2 most important things happening today. <p class="bottom-line"> tags. <b> tags on numbers.

LINE 3 — SUMMARY JSON (one line):
{"headline":"~10 words","talking_point":"angle + WHY in ~15 words","client_script_topic":"topic + framing ~10 words","water_cooler":"story + subject ~10 words","key_driver":"underlying reason ~10 words"}

Then a blank line, then EXACTLY these HTML sections:

<div class="section section-advisor">
<h2>Advisor Talking Point</h2>
<h3>[Simple, clear headline. Newspaper style, no jargon.]</h3>
<p>2 short paragraphs. HARD LIMIT: 150-200 words. Must cover a DIFFERENT angle than the bottom line.</p>
<div class="client-script">
<p class="client-script-label">If a client asks about [topic]</p>
<p>2 sentences max. Sound like a person talking, not a document.</p>
</div>
</div>

<div class="section">
<h2>What to Watch</h2>
<table class="watch-calendar">
<tr class="watch-group"><td colspan="2">Today</td></tr>
<tr><td class="watch-time">8:30 AM</td><td class="watch-desc">Specific data release name</td></tr>
<tr><td class="watch-time">Earnings</td><td class="watch-desc">Company Name (TICK), Company Name (TICK)</td></tr>
<tr class="watch-group"><td colspan="2">Tomorrow</td></tr>
<tr><td class="watch-time">8:30 AM</td><td class="watch-desc">Specific data release</td></tr>
<tr class="watch-group"><td colspan="2">Next Week</td></tr>
<tr><td class="watch-time">Wed</td><td class="watch-desc">FOMC decision</td></tr>
</table>
BE SPECIFIC. Use the pre-computed earnings AND economic calendar from the user message. The earnings data covers today and the rest of the week — include all of it grouped by day. If no earnings data is provided, note "No major S&P 500 earnings this week." Combine with economic releases into a clean calendar.
IMPORTANT: The calendar table is the ENTIRE section. No prose paragraphs after the table.
</div>

<div class="section section-watercooler">
<h2>Water Cooler</h2>
<h3>[Catchy headline]</h3>
<p>HARD LIMIT: 50-75 words. Must be a COMPLETELY DIFFERENT story from the bottom line and talking point. If those sections covered oil, credit, or geopolitics, the water cooler must be about something else entirely (tech, real estate, billionaires, tax policy, etc). US-focused. End with one italic sentence connecting it to advising.</p>
</div>

Total across ALL sections: 300-400 words. No more. Start with greeting hook. No preamble."""


# ── Parse Response ─────────────────────────────────────────────

def parse_response(raw: str) -> tuple[str, str, str, str]:
    """Parse AI output into: greeting_hook, bottom_line, summary_json, analysis HTML.
    Market data comes from Yahoo Finance, not the AI."""

    # 1. Greeting hook
    greeting_hook = ""
    hook_match = re.search(r'<p class="greeting-hook">(.*?)</p>', raw, re.DOTALL)
    if hook_match:
        greeting_hook = hook_match.group(1).strip()
        print(f"Greeting hook: {greeting_hook[:80]}...")

    # 2. Bottom line
    bottom_line = ""
    bl_match = re.search(r'<p class="bottom-line">(.*?)</p>', raw, re.DOTALL)
    if bl_match:
        bottom_line = bl_match.group(1).strip()
        print(f"Bottom line: {bottom_line[:80]}...")

    # 3. Summary JSON
    summary_json = ""
    sum_match = re.search(r'\{["\']headline["\'].*?\}', raw, re.DOTALL)
    if sum_match:
        try:
            raw_sum = sum_match.group().replace('\n', '')
            json.loads(raw_sum)  # validate
            summary_json = raw_sum
            print(f"Summary parsed: {summary_json[:80]}...")
        except json.JSONDecodeError:
            print("Warning: Could not parse summary JSON", file=sys.stderr)

    # 4. Analysis HTML (sections)
    first_div = raw.find('<div class="section')
    if first_div >= 0:
        analysis = raw[first_div:]
        clean = []
        in_html = False
        for line in analysis.split('\n'):
            s = line.strip()
            if any(s.startswith(t) for t in ['<div', '<h2', '<h3', '<p', '</', '<table', '<tr', '<td']):
                in_html = True
            if in_html or s == '' or s.startswith('<') or '<b>' in s or '<i>' in s:
                clean.append(line)
            if s == '</div>' and '</td>' not in s:
                in_html = False
        analysis = '\n'.join(clean)
    else:
        analysis = raw

    return greeting_hook, bottom_line, summary_json, analysis.strip()


# ── Build Market Card ──────────────────────────────────────────

def build_market_card(data: dict, bottom_line: str) -> str:
    """Build the Layout D unified market table + bottom line."""
    if not data:
        return ""

    F = "font-family:Georgia,'Times New Roman',serif;"
    blank = {"level": "—", "ytd": "—", "mtd": "—"}

    def get(key):
        return data.get(key, blank)

    def color(v):
        v = v.strip()
        if v.startswith("+"): return "#1a7a3a"
        elif v.startswith("-"): return "#b91c1c"
        return "#6b8db5"

    hdr_c = f'{F}font-size:9px;font-weight:bold;letter-spacing:0.8px;text-transform:uppercase;color:#8aacc8;padding:0 6px 6px;text-align:right;'
    hdr_cl = f'{F}font-size:9px;font-weight:bold;letter-spacing:0.8px;text-transform:uppercase;color:#8aacc8;padding:0 6px 6px;text-align:left;'
    idx_c = f'{F}font-size:12px;font-weight:bold;color:#142d4c;padding:6px 6px;'
    num_c = f'{F}font-size:12px;padding:6px 6px;text-align:right;font-variant-numeric:tabular-nums;'

    def nr(val):
        return f'<td style="{num_c}color:{color(val)};">{val}</td>'

    def lv(val):
        return f'<td style="{num_c}color:#142d4c;">{val}</td>'

    def row(name, key, last=False):
        d = get(key)
        sep = "" if last else "border-bottom:1px solid #e4e9f0;"
        return (
            f'<tr style="{sep}">'
            f'<td style="{idx_c}">{name}</td>'
            f'{lv(d["level"])}{nr(d["ytd"])}{nr(d["mtd"])}'
            f'</tr>'
        )

    card_bg = "background:#f6f8fb;border-radius:4px;padding:16px 16px 12px;margin-bottom:16px;"
    card_lbl = f'{F}font-size:9px;font-weight:bold;letter-spacing:1.5px;text-transform:uppercase;color:#8aacc8;margin:0 0 10px;'

    html = f'''<div style="{card_bg}">
<p style="{card_lbl}">Markets</p>
<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
<tr>
  <td style="{hdr_cl}width:30%;"></td>
  <td style="{hdr_c}width:23%;">Level</td>
  <td style="{hdr_c}width:23%;">YTD</td>
  <td style="{hdr_c}width:24%;">MTD</td>
</tr>
{row("S&amp;P 500", "sp500")}
{row("Nasdaq", "nasdaq")}
{row("Dow Jones", "dow")}
{row("10Y Yield", "yield10y")}
{row("WTI Crude", "wti")}
{row("Gold", "gold")}
{row("Bitcoin", "btc", last=True)}
</table>
</div>'''

    # Bottom line
    if bottom_line:
        html += (
            f'<p style="{F}font-size:14.5px;line-height:1.7;color:#3a4a5c;'
            f'margin:0 0 0;padding:0 0 20px;border-bottom:1px solid #dce3eb;">'
            f'{bottom_line}</p>'
        )

    return html


# ── Inline Styles for Email ────────────────────────────────────

FONT = "font-family:Georgia,'Times New Roman',serif;"
S_ADVISOR = f"background:#f4f7fa;padding:28px 36px;border-left:4px solid #142d4c;border-bottom:1px solid #dce3eb;margin:0 -36px;margin-bottom:0;"
S_WATERCOOLER = f"background:#f8f9fb;padding:28px 36px;margin:0 -36px;border-bottom:none;"
S_SECTION = f"margin:0;padding:28px 0;border-bottom:1px solid #dce3eb;"
S_SECTION_LAST = f"margin:0;padding:28px 0;border-bottom:none;"
S_H2 = f"{FONT}font-size:10px;font-weight:bold;color:#8aacc8;margin:0 0 8px;letter-spacing:2.5px;text-transform:uppercase;"
S_H3 = f"{FONT}font-size:18px;font-weight:normal;color:#142d4c;margin:0 0 14px;letter-spacing:-0.3px;line-height:1.3;"
S_P = f"{FONT}font-size:14.5px;line-height:1.65;color:#3a4a5c;margin:0 0 12px;"
S_CLIENT_SCRIPT = f"background:#ffffff;border:1px solid #dce3eb;border-radius:4px;padding:18px 20px;margin-top:16px;"
S_CLIENT_LABEL = f"{FONT}font-size:10px;font-weight:bold;letter-spacing:1.5px;text-transform:uppercase;color:#142d4c;margin:0 0 8px;"
S_CLIENT_P = f"{FONT}font-size:14px;line-height:1.65;color:#4a5d72;margin:0 0 8px;"
S_WATCH_TABLE = f"width:100%;border-collapse:collapse;margin:4px 0 0;"
S_WATCH_GROUP_TD = f"{FONT}font-size:11px;font-weight:bold;letter-spacing:1px;text-transform:uppercase;color:#8aacc8;padding:14px 0 6px;"
S_WATCH_TIME = f"{FONT}font-size:13px;color:#142d4c;font-weight:bold;width:80px;white-space:nowrap;padding:6px 12px 6px 0;vertical-align:top;"
S_WATCH_DESC = f"{FONT}font-size:14px;color:#3a4a5c;line-height:1.5;padding:6px 0;vertical-align:top;"


def inline_analysis_styles(html: str) -> str:
    """Convert class-based HTML from the AI into fully inlined email-safe HTML."""
    html = re.sub(r'<div class="section section-advisor">', f'<div style="{S_ADVISOR}">', html)
    html = re.sub(r'<div class="section section-watercooler">', f'<div style="{S_WATERCOOLER}">', html)
    html = re.sub(r'<div class="section">', f'<div style="{S_SECTION}">', html)
    html = re.sub(r'<div class="client-script">', f'<div style="{S_CLIENT_SCRIPT}">', html)
    html = re.sub(r'<p class="client-script-label">', f'<p style="{S_CLIENT_LABEL}">', html)
    html = re.sub(r'<table class="watch-calendar">', f'<table style="{S_WATCH_TABLE}">', html)
    html = re.sub(r'<tr class="watch-group"><td colspan="2">', f'<tr><td colspan="2" style="{S_WATCH_GROUP_TD}">', html)
    html = re.sub(r'<td class="watch-time">', f'<td style="{S_WATCH_TIME}">', html)
    html = re.sub(r'<td class="watch-desc">', f'<td style="{S_WATCH_DESC}">', html)
    html = re.sub(r'<h2>', f'<h2 style="{S_H2}">', html)
    html = re.sub(r'<h3>', f'<h3 style="{S_H3}">', html)
    # Strip <cite> tags from web search (keep inner text)
    html = re.sub(r'<cite[^>]*>', '', html)
    html = re.sub(r'</cite>', '', html)
    # Convert markdown *italic* to <i> tags
    html = re.sub(r'\*([^*]+)\*', r'<i>\1</i>', html)
    # Paragraphs — style ALL bare <p> tags (no existing style attribute)
    html = re.sub(r'<p(?!\s+style)>', f'<p style="{S_P}">', html)
    # Fix paragraphs inside client-script
    def fix_client(match):
        block = match.group(0)
        block = re.sub(rf'<p style="{re.escape(S_P)}">', f'<p style="{S_CLIENT_P}">', block)
        return block
    html = re.sub(rf'<div style="{re.escape(S_CLIENT_SCRIPT)}">.*?</div>', fix_client, html, flags=re.DOTALL)
    # Remove border from last standard section
    sections = list(re.finditer(re.escape(f'<div style="{S_SECTION}">'), html))
    if sections:
        last = sections[-1]
        old = last.group(0)
        new = old.replace(S_SECTION, S_SECTION_LAST)
        idx = last.start()
        html = html[:idx] + new + html[idx + len(old):]
    # Clean remaining class attributes
    html = re.sub(r' class="[^"]*"', '', html)

    # Enforce h2 labels — if the model skipped them, insert them
    advisor_h2 = f'<h2 style="{S_H2}">Advisor Talking Point</h2>'
    if re.escape(S_ADVISOR) in re.escape(html) and "Advisor Talking Point" not in html:
        html = html.replace(
            f'<div style="{S_ADVISOR}">',
            f'<div style="{S_ADVISOR}">\n{advisor_h2}',
        )

    watercooler_h2 = f'<h2 style="{S_H2}">Water Cooler</h2>'
    if S_WATERCOOLER in html and "Water Cooler" not in html:
        html = html.replace(
            f'<div style="{S_WATERCOOLER}">',
            f'<div style="{S_WATERCOOLER}">\n{watercooler_h2}',
        )

    return html


# ── Email Template ─────────────────────────────────────────────

def build_email_html(market_card: str, analysis: str, greeting_hook: str,
                     date_str: str, name: str = "", unsub_url: str = "") -> str:
    # Build greeting
    greeting = ""
    first_name = name.split()[0] if name else ""
    if first_name and greeting_hook:
        greeting = (
            f'<p style="font-family:Georgia,\'Times New Roman\',serif;font-size:15px;'
            f'color:#3a4a5c;margin:0 0 20px 0;line-height:1.65;">'
            f'Good morning, {first_name}. {greeting_hook}</p>'
        )
    elif first_name:
        greeting = (
            f'<p style="font-family:Georgia,\'Times New Roman\',serif;font-size:15px;'
            f'color:#3a4a5c;margin:0 0 20px 0;">Good morning, {first_name}.</p>'
        )
    elif greeting_hook:
        greeting = (
            f'<p style="font-family:Georgia,\'Times New Roman\',serif;font-size:15px;'
            f'color:#3a4a5c;margin:0 0 20px 0;line-height:1.65;">'
            f'Good morning. {greeting_hook}</p>'
        )

    unsub = ""
    if unsub_url:
        unsub = f' &bull; <a href="{unsub_url}" style="color:#6b8db5;text-decoration:none;">Unsubscribe</a>'
    try:
        d = datetime.strptime(date_str, "%B %d, %Y")
        newspaper_date = f"{d.strftime('%A')}, {date_str}"
    except Exception:
        newspaper_date = date_str

    # Inline all CSS classes in AI output
    styled_analysis = inline_analysis_styles(analysis)

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#e8eef4;font-family:Georgia,'Times New Roman',serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#e8eef4;padding:32px 16px;">
<tr><td align="center">
<table width="640" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:4px;overflow:hidden;">

<!-- Header -->
<tr><td style="background:#ffffff;padding:36px 36px 0;text-align:center;">
  <p style="font-family:Georgia,'Times New Roman',serif;font-size:10px;color:#8aacc8;margin:0 0 14px;letter-spacing:3px;text-transform:uppercase;">Briefly Wealth</p>
  <table width="100%" cellpadding="0" cellspacing="0"><tr><td style="border-top:2px solid #142d4c;height:0;font-size:0;line-height:0;"></td></tr></table>
  <h1 style="font-family:Georgia,'Times New Roman',serif;font-size:32px;font-weight:normal;color:#142d4c;margin:14px 0 12px;letter-spacing:-0.5px;line-height:1.1;">The Morning Brief</h1>
  <table width="100%" cellpadding="0" cellspacing="0"><tr><td style="border-top:1px solid #142d4c;height:0;font-size:0;line-height:0;"></td></tr></table>
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:10px;"><tr>
    <td style="text-align:left;"><p style="font-family:Georgia,'Times New Roman',serif;font-size:12px;color:#5a7a94;margin:0;">{newspaper_date}</p></td>
    <td style="text-align:right;"><p style="font-family:Georgia,'Times New Roman',serif;font-size:12px;color:#5a7a94;margin:0;">Pre-Market Edition</p></td>
  </tr></table>
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:10px;"><tr><td style="border-top:1px solid #d0dae4;height:0;font-size:0;line-height:0;"></td></tr></table>
</td></tr>

<!-- Body -->
<tr><td style="padding:24px 36px 0;">
  {greeting}{market_card}{styled_analysis}
</td></tr>

<!-- Subscribe CTA -->
<tr><td style="padding:24px 36px;border-top:1px solid #d4dee8;background:#f4f7fa;text-align:center;">
  <p style="font-family:Georgia,'Times New Roman',serif;font-size:14px;color:#3a4a5c;margin:0 0 14px;">Know an advisor who'd find this useful?</p>
  <a href="{SUBSCRIBE_URL}" style="display:inline-block;font-family:Georgia,'Times New Roman',serif;font-size:14px;color:#ffffff;background:#142d4c;padding:12px 28px;border-radius:4px;text-decoration:none;letter-spacing:0.3px;">Subscribe to The Morning Brief</a>
</td></tr>

<!-- Footer -->
<tr><td style="padding:16px 36px 20px;border-top:1px solid #e4ecf4;background:#f4f7fa;">
  <p style="font-family:Georgia,'Times New Roman',serif;font-size:10px;color:#a8bcc8;line-height:1.6;margin:0;text-align:center;">
    <i>AI-generated using live market data. Always verify independently. Not investment advice.</i><br>
    Sent by Briefly Wealth{unsub}
  </p>
</td></tr>
</table></td></tr></table></body></html>"""


# ── Generate Brief ─────────────────────────────────────────────

def generate_brief(date_str: str, market_data: dict,
                   futures_text: str = "", earnings_text: str = "",
                   econ_text: str = "",
                   recent_summaries: str | None = None) -> tuple[str, str, str, str]:
    """Returns: (market_card_html, greeting_hook, analysis_html, summary_json)"""
    user_msg = (
        f"Today is {date_str}. Write this morning's briefing. "
        f"ALL data is pre-computed below. Do NOT search for market data, futures, earnings, or economic releases. "
        f"Do ONE search for: overnight news/developments and a US-focused Water Cooler story."
    )
    if futures_text:
        user_msg += f"\n\n{futures_text}"
    if earnings_text:
        user_msg += f"\n\n{earnings_text}"
    if econ_text:
        user_msg += f"\n\n{econ_text}"
    if recent_summaries:
        # Build a short context summary from recent key_drivers
        drivers = []
        for line in recent_summaries.split("\n"):
            if "key_driver" in line:
                try:
                    j = json.loads(line.split("] ", 1)[1])
                    drivers.append(j.get("key_driver", ""))
                except (json.JSONDecodeError, IndexError):
                    pass
        if drivers:
            recent_drivers = ", ".join(dict.fromkeys(drivers))  # unique, ordered
            user_msg += f"\n\nCONTEXT: Recent days have been driven by: {recent_drivers}"

        user_msg += (
            f"\n\nRecent briefings (avoid repeating these angles):\n"
            f"---\n{recent_summaries}\n---\n"
            f"You MAY revisit a topic if the key_driver changed. "
            f"You MUST NOT reuse the same talking point angle, client script, or Water Cooler story."
        )

    payload = {
        "model": MODEL,
        "max_tokens": 2048,
        "system": SYSTEM_PROMPT,
        "tools": [{"type": "web_search_20250305", "name": "web_search"}],
        "messages": [{"role": "user", "content": user_msg}],
    }
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"API error {e.code}: {e.read().decode('utf-8', errors='replace')}", file=sys.stderr)
        raise

    text_blocks = [b["text"] for b in data.get("content", []) if b.get("type") == "text"]
    if not text_blocks:
        raise ValueError("API returned no text content")

    for i, block in enumerate(text_blocks):
        print(f"  Block {i}: {len(block)} chars")

    raw = "\n".join(text_blocks)
    greeting_hook, bottom_line, summary_json, analysis = parse_response(raw)
    market_card = build_market_card(market_data, bottom_line)
    return market_card, greeting_hook, analysis, summary_json


# ── Send Email ─────────────────────────────────────────────────

def send_email(html: str, date_str: str, to: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"The Briefly Morning Brief | {date_str}"
    msg["From"] = f"Briefly Wealth <{GMAIL_ADDRESS}>"
    msg["To"] = to
    msg["List-Unsubscribe"] = f"<mailto:{GMAIL_ADDRESS}?subject=Unsubscribe>"
    msg.attach(MIMEText(f"The Briefly Morning Brief - {date_str}\nBest viewed in HTML.", "plain"))
    msg.attach(MIMEText(html, "html"))
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
        s.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        s.sendmail(GMAIL_ADDRESS, [to], msg.as_string())


# ── Main ───────────────────────────────────────────────────────

def main():
    today = datetime.now()
    date_str = today.strftime("%B %d, %Y")
    print(f"=== The Briefly Morning Brief | {date_str} ===\n")

    if is_us_market_holiday(today.date()):
        print("US markets are closed today. No brief to send.")
        return

    print("Fetching subscribers...")
    subs = get_subscribers()
    if not subs:
        print("No active subscribers. Exiting.")
        return
    print(f"{len(subs)} active subscriber(s)\n")

    print("Fetching market data from Yahoo Finance...")
    market_data = fetch_market_data()
    if not market_data:
        print("Warning: Could not fetch market data. Proceeding with empty card.", file=sys.stderr)
    print(f"{len(market_data)}/7 tickers fetched\n")

    print("Fetching pre-market futures...")
    futures_text = fetch_futures()
    if not futures_text:
        print("No futures data available\n")
    else:
        print(f"Futures: {futures_text}\n")

    print("Fetching earnings calendar...")
    earnings_text = fetch_earnings_calendar()
    if not earnings_text:
        print("No upcoming earnings found in watchlist\n")
    else:
        print(f"Earnings data: {len(earnings_text)} chars\n")

    print("Fetching economic calendar from FRED...")
    econ_text = fetch_fred_calendar()
    if not econ_text:
        print("No FRED releases found in this window\n")
    else:
        print(f"Economic calendar: {len(econ_text)} chars\n")

    print("Fetching recent summaries...")
    recent_summaries = get_recent_summaries(10)
    if not recent_summaries:
        print("No previous summaries found (first run or empty table)\n")

    print("Generating brief...")
    market_card, greeting_hook, analysis, summary_json = generate_brief(
        date_str, market_data, futures_text, earnings_text, econ_text, recent_summaries
    )
    print(f"Market card: {len(market_card)} chars")
    print(f"Greeting hook: {len(greeting_hook)} chars")
    print(f"Analysis: {len(analysis)} chars")
    print(f"Summary: {len(summary_json)} chars\n")

    print("Sending...")
    ok, fail = 0, 0
    for s in subs:
        try:
            unsub = f"{UNSUBSCRIBE_BASE_URL}?token={s['unsubscribe_token']}"
            html = build_email_html(market_card, analysis, greeting_hook, date_str, s["name"], unsub)
            send_email(html, date_str, s["email"])
            label = s["name"] or s["email"]
            print(f"  \u2713 {label} <{s['email']}>")
            ok += 1
            time.sleep(1)
        except Exception as e:
            print(f"  \u2717 {s['email']} \u2014 {e}", file=sys.stderr)
            fail += 1

    # Save today's brief for future context
    if ok > 0:
        print("\nSaving brief to archive...")
        save_brief(date_str, greeting_hook, analysis, summary_json)

    print(f"\nDone. Sent: {ok} | Failed: {fail}")


if __name__ == "__main__":
    main()
