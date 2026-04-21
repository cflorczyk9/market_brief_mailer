"""
Daily Market Brief — Generate & Send (v3)
Per-section generation: Haiku for main + watch, Sonnet for Water Cooler.
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
from collections import Counter
import concurrent.futures
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
    "SUBSCRIBE_URL", "https://brieflywealth.com/#newsletter"
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


def get_yesterday_brief() -> dict | None:
    """Fetch yesterday's greeting hook and summary for narrative threading."""
    url = (
        f"{SUPABASE_URL}/rest/v1/briefs"
        f"?select=brief_date,greeting_hook,summary"
        f"&order=brief_date.desc&limit=1"
    )
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    })
    try:
        with urllib.request.urlopen(req) as resp:
            rows = json.loads(resp.read())
        if rows and rows[0].get("greeting_hook"):
            print(f"  Yesterday's brief ({rows[0]['brief_date']}): {rows[0]['greeting_hook'][:60]}...")
            return rows[0]
    except Exception as e:
        print(f"Warning: Could not fetch yesterday's brief: {e}", file=sys.stderr)
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

CONTINUOUS_TICKERS = {"CL=F", "GC=F", "BTC-USD"}


def _fetch_one_ticker(key, ticker, start_date, end_date, year_start, month_start):
    """Fetch a single ticker's data. Returns (key, data_dict) or (key, None)."""
    try:
        t = yf.Ticker(ticker)
        ed = end_date + timedelta(days=1) if ticker in CONTINUOUS_TICKERS else end_date
        hist = t.history(start=start_date.isoformat(), end=ed.isoformat())
        hist = hist.dropna(subset=["Close"])
        if hist.empty:
            return key, None

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

        return key, {"level": level, "ytd": ytd_str, "mtd": mtd_str}
    except Exception:
        return key, None


def fetch_market_data() -> dict:
    """Fetch closing prices + YTD/MTD returns from Yahoo Finance (parallel)."""
    today = date.today()
    year_start = date(today.year, 1, 1)
    month_start = date(today.year, today.month, 1)
    start_date = year_start - timedelta(days=10)

    result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
        futures = {
            executor.submit(_fetch_one_ticker, key, ticker, start_date, today, year_start, month_start): key
            for key, ticker in TICKERS.items()
        }
        for future in concurrent.futures.as_completed(futures):
            key, data = future.result()
            if data:
                result[key] = data
                print(f"  {key}: {data['level']}  YTD {data['ytd']}  MTD {data['mtd']}")
            else:
                print(f"  Warning: No data for {key}", file=sys.stderr)

    return result


def fetch_futures() -> str:
    """Fetch pre-market futures from Yahoo Finance (parallel). Returns compact text for the AI."""
    def _fetch_one_future(name, ticker):
        try:
            t = yf.Ticker(ticker)
            data = t.history(period="2d")
            if data.empty or len(data) < 2:
                return None
            prev_close = data["Close"].iloc[-2]
            current = data["Close"].iloc[-1]
            change_pct = ((current / prev_close) - 1) * 100
            sign = "+" if change_pct >= 0 else ""
            return f"{name} futures: {sign}{change_pct:.2f}%"
        except Exception:
            return None

    lines = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(_fetch_one_future, name, ticker): name for name, ticker in FUTURES_TICKERS.items()}
        for future in concurrent.futures.as_completed(futures):
            line = future.result()
            if line:
                lines.append(line)

    if not lines:
        return ""
    result = "PRE-MARKET FUTURES: " + " | ".join(lines)
    print(f"  {result}")
    return result


# ── Earnings Calendar (Yahoo Finance) ──────────────────────────

EARNINGS_WATCHLIST = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "TSLA", "AVGO", "BRK-B",
    "JPM", "V", "MA", "BAC", "GS", "MS", "BLK", "SCHW", "C", "AXP", "WFC",
    "USB", "PNC", "CB", "MMC",
    "UNH", "LLY", "JNJ", "ABBV", "MRK", "PFE", "TMO", "ABT", "DHR", "BMY",
    "GILD", "ISRG", "MDT", "AMGN", "CVS", "ELV", "CI",
    "WMT", "PG", "COST", "HD", "MCD", "KO", "PEP", "NKE", "SBUX", "TGT",
    "LOW", "TJX", "ROST", "DG", "DLTR", "CL", "EL",
    "ADBE", "CRM", "NFLX", "AMD", "INTC", "QCOM", "TXN", "ORCL", "NOW",
    "AMAT", "MU", "LRCX", "KLAC", "SNPS", "CDNS", "PANW", "CRWD",
    "CAT", "DE", "HON", "GE", "BA", "RTX", "LMT", "UPS", "FDX",
    "MMM", "GD", "NOC",
    "XOM", "CVX", "COP", "SLB", "EOG",
    "DIS", "CMCSA", "TMUS", "VZ",
    "ACN", "LEN", "ULTA", "DKS", "LULU", "FIS",
]

def fetch_earnings_calendar() -> str:
    """Fetch earnings for top 100 S&P 500 companies: today + next 5 trading days (parallel)."""
    today = date.today()
    window_end = today + timedelta(days=7)

    def _check_one(ticker):
        try:
            t = yf.Ticker(ticker)
            dates = t.earnings_dates
            if dates is None or dates.empty:
                return None
            for dt in dates.index:
                ed = dt.date()
                if today <= ed <= window_end:
                    time_str = ""
                    if hasattr(dt, 'hour'):
                        if dt.hour < 10:
                            time_str = " (BMO)"
                        elif dt.hour >= 16:
                            time_str = " (AMC)"
                    return (ed, ticker, time_str)
        except Exception:
            pass
        return None

    earnings = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(_check_one, EARNINGS_WATCHLIST)
        for r in results:
            if r:
                earnings.append(r)

    print(f"  Checked {len(EARNINGS_WATCHLIST)} tickers")

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

# Each entry: release_id -> (display_name, release_time_et, why_it_matters).
# release_time is ET; empty string means "see announcement" (FOMC statements float).
# why_it_matters is a short advisor-facing annotation — what the release tells a client.
FRED_RELEASES = {
    10:  ("CPI",                            "8:30 AM",  "inflation heat check; moves Fed"),
    46:  ("PPI",                            "8:30 AM",  "producer inflation; precedes CPI"),
    50:  ("Employment Situation (NFP)",     "8:30 AM",  "jobs report; biggest single mover"),
    180: ("Unemployment Claims",            "8:30 AM",  "weekly labor market pulse"),
    53:  ("GDP",                            "8:30 AM",  "growth scorecard; recession signal"),
    9:   ("Retail Sales",                   "8:30 AM",  "consumer spending pulse"),
    54:  ("Personal Income & Outlays",      "8:30 AM",  "includes PCE — Fed's inflation gauge"),
    13:  ("Industrial Production",          "9:15 AM",  "manufacturing sector pulse"),
    11:  ("Employment Cost Index",          "8:30 AM",  "quarterly wage pressure"),
    91:  ("Consumer Sentiment (Michigan)",  "10:00 AM", "household confidence bellwether"),
    14:  ("Consumer Credit",                "3:00 PM",  "borrowing appetite"),
    291: ("Existing Home Sales",            "10:00 AM", "existing-market housing pulse"),
    97:  ("New Home Sales",                 "10:00 AM", "new-construction housing pulse"),
    27:  ("Housing Starts",                 "8:30 AM",  "builder activity leading indicator"),
    192: ("JOLTs",                          "10:00 AM", "job openings; Fed watches closely"),
    101: ("FOMC Press Release",             "2:00 PM",  "Fed rate decision"),
}

def fetch_fred_calendar() -> str:
    """Fetch upcoming economic releases from FRED API for the next 7 days."""
    if not FRED_API_KEY:
        print("  No FRED_API_KEY set, skipping economic calendar")
        return ""

    today = date.today()
    window_end = today + timedelta(days=7)

    # Any market-moving indicator releases at most weekly. Anything that fires
    # more often is operational plumbing (daily Commercial Paper, weekly bank
    # balance sheets) and is noise in a pre-market brief. Drop it.
    past_30 = today - timedelta(days=30)

    releases = []
    for release_id, (name, release_time, why) in FRED_RELEASES.items():
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

            all_dates = [date.fromisoformat(rd["date"]) for rd in data.get("release_dates", [])]

            recent_count = sum(1 for d in all_dates if past_30 <= d <= today)
            if recent_count > 8:
                print(f"  Skipping release_id={release_id} ({name}): {recent_count} releases in last 30 days — operational, not market-moving")
                continue

            for rd_date in all_dates:
                if today <= rd_date <= window_end:
                    releases.append((rd_date, name, release_time, why))
                    break
        except Exception:
            continue

    if not releases:
        print("  No FRED releases found in this window")
        return ""

    releases.sort(key=lambda x: x[0])

    lines = []
    current_date = None
    for rd_date, name, release_time, why in releases:
        if rd_date != current_date:
            day_label = rd_date.strftime("%A, %b %d")
            if rd_date == today:
                day_label += " (today)"
            lines.append(f"\n{day_label}:")
            current_date = rd_date
        time_part = release_time if release_time else "(time TBD)"
        lines.append(f"  {time_part} — {name} — {why}")

    result = "ECONOMIC CALENDAR (upcoming releases):" + "".join(lines)
    print(f"  Found {len(releases)} releases in this window")
    return result


# ── Prompt Fragments ──────────────────────────────────────────

TONE_RULES = """Tone: sharp colleague in the hallway before the first call. Professional, clear, occasionally wry.

SIMPLE LANGUAGE EVERYWHERE. Every sentence should be understandable by someone in their first year in finance. When you have a choice between an industry term and a simpler way to say it, always pick the simpler version. "Collateral calls on levered credit positions are cascading" should be "Lenders are demanding their money back, and it's spreading." Industry terms are okay when there's no simpler alternative, but default to plain English. Specific rules:
- Write short, clear sentences. If a sentence needs two commas, split it.
- Headlines must read like a newspaper, not a trading desk: "The Fed rate cut timeline just got pushed back" not "Duration is pain again."
- Explain any law, policy, or non-obvious concept in the same sentence.
- Never say "the 10-year" alone — say "the 10-year Treasury yield."
- Avoid dense compound phrases like "stagflation risk reverses the rate-cut narrative." Say what's actually happening: "Inflation is rising and the economy is slowing, which makes it harder for the Fed to cut rates."
- The client script especially must sound like a human talking, not a research note.

POLITICAL NEUTRALITY. This newsletter is for financial advisors across the political spectrum. When covering policy (tariffs, regulation, fiscal spending, executive orders, Fed appointments, etc.), present what happened and why it matters for markets/portfolios without editorializing on whether the policy is good or bad. Specific rules:
- State the policy or action factually. "The White House proposed X" not "The White House's misguided X."
- Present market implications, not political opinions. "Tariffs on steel would raise input costs for manufacturers" is fine. "The tariff plan misses the real problem" is not.
- If there are competing views, attribute them: "Supporters say X, while critics argue Y." Don't adopt either side.
- Never use language like "soundbite solution," "political theater," "common-sense reform," or any phrasing that signals approval or disapproval of a policy.
- When covering trade policy, immigration policy, energy policy, defense spending, or tax changes, always frame through the lens of "what this means for portfolios" rather than "whether this is the right call."

SENTENCE VARIETY. Avoid repetitive openers and stock connectives within a brief and across briefs. Specific rules:
- BANNED PHRASES (do not use any of these, even once): "Now we see," "Now [this morning/today/the market]," "This matters because," "This matters for," "The real story is," "The real test is," "The real question is," "Here's the calculus," "Here's what to watch," "Here's the thing," "Watch for [X to Y]," "Sometimes the [X] isn't [Y]," "What's interesting is," "The reality is," "It's not [X], it's [Y]," "The bottom line is."
- Do not start two consecutive sentences with the same word or grammatical structure (e.g., not two sentences in a row that start with a noun phrase + "is").
- Vary sentence length within every paragraph. At least one short sentence (under 10 words) per paragraph.
- The greeting hook, bottom line, and Advisor Talking Point should not all open with the same grammatical pattern. If the greeting opens with a subject + verb, vary the bottom line opener (subordinate clause, prepositional phrase, contrasting conjunction, etc.).
- The italic closer in the Water Cooler must not follow the formula "Sometimes the [X] isn't [Y] — it's [Z]." Find a different shape.
"""

SYSTEM_PROMPT_MAIN = f"""You write a daily pre-market morning briefing. The TOP of the email (greeting + bottom line) is for ANYONE interested in markets — not just financial advisors. Write like a sharp friend explaining what's happening in the world this morning. No jargon, no insider framing. The advisor-specific content comes later in the Advisor Talking Point section.

{TONE_RULES}

IMPORTANT: Market data, pre-market futures, earnings calendar, AND economic calendar are ALL pre-computed in the user message. Do NOT search for any of these. Use 1 web search ONLY for: overnight news/developments that matter for markets today.

OUTPUT FORMAT — in this EXACT order:

GROUNDING — CRITICAL: Only describe a data release as "this morning" / "today" if it appears under the "(today)" header in the ECONOMIC CALENDAR block below. If the calendar shows a release on a future day, write it as anticipation for that specific weekday, not today. If the calendar is empty, do not invent a release. Use the release time printed in the calendar block — never invent a time.

LINE 1 — GREETING HOOK: One sentence. Start with the big picture — what's happening in the world this morning and why it matters. Lead with the story, not the data release. Frame it as something a curious person would want to know, not something a trader needs to react to. This is a pre-market briefing, so frame it as anticipation, not recap. If yesterday's story set up a question that today answers, the hook should frame that question in plain, human terms. Good: "Retail sales will tell us whether the hiring strength is actually showing up in spending." Bad: "Retail Sales and Housing Starts drop at 8:30 AM." Prioritize variety across days — if an ongoing story dominated recent greetings, find a different angle and fold the ongoing story into the bottom line instead. <p class="greeting-hook"> tags.

LINE 2 — BOTTOM LINE: 2-3 flowing sentences. Plain prose, NO labels, NO sub-headings. This is the zoom-in from the hook: explain what's actually happening, why today matters, and where markets are sitting. Mention futures direction. The tone is still accessible and human — you're telling a story, not writing a research note. Connect yesterday's story to today's setup when relevant: [short callback to yesterday — a phrase, not a re-explanation] + [what's different today] + [what's at stake for people watching]. Do NOT restate yesterday's data points (no "178,000 payrolls added" if that was in yesterday's brief). Assume the reader was there. Use shorthand callbacks: "Friday's hiring strength," "that tariff announcement," "the oil spike." Good example (structure, not content — use whatever release is actually on today's calendar): "The economy added jobs at a pace no one expected last Friday, and that strength pushed bond yields higher. Retail sales this morning test whether that hiring is translating into real spending, or if households are sitting on their hands. Futures are flat — the market's waiting for the answer." <p class="bottom-line"> tags. <b> tags on numbers.

LINE 3 — SUMMARY JSON (one line):
{{"headline":"~10 words","primary_theme":"one of: energy, rates/fed, earnings, trade/tariffs, tech, housing, labor, credit, geopolitics, consumer, crypto, other","talking_point_theme":"one of the same theme categories — MUST differ from primary_theme","talking_point":"angle + WHY in ~15 words","client_script_topic":"topic + framing ~10 words","key_driver":"underlying reason ~10 words"}}

Then a blank line, then EXACTLY this HTML section:

<div class="section section-advisor">
<h2>Advisor Talking Point</h2>
<h3>[Simple, clear headline. Newspaper style, no jargon.]</h3>
<p>THIS SECTION shifts to advisor-specific content. The greeting and bottom line above were written for a broad audience — this section is where you speak directly to financial advisors. 2 short paragraphs. HARD LIMIT: 150-200 words. Must cover a DIFFERENT SECTOR OR THEME than the greeting and bottom line. If the greeting and bottom line covered energy/oil, this section must cover something else entirely — earnings, credit markets, consumer data, housing, tech, labor, etc.</p>
<div class="client-script">
<p class="client-script-label">If a client asks about [topic]</p>
<p>2 sentences max. Sound like a person talking, not a research note.</p>
</div>
</div>

NARRATIVE THREADING (critical — makes the brief feel like one continuous story across days):
This is a PRE-MARKET briefing. You write BEFORE data comes out, not after. That means threading works as an anticipation cycle across days. Examples use placeholder release names — substitute whatever the ECONOMIC CALENDAR actually shows today:
- Day 1: You set up a story. ("Jobs came in strong. Here's what that means for rate expectations.")
- Day 2: Today's data release tests yesterday's thesis. Frame it as a question. ("After Friday's hiring surprise pushed yields higher, this morning's [release from today's calendar] is the gut check — does the other side of the economy line up with the jobs picture, or not?")
- Day 3: Yesterday's data came in. NOW you can resolve it and advance the narrative. ("[Yesterday's release] disappointed yesterday despite the strong jobs market. That gap is the story heading into the next data point.")
Each day either SETS UP a question, FRAMES a test of a previous setup, or RESOLVES a question from the day before. Never just drop yesterday's data into the middle of a paragraph without connecting it to today's angle.
- Threading is NOT repeating. Never re-explain yesterday's data. Assume the reader saw it. A short callback phrase ("Friday's hiring strength," "that tariff announcement," "the oil spike") is enough before pivoting to what's new today.
- Threading is NOT continuing the same theme as the lead. Always lead with what's genuinely new today. Yesterday's story becomes context in the bottom line, not the headline.
- If yesterday's story has zero relevance to today, don't force a connection. Just write a clean, fresh brief.

SECTION DIVERSITY RULES (critical):
1. The greeting + bottom line usually cover today's biggest story. However, if the anti-repetition context includes a GREETING LEAD ROTATION warning, you MUST avoid that theme entirely — not just in the greeting, but in the bottom line and talking point too. The entire editorial brief should feel fresh. Only the What to Watch calendar is exempt.
2. The Advisor Talking Point MUST cover a DIFFERENT THEME/SECTOR than the greeting and bottom line. If the top story is energy, the talking point should be about earnings, credit, housing, consumer data, labor, tech, or anything else. Find a fresh angle from the earnings calendar, economic data, or a different market sector.
3. ADJACENCY GUARD: A different theme label is not enough. The Advisor Talking Point must not be a downstream consequence, sector-application, or reframing of the SAME underlying story as the greeting/bottom line. It must be a genuinely separate story line. Examples of what's BANNED: greeting=oil spike → TP=how earnings season copes with higher oil (same story, different sector); greeting=bank earnings beat → TP=Fed rate path implications for those banks (same story, different framing); greeting=Iran ceasefire → TP=credit spreads tightening on the relief (same story, different asset class). Examples of what's GOOD: greeting=oil spike → TP=housing affordability under stuck mortgage rates; greeting=bank earnings → TP=a specific sector's pre-announcement (e.g., a retailer's guidance cut). Test yourself: if you removed the greeting/bottom line entirely, would the talking point still make sense as a standalone story? If no, it's adjacency — pick something else.
4. Never state the same fact, company name, data point, or event in both the bottom line and the talking point.
5. The primary_theme and talking_point_theme fields in the summary JSON MUST be different from each other.

Total across greeting + bottom line + talking point: 200-300 words. No more. Start with greeting hook. No preamble.

Then, AFTER the Advisor Talking Point section, output the "What to Watch" calendar section using the pre-computed earnings AND economic calendar data from the user message.

CALENDAR RULES (strict):
1. Copy the date header, release time, release name, and "why it matters" annotation EXACTLY from the ECONOMIC CALENDAR block below. Do not invent a release time. Do not paraphrase the annotation.
2. Use the calendar's date labels verbatim ("Tuesday, Apr 21 (today)", "Thursday, Apr 23", etc.). Do NOT re-bucket events under "Today" / "Tomorrow" / "Next Week" generic headers.
3. Render each economic release as a table row: time in the first cell, "Release Name — why it matters" in the second cell.
4. If an earnings block is provided, add an Earnings row under the matching date with the ticker list verbatim. If no earnings data is provided, OMIT the Earnings row entirely. Never write "No earnings this week", "No major S&P 500 earnings today", or any similar placeholder — an empty cell is wrong, a missing row is right.
5. Never use markdown formatting. Bold is <b>X</b>, italics are <i>X</i>. Do not write **X** or *X*.
6. Do not emit any tool-use tags such as &lt;invoke&gt;, &lt;function_calls&gt;, or similar. The output must be clean HTML only.

Output this HTML shape (the specific dates/releases below are a structural example — replace with whatever the data blocks below actually contain):

<div class="section">
<h2>What to Watch</h2>
<table class="watch-calendar">
<tr class="watch-group"><td colspan="2">Tuesday, Apr 21 (today)</td></tr>
<tr><td class="watch-time">8:30 AM</td><td class="watch-desc">Retail Sales — consumer spending pulse</td></tr>
<tr><td class="watch-time">Earnings</td><td class="watch-desc">Company Name (TICK), Company Name (TICK)</td></tr>
<tr class="watch-group"><td colspan="2">Thursday, Apr 23</td></tr>
<tr><td class="watch-time">8:30 AM</td><td class="watch-desc">Unemployment Claims — weekly labor market pulse</td></tr>
<tr class="watch-group"><td colspan="2">Friday, Apr 24</td></tr>
<tr><td class="watch-time">10:00 AM</td><td class="watch-desc">Consumer Sentiment (Michigan) — household confidence bellwether</td></tr>
</table>
</div>

The calendar table is the ENTIRE section — no prose before or after."""

SYSTEM_PROMPT_WATERCOOLER = f"""You write the "Water Cooler" section for a daily financial advisor morning briefing. Your job is to find ONE interesting, real, US-focused story that is completely unrelated to the main market news of the day.

{TONE_RULES}

CRITICAL RULES:
1. You MUST use web search to find a REAL story. NEVER invent or fabricate a story. Only use information that appears in your search results. If your first search overlaps with today's topics, try different queries (e.g. "quirky business news today", "unusual corporate story this week", "surprising industry milestone", "weird business trend").
2. The story must be COMPLETELY UNRELATED to the topics already covered in today's briefing (provided below). Different companies, different sectors, different subject matter entirely.
3. The story must NOT repeat any topic from recent briefings (provided below).
4. Keep it observational and wry, not editorial. The italic sentence at the end should connect to advising or markets, not pass judgment on policymakers.
5. US-focused stories only.

OUTPUT FORMAT — output ONLY this HTML, nothing else:

<div class="section section-watercooler">
<h2>Water Cooler</h2>
<h3>[Catchy headline]</h3>
<p>HARD LIMIT: 50-75 words. End with one italic sentence connecting it to advising.</p>
</div>

Also output on a separate line after the closing </div>:
WATER_COOLER_SUMMARY: [company/org name or key noun] | [10 word description of the story]

Example: "Trader Joe's | Glass contamination recall expands to fried rice products"
Example: "UC Riverside | Study finds AI data centers will consume NYC's daily water supply"

No preamble. Start directly with the <div> tag."""


# ── API Helper ────────────────────────────────────────────────

def call_anthropic(model: str, system: str, user_msg: str,
                   max_tokens: int = 1024, use_search: bool = False) -> str:
    """Call Anthropic API and return joined text blocks."""
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user_msg}],
    }
    if use_search:
        payload["tools"] = [{"type": "web_search_20250305", "name": "web_search"}]

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

    usage = data.get("usage", {})
    in_tok = usage.get("input_tokens", 0)
    out_tok = usage.get("output_tokens", 0)
    print(f"  Tokens: {in_tok} in / {out_tok} out ({model})")

    return "\n".join(text_blocks)


# ── Parse Helpers ─────────────────────────────────────────────

def _md_bold_to_html(text: str) -> str:
    """Convert leaked markdown **bold** to <b>bold</b>. Haiku sometimes emits
    markdown despite the prompt asking for <b> tags; this normalizes before
    the text lands in HTML."""
    return re.sub(r'\*\*([^*\n]+?)\*\*', r'<b>\1</b>', text)


def _strip_model_artifacts(html: str) -> str:
    """Remove stray tokens the model occasionally emits: tool-use-ish tags,
    the WATER_COOLER_SUMMARY bookkeeping line, and any lingering markdown
    bold. These would otherwise render as literal text in the email."""
    html = re.sub(r'</?invoke[^>]*>', '', html)
    html = re.sub(r'</?antml:[^>]+>', '', html)
    html = re.sub(r'^\s*WATER_COOLER_SUMMARY:.*$', '', html, flags=re.MULTILINE)
    html = _md_bold_to_html(html)
    return html


def parse_main_response(raw: str) -> tuple[str, str, str, str]:
    """Parse greeting + bottom line + advisor section from main call."""

    greeting_hook = ""
    hook_match = re.search(r'<p class="greeting-hook">(.*?)</p>', raw, re.DOTALL)
    if hook_match:
        greeting_hook = _md_bold_to_html(hook_match.group(1).strip())
        print(f"  Greeting hook: {greeting_hook[:80]}...")

    bottom_line = ""
    bl_match = re.search(r'<p class="bottom-line">(.*?)</p>', raw, re.DOTALL)
    if bl_match:
        bottom_line = _md_bold_to_html(bl_match.group(1).strip())
        print(f"  Bottom line: {bottom_line[:80]}...")

    summary_json = ""
    sum_match = re.search(r'\{["\']headline["\'].*?\}', raw, re.DOTALL)
    if sum_match:
        try:
            raw_sum = sum_match.group().replace('\n', '')
            json.loads(raw_sum)
            summary_json = raw_sum
            print(f"  Summary parsed: {summary_json[:80]}...")
        except json.JSONDecodeError:
            print("  Warning: Could not parse summary JSON", file=sys.stderr)

    advisor_html = ""
    first_div = raw.find('<div class="section')
    if first_div >= 0:
        section = raw[first_div:]
        clean = []
        in_html = False
        for line in section.split('\n'):
            s = line.strip()
            if any(s.startswith(t) for t in ['<div', '<h2', '<h3', '<p', '</', '<table', '<tr', '<td']):
                in_html = True
            if in_html or s == '' or s.startswith('<') or '<b>' in s or '<i>' in s:
                clean.append(line)
            if s == '</div>' and '</td>' not in s:
                in_html = False
        advisor_html = _strip_model_artifacts('\n'.join(clean))

    return greeting_hook, bottom_line, summary_json, advisor_html.strip()


def parse_html_section(raw: str) -> str:
    """Extract clean HTML section from a response."""
    first_div = raw.find('<div class="section')
    if first_div >= 0:
        section = raw[first_div:]
        clean = []
        in_html = False
        for line in section.split('\n'):
            s = line.strip()
            if any(s.startswith(t) for t in ['<div', '<h2', '<h3', '<p', '</', '<table', '<tr', '<td']):
                in_html = True
            if in_html or s == '' or s.startswith('<') or '<b>' in s or '<i>' in s:
                clean.append(line)
            if s == '</div>' and '</td>' not in s:
                in_html = False
        return _strip_model_artifacts('\n'.join(clean)).strip()
    return _strip_model_artifacts(raw).strip()


def parse_watercooler_summary(raw: str) -> str:
    """Extract WATER_COOLER_SUMMARY line from watercooler response."""
    for line in raw.split('\n'):
        if line.strip().startswith('WATER_COOLER_SUMMARY:'):
            return line.split(':', 1)[1].strip()
    return ""


# ── Build Market Card ──────────────────────────────────────────

def build_market_card(data: dict, bottom_line: str) -> str:
    """Build the Layout D unified market table + bottom line."""
    if not data:
        return ""

    F = "font-family:Georgia,'Times New Roman',serif;"
    blank = {"level": "\u2014", "ytd": "\u2014", "mtd": "\u2014"}

    def get(key):
        return data.get(key, blank)

    def color(v):
        v = v.strip()
        if v.startswith("+"): return "#1a7a3a"
        elif v.startswith("-"): return "#b91c1c"
        return "#6b8db5"

    hdr_c = f'{F}font-size:11px;font-weight:bold;letter-spacing:0.8px;text-transform:uppercase;color:#6b8db5;padding:0 6px 6px;text-align:right;'
    hdr_cl = f'{F}font-size:11px;font-weight:bold;letter-spacing:0.8px;text-transform:uppercase;color:#6b8db5;padding:0 6px 6px;text-align:left;'
    idx_c = f'{F}font-size:14px;font-weight:bold;color:#142d4c;padding:7px 6px;'
    num_c = f'{F}font-size:14px;padding:7px 6px;text-align:right;font-variant-numeric:tabular-nums;'

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
    card_lbl = f'{F}font-size:10px;font-weight:bold;letter-spacing:1.5px;text-transform:uppercase;color:#6b8db5;margin:0 0 10px;'

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

    if bottom_line:
        html += (
            f'<p style="{F}font-size:16px;line-height:1.7;color:#2c3e50;'
            f'margin:0 0 0;padding:0 0 20px;border-bottom:1px solid #dce3eb;">'
            f'{bottom_line}</p>'
        )

    return html


# ── Inline Styles for Email ────────────────────────────────────

FONT = "font-family:Georgia,'Times New Roman',serif;"
S_ADVISOR = f"background:#f4f7fa;padding:28px 24px;border-left:4px solid #142d4c;border-bottom:1px solid #dce3eb;margin:0 -24px;margin-bottom:0;"
S_WATERCOOLER = f"background:#f8f9fb;padding:28px 24px;margin:0 -24px;border-bottom:none;"
S_SECTION = f"margin:0;padding:28px 0;border-bottom:1px solid #dce3eb;"
S_SECTION_LAST = f"margin:0;padding:28px 0;border-bottom:none;"
S_H2 = f"{FONT}font-size:11px;font-weight:bold;color:#6b8db5;margin:0 0 8px;letter-spacing:2.5px;text-transform:uppercase;"
S_H3 = f"{FONT}font-size:20px;font-weight:normal;color:#142d4c;margin:0 0 14px;letter-spacing:-0.3px;line-height:1.3;"
S_P = f"{FONT}font-size:16px;line-height:1.65;color:#2c3e50;margin:0 0 12px;"
S_CLIENT_SCRIPT = f"background:#ffffff;border:1px solid #dce3eb;border-radius:4px;padding:18px 20px;margin-top:16px;"
S_CLIENT_LABEL = f"{FONT}font-size:11px;font-weight:bold;letter-spacing:1.5px;text-transform:uppercase;color:#142d4c;margin:0 0 8px;"
S_CLIENT_P = f"{FONT}font-size:15px;line-height:1.65;color:#3a4a5c;margin:0 0 8px;"
S_WATCH_TABLE = f"width:100%;border-collapse:collapse;margin:4px 0 0;"
S_WATCH_GROUP_TD = f"{FONT}font-size:12px;font-weight:bold;letter-spacing:1px;text-transform:uppercase;color:#6b8db5;padding:14px 0 6px;"
S_WATCH_TIME = f"{FONT}font-size:14px;color:#142d4c;font-weight:bold;width:80px;white-space:nowrap;padding:6px 12px 6px 0;vertical-align:top;"
S_WATCH_DESC = f"{FONT}font-size:15px;color:#2c3e50;line-height:1.5;padding:6px 0;vertical-align:top;"


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
    html = re.sub(r'<cite[^>]*>', '', html)
    html = re.sub(r'</cite>', '', html)
    html = re.sub(r'\*([^*]+)\*', r'<i>\1</i>', html)
    html = re.sub(r'<p(?!\s+style)>', f'<p style="{S_P}">', html)
    def fix_client(match):
        block = match.group(0)
        block = re.sub(rf'<p style="{re.escape(S_P)}">', f'<p style="{S_CLIENT_P}">', block)
        return block
    html = re.sub(rf'<div style="{re.escape(S_CLIENT_SCRIPT)}">.*?</div>', fix_client, html, flags=re.DOTALL)
    sections = list(re.finditer(re.escape(f'<div style="{S_SECTION}">'), html))
    if sections:
        last = sections[-1]
        old = last.group(0)
        new = old.replace(S_SECTION, S_SECTION_LAST)
        idx = last.start()
        html = html[:idx] + new + html[idx + len(old):]
    html = re.sub(r' class="[^"]*"', '', html)

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
    greeting = ""
    first_name = name.split()[0] if name else ""
    if first_name and greeting_hook:
        greeting = (
            f'<p style="font-family:Georgia,\'Times New Roman\',serif;font-size:16px;'
            f'color:#2c3e50;margin:0 0 20px 0;line-height:1.65;">'
            f'Good morning, {first_name}. {greeting_hook}</p>'
        )
    elif first_name:
        greeting = (
            f'<p style="font-family:Georgia,\'Times New Roman\',serif;font-size:16px;'
            f'color:#2c3e50;margin:0 0 20px 0;">Good morning, {first_name}.</p>'
        )
    elif greeting_hook:
        greeting = (
            f'<p style="font-family:Georgia,\'Times New Roman\',serif;font-size:16px;'
            f'color:#2c3e50;margin:0 0 20px 0;line-height:1.65;">'
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

    styled_analysis = inline_analysis_styles(analysis)

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<style>
  @media screen and (max-width: 680px) {{
    .outer-table {{ padding: 0 !important; }}
    .inner-table {{ width: 100% !important; border-radius: 0 !important; }}
  }}
</style>
</head>
<body style="margin:0;padding:0;background:#e8eef4;font-family:Georgia,'Times New Roman',serif;">
<table width="100%" cellpadding="0" cellspacing="0" class="outer-table" style="background:#e8eef4;padding:32px 16px;">
<tr><td align="center">
<!--[if mso]><table width="640" cellpadding="0" cellspacing="0"><tr><td><![endif]-->
<table cellpadding="0" cellspacing="0" class="inner-table" style="background:#ffffff;border-radius:4px;overflow:hidden;width:100%;max-width:640px;">

<!-- Header -->
<tr><td style="background:#ffffff;padding:28px 24px 0;text-align:center;">
  <p style="font-family:Georgia,'Times New Roman',serif;font-size:11px;color:#6b8db5;margin:0 0 14px;letter-spacing:3px;text-transform:uppercase;">Briefly Wealth</p>
  <table width="100%" cellpadding="0" cellspacing="0"><tr><td style="border-top:2px solid #142d4c;height:0;font-size:0;line-height:0;"></td></tr></table>
  <h1 style="font-family:Georgia,'Times New Roman',serif;font-size:32px;font-weight:normal;color:#142d4c;margin:14px 0 12px;letter-spacing:-0.5px;line-height:1.1;">The Morning Brief</h1>
  <table width="100%" cellpadding="0" cellspacing="0"><tr><td style="border-top:1px solid #142d4c;height:0;font-size:0;line-height:0;"></td></tr></table>
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:10px;"><tr>
    <td style="text-align:left;"><p style="font-family:Georgia,'Times New Roman',serif;font-size:13px;color:#4a5d72;margin:0;">{newspaper_date}</p></td>
    <td style="text-align:right;"><p style="font-family:Georgia,'Times New Roman',serif;font-size:13px;color:#4a5d72;margin:0;">Pre-Market Edition</p></td>
  </tr></table>
  <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:10px;"><tr><td style="border-top:1px solid #d0dae4;height:0;font-size:0;line-height:0;"></td></tr></table>
</td></tr>

<!-- Body -->
<tr><td style="padding:24px 24px 0;">
  {greeting}{market_card}{styled_analysis}
</td></tr>

<!-- Subscribe CTA -->
<tr><td style="padding:24px 24px;border-top:1px solid #d4dee8;background:#f4f7fa;text-align:center;">
  <p style="font-family:Georgia,'Times New Roman',serif;font-size:15px;color:#2c3e50;margin:0 0 14px;">Know an advisor who'd find this useful?</p>
  <a href="{SUBSCRIBE_URL}" style="display:inline-block;font-family:Georgia,'Times New Roman',serif;font-size:15px;color:#ffffff;background:#142d4c;padding:12px 28px;border-radius:4px;text-decoration:none;letter-spacing:0.3px;">Subscribe to The Morning Brief</a>
</td></tr>

<!-- Footer -->
<tr><td style="padding:16px 24px 20px;border-top:1px solid #e4ecf4;background:#f4f7fa;">
  <p style="font-family:Georgia,'Times New Roman',serif;font-size:11px;color:#8aacc8;line-height:1.6;margin:0;text-align:center;">
    <i>AI-generated using live market data. Always verify independently. Not investment advice.</i><br>
    Sent by Briefly Wealth{unsub}
  </p>
</td></tr>
</table>
<!--[if mso]></td></tr></table><![endif]-->
</td></tr></table></body></html>"""


# ── Generate Brief (per-section, hybrid) ─────────────────────

def generate_brief(date_str: str, market_data: dict,
                   futures_text: str = "", earnings_text: str = "",
                   econ_text: str = "",
                   recent_summaries: str | None = None,
                   yesterday_brief: dict | None = None) -> tuple[str, str, str, str]:
    """Returns: (market_card_html, greeting_hook, analysis_html, summary_json)"""

    # ── Build anti-repetition context ──
    anti_rep = ""
    if recent_summaries:
        # Extract structured theme data from recent briefs
        themes = []          # (date, primary_theme)
        tp_themes = []       # (date, talking_point_theme)
        cs_topics = []       # client_script_topic strings
        drivers = []
        greeting_headlines = []  # (date, headline, primary_theme)
        for line in recent_summaries.split("\n"):
            try:
                date_part, json_part = line.split("] ", 1)
                brief_date = date_part.strip("[")
                j = json.loads(json_part)
                if j.get("primary_theme"):
                    themes.append((brief_date, j["primary_theme"]))
                if j.get("talking_point_theme"):
                    tp_themes.append((brief_date, j["talking_point_theme"]))
                if j.get("client_script_topic"):
                    cs_topics.append(j["client_script_topic"])
                if j.get("key_driver"):
                    drivers.append(j["key_driver"])
                if j.get("headline"):
                    greeting_headlines.append((brief_date, j["headline"], j.get("primary_theme", "")))
            except (json.JSONDecodeError, IndexError, ValueError):
                pass

        # Count how often each theme has been the primary theme recently
        primary_counts = Counter(t[1] for t in themes)

        # Build the context the model sees
        if drivers:
            recent_drivers = ", ".join(dict.fromkeys(drivers))
            anti_rep += f"\nCONTEXT: Recent days have been driven by: {recent_drivers}"

        # Theme frequency warning
        overused = [t for t, c in primary_counts.items() if c >= 3]
        if overused:
            anti_rep += (
                f"\n\nTHEME SATURATION WARNING: These themes have dominated recent briefs: "
                f"{', '.join(overused)}. "
                f"Your Advisor Talking Point MUST NOT cover any of these themes. "
                f"Choose from: earnings, credit, housing, labor, consumer, tech, trade/tariffs, rates/fed, or other under-covered sectors."
                f" Additionally, strongly avoid leading the greeting with these "
                f"saturated themes unless a discrete new event demands it."
            )

        # Detect dominant greeting theme in recent window
        # Group related themes so e.g. "energy" and "geopolitics" covering
        # the same oil/conflict story count together
        _THEME_GROUPS = {
            "energy": "energy/geopolitics", "geopolitics": "energy/geopolitics",
            "rates/fed": "macro", "consumer": "macro", "labor": "macro",
        }
        def _group(t): return _THEME_GROUPS.get(t, t)

        # Consecutive-day detector
        consecutive_same = None
        if len(themes) >= 2:
            yesterday_group = _group(themes[-1][1])
            day_before_group = _group(themes[-2][1])
            if yesterday_group == day_before_group:
                consecutive_same = yesterday_group

        # Dominant theme detector (window: last 4 briefs)
        dominant_label = None
        dominant_count = 0
        recent_window = 4
        if len(themes) >= 2:
            recent_groups = [_group(t) for _, t in themes[-recent_window:]]
            group_counts = Counter(recent_groups)
            dominant_label, dominant_count = group_counts.most_common(1)[0]

        rotation_triggered = False

        # Tier 1 — Hard rotation: 3+ of last 4, or 2+ with consecutive days
        if dominant_count >= 3 or (dominant_count >= 2 and consecutive_same):
            rotation_triggered = True
            print(f"    ⚠ HARD greeting rotation: '{dominant_label}' led {dominant_count}/last {min(len(themes), recent_window)} briefs (consecutive={bool(consecutive_same)})")
            anti_rep += (
                f"\n\nGREETING LEAD ROTATION — MANDATORY: The greeting has led with "
                f"'{dominant_label}' for {dominant_count} of the last "
                f"{min(len(themes), recent_window)} days. Today's brief must be ENTIRELY "
                f"about a different theme. Do NOT mention {dominant_label} anywhere in the "
                f"greeting hook, bottom line, or advisor talking point — not as the lead, "
                f"not as context, not as a modifier, not at all. The entire editorial "
                f"brief must read as if {dominant_label} does not exist today. "
                f"Choose from earnings, economic data, a sector move, credit markets, "
                f"housing, labor, consumer data, tech, or any angle that has NOT appeared "
                f"in recent briefs. Only the What to Watch calendar is exempt."
            )

        # Tier 2 — Soft rotation: 2 of last 4 (not consecutive)
        elif dominant_count >= 2 and dominant_label:
            rotation_triggered = True
            print(f"    ⚡ Soft greeting rotation: '{dominant_label}' led {dominant_count}/last {min(len(themes), recent_window)} briefs")
            anti_rep += (
                f"\n\nGREETING DIVERSITY NOTICE: '{dominant_label}' has led "
                f"{dominant_count} of the last {min(len(themes), recent_window)} greetings. "
                f"Strongly prefer avoiding {dominant_label} entirely today — not just in "
                f"the greeting, but in the bottom line and talking point too. Write the "
                f"brief as if {dominant_label} is not a story today. Only mention "
                f"{dominant_label} if there is a MAJOR, discrete event (e.g., "
                f"a country declares war, a central bank makes an emergency rate decision, "
                f"a Fortune 50 company collapses) — not a continuation or incremental "
                f"update of an ongoing story."
            )

        if greeting_headlines:
            recent_greetings = [f"{d}: [{t}] {h}" for d, h, t in greeting_headlines[-5:]]
            anti_rep += (
                f"\n\nRECENT GREETING HEADLINES (avoid repeating the same angle):\n"
                + "\n".join(f"- {x}" for x in recent_greetings)
            )

        # Talking point theme saturation + hard block on recent TP themes
        if tp_themes:
            tp_counts = Counter(t[1] for t in tp_themes)
            tp_overused = [t for t, c in tp_counts.items() if c >= 3]
            if tp_overused:
                print(f"    ⚠ TP saturation: {tp_overused} appeared 3+ times in last {len(tp_themes)} briefs")
                anti_rep += (
                    f"\n\nTALKING POINT SATURATION WARNING: These themes have dominated "
                    f"the Advisor Talking Point in recent briefs: {', '.join(tp_overused)}. "
                    f"Today's talking_point_theme MUST NOT be any of these. "
                    f"Choose from earnings, credit, housing, labor, consumer, tech, "
                    f"trade/tariffs, rates/fed, geopolitics, energy, crypto, or another "
                    f"under-covered sector."
                )

            recent_tp_block = list(dict.fromkeys(t for _, t in tp_themes[-3:]))
            recent_tp_display = [f"{d}: {t}" for d, t in tp_themes[-5:]]
            anti_rep += (
                f"\n\nRECENT TALKING POINT THEMES (last 5 briefs):\n"
                + "\n".join(f"- {x}" for x in recent_tp_display)
                + f"\n\nHARD BLOCK: today's talking_point_theme MUST differ from the "
                f"last 3 TP themes ({', '.join(recent_tp_block)}). No exceptions."
            )

        # Recent client scripts (must not reuse)
        if cs_topics:
            anti_rep += (
                f"\n\nRECENT CLIENT SCRIPTS (do NOT reuse these topics):\n"
                + "\n".join(f"- {x}" for x in cs_topics[-5:])
            )

        if rotation_triggered:
            anti_rep += (
                f"\n\nReminder: do NOT mention {dominant_label} anywhere in the brief — "
                f"not in the greeting hook, not in the bottom line, not in the talking point, "
                f"not even as a passing reference. The entire editorial brief must be fresh. "
                f"The Advisor Talking Point MUST also be a different theme from the last 2 "
                f"talking point themes above. "
                f"The client script MUST NOT reuse any topic from the list above."
            )
        else:
            anti_rep += (
                f"\n\nThe greeting and bottom line should cover today's most notable story. "
                f"Prefer a FRESH angle or theme when possible — variety keeps readers engaged. "
                f"If the biggest story is the same theme as yesterday's greeting, consider "
                f"whether there is a genuinely different angle worth leading with instead, "
                f"and weave the ongoing story into the bottom line. "
                f"The Advisor Talking Point MUST cover a DIFFERENT THEME than the "
                f"greeting/bottom line AND different from the last 2 talking point themes above. "
                f"The client script MUST NOT reuse any topic from the list above."
            )

    # ── CALL 1: Greeting + Bottom Line + Advisor + What to Watch (Haiku, web search) ──
    print("\n  [1/2] Generating main brief + What to Watch (Haiku)...")
    main_msg = (
        f"Today is {date_str}. Write this morning's briefing. "
        f"ALL data is pre-computed below. Do NOT search for market data, futures, earnings, or economic releases. "
        f"Do ONE search for: overnight news/developments that matter for markets today."
    )
    if futures_text:
        main_msg += f"\n\n{futures_text}"
    if earnings_text:
        main_msg += f"\n\n{earnings_text}"
    else:
        main_msg += (
            "\n\nEARNINGS: no S&P 500 earnings were returned for the next 7 days. "
            "In the What to Watch table, OMIT the Earnings row entirely — do not add a "
            "placeholder row, do not say 'No earnings this week', do not name any companies. "
            "Each date group should contain only its economic-release rows. "
            "Do not reference any earnings dates in the greeting, bottom line, or talking point."
        )
    if econ_text:
        main_msg += f"\n\n{econ_text}"
    else:
        main_msg += (
            "\n\nECONOMIC CALENDAR: no FRED releases were returned for the next 7 days. "
            "Do NOT invent a release. Do NOT claim any indicator is releasing today. "
            "If there are also no earnings, OMIT the entire What to Watch section — do not "
            "emit the <div class=\"section\"><h2>What to Watch</h2>...</div> block at all."
        )

    # Inject yesterday's brief for narrative threading
    if yesterday_brief:
        yb_hook = yesterday_brief.get("greeting_hook", "")
        yb_date = yesterday_brief.get("brief_date", "")
        yb_summary = yesterday_brief.get("summary", "")
        if yb_hook:
            main_msg += f"\n\nYESTERDAY'S BRIEF ({yb_date}):"
            main_msg += f"\nGreeting hook: {yb_hook}"
            if yb_summary:
                try:
                    yb_json = json.loads(yb_summary)
                    if yb_json.get("headline"):
                        main_msg += f"\nHeadline: {yb_json['headline']}"
                    if yb_json.get("primary_theme"):
                        main_msg += f"\nTheme: {yb_json['primary_theme']}"
                    if yb_json.get("key_driver"):
                        main_msg += f"\nKey driver: {yb_json['key_driver']}"
                except (json.JSONDecodeError, ValueError):
                    pass
            main_msg += (
                "\nUse this to THREAD today's brief with yesterday's when relevant. "
                "Do not repeat yesterday's story — connect it to what's new today."
            )

    if anti_rep:
        main_msg += f"\n{anti_rep}"

    main_raw = call_anthropic(MODEL, SYSTEM_PROMPT_MAIN, main_msg,
                              max_tokens=2048, use_search=True)
    greeting_hook, bottom_line, summary_json, sections_html = parse_main_response(main_raw)
    market_card = build_market_card(market_data, bottom_line)

    # ── CALL 2: Water Cooler (Haiku, web search) ──
    print("\n  [2/2] Generating Water Cooler (Haiku)...")

    covered_topics = "TOPICS ALREADY COVERED IN TODAY'S BRIEFING (do NOT overlap with these):\n"
    covered_topics += f"- Greeting: {greeting_hook}\n"
    covered_topics += f"- Bottom line: {bottom_line}\n"
    if summary_json:
        try:
            sj = json.loads(summary_json)
            covered_topics += f"- Talking point: {sj.get('talking_point', '')}\n"
            covered_topics += f"- Key driver: {sj.get('key_driver', '')}\n"
        except json.JSONDecodeError:
            pass

    wc_msg = f"Today is {date_str}.\n\n{covered_topics}"
    if recent_summaries:
        wc_topics = []
        for line in recent_summaries.split("\n"):
            if "water_cooler" in line:
                try:
                    j = json.loads(line.split("] ", 1)[1])
                    wc_topics.append(j.get("water_cooler", ""))
                except (json.JSONDecodeError, IndexError):
                    pass
        if wc_topics:
            wc_msg += "\n\nRECENT WATER COOLER STORIES (do NOT repeat these or cover the same subject/entity, even from a different angle):\n"
            for t in wc_topics:
                wc_msg += f"- {t}\n"
            wc_msg += "If a story involves the same company, study, person, or subject as any item above, it counts as a repeat. Find something completely different."

    wc_raw = call_anthropic(MODEL, SYSTEM_PROMPT_WATERCOOLER, wc_msg,
                            max_tokens=512, use_search=True)
    wc_html = parse_html_section(wc_raw)

    # Add water cooler summary to the summary JSON
    wc_summary = parse_watercooler_summary(wc_raw)
    if wc_summary and summary_json:
        try:
            sj = json.loads(summary_json)
            sj["water_cooler"] = wc_summary
            summary_json = json.dumps(sj)
        except json.JSONDecodeError:
            pass

    # ── Combine all sections ──
    analysis = f"{sections_html}\n\n{wc_html}"

    return market_card, greeting_hook, analysis, summary_json


# ── Send Email ─────────────────────────────────────────────────

def send_email(html: str, date_str: str, to: str, smtp_conn=None):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"The Briefly Morning Brief | {date_str}"
    msg["From"] = f"Briefly Wealth <{GMAIL_ADDRESS}>"
    msg["To"] = to
    msg["List-Unsubscribe"] = f"<mailto:{GMAIL_ADDRESS}?subject=Unsubscribe>"
    msg.attach(MIMEText(f"The Briefly Morning Brief - {date_str}\nBest viewed in HTML.", "plain"))
    msg.attach(MIMEText(html, "html"))
    if smtp_conn:
        smtp_conn.sendmail(GMAIL_ADDRESS, [to], msg.as_string())
    else:
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
            s.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            s.sendmail(GMAIL_ADDRESS, [to], msg.as_string())


# ── Main ───────────────────────────────────────────────────────

def main():
    test_mode = "--test" in sys.argv
    today = datetime.now()
    date_str = today.strftime("%B %d, %Y")
    print(f"=== The Briefly Morning Brief | {date_str} ===")
    if test_mode:
        print("*** TEST MODE — sending only to connor.florczyk@brieflywealth.com ***")
    print()

    if not test_mode and is_us_market_holiday(today.date()):
        print("US markets are closed today. No brief to send.")
        return

    if test_mode:
        subs = [{"email": "connor.florczyk@brieflywealth.com", "name": "Connor", "firm": "", "unsubscribe_token": "test"}]
    else:
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

    print("Fetching yesterday's brief for narrative threading...")
    yesterday_brief = get_yesterday_brief()
    if not yesterday_brief:
        print("No previous brief found for threading\n")

    print("Generating brief (2 calls)...")
    market_card, greeting_hook, analysis, summary_json = generate_brief(
        date_str, market_data, futures_text, earnings_text, econ_text,
        recent_summaries, yesterday_brief
    )
    print(f"\nMarket card: {len(market_card)} chars")
    print(f"Greeting hook: {len(greeting_hook)} chars")
    print(f"Analysis: {len(analysis)} chars")
    print(f"Summary: {len(summary_json)} chars\n")

    print("Sending...")
    ok, fail = 0, 0
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as smtp_conn:
        smtp_conn.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        for s in subs:
            try:
                unsub = f"{UNSUBSCRIBE_BASE_URL}?token={s['unsubscribe_token']}"
                html = build_email_html(market_card, analysis, greeting_hook, date_str, s["name"], unsub)
                send_email(html, date_str, s["email"], smtp_conn=smtp_conn)
                label = s["name"] or s["email"]
                print(f"  \u2713 {label} <{s['email']}>")
                ok += 1
                time.sleep(1)
            except Exception as e:
                print(f"  \u2717 {s['email']} \u2014 {e}", file=sys.stderr)
                fail += 1

    if ok > 0:
        print("\nSaving brief to archive...")
        save_brief(date_str, greeting_hook, analysis, summary_json)

    print(f"\nDone. Sent: {ok} | Failed: {fail}")


if __name__ == "__main__":
    main()
