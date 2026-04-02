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


# ── Config ─────────────────────────────────────────────────────

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
GMAIL_ADDRESS = os.environ["GMAIL_ADDRESS"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
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
    """Save today's brief and summary to Supabase."""
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
    url = f"{SUPABASE_URL}/rest/v1/briefs"
    req = urllib.request.Request(url, data=payload, method="POST", headers={
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    })
    try:
        with urllib.request.urlopen(req) as resp:
            print(f"Brief saved for {iso_date}")
    except Exception as e:
        print(f"Warning: Could not save brief: {e}", file=sys.stderr)


# ── Prompt ─────────────────────────────────────────────────────

SYSTEM_PROMPT = """You write daily pre-market morning briefings for financial advisors. MORNING BRIEFING, not a recap. Prepare them for the day ahead.

Tone: sharp colleague in the hallway before the first call. Professional, clear, occasionally wry.

BREVITY IS CRITICAL. This email should take 60 seconds to read. Every sentence must earn its place. If you're writing more than the word counts below, you're writing too much.

Use MINIMAL web searches — 2-3 searches max. Combine queries (e.g. "S&P 500 Nasdaq Dow closing prices YTD MTD March 12 2026"). Do not search for every data point individually.

OUTPUT FORMAT — in this EXACT order:

LINE 1 — MARKET DATA JSON (one line):
{"sp500":"6775.80|+2.8%|-1.2%","nasdaq":"22716.14|+1.4%|-2.1%","dow":"47417.27|+3.1%|-0.8%","yield10y":"4.20%|+42bp|+9bp","wti":"$87.25|+31.2%|+14.8%","gold":"$2948|+11.4%|+3.2%"}
Format: "level|ytd|mtd" for each key.

LINE 2 — GREETING HOOK: One sentence, what matters THIS MORNING. <p class="greeting-hook"> tags.

LINE 3 — BOTTOM LINE: 2-3 sentences MAXIMUM. Pre-market direction, the "so what," one or two things to watch today. <p class="bottom-line"> tags. <b> tags on numbers.

LINE 4 — SUMMARY JSON (one line):
{"headline":"~10 words","talking_point":"angle + WHY in ~15 words","client_script_topic":"topic + framing ~10 words","water_cooler":"story + subject ~10 words","key_driver":"underlying reason ~10 words"}

Then a blank line, then EXACTLY these HTML sections:

<div class="section section-advisor">
<h2>Advisor Talking Point</h2>
<h3>[Plain-language framing of the key dynamic]</h3>
<p>2 short paragraphs. HARD LIMIT: 150-200 words total. No more. Get to the point fast.</p>
<div class="client-script">
<p class="client-script-label">If a client asks about [topic]</p>
<p>2 sentences max. Natural, confident, jargon-light.</p>
</div>
</div>

<div class="section">
<h2>What to Watch</h2>
<table class="watch-calendar">
<tr class="watch-group"><td colspan="2">Today</td></tr>
<tr><td class="watch-time">8:30 AM</td><td class="watch-desc">Data release</td></tr>
<tr class="watch-group"><td colspan="2">Tomorrow</td></tr>
<tr><td class="watch-time">Earnings</td><td class="watch-desc">Company (TICK)</td></tr>
<tr class="watch-group"><td colspan="2">Next Week</td></tr>
<tr><td class="watch-time">Wed</td><td class="watch-desc">FOMC decision</td></tr>
</table>
IMPORTANT: The calendar table is the ENTIRE section. Do NOT write any prose paragraphs after the table. Just the table, nothing else.
</div>

<div class="section section-watercooler">
<h2>Water Cooler</h2>
<h3>[Catchy headline]</h3>
<p>HARD LIMIT: 50-75 words. One short paragraph. US-focused story an advisor would mention at dinner. End with one italic sentence connecting it to advising.</p>
</div>

Total across ALL sections: 300-400 words. No more. Start with market data JSON. No preamble."""


# ── Parse Response ─────────────────────────────────────────────

def parse_response(raw: str) -> tuple[dict, str, str, str, str]:
    """Parse into: market_data, greeting_hook, bottom_line, summary_json, analysis HTML."""

    # 1. Market data JSON
    market_data = {}
    md_match = re.search(r'\{["\']sp500["\'].*"gold"[^}]*\}', raw, re.DOTALL)
    if not md_match:
        md_match = re.search(r'\{["\']sp500["\'].*?\}', raw)
    if md_match:
        try:
            raw_json = md_match.group().replace('\n', '')
            j = json.loads(raw_json)
            for key in ["sp500", "nasdaq", "dow", "yield10y", "wti", "gold"]:
                if key in j:
                    parts = j[key].split("|")
                    if len(parts) == 3:
                        market_data[key] = {"level": parts[0], "ytd": parts[1], "mtd": parts[2]}
                    elif len(parts) == 2:
                        market_data[key] = {"level": parts[0], "ytd": parts[1], "mtd": "—"}
            print(f"Market data parsed: {len(market_data)}/6 keys")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Could not parse market data: {e}", file=sys.stderr)

    # 2. Greeting hook
    greeting_hook = ""
    hook_match = re.search(r'<p class="greeting-hook">(.*?)</p>', raw, re.DOTALL)
    if hook_match:
        greeting_hook = hook_match.group(1).strip()
        print(f"Greeting hook: {greeting_hook[:80]}...")

    # 3. Bottom line
    bottom_line = ""
    bl_match = re.search(r'<p class="bottom-line">(.*?)</p>', raw, re.DOTALL)
    if bl_match:
        bottom_line = bl_match.group(1).strip()
        print(f"Bottom line: {bottom_line[:80]}...")

    # 4. Summary JSON
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

    # 5. Analysis HTML (sections)
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

    return market_data, greeting_hook, bottom_line, summary_json, analysis.strip()


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
{row("Gold", "gold", last=True)}
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
    # Paragraphs — bare <p> tags only
    html = re.sub(r'<p>(?!<)', f'<p style="{S_P}">', html)
    html = re.sub(r'<p>\n', f'<p style="{S_P}">\n', html)
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

def generate_brief(recap_day: str, date_str: str,
                   recent_summaries: str | None = None) -> tuple[str, str, str, str]:
    """Returns: (market_card_html, greeting_hook, analysis_html, summary_json)"""
    user_msg = (
        f"Today is {date_str}. Write this morning's briefing. "
        f"Do 2-3 searches MAX. Suggested queries: "
        f"(1) '{recap_day} stock market closing prices S&P Nasdaq Dow YTD MTD {date_str}' "
        f"(2) 'US pre-market futures oil gold 10-year yield today {date_str}' "
        f"(3) 'economic calendar earnings today {date_str} US business news'. "
        f"Extract all data you need from those results. Do not search for each number individually."
    )
    if recent_summaries:
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
    market_data, greeting_hook, bottom_line, summary_json, analysis = parse_response(raw)
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

    lookback = today - timedelta(days=1)
    while is_us_market_holiday(lookback.date()):
        lookback -= timedelta(days=1)
    recap_day = lookback.strftime("%A")
    print(f"Previous session: {recap_day}\n")

    print("Fetching recent summaries...")
    recent_summaries = get_recent_summaries(10)
    if not recent_summaries:
        print("No previous summaries found (first run or empty table)\n")

    print("Generating brief...")
    market_card, greeting_hook, analysis, summary_json = generate_brief(
        recap_day, date_str, recent_summaries
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
