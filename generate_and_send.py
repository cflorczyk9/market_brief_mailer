"""
Daily Market Brief — Generate & Send
Single API call generates JSON data + analysis HTML.
Python builds the scoreboard and email template.
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


# ── Prompt ─────────────────────────────────────────────────────

SYSTEM_PROMPT = """You write daily pre-market briefings for financial advisors. Authoritative, concise, no fluff. Advisors need to sound informed by 8am.

OUTPUT FORMAT — follow this exactly:

Line 1: A JSON object with closing data (one line, no line breaks inside):
{"sp500":"5770.20|-1.71%","nasdaq":"18196.22|-2.61%","dow":"42801.72|-1.30%","yield10y":"4.23%|-7bp","wti":"$66.80|-1.40%","gold":"$2938|+0.60%"}

Then a blank line, then the analysis as HTML sections. 800-1100 words total.

FORMATTING — this is important for readability:
- Wrap ALL percentage moves, index levels, yield levels, dollar amounts, and basis point changes in <b> tags
- Wrap stock ticker names and sector names in <i> tags on first mention
- Example: <i>Nvidia</i> fell <b>3.8%</b> on the session, dragging the SOX index down <b>3.1%</b>
- Example: The <b>10-year yield fell 7bp to 4.23%</b> as traders priced in <b>75bp of cuts</b> by year-end
- Example: <i>WTI crude</i> slipped <b>1.4% to $66.80/bbl</b>
- Every number that an advisor would want to quote to a client should be in <b> tags

Sections:

<div class="section">
<h2>Market Recap</h2>
<p>Big picture. Dominant narrative. What drove the session and why it matters. 2-3 paragraphs.</p>
</div>

<div class="section">
<h2>Equities</h2>
<p>Sectors: leaders and laggards. Specific stocks with % moves. Earnings. Breadth. 2-3 paragraphs.</p>
</div>

<div class="section">
<h2>Fixed Income & Rates</h2>
<p>Yield moves, Fed commentary, what bonds are signaling. 1-2 paragraphs.</p>
</div>

<div class="section">
<h2>Commodities & Currencies</h2>
<p>Oil, gold, DXY with levels and context. 1-2 paragraphs.</p>
</div>

<div class="section">
<h2>What to Watch Today</h2>
<p>Data releases with times ET. Earnings on deck. Fed speakers. 1-2 paragraphs.</p>
</div>

<div class="section">
<h2>Advisor Talking Point</h2>
<p>One grounded insight for a client call. 2-3 sentences.</p>
</div>

CRITICAL: No preamble. No "let me search" text. Start with the JSON line, then the HTML. Real data only."""


# ── Parse Response ─────────────────────────────────────────────

def parse_response(raw: str) -> tuple[dict, str]:
    """Parse the combined response into scoreboard data and analysis HTML."""
    # Find JSON line (compact format: "sp500":"level|change")
    score_data = {}
    json_match = re.search(r'\{["\']sp500["\'].*?\}', raw)
    if json_match:
        try:
            j = json.loads(json_match.group())
            for key in ["sp500", "nasdaq", "dow", "yield10y", "wti", "gold"]:
                if key in j:
                    parts = j[key].split("|")
                    if len(parts) == 2:
                        score_data[key] = {"level": parts[0], "change": parts[1]}
            if len(score_data) == 6:
                print(f"Scoreboard parsed: {json.dumps(score_data)}")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Could not parse scoreboard: {e}", file=sys.stderr)

    # Extract analysis: everything from first <div class="section"> onwards
    first_div = raw.find('<div class="section">')
    if first_div >= 0:
        analysis = raw[first_div:]
        # Remove any non-HTML lines mixed in between sections
        clean = []
        in_html = False
        for line in analysis.split('\n'):
            s = line.strip()
            if s.startswith('<div') or s.startswith('<h2') or s.startswith('<p') or s.startswith('</'):
                in_html = True
            if in_html or s == '' or s.startswith('<') or '<b>' in s or '<i>' in s:
                clean.append(line)
            if s == '</div>':
                in_html = False
        analysis = '\n'.join(clean)
    else:
        analysis = raw

    return score_data, analysis.strip()


# ── Build Scoreboard HTML ──────────────────────────────────────

def build_scoreboard(data: dict) -> str:
    if not data:
        return ""
    def color(v):
        if v.startswith("+"): return "#1a7a3a"
        elif v.startswith("-"): return "#b91c1c"
        return "#8aacc8"
    def cell(label, d, last=False):
        bb = "border-bottom:none;padding-bottom:8px;" if last else "border-bottom:1px solid #eaeff5;"
        return (
            f'<td style="text-align:center;padding:14px 6px;width:33.33%;{bb}">'
            f'<span style="display:block;font-family:Georgia,serif;font-size:10px;letter-spacing:0.8px;text-transform:uppercase;color:#8aacc8;margin-bottom:5px;">{label}</span>'
            f'<span style="display:block;font-family:Georgia,serif;font-size:18px;font-weight:bold;color:#142d4c;margin-bottom:3px;letter-spacing:-0.3px;">{d["level"]}</span>'
            f'<span style="display:block;font-family:Georgia,serif;font-size:12px;color:{color(d["change"])};">{d["change"]}</span>'
            f'</td>'
        )
    return (
        '<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;margin-bottom:28px;"><tr>'
        + cell("S&P 500", data["sp500"]) + cell("Nasdaq", data["nasdaq"]) + cell("Dow Jones", data["dow"])
        + '</tr><tr>'
        + cell("10Y Yield", data["yield10y"], True) + cell("WTI Crude", data["wti"], True) + cell("Gold", data["gold"], True)
        + '</tr></table>'
    )


# ── Email Template ─────────────────────────────────────────────

def build_email_html(scoreboard: str, analysis: str, date_str: str, name: str = "", unsub_url: str = "") -> str:
    greeting = ""
    if name:
        greeting = (f'<p style="font-family:Georgia,\'Times New Roman\',serif;font-size:15px;'
                    f'color:#2c3e50;margin:0 0 24px 0;">Good morning, {name.split()[0]}.</p>')
    unsub = ""
    if unsub_url:
        unsub = f' &bull; <a href="{unsub_url}" style="color:#6b8db5;text-decoration:none;">Unsubscribe</a>'
    try:
        d = datetime.strptime(date_str, "%B %d, %Y")
        newspaper_date = f"{d.strftime('%A')}, {date_str}"
    except Exception:
        newspaper_date = date_str

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#e8eef4;font-family:Georgia,'Times New Roman',serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#e8eef4;padding:32px 16px;">
<tr><td align="center">
<table width="640" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:4px;overflow:hidden;">
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
<tr><td style="padding:24px 36px 36px;">
  <style>
    .scoreboard{{margin:0 0 28px;padding:0 0 4px}}
    .scoreboard table{{border-collapse:collapse;width:100%}}
    .score-cell{{text-align:center;padding:14px 6px;border-bottom:1px solid #eaeff5;width:33.33%}}
    .score-cell.last-row{{border-bottom:none;padding-bottom:8px}}
    .score-label{{display:block;font-family:Georgia,'Times New Roman',serif;font-size:10px;letter-spacing:0.8px;text-transform:uppercase;color:#8aacc8;margin-bottom:5px}}
    .score-value{{display:block;font-family:Georgia,'Times New Roman',serif;font-size:18px;font-weight:bold;color:#142d4c;margin-bottom:3px;letter-spacing:-0.3px}}
    .score-change{{display:block;font-family:Georgia,'Times New Roman',serif;font-size:12px}}
    .score-change.positive{{color:#1a7a3a}}
    .score-change.negative{{color:#b91c1c}}
    .score-change.flat{{color:#8aacc8}}
    .section{{margin:0 0 28px;padding:0 0 24px;border-bottom:1px solid #eaeff5}}
    .section:last-child{{margin-bottom:0;padding-bottom:0;border-bottom:none}}
    .section h2{{font-family:Georgia,'Times New Roman',serif;font-size:13px;font-weight:normal;color:#142d4c;margin:0 0 12px;letter-spacing:2px;text-transform:uppercase}}
    .section p{{font-family:Georgia,'Times New Roman',serif;font-size:15px;line-height:1.75;color:#2c3e50;margin:0 0 12px}}
    .section p:last-child{{margin-bottom:0}}
    .section b{{color:#142d4c}}
  </style>
  {greeting}{scoreboard}{analysis}
</td></tr>

<!-- Subscribe CTA -->
<tr><td style="padding:24px 36px;border-top:1px solid #d4dee8;background:#f4f7fa;text-align:center;">
  <p style="font-family:Georgia,'Times New Roman',serif;font-size:14px;color:#2c3e50;margin:0 0 14px;">Know an advisor who'd find this useful?</p>
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

def generate_brief(recap_day: str, date_str: str) -> tuple[str, str]:
    payload = {
        "model": MODEL,
        "max_tokens": 4096,
        "system": SYSTEM_PROMPT,
        "tools": [{"type": "web_search_20250305", "name": "web_search"}],
        "messages": [{"role": "user", "content": (
            f"Today is {date_str}. Recap {recap_day}'s markets and preview today. "
            f"Search for closing prices, sector performance, earnings, yields, commodities, "
            f"and the economic calendar."
        )}],
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

    # Combine all text blocks
    text_blocks = [b["text"] for b in data.get("content", []) if b.get("type") == "text"]
    if not text_blocks:
        raise ValueError("API returned no text content")

    for i, block in enumerate(text_blocks):
        print(f"  Block {i}: {len(block)} chars")

    raw = "\n".join(text_blocks)
    score_data, analysis = parse_response(raw)
    scoreboard = build_scoreboard(score_data)
    return scoreboard, analysis


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
    print(f"Recapping {recap_day}'s session\n")

    print("Generating brief...")
    scoreboard, analysis = generate_brief(recap_day, date_str)
    print(f"Scoreboard: {len(scoreboard)} chars")
    print(f"Analysis: {len(analysis)} chars\n")

    print("Sending...")
    ok, fail = 0, 0
    for s in subs:
        try:
            unsub = f"{UNSUBSCRIBE_BASE_URL}?token={s['unsubscribe_token']}"
            html = build_email_html(scoreboard, analysis, date_str, s["name"], unsub)
            send_email(html, date_str, s["email"])
            label = s["name"] or s["email"]
            print(f"  ✓ {label} <{s['email']}>")
            ok += 1
            time.sleep(1)
        except Exception as e:
            print(f"  ✗ {s['email']} — {e}", file=sys.stderr)
            fail += 1

    print(f"\nDone. Sent: {ok} | Failed: {fail}")


if __name__ == "__main__":
    main()
