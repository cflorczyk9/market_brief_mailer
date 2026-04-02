"""
Daily Market Brief — Generate & Send
Reads subscribers from Supabase, generates a brief via Claude + web search,
sends personalized emails through Gmail SMTP.
"""

import os
import sys
import json
import smtplib
import ssl
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
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

MODEL = "claude-haiku-4-5-20251001"


# ── Supabase ───────────────────────────────────────────────────

def get_subscribers() -> list[dict]:
    url = (
        f"{SUPABASE_URL}/rest/v1/subscribers"
        f"?status=eq.active&select=email,name,firm,unsubscribe_token"
    )
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    })
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


# ── Prompt ─────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a senior market strategist writing a concise daily pre-market briefing for financial advisors. Your tone is authoritative but accessible, like a well-respected colleague sending a morning note. No fluff, no filler. Advisors are busy and need to sound informed by 8am.

FORMATTING RULES:
- Output clean HTML only. No markdown.
- Be specific with numbers, percentages, and levels.
- Keep the total brief to 600-900 words.
- Natural, professional voice. No cliches like "buckle up" or "all eyes on."
- Each section: 2-4 short paragraphs max.

Structure your output EXACTLY as follows:

<div class="section">
<h2>Market Snapshot</h2>
<p>Overall tone. S&P 500, Nasdaq, Dow closing levels and percentage moves.</p>
</div>

<div class="section">
<h2>Equities</h2>
<p>What drove equity markets? Sector leadership, notable movers, earnings. Specific names and numbers.</p>
</div>

<div class="section">
<h2>Fixed Income & Rates</h2>
<p>10-year and 2-year Treasury yields. Credit spreads. Fed commentary or rate expectations.</p>
</div>

<div class="section">
<h2>Commodities & Currencies</h2>
<p>Oil (WTI/Brent), gold, dollar index. Brief unless something notable happened.</p>
</div>

<div class="section">
<h2>What to Watch Today</h2>
<p>Economic data releases with times. Earnings on deck. Fed speakers. Geopolitical items.</p>
</div>

<div class="section">
<h2>Advisor Talking Point</h2>
<p>One insight an advisor could use in a client conversation today. 2-3 sentences max.</p>
</div>

Real numbers, real names, real levels. Use web search results carefully."""


# ── Email Template ─────────────────────────────────────────────

def build_email_html(brief: str, date_str: str, name: str = "", unsub_url: str = "") -> str:
    greeting = ""
    if name:
        greeting = (
            f'<p style="font-family:Georgia,serif;font-size:15px;'
            f'color:#2c2c2c;margin:0 0 20px 0;">Good morning, {name.split()[0]}.</p>'
        )
    unsub = ""
    if unsub_url:
        unsub = f' &bull; <a href="{unsub_url}" style="color:#8a7d6b;">Unsubscribe</a>'

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f7f4ef;font-family:Georgia,serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f7f4ef;padding:32px 16px;">
<tr><td align="center">
<table width="640" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:8px;border:1px solid #e8e2d8;">
<tr><td style="padding:32px 36px 20px;border-bottom:2px solid #28231c;">
  <h1 style="font-family:Georgia,serif;font-size:26px;font-weight:normal;color:#28231c;margin:0 0 4px;">The Daily Market Brief</h1>
  <p style="font-family:Arial,sans-serif;font-size:13px;color:#8a7d6b;margin:0;">{date_str} &bull; Pre-Market Edition</p>
</td></tr>
<tr><td style="padding:28px 36px 32px;">
  <style>
    .section{{margin-bottom:24px}}.section:last-child{{margin-bottom:0}}
    .section h2{{font-family:Arial,sans-serif;font-size:11px;font-weight:bold;letter-spacing:1.5px;
      text-transform:uppercase;color:#8a7d6b;margin:0 0 10px;padding-bottom:6px;border-bottom:1px solid #e8e2d8}}
    .section p{{font-family:Georgia,serif;font-size:15px;line-height:1.65;color:#2c2c2c;margin:0 0 10px}}
    .section p:last-child{{margin-bottom:0}}
  </style>
  {greeting}{brief}
</td></tr>
<tr><td style="padding:16px 36px 24px;border-top:1px solid #e8e2d8;">
  <p style="font-family:Arial,sans-serif;font-size:11px;color:#b5ab9e;line-height:1.5;margin:0;">
    AI-generated using live market data. Always verify independently. Not investment advice.<br><br>
    Sent by Briefly Wealth{unsub}
  </p>
</td></tr>
</table></td></tr></table></body></html>"""


# ── Generate Brief ─────────────────────────────────────────────

def generate_brief() -> str:
    today = datetime.now()
    dow = today.weekday()
    recap_day = "Friday" if dow in (0, 5, 6) else (today - timedelta(days=1)).strftime("%A")
    date_str = today.strftime("%B %d, %Y")

    payload = {
        "model": MODEL,
        "max_tokens": 4096,
        "system": SYSTEM_PROMPT,
        "tools": [{"type": "web_search_20250305", "name": "web_search"}],
        "messages": [{"role": "user", "content": (
            f"Generate today's pre-market briefing. Today is {date_str}. "
            f"Recap {recap_day}'s market action and preview what's ahead today. "
            f"Search for the latest market data, closing prices, futures, economic calendar, "
            f"and overnight developments. Be thorough to get accurate numbers."
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
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"API error {e.code}: {e.read().decode('utf-8', errors='replace')}", file=sys.stderr)
        raise

    text = "\n".join(b["text"] for b in data.get("content", []) if b.get("type") == "text").strip()
    if not text:
        raise ValueError("API returned no text content")
    return text


# ── Send Email ─────────────────────────────────────────────────

def send_email(html: str, date_str: str, to: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Daily Market Brief | {date_str}"
    msg["From"] = f"Briefly Wealth <{GMAIL_ADDRESS}>"
    msg["To"] = to
    msg["List-Unsubscribe"] = f"<mailto:{GMAIL_ADDRESS}?subject=Unsubscribe>"
    msg.attach(MIMEText(f"Daily Market Brief - {date_str}\nBest viewed in HTML.", "plain"))
    msg.attach(MIMEText(html, "html"))

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
        s.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        s.sendmail(GMAIL_ADDRESS, [to], msg.as_string())


# ── Main ───────────────────────────────────────────────────────

def main():
    date_str = datetime.now().strftime("%B %d, %Y")
    print(f"=== Daily Market Brief | {date_str} ===\n")

    print("Fetching subscribers...")
    subs = get_subscribers()
    if not subs:
        print("No active subscribers. Exiting.")
        return
    print(f"{len(subs)} active subscriber(s)\n")

    print("Generating brief...")
    brief = generate_brief()
    print(f"Done ({len(brief)} chars)\n")

    print("Sending...")
    ok, fail = 0, 0
    for s in subs:
        try:
            unsub = f"{UNSUBSCRIBE_BASE_URL}?token={s['unsubscribe_token']}"
            html = build_email_html(brief, date_str, s["name"], unsub)
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
