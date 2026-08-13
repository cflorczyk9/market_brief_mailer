"""
Send one test brief per candidate model to Connor only (2026-08 bake-off:
current Gemini 2.5 Flash vs Gemini 3.6 Flash, Gemini 3 Flash, DeepSeek V4
Flash, Qwen 3.7 Plus, Kimi K2.6).

Reuses the REAL prod pipeline (generate_brief -> build_email_html -> SMTP) so the
emails are byte-for-byte what production would send, EXCEPT the model call is
monkeypatched to route through OpenRouter (with the web-search plugin) so the
non-Google models work. Market data / calendars are fetched LIVE when yfinance
is importable (the GitHub Actions path); otherwise the hardcoded June values
below keep local preview runs working with no dependencies.

SAFETY: recipient is hardcoded to a single test address. Subscribers are NEVER
fetched. There is no path here that emails anyone but the test address.

Always writes preview_<model>.html. Sends via Gmail SMTP only if GMAIL_ADDRESS +
GMAIL_APP_PASSWORD are in the environment.

Usage:
  OPENROUTER_API_KEY=sk-or-... \
  [GMAIL_ADDRESS=... GMAIL_APP_PASSWORD=...] \
  python3 send_test_emails.py
"""

import os
import sys
import ssl
import types
import json
import time
import smtplib
import urllib.request
import urllib.error
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

sys.modules.setdefault("yfinance", types.ModuleType("yfinance"))
for _k in ("ANTHROPIC_API_KEY", "GMAIL_ADDRESS", "GMAIL_APP_PASSWORD",
           "SUPABASE_URL", "SUPABASE_SERVICE_KEY"):
    os.environ.setdefault(_k, "stub")

import generate_and_send as gs  # noqa: E402

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
if not OPENROUTER_API_KEY:
    sys.exit("Set OPENROUTER_API_KEY first.")

TEST_RECIPIENT = "connor.florczyk@brieflywealth.com"  # the ONLY address used
DATE_STR = "June 06, 2026"

MODELS = [
    # (label for the subject line, OpenRouter slug)
    ("Gemini 2.5 Flash - current", "google/gemini-2.5-flash"),
    ("Gemini 3.6 Flash", "google/gemini-3.6-flash"),
    ("Gemini 3 Flash", "google/gemini-3-flash-preview"),
    ("DeepSeek V4 Flash", "deepseek/deepseek-v4-flash"),
    ("Qwen 3.7 Plus", "qwen/qwen3.7-plus"),
    ("Kimi K2.6", "moonshotai/kimi-k2.6"),
]

# Real market values (June 5 2026 close, from the live brief).
MARKET_DATA = {
    "sp500":    {"level": "7,584.31",  "ytd": "+10.8%", "mtd": "+0.1%"},
    "nasdaq":   {"level": "26,830.96", "ytd": "+15.4%", "mtd": "-0.5%"},
    "dow":      {"level": "51,561.93", "ytd": "+7.3%",  "mtd": "+1.0%"},
    "yield10y": {"level": "4.48%",     "ytd": "+31bp",  "mtd": "+2bp"},
    "wti":      {"level": "$92.79",    "ytd": "+61.6%", "mtd": "+6.2%"},
    "gold":     {"level": "$4,494.20", "ytd": "+3.9%",  "mtd": "-1.5%"},
    "btc":      {"level": "$62,142",   "ytd": "-29.0%", "mtd": "-15.5%"},
}
FUTURES_TEXT = "PRE-MARKET FUTURES: S&P 500 futures: -0.60% | Nasdaq futures: -0.90% | Dow futures: -0.30%"
EARNINGS_TEXT = ""
ECON_TEXT = (
    "ECONOMIC CALENDAR (next 7 days):\n"
    "Friday, Jun 06 (today)\n  8:30 AM  Employment Situation (NFP)\n  3:00 PM  Consumer Credit\n"
    "Wednesday, Jun 10\n  8:30 AM  CPI\n"
    "Thursday, Jun 11\n  8:30 AM  PPI\n  8:30 AM  Unemployment Claims"
)
RECENT_SUMMARIES = ""
YESTERDAY_BRIEF = {
    "brief_date": "2026-06-05",
    "greeting_hook": "Broadcom's weak guidance knocked momentum out of the AI trade that has driven markets higher this year.",
    "summary": json.dumps({
        "headline": "Broadcom guidance miss rattles AI trade ahead of jobs data",
        "primary_theme": "tech",
        "key_driver": "doubts about durability of AI capex spending",
    }),
}

# ── Live data (GitHub Actions path) ─────────────────────────────
# When yfinance is real (the workflow pip-installs it), replace every
# hardcoded input above with the same live fetches production uses, so each
# candidate model writes about TODAY. Local runs without deps keep the June
# fixtures. Any individual fetch failure falls back to its fixture value.
def _load_real_yfinance() -> bool:
    """The stub inserted before the gs import masks a real yfinance install.
    Swap the real package in (and rebind gs.yf) when one exists."""
    if getattr(sys.modules.get("yfinance"), "__file__", None):
        return True  # already real
    try:
        del sys.modules["yfinance"]
        import yfinance as _yf_real
        gs.yf = _yf_real
        return True
    except ImportError:
        sys.modules["yfinance"] = types.ModuleType("yfinance")
        return False


LIVE_DATA = False
if _load_real_yfinance():
    try:
        _md = gs.fetch_market_data()
        if len(_md) >= gs.MIN_TICKERS_FOR_TABLE:
            MARKET_DATA = _md
            LIVE_DATA = True
            from datetime import datetime
            from zoneinfo import ZoneInfo
            DATE_STR = datetime.now(ZoneInfo("America/New_York")).strftime("%B %d, %Y")
    except Exception as e:
        print(f"live market data failed, using fixtures: {e}", file=sys.stderr)
    if LIVE_DATA:
        for _name, _fn in [("futures", "fetch_futures"),
                           ("earnings", "fetch_earnings_calendar"),
                           ("econ", "fetch_fred_calendar")]:
            try:
                _val = getattr(gs, _fn)()
                if _name == "futures": FUTURES_TEXT = _val
                elif _name == "earnings": EARNINGS_TEXT = _val
                else: ECON_TEXT = _val
            except Exception as e:
                print(f"live {_name} fetch failed, using fixture: {e}", file=sys.stderr)
        if os.environ.get("SUPABASE_URL", "stub") != "stub":
            try:
                RECENT_SUMMARIES = gs.get_recent_summaries() or ""
                YESTERDAY_BRIEF = gs.get_yesterday_brief() or YESTERDAY_BRIEF
            except Exception as e:
                print(f"Supabase context fetch failed, using fixtures: {e}", file=sys.stderr)
print(f"inputs: {'LIVE ' + DATE_STR if LIVE_DATA else 'June fixtures'}")

# ── Monkeypatch the model call to route through OpenRouter ──
_CURRENT_SLUG = {"slug": None}


def _openrouter_call(model, system, user_msg, max_tokens=1024, use_search=False):
    # Reasoning-mode models (Gemini 3.x, Kimi, DeepSeek) spend hidden thinking
    # tokens INSIDE max_tokens; at the prod cap of 2000 the visible answer came
    # back truncated or empty. Give test calls headroom and pin effort low so
    # the comparison measures the answer, not the thinking budget. If a
    # candidate wins, prod needs the same two settings.
    payload = {
        "model": _CURRENT_SLUG["slug"],
        "temperature": 0.7,
        "max_tokens": max(max_tokens, 8000),
        "reasoning": {"effort": "low"},
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ],
    }
    if use_search:
        payload["plugins"] = [{"id": "web", "max_results": 3}]
    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://brieflywealth.com",
            "X-Title": "market_brief_mailer test send",
        },
    )
    with urllib.request.urlopen(req, timeout=240) as resp:
        data = json.loads(resp.read())
    return data["choices"][0]["message"].get("content") or ""


gs.call_anthropic = _openrouter_call


def build_one(slug):
    _CURRENT_SLUG["slug"] = slug
    market_card, greeting_hook, analysis, summary_json = gs.generate_brief(
        DATE_STR, MARKET_DATA, FUTURES_TEXT, EARNINGS_TEXT, ECON_TEXT,
        RECENT_SUMMARIES, YESTERDAY_BRIEF,
    )
    unsub = "https://brieflywealth.com/unsubscribe.html?token=test"
    html = gs.build_email_html(market_card, analysis, greeting_hook, DATE_STR, "Connor", unsub)
    return html


def main():
    creds = os.environ.get("GMAIL_ADDRESS", "stub"), os.environ.get("GMAIL_APP_PASSWORD", "stub")
    can_send = creds[0] != "stub" and creds[1] != "stub"
    built = []
    for label, slug in MODELS:
        print(f"\n=== Building: {label} ({slug}) ===")
        try:
            html = build_one(slug)
        except (Exception, SystemExit) as e:
            # generate_brief sys.exit()s on an unusable brief; in the bake-off
            # that is one candidate's failure, not a reason to stop the rest.
            print(f"  FAILED: {e}", file=sys.stderr)
            continue
        path = f"preview_{slug.split('/', 1)[1].replace('/', '-')}.html"
        with open(path, "w") as f:
            f.write(html)
        print(f"  wrote {path} ({len(html)} chars)")
        built.append((label, html))

    if not can_send:
        print("\nNo GMAIL_ADDRESS / GMAIL_APP_PASSWORD in env -> generated HTML only, did not send.")
        print("Previews written. To send to yourself, rerun with those two env vars set.")
        return

    print(f"\nSending {len(built)} test emails to {TEST_RECIPIENT} ONLY...")
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as smtp:
        smtp.login(creds[0], creds[1])
        for label, html in built:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[TEST: {label}] The Briefly Morning Brief | {DATE_STR}"
            msg["From"] = creds[0]
            msg["To"] = TEST_RECIPIENT
            msg.attach(MIMEText("Best viewed in HTML.", "plain"))
            msg.attach(MIMEText(html, "html"))
            smtp.sendmail(creds[0], [TEST_RECIPIENT], msg.as_string())
            print(f"  sent: {label}")
            time.sleep(1)
    print("Done.")


if __name__ == "__main__":
    main()
