#!/usr/bin/env python3
"""Watchdog: verify today's brief was sent. Alert + last-ditch retry if not.

Runs after the primary 6:00 AM ET send window has closed (Tier 2 GH backups end
at 7:30 ET, this fires at 8:00 ET). Queries Supabase `briefs` for today's
brief_date row. If missing: emails Connor and POSTs workflow_dispatch to the
daily-brief workflow as a last-ditch attempt (the main script's noon-ET bail
still gates whether that send actually goes out).

Reuses existing repo secrets: GMAIL_ADDRESS, GMAIL_APP_PASSWORD, SUPABASE_URL,
SUPABASE_SERVICE_KEY, GITHUB_TOKEN (auto-injected by Actions; needs
`permissions: actions: write` in the workflow file). No new secrets required.

Exit codes: 0 = brief present, 1 = brief missing (alert sent), 2 = Supabase
unreachable (alert sent), 3 = alert send also failed (silent — investigate).
"""
import json
import os
import smtplib
import ssl
import sys
import urllib.error
import urllib.request
from datetime import datetime
from email.message import EmailMessage
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
ALERT_TO = "connor.florczyk@brieflywealth.com"
REPO = "cflorczyk9/market_brief_mailer"
WORKFLOW_FILE = "daily-brief.yml"

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
GMAIL_ADDRESS = os.environ["GMAIL_ADDRESS"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()


def today_iso() -> str:
    return datetime.now(ET).date().isoformat()


def brief_exists(iso_date: str) -> bool:
    url = (
        f"{SUPABASE_URL}/rest/v1/briefs"
        f"?select=brief_date&brief_date=eq.{iso_date}&limit=1"
    )
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        rows = json.loads(resp.read())
    return bool(rows)


def attempt_dispatch() -> str:
    if not GITHUB_TOKEN:
        return "skipped (GITHUB_TOKEN not present)"
    url = f"https://api.github.com/repos/{REPO}/actions/workflows/{WORKFLOW_FILE}/dispatches"
    body = json.dumps({"ref": "main", "inputs": {"test_mode": "false"}}).encode()
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return f"dispatched (HTTP {resp.getcode()})"
    except urllib.error.HTTPError as e:
        snippet = e.read().decode("utf-8", errors="replace")[:200]
        return f"FAILED (HTTP {e.code}: {snippet})"
    except Exception as e:
        return f"FAILED ({e})"


def send_email(subject: str, body: str) -> None:
    msg = EmailMessage()
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = ALERT_TO
    msg["Subject"] = subject
    msg.set_content(body)
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
        s.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        s.send_message(msg)


def main() -> int:
    iso = today_iso()
    now_str = datetime.now(ET).strftime("%Y-%m-%d %H:%M %Z")
    print(f"Watchdog checking briefs for {iso} (now {now_str})")

    try:
        if brief_exists(iso):
            print(f"OK — brief row exists for {iso}")
            return 0
    except Exception as e:
        print(f"Supabase check failed: {e}", file=sys.stderr)
        try:
            send_email(
                f"ALERT: watchdog could not reach Supabase ({iso})",
                f"Watchdog at {now_str} could not verify today's send.\n"
                f"Supabase error: {e}\n\n"
                f"Manual rerun:\n"
                f"  https://github.com/{REPO}/actions/workflows/{WORKFLOW_FILE}\n",
            )
        except Exception as e2:
            print(f"Alert email also failed: {e2}", file=sys.stderr)
            return 3
        return 2

    print(f"MISSING — no row for {iso}. Dispatching + alerting.")
    dispatch_result = attempt_dispatch()
    print(f"Dispatch result: {dispatch_result}")
    try:
        send_email(
            f"ALERT: market brief did NOT send today ({iso})",
            f"Watchdog ran at {now_str}.\n"
            f"No Supabase `briefs` row for brief_date={iso}.\n\n"
            f"Most likely cause: cron-job.org primary 5:30 ET dispatch failed "
            f"(check execution history + PAT validity) AND GH schedule backups "
            f"did not back-fill in time.\n\n"
            f"Last-ditch dispatch: {dispatch_result}\n"
            f"  (the daily-brief script bails after noon ET, so a late "
            f"dispatch may still no-op)\n\n"
            f"Manual rerun:\n"
            f"  https://github.com/{REPO}/actions/workflows/{WORKFLOW_FILE}\n"
            f"  -> Run workflow -> test_mode=false\n",
        )
    except Exception as e:
        print(f"Alert email failed: {e}", file=sys.stderr)
        return 3
    return 1


if __name__ == "__main__":
    sys.exit(main())
