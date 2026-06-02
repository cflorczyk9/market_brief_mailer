# Market Brief Mailer — Claude Code Instructions

## Overview

Automated daily pre-market morning briefing for financial advisors. Generates AI-powered market analysis and emails it to subscribers at 6:00 AM ET on weekdays. Trigger fires at 5:30 AM ET (external cron-job.org -> GH workflow_dispatch); script generates the brief, then sleeps until 6:00 AM ET sharp before sending so delivery time is consistent.

## Stack

- **Language**: Python 3.12 (single file: `generate_and_send.py`)
- **AI**: Anthropic API — Haiku for main brief, web search tool for Water Cooler section
- **Data Sources**: Yahoo Finance (yfinance), FRED API (economic calendar), earnings calendar (77 large-cap stocks)
- **Email**: Gmail SMTP (SSL)
- **Database**: Supabase PostgreSQL (subscribers + briefs tables)
- **CI/CD**: GitHub Actions; primary trigger is external (cron-job.org POSTs workflow_dispatch at 5:30 AM ET weekdays). GH `schedule:` is backup only (it drifts 5+ hours and silently drops runs on low-activity repos).

## Architecture

### Execution Flow
1. Check if US market is open (holiday detection)
2. Fetch market data (7 indices parallel), pre-market futures, earnings, econ calendar
3. Fetch last 10 briefs for anti-repetition context
4. Call Claude Haiku → greeting, bottom line, advisor talking point, summary JSON
5. Call Claude with web search → Water Cooler section (unrelated story)
6. Build HTML email with market data card + analysis
7. Send to active subscribers via Gmail SMTP
8. Save brief to Supabase

### Multi-Model Strategy
- **Haiku**: Main briefing generation (fast, cheap, 2000 token limit)
- **Web Search Tool**: Overnight news and Water Cooler content

### Anti-Repetition System
- Stores 10-day summary ledger in Supabase `briefs` table
- Passes recent summaries as context to prevent repeating themes
- Greeting/bottom line cover main story; Talking Point must cover a DIFFERENT theme

## Critical Rules

- **Never make unbounded API calls** — the script has defined token limits and section caps
- **Test mode**: `python generate_and_send.py --test` sends only to Connor's email
- **No requirements.txt** — uses minimal stdlib deps + yfinance + anthropic SDK
- **Prompt tone**: Plain language, no jargon, finance-first-year accessible, politically neutral

## Environment Variables

```
ANTHROPIC_API_KEY, GMAIL_ADDRESS, GMAIL_APP_PASSWORD,
SUPABASE_URL, SUPABASE_SERVICE_KEY, FRED_API_KEY (optional)
```

## Database Schema (setup.sql)

- `subscribers` — email, name, firm, status, unsubscribe_token
- `briefs` — brief_date, greeting_hook, analysis HTML, summary JSON
- RLS enabled, public subscribe/unsubscribe functions
