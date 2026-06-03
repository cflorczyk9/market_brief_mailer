# market-brief-trigger (Cloudflare Worker)

Third independent trigger for `market_brief_mailer`. Lives on Cloudflare so a coordinated outage with cron-job.org + GitHub Actions schedule would require three vendors down at once.

## What it does

| Cron (UTC)         | Action                                          |
|--------------------|-------------------------------------------------|
| `30 9 * * 1-5`     | POST `daily-brief.yml` dispatch (5:30 EDT)      |
| `30 10 * * 1-5`    | POST `daily-brief.yml` dispatch (5:30 EST)      |
| `0 12 * * 1-5`     | POST `watchdog.yml` dispatch (8:00 EDT / 7 EST) |

Brief script dedups on Supabase `briefs.brief_date`, so a second daily dispatch is a safe no-op.

## One-time deploy (run once on your laptop)

Prerequisites: a free [Cloudflare account](https://dash.cloudflare.com/sign-up) (no credit card required for Workers free tier).

```bash
cd cloudflare-worker

# 1. install wrangler (one-time, ~30s)
npm install

# 2. log into Cloudflare (opens browser)
npx wrangler login

# 3. generate a fine-scoped GitHub PAT and store it as a Worker secret
#    PAT settings: github.com/settings/personal-access-tokens
#      Resource owner: cflorczyk9
#      Repository access: Only select repositories → cflorczyk9/market_brief_mailer
#      Permissions: Actions → Read and write
#      Expiry: 1 year (set a calendar reminder)
npx wrangler secret put GH_PAT
# (paste the PAT when prompted, hit enter)

# 4. deploy (creates the Worker + schedules the crons)
npx wrangler deploy
```

That's it. Wrangler prints the deployed URL (something like `https://market-brief-trigger.<your-subdomain>.workers.dev`).

## Verify

```bash
# Confirm the worker is alive and the secret is set
curl https://market-brief-trigger.<your-subdomain>.workers.dev/health
# Expected: {"ok":true,"pat_configured":true,"repo":"cflorczyk9/market_brief_mailer"}

# Manually fire the daily brief (real send — only do this if today's not sent)
curl https://market-brief-trigger.<your-subdomain>.workers.dev/dispatch-brief
# Expected: dispatched daily-brief.yml (HTTP 204)
```

In Cloudflare dashboard: **Workers & Pages → market-brief-trigger → Logs → Live**. Cron firings appear here every weekday.

## When to rotate the PAT

GitHub fine-scoped PATs default to a max 1-year expiry. Set a calendar reminder. If the PAT expires, the Worker will return HTTP 401s from `/dispatch-*` and silently fail crons — but the existing watchdog email will still fire (assuming GitHub Actions schedule or cron-job.org backup picks up the daily brief).

## Cost

Cloudflare Workers free tier:
- 100,000 requests/day
- This worker fires 3 crons × ~22 weekdays = ~66 requests/month

Free indefinitely.
