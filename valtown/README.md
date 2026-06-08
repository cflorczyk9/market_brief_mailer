# Val Town trigger — second independent punctual leg

**STATUS: code only. NOT live until deployed to Val Town (see steps below).**
Until the GitHub Actions run history shows a `workflow_dispatch` run landing
around 10:20 / 11:20 UTC on weekdays, this leg is NOT actually firing — do not
count it as redundancy before you've verified one real run.

## Why this exists

`market-brief-trigger.ts` is the **second** punctual trigger for the 6:30 AM ET
brief. The first is a cron-job.org job. Both fire `workflow_dispatch` shortly
before 6:30 ET; the brief script holds until 6:30 and the Supabase dedup makes
the second firing a harmless no-op. They run on independent infrastructure
(Val Town vs cron-job.org) so one vendor's outage can't miss the send.

This replaces the deleted Cloudflare Worker, which was never deployed because it
needed a Cloudflare account. Val Town's free tier needs no card.

## Deploy

1. Sign up free at https://val.town
2. New val → paste `market-brief-trigger.ts`
3. **Environment Variables** → `GH_PAT` = a GitHub fine-grained PAT scoped to
   only `cflorczyk9/market_brief_mailer`, permission **Actions: Read and write**.
   Use a **different** token than cron-job.org so the two legs fail
   independently.
4. **Triggers → Cron** → `20 10,11 * * 1-5`
5. **Run** once to smoke-test. Expect `dispatched daily-brief.yml (HTTP 204)`
   and a new run in GitHub Actions.

## Monitoring

The val throws on any non-2xx from GitHub. Val Town emails you when a scheduled
val errors, so an expired/revoked `GH_PAT` alerts you the same morning instead
of failing silently.

## The full delivery stack

| Layer | Fires | Role |
|-------|-------|------|
| cron-job.org | ~6:20 ET | primary punctual trigger (own PAT + failure email) |
| **Val Town (this)** | ~6:20 ET | independent punctual trigger (own PAT + error email) |
| GitHub staggered crons | 09:37–13:37 UTC | floor: eventual delivery if both punctual legs die |
| watchdog.yml | ~10 ET | catches a total miss, emails + re-dispatches |
