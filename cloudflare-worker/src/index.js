// Cloudflare Worker — third independent trigger for market_brief_mailer.
//
// Why it exists: cron-job.org (primary) and GitHub Actions schedule (backup)
// both silently failed simultaneously on 2026-06-03. Cloudflare's cron runs
// on independent infrastructure, so a coordinated outage with the other two
// would require three vendors down at once.
//
// What it does:
//   - 09:30 UTC weekdays  → POST workflow_dispatch to daily-brief.yml
//   - 10:30 UTC weekdays  → POST workflow_dispatch to daily-brief.yml (DST-shift backup)
//   - 12:00 UTC weekdays  → POST workflow_dispatch to watchdog.yml
//
// Why two primary times: Cloudflare cron is UTC-only and can't auto-follow
// DST. 09:30 UTC matches 5:30 EDT (summer); 10:30 UTC matches 5:30 EST
// (winter). Both fire year-round; the brief script's Supabase dedup makes
// the second daily firing a no-op when both succeed.
//
// Auth: a fine-scoped GitHub PAT stored as the GH_PAT Cloudflare secret.
// Scope required: actions:write on cflorczyk9/market_brief_mailer only.

const REPO = "cflorczyk9/market_brief_mailer";
const REF = "main";

async function dispatchWorkflow(workflow, env, inputs = {}) {
  if (!env.GH_PAT) {
    throw new Error("GH_PAT secret not configured");
  }
  const url = `https://api.github.com/repos/${REPO}/actions/workflows/${workflow}/dispatches`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${env.GH_PAT}`,
      "Accept": "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "Content-Type": "application/json",
      "User-Agent": "market-brief-trigger-worker",
    },
    body: JSON.stringify({ ref: REF, inputs }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`GitHub dispatch ${workflow} failed: HTTP ${res.status} ${text.slice(0, 300)}`);
  }
  return `dispatched ${workflow} (HTTP ${res.status})`;
}

export default {
  async scheduled(event, env, ctx) {
    let result;
    if (event.cron === "30 9 * * 1-5" || event.cron === "30 10 * * 1-5") {
      result = await dispatchWorkflow("daily-brief.yml", env, { test_mode: "false" });
    } else if (event.cron === "0 12 * * 1-5") {
      result = await dispatchWorkflow("watchdog.yml", env, {});
    } else {
      result = `unknown cron ${event.cron} — ignoring`;
    }
    console.log(`[${event.scheduledTime}] cron=${event.cron} → ${result}`);
  },

  // Manual trigger via HTTPS, for debugging from a browser/curl.
  // Visit https://market-brief-trigger.<your>.workers.dev/health to confirm
  // the worker is alive without firing GitHub.
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      const hasPAT = Boolean(env.GH_PAT);
      return new Response(JSON.stringify({ ok: true, pat_configured: hasPAT, repo: REPO }) + "\n", {
        headers: { "Content-Type": "application/json" },
      });
    }
    if (url.pathname === "/dispatch-brief") {
      try {
        const r = await dispatchWorkflow("daily-brief.yml", env, { test_mode: "false" });
        return new Response(r + "\n");
      } catch (e) {
        return new Response(`ERR: ${e.message}\n`, { status: 500 });
      }
    }
    if (url.pathname === "/dispatch-watchdog") {
      try {
        const r = await dispatchWorkflow("watchdog.yml", env, {});
        return new Response(r + "\n");
      } catch (e) {
        return new Response(`ERR: ${e.message}\n`, { status: 500 });
      }
    }
    return new Response(
      "market_brief_mailer trigger worker.\n" +
      "Endpoints:\n" +
      "  GET /health             — confirm worker is alive\n" +
      "  GET /dispatch-brief     — manually fire daily-brief.yml\n" +
      "  GET /dispatch-watchdog  — manually fire watchdog.yml\n",
      { headers: { "Content-Type": "text/plain" } },
    );
  },
};
