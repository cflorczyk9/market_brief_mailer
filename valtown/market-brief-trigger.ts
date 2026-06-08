// Val Town scheduled val — SECOND independent punctual trigger for the
// market brief. Lives on Val Town's infra, separate from cron-job.org and
// GitHub, so a cron-job.org outage doesn't take down 6:30 ET delivery.
//
// HOW TO DEPLOY (this file is NOT live until you do this):
//   1. Create a free account at val.town
//   2. New val → paste this file's contents
//   3. Val settings → Environment Variables → add GH_PAT = <a GitHub
//      fine-grained PAT scoped to cflorczyk9/market_brief_mailer with
//      "Actions: Read and write". Use a SEPARATE token from cron-job.org's
//      so the two legs fail independently.
//   4. Val settings → Triggers → Cron → set schedule:  20 10,11 * * 1-5
//      (Val Town cron is UTC. 10:20 UTC = 6:20 EDT; 11:20 UTC = 6:20 EST.
//       Both fire year-round; whichever lands before 6:30 ET holds-and-sends,
//       the other no-ops via the brief's Supabase dedup.)
//   5. Click "Run" once to smoke-test. Expect "dispatched ... HTTP 204" and a
//      new Daily Market Brief run in GitHub Actions.
//
// SELF-MONITORING: this throws on any non-2xx from GitHub (e.g. 401 from an
// expired/revoked PAT). Val Town emails you when a scheduled val errors, so a
// dead token surfaces immediately instead of failing silently.

const REPO = "cflorczyk9/market_brief_mailer";
const WORKFLOW = "daily-brief.yml";

export default async function () {
  const token = Deno.env.get("GH_PAT");
  if (!token) throw new Error("GH_PAT environment variable not set");

  const url = `https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${token}`,
      "Accept": "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "Content-Type": "application/json",
      "User-Agent": "valtown-market-brief-trigger",
    },
    body: JSON.stringify({ ref: "main", inputs: { test_mode: "false" } }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`GitHub dispatch failed: HTTP ${res.status} ${body.slice(0, 300)}`);
  }
  console.log(`dispatched ${WORKFLOW} (HTTP ${res.status})`);
}
