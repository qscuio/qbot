import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("dashboard redirects never expose the private origin port", async () => {
  const config = await readFile("../../deploy/qbot-dashboard.nginx.conf", "utf8");

  assert.doesNotMatch(config, /return 302 \/dashboard\//);
  assert.equal(
    config.match(/return 302 https:\/\/\$host\/dashboard\//g)?.length,
    2,
  );
});

test("dashboard HTML is fresh while versioned assets can be cached", async () => {
  const config = await readFile("../../deploy/qbot-dashboard.nginx.conf", "utf8");
  const html = await readFile("index.html", "utf8");
  const app = await readFile("js/app.js", "utf8");
  const versions = [
    ...html.matchAll(/(?:dashboard\.css|lightweight-charts\.js|app\.js)\?v=([\w.-]+)/g),
    ...app.matchAll(/(?:api\.js|chart\.js|company-panels\.js|state\.js)\?v=([\w.-]+)/g),
  ].map((match) => match[1]);

  assert.equal(versions.length, 7);
  assert.equal(new Set(versions).size, 1);
  assert.equal(versions[0], "20260719.5");
  assert.match(config, /set \$dashboard_cache_control "no-store";/);
  assert.match(
    config,
    /set \$dashboard_cache_control "public, max-age=31536000, immutable";/,
  );
  assert.ok(
    config.includes(
      'if ($request_uri ~* "^/dashboard/.+\\.(?:css|js)\\?v=[A-Za-z0-9._-]+$") {',
    ),
  );
  assert.doesNotMatch(config, /if \(\$uri /);
  assert.match(config, /add_header Cache-Control \$dashboard_cache_control always;/);
});

test("deployment runs the resumable daily-bar repair after the service is healthy", async () => {
  const workflow = await readFile("../../.github/workflows/deploy.yml", "utf8");
  const healthCheck = workflow.indexOf("- name: Health check");
  const repair = workflow.indexOf("- name: Repair persisted OHLCV data");

  assert.ok(healthCheck >= 0);
  assert.ok(repair > healthCheck);
  assert.match(workflow, /command_timeout:\s*6h/);
  assert.match(workflow, /--repair-daily-bars/);
});

test("deployment launches one detached resumable company repair after health", async () => {
  const workflow = await readFile("../../.github/workflows/deploy.yml", "utf8");
  const healthCheck = workflow.indexOf("- name: Health check");
  const repairStart = workflow.indexOf("- name: Start company intelligence repair");
  const nextStep = workflow.indexOf("\n      - name:", repairStart + 1);
  const repair = workflow.slice(repairStart, nextStep);

  assert.ok(healthCheck >= 0);
  assert.ok(repairStart > healthCheck);
  assert.match(repair, /set -e/);
  assert.match(repair, /systemctl is-active --quiet "\$unit"/);
  assert.match(repair, /systemctl show qbot --property=User --value/);
  assert.match(repair, /systemctl show "\$unit" --property=LoadState --value/);
  assert.match(repair, /systemctl stop "\$unit"/);
  assert.match(repair, /systemctl reset-failed "\$unit"/);
  assert.match(repair, /systemd-run --no-block --collect/);
  assert.match(repair, /--unit="\$unit"/);
  assert.match(repair, /unit="qbot-company-intelligence-repair"/);
  assert.match(repair, /--property=Type=exec/);
  assert.match(repair, /--property=EnvironmentFile=\/opt\/qbot\/\.env/);
  assert.match(repair, /--property="User=\$service_user"/);
  assert.match(repair, /--property=UMask=0077/);
  assert.match(repair, /--property=StandardOutput=journal/);
  assert.match(repair, /--property=StandardError=journal/);
  assert.match(repair, /--working-directory=\/opt\/qbot/);
  assert.match(repair, /\/opt\/qbot\/qbot --repair-company-intelligence/);
  assert.match(repair, /queued_state=.*systemctl is-active "\$unit"/);
  assert.match(repair, /active\|activating/);
  assert.match(repair, /journalctl -u "\$unit"/);
  assert.doesNotMatch(repair, /--wait/);
  assert.doesNotMatch(repair, /github\.run_(?:id|attempt)/);
});
