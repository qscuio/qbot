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

test("dashboard assets cannot be retained as a stale frontend", async () => {
  const config = await readFile("../../deploy/qbot-dashboard.nginx.conf", "utf8");
  const cacheHeader = config.indexOf('add_header Cache-Control "no-store" always;');
  const dashboardLocation = config.indexOf("location /dashboard/ {");

  assert.notEqual(cacheHeader, -1);
  assert.ok(cacheHeader < dashboardLocation);
});
